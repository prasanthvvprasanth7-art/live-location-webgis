from pathlib import Path
from typing import Any
from datetime import datetime, timezone
import asyncio
import hashlib
import hmac
import json
import os
import secrets
import time

import psycopg
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is missing. Create .env from .env.example and set your PostgreSQL/PostGIS connection string."
    )

app = FastAPI(title="Live Location Web GIS - Phase 4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


VALID_DURATIONS = {15, 30, 60, 120}


class CreateSessionRequest(BaseModel):
    duration_minutes: int = Field(default=60)


class LiveSession:
    def __init__(self, token: str):
        self.public_token = token
        self.host_socket: WebSocket | None = None
        self.viewer_sockets: dict[str, WebSocket] = {}


sessions: dict[str, LiveSession] = {}


def db():
    return psycopg.connect(DATABASE_URL)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def hash_host_secret(secret: str, salt: bytes | None = None) -> str:
    if salt is None:
        salt = secrets.token_bytes(16)

    digest = hashlib.pbkdf2_hmac(
        "sha256",
        secret.encode("utf-8"),
        salt,
        200_000,
    )

    return f"{salt.hex()}${digest.hex()}"


def verify_host_secret(secret: str, stored: str) -> bool:
    try:
        salt_hex, digest_hex = stored.split("$", 1)
        salt = bytes.fromhex(salt_hex)
    except (ValueError, TypeError):
        return False

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        secret.encode("utf-8"),
        salt,
        200_000,
    )

    return hmac.compare_digest(candidate.hex(), digest_hex)


def get_session_row(public_token: str) -> dict[str, Any] | None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    public_token,
                    host_secret_hash,
                    created_at,
                    expires_at,
                    revoked_at
                FROM live_sessions
                WHERE public_token = %s
                """,
                (public_token,),
            )
            row = cur.fetchone()

            if not row:
                return None

            return {
                "public_token": row[0],
                "host_secret_hash": row[1],
                "created_at": row[2],
                "expires_at": row[3],
                "revoked_at": row[4],
            }


def session_is_active(row: dict[str, Any]) -> bool:
    if row is None:
        return False

    if row["revoked_at"] is not None:
        return False

    return row["expires_at"] > utc_now()


def location_summary(public_token: str) -> dict[str, Any]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS point_count,
                    MIN(captured_at) AS first_point,
                    MAX(captured_at) AS last_point,
                    COALESCE(MAX(speed_mps), 0) AS max_speed_mps,
                    COALESCE(AVG(NULLIF(speed_mps, 0)), 0) AS avg_speed_mps
                FROM location_points
                WHERE public_token = %s
                """,
                (public_token,),
            )
            row = cur.fetchone()

            point_count = row[0] or 0
            first_point = row[1]
            last_point = row[2]
            max_speed_mps = float(row[3] or 0)
            avg_speed_mps = float(row[4] or 0)

            cur.execute(
                """
                WITH ordered AS (
                    SELECT
                        geom,
                        LAG(geom) OVER (
                            ORDER BY captured_at, id
                        ) AS previous_geom
                    FROM location_points
                    WHERE public_token = %s
                ),
                segments AS (
                    SELECT
                        ST_Distance(
                            geom::geography,
                            previous_geom::geography
                        ) AS meters
                    FROM ordered
                    WHERE previous_geom IS NOT NULL
                )
                SELECT COALESCE(SUM(meters), 0)
                FROM segments
                """,
                (public_token,),
            )

            distance_m = float(cur.fetchone()[0] or 0)

    duration_seconds = 0.0

    if first_point and last_point:
        duration_seconds = max(
            0.0,
            (last_point - first_point).total_seconds(),
        )

    return {
        "point_count": point_count,
        "first_point": first_point.isoformat() if first_point else None,
        "last_point": last_point.isoformat() if last_point else None,
        "distance_m": round(distance_m, 2),
        "distance_km": round(distance_m / 1000.0, 3),
        "duration_seconds": round(duration_seconds, 1),
        "duration_minutes": round(duration_seconds / 60.0, 1),
        "max_speed_kmh": round(max_speed_mps * 3.6, 2),
        "avg_speed_kmh": round(avg_speed_mps * 3.6, 2),
    }


async def send_json(websocket: WebSocket, data: dict[str, Any]) -> None:
    await websocket.send_text(json.dumps(data, default=str))


async def broadcast_viewers(
    session: LiveSession,
    data: dict[str, Any],
) -> None:
    payload = json.dumps(data, default=str)
    dead = []

    for viewer_id, websocket in list(session.viewer_sockets.items()):
        try:
            await websocket.send_text(payload)
        except Exception:
            dead.append(viewer_id)

    for viewer_id in dead:
        session.viewer_sockets.pop(viewer_id, None)


async def close_session_connections(
    session: LiveSession,
    reason: str,
) -> None:
    payload = {
        "type": "session_closed",
        "reason": reason,
    }

    sockets = []

    if session.host_socket is not None:
        sockets.append(session.host_socket)

    sockets.extend(session.viewer_sockets.values())

    for websocket in sockets:
        try:
            await send_json(websocket, payload)
            await websocket.close(code=1000)
        except Exception:
            pass

    session.host_socket = None
    session.viewer_sockets.clear()


async def expiry_loop() -> None:
    while True:
        await asyncio.sleep(10)

        now = utc_now()

        with db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT public_token
                    FROM live_sessions
                    WHERE revoked_at IS NULL
                      AND expires_at <= %s
                    """,
                    (now,),
                )

                expired_tokens = [
                    row[0]
                    for row in cur.fetchall()
                ]

                if expired_tokens:
                    cur.execute(
                        """
                        UPDATE live_sessions
                        SET revoked_at = %s
                        WHERE public_token = ANY(%s)
                        """,
                        (
                            now,
                            expired_tokens,
                        ),
                    )

        for token in expired_tokens:
            session = sessions.pop(token, None)

            if session is not None:
                await close_session_connections(
                    session,
                    "This live-location link has expired.",
                )


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(expiry_loop())


@app.get("/")
async def home():
    index_file = STATIC_DIR / "index.html"

    if not index_file.exists():
        raise HTTPException(
            status_code=500,
            detail="static/index.html was not found",
        )

    return FileResponse(index_file)


@app.get("/share/{public_token}")
async def shared_page(public_token: str):
    row = get_session_row(public_token)

    if not session_is_active(row):
        raise HTTPException(
            status_code=404,
            detail="This live-location link is invalid, revoked, or expired.",
        )

    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health():
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int
                FROM live_sessions
                WHERE revoked_at IS NULL
                  AND expires_at > %s
                """,
                (utc_now(),),
            )

            active_sessions = cur.fetchone()[0]

    return {
        "status": "ok",
        "service": "live-location-phase-4",
        "active_sessions": active_sessions,
        "database": "PostgreSQL/PostGIS",
    }


@app.post("/api/sessions")
async def create_session(request: CreateSessionRequest):
    if request.duration_minutes not in VALID_DURATIONS:
        raise HTTPException(
            status_code=400,
            detail="Duration must be 15, 30, 60, or 120 minutes.",
        )

    public_token = secrets.token_urlsafe(24)
    host_secret = secrets.token_urlsafe(32)
    host_secret_hash = hash_host_secret(host_secret)

    created_at = utc_now()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO live_sessions (
                    public_token,
                    host_secret_hash,
                    created_at,
                    expires_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    public_token,
                    host_secret_hash,
                    created_at,
                    created_at.timestamp()
                    and created_at
                    + __import__("datetime").timedelta(
                        minutes=request.duration_minutes
                    ),
                ),
            )

    sessions[public_token] = LiveSession(public_token)

    row = get_session_row(public_token)

    return {
        "public_token": public_token,
        "host_secret": host_secret,
        "share_url": f"/share/{public_token}",
        "expires_at": row["expires_at"].isoformat(),
        "duration_minutes": request.duration_minutes,
    }


@app.get("/api/sessions/{public_token}")
async def session_info(public_token: str):
    row = get_session_row(public_token)

    if not session_is_active(row):
        raise HTTPException(
            status_code=404,
            detail="This live-location link is invalid, revoked, or expired.",
        )

    summary = location_summary(public_token)

    return {
        "public_token": public_token,
        "created_at": row["created_at"].isoformat(),
        "expires_at": row["expires_at"].isoformat(),
        "host_online": (
            public_token in sessions
            and sessions[public_token].host_socket is not None
        ),
        "has_location": summary["point_count"] > 0,
        "summary": summary,
    }


@app.get("/api/sessions/{public_token}/stats")
async def session_stats(public_token: str):
    row = get_session_row(public_token)

    if not session_is_active(row):
        raise HTTPException(
            status_code=404,
            detail="This live-location link is invalid, revoked, or expired.",
        )

    return location_summary(public_token)


@app.post("/api/sessions/{public_token}/revoke")
async def revoke_session(public_token: str, host_secret: str):
    row = get_session_row(public_token)

    if not session_is_active(row):
        raise HTTPException(
            status_code=404,
            detail="This live-location link is invalid, revoked, or expired.",
        )

    if not verify_host_secret(
        host_secret,
        row["host_secret_hash"],
    ):
        raise HTTPException(
            status_code=403,
            detail="Invalid host credentials.",
        )

    now = utc_now()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE live_sessions
                SET revoked_at = %s
                WHERE public_token = %s
                """,
                (now, public_token),
            )

    session = sessions.pop(public_token, None)

    if session is not None:
        await close_session_connections(
            session,
            "The location owner revoked this share link.",
        )

    return {
        "status": "revoked",
        "message": "Live-location session revoked.",
    }


@app.get("/api/sessions/{public_token}/track.geojson")
async def track_geojson(public_token: str):
    row = get_session_row(public_token)

    if not session_is_active(row):
        raise HTTPException(
            status_code=404,
            detail="This live-location link is invalid, revoked, or expired.",
        )

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    json_build_object(
                        'type', 'FeatureCollection',
                        'features',
                        COALESCE(
                            json_agg(
                                json_build_object(
                                    'type', 'Feature',
                                    'geometry', ST_AsGeoJSON(geom)::json,
                                    'properties', json_build_object(
                                        'captured_at', captured_at,
                                        'accuracy_m', accuracy_m,
                                        'speed_mps', speed_mps,
                                        'heading_deg', heading_deg
                                    )
                                )
                                ORDER BY captured_at, id
                            ),
                            '[]'::json
                        )
                    )::text
                FROM location_points
                WHERE public_token = %s
                """,
                (public_token,),
            )

            geojson_text = cur.fetchone()[0]

    return Response(
        content=geojson_text,
        media_type="application/geo+json",
    )


@app.websocket("/ws/{public_token}")
async def websocket_endpoint(
    websocket: WebSocket,
    public_token: str,
):
    row = get_session_row(public_token)

    if not session_is_active(row):
        await websocket.close(
            code=1008,
            reason="Invalid or expired share link.",
        )
        return

    supplied_secret = websocket.query_params.get("host_secret")

    is_host = bool(
        supplied_secret
        and verify_host_secret(
            supplied_secret,
            row["host_secret_hash"],
        )
    )

    if supplied_secret and not is_host:
        await websocket.close(
            code=1008,
            reason="Invalid host credentials.",
        )
        return

    await websocket.accept()

    session = sessions.setdefault(
        public_token,
        LiveSession(public_token),
    )

    if is_host:
        if session.host_socket is not None:
            try:
                await session.host_socket.close(
                    code=1000,
                    reason="Host reconnected.",
                )
            except Exception:
                pass

        session.host_socket = websocket

        await send_json(
            websocket,
            {
                "type": "role",
                "role": "host",
                "expires_at": row["expires_at"].isoformat(),
            },
        )

        await send_json(
            websocket,
            {
                "type": "host_status",
                "online": True,
            },
        )

        await broadcast_viewers(
            session,
            {
                "type": "host_status",
                "online": True,
            },
        )

        try:
            while True:
                current = get_session_row(public_token)

                if not session_is_active(current):
                    await close_session_connections(
                        session,
                        "This live-location link has expired or been revoked.",
                    )
                    sessions.pop(public_token, None)
                    return

                raw_message = await websocket.receive_text()

                try:
                    message = json.loads(raw_message)
                except json.JSONDecodeError:
                    await send_json(
                        websocket,
                        {
                            "type": "error",
                            "message": "Invalid message format.",
                        },
                    )
                    continue

                if message.get("type") != "location":
                    continue

                try:
                    latitude = float(message["latitude"])
                    longitude = float(message["longitude"])
                    accuracy = float(message.get("accuracy", 0))
                except (KeyError, TypeError, ValueError):
                    await send_json(
                        websocket,
                        {
                            "type": "error",
                            "message": "Invalid GPS values.",
                        },
                    )
                    continue

                if not -90 <= latitude <= 90:
                    continue

                if not -180 <= longitude <= 180:
                    continue

                speed = message.get("speed")
                heading = message.get("heading")
                timestamp_ms = message.get(
                    "timestamp",
                    int(time.time() * 1000),
                )

                try:
                    speed_mps = (
                        float(speed)
                        if speed is not None
                        else None
                    )
                except (TypeError, ValueError):
                    speed_mps = None

                try:
                    heading_deg = (
                        float(heading)
                        if heading is not None
                        else None
                    )
                except (TypeError, ValueError):
                    heading_deg = None

                try:
                    captured_at = datetime.fromtimestamp(
                        float(timestamp_ms) / 1000,
                        tz=timezone.utc,
                    )
                except (TypeError, ValueError, OSError):
                    captured_at = utc_now()

                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO location_points (
                                public_token,
                                captured_at,
                                latitude,
                                longitude,
                                accuracy_m,
                                speed_mps,
                                heading_deg,
                                geom
                            )
                            VALUES (
                                %s,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s,
                                %s,
                                ST_SetSRID(
                                    ST_MakePoint(%s, %s),
                                    4326
                                )
                            )
                            """,
                            (
                                public_token,
                                captured_at,
                                latitude,
                                longitude,
                                max(0.0, accuracy),
                                speed_mps,
                                heading_deg,
                                longitude,
                                latitude,
                            ),
                        )

                location = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "accuracy": max(0.0, accuracy),
                    "speed": speed_mps,
                    "heading": heading_deg,
                    "timestamp": int(
                        captured_at.timestamp() * 1000
                    ),
                }

                await broadcast_viewers(
                    session,
                    {
                        "type": "location",
                        "location": location,
                    },
                )

                await send_json(
                    websocket,
                    {
                        "type": "location",
                        "location": location,
                    },
                )

        except WebSocketDisconnect:
            if session.host_socket is websocket:
                session.host_socket = None

            await broadcast_viewers(
                session,
                {
                    "type": "host_status",
                    "online": False,
                },
            )

        except Exception:
            if session.host_socket is websocket:
                session.host_socket = None

            await broadcast_viewers(
                session,
                {
                    "type": "host_status",
                    "online": False,
                },
            )

    else:
        viewer_id = secrets.token_urlsafe(8)
        session.viewer_sockets[viewer_id] = websocket

        await send_json(
            websocket,
            {
                "type": "role",
                "role": "viewer",
                "expires_at": row["expires_at"].isoformat(),
            },
        )

        await send_json(
            websocket,
            {
                "type": "host_status",
                "online": session.host_socket is not None,
            },
        )

        try:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            latitude,
                            longitude,
                            accuracy_m,
                            speed_mps,
                            heading_deg,
                            EXTRACT(EPOCH FROM captured_at) * 1000
                        FROM location_points
                        WHERE public_token = %s
                        ORDER BY captured_at DESC, id DESC
                        LIMIT 1
                        """,
                        (public_token,),
                    )

                    latest = cur.fetchone()

            if latest:
                await send_json(
                    websocket,
                    {
                        "type": "location",
                        "location": {
                            "latitude": latest[0],
                            "longitude": latest[1],
                            "accuracy": latest[2],
                            "speed": latest[3],
                            "heading": latest[4],
                            "timestamp": int(latest[5]),
                        },
                    },
                )

            while True:
                current = get_session_row(public_token)

                if not session_is_active(current):
                    await websocket.close(
                        code=1000,
                        reason="Live-location link expired.",
                    )
                    return

                # Viewer is view-only; this keeps the socket open.
                await websocket.receive_text()

        except WebSocketDisconnect:
            session.viewer_sockets.pop(viewer_id, None)

        except Exception:
            session.viewer_sockets.pop(viewer_id, None)
