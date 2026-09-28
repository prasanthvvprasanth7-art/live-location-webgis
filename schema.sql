CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS live_sessions (
    public_token TEXT PRIMARY KEY,
    host_secret_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_live_sessions_expiry
ON live_sessions (expires_at);

CREATE INDEX IF NOT EXISTS idx_live_sessions_revoked
ON live_sessions (revoked_at);


CREATE TABLE IF NOT EXISTS location_points (
    id BIGSERIAL PRIMARY KEY,
    public_token TEXT NOT NULL
        REFERENCES live_sessions(public_token)
        ON DELETE CASCADE,

    captured_at TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    accuracy_m DOUBLE PRECISION NOT NULL DEFAULT 0,
    speed_mps DOUBLE PRECISION NULL,
    heading_deg DOUBLE PRECISION NULL,

    geom geometry(Point, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_location_points_session_time
ON location_points (public_token, captured_at, id);

CREATE INDEX IF NOT EXISTS idx_location_points_geom
ON location_points
USING GIST (geom);
