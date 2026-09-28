# Live Location Web GIS — Phase 4

Phase 4 upgrades the application from in-memory session tracking to
**PostgreSQL + PostGIS** storage.

## New architecture

```text
Browser GPS
    ↓
WebSocket
    ↓
FastAPI
    ↓
PostgreSQL + PostGIS
    ├── live_sessions
    └── location_points
           ↓
     Track analytics
           ↓
   Leaflet Web GIS
```

## New features

- PostgreSQL-backed sessions
- PostGIS point geometry (`EPSG:4326`)
- Persistent location history
- Distance travelled using PostGIS geography distance
- Track duration
- Maximum speed
- Average speed
- Track point count
- GeoJSON track download
- Temporary session expiry
- Session revocation
- Host secret stored as a salted PBKDF2 hash in the database
- Viewer remains view-only

## Important privacy/security note

This is still a portfolio/development application.

The viewer link is link-based privacy:
anyone who obtains the viewer URL can view the session until expiry/revocation.

The database stores a hash of the host secret, not the plaintext host secret.

Production use would additionally need:
- HTTPS/WSS
- authentication
- authorization
- rate limiting
- careful CORS policy
- secure secret/session handling
- data retention/deletion policy
- privacy/legal controls

## PostgreSQL/PostGIS setup

Create a PostgreSQL database named:

```text
live_location
```

Enable the PostGIS extension and create the tables by running:

```text
schema.sql
```

In pgAdmin 4:

1. Connect to your PostgreSQL server.
2. Create database `live_location`.
3. Open Query Tool for `live_location`.
4. Open/copy `schema.sql`.
5. Execute it.
6. Confirm that these tables exist:
   - `live_sessions`
   - `location_points`

## Connection file

Copy:

```text
.env.example
```

to:

```text
.env
```

Then update the password:

```text
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/live_location
```

Do not commit `.env` to GitHub.

Add this to `.gitignore`:

```text
.env
venv/
__pycache__/
*.pyc
```

## Install

Open PowerShell in the Phase 4 folder:

```powershell
python -m venv venv
venv\Scripts\activate
python -m pip install -r requirements.txt
```

## Run

```powershell
python -m uvicorn main:app --reload
```

Open:

```text
http://127.0.0.1:8000
```

## Test

1. Create a live link.
2. Open Host View.
3. Start live location.
4. Allow GPS.
5. Open the copied share link in a second browser.
6. Watch the viewer marker update.
7. Check Track Analytics.
8. Click Download Track to create a GeoJSON file.
9. Check PostgreSQL/PostGIS with:

```sql
SELECT COUNT(*) FROM location_points;

SELECT
    public_token,
    captured_at,
    latitude,
    longitude
FROM location_points
ORDER BY captured_at DESC
LIMIT 10;
```

To inspect the geometry:

```sql
SELECT
    id,
    public_token,
    ST_AsText(geom) AS geometry
FROM location_points
LIMIT 10;
```

## Recommended next step

Phase 5:
- nearby hotel/hospital search
- search based on current location
- place cards with distance
- "Open in Google Maps"
- optional Google Places API integration
