# Flight Tracker

Flight Tracker is an experimental real-time flight visualization and delay-propagation system. It combines a React/D3 world map with a FastAPI backend, Redis pub/sub, PostgreSQL event storage, a NetworkX dependency graph, and a small scikit-learn delay model.

## Current capabilities

- Displays live flights streamed from the backend over WebSocket on an interactive world map, with automatic reconnect if the connection drops.
- Shows routes, aircraft positions, delays, gates, and flight details.
- Defines a FlightAware AeroAPI ingestion client and a no-cost mock client.
- Publishes backend flight events through Redis, streams them over WebSockets, and persists every polled snapshot to PostgreSQL.
- Builds aircraft-turn and gate-reuse relationships with NetworkX.
- Propagates delays through the graph and reassigns conflicting gates.
- Loads a trained regression model and predicts arrival delay, using the flight's own schedule to estimate air time/distance when the source doesn't supply them.
- Prunes landed flights out of the in-memory graph after 24 hours so it doesn't grow unbounded for the life of the process.

## Cost protection and API key

Live FlightAware calls are off by default, even if `FLIGHTAWARE_API_KEY` is still present in your shell or `.env` file. The backend and `test_api.py` require both:

```env
ENABLE_FLIGHTAWARE_API=true
FLIGHTAWARE_API_KEY=your_key
```

Keep `ENABLE_FLIGHTAWARE_API=false` (or unset it) to use mock data and avoid AeroAPI usage.

This code-level switch prevents this project from making paid calls; it does not revoke the credential at FlightAware. To fully disable the key, delete or rotate it in the FlightAware account dashboard and remove it from shell profiles, deployment secrets, and any local `.env` file. Never commit the key.

## Architecture

```text
FlightAware or mock client
          |
          v
   ingestion worker ──┬──> Redis pub/sub ──> FastAPI WebSocket ──> React/D3 browser
                       │                            |      |
                       │                            |      +──> delay model
                       │                            +──────────> graph engine
                       v
                  PostgreSQL (flight_events, active_flights)
```

`graph_engine.load_from_db()` reloads `active_flights` into memory on backend
startup; `GraphEngine.prune_expired_flights()` removes landed flights more
than 24 hours old from the in-memory graph (Postgres rows are not pruned).

## Prerequisites

- Python 3.10+
- Node.js and npm
- PostgreSQL
- Redis

Install the pinned backend dependencies from `requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` installs `uvicorn[standard]`, which pulls in `websockets`.
Plain `uvicorn` (with no extras) does not include WebSocket support and will
silently 404 every request to `/ws/{airport_code}` — install from
`requirements.txt` rather than `pip install fastapi uvicorn ...` by hand.

Install the frontend dependencies:

```bash
cd frontend
npm install
cd ..
```

## Configuration

Copy the example configuration and edit database values as needed:

```bash
cp .env.example .env
```

The safe default is mock ingestion:

```env
ENABLE_FLIGHTAWARE_API=false
TARGET_AIRPORT=KJFK
```

Create the PostgreSQL database if it does not exist:

```bash
createdb flight_tracker
```

The schema is defined in `flight_tracker/db/writer.py`. The backend expects the `active_flights` table during startup, so initialize the schema before the first full backend run.

## Running the project

Start PostgreSQL and Redis, then start the backend from the repository root:

```bash
uvicorn flight_tracker.server:app --reload
```

In another terminal, start the frontend:

```bash
cd frontend
npm start
```

Open `http://localhost:3000`. The frontend expects the backend at
`http://localhost:8000` (uvicorn's default) and fetches `/api/config`, then
connects to `/ws/{airport_code}` for live flight updates. If the backend
runs somewhere else, set `REACT_APP_BACKEND_URL` before starting the dev
server, e.g. `REACT_APP_BACKEND_URL=http://localhost:9000 npm start`.

## Tests and checks

Frontend tests:

```bash
cd frontend
npm test -- --watchAll=false
```

Frontend production build:

```bash
cd frontend
npm run build
```

There is not yet a backend unit-test suite. `test_api.py` is a manual live integration script, not an automated test; it can incur FlightAware charges and is blocked unless live API access is explicitly enabled.

## Project status

This is a functional prototype, not yet a production-ready end-to-end tracker.

- The map UI connects to the backend's `/ws/{airport_code}` WebSocket and renders live flight events, with automatic reconnect; it no longer generates its own mock flights.
- The backend pipeline is implemented, but it depends on running PostgreSQL and Redis services.
- The ingestion worker persists every polled snapshot to PostgreSQL (`flight_events` + `active_flights`) and ensures the schema exists on startup, but nothing prunes old rows from Postgres itself — only the in-memory graph is pruned.
- Backend automated tests, deployment configuration, authentication, and observability are still missing. The frontend has a small Jest/RTL test suite; the backend does not.
- The included ML model is ignored by Git, so a fresh clone requires retraining with `ml/train.py` (paths are resolved relative to the script, not hardcoded) and the source CSV.
- CORS is configured only for the local React development server (`http://localhost:3000`).

## Recommended next steps

1. Add a backend automated test suite (pytest) for graph propagation, event parsing, and ingestion safety.
2. Add database migrations and a cleanup job/TTL for `flight_events` and `active_flights` in Postgres.
3. Make PostgreSQL/Redis startup reproducible with Docker Compose.
4. Add authentication and rate limiting before deploying anywhere shared.
5. Add usage monitoring and budget alerts before considering live FlightAware API access again.
