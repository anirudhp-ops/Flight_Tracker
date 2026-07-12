# Flight Tracker

Flight Tracker is an experimental real-time flight visualization and delay-propagation system. It combines a React/D3 world map with a FastAPI backend, Redis pub/sub, PostgreSQL event storage, a NetworkX dependency graph, and a small scikit-learn delay model.

The repository currently runs on generated mock flight data by default. Paid FlightAware AeroAPI access is explicitly disabled unless two environment variables are set.

## Current capabilities

- Displays 100 generated flights on an interactive world map.
- Shows routes, aircraft positions, delays, gates, and flight details.
- Defines a FlightAware AeroAPI ingestion client and a no-cost mock client.
- Publishes backend flight events through Redis and streams them over WebSockets.
- Defines PostgreSQL event-history and active-flight tables plus write helpers.
- Builds aircraft-turn and gate-reuse relationships with NetworkX.
- Propagates delays through the graph and attempts gate-conflict reassignment.
- Loads a trained regression model and predicts arrival delay.

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
 ingestion worker -> Redis pub/sub -> FastAPI WebSocket -> browser
                              |               |
                              |               +-> delay model
                              +------------------> graph engine

PostgreSQL support exists for historical events and active-flight state, but persistence is not yet connected to the ingestion worker.
```

## Prerequisites

- Python 3.10+
- Node.js and npm
- PostgreSQL
- Redis

The Python package metadata is currently incomplete, so install the imported backend dependencies directly:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn redis asyncpg python-dotenv httpx pydantic networkx numpy scikit-learn
```

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

Open `http://localhost:3000`.

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

- The map UI works independently but currently generates its own mock flights and does not connect to the backend WebSocket.
- The backend pipeline is implemented, but it depends on running PostgreSQL and Redis services.
- Live ingestion publishes events to Redis but does not currently persist them through the database writer.
- Database schema creation exists but is not wired into normal server startup or a migration command.
- Python dependencies are not declared in `setup.py` or a requirements/lock file.
- Backend automated tests, deployment configuration, authentication, observability, and robust error handling are still missing.
- The included ML model is ignored by Git, so a fresh clone may require retraining with `ml/train.py` and the source CSV.
- CORS is configured only for the local React development server.

## Recommended next steps

1. Connect `FlightMap` to `/ws/{airport_code}` and use backend events instead of frontend-only generated flights.
2. Add a requirements file or modern `pyproject.toml`, plus database migrations.
3. Add backend unit/integration tests for parsing, graph propagation, and ingestion safety.
4. Make PostgreSQL/Redis startup reproducible with Docker Compose.
5. Add rate limits, usage monitoring, and budget alerts before considering live API access again.
