# Flight Tracker — Architecture Assessment

Assessed 2026-08-07. Based on a full read of every source file in the repo and a live run of the backend and frontend on mock data (details in "Verification" at the end). This is a snapshot of a functional prototype, not a production system — the goal here is precision about what exists, not a sales pitch.

## Current architecture (as verified)

```
                                   ┌───────────────────────────┐
                                   │   MockFlightAwareClient    │  (default; live client
                                   │   or FlightAwareClient     │   exists but is gated off)
                                   └──────────────┬─────────────┘
                                                   │ AirportSnapshot
                                                   ▼
                                   ┌───────────────────────────┐
                                   │  ingestion/worker.py       │  poll loop, every
                                   │  run(client, publisher)    │  POLL_INTERVAL_SECONDS
                                   └──────────────┬─────────────┘
                                                   │ per-flight publish()
                                                   ▼
                                   ┌───────────────────────────┐
                                   │  Redis pub/sub              │  channel: flights:{airport}
                                   └──────────────┬─────────────┘
                                                   │ pubsub.listen()
                                                   ▼
┌────────────────────┐   on connect: dump graph   ┌───────────────────────────┐
│  Browser WebSocket  │◄──────────────────────────┤  FastAPI /ws/{airport}     │
│  client (none exist │   then: stream new/        │  server.py                │
│  in the app today)  │   propagated/reassigned    │                            │
└─────────────────────┘   events as they arrive    └──────┬──────────┬─────────┘
                                                            │          │
                                              GraphEngine   │          │  DelayPredictor
                                          (in-memory nx     │          │  (ml/model.pkl,
                                           DiGraph, per-    │          │   RandomForest,
                                           process, lost    │          │   loaded once
                                           on restart)      │          │   at import time)
                                                            ▼          ▼
                                             aircraft_turn / gate_reuse edges,
                                             delay propagation (BFS, 0.75 decay),
                                             gate-conflict reassignment

  ┌──────────────────────────────┐
  │ PostgreSQL                    │  flight_events (append log) + active_flights
  │ flight_tracker/db/writer.py   │  (upsert). Schema + write path exist but are
  │                                │  ONLY invoked by test_api.py, never by the
  │                                │  running server or the ingestion worker.
  └──────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────┐
│  React frontend (frontend/src)                                          │
│  App.js → FlightMap.jsx: generates its OWN 100 mock flights client-side │
│  with D3 + topojson on an SVG world map. Does not open a WebSocket,     │
│  does not call /api/config, has no network dependency on the backend   │
│  at all. frontend/src/data/flights.js is separate mock data, imported  │
│  nowhere — dead code.                                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

The backend pipeline (mock client → Redis → WebSocket → graph → predictor) is real and runs end-to-end. The frontend is a **separate, self-contained demo** that happens to live in the same repo — today there are two independent "mock flight" systems that never talk to each other.

---

## Component-by-component

### 1. Backend entry point & FastAPI routes — `flight_tracker/server.py`

**Implementation:** Single-file FastAPI app. Three surfaces:
- `GET /api/config` — returns `{"target_airport": ...}` from env. Unused by the frontend today.
- `@app.on_event("startup")` — opens an asyncpg pool, calls `graph_engine.load_from_db(pool)`, then decides mock vs. live client and spawns the ingestion worker as an `asyncio.create_task`.
- `@app.websocket("/ws/{airport_code}")` — on connect, dumps every node currently in the in-memory graph, then subscribes to Redis and streams each incoming event plus any delay-propagation/gate-reassignment side effects.

**Works well:**
- The cost-protection gate (`ENABLE_FLIGHTAWARE_API` + non-empty, non-placeholder key, both required) is correctly implemented and defaults safely closed (`server.py:51-60`).
- `on_event("startup")` correctly does one-time setup (pool, graph load, worker spawn) rather than doing it per-request.

**Needs refactoring:**
- `main.py` is an empty file at the repo root — a dead/misleading entry point. Either wire it up (`uvicorn.run(...)`) or delete it.
- `startup()` hardcodes `DB_USER` default as `"anirudhparasramouria"` (`server.py:43`), while `.env.example`, the README, and `db/writer.py`'s own call sites all use `"postgres"`. Whoever doesn't set `DB_USER` explicitly gets a role that only exists on the original author's machine.
- The asyncpg pool opened at startup is never closed and never referenced again after `load_from_db` — `graph_engine.load_from_db` is the only consumer, yet the pool stays alive for the app's lifetime for no reason. No `shutdown` handler to close it or the Redis client.
- `asyncio.create_task(worker_run(...))` — the task object is discarded. If the worker task raises an unhandled exception, it dies silently; nothing supervises or restarts it, and there is no `add_done_callback` to at least log the failure loudly.
- The WebSocket handler mixes three responsibilities in one function: graph mutation, ML prediction, and gate reassignment. It also predicts using **hardcoded zero placeholders** for `air_time` and `distance` (`server.py:91-93`) — two of the model's real features (see ML section) — even though the model was trained expecting nonzero values for both. This silently degrades prediction quality rather than failing loudly.
- One shared `graph_engine` and one shared `redis_client`/`pubsub` model per WebSocket connection is fine for the current single-airport prototype, but if two browser tabs connect to `/ws/KJFK` simultaneously, both get independent `pubsub.subscribe` calls against the same channel — not wrong, but worth knowing it doesn't currently fan out from one shared subscription.
- No exception handling in the WebSocket loop beyond `WebSocketDisconnect`. A malformed Redis payload (`FlightEvent.model_validate_json` failure) would crash the whole handler for that connection.

**Missing for production:** authentication/authorization on `/ws/*`, request logging, health-check endpoint, rate limiting, structured logging instead of `print()`, and any error surface for the frontend when the WebSocket drops.

### 2. Database schema & queries — `flight_tracker/db/writer.py`

**Implementation:** Two tables defined in a raw `CREATE_TABLES_SQL` string — `flight_events` (append-only history) and `active_flights` (upsert-on-`flight_key`, guarded by `WHERE active_flights.last_updated < EXCLUDED.last_updated` so out-of-order writes can't regress state). `write_events()` batches both inserts in one transaction via `executemany`.

**Works well:**
- The upsert guard against stale writes (`writer.py:111`) is a genuinely good pattern for out-of-order event delivery.
- Using a real transaction across the two `executemany` calls (`writer.py:84`) keeps `flight_events` and `active_flights` consistent.

**Needs refactoring:**
- **This code path is dead in normal operation.** `ensure_schema()` and `write_events()` are called only from `test_api.py`, the manual/paid-API script — never from `server.py` or `ingestion/worker.py`. Verified by grep: zero other call sites. The README already flags this ("persistence is not yet connected to the ingestion worker") but it's worth restating plainly: **running the app today writes nothing to Postgres.** The only reason `GraphEngine.load_from_db` has anything to load is leftover rows from a previous manual `test_api.py` run (confirmed live — see Verification).
- No migration tooling — `CREATE TABLE IF NOT EXISTS` run ad hoc is the entire schema story. Any future column change has no upgrade path.
- Raw SQL string with no versioning; schema and ORM-less model definitions (`models/events.py`) can drift silently since nothing type-checks one against the other.
- `active_flights` has no TTL/cleanup — flights never get removed after landing, so the table grows forever and, worse, **stale rows get reloaded into the live graph on every server restart** (verified: a prior live-API test run left flights dated back to 2026-05-19 in the table, and they appeared in a fresh server's in-memory graph and WebSocket snapshot alongside brand-new mock flights).

**Missing for production:** connection retry/backoff, migrations (Alembic or similar), a scheduled job (or ingestion-time logic) to expire landed/cancelled flights, indices beyond the single `flight_key` index, and the ingestion worker actually calling `write_events`.

### 3. Redis usage — `flight_tracker/ingestion/publisher.py` + `server.py`

**Implementation:** Thin pub/sub only — `RedisPublisher.publish()` does `redis.publish(f"flights:{airport}", event.model_dump_json())`; the WebSocket handler subscribes to the same channel. No Redis Streams, no consumer groups, no persistence in Redis itself — it is purely a fan-out bus between the poll loop and connected WebSocket clients.

**Works well:** Minimal and correct for what it does. `redis.asyncio` client, async pub/sub, clean separation via the `RedisPublisher` wrapper — verified live traffic flowing through it.

**Needs refactoring:**
- Pub/sub means **any event published while no WebSocket client is subscribed is lost forever** — there's no durability. Since the initial graph dump on connect papers over most of this for a single long-running server, it's not visible until the process restarts (see DB point above) or multiple airports/clients are involved.
- No reconnect/retry logic if Redis drops mid-stream; `aioredis.from_url` is created once at import time (`server.py:34`) with no health check.
- `redis_client` and `pubsub` objects aren't explicitly closed anywhere (no shutdown hook).

**Missing for production:** Redis Streams (or similar) if replay/at-least-once delivery matters, connection resilience, and observability (queue depth, publish/consume rates).

### 4. WebSocket implementation

Covered mostly above. One more concrete gap worth flagging on its own: **plain `uvicorn` (as the README's install command literally specifies: `pip install fastapi uvicorn redis ...`) does not include WebSocket support.** Verified live — starting the server with only `uvicorn` installed causes every `/ws/*` request to 404 with `WARNING: No supported WebSocket library detected`. It only started working after separately installing `websockets` (or `uvicorn[standard]`). This is a real first-run trap for anyone following the README exactly as written, and the core feature (`/ws/{airport_code}`) is silently broken until it's fixed.

### 5. ML pipeline — `ml/train.py`, `ml/predictor.py`

**Implementation:** `train.py` is a standalone script (not invoked by anything else) that reads a local CSV, label-encodes carrier/origin/dest, trains a `RandomForestRegressor` on `[carrier, origin, dest, dep_delay, air_time, distance, carrier_delay, weather_delay, nas_delay, late_aircraft_delay]` → `arr_delay`, and pickles the bundle to `ml/model.pkl`. `predictor.py` loads that pickle once and exposes `.predict(...)`.

**Works well:**
- `predictor.py`'s unseen-label fallback (`except ValueError: return dep_delay`, `predictor.py:22-24`) is a sensible degrade-gracefully choice for airlines/airports not in the training set.
- Clean separation between training (offline, one-off) and inference (`DelayPredictor` class used at runtime) is the right shape.

**Needs refactoring:**
- `train.py` has **absolute, machine-specific paths** hardcoded twice (`/Users/anirudhparasramouria/Desktop/Flight_Tracker/T_ONTIME_MARKETING.csv` and `.../ml/model.pkl`, `train.py:9` and `train.py:61`) — not portable to any other machine or CI.
- As noted in the server section, the only runtime call site (`server.py:87-94`) passes `air_time=0, distance=0` unconditionally — two of the ten trained features are always wrong at inference time. The model was trained on real nonzero air-time/distance values, so live predictions are running on inputs outside the distribution they were fit on. This isn't a crash, just a real accuracy problem, silently.
- `model.pkl` is gitignored (`*.pkl`), so a fresh clone has no model and `DelayPredictor.__init__` will raise `FileNotFoundError` at **module import time** (`server.py:12` imports `ml.predictor`, `server.py:35` instantiates it) — the whole server fails to start, not just the prediction feature, until someone manually runs `train.py` (which itself needs the also-gitignored CSV).
- Version skew: the checked-in `model.pkl` (if present) was pickled under scikit-learn 1.7.2; installing the versions the README asks for today (no pin) resolves to 1.9.x, producing `InconsistentVersionWarning` on every load. Verified live. Not fatal today, but pickle-based model persistence across sklearn versions is fragile.
- No model evaluation artifact is kept (MAE/R² are printed to stdout and discarded) — no way to compare model versions over time.

**Missing for production:** dependency-injected paths (env var or config, not hardcoded), a documented/scripted way to reproduce `model.pkl`, a model registry or at least a versioned filename, passing real `air_time`/`distance` at inference (from the flight event or a route-distance lookup), and pinning `scikit-learn` in requirements.

### 6. Graph logic — `flight_tracker/graph/engine.py`

**Implementation:** `GraphEngine` wraps a `networkx.DiGraph`. `add_edges_for_flight` links flights sharing an aircraft (`aircraft_turn`) or an overlapping gate window (`gate_reuse`) by scanning **every existing node** on each new event (`O(n)` per event, `O(n²)` overall). `propagate_delay` does a BFS from the triggering flight, decaying delay by 0.75 per hop and taking `max(existing, propagated)`. `resolve_gate_conflicts` scans all `gate_reuse` edges, and on overlap reassigns the downstream flight to the first free gate from a fixed 56-gate pool (`A1–A14, B1–B14, C1–C14, T1–T14`).

**Works well:**
- The delay-propagation BFS with decay and `max()`-merge (not blind overwrite) is a reasonable, understandable model for cascading delays — verified live via the WebSocket stream showing propagated events distinct from source events.
- Modeling both `aircraft_turn` and `gate_reuse` as distinct edge types is a clean way to represent two genuinely different propagation mechanisms.
- The whole thing is deliberately simple and legible — a real strength for a prototype meant to demonstrate the concept.

**Needs refactoring:**
- `add_edges_for_flight` is O(n) per new event against every node ever added, with no expiry — this degrades linearly (then, across many events, quadratically) as the graph grows, and it never removes old/landed flights, so it grows unbounded for the life of the process.
- The whole graph lives in a single process's memory (`self.graph`) — it is **lost on every restart** except for whatever gets reloaded from `active_flights` (which, per the DB section, the running app never writes to, so `load_from_db` is really just replaying stale test data, not real state).
- `resolve_gate_conflicts` reassigns using `free_gates[0]` (first available), and runs on **every single incoming event** regardless of whether that event changed anything gate-related — it re-scans all edges every time (`server.py:101`, called unconditionally in the WebSocket loop).
- Delay decay factor (0.75) and the gate pool (56 fixed gates, `A/B/C/T` × 1–14) are magic numbers with no real-airport basis and no way to configure per target airport.
- `propagate_delay`'s BFS mutates `self.graph.nodes[...]["delay_minutes"]` directly while iterating — works today because it's single-threaded asyncio, but there's no lock/guard if this were ever made concurrent.

**Missing for production:** graph pruning/expiry for landed or old flights, persistence of graph state across restarts (or acceptance that it's intentionally ephemeral, documented as such), and real gate-inventory data instead of a synthetic pool.

### 7. Frontend — React/D3 (`frontend/src`)

**Implementation:** `App.js` renders a single `<FlightMap />`. `FlightMap.jsx` (352 lines) is self-contained: a hardcoded ~140-entry `airports` lat/lon table, `generateMockFlights()` producing 100 random flights entirely client-side, D3 (`d3.geoNaturalEarth1`) rendering a world map from a local `countries-110m.json` (with a CDN fallback), bezier-curve flight paths, clickable plane markers with a detail side panel, and a legend.

**Works well:**
- Visually complete and functional in isolation — verified live (`npm start` compiles clean, serves 200 on `localhost:3000`).
- The bezier interpolation for plane position/heading (`bezierPoint`/`bezierTangent`/`ctrlPoint`) is a nice touch — planes are correctly positioned and oriented along their route arc based on elapsed time.
- Local `countries-110m.json` with CDN fallback (`FlightMap.jsx:188-196`) is a sensible resilience choice for a map asset.
- `parseDate`'s tolerance for space-vs-`T` separators and short timezone offsets is defensive in a way that suggests real debugging against inconsistent date formats — worth keeping if the frontend starts consuming real backend timestamps.

**Needs refactoring:**
- **Zero connection to the backend.** No `WebSocket`, no `fetch` to `/api/config` or `/ws/*` anywhere in `frontend/src` (verified by grep — no matches). This is the single biggest gap between the two halves of the app: the backend pipeline is real and running, and the frontend is a disconnected art piece showing different, purely client-generated flights. The README already names this as the #1 recommended next step.
- `frontend/src/data/flights.js` (hardcoded flight/airport/chain data) is imported nowhere in the codebase — dead code.
- `App.test.js` is the unmodified Create React App boilerplate test (`getByText(/learn react/i)`) — `App.js` hasn't rendered that text since `FlightMap` was added. The test asserts on content that no longer exists.
- All 352 lines of `FlightMap.jsx` are one component: data generation, projection math, D3 imperative rendering, and the info-panel UI all live together with no separation (no hooks/helpers extracted, no sub-components). Fine at this size, but it's already at the point where splitting rendering from data-generation would pay off, especially once real WebSocket data replaces `generateMockFlights`.
- Inline style objects everywhere (no CSS modules/styled-components/Tailwind) — consistent, but will get unwieldy if more views are added.

**Missing for production:** the actual backend integration (WebSocket client, loading/error/reconnect states), a working test suite, environment-based API URL config (currently nothing references a backend URL at all), and accessibility (the SVG map has no keyboard navigation or ARIA labeling for the plane markers).

### 8. Tests

**Current state, verified live:**
- `frontend/src/App.test.js` — **fails**: not because of assertion mismatch (never gets that far) but because Jest's default CRA transform can't parse d3 v7's ESM-only build (`SyntaxError: Unexpected token 'export'` in `node_modules/d3/src/index.js`). `npm test -- --watchAll=false` exits with 1 failed suite, 0 tests run.
- `test_api.py` (root) — not an automated test despite the name; it's a manual script gated behind `ENABLE_FLIGHTAWARE_API=true` that hits the real paid API and writes to Postgres. Correctly refuses to run (`raise SystemExit`) when the safe defaults are in place. Not something CI could run.
- **No backend automated tests exist at all** — no coverage for `GraphEngine` (delay propagation, gate conflict resolution), `FlightEvent` validation, the mock/live client parsing logic, or the WebSocket handler.

**Missing for production:** a real backend test suite (pytest, easily targetable given the pure-Python `GraphEngine` and `_raw_flight_to_event` functions), a Jest config that handles d3's ESM output (`transformIgnorePatterns` override, or swap to `react-scripts`' documented workaround), and a rewritten `App.test.js` that asserts on what the app actually renders.

### 9. Configuration management

**Implementation:** `python-dotenv` loads `.env` in `server.py`; every value read via `os.getenv(key, default)` scattered across `server.py`, `ingestion/worker.py`, `db/writer.py` call sites, and `test_api.py`. `.env.example` documents all keys with the safe mock-only defaults.

**Works well:**
- The cost-protection design (two separate env vars both required, defaults closed) is correctly implemented and consistently checked in both `server.py` and `test_api.py`.
- `.env.example` is accurate and matches what the code actually reads — verified key-by-key.

**Needs refactoring:**
- No centralized settings object — config reads are inlined with `os.getenv(...)` at each use site, several with **inconsistent defaults** for the same logical value (the `DB_USER` mismatch noted in §1 is the clearest example). A single `pydantic.BaseSettings`/dataclass loaded once would remove this class of bug entirely, and pydantic is already a dependency.
- No validation at startup — a malformed `POLL_INTERVAL_SECONDS` (non-numeric) would crash with a raw `ValueError` deep in `ingestion/worker.py` rather than a clear config error at boot.

**Missing for production:** secrets management beyond a local `.env` file (fine for now, not fine once this is deployed anywhere shared), and environment-specific config (dev/staging/prod) rather than one flat file.

---

## What to preserve

- The cost-protection gate for the live FlightAware API (`ENABLE_FLIGHTAWARE_API` + key, both required, default-closed) — correctly implemented in both `server.py` and `test_api.py`. Don't loosen this.
- The `FlightEvent`/`AirportSnapshot` Pydantic models (`models/events.py`) — clean, typed, with sensible computed properties (`flight_key`, `is_delayed`) and a negative-delay validator. Good foundation to build on.
- `GraphEngine`'s core algorithmic shape: `aircraft_turn`/`gate_reuse` edge typing, BFS delay propagation with decay-and-merge, gate-conflict detection — the concept is sound even though the implementation needs the refactors listed above.
- The mock/live client interface symmetry (`MockFlightAwareClient` and `FlightAwareClient` both expose `get_airport_flights(airport_icao) -> AirportSnapshot`) — this is exactly the right shape for swapping data sources without touching the worker or publisher.
- `active_flights`'s upsert-with-staleness-guard pattern (`WHERE active_flights.last_updated < EXCLUDED.last_updated`) once the write path is actually wired in.
- The FlightMap.jsx visual/interaction design (bezier routes, click-to-inspect, status coloring) — worth keeping once it's wired to real data rather than rebuilding it.

## What needs refactoring (ranked, most impactful first)

1. **Wire the ingestion worker to `db/writer.write_events`** — right now nothing persists during normal operation; `ensure_schema`/`write_events` are dead code outside the manual test script.
2. **Connect the frontend to the backend WebSocket** — the two halves of the app currently don't communicate at all; this is the single largest gap.
3. **Fix the uvicorn WebSocket dependency** — pin `uvicorn[standard]` (or add `websockets`/`wsproto` explicitly) so `/ws/*` doesn't silently 404 on a fresh install following the README exactly.
4. **Centralize config into one settings object** — eliminate the `DB_USER` default mismatch and similar drift by reading env vars in exactly one place.
5. **Pass real `air_time`/`distance` into `DelayPredictor.predict`** instead of hardcoded zeros — otherwise the model's live predictions are silently degraded.
6. **Add graph pruning/expiry** in `GraphEngine` so memory (and the O(n) edge scan) doesn't grow unbounded for the life of the process.
7. **Fix the frontend test setup** (Jest can't parse d3's ESM build) and rewrite `App.test.js` to test what's actually rendered.
8. **Remove dead code**: `main.py` (empty), `frontend/src/data/flights.js` (unimported).
9. **Make `ml/train.py` portable** — no hardcoded absolute paths, pin `scikit-learn` version to match what `model.pkl` was trained with.
10. **Add a shutdown handler** in `server.py` to close the asyncpg pool and Redis connection cleanly.

## What's missing for production

- Backend automated test suite (pytest) — currently zero coverage of `GraphEngine`, event parsing, or the WebSocket handler.
- Database migrations (Alembic or equivalent) — schema is a single `CREATE TABLE IF NOT EXISTS` block, no versioning.
- Data lifecycle management — no TTL/expiry for landed or cancelled flights in `active_flights`, no cleanup job.
- Observability — no structured logging (currently `print()`), no metrics, no health-check endpoint.
- Resilience — no retry/backoff on Redis or Postgres connection loss, no supervision of the ingestion worker task if it crashes.
- Authentication/authorization — `/ws/*` and `/api/*` are fully open.
- Multi-airport / multi-tenant support — `TARGET_AIRPORT` is a single global env var; the graph, Redis channel naming, and frontend are all built around exactly one airport at a time.
- Deployment tooling — no Dockerfile, no docker-compose for Postgres+Redis+backend+frontend, no CI config.
- Rate limiting and usage monitoring for the (currently disabled) live FlightAware API path, ahead of ever re-enabling it.
- Frontend loading/error/reconnect states for a real WebSocket connection (irrelevant today since there is no connection, but required once §"What needs refactoring" item 2 is done).

---

## Verification

Everything above marked "verified" or "verified live" was actually run, not inferred from reading code:

- Created `.venv`, installed `fastapi uvicorn redis asyncpg python-dotenv httpx pydantic networkx numpy scikit-learn` — clean install, versions: fastapi 0.141.1, pydantic 2.13.4, networkx 3.6.1, scikit-learn 1.9.0.
- Confirmed local Redis (`redis-cli ping` → `PONG`) and PostgreSQL already running; created a local `postgres` superuser role to match `.env.example`'s `DB_USER=postgres` default (the `flight_tracker` database already existed).
- Started `uvicorn flight_tracker.server:app` — first attempt: `/ws/*` returned 404, uvicorn logged `No supported WebSocket library detected`. Installed `websockets`, restarted — WebSocket then worked.
- Connected a raw Python `websockets` client to `ws://127.0.0.1:8000/ws/KJFK`: received the initial graph dump (10 flights, revealed to be **stale rows from a prior live-API test run**, dated 2026-05-19 to 2026-06-15), then received freshly generated mock flights (`*-mock-*` IDs) streaming in via Redis pub/sub with real delay values, confirming the full ingestion → Redis → WebSocket pipeline works end-to-end on mock data.
- Confirmed `ml/model.pkl` loads (with a scikit-learn version-skew warning, non-fatal: trained under 1.7.2, loaded under 1.9.0).
- `cd frontend && npm test -- --watchAll=false` → **fails**, Jest can't transform d3's ESM build.
- `cd frontend && npm start` → compiles successfully, serves `200` on `localhost:3000`.
- Stopped both servers and confirmed ports 8000/3000 were released cleanly after testing.
