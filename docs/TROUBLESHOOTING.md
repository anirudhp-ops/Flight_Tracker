# Troubleshooting

Common issues, how to actually diagnose them (not just the fix), and where the deeper reference doc lives.

## WebSocket won't connect

1. Is the backend even up? `curl http://localhost:8000/health` — if this fails, the WebSocket was never going to work; fix the backend first (see "Backend won't start" below).
2. If `/health` returns `200`, check the browser console for the actual close code/reason (`useFlightData.js` logs reconnect attempts). A `403`/connection-refused at the HTTP-upgrade step usually means you're hitting the wrong port or origin — the frontend's default WebSocket target is derived from `REACT_APP_BACKEND_URL` (falls back to `http://localhost:8000`, converted to `ws://`); confirm it matches where the backend is actually listening.
3. Confirm CORS isn't the issue: `server.py`'s `CORSMiddleware` currently allows only `http://localhost:3000` — if you're serving the frontend from a different origin/port, the browser will block it silently in some cases.
4. Test the WebSocket directly, bypassing the frontend entirely: `./scripts/smoke-tests.sh` includes a real WebSocket handshake check (asserts a `SNAPSHOT` message arrives). If that passes but the browser still won't connect, the problem is frontend-side, not backend.

## Flights not appearing on the map

1. Check `/health`'s `services.kafka` — if not `"ok"`, the Kafka producer never started; nothing downstream will have data. See "Kafka connection issues" below.
2. Check the backend logs for `"Event processed"` lines (`docker compose logs -f backend` or your terminal if running via `uvicorn` directly) — if these aren't appearing, either the ingestion worker isn't running (look for `"Ingestion client: Mock FlightAware Client started"` near startup) or events are failing before reaching the persistence pipeline.
3. Check `GET /health/dlq` — a nonzero, climbing count means events are reaching the pipeline but failing and getting dead-lettered. Inspect with `python scripts/inspect_dlq.py` to see the actual error type/message per failed event.
4. Confirm you're not running **two instances of the backend at once** (`ps aux | grep uvicorn`, or `docker compose ps` plus a stray host process). A second process joins the same Kafka consumer groups and silently splits partitions with the first — this happened during Phase I development and looked exactly like "some flights just don't show up." See `flight_tracker/TESTING.md`'s warning about this.

## High latency / slow-feeling map updates

1. Open the Grafana dashboard (`http://localhost:3001`, `admin`/`admin`) — panel 2 (event processing latency p50/p95/p99) and panel 6 (Kafka consumer lag) are the two to check first.
2. If `kafka_consumer_lag` for `delay-predictor` is climbing and not recovering, this is very likely the known, documented bottleneck: the single-instance `DelayPropagationWorker` does not sustain 100+ events/sec — see [docs/PERFORMANCE.md](PERFORMANCE.md) for the full root-cause analysis and what to do about it. Panel 7 (graph size) growing without bound is a contributing factor: `prune_expired_flights()` only removes *landed* flights, so a long-running process's per-event cost keeps climbing.
3. If lag is `event-processor-pool` (the persistence pool) specifically, and `WORKER_COUNT` is already at 4 (the default), check `flight-events`'s partition count — a 4th+ worker sits idle beyond 3 partitions. See [`flight_tracker/workers/CONCURRENCY.md`](../flight_tracker/workers/CONCURRENCY.md#how-to-scale).
4. If neither consumer group shows lag but the *frontend* still feels slow, check `websocket_message_latency_seconds` (queue backlog, not network time) and `active_websocket_connections` — an unexpectedly high connection count can mean stale/zombie tabs still consuming.

## Database queries slow

1. Confirm the query is actually the slow part: check `database_query_latency_seconds` (labeled by `query_type`) in Prometheus/Grafana before assuming — the perceived slowness might be upstream (see "High latency" above).
2. Run `EXPLAIN ANALYZE` on the actual query against your local data volume, don't assume an index isn't being used — `flight_tracker/db/OPTIMIZATION.md` and [docs/BENCHMARKS.md](BENCHMARKS.md#database-active_flights-query-by-airport-index-confirmed-used-at-every-size) both confirm the relevant index (`idx_active_flights_airport_code_last_updated`) is used at every tested size; growth in that query's latency is legitimately result-set size (more flights for that airport = more rows returned), not a missing index.
3. Check `database_connection_pool_size` against `DB_POOL_MAX_SIZE` (default 50) — if writes are queuing on `pool.acquire()`, the pool itself is saturated, not any single query.

## Backend won't start

- **`UndefinedTableError: relation "active_flights" does not exist`**: shouldn't happen anymore — `server.py`'s `startup()` calls `ensure_schema()` (idempotent `CREATE TABLE IF NOT EXISTS`) before anything else touches the database. If you see this on a version of the code before that fix, or against a database whose user lacks `CREATE TABLE` permission, that's the thing to check.
- **Kafka connection refused / topics not found**: confirm Kafka is actually reachable at whatever `KAFKA_BOOTSTRAP_SERVERS` points to — `localhost:9092` for a host-run backend, `kafka:29092` for the containerized `backend` service (see [docs/DEPLOYMENT.md](DEPLOYMENT.md#local-development-docker-compose) for why there are two). Topics auto-create by default, but with default (unconfigured) partition counts — run `scripts/create_kafka_topics.sh` (idempotent) to get the partition counts the app actually expects.
- **`ml/model.pkl` not found**: it's gitignored and not part of a fresh clone. `python ml/train.py ml/fixtures/ci_sample_ontime.csv` (small checked-in sample) or `python ml/train.py` (needs your own `T_ONTIME_MARKETING.csv` in the repo root) — see `ml/train.py`'s own docstring.

## Docker / docker-compose issues

- **Port already in use** (`5432`, `6379`, `8000`, `3000`, `9092`): most likely a native (non-Docker) Postgres/Redis/Kafka or a host-run `uvicorn`/`npm start` already bound to that port. `docker-compose.yml` deliberately puts Postgres/Redis on non-default host ports (5433/6380) to avoid this for those two — Kafka (9092), the backend (8000), and the frontend (3000) still need the native equivalent stopped first if you're switching from a host-process workflow to the containerized one. `lsof -iTCP -sTCP:LISTEN -P` shows what's holding a port.
- **`npm ci` fails during `docker compose build`** with a lockfile-sync error: this is a real, reproducible issue if `frontend/package-lock.json` was last regenerated under a different Node major version than the one `frontend/Dockerfile` uses (`node:20-alpine`) — `npm ci` is strict about exact lockfile-vs-package.json agreement, and different Node/npm versions can resolve optional dependencies differently. Regenerate the lockfile under the same Node version the Dockerfile uses: `docker run --rm -v "$PWD/frontend:/app" -w /app node:20-alpine npm install`, then commit the result.
- **Backend container starts but `/health` never responds**: check `docker compose logs backend` for the actual startup exception rather than assuming it's hung — a fast, clean crash-and-exit and a genuine hang look identical from `docker compose ps`'s "Up" status until the container actually dies.

## Deploy failed

There is no live deployment for this project yet (see [docs/DEPLOYMENT.md § AWS](DEPLOYMENT.md#aws-not-yet-deployed)), so "deploy" here means `docker compose up` locally or in CI, not a cloud rollout:

1. `docker compose logs <service>` for the failing container's actual error.
2. `docker compose ps` — confirm which service failed its healthcheck (`Up` vs `Up (unhealthy)` vs exited).
3. Run `./scripts/smoke-tests.sh` manually against whatever's actually running (`./scripts/smoke-tests.sh <frontend-url> <backend-url>`) — it checks `/health`, a real API route, and a real WebSocket handshake, and will name which one failed rather than just "something's wrong."

Once a real AWS deployment exists, this section will cover the actual failure modes for that target (task failing to start, ALB health check failures, etc.) instead — not written speculatively today.
