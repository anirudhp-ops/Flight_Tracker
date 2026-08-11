# Changelog

This project was built in phases, not semantic-versioned releases — each entry below corresponds to a real, distinct commit (see `git log`), listed newest first. Where a phase has its own detailed write-up, it's linked.

## Phase L — Containerization & CI/CD automation

- Added `Dockerfile` (backend) and `frontend/Dockerfile` (nginx-served production React build); wired both into `docker-compose.yml` alongside the existing Postgres/Redis/Kafka/Prometheus/Grafana services.
- Split Kafka into two listeners (`PLAINTEXT_HOST` for host-run processes, `PLAINTEXT` for the containerized backend) — a single-listener broker only ever advertised `localhost:9092`, unreachable from inside its own container network.
- Fixed a real latent bug found by actually building and running the containerized stack: `server.py` never called `ensure_schema()` on startup (only the manual `test_api.py` script did) — a genuinely fresh database hit `UndefinedTableError` immediately. Now called idempotently on every startup.
- Fixed `frontend/package-lock.json` drift under Node 20 (the version both `test.yml` CI and the new Docker image pin) — `npm ci` was failing.
- Added `.github/workflows/security.yml` (daily + push-triggered dependency scan) and `scripts/smoke-tests.sh` (health, `/api/flights/{id}`, and a real WebSocket handshake — used for post-deploy verification once a real deploy target exists).

## Phase J — Production observability

Prometheus metrics, an auto-provisioned 10-panel Grafana dashboard, structured JSON logging with `request_id` tracing across the whole pipeline (HTTP requests get a fresh UUID; the Kafka pipeline reuses the event's own `event_id` end to end), and `GET /health` as a single aggregated liveness/metrics snapshot. See [`flight_tracker/OBSERVABILITY.md`](flight_tracker/OBSERVABILITY.md).

## Phase I — Automated test suite, load/benchmark scripts, CI

154 backend unit tests (pytest), 28 frontend tests (Jest/RTL), a real integration test suite against live Kafka/Postgres, k6-based WebSocket load testing, Kafka throughput/cascade load testing, and component-level benchmarks (graph, DB, cache, ML). Found and reported two genuine performance limitations rather than hiding them (see [docs/PERFORMANCE.md](docs/PERFORMANCE.md)). Added `.github/workflows/test.yml` (CI on every PR/push). See [`flight_tracker/LOAD_TEST_REPORT.md`](flight_tracker/LOAD_TEST_REPORT.md).

## Phase G — Real-time WebSocket system & frontend integration

Replaced bare `FlightEvent` JSON over the WebSocket with a typed `WSMessage` envelope (`SNAPSHOT`/`FLIGHT_UPDATE`/`DELAY_PREDICTION`/`PROPAGATION_EVENT`/`GATE_REASSIGNMENT`/`HEARTBEAT`), so ML predictions, confidence, propagation chains, and gate reassignments (already computed server-side since Phase F) actually reach the browser. See [`frontend/PHASE_G_REPORT.md`](frontend/PHASE_G_REPORT.md) and [`frontend/REAL_TIME_ARCHITECTURE.md`](frontend/REAL_TIME_ARCHITECTURE.md).

## Phase F — Delay propagation integration

Wired `GraphEngine`'s BFS delay propagation and gate-conflict resolution into the live Kafka pipeline via a new single-instance `DelayPropagationWorker`, replacing Phase D's two separate single-purpose consumers. Added a configurable per-airport gate pool. See [`flight_tracker/workers/PHASE_F_REPORT.md`](flight_tracker/workers/PHASE_F_REPORT.md) and [`flight_tracker/graph/STATE_MANAGEMENT.md`](flight_tracker/graph/STATE_MANAGEMENT.md).

## Phase E — Concurrent worker pool, retry, idempotency, supervision

Replaced Phase D's two single-consumer pipelines with `WORKER_COUNT` concurrent workers running the full per-event pipeline, an `asyncio.Lock`-guarded `GraphEngine` (later removed from this pipeline entirely in Phase F), jittered exponential backoff for transient failures, a real DB-level idempotency guarantee (`UNIQUE(flight_id, captured_at)`, superseding a deliberate Phase D non-guarantee — see [`flight_tracker/events/IDEMPOTENCY.md`](flight_tracker/events/IDEMPOTENCY.md)), and per-worker crash-restart supervision. See [`flight_tracker/workers/PHASE_E_REPORT.md`](flight_tracker/workers/PHASE_E_REPORT.md) and [`flight_tracker/workers/CONCURRENCY.md`](flight_tracker/workers/CONCURRENCY.md).

## Phase D — Event-driven architecture with Kafka

Replaced Redis pub/sub (Phase B/C) with Kafka for the event pipeline — durable, replayable, at-least-once delivery with a dead-letter queue, versus pub/sub's "lost if nobody's listening right now." See [`flight_tracker/events/KAFKA_ARCHITECTURE.md`](flight_tracker/events/KAFKA_ARCHITECTURE.md).

## Phase C — PostgreSQL + Redis improvements

The upsert-with-staleness-guard write pattern for `active_flights`, connection pooling, and the cache-aside Redis layer in front of the read-heavy GET endpoints. See [`flight_tracker/db/DATABASE_DESIGN.md`](flight_tracker/db/DATABASE_DESIGN.md).

## Earlier (pre-phase-naming)

The initial FastAPI backend, PostgreSQL persistence, Redis pub/sub, the D3 map frontend, the first BFS delay-propagation and gate-reassignment implementations, and the first ML delay predictor were all built before this project adopted formal lettered phases — see `git log --oneline --reverse` for the raw commit history from `a1e3591` (initial commit) onward.

## Known limitations

- **Sustained throughput ceiling: ~25 events/sec, not the 100/sec originally targeted.** The single-instance `DelayPropagationWorker` (deliberately not horizontally scaled — it owns in-memory, non-shardable graph state) is the bottleneck. Root cause, numbers, and remediation options: [docs/PERFORMANCE.md](docs/PERFORMANCE.md).
- **ML prediction latency (p95 18.4ms) misses its <10ms target.** Not yet investigated further.
- **`flight_events` has no retention/archival policy** — grows unbounded (visible on every cleanup pass, not hidden).
- **No authentication or rate limiting** — this is a local/prototype system, not hardened for untrusted traffic.
- **No live deployment** — Dockerfiles exist and are tested; nothing is actually running in AWS or any other cloud. See [docs/DEPLOYMENT.md § AWS](docs/DEPLOYMENT.md#aws-not-yet-deployed).
- **No database migration tool** — schema changes are applied via an idempotent SQL block on every startup, adequate at current scale but not a substitute for real migration tooling at a larger scale.

## Roadmap (not committed to, listed as real open options)

- Prune non-landed stale flights on a schedule (the highest-leverage fix for the throughput ceiling — see [docs/PERFORMANCE.md](docs/PERFORMANCE.md)).
- Shard `DelayPropagationWorker` by airport if multi-airport tracking is ever added.
- A real AWS deployment: compute target (ECS/Fargate vs. App Runner), managed Kafka/Postgres/Redis, a strategy for serving `ml/model.pkl` (~955MB) to a running container, secrets management, and a `deploy.yml` workflow — see [docs/DEPLOYMENT.md § AWS](docs/DEPLOYMENT.md#aws-not-yet-deployed) for the specific open decisions.
- A retention/archival policy for `flight_events`.
- A real migration tool if/when schema changes need independent review or rollback.
