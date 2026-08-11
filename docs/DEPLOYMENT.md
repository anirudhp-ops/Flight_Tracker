# Deployment Guide

## Local development: docker-compose

This is the tested, working path — the exact steps below were run against this repo's actual `docker-compose.yml`, not described from the file alone.

```bash
# One-time: train a model. ml/model.pkl is gitignored (~955MB) and mounted
# into the backend container at runtime, not baked into the image.
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
python ml/train.py ml/fixtures/ci_sample_ontime.csv   # or your own T_ONTIME_MARKETING.csv for a real-data model

docker compose up -d
```

This starts seven containers: `postgres`, `redis`, `kafka`, `backend`, `frontend`, `prometheus`, `grafana`. Health-gated startup order (`depends_on: condition: service_healthy`) means `backend` won't start until Postgres/Redis/Kafka report healthy.

| Service | Host port | Notes |
|---|---|---|
| `frontend` | 3000 | nginx serving the production React build |
| `backend` | 8000 | FastAPI/uvicorn |
| `postgres` | 5433 (→ container's 5432) | non-default host port, deliberately — see below |
| `redis` | 6380 (→ container's 6379) | same reason |
| `kafka` | 9092 | `PLAINTEXT_HOST` listener only — see below |
| `prometheus` | 9090 | |
| `grafana` | 3001 | `admin`/`admin` on first login |

**Why Postgres/Redis are on non-default host ports (5433/6380)**: so they don't collide with a native `brew`-installed Postgres/Redis you might also be running against this same project (see [Running locally, host processes](#local-development-host-processes-no-docker) below) — inside the compose network, `backend` always talks to them via their container-internal ports (5432/6379) and service names, never the remapped host ports.

**Why Kafka has two listeners**: a host-run backend (`uvicorn` outside Docker) and the containerized `backend` service need different addresses to reach the same broker — a container can't dial `localhost:9092` and reach anything but itself. `PLAINTEXT_HOST` (`localhost:9092`, published to the host) serves the former; `PLAINTEXT` (`kafka:29092`, container-network-only, not published) serves the latter. See `docker-compose.yml`'s own header comment for the full explanation.

Verify it's actually working:

```bash
curl http://localhost:8000/health
./scripts/smoke-tests.sh
```

Useful compose commands:

```bash
docker compose logs -f backend       # follow backend logs
docker compose ps                    # container status/health
docker compose restart backend       # after an env var or code change
docker compose down                  # stop everything (add -v to also drop volumes)
docker compose build backend         # rebuild after a requirements.txt or source change
```

## Local development: host processes (no Docker)

For active backend/frontend development with hot reload — `uvicorn --reload` and `npm start` don't exist inside the Docker images.

```bash
# Infra only, via compose (or native brew installs of postgres/redis/kafka):
docker compose up -d postgres redis kafka

cp .env.example .env
createdb flight_tracker     # first time only

source .venv/bin/activate
uvicorn flight_tracker.server:app --reload

cd frontend && npm install && npm start
```

Open http://localhost:3000. If your Postgres/Redis are the compose containers rather than native installs, point `.env` at their **host-mapped** ports (5433/6380) instead of the defaults — this is the one case where the remapped ports above matter.

## Environment variables

All read once, at import time, by `flight_tracker/config.py` (`pydantic-settings`, from `.env` and the process environment; an invalid/missing required value raises immediately, naming the field). Defaults shown are the code's actual defaults.

| Variable | Default | Notes |
|---|---|---|
| `ENABLE_FLIGHTAWARE_API` | `false` | Must be `true` **and** a real `FLIGHTAWARE_API_KEY` set for live (paid) API calls — both required, by design. See README's "Cost protection" note. |
| `FLIGHTAWARE_API_KEY` | (none) | Only needed if the above is `true`. |
| `TARGET_AIRPORT` | `KJFK` | The one airport this process tracks. |
| `POLL_INTERVAL_SECONDS` | `60` | Ingestion worker's poll cadence. |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | |
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD` | `localhost` / `5432` / `flight_tracker` / `postgres` / (none) | |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | `20` / `50` | One shared pool for `server.py` + all workers — see `flight_tracker/db/OPTIMIZATION.md`. |
| `DB_POOL_MAX_QUERIES` | `50000` | |
| `DB_POOL_MAX_CACHED_STATEMENT_LIFETIME` | `300` | |
| `CACHE_FLIGHT_TTL_SECONDS` / `CACHE_AIRPORT_TTL_SECONDS` / `CACHE_DELAYS_TTL_SECONDS` | `300` / `600` / `120` | Redis cache-aside TTLs — see `flight_tracker/db/DATABASE_DESIGN.md` for why these specific values. |
| `DB_CLEANUP_INTERVAL_SECONDS` / `DB_CLEANUP_MAX_AGE_HOURS` | `3600` / `24` | How often, and how old, before a landed flight's `active_flights` row is deleted. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Use `kafka:29092` if the backend itself runs inside `docker-compose.yml`'s network (see above). |
| `KAFKA_TOPIC_FLIGHT_EVENTS` / `KAFKA_TOPIC_PROCESSED_FLIGHTS` / `KAFKA_TOPIC_DELAY_PREDICTIONS` / `KAFKA_TOPIC_DEAD_LETTER` | `flight-events` / `processed-flights` / `delay-predictions` / `dead-letter-events` | |
| `KAFKA_CONSUMER_LAG_WARNING_THRESHOLD` | `100` | |
| `KAFKA_DLQ_WARNING_THRESHOLD` | `10` | Failures/hour before `GET /health/dlq`'s `warning` flips true. |
| `KAFKA_METRICS_LOG_INTERVAL_SECONDS` | `10` | |
| `WORKER_COUNT` | `4` | Concurrent persistence workers — capped in practice by `flight-events`'s 3 partitions; see `flight_tracker/workers/CONCURRENCY.md`. |
| `WORKER_RETRY_MAX_ATTEMPTS` / `WORKER_RETRY_INITIAL_DELAY_MS` / `WORKER_RETRY_MAX_DELAY_MS` / `WORKER_RETRY_BACKOFF_FACTOR` / `WORKER_RETRY_JITTER_FRACTION` | `3` / `100` / `30000` / `2.0` / `0.1` | Exponential backoff with jitter for DB writes and Kafka publishes. |
| `WORKER_BACKPRESSURE_LAG_THRESHOLD` / `WORKER_BACKPRESSURE_LOW_LAG_THRESHOLD` / `WORKER_BACKPRESSURE_THROTTLE_SECONDS` | `1000` / `10` / `0.05` | |
| `WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS` | `3600` | Redis fast-path idempotency check TTL. |
| `WORKER_SUPERVISOR_RESTART_DELAY_SECONDS` / `WORKER_SHUTDOWN_TIMEOUT_SECONDS` | `5.0` / `30.0` | |
| `GATE_POOL_TERMINALS` | `["A","B","C","T"]` | |
| `GATE_POOL_GATES_PER_TERMINAL` | `14` | 56 gates total with the defaults. |
| `GATE_POOL_OVERRIDES` | `{}` | JSON dict, e.g. `{"KJFK":["A1","A2","B1"]}`, for an airport whose real layout doesn't match the generated pool. |

See `.env.example` for a working starting file (safe defaults, mock ingestion, no API key needed).

## Database schema & migrations

**There is no migration tool** (no Alembic, no versioned migration files). Schema is defined as one idempotent SQL block in `flight_tracker/db/writer.py` (`CREATE_TABLES_SQL`, `CREATE TABLE IF NOT EXISTS`) plus a second idempotent block (`SCHEMA_MIGRATIONS_SQL` — `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, index changes, one-time data cleanup) applied unconditionally, in order, on **every** backend startup via `ensure_schema()`. This is adequate at this project's current scale and rate of schema change (a handful of columns/indexes added across several phases) but would need a real migration tool (Alembic, or similar) before schema changes needed to be reviewed, ordered, or rolled back independently of app code — that's a real limitation to know about, not a hidden one.

## Scaling considerations

The single biggest thing to know before scaling this app: **`DelayPropagationWorker` is deliberately single-instance** — it owns the in-memory `GraphEngine`, which isn't safely shardable across processes today (see [docs/ARCHITECTURE.md](ARCHITECTURE.md#graphengine-flight_trackergraphenginepy)). This is the measured throughput ceiling (~25 events/sec sustained, not the 100/sec originally targeted — see [docs/PERFORMANCE.md](PERFORMANCE.md)), and no amount of scaling the rest of the stack changes it without a redesign (centralize the graph behind its own service, or partition it consistently by flight).

What **does** scale today, safely, with no code change:

- **The persistence worker pool**: raise `WORKER_COUNT`, up to `flight-events`'s partition count (3 by default — a 4th worker sits idle). Raising the ceiling further means repartitioning the topic (`kafka-topics --alter --partitions N`, one-directional) and bumping `WORKER_COUNT` to match.
- **Postgres connection pool**: `DB_POOL_MAX_SIZE`, currently sized (50) to stay well under a local Postgres's default `max_connections=100` — check that ceiling before raising it in a managed/production Postgres.

Full scaling discussion, including what a multi-process `DelayPropagationWorker` would actually require: [`flight_tracker/workers/CONCURRENCY.md`](../flight_tracker/workers/CONCURRENCY.md)'s "How to scale".

## Monitoring

Prometheus (`http://localhost:9090`) scrapes the backend's `GET /metrics` every 15s; Grafana (`http://localhost:3001`, `admin`/`admin`) has a 10-panel dashboard auto-provisioned from `flight_tracker/observability/grafana_dashboard.json` — throughput, latency percentiles, error rate, cache hit rate, consumer lag, graph size, and more. Structured JSON logs carry a `request_id` that traces one unit of work across the whole pipeline. Full reference: [`flight_tracker/OBSERVABILITY.md`](../flight_tracker/OBSERVABILITY.md).

`alert_rules.yml` defines Prometheus alerting rules (e.g. `HighConsumerLag`, `HighEventProcessingLatency`) — loaded by the `prometheus` compose service but Prometheus's Alertmanager itself isn't part of this stack, so alerts fire in Prometheus's own UI (`/alerts`) rather than paging anyone yet.

## Troubleshooting

See [docs/TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues (WebSocket won't connect, flights not appearing, high latency, slow queries) and how to diagnose each.

## AWS (not yet deployed)

**Honest current state**: this project is not deployed anywhere. There's no Terraform, no CloudFormation, no live AWS account configured for it, and no `deploy.yml` GitHub Actions workflow — `.github/workflows/` currently has `test.yml` (CI) and `security.yml` (dependency scanning) only. What exists is a `Dockerfile` and `frontend/Dockerfile` that build real, working images (verified: built and run via `docker compose up` locally) — deployable, not deployed.

What actually deploying to AWS would require, as real open decisions rather than a prescribed path:

1. **Compute target** — the two realistic options for this app's shape (one backend container running background Kafka consumer tasks alongside its HTTP server, plus a static frontend): **ECS on Fargate** (more control, more setup — task definitions, an ALB, a VPC) or **App Runner** (less setup, less control over networking — worth checking whether it supports the long-lived background tasks this backend runs in-process). Neither is built.
2. **Managed Kafka, Postgres, Redis** — MSK (or a self-managed broker on EC2), RDS Postgres, ElastiCache Redis are the natural fits for what `docker-compose.yml` runs locally. None provisioned.
3. **Where `ml/model.pkl` lives at runtime** — it's ~955MB, gitignored, and deliberately not baked into the Docker image (see the `Dockerfile`'s own comment). In production it needs to come from somewhere the container can reach at startup: an S3 download in the entrypoint, or an EFS mount, are the two realistic options. Neither built.
4. **Secrets** — `FLIGHTAWARE_API_KEY`, `DB_PASSWORD`, etc. would move from `.env`/compose `environment:` blocks to AWS Secrets Manager or SSM Parameter Store. Not built.
5. **A real `deploy.yml`** — once 1–4 above are actual decisions, not open questions, a GitHub Actions deploy workflow (build → push to ECR → update the ECS service or App Runner deployment → run `scripts/smoke-tests.sh` against the real URL) is the natural next step, following the same pattern `test.yml` already establishes for CI.

This section will be replaced with real, tested deployment instructions once 1–4 above are actually decided and built — not before.
