# Observability

Phase J: Prometheus metrics, a Grafana dashboard, structured JSON logging,
and request-id tracing, so a production issue can be debugged from
dashboards and logs instead of by re-reading code. See
`flight_tracker/TESTING.md` for the four testing layers this complements —
that answers "is it correct and fast enough," this answers "can I see what
it's doing right now, in production."

## Running the stack

```bash
docker compose up -d prometheus grafana   # Postgres/Redis/Kafka as usual too, if not already up
uvicorn flight_tracker.server:app         # the backend itself — runs on the host, not in docker-compose
```

- Prometheus: <http://localhost:9090> (scrapes the backend's `GET /metrics` every 15s — see `prometheus.yml`)
- Grafana: <http://localhost:3001> (`admin`/`admin` on first login; the dashboard below is auto-provisioned, no manual import)
- Backend health: <http://localhost:8000/health>

Verify the whole stack actually works end to end (real load, real
Prometheus scrape, real log parsing — no mocks) with:

```bash
python scripts/test_observability.py            # 60s load, ~9 checks
python scripts/test_observability.py --duration 15 --rate 15   # faster smoke run
```

## Metrics reference

All defined once in `flight_tracker/metrics/__init__.py`, exposed at
`GET /metrics` in Prometheus text format via `prometheus_client`.

### Counters (monotonic — read with `rate()`/`increase()` in PromQL)

| Metric | Labels | Meaning |
|---|---|---|
| `events_received_total` | `topic` | A worker or `DelayPropagationWorker` pulled a message off Kafka. Incremented in `worker_pool.py`'s and `delay_propagation_worker.py`'s own consume loops, before parsing — counts every delivery, including ones that later fail to parse. |
| `events_processed_total` | `worker_id` | A `flight-events` message finished the persistence pipeline (idempotency check → DB write → forward) successfully. Excludes idempotent skips (see `AsyncEventProcessor.process_flight_event`). |
| `events_failed_total` | `reason` (exception class name) | Either pipeline raised. Every failure here also gets dead-lettered to `dead-letter-events` — see `GET /health/dlq`. |
| `predictions_generated_total` | — | One `PredictionEvent` published to `delay-predictions`. `DelayPropagationProcessor._publish()` is the single call site — one increment per triggering flight, per propagated neighbor, and per gate reassignment. |
| `propagations_triggered_total` | — | A delayed flight (`delay_minutes > 0`) triggered a BFS cascade via `GraphEngine.propagate_delay`. Counts attempts, not flights actually affected — an attempt with zero downstream neighbors still counts once. |
| `cache_hits_total` / `cache_misses_total` | — | `CacheLayer.get_cached()` outcome, for all three cache-aside endpoints (`/api/flights/{id}`, `/api/airports/{code}/snapshot`, `/api/flights/{id}/delays`). |

### Histograms (use `histogram_quantile()` for percentiles)

| Metric | Labels | Buckets | Meaning |
|---|---|---|---|
| `event_processing_latency_seconds` | — | 10/50/100/500ms, 1s | Wall time of one `AsyncEventProcessor.process_flight_event` call OR one `DelayPropagationProcessor.process` call — both pipelines report into the same histogram (they're both "how long did this event take"; `database_query_latency_seconds` and `propagation_latency_seconds` below narrow down *where* time went once this one looks slow). |
| `propagation_latency_seconds` | — | default | Just the `GraphEngine.propagate_delay()` BFS call, isolated from prediction/publish overhead around it. |
| `database_query_latency_seconds` | `query_type` (`get_flight_status`, `get_airport_snapshot`, `get_recent_delays`, `write_events`) | default | Wall time per named query, in `db/reader.py` and `db/writer.py`. |
| `websocket_message_latency_seconds` | — | default | Time from a message entering the per-connection `outbox` queue (`/ws/{airport_code}`'s `stream_events`/`send_heartbeats`) to `websocket.send_text()` actually being called on it — queue backlog, not network time. |

### Gauges (current value)

| Metric | Labels | Meaning |
|---|---|---|
| `active_websocket_connections` | — | Incremented on `websocket.accept()`, decremented in the handler's `finally` — always accurate even on an unhandled exception. |
| `graph_node_count` / `graph_edge_count` | — | `GraphEngine.graph`'s size, updated on every `process_event()` call inside `DelayPropagationProcessor.process`. Ever-growing without a plateau usually means `prune_expired_flights()` isn't running or isn't keeping up — see `LOAD_TEST_REPORT.md` Section 5 for what unbounded growth does to per-event latency. |
| `kafka_consumer_lag` | `consumer_group` (`event-processor-pool`, `delay-predictor`) | Set every `settings.kafka_metrics_log_interval_seconds` (default 10s) by `Supervisor.run_periodic_metrics_logging()`, from each pool's own `.total_lag()` — the same number the pool-wide "Workers: N, events/sec: ..." log line reports. |
| `database_connection_pool_size` | — | `pool.get_size()` (asyncpg), sampled on every `write_events()` call. Approaching `settings.db_pool_max_size` (default 50) under sustained load means writes are queuing on `pool.acquire()`. |

## Grafana dashboard

`flight_tracker/observability/grafana_dashboard.json`, auto-provisioned via
`grafana/provisioning/`. Ten panels, roughly in "is it working" → "why not"
order:

1. **Throughput** — `events_received_total` rate by topic. Your top-line "is anything moving" check.
2. **Event processing latency (p50/p95/p99)** — from `event_processing_latency_seconds`. See "Performance baseline" below for what's normal.
3. **Error rate** — `events_failed_total` rate by reason. Should sit at ~0; a nonzero, sustained value means check `GET /health/dlq` and the `reason` label for the exception class.
4. **Active WebSocket connections** — sanity-check against actual connected browser tabs if something looks wrong on the frontend.
5. **Cache hit rate** — `cache_hits_total / (hits + misses)`. A sudden drop usually means Redis was restarted/flushed (TTLs are 2–10 minutes — see `config.py`'s `cache_*_ttl_seconds` — so it should recover within a few minutes on its own).
6. **Kafka consumer lag** — per consumer group, with a threshold line at 100 (matches `alert_rules.yml`'s `HighConsumerLag` and `settings.kafka_consumer_lag_warning_threshold`).
7. **Graph size** — nodes/edges over time. Should plateau, not grow forever (see gauge table above).
8. **Propagation latency (p95)** — isolates cascade cost from the rest of the pipeline.
9. **Database connection pool size** — headroom against `settings.db_pool_max_size`.
10. **Predictions/sec** — throughput of the ML/propagation side specifically, as distinct from panel 1's raw Kafka intake.

## Logging

`flight_tracker/logging_config.py`'s `JSONFormatter` is attached to the
**root** logger once, at `server.py` import time (`configure_logging()`) —
every module's `logging.getLogger(__name__)` (including third-party ones
like `aiokafka`) inherits it by propagation. Every line is one JSON object:

```json
{"timestamp": "...", "level": "INFO", "logger": "flight_tracker.workers.event_processor",
 "message": "Event processed", "request_id": "6f1e...", "flight_id": "UA100-mock-3",
 "worker_id": "worker-2", "latency_ms": 12.4}
```

`request_id`/`flight_id`/`worker_id` are always present (`null` if not
applicable to that log line); anything else passed via `extra={...}` (e.g.
`latency_ms`, `lag`, `propagated_count`) shows up as its own field too.

**What `request_id` actually is** depends on where the line comes from —
deliberately not a single generated-at-the-edge id, because this system has
two different kinds of "one unit of work":

- **HTTP requests**: a fresh UUID per request, generated by
  `flight_tracker/middleware/request_id.py`, also returned as the
  `X-Request-ID` response header.
- **The Kafka pipeline** (ingestion → worker pool → `DelayPropagationWorker`
  → WebSocket stream): reuses `FlightEventEnvelope.event_id`, which already
  flows unchanged from `flight-events` through `processed-flights` (the
  worker pool forwards the *same* envelope object — see
  `event_processor.py`) — so the same `request_id` naturally appears in an
  `"Event processed"` line and, later, a `"Delay propagation processed"`
  line for that same event. `scripts/test_observability.py`'s
  `check_request_id_tracing()` verifies this holds under real load.

### Levels — what goes where

| Level | Use for | Examples in this codebase |
|---|---|---|
| DEBUG | High-volume, only useful when actively debugging | Cache hit/miss (`redis_cache.py`) |
| INFO | Normal operation, one line per unit of work | "Event processed", "Delay propagation processed", "Idempotent skip", WebSocket connect/disconnect |
| WARNING | Degraded but self-recovering | Consumer lag above threshold (backpressure kicking in) |
| ERROR | A single unit of work failed | "Event processing failed" / "Delay propagation processing failed" (both include `exc_info`) |
| CRITICAL | A whole background task died and isn't coming back on its own | `_crash_logger` in `server.py` — note the `Supervisor` *does* auto-restart individual workers (that's a WARNING-level event, logged by `Supervisor._supervised_loop`); this is for the outer supervising task itself dying |

## Health check

`GET /health` — a single aggregated liveness/metrics snapshot, distinct
from the more detailed `GET /health/db` and `GET /health/dlq` (kept as-is;
this doesn't replace them):

```json
{
  "status": "healthy",
  "timestamp": "2026-08-09T23:20:20Z",
  "services": {"database": "ok", "redis": "ok", "kafka": "ok"},
  "metrics": {"events_per_second": 6.45, "avg_latency_ms": 47.38, "error_rate": 0.0, "consumer_lag": 0}
}
```

`events_per_second` here is a **cumulative average since process start**,
not a trailing window — for a real rate, use Prometheus
(`rate(events_processed_total[1m])`). This endpoint is for "is it alive and
roughly how busy," suitable for a load balancer or uptime check; Grafana is
the right tool for an actual trend.

## Troubleshooting

- **A panel shows "No data"**: confirm the backend is actually running and
  `curl http://localhost:8000/metrics` returns text. If it does but
  Grafana still shows nothing, check Prometheus's own target page
  (<http://localhost:9090/targets>) — `flight-backend` should be `UP`. On
  Linux (not Docker Desktop), `host.docker.internal` needs the
  `extra_hosts: host-gateway` entry already in `docker-compose.yml`'s
  `prometheus` service; confirm with `docker compose exec prometheus getent hosts host.docker.internal`.
- **`kafka_consumer_lag` climbing and not recovering**: check `events/sec`
  vs. `error_rate` on the same dashboard — a high error rate means events
  are being dead-lettered (not stuck), which shows as lag *not* actually
  growing unboundedly; if error rate is ~0 and lag still climbs, see
  `LOAD_TEST_REPORT.md` Section 4 for the known single-instance
  `DelayPropagationWorker` throughput ceiling (~25 events/sec sustained).
- **`graph_node_count` growing without bound**: `GraphEngine.prune_expired_flights()`
  isn't being called on a schedule anywhere in `server.py`'s current
  startup — this is a known gap (`README.md`'s "Recommended next steps"),
  not something Phase J added; the metric exists specifically so it's
  visible instead of silent.
- **Logs aren't valid JSON**: `configure_logging()` must run before
  anything logs — it's called at `flight_tracker/server.py` import time,
  before `app = FastAPI()`. A log line from a module that logs at *import*
  time, before `server.py` is imported, would miss it; none currently do.
- **A specific event never shows up downstream**: grep the logs for its
  `request_id` (== the envelope's `event_id`, printed in every DLQ payload
  too — see `failure_handler.py`) across both `"Event processed"` and
  `"Delay propagation processed"` lines to find which stage it stopped at.

## Performance baseline

From `flight_tracker/LOAD_TEST_REPORT.md` (Phase I, real load against this
same stack — re-run the commands there, not restated here, if these
numbers go stale):

| Path | p50 | p95 | p99 |
|---|---|---|---|
| Kafka publish | 0.9–1.3ms | 7.0–10.3ms | 32.4–45.0ms |
| End-to-end (publish → prediction), 25 events/sec sustained | 10ms | 81ms | 179ms |
| End-to-end, 100 events/sec sustained | 279ms | 10,016ms | 11,308ms *(degrades — see below)* |
| ML prediction (`DelayPredictor.predict`) | 16.7ms | 18.4ms | 28.2ms |
| DB write (`write_events`) | — | — | 25–138ms (integration test range) |

**This system reliably sustains ~25 events/sec end-to-end; it does not
sustainably keep up with 100 events/sec** — the bottleneck is the
single-instance `DelayPropagationWorker` (`GraphEngine` is in-process,
non-shardable state), not Kafka or Postgres. `alert_rules.yml`'s
`HighEventProcessingLatency` (p95 > 1s for 5m) is set well above the
healthy-load p95 (~81ms) specifically so it fires on this exact known
degradation mode, not on ordinary jitter.
