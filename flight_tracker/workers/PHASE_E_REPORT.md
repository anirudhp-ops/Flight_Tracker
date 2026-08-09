# Phase E final report: concurrency and async processing

Summary of what was built, decided, found, and verified for Phase E. For
the technical reference docs this report points to, see
`CONCURRENCY.md` (architecture, concurrency model, scaling) and
`PERFORMANCE.md` (full load-test numbers). This file is the narrative
wrap-up; those are the reference material.

## Architectural decisions made upfront

Phase E's brief specified a worker-pool design that overlapped
significantly with what Phase D had already built. Four judgment calls
were made and documented before writing code, all of which held up under
testing:

1. **Worker pool replaces Phase D's two consumers**, not adds a third.
   `event_processor.py` merges validate → graph propagation → prediction →
   DB write → forward into one per-event pipeline. Phase D's
   `ingestion/consumer_runner.py` and `events/delay_prediction_consumer.py`
   are kept as files (a legitimate, simpler reference design) but are not
   started by `server.py` — running all three against the same topics
   would mean double-processing.
2. **`GraphEngine` mutation is guarded by one shared `asyncio.Lock`**
   across all workers — this was flagged as a real, unaddressed risk in
   Phase D's own `KAFKA_ARCHITECTURE.md` before concurrent consumers
   existed to actually trigger it. DB writes and Kafka publishes happen
   outside the lock so only the genuinely shared, non-atomic graph
   mutation is serialized.
3. **One shared asyncpg pool, not two.** The requested `min=20/max=50`
   pool size, doubled across a separate server.py pool and a separate
   worker-pool pool, would sit at exactly Postgres's
   `max_connections=100` with zero headroom. The worker pool reuses
   `server.py`'s existing pool instead.
4. **`flight_events` gained a real `UNIQUE(flight_id, captured_at)`
   index**, reversing a decision documented in Phase D's `IDEMPOTENCY.md`
   (that table was deliberately append-only/non-deduplicated). Required a
   one-time migration deleting 214 pre-existing duplicate-row groups
   before the unique index could even be created. `IDEMPOTENCY.md` was
   updated to explain the reversal honestly, not silently rewritten.

## Two real bugs found and fixed via testing

- **Malformed-message crash loop.** `FlightEventEnvelope.from_json()`
  originally ran outside the try/except wrapping
  `process_flight_event()` in `Worker.run()`. A genuinely malformed
  message would escape `run()` entirely, trigger the Supervisor's
  crash-restart path instead of dead-lettering, and — since its offset
  was never committed — crash the restarted worker on the exact same
  message again, forever. Caught by publishing a real non-JSON message to
  a live worker; fixed by moving parsing inside the same try/except as
  processing.
- **Error-count undercounting.** `WorkerPool.total_events_failed`
  originally summed only `processor.events_failed`, which never
  increments for envelope-parse failures (they don't reach
  `process_flight_event()`). Now sums each worker's
  `FailureHandler.errors_processed`, which does count them. Found via the
  same malformed-message test.

## Results against the phase's 6 stated targets

| Target | Result | Met? |
|---|---|---|
| 1000+ events/sec (4 workers, 3 partitions) | **405.4 events/sec** | ❌ Not met — root cause: `flight-events` has 3 partitions, so a 4th worker in the default `WORKER_COUNT=4` pool is never assigned one and processes zero events (observed directly: `Worker worker-3 stopped (processed=0, ...)` in both the integration test and the load test) |
| p99 latency < 200ms | **27.9ms** | ✅ 7x under target |
| < 1% error rate | **0.00%** | ✅ |
| Zero data loss under failure | Verified via a deterministic crash-before-commit simulation and a real `SIGKILL` recovery test | ✅ |
| Graceful restart on worker crash, < 5s | Mechanism verified directly (crash → log → wait → restart, exactly the crashed worker) | ✅ |
| No duplicate processing | 990/990 valid events in the integration test produced 990 distinct rows | ✅ |

Baseline (1 worker) also beat the phase's own 100 events/sec expectation:
**130.9 events/sec** measured. 4-worker throughput (405.4 events/sec) is a
**3.10x speedup**, consistent with 3 workers being concurrently active,
not 4 — near-linear scaling relative to what was actually running, not a
sign of some other bottleneck.

The 1000+ events/sec target is reported as not met, honestly, with its
root cause explained rather than adjusted to look like a pass. The actual
lever to reach it — more `flight-events` partitions
(`kafka-topics --alter --partitions N`) — is documented in
`CONCURRENCY.md`'s "How to scale" section as a deliberate future
infrastructure decision, not something to bump reflexively.

## What was verified live (not asserted)

- **Partition rebalancing**: confirmed via real `ConsumerRebalanceListener`
  logs as workers joined the group one at a time.
- **Idempotency**: same event published twice → exactly one
  `flight_events` row (DB constraint); Redis-cache-skip path separately
  verified under real historical-backlog replay (140 skips observed in
  one run).
- **Backpressure**: real `WARNING` logs fired under genuine lag exceeding
  threshold while draining an actual historical backlog — not simulated.
- **Supervisor**: a deterministic test (fake worker crashes once, like a
  broken Kafka connection would) confirmed detection, logging, the
  configured wait, and a restart of exactly the crashed worker — its
  sibling's restart count stayed at zero throughout.
- **Concurrent writes**: 4 workers writing 200 unique flights
  simultaneously produced exactly 200 rows, 200 distinct `flight_key`s,
  zero corruption, zero failures.
- **Integration test**: 1,000 events (990 valid + 10 intentionally
  malformed) through 4 workers — 990/990 processed exactly once, 10/10
  correctly dead-lettered, zero crashes, p99 latency 25.2ms.
- **Load test**: `scripts/load_test_workers.py`, full numbers in
  `PERFORMANCE.md`.
- **Graceful shutdown**: `SIGINT` with all 4 workers active mid-poll
  produced a clean stop with accurate final per-worker counts and full
  process exit, inside the 30s timeout required by the phase spec.
- **Full regression**: backend health endpoints (`/health/db`,
  `/health/dlq`) correct, WebSocket pipeline streaming correctly, and the
  frontend rendering live flights end-to-end through the new pipeline in
  a real browser check with zero console errors.

## Known limitations (see CONCURRENCY.md for full detail)

- 3-partition ceiling on `flight-events` caps effective concurrency below
  `WORKER_COUNT` unless partition count is raised to match.
- `GraphEngine`'s lock protects correctness but fully serializes graph
  mutation across all workers — not observed as a bottleneck at tested
  load, but a real ceiling if graph-heavy throughput needs grow.
- Worker startup cost scales with `WORKER_COUNT` (~20s to fully start 4
  workers, due to each join triggering a full group rebalance).
- No automatic DLQ replay — `scripts/inspect_dlq.py` and `GET /health/dlq`
  make failures visible; re-publishing a dead-lettered event is a manual,
  human-in-the-loop step today.
