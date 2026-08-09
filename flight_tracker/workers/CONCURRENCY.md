# Concurrent worker pool architecture

## Why this exists, and what it replaces

Phase D had exactly one consumer for each pipeline stage: one
`flight-processor` reading `flight-events`, one `delay-predictor` reading
`processed-flights`. Phase E replaces both with `WORKER_COUNT` (default 4)
concurrent workers that each run the *entire* per-event pipeline
themselves — validate, propagate delay through the graph, predict, write
to Postgres, forward downstream — reading `flight-events` directly.

`flight_tracker/ingestion/consumer_runner.py` and
`flight_tracker/events/delay_prediction_consumer.py` (Phase D) are **kept
as files but not started** by `server.py` anymore — running them
alongside the new pool would mean three separate things consuming (and,
worse, double-processing) the same topics. They're left in place as a
worked reference for "the simple single-consumer version," which is
genuinely a different, sometimes more appropriate, design — see "How to
scale" below for when you'd actually want that instead of this.

## Architecture

```
                    ┌───────────────────────────────────────┐
                    │ Topic: flight-events (3 partitions)     │
                    └───────┬──────────┬──────────┬──────────┘
                            │          │          │
                    partition 0  partition 1  partition 2
                            │          │          │
              ┌─────────────┴──┐ ┌─────┴──────┐ ┌─┴──────────┐      ┌────────────┐
              │ worker-0         │ │ worker-1    │ │ worker-2    │      │ worker-3    │
              │ (Worker +        │ │             │ │             │      │  IDLE —     │
              │  AsyncEventProc.)│ │             │ │             │      │  no 4th     │
              └─────────────┬────┘ └─────┬───────┘ └─────┬───────┘      │  partition  │
                            │            │               │              │  to own     │
                            │  each: idempotency check    │              └────────────┘
                            │  -> graph_lock: process_event,
                            │     propagate_delay, resolve_gate_conflicts
                            │  -> retrying DB write (active_flights +
                            │     flight_events, one transaction)
                            │  -> publish: processed-flights,
                            │     delay-predictions
                            ▼
              ┌─────────────────────────────────────────┐
              │ Postgres: active_flights, flight_events    │  (one shared
              │ Redis: processed:{flight_id}:{timestamp}   │   asyncpg pool,
              └─────────────────────────────────────────┘   min=20 max=50)

  Any exception (parse failure OR processing failure)
              │
              ▼
  ┌─────────────────────────┐
  │ Topic: dead-letter-events  │  (1 partition — see events/KAFKA_ARCHITECTURE.md)
  └─────────────────────────┘

  Supervisor wraps every Worker.run() in a crash -> log -> wait 5s -> restart loop.
  Periodic log every 10s: "Workers: N, events/sec: X, lag: Y, errors: Z, restarts: W"
```

`GraphEngine` (shared, in-memory), the ML predictor (shared, stateless),
and the asyncpg pool (shared) live once, at `server.py`'s module level, and
get passed into every worker's `AsyncEventProcessor`. Each worker's
`AsyncEventProcessor` instance is otherwise independent — its own metrics
counters, its own idempotency-check calls — see "Concurrency model" for
what that split actually protects.

## Concurrency model

**Not OS threads, not multiprocessing — `asyncio` tasks, cooperatively
scheduled on one thread.** `WorkerPool` starts `WORKER_COUNT` `Worker.run()`
coroutines as separate `asyncio.Task`s; the Python interpreter runs exactly
one of them at a time, switching between them at `await` points (a Kafka
fetch, a Postgres query, a `asyncio.sleep`). This matters for reasoning
about what's actually safe to share:

- **Shared, safe as-is**: the asyncpg pool (designed for concurrent
  `acquire()` from multiple coroutines — that's its whole purpose),
  `KafkaEventProducer` instances (each `publish()` call is a self-contained
  awaited operation), the Redis client, the ML predictor (stateless
  `.predict()` calls).
- **Shared, NOT safe without an explicit guard**: `GraphEngine`. Its
  methods (`process_event`, `propagate_delay`, `resolve_gate_conflicts`)
  each do multiple non-atomic mutations to the same `networkx.DiGraph` —
  `add_edges_for_flight` iterates every existing node while another
  worker's `resolve_gate_conflicts` could be mutating edges. This is
  exactly the "dictionary changed size during iteration" failure mode
  already found and fixed once for the *single-consumer* case (Phase C) —
  with N concurrently-interleaving workers, an equivalent unguarded race
  is far more likely, not less. `build_worker_pool()`
  (`worker_pool.py`) constructs one `asyncio.Lock` shared by every
  worker's `AsyncEventProcessor`, and `process_flight_event()` holds it for
  the entire graph-mutation block (`process_event` →
  `propagate_delay` → `resolve_gate_conflicts`) — DB writes and Kafka
  publishes happen *outside* the lock, so the actually-slow, await-heavy
  work still runs concurrently across workers. Only the graph itself is
  serialized.

**Per-worker vs. shared state, deliberately**: `AsyncEventProcessor` is one
instance *per worker* (own `events_processed`, `events_failed`,
`retries_attempted`, `idempotent_skips`, `latencies_ms`) specifically so
`scripts/load_test_workers.py` and the periodic metrics line can report
real per-worker and pool-wide numbers without extra bookkeeping — but it
holds references to the *shared* pool/graph/lock/predictor/producers passed
into `build_worker_pool()`, not private copies of them.

## Idempotency

Two layers, verified independently (see `flight_tracker/tests/test_workers.py`
and the integration test run whose numbers are in `PERFORMANCE.md`):

1. **Redis cache-aside, checked first** (`AsyncEventProcessor.process_flight_event`):
   key `processed:{flight_id}:{timestamp}`, TTL 1 hour
   (`WORKER_IDEMPOTENCY_CACHE_TTL_SECONDS`). A cache hit skips the entire
   graph/ML/DB pipeline for that event — pure optimization, avoiding
   redundant work when the *exact* same event is redelivered (a producer
   retry after a network blip, at-least-once semantics doing what they do).
2. **Postgres `UNIQUE(flight_id, captured_at)` index on `flight_events`,
   `ON CONFLICT DO NOTHING`** (`db/writer.py`) — the actual correctness
   guarantee. If the Redis cache is stale, was just evicted, or Redis
   itself restarted and lost its data, this is what still prevents a
   duplicate row. This is a genuine change from Phase D's
   `IDEMPOTENCY.md`, which documented `flight_events` as deliberately
   *non*-deduplicated — see that file's updated version for why Phase E
   reverses that call.

Verified together, live: publishing the identical event twice through a
real 2-worker pool produced exactly one `flight_events` row; a 990-event
integration test with 10 intentionally malformed messages processed every
valid event exactly once (990 distinct `flight_key`s from 990 valid
inputs) while dead-lettering all 10 malformed ones.

## Retry strategy

`flight_tracker/workers/retry.py`'s `@retry_with_backoff`:

```
delay = min(max_delay_ms, initial_delay_ms * backoff_factor ** (attempt - 1))
      * (1 + uniform(-jitter_fraction, +jitter_fraction))
```

Defaults (all overridable per call-site, normally read from
`settings.worker_retry_*`): 3 total attempts, 100ms initial delay, 30s cap,
2.0x growth per attempt, ±10% jitter. Applied to `AsyncEventProcessor`'s DB
write (`_write_to_db_once`, one retry-wrapped instance built per-worker in
`__init__` so `on_retry` bumps that worker's own `retries_attempted`
counter) and to `KafkaEventProducer.publish()` (Phase D's original
bespoke retry loop was replaced with this same decorator during this
phase, rather than running two independently-tuned retry mechanisms for
the same kind of failure — see `events/kafka_producer.py`).

The jitter specifically exists because of the concurrency this phase adds:
without it, N workers hitting the same transient failure (a Postgres blip,
a broker hiccup) at close to the same instant would all retry in lockstep
— a synchronized burst hitting the just-recovering dependency at once.

## Failure handling

Any exception during a message's handling — including one that happens
*before* `AsyncEventProcessor.process_flight_event()` is even reachable,
like `FlightEventEnvelope.from_json()` failing on truly malformed
bytes — is caught in `Worker.run()` and routed to
`FailureHandler.handle_failure()` (`failure_handler.py`), which logs
loudly and publishes the original payload, error type/message/traceback,
timestamp, and `worker_id` to `dead-letter-events`. The offset is
**committed either way**, success or dead-lettered failure — the
alternative (not committing a failed message) would make the same poison
message get re-fetched and re-fail forever on every restart, since nothing
ever advances past it.

This last point was not just theoretical — it's exactly the bug caught
while building the integration test: `envelope = FlightEventEnvelope.from_json(...)`
originally lived *outside* the try/except that wrapped
`process_flight_event()`, so a genuinely malformed message would escape
`Worker.run()` entirely, trigger the Supervisor's crash-restart path
instead of the DLQ path, and — because its offset was never committed —
crash the restarted worker again on the exact same message, forever.
Fixed by moving parsing inside the same try/except as processing; verified
with a real non-JSON message published directly to the topic.

## Backpressure

Each `Worker.run()` iteration checks its own current lag
(`Worker.lag()` — highwater mark minus current position, summed across its
assigned partitions) before processing the next message. Above
`WORKER_BACKPRESSURE_LAG_THRESHOLD` (default 1000), it logs a warning and
sleeps `WORKER_BACKPRESSURE_THROTTLE_SECONDS` (default 50ms) before
continuing — a real, deliberately small throttle, not a full stop; the
point is to make sustained backlog visible and slightly self-limiting, not
to make it worse by starving the very consumer trying to drain it.

**On "batch size"**: the phase brief describes backpressure in terms of
growing/shrinking a batch size. This consumer loop processes one message
at a time (`async for message in self._consumer`), not in fetched batches,
so there's no batch size to tune — the low-lag "speed up" case is already
the default, unthrottled per-message path; there's nothing further to
speed up into. Verified live: a real historical backlog (thousands of
messages from earlier testing) reliably produced the lag warning
repeatedly while draining, then stopped once lag returned under threshold.

## Worker supervision

`Supervisor` (`supervisor.py`) wraps each `Worker.run()` coroutine in
`_supervised_loop()`: catches any exception other than `CancelledError`,
logs it with a per-worker failure count, stops the crashed worker cleanly,
waits `WORKER_SUPERVISOR_RESTART_DELAY_SECONDS` (default 5s), and restarts
*only* that worker — its siblings are untouched. `run_forever()` runs all
`N` supervised loops via `asyncio.gather(..., return_exceptions=True)`, as
specified; the actual crash/restart decision-making lives inside each
loop, not in how `gather` is called, since `gather` alone has no primitive
for "react to one task finishing while the others keep running."

Verified with a deterministic test (a fake worker whose `run()` raises
once, like a broken Kafka connection would, then behaves normally): the
Supervisor detected the crash, logged it, wa­ited the configured delay,
and restarted exactly the crashed worker — its sibling's restart count
stayed at zero throughout.

## Known limitations

- **The 3-partition ceiling is real and was hit in every load test.**
  `flight-events` has 3 partitions; a 4th worker in the default
  `WORKER_COUNT=4` pool is never assigned any partition and processes zero
  events — confirmed directly (`Worker worker-3 stopped (processed=0, ...)`)
  in both the integration test and the load test. See `PERFORMANCE.md` for
  what this means for the 1000+ events/sec target.
- **`GraphEngine` concurrency is protected, not eliminated as a
  bottleneck.** The lock means graph mutation across all active workers is
  fully serialized — under enough load, that block (not the DB write, not
  the Kafka publishes) would become the throughput ceiling instead. Not
  observed at this phase's tested load, but real and worth knowing before
  assuming N workers scales graph-heavy throughput linearly.
- **Worker startup cost scales with `WORKER_COUNT`.** Each worker joining
  a consumer group triggers a full rebalance across every *already-joined*
  member — starting 4 workers sequentially measured ~20 seconds of
  cumulative rebalance overhead before the pool was fully assigned and
  running (observed directly in server startup logs). This is Kafka's
  normal rebalance behavior, not a bug, but it means `WORKER_COUNT` isn't
  free to bump arbitrarily high without startup time growing with it.
- **Idempotency cache has no negative-result caching and no
  invalidation-on-write**, consistent with the cache-aside design in Phase
  C/D — a value is wrong for at most its TTL, by design, not forever.
- **DLQ has no automatic replay.** `scripts/inspect_dlq.py` (Phase D) and
  `GET /health/dlq` make failures visible; nothing currently re-publishes
  a dead-lettered event back onto `flight-events` for another attempt —
  that's a deliberate human-in-the-loop step today, not automated.

## How to scale

**More workers, same partition count**: raise `WORKER_COUNT` past the
partition count and the extra workers simply idle — not useful on its own.

**More partitions** (the lever that actually raises the throughput
ceiling): `kafka-topics --alter --topic flight-events --partitions N`
(count only ever increases, never decreases) and raise `WORKER_COUNT` to
match. More partitions means smaller, more numerous per-partition ordering
scopes — fine for this app (ordering only matters per-`flight_id`, and the
default partitioner's key-hashing is unaffected by partition count for
ordering-within-a-key purposes) but is exactly the kind of infrastructure
change that deserves its own deliberate decision rather than a
reflexive bump — see `PERFORMANCE.md`'s discussion of the 1000+ events/sec
target for why this wasn't done automatically here.

**Beyond one process**: nothing here assumes `server.py`'s workers run in
this one OS process specifically — `Worker`/`Supervisor`/`WorkerPool` don't
reference anything process-local except the shared `GraphEngine`/lock. A
separate worker process (or container) *could* join the same
`event-processor-pool` consumer group and Kafka would rebalance partitions
across processes exactly as it does across in-process tasks today — but it
would need its own `GraphEngine` (or a way to share one across processes,
which doesn't exist yet), making that the real blocker to true
multi-process horizontal scaling, not the Kafka/worker-pool code itself.
