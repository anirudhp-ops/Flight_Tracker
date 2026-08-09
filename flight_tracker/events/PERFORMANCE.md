# Kafka pipeline performance

Produced by `scripts/measure_kafka_performance.py` against the real local
broker, Postgres, and Redis, with the full server
(`uvicorn flight_tracker.server:app`) already running and its own
ingestion worker active in the background (so these numbers include real
background load, not an idle system). Every number below is from an actual
run; re-run the script to reproduce or refresh them.

## 1. Event-to-dashboard latency

**Target: < 500ms end-to-end. Result: comfortably met — max observed 165ms,
under half the target even at the slowest sample.**

Measured wall-clock time from "event created" (a timestamp taken the
instant before `producer.publish()`) to "received on `/ws/KJFK`" — the same
endpoint the frontend uses — spanning the full real pipeline: `flight-events`
→ `consumer_runner` (Postgres write) → `processed-flights` →
`delay_prediction_consumer` (graph propagation + ML prediction) →
`delay-predictions` → this WebSocket connection.

| | Kafka pipeline (30 samples) |
|---|---|
| min | 30.7 ms |
| median | 116.6 ms |
| mean | 104.6 ms |
| max | 169.6 ms |
| under 500ms target | 30/30 |

**Isolated Redis pub/sub baseline** (publish → subscriber receives, same
process, same machine): **0.1–0.2 ms median**. This is not a re-measurement
of the old Phase B/C app — that code path no longer exists after this
phase's rewrite, so reverting it just to benchmark would have meant testing
different code than what's actually deployed. It's an honest baseline for
"what a single in-memory pub/sub hop costs" against which the
~100ms Kafka pipeline can be read correctly: the difference is not "Kafka
is slow," it's "four extra durable hops, three separate consumer processes,
a Postgres write, and an ML inference all sit in between," each adding a
small amount, in exchange for the durability guarantees in
`KAFKA_ARCHITECTURE.md` that pub/sub never had. Redis pub/sub was never
going to lose to itself; the honest comparison is 0.15ms with zero
durability vs. ~105ms with at-least-once delivery, replay, and ordering —
both true, neither the whole story alone.

**A real measurement pitfall worth recording**: the first attempt at this
test showed 12/30 (then 0/30) events "never arriving." Root cause: a
newly-opened `/ws/{airport_code}` connection sends its entire initial graph
dump (768 messages, sequentially, one `send_text()` per flight) *before*
starting that connection's own Kafka consumer — and that consumer's
`auto_offset_reset="latest"` means events published before its group
finishes rebalancing are, correctly, invisible to it (see
`KAFKA_ARCHITECTURE.md`, "Offset management"). The benchmark's original
0.5s post-dump quiet-period wasn't enough margin; 3s quiet-period + a 2s
settle buffer fixed it (both now in the script). This is a real
characteristic of new connections, not just a test artifact — a browser
tab that immediately triggers backend activity in its first couple of
seconds after connecting could plausibly miss the very earliest events, the
same way it always could have with Redis pub/sub's own "must already be
subscribed" requirement.

## 2. Throughput

**Producer**: publishing 1,000 events *concurrently*
(`asyncio.gather`, `acks="all"`, idempotent) took **0.081s — 12,383.8
events/sec**. (A naive sequential-await loop measured ~100–130 events/sec
instead — not a pipeline limit, just the cost of awaiting each
fire-and-confirm round-trip one at a time before starting the next. Worth
noting because it's an easy, misleading number to report by accident.)

**Consumer (`flight-processor`)**: draining that same 1,000-event burst —
validate, write to Postgres, forward to `processed-flights`, commit, one
message at a time — took **3.773s: 265.1 events/sec** actually processed
end-to-end. This is the pipeline's real, currently-single-instance
bottleneck, and it's a legitimate one: each message does a full Postgres
round-trip, not a memory operation.

## 3. Consumer lag under load

Sampled the **real** `flight-processor` consumer group (via
`kafka-consumer-groups --describe`, from outside the group — sampling
doesn't join it and steal partitions) immediately after the 1,000-event
concurrent burst landed, faster than the consumer could drain it:

| | Lag (messages) |
|---|---|
| Immediately after burst (producer already done, consumer still catching up) | **739** |
| After full drain | **0** |

Lag rose because 1,000 messages arrived in 81ms while the consumer can only
process ~265/sec — expected backpressure, not a bug — and fully recovered
to 0 within the 3.773s drain, with zero message loss (verified separately
and more rigorously in the crash-simulation test documented in
`KAFKA_ARCHITECTURE.md`'s "Failure scenarios").

## What this does and doesn't tell you

- Confirmed: end-to-end latency target (< 500ms) is met with wide margin
  under real background load, not just in isolation.
- Confirmed: the single-instance `flight-processor`/`delay-predictor`
  consumers can absorb a 1,000-message burst and fully recover, with lag as
  the visible, monitored symptom rather than silent data loss or an
  unbounded queue.
- **Not** a capacity planning number: 265 events/sec is this specific
  unoptimized single consumer instance doing one `write_events()` call (one
  Postgres round-trip) per message. `KAFKA_ARCHITECTURE.md`'s "Scaling to
  multiple consumers" section covers the real lever (more
  `flight-processor` instances, up to the 3-partition ceiling) if this
  needs to go higher — not benchmarked here because there's no current
  traffic anywhere close to needing it (the actual ingestion worker
  publishes ~20 events/minute).
