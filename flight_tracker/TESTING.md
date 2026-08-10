# Testing

This project has four layers of tests, each answering a different
question. Run them in this order — each one assumes the layer above it
already passes.

1. **Unit tests** (pytest + Jest) — is each component correct in isolation?
2. **Integration tests** — does the real, deployed pipeline work end to end?
3. **Load tests** (k6 + Kafka) — does it hold up under realistic concurrent load?
4. **Benchmarks** — how does each hot path's latency scale with data size?

See `flight_tracker/LOAD_TEST_REPORT.md` for the actual numbers from the
last full run of all four layers.

## Prerequisites

All layers below run against the same real, local infrastructure this
project's `README.md` already has you set up — no mocks, consistent
throughout this codebase (see e.g. `flight_tracker/tests/test_workers.py`'s
own docstring):

- Local Postgres reachable via `flight_tracker/config.py`'s settings
- Local Redis
- A local Kafka broker with the topics from `scripts/create_kafka_topics.sh`
  already created (native via `brew services start kafka`, or
  `docker compose up -d kafka` — either publishes on `localhost:9092`)

```bash
brew services start kafka   # or: docker compose up -d kafka
scripts/create_kafka_topics.sh
```

**Before trusting any load/integration test number, make sure no *other*
instance of this app is already running.** A second `uvicorn
flight_tracker.server:app` process joins the same Kafka consumer groups
and silently splits partitions with the one your test started — this
actually happened during Phase I development and cost 30/50 "missing"
cascade propagations before it was traced back to a stray process (see
`LOAD_TEST_REPORT.md`, Section 2). Check with:

```bash
ps aux | grep uvicorn
```

## 1. Unit tests

### Backend (pytest)

```bash
pip install -r requirements.txt
pip install pytest-cov   # not in requirements.txt: dev-only, coverage reporting
pytest flight_tracker/tests/ -v
pytest flight_tracker/tests/ --cov --cov-config=.coveragerc --cov-report=term-missing
```

What's covered, by file:

| File | Covers |
|---|---|
| `test_events.py` | `FlightEvent`/`FlightEventEnvelope` validation, computed properties, status/event-type mapping, JSON round-trip |
| `test_graph.py` | `GraphEngine`: aircraft-turn/gate-reuse edges, delay decay+BFS, gate-conflict resolution, expired-flight pruning |
| `test_predictor.py` | `DelayPredictor`: real trained model, known vs. unseen labels, ICAO/IATA normalization, confidence |
| `test_cache.py` | `CacheLayer`: cache-aside get_or_set, TTL expiry, invalidation, hit-rate tracking |
| `test_retry.py` | `@retry_with_backoff`: success-after-failures, exhaustion, exponential backoff, jitter |
| `test_idempotency.py` | DB-level UNIQUE constraint dedup, and `AsyncEventProcessor`'s Redis-cache-based dedup |
| `test_workers.py` | `FailureHandler`: dead-letter publishing with full metadata |
| `test_worker_pool.py` | `WorkerPool`/`Supervisor`: lifecycle, metrics aggregation, real end-to-end message processing, crash-restart |
| `test_delay_propagation_worker.py`, `test_delay_propagation_processor.py` | `DelayPropagationProcessor`/`Worker`: gate reassignment, delay prediction+propagation, lifecycle |
| `test_db_reader.py` | `db/reader.py` query functions against real Postgres |
| `test_dlq_utils.py` | `fetch_dlq_events` against real Kafka |
| `test_ingestion_client.py` | AeroAPI response parsing (pure functions) and `MockFlightAwareClient` |
| `test_websocket_messages.py` | `classify_prediction_event` and WS message construction (pure) |

**Not covered by pytest, by design:** `server.py` (FastAPI wiring,
startup/shutdown, the WebSocket handler) and
`ingestion/worker.py` (the poll-forever loop). Both are thin
orchestration over already-tested components and only meaningfully
exercised by actually running the app — covered by the integration tests
below instead, not by unit-testing a mocked FastAPI app.

**Target: 80%+ coverage.** Last measured: 74% overall, 91% excluding the
two files above (see `LOAD_TEST_REPORT.md`, Section 1).

### Frontend (Jest + React Testing Library)

```bash
cd frontend
npm test -- --watchAll=false --coverage
```

| File | Covers |
|---|---|
| `App.test.js` | Full app: config fetch, WebSocket connect, snapshot loading, live updates, disconnect/reconnect UI |
| `useFlightData.test.js` | The WebSocket hook: SNAPSHOT/DELAY_PREDICTION/PROPAGATION_EVENT/GATE_REASSIGNMENT handling, subscribe/unsubscribe, connection lifecycle |
| `FlightDetail.test.js` | Delay color coding, prediction confidence, propagation chain display, gate reassignment badge |
| `FlightMap.test.js` | SVG rendering, plane markers (including filtering unresolvable airports), click/keyboard selection, cascade overlay |
| `GateMap.test.js` | Gate occupancy display, reassignment highlighting |

**Target: 70%+ coverage.** Last measured: 82.69% statements (see
`LOAD_TEST_REPORT.md`, Section 1).

## 2. Integration tests

```bash
python scripts/integration_tests.py
```

Starts a throwaway `uvicorn` instance, publishes real `FlightEvent`s to
Kafka, and watches Postgres/`delay-predictions` for the result — see the
script's own module docstring for exactly what it tests and why it
adapts the brief's "docker-compose + stop one worker" framing to this
project's actual single-process architecture. Takes about 15–20 seconds;
writes `scripts/integration_test_results.json`.

## 3. Load tests

### WebSocket (k6)

Requires a running server (`uvicorn flight_tracker.server:app` — this
script does not start one itself):

```bash
brew install k6   # if not already installed
k6 run scripts/load_test_k6.js
k6 run --summary-export=scripts/k6_summary.json scripts/load_test_k6.js   # for the JSON numbers
WS_URL=ws://127.0.0.1:8123 AIRPORT=KJFK k6 run scripts/load_test_k6.js    # non-default target
```

Runs Scenario 1 (Baseline: 10 VUs, 60s) then Scenario 4 (Spike: 0→100→0
VUs over 120s) back to back — about 3.5 minutes total. Reads the
threshold pass/fail from k6's own output (`ws_connect_success` rate
>99% in both scenarios); interpret the printed `ws_connect_latency_ms`
and `ws_first_message_latency_ms` percentiles the same way you would any
other p50/p95/p99 latency metric — a growing p95/p99 relative to p50
during the spike stage is the signal to watch for, not just the pass/fail
threshold.

### Kafka throughput & cascade (Python)

k6 has no Kafka producer and this app has no HTTP endpoint that publishes
to `flight-events`, so these two scenarios (also against an
already-running server) use `aiokafka` directly instead:

```bash
python scripts/load_test_kafka.py throughput --rate 100 --duration 60
python scripts/load_test_kafka.py cascade --count 50 --fanout 2
python scripts/load_test_kafka.py all
```

`throughput` reports publish latency (Kafka producer round-trip) AND
end-to-end latency (publish → the corresponding `delay-predictions`
message, sampled every 10th event) separately — a widening gap between
the two under load means the bottleneck is downstream of Kafka (the
persistence pool or `DelayPropagationWorker`), not Kafka itself. See
`LOAD_TEST_REPORT.md` Section 4 for why 100 events/sec sustained doesn't
currently hold up end-to-end even though publish latency stays low.

`cascade` builds `--count` independent trigger flights (each with its own
`--fanout` aircraft-turn-linked neighbors), confirms every setup flight
is graph-ready (its own prediction observed on `delay-predictions`)
*before* firing any delayed update — required for the aircraft-turn
edges to end up pointing the right direction; see the script's own
comments on `add_edges_for_flight`'s existing-node → new-node edge
direction. Then fires all triggers simultaneously and times how long
every cascade takes to fully propagate.

## 4. Benchmarks

```bash
python scripts/benchmark.py all     # or: graph | db | cache | ml
```

No running server required — talks to `GraphEngine`, Postgres, Redis,
and the trained model directly. Cleans up its own Postgres/Redis rows on
exit. Takes 1–2 minutes total (the cache benchmark's 10,000-object
populate loop dominates). Writes `scripts/benchmark_results.json`.

| Benchmark | What it isolates |
|---|---|
| GraphEngine | Marginal cost of one more event, and one BFS delay propagation, as graph size grows (500 → 10,000 nodes) — the direct explanation for the Kafka throughput ceiling found in Section 4 of the load test report |
| Database | `active_flights` query latency vs. row count, with `EXPLAIN ANALYZE` confirming index usage at every size |
| Cache | Redis vs. Postgres latency for the same 1,000 lookups, hit rate, % speedup |
| ML prediction | Per-prediction latency against the real trained model (target: p95 < 10ms) |

## Why these targets

- **80%/70% coverage**: high enough to catch regressions in the actual
  business logic (delay decay math, idempotency, cache-aside semantics)
  without chasing 100% on framework glue code that would just be
  re-testing FastAPI/React themselves.
- **Event → prediction in <5s**: the frontend polls nothing — it's
  purely WebSocket-pushed, so this is the actual user-perceived "how
  stale can the map be" latency.
- **Cascade in <5s**: a real airport delay can legitimately ripple
  through dozens of connected flights (shared aircraft, shared gates);
  this bounds how long the map takes to reflect the full ripple, not
  just the first hop.
- **<1% error rate under spike**: distinguishes "the system degrades
  gracefully under a traffic spike" from "the system falls over," which
  matters more for a live-map product than raw peak throughput.
