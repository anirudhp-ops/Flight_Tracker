# Phase F final report: delay propagation integration

Summary of what was built, decided, found, and verified for Phase F. See
`flight_tracker/graph/STATE_MANAGEMENT.md` for the graph-state design
decision and `delay_propagation_worker.py`'s own docstring for the
single-instance architecture rationale — this file is the narrative
wrap-up, those are the reference material.

## What changed

- **GraphEngine gained a real, configurable gate pool** (`config.py`'s
  `GATE_POOL_TERMINALS`/`GATE_POOL_GATES_PER_TERMINAL`/
  `GATE_POOL_OVERRIDES`) and airport-scoping (`GraphEngine(airport_code=...)`),
  replacing the old hardcoded `["A","B","C","T"] x 1-14` pool.
  `resolve_gate_conflicts()`'s decay/merge logic is untouched.
- **`propagate_delay()` now returns `(FlightEvent, hop_count)` pairs**
  instead of bare `FlightEvent`s, so propagated predictions can carry
  `propagation_hops`. The BFS traversal, 0.75-per-hop decay, and
  max()-merge are byte-for-byte the same as before — verified directly by
  building a genuine multi-hop chain (not the same-aircraft mesh
  `add_edges_for_flight` normally produces) and confirming decay
  100→75→56→42 and hops 1/2/3 exactly.
- **`ml/predictor.py` gained real per-prediction confidence**
  (`predict_with_confidence()`), computed from RandomForest tree-ensemble
  agreement (`self.model.estimators_`), not a fabricated static number or
  the model's overall R². `predict()` is unchanged.
- **New `PredictionEvent` model** (`flight_tracker/models/prediction_event.py`),
  published to `delay-predictions` in place of `FlightEventEnvelope`.
  Deliberately still carries a full `flight_event: FlightEvent` (a
  deviation from the task brief's literal field list) so the WebSocket
  handler doesn't need a DB/graph lookup per message to reconstruct what
  the frontend needs.
- **New `DelayPropagationWorker`** (`flight_tracker/workers/delay_propagation_worker.py`),
  the single instance that owns the one `GraphEngine`. Consumes
  `processed-flights`, runs graph mutation + gate-conflict resolution
  unconditionally (matches preserved behavior, not the task brief's
  pseudocode nesting), runs propagation + prediction only when
  `delay_minutes > 0`, and always publishes a `PredictionEvent` for the
  triggering flight (even non-delayed ones, `model_confidence=1.0`
  "trivial case") plus one per propagated/reassigned flight. Duck-types
  `worker_pool.py`'s `Worker` interface closely enough to be wrapped in
  `WorkerPool([...])` + `Supervisor(...)` unmodified, for the same
  crash-restart supervision Phase E's pool gets.
- **`event_processor.py` simplified to pure persistence.** Removed
  `graph_engine`/`graph_lock`/`predictor`/`prediction_producer` — the
  `asyncio.Lock` that existed only to guard concurrent graph mutation is
  gone entirely (not just unused), since nothing in this pipeline touches
  the graph anymore.
- **`server.py` wired up.** `GraphEngine(airport_code=settings.target_airport)`
  + `DelayPredictor` feed a `DelayPropagationWorker`, wrapped in its own
  `WorkerPool`/`Supervisor`, started/stopped alongside (not instead of)
  the Phase E persistence pool. WebSocket handler now parses
  `PredictionEvent` and forwards `.flight_event.model_dump_json()`,
  preserving exact frontend compatibility.
- **Phase D's `ingestion/consumer_runner.py` and
  `events/delay_prediction_consumer.py` deleted** — fully superseded (by
  Phase E's pool and this phase's `DelayPropagationWorker`,
  respectively). **`events/kafka_consumer.py` (the generic wrapper those
  two files were the only users of) deleted too** — confirmed via grep
  that nothing else imports it; same "fully superseded, not a hypothetical
  future need" reasoning.

## A real, previously-invisible bug found and fixed

**Every ML prediction since Phase B silently fell back to echoing
`dep_delay`, never actually running the model, on effectively all real
traffic.** Root cause: the trained model's label encoders were fit on
3-letter IATA airport codes (`JFK`) — confirmed directly against
`ml/model.pkl`'s `le_origin.classes_` (359 entries, all 3 characters,
`"JFK"` present, `"KJFK"` absent) — while this app's entire ingestion
pipeline (mock client and the live AeroAPI parser) produces 4-letter ICAO
codes (`KJFK`). `predict()`'s existing unseen-label `except ValueError:
return dep_delay` fallback masked this completely; there was no signal
that the model was never actually running. Phase F's new
`predict_with_confidence()` is what surfaced it — real traffic was coming
back with `model_confidence=0.0` across the board on first observation
against the live pipeline (my initial validation, using literal `"JFK"`/
`"LAX"` test inputs rather than the pipeline's actual `"KJFK"`/`"KLAX"`
shape, didn't catch this).

Fixed with `DelayPredictor._normalize_airport_code()`: strips a leading
`"K"` from 4-letter codes before encoding, which recovers the IATA form
for the standard mainland-US ICAO convention (`K` + IATA code) that
covers the large majority of real US airports. This is a real but
partial fix, documented as such in the code: Hawaii/Alaska/territories
(`PHNL`, `PANC`, `TJSJ`, ...) don't follow that convention and still hit
the fallback — verified directly (`PHNL` still returns `confidence=0.0`,
`dep_delay` echoed unchanged). Re-verified against live production
traffic post-fix: real, varied confidence values observed (0.0, 0.038,
0.437, 0.729, 1.0 across different flights), not a flat 0 or 1.

## What was verified live (not asserted)

- **Full stack startup**: real Postgres/Redis/Kafka (already running
  locally, not started for this session), `uvicorn flight_tracker.server:app`
  reaching `Application startup complete` with both the Phase E
  persistence pool (4 workers) and the new `DelayPropagationWorker`
  starting cleanly, zero errors.
- **`/health/db`**: `active_flights_rows`/`flight_events_rows` populated
  and growing, pool at configured size, Redis `ok`.
- **`/health/dlq`**: 0 dead-lettered events throughout the entire test
  session (both before and after the predictor fix).
- **End-to-end propagation**: unit-level test against a real 2-flight
  same-aircraft graph confirmed a delay on the source flight produces a
  `PredictionEvent` for the source (real model prediction + confidence)
  and a separate `PredictionEvent` for the propagated flight carrying the
  correct `propagation_source` (source's `flight_key`) and
  `propagation_hops=1`.
- **Live production traffic**: consumed real messages off `delay-predictions`
  post-restart — confirmed `PredictionEvent`-shaped JSON (not the old
  `FlightEventEnvelope` shape), confirmed varied real confidence scores
  (not always 0.0 or 1.0), confirmed `WN`/`DL`/`AA` carrier + `KJFK`-style
  ICAO codes now producing genuine model output instead of the
  dep_delay-echo fallback.
- **WebSocket**: connected a real client to `/ws/KJFK`, received 30
  messages (initial graph dump + live stream) in the exact `FlightEvent`
  JSON shape the frontend expects — confirms `PredictionEvent.from_json()`
  parsing + `.flight_event.model_dump_json()` forwarding works correctly
  end-to-end.
- **Frontend**: real browser check (Chrome via MCP) against the running
  dev server — map rendered 1665 flights / 335 delayed live, click-to-
  inspect on a real dot returned real backend data (flight number, route,
  aircraft, gate, live status), zero console errors.
- **Graceful shutdown**: `SIGINT` to the running server — both the
  4-worker persistence pool and the single `DelayPropagationWorker`
  cancelled and stopped cleanly with accurate final counts
  (`DelayPropagationWorker delay-propagation-0 stopped (processed=28,
  failed=0, restarts=0)`), producer/DB pool/Redis client all closed, zero
  hangs, zero errors.
- **Test suites**: `pytest flight_tracker/tests/` — 7/7 passed (retry
  backoff, DB-level idempotency against real Postgres, DLQ metadata) —
  none of these touch the changed `AsyncEventProcessor`/`build_worker_pool`
  signatures, so they remain valid, unmodified coverage. Frontend
  `npm test` — 4/4 passed, unaffected by backend changes (same WebSocket
  message shape).

## Known limitations (honest, not silently smoothed over)

- The airport-code normalization fix is mainland-US-only by convention;
  Hawaii/Alaska/territories still hit the unseen-label fallback. A full
  fix would need either retraining on ICAO-coded data or a real
  ICAO→IATA lookup table — out of scope for this phase, flagged here as a
  known next step rather than fixed silently or left undocumented.
- `DelayPropagationWorker` has no Redis idempotency guard (unlike the
  Phase E pool) — deliberate, not an oversight: it's single-instance, so
  Kafka's own commit-after-process offset handling is the only replay
  guard that's actually needed (see the processor's own docstring for the
  full reasoning on why a redelivered message is safe to reprocess as-is).
- `GraphEngine` remains ephemeral/in-memory by design (see
  `STATE_MANAGEMENT.md`) — a `DelayPropagationWorker` restart rebuilds
  the graph from `active_flights` via `load_from_db()`-equivalent logic
  only at server startup, not on a mid-run crash-restart (the Supervisor
  restarts the worker's Kafka consumer, not the graph rebuild) — a
  crash-restarted worker resumes with whatever graph state
  `graph_engine` still holds in the same process, which is correct since
  the process itself didn't restart, only the worker's consume loop did.
