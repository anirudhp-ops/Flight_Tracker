# Performance: Bottleneck Analysis & Recommendations

This is the analysis document — *why* the system performs the way it does and what to actually do about it. For the raw numbers, see [docs/BENCHMARKS.md](BENCHMARKS.md) and [`flight_tracker/LOAD_TEST_REPORT.md`](../flight_tracker/LOAD_TEST_REPORT.md); for what to watch in production, see [`flight_tracker/OBSERVABILITY.md`](../flight_tracker/OBSERVABILITY.md)'s "Performance baseline".

## The headline finding

**This system reliably sustains ~25 events/sec end-to-end. It does not sustain a steady 100 events/sec** — publish latency to Kafka stays fast at both rates (sub-50ms even at p99), but end-to-end latency (publish → prediction) at 100/sec degrades to a p50 of 279ms and a p99 of over 11 seconds. This was measured directly, not projected.

## Root cause: the single-instance `DelayPropagationWorker`

Two compounding facts explain it:

1. **It's deliberately not horizontally scaled.** It's the only thing that mutates `GraphEngine`, an in-memory, in-process, non-shardable data structure (see [docs/ARCHITECTURE.md](ARCHITECTURE.md#graphengine-flight_trackergraphenginepy)). At sustained high throughput, its single consumer accumulates Kafka lag faster than it can drain.
2. **Its per-event cost is not constant — it grows with graph size.** `GraphEngine.add_edges_for_flight()` is O(V): every new event is compared against every existing node to find `aircraft_turn`/`gate_reuse` relationships. Nothing currently prunes non-`LANDED` flights from the graph (`prune_expired_flights()` only removes *landed* flights older than 24h), so a long-running or heavily-loaded process's per-event cost keeps climbing as `active_flights` accumulates.

This was directly reproduced: the same 100 events/sec run against a graph that had accumulated ~2,200 stale nodes from earlier test runs showed p50 e2e latency of 892ms, versus 279ms against a freshly-truncated `active_flights`. The benchmark table in [docs/BENCHMARKS.md](BENCHMARKS.md#graphengine-marginal-cost-at-graph-size-n) shows the same linear growth in isolation: at 10,000 nodes, one more event costs ~16ms in edge-scanning alone, before any DB write or ML prediction.

## The other known gap: ML prediction latency

The trained `RandomForestRegressor` (`ml/predictor.py`) runs single-row inference at p95 18.4ms, p99 28.2ms — 1.5–3x the phase target of p95 < 10ms, consistently, not just at the tail. This is the model's own inference cost, not serialization/integration overhead in `DelayPredictor`. Not investigated further in Phase I (a measurement phase, not a remediation phase) — see recommendations below.

## What's *not* the bottleneck

Worth stating explicitly, since it's tempting to suspect the more "exotic" parts of the stack first:

- **Kafka publishing**: sub-50ms at p99 even under 100 events/sec load.
- **The WebSocket layer**: 0% errors, 100% connect success under a 0→100 concurrent connection spike; not the bottleneck for any tested scenario.
- **Postgres**: query latency scales with result-set size as expected (more rows for an airport legitimately means a bigger response), with the relevant index confirmed used via `EXPLAIN ANALYZE` at every tested size — not a missing-index problem.
- **The persistence worker pool**: stateless, horizontally scalable up to the partition ceiling; not implicated in the degradation above (which is specifically about the single-instance propagation worker).

## Recommendations (not implemented — real future work)

In rough order of effort-to-impact:

1. **Prune non-landed stale flights on a schedule, not just landed ones.** `prune_expired_flights()` already exists and runs periodically for landed flights; the same mechanism (or a variant with a different, probably shorter, age threshold) applied to flights that are merely old — cancelled, diverted, or simply never updated again — would cap graph growth and directly address the O(V) cost driver above. This is the highest-leverage, lowest-effort fix available.
2. **Shard `DelayPropagationWorker` by airport, if this app ever tracks more than one.** The current single-instance design is explicitly a single-airport, single-graph assumption (`TARGET_AIRPORT` is one global setting). Tracking N airports with N independent graphs is a natural partitioning boundary that doesn't require solving the harder "shard one graph across processes" problem.
3. **Investigate the ML model's per-prediction cost**: fewer estimators in the `RandomForestRegressor`, or batching predictions if the pipeline can tolerate a small latency/throughput tradeoff, are the two obvious levers — neither tried yet.
4. **If graph-sharding-by-airport isn't enough, or this becomes genuinely multi-region**: centralize `GraphEngine` behind its own small internal service (so multiple `DelayPropagationWorker`-equivalent processes share one authoritative graph instead of each owning a private copy) — a materially bigger change, deliberately not attempted before the simpler options above are tried.
5. **More partitions + more persistence workers** raises the *persistence* pool's ceiling, but does nothing for the actual bottleneck identified above — worth knowing before reaching for this lever first. See [`flight_tracker/workers/CONCURRENCY.md`](../flight_tracker/workers/CONCURRENCY.md#how-to-scale) for the mechanics if the persistence pool ever does become the limiting factor instead.

## What to watch in production

`kafka_consumer_lag` (per consumer group) and `graph_node_count` are the two Prometheus metrics that would show this degradation mode developing before latency alerts fire — see the Grafana dashboard panels 6 and 7, and `alert_rules.yml`'s `HighEventProcessingLatency` threshold (set above the healthy-load p95 of ~81ms specifically so it fires on this exact degradation, not on ordinary jitter). Full metric reference: [`flight_tracker/OBSERVABILITY.md`](../flight_tracker/OBSERVABILITY.md).
