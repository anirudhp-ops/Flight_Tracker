# Query optimization findings

Measured against the real local Postgres instance (`flight_tracker` db,
`max_connections=100`), with `active_flights` seeded to **1,008 rows**
(998 `airport_code='KJFK'`, 10 scattered across other codes — leftover real
data from earlier manual FlightAware testing) and `flight_events` at 430
rows, after `VACUUM ANALYZE`. All numbers below are actual `EXPLAIN ANALYZE`
output and real Python `time.perf_counter()` measurements, not estimates.

## 1. `GraphEngine.load_from_db()` — does it use the index?

**No, not on this deployment's actual data — and that's the planner making
the right call, not a bug.**

```
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM active_flights WHERE airport_code = 'KJFK' ORDER BY last_updated DESC;

 Sort  (cost=80.31..82.81 rows=998 width=102) (actual time=0.762..0.836 rows=998 loops=1)
   Sort Key: last_updated DESC
   ->  Seq Scan on active_flights  (cost=0.00..30.60 rows=998 width=102) (actual time=0.017..0.372 rows=998 loops=1)
         Filter: (airport_code = 'KJFK'::text)
         Rows Removed by Filter: 10
 Planning Time: 0.666 ms
 Execution Time: 0.961 ms
```

`idx_active_flights_airport_code_last_updated` exists but Postgres chooses a
sequential scan, because the filter matches 998 of 1,008 rows (~99%) — this
app only tracks one airport at a time (`TARGET_AIRPORT`), so on a
single-airport deployment this query is barely selective at all, and reading
the whole table directly is cheaper than bouncing through an index.

The index **does** engage the moment the filter is actually selective —
confirmed directly:

```
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM active_flights WHERE airport_code = 'KSBA' ORDER BY last_updated DESC;

 Index Scan Backward using idx_active_flights_airport_code_last_updated on active_flights
   (cost=0.28..8.29 rows=1 width=102) (actual time=0.022..0.023 rows=1 loops=1)
   Index Cond: (airport_code = 'KSBA'::text)
 Execution Time: 0.061 ms
```

**Practical implication:** on today's single-airport-at-a-time usage
pattern, this index is mostly dormant. It earns its keep the moment this
becomes a multi-airport deployment (several `airport_code` values, each a
small slice of the table) — which is exactly the direction `TARGET_AIRPORT`
being a single global setting suggests this project is headed. Keeping the
index now costs a small amount of write overhead and disk space in exchange
for not having to remember to add it later.

**Where the real cost is:** raw fetch of those 998 rows takes **4.35 ms**.
The full `GraphEngine.load_from_db()` call — fetch + constructing a
`FlightEvent` per row + `add_edges_for_flight()`'s O(n²) all-pairs scan over
every flight added so far — takes **796.50 ms**. That's **99.5% of the time
spent outside the database**, in the in-memory graph construction that was
already flagged as O(n²) in `ARCHITECTURE_ASSESSMENT.md`. No index or query
change touches this; it's a Python/graph-engine problem, out of scope for
this phase, but worth stating precisely rather than implying the DB layer is
the bottleneck when it measurably isn't.

## 2. Active flights by airport — does it use the index?

Same query as above. Same answer: not at ~99% selectivity, yes at high
selectivity. See above — this *is* "active flights by airport."

## 3. Baseline timings (as requested)

| Measurement | Result |
|---|---|
| Raw asyncpg fetch, 998-row `active_flights` (airport-filtered, ordered) | **4.35 ms** |
| Full `GraphEngine.load_from_db()` for the same 998 rows (fetch + graph construction) | **796.50 ms** |
| `flight_events` by `flight_id`, 100 sequential queries (delay-propagation lookup pattern), *with* `idx_flight_events_flight_id_captured_at` | **18.34 ms total / 0.183 ms avg** |
| Same single query, index disabled (`enable_indexscan=off`), forced seq scan over 430 rows | **0.116 ms**, 12 buffer reads vs. 2 with the index |

The flight_events index roughly halves per-query buffer reads even at only
430 rows; the gap widens as the table grows, since nothing currently
archives it (see `flight_tracker/db/README.md`, Cleanup policy).

## 4. Connection pool sizing rationale

Settings live in `config.py` (`db_pool_min_size=10`, `db_pool_max_size=20`,
`db_pool_max_queries=50000`, `db_pool_max_cached_statement_lifetime=300`),
applied via `db.writer.create_pool()` — every caller (`server.py`'s startup
pool, the ingestion worker's own pool) gets the same settings from one
place, same pattern as the rest of `config.py`.

- **`min_size=10`**: keeps 10 connections warm so a burst of concurrent
  WebSocket connections' initial graph-dump reads (each currently opens its
  own pool acquire, not yet cached — see `DATABASE_DESIGN.md`) doesn't pay
  connection-setup latency on the first requests after startup.
- **`max_size=20`**: this app currently opens **two independent pools**
  (`server.py`'s for `load_from_db`, the worker's for `write_events`/
  `cleanup_stale_active_flights`), so worst case is 2 × 20 = 40 connections
  against a `max_connections=100` Postgres — comfortable headroom, verified
  against the actual local server (`SHOW max_connections` = 100). This is
  sized as specified in the phase brief; it is oversized for this app's
  actual concurrency today (a single backend process, no horizontal
  scaling) and is explicitly a "production-grade defaults" setting to grow
  into, not a measured requirement.
- **`max_queries=50000`**: recycles a connection after 50k queries so a
  long-lived pool doesn't accumulate unbounded server-side session state.
  Irrelevant at current traffic (the ingestion worker issues on the order
  of tens of queries per hour), included for when traffic grows.
- **`max_cached_statement_lifetime=300`**: asyncpg caches prepared
  statements per-connection; 300s bounds how long a stale cached plan can
  live before Postgres re-plans it, relevant once query patterns diversify
  beyond the current handful of hardcoded statements.

None of these were load-tested against real concurrent traffic — there
isn't any yet. They're deliberately conservative "won't hurt, ready for
more" defaults, documented as such rather than presented as measured
optimal values.
