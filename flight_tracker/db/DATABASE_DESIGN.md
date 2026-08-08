# Database & caching design decisions

This is the "why" doc. For schema mechanics see `README.md`, for query
profiling see `OPTIMIZATION.md`, for cache benchmark numbers see
`PERFORMANCE.md`. This one ties the decisions together.

## Why two tables, not one or three

**Not one table.** `flight_events` (append-only history) and
`active_flights` (current-state snapshot, one row per flight) have
genuinely different write patterns and query patterns. Every ingestion poll
inserts N new `flight_events` rows unconditionally — it's a log. The same
poll *upserts* N `active_flights` rows, keyed by `flight_key`, because
"what is currently true about flight AA123" only has one answer at a time.
Merging them into one table would force a choice: either make it
append-only (and then "get the current state" becomes "find the latest row
per flight_key," a query that gets slower as history grows — the opposite
of what the frontend's initial-load path needs), or make it upsert-only
(and lose the history the delay model / a future "what changed and when"
view would need). Two tables means each one's access pattern matches its
storage pattern.

**Not three.** The obvious third table would be a dedicated `airports` or
`gates` reference table. Wasn't added because nothing in this codebase
queries airports or gates as first-class entities yet — `origin`/
`destination`/`gate_id` are just strings on a flight row, and the frontend's
`airports` lat/lon lookup is a hardcoded JS object, not DB-backed. Adding a
reference table with no current reader would be schema speculation, not a
design decision serving an actual query.

## The upsert-with-staleness-guard pattern

Covered mechanically in `README.md`. The design reason it exists: the
ingestion worker has no ordering guarantee across polls (network delays,
retries, or — once this becomes multi-instance — multiple workers writing
concurrently could all interleave). A plain `ON CONFLICT DO UPDATE` would
let a slow, stale write silently clobber a newer one. The
`WHERE active_flights.last_updated < EXCLUDED.last_updated` guard makes
"lost update" structurally impossible instead of relying on the ingestion
worker never being out of order — a much weaker guarantee to depend on.

## Caching strategy and TTLs

Cache-aside (`flight_tracker/cache/redis_cache.py`), not write-through:
Redis holds a copy, Postgres is always the source of truth, and a total
Redis flush degrades to "every read hits the DB" rather than "data is
gone." This matters here specifically because the ingestion worker **does
not invalidate cache keys on write** — a deliberate simplification, not an
oversight. Wiring invalidation into `write_events()` would mean threading a
`CacheLayer` into the ingestion worker and coupling write-path code to a
read-path concern, for a payoff that TTLs already deliver: every cached
value is wrong for at most its TTL window, by design, and that bound is the
whole point of choosing cache-aside-with-TTL over invalidate-on-write.

TTLs were sized against how fast each thing actually changes, not
uniformly:

- **Flight status, 5 min** (`flights:{flight_id}`) — `delay_minutes`/
  `status` change on the ingestion worker's own poll cadence
  (`POLL_INTERVAL_SECONDS`, default 60s). 5 minutes means a cached read can
  be several polls stale in the worst case — acceptable for "what's this
  flight's status," not acceptable if this were the delay-propagation input
  (it isn't; that path reads live off the WebSocket/graph, not this cache).
- **Airport snapshot, 10 min** (`airports:{airport_code}`) — the most
  expensive of the three to compute (full unbounded scan of every active
  flight at that airport, see `PERFORMANCE.md`) and the least
  latency-sensitive; a dashboard-style "what's happening at KJFK right now"
  view tolerates being up to 10 minutes stale far more than a single
  flight's delay status does.
- **Recent delays, 2 min** (`delays:{flight_id}`) — the shortest TTL,
  because this is the one most likely to back something delay-propagation-
  adjacent later, and stale delay data is the most misleading kind of stale
  data in this app specifically (it's a delay-propagation tracker).

No negative caching: `CacheLayer.get_or_set()` only caches non-`None`
results, so a request for a flight_id that doesn't exist always hits
Postgres. A 404 getting cached for 5 minutes would mask a flight that
starts existing moments later — worse than the extra DB round-trip.

## Query patterns and indexes

See `OPTIMIZATION.md` for the actual `EXPLAIN ANALYZE` output. Summary of
the decision: `idx_active_flights_airport_code_last_updated` is currently
mostly idle on this single-airport deployment (Postgres correctly prefers a
sequential scan when the filter matches ~99% of the table), kept anyway
because it's cheap relative to the multi-airport future `TARGET_AIRPORT`
being a single global setting already points toward, and because "add the
index now, confirmed to work on selective queries" beats "remember to add
it later under production load."
`idx_flight_events_flight_id_captured_at` is doing real work today — it
backs every "recent events for this flight" lookup (delay propagation,
the recent-delays cache-aside endpoint) and measurably roughly halves
buffer reads even at the current small table size.

## Connection pooling

See `OPTIMIZATION.md` for the exact settings and sizing math. Short
version: sized per the phase brief's production-grade defaults
(`min_size=10, max_size=20, max_queries=50000,
max_cached_statement_lifetime=300`), applied through one function
(`db.writer.create_pool()`) so every pool in the app — `server.py`'s and
the ingestion worker's — gets the same settings from `config.py` rather
than each call site guessing its own numbers. Verified against real local
Postgres (`max_connections=100`) that two pools at `max_size=20` each stay
well under the connection ceiling.

## Cleanup policy

See `README.md` for the mechanics. Design reason `active_flights` gets a
real `DELETE` (not a soft-delete/archive column) while `flight_events` gets
neither: `flight_events` already *is* the archive — a landed flight's full
history lives there permanently, so there's nothing to lose by removing its
`active_flights` row once it's old enough that "currently active" no longer
applies to it. `flight_events` growing forever with no retention policy is
a genuine, acknowledged gap (its row count is logged on every cleanup pass
specifically so the growth stays visible), not something this phase solved
— archiving/partitioning that table is future work, deliberately deferred
rather than solved incompletely under this phase's scope.
