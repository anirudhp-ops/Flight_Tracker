# Database schema

Schema is defined and applied in `writer.py` (`CREATE_TABLES_SQL` +
`SCHEMA_MIGRATIONS_SQL`, run by `ensure_schema()`). There's no migration
framework — see [`DATABASE_DESIGN.md`](./DATABASE_DESIGN.md) for why that's
an acceptable tradeoff today and what replaces it later.

## Tables

### `flight_events`

Append-only history: one row per ingested snapshot of a flight, forever
(nothing deletes from this table — see "Cleanup policy" below). This is the
audit log — "what did we know about flight AA123 at 14:32 vs 14:33."

Primary key is a synthetic `id BIGSERIAL`, not `flight_key`, because the
same flight legitimately produces many rows over its lifecycle (departure,
delay updates, arrival).

### `active_flights`

Current-state snapshot: **one row per flight** (`flight_key` is the primary
key), overwritten in place as new data arrives. This is what the frontend's
initial WebSocket dump and `GraphEngine.load_from_db()` actually read — "what
are all the flights right now," not history.

`airport_code` was added in Phase C. It's the `TARGET_AIRPORT` the ingestion
worker was configured with at write time (`settings.target_airport`), not
derived from `origin`/`destination` — a flight departing JFK and one landing
at JFK should both count as "a JFK flight" for this table's purposes, and
inferring that from two different columns depending on direction would be
fragile. Rows written before this column existed were backfilled from
`origin` as a best-effort approximation (see `SCHEMA_MIGRATIONS_SQL`).

## Indexes

| Index | Table | Columns | Serves |
|---|---|---|---|
| `flight_events_pkey` | `flight_events` | `id` | PK lookup |
| `idx_flight_events_flight_key` | `flight_events` | `flight_key` | pre-existing, unused by any current query — kept for now, candidate for removal (see OPTIMIZATION.md) |
| `idx_flight_events_flight_id_captured_at` | `flight_events` | `(flight_id, captured_at)` | "recent events for flight X" — the delay-propagation / flight-history lookup pattern, and the cache-aside recent-delays endpoint |
| `active_flights_pkey` | `active_flights` | `flight_key` | PK lookup, and the upsert's `ON CONFLICT` target |
| `idx_active_flights_airport_code_last_updated` | `active_flights` | `(airport_code, last_updated)` | "active flights for airport X, most recent first" — `GraphEngine.load_from_db()` and the airport-snapshot cache-aside endpoint |

The task spec for this phase named `flight_events(flight_id, timestamp)` and
`active_flights(airport_code, last_updated)`. The second exists as specified.
The first is implemented as `(flight_id, captured_at)` — `flight_events` has
no `timestamp` column; the ingestion-capture time is `captured_at`. (The
FlightEvent model's own `timestamp` field is what gets written *into*
`captured_at` — see `write_events()`.)

**Whether an index actually gets used depends on the data**, not just its
existence — see [`OPTIMIZATION.md`](./OPTIMIZATION.md) for real
`EXPLAIN ANALYZE` output showing the airport index sitting unused on a
single-airport deployment (Postgres correctly prefers a sequential scan when
a filter matches ~99% of the table) versus engaging immediately once a query
is actually selective.

## The staleness-guard upsert pattern

```sql
INSERT INTO active_flights (...) VALUES (...)
ON CONFLICT (flight_key) DO UPDATE SET
    ...,
    last_updated = EXCLUDED.last_updated
WHERE active_flights.last_updated < EXCLUDED.last_updated
```

The ingestion worker polls on a fixed interval, publishes events to Redis,
and writes to Postgres — there's no guarantee those writes land in the same
order they were generated, especially once retries or multiple ingestion
sources exist. Without the `WHERE` clause, a slow/delayed write could
silently overwrite a newer row with older data. The guard makes the upsert a
no-op whenever the incoming row is not strictly newer than what's already
stored, so out-of-order writes can't regress visible state. `flight_events`
doesn't need this — it's append-only, there's nothing to regress.

## Cleanup policy

- `active_flights`: `cleanup_stale_active_flights()` deletes rows where
  `status = 'landed' AND last_updated < now() - <max_age>` (default 24h),
  run every `DB_CLEANUP_INTERVAL_SECONDS` (default 1h) by the ingestion
  worker. This is a real `DELETE`, not a soft-delete/archive — flight_events
  already has the full history for anything that needs to be looked up
  later.
- `flight_events`: **nothing deletes from this table today.** The worker
  logs its row count on every cleanup pass so growth is visible, but there
  is no archival or retention policy yet. This is a known gap, not an
  oversight — see `DATABASE_DESIGN.md` and the root `ARCHITECTURE_ASSESSMENT.md`.
- The in-memory graph (`GraphEngine.prune_expired_flights`) is pruned
  independently of Postgres, on its own schedule, because the graph is
  rebuilt from `active_flights` on every restart — a row Postgres never
  cleans up would keep reappearing in the graph even after an in-memory
  prune removed it.
