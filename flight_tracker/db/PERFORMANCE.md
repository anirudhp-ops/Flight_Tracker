# Cache performance: before/after

Produced by running `scripts/measure_db_performance.py` against the real
local Postgres + Redis instances — `active_flights` had **1,058 rows for
KJFK** (`TARGET_AIRPORT`) at measurement time, `flight_events` around 490.
Every number below is from that actual run, not an estimate. Re-run the
script yourself to reproduce or refresh these numbers; it prints the same
report.

## Result 1: repeated flight_events lookup (apples-to-apples)

Same query (`get_recent_delays` — `flight_events` filtered by `flight_id`,
ordered by `captured_at DESC`, `LIMIT 20`), run 100 times, direct-DB vs.
cache-aside:

| | Total (100 calls) | Avg per call |
|---|---|---|
| Direct DB, no cache | 33.94 ms | 0.339 ms |
| Cache-aside (1 miss + 99 hits) | 14.24 ms | 0.142 ms |

**2.4x faster overall.** This is the clean comparison — identical query,
identical result shape, only the cache is different — so this is the number
to trust as representative of what caching this specific access pattern
buys you.

## Result 2: airport snapshot, miss vs. hit

| | Time |
|---|---|
| Cache MISS (call 1 — runs the DB query, populates Redis) | 9.96 ms |
| Cache HIT (calls 2–10, avg) | 2.221 ms |

**A hit is 4.5x faster than a miss.** Read this number carefully, though —
it is **not** directly comparable to "Result 1" or to a hypothetical
"500-row baseline," because the miss here isn't row-count-matched against
anything above:

- The airport-snapshot endpoint (`GET /api/airports/{airport_code}/snapshot`)
  intentionally has **no row cap** — it returns every currently-active
  flight for the airport, all 1,058 KJFK rows, because capping "what's
  currently active" at an arbitrary number would be the wrong product
  behavior, not just a different benchmark parameter.
- A separate, deliberately capped baseline query (`... LIMIT 500`, matching
  the phase brief's "load 500 active flights") measured **2.38 ms** for 500
  rows via a direct, uncached `asyncpg` fetch — faster than the 9.96 ms
  snapshot miss above, because it's fetching half as many rows *and* isn't
  paying JSON-encoding + a Redis `SET` on top, which the cache-populating
  miss does.

So: caching a query that returns "everything, unbounded" pays an
up-front serialization/Redis-write cost on the first call, then repays it
many times over on every subsequent call within the TTL window. Caching a
query that's already cheap and bounded (like the 500-row direct fetch) has
less to gain relative to its own cost, and isn't what's cached here (there's
no capped-snapshot endpoint — the two aren't the same code path).

## Cache hit rate

**98.2% (108 hits / 2 misses)** during the benchmark run.

This number reflects the benchmark's own access pattern — it deliberately
hits the *same two keys* (`airports:KJFK`, `delays:AA4586-mock-0`) 10 and
100 times respectively — not real, diverse production traffic. Treat it as
"the cache works and serves repeated reads correctly," not as a prediction
of real-world hit rate, which depends entirely on how concentrated actual
request traffic is on the same flights/airports within each TTL window (5
min for flight status, 10 min for airport snapshots, 2 min for delays).

## What this does and doesn't tell you

- Confirmed: the cache-aside layer works correctly (MISS then HIT, TTL
  expiry untested here but implemented via Redis `EX`), and measurably
  speeds up repeated identical reads.
- Not measured: performance under concurrent load, cache behavior once TTLs
  actually expire mid-benchmark, or hit rate under realistic (non-repeated)
  request patterns. This app has no real concurrent user traffic yet to
  measure those against honestly — see `DATABASE_DESIGN.md` for why TTL
  values were chosen the way they were regardless.
