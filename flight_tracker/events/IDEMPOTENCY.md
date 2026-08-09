# Delivery semantics: at-least-once, not exactly-once

> **Update (Phase E)**: the "What idempotency does NOT cover" section below
> was accurate for Phase D but is now **wrong about `flight_events`** — see
> "Phase E: flight_events becomes idempotent too" at the end of this file
> for what changed, why, and what's still true from the original text below
> (kept as-is rather than silently edited, since the reasoning that led to
> the original decision is still worth having on record).

**Chosen: at-least-once delivery** — Kafka's default, and what this app
actually implements end to end. Exactly-once semantics (Kafka transactions
across the producer → consumer → DB write) were not built.

**Why**: exactly-once is real, load-bearing complexity — transactional
producers, read-committed isolation on every consumer, and a DB write that
has to participate in the same transaction as the offset commit (or an
outbox-pattern workaround if it can't). None of that complexity is
justified for what this app is actually protecting: a flight's *current*
delay status, ingested at most once a minute, where "there was briefly an
extra identical event during a rare producer retry" has zero real
consequence. If this handled financial transactions instead of flight
delays, that tradeoff would flip.

## How idempotency is actually achieved — corrected from the original spec

The original phase brief described this as "flight_id + timestamp (write
guard in DB)... second write is rejected by DB constraint." That's close
but not what the code does, and the difference matters:

- `active_flights`' primary key is **`flight_key`** (`{airline_code}{flight_number}-{date}`),
  not a `flight_id`+`timestamp` compound key. `flight_id` isn't unique
  across a flight's history in this schema at all — see
  `flight_tracker/db/README.md`.
- A duplicate write is **not rejected by a constraint violation**. It's
  silently absorbed by the staleness-guard upsert already in place from
  Phase C:

  ```sql
  INSERT INTO active_flights (...) VALUES (...)
  ON CONFLICT (flight_key) DO UPDATE SET ...
  WHERE active_flights.last_updated < EXCLUDED.last_updated
  ```

  Two writes for the same `flight_key` with the same (or non-newer)
  `last_updated` — exactly what redelivering the identical event produces
  — result in one `INSERT` and one no-op `UPDATE`, not an insert followed
  by a rejected/erroring second insert. No exception, no DLQ entry, no
  retry: the second write just doesn't change anything.

This is the same mechanism Phase C built for a different reason (protecting
against *out-of-order* writes clobbering newer data) turning out to also
solve *duplicate* writes for free — both are instances of "don't let a
write that isn't newer than what's already there take effect."

## What idempotency does NOT cover

**`flight_events` is not idempotent, on purpose.** It's an append-only
history log (no `ON CONFLICT` at all — see `db/README.md`) — redelivering
the same event legitimately inserts a second row. That's not a bug to fix;
deduplicating an audit log would mean deciding which of two "identical"
delivery attempts is the one that "really happened," which is a harder and
less useful problem than it sounds, for no real benefit here. If
`flight_events` needs exactly-once history later, that's a deliberate
future decision, not an oversight in this one.

The dead-letter-events topic (see `dlq_utils.py`) is also not deduplicated
— a message that fails processing twice (e.g., redelivered after a
consumer crash before its first DLQ publish committed) produces two DLQ
records. Given DLQ volume is expected to be low, this is treated as
acceptable noise rather than something worth deduplicating.

## Verified

Published the identical `FlightEventEnvelope` (same `flight_key`
`AA9999-20260809`, same `timestamp`) to `flight-events` twice, ran it
through the real `consumer_runner` against live Postgres:

```
active_flights: 1 row  (flight_key AA9999-20260809, delay_minutes=20)
flight_events:  2 rows (same flight_key, same delay_minutes, same captured_at)
```

Exactly the split described above: current-state table stayed idempotent,
history log recorded both deliveries. **This specific outcome (2 rows in
flight_events) no longer reproduces after Phase E — see below.**

## Phase E: flight_events becomes idempotent too

`flight_tracker/workers/CONCURRENCY.md`'s phase brief specified a real
`UNIQUE(flight_id, event_timestamp)` database constraint for idempotency,
which directly contradicts the Phase D decision above ("`flight_events` is
not idempotent, on purpose"). Rather than silently ignore the new
instruction to preserve the old doc's consistency, or silently rewrite
history to pretend this was always the plan: **the Phase D reasoning was
reconsidered and the decision changed.**

**What changed**: `db/writer.py` now has a `UNIQUE` index on
`flight_events (flight_id, captured_at)` (`captured_at` is the real column
name — see the "corrected from the original spec" note above, which still
applies), and `write_events()` inserts with `ON CONFLICT (flight_id,
captured_at) DO NOTHING`. A required one-time migration deleted 214
pre-existing duplicate-group rows that had accumulated during Phase D's
deliberately-non-deduplicated period, before the unique index could even
be created (`CREATE UNIQUE INDEX` fails outright on data that already
violates it) — see the migration comment in `db/writer.py` for the exact
`DELETE` used.

**Why the reconsideration is a real improvement, not just spec-following**:
Phase E adds `WORKER_COUNT` concurrent workers and a Redis idempotency
cache (`processed:{flight_id}:{timestamp}`, `event_processor.py`) as a
fast-path optimization to skip redundant processing. That cache can go
stale, get evicted, or simply not exist yet right after a Redis restart —
at which point, under Phase D's rules, a redelivered event would produce a
second `flight_events` row with no guard at all. The original argument
against deduplicating flight_events ("deciding which of two identical
delivery attempts really happened is a harder problem than it sounds") is
still true in general, but doesn't actually apply here: `ON CONFLICT DO
NOTHING` doesn't decide between two attempts, it just keeps whichever
landed first and discards an exact byte-for-byte duplicate — no
ambiguity, because "exact duplicate" is a well-defined concept
(`flight_id` + `captured_at` matching) while "which delivery is more
correct" would not have been.

**What's still true from the Phase D reasoning above**: the DLQ's
non-deduplication (a message failing twice produces two DLQ records) is
unaffected — dead-letter-events has no unique constraint and none is
planned; DLQ volume stays low enough that this remains acceptable noise.
And the core at-least-once-not-exactly-once choice at the top of this file
is unchanged — this update only tightens what happens when the *same*
event is delivered more than once, not the overall delivery guarantee.

**Verified** (`flight_tracker/tests/test_workers.py`,
`test_idempotency_db_constraint_prevents_duplicate_flight_events`):
publishing the identical `(flight_id, timestamp)` event twice through
`write_events()` now produces exactly **one** `flight_events` row, not
two. A companion test
(`test_idempotency_different_timestamps_both_recorded`) confirms two
genuinely different events for the same `flight_id` are still both
recorded — only exact duplicates are discarded.
