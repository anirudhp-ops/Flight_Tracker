# Delivery semantics: at-least-once, not exactly-once

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
history log recorded both deliveries.
