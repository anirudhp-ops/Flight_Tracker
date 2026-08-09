# Kafka architecture

## Data flow

```
┌─────────────────┐
│ FlightAware or   │
│ mock client      │
└─────────┬────────┘
          │ FlightEvent
          ▼
┌──────────────────────┐         ┌───────────────────────────────┐
│ ingestion/worker.py    │  publish │ Topic: flight-events            │
│ (pure Kafka producer,  │─────────▶│ 3 partitions, 7d retention,     │
│  no DB/Redis access)   │  key=    │ key=flight_id                   │
└──────────────────────┘  flight_id└──────────────┬──────────────────┘
                                                    │ consume
                                                    ▼
                                    ┌───────────────────────────────────┐
                                    │ ingestion/consumer_runner.py         │
                                    │ group: flight-processor              │
                                    │  - validate (pydantic)                │
                                    │  - write_events() → Postgres          │
                                    │  - hourly cleanup_stale_active_flights│
                                    └──────────────┬────────────────────────┘
                                                    │ publish (on success)
                                                    ▼
                                    ┌───────────────────────────────────┐
                                    │ Topic: processed-flights             │
                                    │ 3 partitions, 7d retention           │
                                    └──────────────┬────────────────────────┘
                                                    │ consume
                                                    ▼
                                    ┌───────────────────────────────────┐
                                    │ events/delay_prediction_consumer.py  │
                                    │ group: delay-predictor               │
                                    │  - GraphEngine.process_event          │
                                    │  - GraphEngine.propagate_delay        │
                                    │  - GraphEngine.resolve_gate_conflicts │
                                    │  - DelayPredictor.predict             │
                                    │  - periodic prune_expired_flights     │
                                    └──────────────┬────────────────────────┘
                                                    │ publish (event + any
                                                    │ propagated/reassigned
                                                    │ side effects)
                                                    ▼
                                    ┌───────────────────────────────────┐
                                    │ Topic: delay-predictions             │
                                    │ 3 partitions, 7d retention           │
                                    └──────────────┬────────────────────────┘
                                                    │ consume (one throwaway
                                                    │ consumer group per
                                                    │ connected browser tab)
                                                    ▼
                                    ┌───────────────────────────────────┐
                                    │ server.py /ws/{airport_code}         │
                                    │ group: websocket-stream-<uuid4>      │
                                    │ auto_offset_reset=latest             │
                                    └───────────────────────────────────┘

Any handler exception, in any consumer above ──▶ Topic: dead-letter-events
                                                  1 partition, 30d retention
                                                  (scripts/inspect_dlq.py,
                                                   GET /health/dlq)
```

Redis still exists (`flight_tracker/cache/redis_cache.py`) but only for the
Phase C cache-aside GET endpoints (`/api/flights/{id}`,
`/api/airports/{code}/snapshot`, `/api/flights/{id}/delays`) — it carries
zero event-streaming traffic now. See "Kafka vs Redis pub/sub" below for why
that split.

## Partition strategy

All three application topics use **3 partitions, keyed by `flight_id`**
(`KafkaEventProducer.publish()` — the default partitioner hashes the key,
so the same flight always lands on the same partition). Two consequences:

- **Ordering per flight is guaranteed.** All events for `AA123` arrive at
  its consumer in the order they were produced, because they're all on the
  the same partition and Kafka only guarantees order within a partition.
  Events for `AA123` and `DL456` have no ordering guarantee relative to
  each other — and none is needed; nothing in this pipeline compares
  different flights' event order.
- **3 partitions is a parallelism ceiling, chosen conservatively.** A
  consumer group can have at most as many active consumers as the topic
  has partitions — a 4th `flight-processor` instance would sit idle. 3 was
  picked as "enough to demonstrate/support a few parallel consumers later"
  for a single-broker dev setup tracking one airport, not derived from a
  load target — there's no load test justifying a specific number yet (see
  `PERFORMANCE.md`).

`dead-letter-events` is the deliberate exception: **1 partition**. DLQ
volume should be low by design (see `IDEMPOTENCY.md` — most failures are
either transient and eventually processed successfully, or a real bug that
needs a human, not more parallelism). One partition keeps failures in a
single, roughly-time-ordered stream that's easy to review end-to-end
(`scripts/inspect_dlq.py`), rather than scattered across three.

## Consumer group strategy

| Group | Topic | Purpose | Instances today |
|---|---|---|---|
| `flight-processor` | `flight-events` | persist to Postgres, forward | 1 |
| `delay-predictor` | `processed-flights` | graph + ML, forward | 1 |
| `websocket-stream-<uuid4>` (one per connection) | `delay-predictions` | stream to one browser tab | N (one per open tab) |

`flight-processor` and `delay-predictor` are conventional consumer groups:
every member of a group **splits** the topic's partitions between them —
add a second `flight-processor` instance and each handles roughly half the
partitions, doubling throughput (up to the 3-partition ceiling above).

The WebSocket case is different on purpose. Every connected browser tab
needs to see **all** delay-prediction events, not a share of them — so each
`/ws/{airport_code}` connection gets a **unique, never-reused** `group_id`
(`server.py`, `f"{settings.kafka_consumer_group_websocket}-{uuid4()}"`).
Kafka has no concept of "broadcast to every subscriber" the way Redis
pub/sub did; a unique group per subscriber is how you get that behavior
back on top of Kafka's group-splits-partitions model.

## Offset management

`flight-processor` and `delay-predictor`: **manual commit, after
processing, not before** (`KafkaEventConsumer.run()`) — `enable_auto_commit=False`,
and `consumer.commit()` is only called once the handler
(DB write + forward, or graph processing + forward) has returned
successfully, or once a failed message has been routed to the DLQ. This is
the mechanical basis of the at-least-once guarantee in `IDEMPOTENCY.md`: if
the process dies between "handler succeeded" and "commit landed," the
message gets redelivered and reprocessed on restart — never silently
dropped. `auto_offset_reset="earliest"` on both: a brand-new consumer group
(or one whose committed offset has expired past retention) starts from the
oldest available message, not "whatever's newest right now" — durability
over "just show me current stuff," appropriate for a pipeline persisting to
Postgres.

The WebSocket consumers are the deliberate exception:
`auto_offset_reset="latest"`. A newly-connected browser tab already gets
"current state" from `graph_engine`'s in-memory dump (`websocket_endpoint`,
before the Kafka consumer even starts) — replaying `delay-predictions`'
entire retained history on top of that would mean re-sending potentially
thousands of stale delay events to every new tab. Nothing commits offsets
here either (`enable_auto_commit=False` and no `.commit()` call) — the
group is thrown away when the connection closes, so there is nothing to
resume.

## Failure scenarios and recovery

- **A consumer process crashes mid-batch.** Whatever it hadn't committed
  gets redelivered to whichever consumer picks up that partition next
  (itself, on restart, or another group member). Verified two ways:
  (1) `SIGKILL`ed the whole running server (`uvicorn` + all three Kafka
  tasks) mid-poll-cycle, no graceful shutdown — on restart,
  `flight-processor` resumed from its last committed offset with no errors
  and continued processing new events correctly (`kafka-consumer-groups
  --describe` showed offsets advancing cleanly, LAG returning to 0).
  (2) A more precise, deterministic test: manually processed one message
  (DB write completed) using a raw consumer under a dedicated group,
  **deliberately skipped the offset commit** to simulate a crash landing in
  the gap between "handler succeeded" and "commit landed," then started a
  fresh consumer under the same group_id. The message **was redelivered**
  — proving it wasn't lost — got reprocessed, and landed correctly:
  `active_flights` still had exactly 1 row (the Phase C staleness-guard
  upsert absorbed the duplicate write), `flight_events` had exactly 2 rows
  (append-only, both deliveries recorded, per `IDEMPOTENCY.md`).
- **A single message can't be processed** (malformed payload, a bug in the
  handler, a transient DB error that isn't transient this time). Routed to
  `dead-letter-events` with the original topic/partition/offset, the error
  type/message/traceback, and a timestamp (`KafkaEventConsumer._send_to_dlq`)
  — then **committed anyway**. Deliberate: not committing would mean the
  same poison message gets re-read and re-DLQ'd forever on every restart,
  turning one bad message into an infinite loop instead of one DLQ record.
  Inspect with `scripts/inspect_dlq.py`; `GET /health/dlq` warns past
  `KAFKA_DLQ_WARNING_THRESHOLD` failures in the last hour.
- **The broker itself is unreachable.** `KafkaEventProducer.publish()`
  retries with backoff (`MAX_PUBLISH_RETRIES=3`,
  `RETRY_BACKOFF_SECONDS` scaling per attempt) before raising — a caller
  three levels up (the ingestion worker's own per-poll `try/except`)
  logs the failure and tries again next poll interval rather than crashing
  the worker.
- **A consumer group member joins or leaves** (deploy, crash, or just this
  app starting up). Kafka rebalances partition ownership among the
  remaining group members; `_RebalanceLogger` in `kafka_consumer.py` logs
  both phases (`on_partitions_revoked`, `on_partitions_assigned`) — visible
  directly in server logs as `INFO: [topic/group] partitions ...`.

## Scaling to multiple consumers

Run another instance of `ingestion/consumer_runner.py`'s `run()` (same
`group_id="flight-processor"`, same topic) and Kafka rebalances the 3
`flight-events` partitions across both instances automatically — no code
change, no coordination logic to write, because that's what a consumer
group *is*. Same for `delay-predictor` on `processed-flights`. The ceiling
is the partition count (3 today, per topic — see above); a 4th consumer in
either group would sit idle until the topic is repartitioned
(`kafka-topics --alter --partitions N`, which only ever increases, never
decreases, partition count).

One real constraint if `delay-predictor` is ever scaled beyond one
instance: `GraphEngine` is an in-memory, single-process object today
(`server.py`'s module-level `graph_engine`, passed by reference into
`delay_prediction_consumer.run()`). Two `delay-predictor` instances would
each need — and diverge on — their own copy of the graph, since nothing
currently shares or synchronizes it across processes. Scaling that consumer
group is safe for the Postgres-writing `flight-processor` group (stateless
per message) but not yet for `delay-predictor` without either centralizing
the graph (e.g., behind a small internal service) or partitioning it
consistently by flight so each instance only ever owns a disjoint subset —
neither built, both real future work, not silently glossed over here.

## Kafka vs. Redis pub/sub — what changed

| | Redis pub/sub (Phases B/C) | Kafka (this phase) |
|---|---|---|
| Delivery if no subscriber is connected | **Lost.** Pub/sub has no log; a message published to a channel with zero subscribers is gone. | **Retained** per topic's retention (7d/30d) — a consumer that starts late, or restarts, reads what it missed. |
| Delivery if the consuming process crashes mid-message | Lost — no offset/ack concept in Redis pub/sub. | **Preserved** — uncommitted messages are redelivered on restart (see "Failure scenarios" above — verified with both a real `SIGKILL` and a deterministic crash-before-commit simulation). |
| Ordering guarantee | None specified/relied on. | Per-partition, per-key (`flight_id`) — same flight's events always arrive in order. |
| Multiple independent readers of the same stream | Natural — every subscriber gets every message. | Requires a unique consumer group per independent reader (see "Consumer group strategy" — this is what the WebSocket handler's per-connection group works around). |
| Replay / reprocess history | Not possible — nothing is stored. | Possible within retention — `auto_offset_reset="earliest"` on a fresh/reset consumer group reprocesses everything still retained. |
| Backpressure if a consumer is slow | None — Redis just drops what it can't deliver to a slow/disconnected subscriber. | The topic buffers; a slow consumer accumulates lag (`kafka_consumer_lag` metric, warned on via `KAFKA_CONSUMER_LAG_WARNING_THRESHOLD`) instead of silently losing data. |
| Operational footprint | One process (already needed for caching). | A broker (KRaft, no separate Zookeeper — see `docker-compose.yml`) plus this app's own producer/consumer processes. Real cost for real guarantees. |

The concrete thing this bought, demonstrated live rather than asserted: a
`SIGKILL`ed server resumed cleanly on restart with offsets picking up
exactly where they left off, and a deliberately-simulated "processed but
crashed before commit" message was redelivered rather than lost — see
"Failure scenarios" above for both tests' exact steps and results. The
Phase B/C Redis pub/sub design could not have made either claim even in
principle — there was nothing for a restarted consumer to catch up *from*.
