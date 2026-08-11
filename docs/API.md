# API Reference

All endpoints are served by the one FastAPI process (`flight_tracker/server.py`), default `http://localhost:8000`. CORS currently allows only `http://localhost:3000` (`server.py`'s `CORSMiddleware` config) — the frontend origin, hardcoded, not yet configurable via env var.

## REST endpoints

### `GET /api/config`

Returns the tracked airport, so the frontend knows what to subscribe to before opening its WebSocket.

```json
{ "target_airport": "KJFK" }
```

### `GET /health`

Single aggregated liveness/metrics snapshot — the one to point a load balancer or uptime check at.

```json
{
  "status": "healthy",
  "timestamp": "2026-08-10T23:20:20+00:00",
  "services": { "database": "ok", "redis": "ok", "kafka": "ok" },
  "metrics": {
    "events_per_second": 6.45,
    "avg_latency_ms": 47.38,
    "error_rate": 0.0,
    "consumer_lag": 0
  }
}
```

`status` is `"healthy"` only if every entry in `services` is `"ok"`, otherwise `"degraded"`. `events_per_second` is a cumulative average since process start, not a trailing window (use Prometheus's `rate(events_processed_total[1m])` for a real rate — see `flight_tracker/OBSERVABILITY.md`).

> The brief this doc was written against assumed separate `/health/db`, `/health/redis`, `/health/kafka` endpoints. That's not what exists — Redis and Kafka status are folded into `/health`'s `services` object above; only the database gets its own, more detailed endpoint (`/health/db`, next).

### `GET /health/db`

More detail than `/health` needs on every poll: row counts and asyncpg pool stats.

```json
{
  "active_flights_rows": 42,
  "flight_events_rows": 5301,
  "redis": "ok",
  "pool": { "size": 20, "min_size": 20, "max_size": 50, "idle": 18 }
}
```

### `GET /health/dlq`

Count of `dead-letter-events` messages in the last hour.

```json
{ "dead_letter_events_last_hour": 0, "warning_threshold": 10, "warning": false }
```

### `GET /metrics`

Prometheus text-format exposition (`prometheus_client.generate_latest()`). See `flight_tracker/OBSERVABILITY.md` for the full metric reference (counters, histograms, gauges) and what each one means.

### `GET /api/flights/{flight_id}`

Cache-aside (Redis, 5 min TTL) current status of one flight. `404` if `flight_id` doesn't exist in `active_flights`.

```bash
curl http://localhost:8000/api/flights/UA100-mock-3
```

```json
{
  "flight_key": "UA100-20260810",
  "flight_id": "UA100-mock-3",
  "airline_code": "UA",
  "flight_number": "100",
  "origin": "KJFK",
  "destination": "KLAX",
  "aircraft_id": "N12345",
  "gate_id": "B7",
  "scheduled_departure": "2026-08-10T18:00:00+00:00",
  "scheduled_arrival": "2026-08-10T21:15:00+00:00",
  "delay_minutes": 23,
  "status": "active",
  "passenger_count": 178,
  "last_updated": "2026-08-10T18:12:00+00:00"
}
```

### `GET /api/airports/{airport_code}/snapshot`

Cache-aside (10 min TTL) — every row in `active_flights` for that airport. Returns `[]`, not `404`, for an airport with no active flights.

```bash
curl http://localhost:8000/api/airports/KJFK/snapshot
```

```json
[
  { "flight_key": "UA100-20260810", "flight_id": "UA100-mock-3", "...": "same shape as above" }
]
```

### `GET /api/flights/{flight_id}/delays`

Cache-aside (2 min TTL) — up to 20 most recent delayed (`delay_minutes > 0`) `flight_events` rows for this flight, newest first.

```bash
curl http://localhost:8000/api/flights/UA100-mock-3/delays
```

```json
[
  {
    "id": 5301, "flight_id": "UA100-mock-3", "flight_key": "UA100-20260810",
    "event_type": "delay", "delay_minutes": 23, "status": "active",
    "captured_at": "2026-08-10T18:12:00+00:00", "...": "full flight_events row"
  }
]
```

## WebSocket

### `ws://localhost:8000/ws/{airport_code}`

One connection per browser tab. On connect, the server sends, in order:

1. One `SNAPSHOT` message — current in-memory graph state for the airport.
2. A bounded replay (up to 100 messages) of recent activity, for a client that connects mid-stream.
3. The live stream — every subsequent `delay-predictions` event, mapped to a typed message.
4. A `HEARTBEAT` every 30 seconds, interleaved with the above, so the client can detect "socket claims open but nothing has arrived" independent of a low-level ping/pong frame.

### Message envelope

Every message (`flight_tracker/websocket/messages.py`) has this shape:

```json
{
  "type": "DELAY_PREDICTION",
  "timestamp": "2026-08-10T18:12:03.441+00:00",
  "flight_id": "UA100-mock-3",
  "data": { "...": "type-specific, see below" }
}
```

`flight_id` is `null` for `SNAPSHOT` and `HEARTBEAT` only.

### Message types

| Type | When sent | `data` contains |
|---|---|---|
| `SNAPSHOT` | Once, right after connect | `{"flights": [<flight_event-shaped dict>, ...]}` — every flight currently in the graph |
| `FLIGHT_UPDATE` | An ordinary status update — not delayed, not a propagation result, not a gate reassignment | Full flight fields (see below) |
| `DELAY_PREDICTION` | A flight has its own predicted delay (`delay_minutes > 0`, not from propagation) | Full flight fields + prediction fields |
| `PROPAGATION_EVENT` | This flight's delay is a BFS-propagated result of another flight's delay | Full flight fields + prediction fields, `propagation_source`/`propagation_hops` set |
| `GATE_REASSIGNMENT` | `resolve_gate_conflicts()` moved this flight's gate | Full flight fields + `gate_reassignment: {old_gate, new_gate}` |
| `HEARTBEAT` | Every 30s | `{}` |

`FLIGHT_UPDATE`/`DELAY_PREDICTION`/`PROPAGATION_EVENT`/`GATE_REASSIGNMENT` all carry the same flattened shape: every `FlightEvent` field (see [docs/DATA_MODEL.md](DATA_MODEL.md#flightevent)) plus `predicted_delay_minutes`, `predicted_arrival_time`, `model_confidence`, `propagation_source` (`null` unless it's a `PROPAGATION_EVENT`), `propagation_hops` (`null` likewise), and `gate_reassignment` (`null` unless it's a `GATE_REASSIGNMENT`) — one object with everything a given message type needs, no cross-referencing by `flight_id` required.

### Example: `PROPAGATION_EVENT`

```json
{
  "type": "PROPAGATION_EVENT",
  "timestamp": "2026-08-10T18:12:04.019+00:00",
  "flight_id": "DL200-mock-7",
  "data": {
    "flight_id": "DL200-mock-7",
    "airline_code": "DL",
    "flight_number": "200",
    "origin": "KJFK",
    "destination": "KORD",
    "aircraft_id": "N67890",
    "gate_id": "A3",
    "scheduled_departure": "2026-08-10T19:00:00+00:00",
    "scheduled_arrival": "2026-08-10T21:00:00+00:00",
    "delay_minutes": 17,
    "status": "active",
    "predicted_delay_minutes": 17,
    "predicted_arrival_time": "2026-08-10T21:17:00+00:00",
    "model_confidence": 0.81,
    "propagation_source": "UA100-mock-3",
    "propagation_hops": 1,
    "gate_reassignment": null
  }
}
```

This is what a client watching `UA100-mock-3` specifically (see below) would also receive, since it originated from that flight's delay even though it's a different `flight_id`.

### Client → server: optional per-flight subscription

The server also reads client-sent frames (used only for this):

```json
{ "action": "subscribe", "flight_id": "UA100-mock-3" }
{ "action": "unsubscribe" }
```

After `subscribe`, the connection only receives messages for that `flight_id` **plus** any `PROPAGATION_EVENT` whose `propagation_source` matches it (so watching one flight still shows its downstream cascade). Malformed or unrecognized frames are ignored, not fatal. With no subscription (the default), a connection receives every message for the tracked airport.
