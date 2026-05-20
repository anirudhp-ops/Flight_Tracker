import asyncpg
from flight_tracker.models.events import FlightEvent

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS flight_events (
    id                  BIGSERIAL PRIMARY KEY,
    flight_id           TEXT        NOT NULL,
    flight_key          TEXT        NOT NULL,
    event_type          TEXT        NOT NULL,
    airline_code        TEXT        NOT NULL,
    flight_number       TEXT        NOT NULL,
    origin              TEXT        NOT NULL,
    destination         TEXT        NOT NULL,
    aircraft_id         TEXT,
    gate_id             TEXT,
    scheduled_departure TIMESTAMPTZ NOT NULL,
    estimated_departure TIMESTAMPTZ,
    actual_departure    TIMESTAMPTZ,
    scheduled_arrival   TIMESTAMPTZ NOT NULL,
    estimated_arrival   TIMESTAMPTZ,
    actual_arrival      TIMESTAMPTZ,
    delay_minutes       INTEGER     NOT NULL DEFAULT 0,
    status              TEXT        NOT NULL,
    passenger_count     INTEGER,
    captured_at         TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_flight_events_flight_key
    ON flight_events (flight_key);

CREATE TABLE IF NOT EXISTS active_flights (
    flight_key          TEXT PRIMARY KEY,
    flight_id           TEXT        NOT NULL,
    airline_code        TEXT        NOT NULL,
    flight_number       TEXT        NOT NULL,
    origin              TEXT        NOT NULL,
    destination         TEXT        NOT NULL,
    aircraft_id         TEXT,
    gate_id             TEXT,
    scheduled_departure TIMESTAMPTZ NOT NULL,
    scheduled_arrival   TIMESTAMPTZ NOT NULL,
    delay_minutes       INTEGER     NOT NULL DEFAULT 0,
    status              TEXT        NOT NULL,
    passenger_count     INTEGER,
    last_updated        TIMESTAMPTZ NOT NULL
);
"""

async def create_pool(host, port, database, user, password):
    return await asyncpg.create_pool(
        host=host, port=port, database=database,
        user=user, password=password,
        min_size=2, max_size=10,
    )

async def ensure_schema(pool):
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLES_SQL)

async def write_events(pool, events: list[FlightEvent]) -> int:
    if not events:
        return 0

    event_rows = []
    active_rows = []

    for e in events:
        event_rows.append((
            e.flight_id, e.flight_key, e.event_type.value,
            e.airline_code, e.flight_number, e.origin, e.destination,
            e.aircraft_id, e.gate_id,
            e.scheduled_departure, e.estimated_departure, e.actual_departure,
            e.scheduled_arrival, e.estimated_arrival, e.actual_arrival,
            e.delay_minutes, e.status.value, e.passenger_count, e.timestamp,
        ))
        active_rows.append((
            e.flight_key, e.flight_id, e.airline_code, e.flight_number,
            e.origin, e.destination, e.aircraft_id, e.gate_id,
            e.scheduled_departure, e.scheduled_arrival,
            e.delay_minutes, e.status.value, e.passenger_count, e.timestamp,
        ))

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO flight_events (
                    flight_id, flight_key, event_type, airline_code, flight_number,
                    origin, destination, aircraft_id, gate_id,
                    scheduled_departure, estimated_departure, actual_departure,
                    scheduled_arrival, estimated_arrival, actual_arrival,
                    delay_minutes, status, passenger_count, captured_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
                """, event_rows,
            )
            await conn.executemany(
                """
                INSERT INTO active_flights (
                    flight_key, flight_id, airline_code, flight_number,
                    origin, destination, aircraft_id, gate_id,
                    scheduled_departure, scheduled_arrival,
                    delay_minutes, status, passenger_count, last_updated
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                ON CONFLICT (flight_key) DO UPDATE SET
                    flight_id     = EXCLUDED.flight_id,
                    aircraft_id   = EXCLUDED.aircraft_id,
                    gate_id       = EXCLUDED.gate_id,
                    delay_minutes = EXCLUDED.delay_minutes,
                    status        = EXCLUDED.status,
                    last_updated  = EXCLUDED.last_updated
                WHERE active_flights.last_updated < EXCLUDED.last_updated
                """, active_rows,
            )
    return len(events)