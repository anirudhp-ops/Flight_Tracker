import asyncpg
from flight_tracker.config import settings
from flight_tracker.models.events import FlightEvent, FlightStatus

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

# Run after CREATE_TABLES_SQL, unconditionally and idempotently, so they also
# apply to a database whose tables already existed before this column/index
# set was introduced (CREATE TABLE IF NOT EXISTS alone won't add a column to
# a table that's already there). See flight_tracker/db/README.md for why
# these specific indexes and this column exist.
SCHEMA_MIGRATIONS_SQL = """
ALTER TABLE active_flights ADD COLUMN IF NOT EXISTS airport_code TEXT;

-- Best-effort backfill for rows written before airport_code existed: origin
-- is the closest approximation available (the mock/live client only ever
-- ingests flights departing from or arriving at the configured
-- TARGET_AIRPORT). New rows get the real value from write_events().
UPDATE active_flights SET airport_code = origin WHERE airport_code IS NULL;

CREATE INDEX IF NOT EXISTS idx_flight_events_flight_id_captured_at
    ON flight_events (flight_id, captured_at);

CREATE INDEX IF NOT EXISTS idx_active_flights_airport_code_last_updated
    ON active_flights (airport_code, last_updated);
"""

async def create_pool(host=None, port=None, database=None, user=None, password=None):
    """Any argument left as None falls back to the shared config.settings."""
    return await asyncpg.create_pool(
        host=host or settings.db_host,
        port=port or settings.db_port,
        database=database or settings.db_name,
        user=user or settings.db_user,
        password=password if password is not None else settings.db_password,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        max_queries=settings.db_pool_max_queries,
        max_cached_statement_lifetime=settings.db_pool_max_cached_statement_lifetime,
    )

async def ensure_schema(pool):
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLES_SQL)
        await conn.execute(SCHEMA_MIGRATIONS_SQL)

async def write_events(pool, events: list[FlightEvent], airport_code: str | None = None) -> int:
    """
    airport_code identifies which tracked airport these events belong to
    (settings.target_airport at ingestion time). Stored on active_flights so
    "active flights for airport X" can be indexed and queried directly
    instead of inferring it from origin/destination. flight_events doesn't
    carry it — it isn't queried per-airport anywhere today.
    """
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
            airport_code,
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
                    delay_minutes, status, passenger_count, last_updated,
                    airport_code
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                ON CONFLICT (flight_key) DO UPDATE SET
                    flight_id     = EXCLUDED.flight_id,
                    aircraft_id   = EXCLUDED.aircraft_id,
                    gate_id       = EXCLUDED.gate_id,
                    delay_minutes = EXCLUDED.delay_minutes,
                    status        = EXCLUDED.status,
                    last_updated  = EXCLUDED.last_updated,
                    airport_code  = EXCLUDED.airport_code
                WHERE active_flights.last_updated < EXCLUDED.last_updated
                """, active_rows,
            )
    return len(events)


async def cleanup_stale_active_flights(pool, max_age_hours: float = 24) -> int:
    """
    Deletes active_flights rows that landed more than max_age_hours ago.
    Mirrors GraphEngine.prune_expired_flights (which prunes the in-memory
    graph) but operates on Postgres — the two are independent because the
    graph is rebuilt from this table on every restart via load_from_db, so a
    row Postgres never cleans up would keep reappearing in the graph even
    after the in-memory prune removed it.
    """
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            DELETE FROM active_flights
            WHERE status = $1 AND last_updated < now() - make_interval(hours => $2)
            """,
            FlightStatus.LANDED.value, max_age_hours,
        )
    # asyncpg command tag looks like "DELETE 42"
    return int(result.split()[-1])