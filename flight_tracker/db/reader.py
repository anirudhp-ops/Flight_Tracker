"""
Read queries backing the cache-aside endpoints in server.py. Kept separate
from writer.py (which only ever writes) so each module has one job.
"""
from flight_tracker.metrics import database_query_latency


async def get_flight_status(pool, flight_id: str) -> dict | None:
    with database_query_latency.labels(query_type="get_flight_status").time():
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM active_flights WHERE flight_id = $1 ORDER BY last_updated DESC LIMIT 1",
                flight_id,
            )
    return dict(row) if row is not None else None


async def get_airport_snapshot(pool, airport_code: str) -> list[dict]:
    with database_query_latency.labels(query_type="get_airport_snapshot").time():
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM active_flights WHERE airport_code = $1 ORDER BY last_updated DESC",
                airport_code,
            )
    return [dict(r) for r in rows]


async def get_recent_delays(pool, flight_id: str, limit: int = 20) -> list[dict]:
    with database_query_latency.labels(query_type="get_recent_delays").time():
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM flight_events
                WHERE flight_id = $1 AND delay_minutes > 0
                ORDER BY captured_at DESC
                LIMIT $2
                """,
                flight_id, limit,
            )
    return [dict(r) for r in rows]
