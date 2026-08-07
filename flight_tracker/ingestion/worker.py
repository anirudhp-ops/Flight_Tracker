import asyncio
from datetime import datetime, timezone

from flight_tracker.config import settings
from flight_tracker.db.writer import create_pool, ensure_schema, write_events
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.ingestion.publisher import RedisPublisher

PRUNE_INTERVAL_SECONDS = 600  # how often to check for expired flights (10 min)
PRUNE_MAX_AGE_HOURS = 24      # how long after landing a flight is kept


async def run(
    client,
    publisher: RedisPublisher,
    airport_code: str,
    graph_engine: GraphEngine,
) -> None:
    poll_interval = settings.poll_interval_seconds

    # The worker owns its own pool, separate from the one server.py uses for
    # graph_engine.load_from_db — this loop is the only thing writing to
    # Postgres and can be lifted out into its own process later without
    # touching server.py.
    pool = await create_pool()
    try:
        await ensure_schema(pool)
        last_prune = datetime.now(timezone.utc)
        # Fire immediately on startup so the map is populated right away,
        # then repeat every poll_interval seconds.
        while True:
            try:
                snapshot = await client.get_airport_flights(airport_code)
                for event in snapshot.flights:
                    await publisher.publish(event, airport_code)
                written = await write_events(pool, snapshot.flights)
                print(
                    f"Ingestion: published {len(snapshot.flights)} flights for "
                    f"{airport_code}, persisted {written} to Postgres"
                )
            except Exception as e:
                print(f"Error in ingestion worker: {e}")

            now = datetime.now(timezone.utc)
            if (now - last_prune).total_seconds() >= PRUNE_INTERVAL_SECONDS:
                removed = graph_engine.prune_expired_flights(max_age_hours=PRUNE_MAX_AGE_HOURS)
                if removed:
                    print(
                        f"Pruned {len(removed)} flights landed >{PRUNE_MAX_AGE_HOURS}h "
                        f"ago from the graph"
                    )
                last_prune = now

            await asyncio.sleep(poll_interval)
    finally:
        await pool.close()