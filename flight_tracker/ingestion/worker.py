import os
import asyncio
from flight_tracker.ingestion.publisher import RedisPublisher


async def run(client, publisher: RedisPublisher, airport_code: str) -> None:
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
    while True:
        try:
            snapshot = await client.get_airport_flights(airport_code)
            for event in snapshot.flights:
                await publisher.publish(event, airport_code)
        except Exception as e:
            print(f"Error in ingestion worker: {e}")
        await asyncio.sleep(poll_interval)