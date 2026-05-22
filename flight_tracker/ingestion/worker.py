import asyncio
from flight_tracker.ingestion.client import MockFlightAwareClient
from flight_tracker.ingestion.publisher import RedisPublisher


async def run(client: MockFlightAwareClient, publisher: RedisPublisher, airport_code: str) -> None:
    while True:
        snapshot = await client.get_airport_flights(airport_code)
        for event in snapshot.flights:
            await publisher.publish(event, airport_code)
        await asyncio.sleep(60)