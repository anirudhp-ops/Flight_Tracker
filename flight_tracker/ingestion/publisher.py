import redis.asyncio as aioredis
from flight_tracker.models.events import FlightEvent


class RedisPublisher:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    async def publish(self, event: FlightEvent, airport_code: str) -> None:
        channel = f"flights:{airport_code}"
        payload = event.model_dump_json()
        await self.redis.publish(channel, payload)