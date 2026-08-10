"""
Cache-aside layer over Redis for read-heavy Postgres queries.

Cache-aside, not write-through: callers check the cache first, fall back to
Postgres on a miss, and populate the cache with what they found. Redis is
never the source of truth here — it can be flushed at any time with no data
loss, only a temporary latency hit while entries refill. Staleness is
bounded by each key's TTL, not by invalidating on every write (see
flight_tracker/db/DATABASE_DESIGN.md for why that tradeoff was made).
"""
import json
import logging
from typing import Any, Awaitable, Callable, Optional

import redis.asyncio as aioredis

from flight_tracker.metrics import cache_hits, cache_misses

logger = logging.getLogger(__name__)


class CacheLayer:
    def __init__(self, redis_client: aioredis.Redis):
        self._redis = redis_client
        self.hits = 0
        self.misses = 0

    async def get_cached(self, key: str) -> Optional[Any]:
        raw = await self._redis.get(key)
        if raw is None:
            self.misses += 1
            cache_misses.inc()
            logger.debug(f"Cache miss for {key}", extra={"hit_rate": round(self.hit_rate, 3)})
            return None
        self.hits += 1
        cache_hits.inc()
        logger.debug(f"Cache hit for {key}", extra={"hit_rate": round(self.hit_rate, 3)})
        return json.loads(raw)

    async def set_cached(self, key: str, value: Any, ttl_seconds: int) -> None:
        # isoformat() for datetimes to match FastAPI's own default encoder
        # (plain str() on a datetime uses a space separator, not "T" —
        # would make a cache hit and a cache miss serialize the same field
        # differently). Falls back to str() for anything else non-JSON-safe
        # (UUID, Decimal, ...) from an asyncpg Record.
        def default(o):
            return o.isoformat() if hasattr(o, "isoformat") else str(o)

        await self._redis.set(key, json.dumps(value, default=default), ex=ttl_seconds)

    async def invalidate(self, key: str) -> None:
        await self._redis.delete(key)

    async def get_or_set(
        self, key: str, ttl_seconds: int, loader: Callable[[], Awaitable[Any]]
    ) -> Any:
        """The standard cache-aside sequence in one call: check cache, on
        miss call loader() (the DB query), cache a non-None result, return it."""
        cached = await self.get_cached(key)
        if cached is not None:
            return cached
        value = await loader()
        if value is not None:
            await self.set_cached(key, value, ttl_seconds)
        return value

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0

    @staticmethod
    def key_flight(flight_id: str) -> str:
        return f"flights:{flight_id}"

    @staticmethod
    def key_airport(airport_code: str) -> str:
        return f"airports:{airport_code}"

    @staticmethod
    def key_delays(flight_id: str) -> str:
        return f"delays:{flight_id}"
