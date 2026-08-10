"""
Unit tests for flight_tracker/cache/redis_cache.py (CacheLayer). Runs
against the real local Redis (settings.redis_host/redis_port) — consistent
with this project's no-mocks testing convention (see test_workers.py).

Run: pytest flight_tracker/tests/test_cache.py -v
"""
import asyncio
import uuid

import pytest
import redis.asyncio as aioredis

from flight_tracker.cache.redis_cache import CacheLayer
from flight_tracker.config import settings


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
    yield client
    await client.aclose()


@pytest.fixture
def cache(redis_client):
    return CacheLayer(redis_client)


def _unique_key(prefix: str) -> str:
    return f"test:{prefix}:{uuid.uuid4().hex[:8]}"


# --- cache_aside: miss -> fetch -> cache -> hit ---------------------------------

async def test_get_or_set_miss_then_fetch_then_cache_then_hit(cache):
    key = _unique_key("aside")
    calls = {"count": 0}

    async def loader():
        calls["count"] += 1
        return {"flight_id": "AA100", "delay_minutes": 15}

    first = await cache.get_or_set(key, ttl_seconds=60, loader=loader)
    assert first == {"flight_id": "AA100", "delay_minutes": 15}
    assert calls["count"] == 1
    assert cache.misses == 1
    assert cache.hits == 0

    second = await cache.get_or_set(key, ttl_seconds=60, loader=loader)
    assert second == {"flight_id": "AA100", "delay_minutes": 15}
    assert calls["count"] == 1  # loader NOT called again — served from cache
    assert cache.hits == 1


async def test_get_or_set_does_not_cache_none_result(cache):
    key = _unique_key("none-result")
    calls = {"count": 0}

    async def loader():
        calls["count"] += 1
        return None

    result = await cache.get_or_set(key, ttl_seconds=60, loader=loader)
    assert result is None
    assert calls["count"] == 1

    # A second call must miss again (None was never cached) and re-invoke loader.
    result2 = await cache.get_or_set(key, ttl_seconds=60, loader=loader)
    assert result2 is None
    assert calls["count"] == 2


async def test_get_cached_returns_none_on_miss(cache):
    result = await cache.get_cached(_unique_key("nonexistent"))
    assert result is None


async def test_set_then_get_cached_round_trips(cache):
    key = _unique_key("roundtrip")
    await cache.set_cached(key, {"a": 1, "b": [1, 2, 3]}, ttl_seconds=60)
    result = await cache.get_cached(key)
    assert result == {"a": 1, "b": [1, 2, 3]}


async def test_set_cached_serializes_datetime_with_isoformat(cache):
    from datetime import datetime, timezone
    key = _unique_key("datetime")
    dt = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    await cache.set_cached(key, {"ts": dt}, ttl_seconds=60)
    result = await cache.get_cached(key)
    assert result["ts"] == dt.isoformat()


# --- TTL expiry -----------------------------------------------------------------

async def test_ttl_expiry_key_gone_after_ttl_elapses(cache):
    key = _unique_key("ttl")
    await cache.set_cached(key, {"x": 1}, ttl_seconds=1)

    immediate = await cache.get_cached(key)
    assert immediate == {"x": 1}

    await asyncio.sleep(1.5)

    expired = await cache.get_cached(key)
    assert expired is None


# --- cache invalidation -----------------------------------------------------------

async def test_invalidate_removes_key(cache):
    key = _unique_key("invalidate")
    await cache.set_cached(key, {"x": 1}, ttl_seconds=60)
    assert await cache.get_cached(key) == {"x": 1}

    await cache.invalidate(key)

    assert await cache.get_cached(key) is None


async def test_invalidate_nonexistent_key_is_a_noop(cache):
    key = _unique_key("invalidate-missing")
    await cache.invalidate(key)  # must not raise
    assert await cache.get_cached(key) is None


# --- hit_rate ---------------------------------------------------------------------

async def test_hit_rate_zero_with_no_activity(cache):
    assert cache.hit_rate == 0.0


async def test_hit_rate_computed_from_hits_and_misses(cache):
    key = _unique_key("hitrate")
    await cache.get_cached(key)  # miss
    await cache.set_cached(key, {"x": 1}, ttl_seconds=60)
    await cache.get_cached(key)  # hit
    await cache.get_cached(key)  # hit

    assert cache.misses == 1
    assert cache.hits == 2
    assert cache.hit_rate == pytest.approx(2 / 3)


# --- key helpers --------------------------------------------------------------------

def test_key_flight_format():
    assert CacheLayer.key_flight("AA100") == "flights:AA100"


def test_key_airport_format():
    assert CacheLayer.key_airport("KJFK") == "airports:KJFK"


def test_key_delays_format():
    assert CacheLayer.key_delays("AA100") == "delays:AA100"
