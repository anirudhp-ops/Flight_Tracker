#!/usr/bin/env python3
"""
Live measurement of DB-only vs. cache-aside query performance, run against
the real local Postgres + Redis instances (not mocked). Findings get written
to flight_tracker/db/PERFORMANCE.md by hand after reviewing this script's
output — this script only measures and prints, it doesn't write the doc, so
the numbers there can be checked against a fresh run.

Usage: python scripts/measure_db_performance.py
"""
import asyncio
import sys
import time
from pathlib import Path

# Runnable directly (`python scripts/measure_db_performance.py`) without
# needing PYTHONPATH set — flight_tracker/ lives at the repo root, one
# level up from this script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis.asyncio as aioredis

from flight_tracker.cache.redis_cache import CacheLayer
from flight_tracker.config import settings
from flight_tracker.db import reader as db_reader
from flight_tracker.db.writer import create_pool

AIRPORT = settings.target_airport
SAMPLE_FLIGHT_LIMIT = 500
FLIGHT_EVENTS_QUERIES = 100


async def time_call(coro):
    t0 = time.perf_counter()
    result = await coro
    t1 = time.perf_counter()
    return (t1 - t0) * 1000, result


async def main():
    pool = await create_pool()
    redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
    cache = CacheLayer(redis_client)

    async with pool.acquire() as conn:
        total_rows = await conn.fetchval(
            "SELECT count(*) FROM active_flights WHERE airport_code = $1", AIRPORT
        )
        sample_flight_id = await conn.fetchval(
            "SELECT flight_id FROM active_flights WHERE airport_code = $1 LIMIT 1", AIRPORT
        )

    # Clear any cache state left over from a previous run of this script (or
    # the live server) so "cache MISS" below is a genuine first-touch miss.
    await cache.invalidate(cache.key_airport(AIRPORT))
    if sample_flight_id:
        await cache.invalidate(cache.key_delays(sample_flight_id))

    sample_size = min(total_rows, SAMPLE_FLIGHT_LIMIT)
    print(f"active_flights rows for {AIRPORT}: {total_rows} (sampling {sample_size})")
    print(f"sample flight_id for per-flight benchmarks: {sample_flight_id}\n")

    if sample_flight_id is None:
        print("No flights in active_flights — run the backend first to seed data. Aborting.")
        await pool.close()
        await redis_client.aclose()
        return

    print("=" * 72)
    print(f"BASELINE — direct Postgres queries, no cache")
    print("=" * 72)

    async def load_sample():
        async with pool.acquire() as conn:
            return await conn.fetch(
                "SELECT * FROM active_flights WHERE airport_code = $1 "
                "ORDER BY last_updated DESC LIMIT $2",
                AIRPORT, SAMPLE_FLIGHT_LIMIT,
            )

    baseline_load_ms, rows = await time_call(load_sample())
    print(f"Load {len(rows)} active flights (direct DB query): {baseline_load_ms:.2f} ms")

    t0 = time.perf_counter()
    for _ in range(FLIGHT_EVENTS_QUERIES):
        await db_reader.get_recent_delays(pool, sample_flight_id)
    baseline_events_total_ms = (time.perf_counter() - t0) * 1000
    print(
        f"flight_events query x{FLIGHT_EVENTS_QUERIES} (direct DB, no cache): "
        f"{baseline_events_total_ms:.2f} ms total, "
        f"{baseline_events_total_ms / FLIGHT_EVENTS_QUERIES:.3f} ms avg"
    )

    print()
    print("=" * 72)
    print("WITH CACHE — cache-aside via CacheLayer")
    print("=" * 72)

    async def load_snapshot_cached():
        return await cache.get_or_set(
            cache.key_airport(AIRPORT),
            settings.cache_airport_ttl_seconds,
            lambda: db_reader.get_airport_snapshot(pool, AIRPORT),
        )

    miss_ms, _ = await time_call(load_snapshot_cached())
    print(f"Airport snapshot, call 1 (cache MISS, populates cache): {miss_ms:.2f} ms")

    hit_times_ms = []
    for _ in range(9):
        ms, _ = await time_call(load_snapshot_cached())
        hit_times_ms.append(ms)
    avg_hit_ms = sum(hit_times_ms) / len(hit_times_ms)
    print(f"Airport snapshot, calls 2-10 (cache HIT): {avg_hit_ms:.3f} ms avg over {len(hit_times_ms)} calls")

    async def get_delays_cached():
        return await cache.get_or_set(
            cache.key_delays(sample_flight_id),
            settings.cache_delays_ttl_seconds,
            lambda: db_reader.get_recent_delays(pool, sample_flight_id),
        )

    t0 = time.perf_counter()
    for _ in range(FLIGHT_EVENTS_QUERIES):
        await get_delays_cached()
    cached_events_total_ms = (time.perf_counter() - t0) * 1000
    print(
        f"flight_events query x{FLIGHT_EVENTS_QUERIES} (cache-aside, 1 miss + "
        f"{FLIGHT_EVENTS_QUERIES - 1} hits): {cached_events_total_ms:.2f} ms total, "
        f"{cached_events_total_ms / FLIGHT_EVENTS_QUERIES:.3f} ms avg"
    )

    print()
    print("=" * 72)
    print("SUMMARY")
    print("=" * 72)
    print(f"Cache hit rate this run: {cache.hit_rate:.1%} ({cache.hits} hits / {cache.misses} misses)")
    if avg_hit_ms:
        print(
            f"Airport snapshot: cache HIT is {miss_ms / avg_hit_ms:.1f}x faster than the "
            f"cache MISS / direct DB query ({avg_hit_ms:.3f} ms vs {miss_ms:.2f} ms)"
        )
    if cached_events_total_ms:
        print(
            f"flight_events x{FLIGHT_EVENTS_QUERIES}: cache-aside total is "
            f"{baseline_events_total_ms / cached_events_total_ms:.1f}x faster than DB-only "
            f"({cached_events_total_ms:.2f} ms vs {baseline_events_total_ms:.2f} ms)"
        )

    await pool.close()
    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
