#!/usr/bin/env python3
"""
Performance benchmarks for the four hot paths the Phase I brief asks
about: GraphEngine (BFS propagation), Postgres queries, the Redis
cache-aside layer, and ML prediction latency. Each benchmark runs in
isolation against the real component (real GraphEngine, real local
Postgres/Redis, the real trained model) — no mocks, same convention as
flight_tracker/tests/. Does not require a running flight_tracker server;
it talks to Postgres/Redis/the model file directly.

Run:
  python scripts/benchmark.py graph
  python scripts/benchmark.py db
  python scripts/benchmark.py cache
  python scripts/benchmark.py ml
  python scripts/benchmark.py all
"""
import argparse
import asyncio
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import redis.asyncio as aioredis

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from flight_tracker.cache.redis_cache import CacheLayer  # noqa: E402
from flight_tracker.config import settings  # noqa: E402
from flight_tracker.db import reader as db_reader  # noqa: E402
from flight_tracker.db.writer import create_pool, write_events  # noqa: E402
from flight_tracker.graph.engine import GraphEngine  # noqa: E402
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus  # noqa: E402
from ml.predictor import DelayPredictor  # noqa: E402

MODEL_PATH = str(REPO_ROOT / "ml" / "model.pkl")
RESULTS: dict = {}


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * p
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def stats_ms(latencies_s: list[float]) -> dict:
    return {
        "p50_ms": round(percentile(latencies_s, 0.5) * 1000, 3),
        "p95_ms": round(percentile(latencies_s, 0.95) * 1000, 3),
        "p99_ms": round(percentile(latencies_s, 0.99) * 1000, 3),
        "mean_ms": round(sum(latencies_s) / len(latencies_s) * 1000, 3) if latencies_s else 0.0,
    }


def make_flight(flight_id: str, *, aircraft_id: str | None = None, delay_minutes: int = 0) -> FlightEvent:
    now = datetime.now(timezone.utc)
    return FlightEvent(
        flight_id=flight_id, event_type=EventType.DEPARTURE, airline_code="BM", flight_number=flight_id,
        origin="KJFK", destination="KBOS", aircraft_id=aircraft_id, gate_id=None,
        scheduled_departure=now, scheduled_arrival=now + timedelta(hours=2),
        delay_minutes=delay_minutes, status=FlightStatus.SCHEDULED, timestamp=now,
    )


# --- Benchmark 1: GraphEngine performance -----------------------------------------

def build_graph_fast(n: int, chain: bool = True) -> tuple[GraphEngine, list[str]]:
    """Builds an N-node graph directly (bulk graph mutation, bypassing
    GraphEngine.process_event's own O(V)-per-insert add_edges_for_flight
    scan) so reaching size N is O(N), not O(N^2) — that O(V) production
    cost is exactly what this benchmark measures separately afterward (one
    real process_event() call against the already-built graph), not
    something to also pay N times just to set up the fixture.

    chain=True links every consecutive flight into ONE long aircraft-turn
    chain (0->1->2->...->N-1) — needed so propagate_delay's BFS actually
    has O(N) reachable nodes to traverse; propagate_delay's own loop keeps
    visiting neighbors even after decay rounds to a 0-minute delta (see
    engine.py: it still enqueues them, just skips the update), so a short
    chain would silently cap the traversal at a handful of hops regardless
    of how large N is and under-measure BFS cost."""
    engine = GraphEngine(airport_code="KJFK")
    node_keys: list[str] = []
    for i in range(n):
        flight = make_flight(f"BENCH-{i}")
        engine.graph.add_node(
            flight.flight_key, event=flight, flight_id=flight.flight_id,
            airline_code=flight.airline_code, flight_number=flight.flight_number,
            aircraft_id=flight.aircraft_id, gate_id=flight.gate_id,
            scheduled_departure=flight.scheduled_departure, scheduled_arrival=flight.scheduled_arrival,
            delay_minutes=0, status=flight.status.value,
        )
        node_keys.append(flight.flight_key)
        if chain and i > 0:
            engine.graph.add_edge(node_keys[i - 1], flight.flight_key, type="aircraft_turn")
    return engine, node_keys


def run_graph_benchmark(sizes=(500, 1000, 5000, 10000)) -> list[dict]:
    print(f"=== Benchmark 1: GraphEngine Performance (sizes={list(sizes)}) ===")
    results = []
    for n in sizes:
        engine, node_keys = build_graph_fast(n)

        # Marginal cost of ONE MORE real event at graph size N — this is
        # the actual per-event production cost (add_edges_for_flight +
        # resolve_gate_conflicts), the thing that determines whether
        # steady-state throughput degrades as the graph grows.
        new_flight = make_flight(f"BENCH-MARGINAL-{n}")
        t0 = time.perf_counter()
        engine.process_event(new_flight)
        marginal_event_s = time.perf_counter() - t0

        # BFS propagation from the start of a chain.
        t0 = time.perf_counter()
        updated = engine.propagate_delay(node_keys[0], 100)
        propagate_s = time.perf_counter() - t0

        row = {
            "graph_size": n,
            "actual_nodes": engine.graph.number_of_nodes(),
            "actual_edges": engine.graph.number_of_edges(),
            "marginal_add_event_ms": round(marginal_event_s * 1000, 3),
            "propagate_delay_bfs_ms": round(propagate_s * 1000, 3),
            "flights_updated_by_bfs": len(updated),
        }
        results.append(row)
        print(f"  N={n:>6}: marginal add_event={row['marginal_add_event_ms']:>8.3f}ms  "
              f"BFS propagate={row['propagate_delay_bfs_ms']:>8.3f}ms  updated={row['flights_updated_by_bfs']}")
    return results


# --- Benchmark 2: Database query performance ---------------------------------------

async def run_db_benchmark(sizes=(500, 1000, 5000, 10000)) -> list[dict]:
    print(f"\n=== Benchmark 2: Database Query Performance (sizes={list(sizes)}) ===")
    pool = await create_pool()
    tag = f"BENCHDB-{uuid.uuid4().hex[:6]}"
    results = []
    inserted = 0
    try:
        for n in sizes:
            batch = [make_flight(f"{tag}-{i}") for i in range(inserted, n)]
            await write_events(pool, batch, airport_code=tag)
            inserted = n

            latencies = []
            for _ in range(20):
                t0 = time.perf_counter()
                await db_reader.get_airport_snapshot(pool, tag)
                latencies.append(time.perf_counter() - t0)

            plan_rows = await pool.fetch(
                "EXPLAIN ANALYZE SELECT * FROM active_flights WHERE airport_code = $1 ORDER BY last_updated DESC",
                tag,
            )
            plan_text = "\n".join(r["QUERY PLAN"] for r in plan_rows)
            index_used = "idx_active_flights_airport_code_last_updated" in plan_text

            row = {"n_flights": n, **stats_ms(latencies), "index_used": index_used}
            results.append(row)
            print(f"  N={n:>6}: p50={row['p50_ms']:>7.3f}ms  p95={row['p95_ms']:>7.3f}ms  "
                  f"p99={row['p99_ms']:>7.3f}ms  index_used={index_used}")
    finally:
        await pool.execute("DELETE FROM active_flights WHERE airport_code = $1", tag)
        await pool.execute("DELETE FROM flight_events WHERE flight_id LIKE $1", f"{tag}-%")
        await pool.close()
    return results


# --- Benchmark 3: Cache performance -------------------------------------------------

async def run_cache_benchmark(n_objects: int = 10000, n_lookups: int = 1000) -> dict:
    print(f"\n=== Benchmark 3: Cache Performance (n_objects={n_objects}, n_lookups={n_lookups}) ===")
    pool = await create_pool()
    redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
    cache = CacheLayer(redis_client)
    tag = f"BENCHCACHE-{uuid.uuid4().hex[:6]}"
    flight_ids = [f"{tag}-{i}" for i in range(n_objects)]

    try:
        flights = [make_flight(fid) for fid in flight_ids]
        await write_events(pool, flights, airport_code=tag)

        t0 = time.perf_counter()
        for fid in flight_ids:
            row = await db_reader.get_flight_status(pool, fid)
            await cache.set_cached(cache.key_flight(fid), row, ttl_seconds=300)
        populate_s = time.perf_counter() - t0

        sample_ids = random.sample(flight_ids, n_lookups)

        db_latencies = []
        for fid in sample_ids:
            t0 = time.perf_counter()
            await db_reader.get_flight_status(pool, fid)
            db_latencies.append(time.perf_counter() - t0)

        cache_hits_before = cache.hits
        cache_misses_before = cache.misses
        cache_latencies = []
        for fid in sample_ids:
            t0 = time.perf_counter()
            await cache.get_cached(cache.key_flight(fid))
            cache_latencies.append(time.perf_counter() - t0)
        lookup_hits = cache.hits - cache_hits_before
        lookup_misses = cache.misses - cache_misses_before
        hit_rate = lookup_hits / (lookup_hits + lookup_misses) if (lookup_hits + lookup_misses) else 0.0

        db_mean = sum(db_latencies) / len(db_latencies)
        cache_mean = sum(cache_latencies) / len(cache_latencies)
        speedup_pct = round((db_mean - cache_mean) / db_mean * 100, 1) if db_mean else 0.0

        result = {
            "n_objects_cached": n_objects,
            "n_lookups": n_lookups,
            "populate_time_s": round(populate_s, 2),
            "cache_hit_rate": round(hit_rate, 4),
            "db_latency": stats_ms(db_latencies),
            "cache_latency": stats_ms(cache_latencies),
            "speedup_pct": speedup_pct,
        }
        print(f"  hit_rate={hit_rate:.1%}  DB p50={result['db_latency']['p50_ms']:.3f}ms  "
              f"cache p50={result['cache_latency']['p50_ms']:.3f}ms  speedup={speedup_pct}%")
        return result
    finally:
        await pool.execute("DELETE FROM active_flights WHERE airport_code = $1", tag)
        await pool.execute("DELETE FROM flight_events WHERE flight_id LIKE $1", f"{tag}-%")
        # Best-effort cache cleanup: TTL would expire these anyway (300s),
        # but no reason to leave 10k keys sitting in dev Redis until then.
        keys = [cache.key_flight(fid) for fid in flight_ids]
        if keys:
            await redis_client.delete(*keys)
        await pool.close()
        await redis_client.aclose()


# --- Benchmark 4: ML prediction latency ---------------------------------------------

def run_ml_benchmark(n: int = 1000) -> dict:
    print(f"\n=== Benchmark 4: ML Prediction Latency (n={n}) ===")
    predictor = DelayPredictor(MODEL_PATH)
    carriers = list(predictor.le_carrier.classes_)
    origins = list(predictor.le_origin.classes_)
    dests = list(predictor.le_dest.classes_)

    latencies = []
    for _ in range(n):
        airline = random.choice(carriers)
        origin = random.choice(origins)
        dest = random.choice(dests)
        dep_delay = random.uniform(0, 120)
        air_time = random.uniform(30, 360)
        distance = random.uniform(100, 3000)
        t0 = time.perf_counter()
        predictor.predict(airline, origin, dest, dep_delay, air_time, distance)
        latencies.append(time.perf_counter() - t0)

    result = {"n_predictions": n, **stats_ms(latencies), "target_p95_under_10ms": percentile(latencies, 0.95) * 1000 < 10}
    print(f"  p50={result['p50_ms']:.3f}ms  p95={result['p95_ms']:.3f}ms  p99={result['p99_ms']:.3f}ms  "
          f"target(<10ms p95)={'MET' if result['target_p95_under_10ms'] else 'MISSED'}")
    return result


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("which", choices=["graph", "db", "cache", "ml", "all"], nargs="?", default="all")
    args = parser.parse_args()

    if args.which in ("graph", "all"):
        RESULTS["graph_engine"] = run_graph_benchmark()
    if args.which in ("db", "all"):
        RESULTS["database"] = await run_db_benchmark()
    if args.which in ("cache", "all"):
        RESULTS["cache"] = await run_cache_benchmark()
    if args.which in ("ml", "all"):
        RESULTS["ml_prediction"] = run_ml_benchmark()

    import json
    out_path = REPO_ROOT / "scripts" / "benchmark_results.json"
    out_path.write_text(json.dumps(RESULTS, indent=2))
    print(f"\nResults written to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
