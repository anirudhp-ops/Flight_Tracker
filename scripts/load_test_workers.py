#!/usr/bin/env python3
"""
Load test for flight_tracker/workers/ — real Kafka/Postgres/Redis, no
mocks. Publishes a batch of unique flight events, runs a WorkerPool (under
a Supervisor) to drain them, and measures throughput, latency percentiles,
error rate, consumer lag, and this process's CPU/memory.

Runs a 1-worker baseline, then a WORKER_COUNT-worker run, so the two are
directly comparable from the same script, same data shape, same machine.

CPU/memory are sampled via psutil at the OS-process level — all N workers
are asyncio tasks inside this one process (not separate OS processes), so
these numbers describe "the whole process while N workers ran," not any
one worker in isolation. Said plainly here because it's an easy thing to
misreport as per-worker.

Usage: python scripts/load_test_workers.py [--events 1000] [--workers 4]
"""
import argparse
import asyncio
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psutil
import redis.asyncio as aioredis

from flight_tracker.config import settings
from flight_tracker.db.writer import create_pool
from flight_tracker.events.event_model import EventSource, wrap_flight_event
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus
from flight_tracker.workers.supervisor import Supervisor
from flight_tracker.workers.worker_pool import build_worker_pool
from ml.predictor import DelayPredictor


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(len(sorted_values) * p))
    return sorted_values[idx]


async def run_one_load_test(worker_count: int, event_count: int, pool, redis_client) -> dict:
    graph_engine = GraphEngine()
    predictor = DelayPredictor(str((Path(__file__).resolve().parent.parent / "ml" / "model.pkl")))
    processed_producer = KafkaEventProducer()
    prediction_producer = KafkaEventProducer()
    await processed_producer.start()
    await prediction_producer.start()

    worker_pool = build_worker_pool(
        pool=pool, redis_client=redis_client, graph_engine=graph_engine, predictor=predictor,
        processed_producer=processed_producer, prediction_producer=prediction_producer,
        airport_code=settings.target_airport, worker_count=worker_count,
    )

    run_tag = uuid4().hex[:8]
    now = datetime.now(timezone.utc)
    producer = KafkaEventProducer()
    await producer.start()
    envelopes = []
    for i in range(event_count):
        fe = FlightEvent(
            flight_id=f"LOADTEST-{run_tag}-{i}", event_type=EventType.DEPARTURE,
            airline_code="LT", flight_number=str(i), origin=settings.target_airport, destination="KLAX",
            scheduled_departure=now, scheduled_arrival=now,
            delay_minutes=(i % 5) * 3, status=FlightStatus.SCHEDULED, timestamp=now,
        )
        envelopes.append(wrap_flight_event(fe, source=EventSource.MOCK))

    proc = psutil.Process()
    proc.cpu_percent()  # first call always returns 0.0 — primes the interval measurement
    cpu_samples: list[float] = []
    mem_samples_mb: list[float] = []

    t_publish_start = time.perf_counter()
    await asyncio.gather(*(producer.publish(settings.kafka_topic_flight_events, e) for e in envelopes))
    publish_elapsed = time.perf_counter() - t_publish_start
    await producer.stop()

    supervisor = Supervisor(worker_pool)
    await supervisor.start()
    run_task = asyncio.create_task(supervisor.run_forever())

    t_drain_start = time.perf_counter()
    deadline = t_drain_start + 120
    while time.perf_counter() < deadline:
        n = await pool.fetchval(
            "SELECT count(*) FROM active_flights WHERE flight_id LIKE $1", f"LOADTEST-{run_tag}-%"
        )
        cpu_samples.append(proc.cpu_percent())
        mem_samples_mb.append(proc.memory_info().rss / (1024 * 1024))
        if n >= event_count:
            break
        await asyncio.sleep(0.2)
    drain_elapsed = time.perf_counter() - t_drain_start

    final_lag = await worker_pool.total_lag()
    latencies_ms = sorted(worker_pool.all_latencies_ms())
    total_processed = worker_pool.total_events_processed
    total_failed = worker_pool.total_events_failed

    await supervisor.stop()
    run_task.cancel()
    try:
        await run_task
    except asyncio.CancelledError:
        pass
    await processed_producer.stop()
    await prediction_producer.stop()

    return {
        "worker_count": worker_count,
        "event_count": event_count,
        "publish_elapsed_s": publish_elapsed,
        "publish_events_per_sec": event_count / publish_elapsed if publish_elapsed else 0,
        "drain_elapsed_s": drain_elapsed,
        "drain_events_per_sec": total_processed / drain_elapsed if drain_elapsed else 0,
        "total_processed": total_processed,
        "total_failed": total_failed,
        "error_rate": total_failed / (total_processed + total_failed) if (total_processed + total_failed) else 0,
        "final_lag": final_lag,
        "restarts": worker_pool.total_restarts,
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
        "latency_max_ms": max(latencies_ms) if latencies_ms else 0,
        "cpu_percent_avg": statistics.mean(cpu_samples) if cpu_samples else 0,
        "cpu_percent_max": max(cpu_samples) if cpu_samples else 0,
        "mem_mb_avg": statistics.mean(mem_samples_mb) if mem_samples_mb else 0,
        "mem_mb_max": max(mem_samples_mb) if mem_samples_mb else 0,
    }


def print_result(label: str, r: dict) -> None:
    print(f"\n=== {label} ({r['worker_count']} worker(s), {r['event_count']} events) ===")
    print(f"  publish:  {r['publish_events_per_sec']:.1f} events/sec ({r['publish_elapsed_s']:.2f}s, concurrent)")
    print(f"  drain:    {r['drain_events_per_sec']:.1f} events/sec ({r['drain_elapsed_s']:.2f}s)")
    print(f"  processed: {r['total_processed']}, failed/DLQ: {r['total_failed']}, error rate: {r['error_rate']:.2%}")
    print(f"  final consumer lag: {r['final_lag']}, worker restarts: {r['restarts']}")
    print(
        f"  latency: p50={r['latency_p50_ms']:.1f}ms p95={r['latency_p95_ms']:.1f}ms "
        f"p99={r['latency_p99_ms']:.1f}ms max={r['latency_max_ms']:.1f}ms"
    )
    print(
        f"  process CPU: avg={r['cpu_percent_avg']:.1f}% max={r['cpu_percent_max']:.1f}% "
        f"(whole process, not per-worker — see module docstring)"
    )
    print(f"  process RSS: avg={r['mem_mb_avg']:.1f}MB max={r['mem_mb_max']:.1f}MB")


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=settings.worker_count)
    args = parser.parse_args()

    pool = await create_pool()
    redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")

    baseline = await run_one_load_test(1, args.events, pool, redis_client)
    print_result("BASELINE", baseline)

    scaled = await run_one_load_test(args.workers, args.events, pool, redis_client)
    print_result(f"{args.workers}-WORKER", scaled)

    print("\n=== COMPARISON ===")
    speedup = scaled["drain_events_per_sec"] / baseline["drain_events_per_sec"] if baseline["drain_events_per_sec"] else 0
    print(f"  throughput speedup: {speedup:.2f}x ({baseline['drain_events_per_sec']:.1f} -> {scaled['drain_events_per_sec']:.1f} events/sec)")
    print(f"  1000+ events/sec target: {'MET' if scaled['drain_events_per_sec'] >= 1000 else 'NOT MET'} (actual: {scaled['drain_events_per_sec']:.1f})")
    print(f"  p99 < 200ms target: {'MET' if scaled['latency_p99_ms'] < 200 else 'NOT MET'} (actual: {scaled['latency_p99_ms']:.1f}ms)")
    print(f"  <1% error rate target: {'MET' if scaled['error_rate'] < 0.01 else 'NOT MET'} (actual: {scaled['error_rate']:.2%})")

    await pool.close()
    await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
