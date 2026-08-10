#!/usr/bin/env python3
"""
Kafka-side load tests: Scenario 2 (Event Throughput) and Scenario 3
(Cascade Load) from the Phase I brief — the two scenarios k6 can't run
itself. See scripts/load_test_k6.js's own module docstring for why (no
Kafka producer in vanilla k6, and this app has no HTTP endpoint that
publishes to flight-events to route around that). This script talks
directly to Kafka via aiokafka, the same client this app's own ingestion
path (flight_tracker/ingestion/worker.py) uses.

Targets an already-running flight_tracker server (README.md's normal
`uvicorn flight_tracker.server:app`, or the throwaway instance
scripts/integration_tests.py knows how to start) — same
target-a-live-system convention as scripts/load_test_k6.js. Does not
start or stop the server itself.

Run:
  python scripts/load_test_kafka.py throughput --rate 100 --duration 60
  python scripts/load_test_kafka.py cascade --count 50 --fanout 5
  python scripts/load_test_kafka.py all
"""
import argparse
import asyncio
import json
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from flight_tracker.config import settings  # noqa: E402
from flight_tracker.events.event_model import EventSource, wrap_flight_event  # noqa: E402
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus  # noqa: E402


def make_flight(flight_id: str, *, delay_minutes: int = 0, aircraft_id: str | None = None,
                 timestamp: datetime | None = None) -> FlightEvent:
    now = datetime.now(timezone.utc)
    return FlightEvent(
        flight_id=flight_id, event_type=EventType.DEPARTURE, airline_code="LT", flight_number=flight_id,
        origin="KJFK", destination="KBOS", aircraft_id=aircraft_id, gate_id=None,
        scheduled_departure=now, scheduled_arrival=now + timedelta(hours=2),
        delay_minutes=delay_minutes, status=FlightStatus.SCHEDULED, timestamp=timestamp or now,
    )


async def publish(producer: AIOKafkaProducer, flight: FlightEvent) -> float:
    envelope = wrap_flight_event(flight, source=EventSource.INTERNAL)
    t0 = time.monotonic()
    await producer.send_and_wait(
        settings.kafka_topic_flight_events,
        value=envelope.to_json().encode("utf-8"),
        key=flight.flight_id.encode("utf-8"),
    )
    return time.monotonic() - t0


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * p
    f, c = int(k), min(int(k) + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


async def start_watcher(group_suffix: str) -> AIOKafkaConsumer:
    """Same seek-to-end pattern as scripts/integration_tests.py's
    start_prediction_watcher — must be started BEFORE publishing whatever
    it's meant to observe."""
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_delay_predictions,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"loadtest-{group_suffix}-{uuid.uuid4().hex[:8]}",
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await consumer.start()
    await consumer.seek_to_end()
    return consumer


async def collect(consumer: AIOKafkaConsumer, flight_ids: set[str], timeout_s: float) -> dict[str, float]:
    seen: dict[str, float] = {}
    start = time.monotonic()
    deadline = start + timeout_s
    while time.monotonic() < deadline and len(seen) < len(flight_ids):
        remaining_ms = max(100, int((deadline - time.monotonic()) * 1000))
        batches = await consumer.getmany(timeout_ms=min(1000, remaining_ms))
        for records in batches.values():
            for record in records:
                payload = json.loads(record.value)
                fid = payload.get("flight_id")
                if fid in flight_ids and fid not in seen:
                    seen[fid] = time.monotonic() - start
    return seen


# --- Scenario 2: Event Throughput ------------------------------------------------

async def run_throughput(rate: float, duration_s: float, sample_every: int = 10) -> dict:
    print(f"=== Scenario 2: Event Throughput ({rate} events/sec for {duration_s:.0f}s) ===")
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    watcher = await start_watcher("throughput")

    tag = uuid.uuid4().hex[:8]
    interval = 1.0 / rate
    publish_latencies: list[float] = []
    sample_ids: dict[str, float] = {}
    e2e_latencies: list[float] = []
    published = 0

    async def consume_loop():
        while True:
            batches = await watcher.getmany(timeout_ms=500)
            now = time.monotonic()
            for records in batches.values():
                for record in records:
                    payload = json.loads(record.value)
                    fid = payload.get("flight_id")
                    sent_at = sample_ids.pop(fid, None)
                    if sent_at is not None:
                        e2e_latencies.append(now - sent_at)

    consume_task = asyncio.create_task(consume_loop())

    start = time.monotonic()
    next_tick = start
    deadline = start + duration_s
    while time.monotonic() < deadline:
        flight_id = f"LOADTEST-THRU-{tag}-{published}"
        t0 = time.monotonic()
        lat = await publish(producer, make_flight(flight_id))
        publish_latencies.append(lat)
        if published % sample_every == 0:
            sample_ids[flight_id] = t0
        published += 1
        next_tick += interval
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
    elapsed = time.monotonic() - start

    # Drain window: let in-flight predictions for the last few published
    # (and sampled) events land before tearing down the consumer.
    await asyncio.sleep(min(10.0, max(3.0, duration_s * 0.1)))
    consume_task.cancel()
    try:
        await consume_task
    except asyncio.CancelledError:
        pass
    await watcher.stop()
    await producer.stop()

    result = {
        "target_events_per_sec": rate,
        "duration_s": round(elapsed, 1),
        "events_published": published,
        "actual_events_per_sec": round(published / elapsed, 1),
        "publish_latency_p50_ms": round((percentile(publish_latencies, 0.5) or 0) * 1000, 2),
        "publish_latency_p95_ms": round((percentile(publish_latencies, 0.95) or 0) * 1000, 2),
        "publish_latency_p99_ms": round((percentile(publish_latencies, 0.99) or 0) * 1000, 2),
        "e2e_latency_samples": len(e2e_latencies),
        "e2e_latency_p50_ms": round((percentile(e2e_latencies, 0.5) or 0) * 1000, 2),
        "e2e_latency_p95_ms": round((percentile(e2e_latencies, 0.95) or 0) * 1000, 2),
        "e2e_latency_p99_ms": round((percentile(e2e_latencies, 0.99) or 0) * 1000, 2),
        "e2e_samples_never_seen": len(sample_ids),
    }
    print(json.dumps(result, indent=2))
    return result


# --- Scenario 3: Cascade Load -----------------------------------------------------

async def run_cascade(count: int, fanout: int, setup_timeout_s: float = 30.0,
                       cascade_timeout_s: float = 10.0) -> dict:
    """`count` independent trigger flights, each with its own `fanout`
    aircraft-turn-linked neighbors (so `count`=50, `fanout`=1-2 approximates
    the brief's "1-100 affected flights" per cascade at load-test scale),
    all delayed simultaneously. Setup ordering follows the same
    trigger-confirmed-before-neighbors rule scripts/integration_tests.py's
    cascade scenario established (and documented the race for) — required
    for aircraft_turn edges to end up pointing FROM the trigger, not into
    it; see graph/engine.py's add_edges_for_flight (existing node ->
    new node)."""
    print(f"=== Scenario 3: Cascade Load ({count} simultaneous delays, "
          f"{fanout} affected flights each = {count * fanout} total) ===")
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()

    tag = uuid.uuid4().hex[:8]
    groups = []
    for g in range(count):
        aircraft_id = f"LOADTEST-CAS-AC-{tag}-{g}"
        trigger_id = f"LOADTEST-CAS-TRIGGER-{tag}-{g}"
        neighbor_ids = [f"LOADTEST-CAS-{tag}-{g}-{i}" for i in range(fanout)]
        groups.append((aircraft_id, trigger_id, neighbor_ids))

    # Phase 1: all `count` triggers, confirmed graph-ready before any neighbor publishes.
    watcher = await start_watcher("cascade-triggers")
    for aircraft_id, trigger_id, _ in groups:
        await publish(producer, make_flight(trigger_id, aircraft_id=aircraft_id))
    trigger_ids = {t for _, t, _ in groups}
    trigger_seen = await collect(watcher, trigger_ids, timeout_s=setup_timeout_s)
    await watcher.stop()
    if len(trigger_seen) != len(trigger_ids):
        print(f"  WARNING: only {len(trigger_seen)}/{len(trigger_ids)} triggers "
              f"confirmed graph-ready within {setup_timeout_s}s")

    # Phase 2: all neighbors, confirmed graph-ready before the delayed trigger fires.
    # Each neighbor shares ITS group's aircraft_id (not the trigger's own
    # flight_id) — that shared value is exactly what makes
    # add_edges_for_flight() draw the trigger -> neighbor aircraft_turn edge.
    watcher = await start_watcher("cascade-neighbors")
    for aircraft_id, _, neighbor_ids in groups:
        for nid in neighbor_ids:
            await publish(producer, make_flight(nid, aircraft_id=aircraft_id))
    all_neighbor_ids = {n for _, _, ns in groups for n in ns}
    neighbor_seen = await collect(watcher, all_neighbor_ids, timeout_s=setup_timeout_s)
    await watcher.stop()
    if len(neighbor_seen) != len(all_neighbor_ids):
        print(f"  WARNING: only {len(neighbor_seen)}/{len(all_neighbor_ids)} neighbors "
              f"confirmed graph-ready within {setup_timeout_s}s")

    # Phase 3: fire all `count` delayed triggers simultaneously, measure
    # how long it takes every group's neighbors to show up as propagated.
    watcher = await start_watcher("cascade-fire")
    t0 = time.monotonic()
    await asyncio.gather(*[
        publish(producer, make_flight(trigger_id, delay_minutes=180, aircraft_id=aircraft_id))
        for aircraft_id, trigger_id, _ in groups
    ])
    seen = await collect(watcher, all_neighbor_ids, timeout_s=cascade_timeout_s)
    elapsed = time.monotonic() - t0
    await watcher.stop()
    await producer.stop()

    result = {
        "cascades": count,
        "affected_per_cascade": fanout,
        "total_affected_flights": len(all_neighbor_ids),
        "propagated_predictions_received": len(seen),
        "all_cascades_propagated": len(seen) == len(all_neighbor_ids),
        "total_propagation_time_s": round(elapsed, 3),
    }
    print(json.dumps(result, indent=2))
    return result


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_thru = sub.add_parser("throughput", help="Scenario 2: Event Throughput")
    p_thru.add_argument("--rate", type=float, default=100.0, help="events/sec (default: 100)")
    p_thru.add_argument("--duration", type=float, default=60.0, help="seconds (default: 60; brief specifies 300)")

    p_cas = sub.add_parser("cascade", help="Scenario 3: Cascade Load")
    p_cas.add_argument("--count", type=int, default=50, help="simultaneous cascades (default: 50)")
    p_cas.add_argument("--fanout", type=int, default=2, help="affected flights per cascade (default: 2)")

    sub.add_parser("all", help="Run both scenarios back to back")

    args = parser.parse_args()
    results = {}

    if args.cmd in ("throughput", "all"):
        rate = getattr(args, "rate", 100.0)
        duration = getattr(args, "duration", 60.0)
        results["throughput"] = await run_throughput(rate, duration)

    if args.cmd in ("cascade", "all"):
        count = getattr(args, "count", 50)
        fanout = getattr(args, "fanout", 2)
        results["cascade"] = await run_cascade(count, fanout)

    out_path = REPO_ROOT / "scripts" / "load_test_kafka_results.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nResults written to {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
