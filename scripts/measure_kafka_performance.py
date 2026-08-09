#!/usr/bin/env python3
"""
Live measurement of the Kafka pipeline's latency, throughput, and consumer
lag under load, against the real local broker/Postgres/Redis — and a real
(isolated) Redis pub/sub latency baseline for comparison. Requires the full
server (uvicorn flight_tracker.server:app) already running, since latency
is measured end-to-end through the Phase E persistence worker pool + the
Phase F DelayPropagationWorker + a real /ws/{airport} connection, not
simulated.

Usage: python scripts/measure_kafka_performance.py
"""
import asyncio
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis.asyncio as aioredis
import websockets

from flight_tracker.config import settings
from flight_tracker.events.event_model import EventSource, wrap_flight_event
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus

WS_URL = "ws://127.0.0.1:8000/ws/KJFK"
LATENCY_SAMPLE_SIZE = 30
THROUGHPUT_BURST_SIZE = 1000


async def redis_pubsub_baseline() -> float:
    """
    Isolated Redis pub/sub round-trip latency: publish -> subscriber
    receives. This is NOT a full replica of the old Phase B/C app (that
    code path no longer exists after this phase's rewrite) — it measures
    only the pub/sub hop itself, as an honest baseline for "what a single
    in-memory bus hop costs" to compare against Kafka's multi-topic,
    disk-backed pipeline below. Returns median latency in ms.
    """
    r = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
    pubsub = r.pubsub()
    channel = f"latency-test-{uuid4()}"
    await pubsub.subscribe(channel)
    await pubsub.get_message(timeout=1)  # discard the subscribe confirmation

    latencies_ms = []
    for _ in range(LATENCY_SAMPLE_SIZE):
        sent_at = time.perf_counter()
        await r.publish(channel, str(sent_at).encode())
        while True:
            msg = await pubsub.get_message(timeout=1, ignore_subscribe_messages=True)
            if msg is not None:
                break
        received_at = time.perf_counter()
        latencies_ms.append((received_at - sent_at) * 1000)

    await pubsub.unsubscribe(channel)
    await r.aclose()
    return statistics.median(latencies_ms)


async def kafka_pipeline_latency() -> list[float]:
    """
    Real end-to-end latency: publishes individually-timestamped flight
    events (bypassing the mock ingestion client so each has a precise
    creation time), then reads them off the actual /ws/KJFK WebSocket —
    the same endpoint the frontend uses — measuring wall-clock time from
    "event created" to "received over the WebSocket." Spans the full
    pipeline: flight-events -> Phase E worker pool (DB write) -> processed-
    flights -> DelayPropagationWorker (graph + ML) -> delay-predictions ->
    this WebSocket connection.
    """
    producer = KafkaEventProducer()
    await producer.start()

    created_at: dict[str, float] = {}
    flight_ids = [f"LATENCY-{uuid4().hex[:8]}" for _ in range(LATENCY_SAMPLE_SIZE)]

    async with websockets.connect(WS_URL) as ws:
        # Drain the initial graph-state dump before starting the timed run.
        # The dump is sent as one sequential message per graph node BEFORE
        # the server even starts this connection's Kafka consumer — with a
        # few hundred nodes that can take a few seconds, so this needs a
        # generous quiet-period, not a short poll: too short and the timed
        # publishes below race the server's consumer startup and are
        # published to a topic position "latest" hasn't been set to yet
        # (auto_offset_reset="latest" — see server.py), making them
        # invisible to this connection by design, not lost.
        drained = 0
        try:
            while True:
                await asyncio.wait_for(ws.recv(), timeout=3.0)
                drained += 1
        except asyncio.TimeoutError:
            pass
        print(f"(drained {drained} initial-dump messages before starting the timed run)")
        # Extra margin: quiescence on the WebSocket stream only proves the
        # graph dump finished, not that this connection's own Kafka
        # consumer has completed its group rebalance and has a valid
        # "latest" position on all 3 delay-predictions partitions yet.
        await asyncio.sleep(2)

        for fid in flight_ids:
            now = datetime.now(timezone.utc)
            fe = FlightEvent(
                flight_id=fid, event_type=EventType.DELAY, airline_code="LT", flight_number="1",
                origin="KJFK", destination="KLAX", scheduled_departure=now, scheduled_arrival=now,
                delay_minutes=10, status=FlightStatus.ACTIVE, timestamp=now,
            )
            created_at[fid] = time.perf_counter()
            await producer.publish(
                settings.kafka_topic_flight_events, wrap_flight_event(fe, source=EventSource.MOCK)
            )

        latencies_ms = []
        deadline = time.perf_counter() + 30
        remaining = set(flight_ids)
        while remaining and time.perf_counter() < deadline:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=2)
            except asyncio.TimeoutError:
                continue
            import json
            data = json.loads(msg)
            fid = data.get("flight_id")
            if fid in remaining:
                latencies_ms.append((time.perf_counter() - created_at[fid]) * 1000)
                remaining.discard(fid)

    await producer.stop()
    if remaining:
        print(f"WARNING: {len(remaining)}/{LATENCY_SAMPLE_SIZE} events never arrived within 30s: {remaining}")
    return latencies_ms


async def sample_consumer_group_lag(group_id: str) -> int:
    """
    Real lag for the actual running consumer group, via the same CLI Kafka
    ships (kafka-consumer-groups --describe) — reading group metadata as an
    outside observer, not by joining the group (which would steal
    partitions from the real consumer and invalidate the measurement).
    """
    proc = await asyncio.create_subprocess_exec(
        "kafka-consumer-groups", "--bootstrap-server", settings.kafka_bootstrap_servers,
        "--describe", "--group", group_id,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    total_lag = 0
    for line in stdout.decode().splitlines():
        parts = line.split()
        # Columns: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        # Header/status lines don't match this shape; skip them.
        if len(parts) >= 6 and parts[0] == group_id:
            try:
                total_lag += int(parts[5])
            except ValueError:
                pass
    return total_lag


async def throughput_and_lag() -> None:
    """
    Publishes a burst of THROUGHPUT_BURST_SIZE events *concurrently*
    (asyncio.gather, not one-at-a-time awaits) to find the pipeline's real
    throughput ceiling rather than the artificial one imposed by awaiting
    each acks="all" round-trip sequentially before starting the next. Then
    tight-polls (no fixed sleep) the DB row count for those events — what
    the Phase E worker pool's persistence handler writes right before
    committing — to find the real drain time at sub-10ms resolution, and
    samples the worker pool's actual consumer group
    (settings.kafka_consumer_group_worker_pool, "event-processor-pool")
    lag right after the burst completes, to see whether it actually backed
    up under load.
    """
    from flight_tracker.db.writer import create_pool

    producer = KafkaEventProducer()
    await producer.start()
    pool = await create_pool()

    envelopes = []
    for i in range(THROUGHPUT_BURST_SIZE):
        now = datetime.now(timezone.utc)
        fe = FlightEvent(
            flight_id=f"THRPUT-{uuid4().hex[:8]}", event_type=EventType.DEPARTURE,
            airline_code="TP", flight_number=str(i), origin="KJFK", destination="KLAX",
            scheduled_departure=now, scheduled_arrival=now, delay_minutes=0,
            status=FlightStatus.SCHEDULED, timestamp=now,
        )
        envelopes.append(wrap_flight_event(fe, source=EventSource.MOCK))

    t0 = time.perf_counter()
    await asyncio.gather(
        *(producer.publish(settings.kafka_topic_flight_events, env) for env in envelopes)
    )
    publish_elapsed = time.perf_counter() - t0
    lag_right_after_burst = await sample_consumer_group_lag(settings.kafka_consumer_group_worker_pool)
    await producer.stop()
    print(
        f"Published {THROUGHPUT_BURST_SIZE} events concurrently in {publish_elapsed:.3f}s "
        f"({THROUGHPUT_BURST_SIZE / publish_elapsed:.1f} events/sec produced)"
    )
    print(f"{settings.kafka_consumer_group_worker_pool} lag immediately after burst completed: {lag_right_after_burst} messages")

    t0 = time.perf_counter()
    deadline = t0 + 30
    processed = 0
    while time.perf_counter() < deadline:
        processed = await pool.fetchval(
            "SELECT count(*) FROM active_flights WHERE flight_id LIKE 'THRPUT-%'"
        )
        if processed >= THROUGHPUT_BURST_SIZE:
            break
        await asyncio.sleep(0.01)
    drain_elapsed = time.perf_counter() - t0
    final_lag = await sample_consumer_group_lag(settings.kafka_consumer_group_worker_pool)
    await pool.close()

    print(
        f"{settings.kafka_consumer_group_worker_pool} drained {processed}/{THROUGHPUT_BURST_SIZE} burst events "
        f"in {drain_elapsed:.3f}s ({processed / drain_elapsed:.1f} events/sec consumed+written)"
    )
    print(f"{settings.kafka_consumer_group_worker_pool} lag after drain: {final_lag} messages (0 = fully caught up)")


async def main():
    print("=" * 72)
    print("1. Redis pub/sub baseline latency (isolated hop, not the old app)")
    print("=" * 72)
    redis_latency = await redis_pubsub_baseline()
    print(f"Redis pub/sub round-trip median latency: {redis_latency:.3f} ms\n")

    print("=" * 72)
    print("2. Kafka pipeline end-to-end latency (event created -> WebSocket)")
    print("=" * 72)
    latencies = await kafka_pipeline_latency()
    if latencies:
        print(f"Samples: {len(latencies)}/{LATENCY_SAMPLE_SIZE}")
        print(f"  min:    {min(latencies):.1f} ms")
        print(f"  median: {statistics.median(latencies):.1f} ms")
        print(f"  mean:   {statistics.mean(latencies):.1f} ms")
        print(f"  max:    {max(latencies):.1f} ms")
        under_500 = sum(1 for l in latencies if l < 500)
        print(f"  under 500ms target: {under_500}/{len(latencies)}")
    else:
        print("No samples received — is the server running (uvicorn flight_tracker.server:app)?")
    print()

    print("=" * 72)
    print("3. Throughput + consumer catch-up under a burst")
    print("=" * 72)
    await throughput_and_lag()


if __name__ == "__main__":
    asyncio.run(main())
