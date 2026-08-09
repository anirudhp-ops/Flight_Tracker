"""
Unit tests for flight_tracker/workers/. These run against the real local
Postgres/Redis/Kafka (no mocks) — consistent with how every other phase of
this project has been verified; a mocked DB write wouldn't tell us whether
the actual ON CONFLICT clause behaves as documented.

Requires: local Postgres reachable via config.py's settings, local Redis,
and a running Kafka broker with the topics from scripts/create_kafka_topics.sh
already created (the DLQ test publishes to dead-letter-events).

Run: pytest flight_tracker/tests/test_workers.py -v
"""
import asyncio
import time
import uuid
from datetime import datetime, timezone

import pytest
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from flight_tracker.config import settings
from flight_tracker.db.writer import create_pool, write_events
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus
from flight_tracker.workers.failure_handler import FailureHandler
from flight_tracker.workers.retry import retry_with_backoff


# --- Retry logic -------------------------------------------------------------

async def test_retry_succeeds_after_transient_failures():
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=3, initial_delay_ms=10, backoff_factor=2.0)
    async def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionError(f"transient failure {calls['count']}")
        return "recovered"

    result = await flaky()
    assert result == "recovered"
    assert calls["count"] == 3


async def test_retry_exhausts_and_raises_last_error():
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=3, initial_delay_ms=10)
    async def always_fails():
        calls["count"] += 1
        raise ValueError(f"permanent failure {calls['count']}")

    with pytest.raises(ValueError, match="permanent failure 3"):
        await always_fails()
    assert calls["count"] == 3  # exactly max_attempts, no more, no fewer


async def test_retry_backoff_grows_exponentially():
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=3, initial_delay_ms=50, backoff_factor=2.0, jitter_fraction=0.0)
    async def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionError("fail")
        return "ok"

    t0 = time.perf_counter()
    await flaky()
    elapsed = time.perf_counter() - t0
    # no jitter (jitter_fraction=0): delays are exactly 50ms then 100ms = 150ms
    assert 0.13 < elapsed < 0.25, f"expected ~0.15s of backoff delay, got {elapsed:.3f}s"


async def test_retry_on_retry_callback_fires_once_per_retry():
    retries = {"count": 0}
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=4, initial_delay_ms=5, on_retry=lambda: retries.__setitem__("count", retries["count"] + 1))
    async def flaky():
        calls["count"] += 1
        if calls["count"] < 4:
            raise ConnectionError("fail")
        return "ok"

    await flaky()
    assert retries["count"] == 3  # 4 attempts, 3 retries between them


# --- Idempotency ---------------------------------------------------------------

@pytest.fixture
async def pool():
    p = await create_pool()
    yield p
    await p.close()


async def test_idempotency_db_constraint_prevents_duplicate_flight_events(pool):
    """Publishing the exact same (flight_id, timestamp) event twice through
    write_events() must produce exactly one flight_events row — the
    UNIQUE(flight_id, captured_at) index + ON CONFLICT DO NOTHING guard."""
    unique_id = f"UTEST-{uuid.uuid4().hex[:8]}"
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    event = FlightEvent(
        flight_id=unique_id, event_type=EventType.DEPARTURE, airline_code="UT", flight_number="1",
        origin="KJFK", destination="KLAX", scheduled_departure=ts, scheduled_arrival=ts,
        delay_minutes=0, status=FlightStatus.SCHEDULED, timestamp=ts,
    )

    await write_events(pool, [event], airport_code="KJFK")
    await write_events(pool, [event], airport_code="KJFK")  # exact duplicate

    count = await pool.fetchval(
        "SELECT count(*) FROM flight_events WHERE flight_id = $1", unique_id
    )
    assert count == 1


async def test_idempotency_different_timestamps_both_recorded(pool):
    """Sanity check on the other side of the same guard: two DIFFERENT
    events for the same flight_id (different timestamps) must NOT be
    deduplicated — only exact (flight_id, timestamp) matches are."""
    unique_id = f"UTEST-{uuid.uuid4().hex[:8]}"
    ts1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
    for ts in (ts1, ts2):
        event = FlightEvent(
            flight_id=unique_id, event_type=EventType.DELAY, airline_code="UT", flight_number="2",
            origin="KJFK", destination="KLAX", scheduled_departure=ts1, scheduled_arrival=ts1,
            delay_minutes=5, status=FlightStatus.ACTIVE, timestamp=ts,
        )
        await write_events(pool, [event], airport_code="KJFK")

    count = await pool.fetchval(
        "SELECT count(*) FROM flight_events WHERE flight_id = $1", unique_id
    )
    assert count == 2


# --- Dead-letter on processing error --------------------------------------------

async def test_failure_handler_publishes_full_metadata_to_dlq():
    dlq_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await dlq_producer.start()
    handler = FailureHandler(dlq_producer)

    unique_flight_id = f"DLQTEST-{uuid.uuid4().hex[:8]}"
    forced_error = ValueError("forced test failure")

    await handler.handle_failure(
        event_id="test-event-id-123",
        flight_id=unique_flight_id,
        raw_payload=b'{"flight_id": "test"}',
        error=forced_error,
        worker_id="test-worker-0",
        original_topic="flight-events",
        original_partition=0,
        original_offset=999,
    )
    await dlq_producer.stop()

    assert handler.errors_processed == 1

    # Read it back from the real topic and check the metadata landed correctly.
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_dead_letter,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"dlq-test-reader-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    import json
    found = None
    try:
        deadline = time.perf_counter() + 15
        while time.perf_counter() < deadline:
            batches = await consumer.getmany(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    payload = json.loads(record.value)
                    if payload.get("flight_id") == unique_flight_id:
                        found = payload
                        break
            if found:
                break
    finally:
        await consumer.stop()

    assert found is not None, "DLQ record was never published/found"
    assert found["error_type"] == "ValueError"
    assert "forced test failure" in found["error"]
    assert found["worker_id"] == "test-worker-0"
    assert found["original_topic"] == "flight-events"
    assert found["original_offset"] == 999
