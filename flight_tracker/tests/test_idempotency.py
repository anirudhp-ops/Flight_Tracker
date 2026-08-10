"""
Idempotency tests, at both layers this project uses (see
flight_tracker/workers/event_processor.py's docstring): the Redis
cache-based dedup that skips re-running the DB-write pipeline for an
already-processed (flight_id, timestamp) pair, and the Postgres
UNIQUE(flight_id, captured_at) constraint that's the actual source of
truth if the cache is stale, evicted, or never populated.

Runs against real local Postgres/Redis/Kafka (no mocks) — consistent with
this project's testing convention (see test_workers.py's docstring).

Run: pytest flight_tracker/tests/test_idempotency.py -v
"""
import uuid
from datetime import datetime, timezone

import pytest
import redis.asyncio as aioredis

from flight_tracker.config import settings
from flight_tracker.db.writer import create_pool, write_events
from flight_tracker.events.event_model import EventSource, wrap_flight_event
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus
from flight_tracker.workers.event_processor import AsyncEventProcessor


@pytest.fixture
async def pool():
    p = await create_pool()
    yield p
    await p.close()


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
    yield client
    await client.aclose()


def _flight(unique_id, ts):
    return FlightEvent(
        flight_id=unique_id, event_type=EventType.DEPARTURE, airline_code="UT", flight_number="1",
        origin="KJFK", destination="KLAX", scheduled_departure=ts, scheduled_arrival=ts,
        delay_minutes=0, status=FlightStatus.SCHEDULED, timestamp=ts,
    )


# --- DB-level duplicate detection (same flight_id + timestamp) ------------------

async def test_duplicate_flight_id_and_timestamp_produces_one_db_row(pool):
    unique_id = f"UTEST-{uuid.uuid4().hex[:8]}"
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    event = _flight(unique_id, ts)

    await write_events(pool, [event], airport_code="KJFK")
    await write_events(pool, [event], airport_code="KJFK")  # exact duplicate

    count = await pool.fetchval(
        "SELECT count(*) FROM flight_events WHERE flight_id = $1", unique_id
    )
    assert count == 1


async def test_same_flight_id_different_timestamps_both_recorded(pool):
    """Only exact (flight_id, timestamp) matches are deduplicated — a real
    update to the same flight at a later timestamp must still be recorded."""
    unique_id = f"UTEST-{uuid.uuid4().hex[:8]}"
    ts1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
    await write_events(pool, [_flight(unique_id, ts1)], airport_code="KJFK")
    await write_events(pool, [_flight(unique_id, ts2)], airport_code="KJFK")

    count = await pool.fetchval(
        "SELECT count(*) FROM flight_events WHERE flight_id = $1", unique_id
    )
    assert count == 2


# --- Cache-based dedup in AsyncEventProcessor ------------------------------------

async def test_processor_skips_db_write_on_cache_hit_duplicate(pool, redis_client):
    """Publishing the same envelope twice through AsyncEventProcessor
    (not write_events() directly) must write to Postgres exactly once —
    the second call short-circuits on the Redis idempotency cache before
    ever reaching the DB."""
    producer = KafkaEventProducer()
    await producer.start()
    try:
        processor = AsyncEventProcessor(
            worker_id="test-worker",
            pool=pool,
            redis_client=redis_client,
            processed_producer=producer,
            airport_code="KJFK",
        )
        unique_id = f"UTEST-{uuid.uuid4().hex[:8]}"
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        envelope = wrap_flight_event(_flight(unique_id, ts), source=EventSource.MOCK)

        ok1, result1, err1 = await processor.process_flight_event(envelope)
        ok2, result2, err2 = await processor.process_flight_event(envelope)

        assert ok1 and ok2
        assert err1 is None and err2 is None
        assert result1.idempotent_skip is False
        assert result2.idempotent_skip is True
        assert processor.idempotent_skips == 1
        assert processor.events_processed == 1

        count = await pool.fetchval(
            "SELECT count(*) FROM flight_events WHERE flight_id = $1", unique_id
        )
        assert count == 1
    finally:
        await producer.stop()


async def test_processor_idempotency_cache_key_is_flight_id_and_timestamp_scoped(pool, redis_client):
    """Two envelopes for the same flight_id but different timestamps must
    NOT be treated as duplicates by the cache layer either."""
    producer = KafkaEventProducer()
    await producer.start()
    try:
        processor = AsyncEventProcessor(
            worker_id="test-worker-2",
            pool=pool,
            redis_client=redis_client,
            processed_producer=producer,
            airport_code="KJFK",
        )
        unique_id = f"UTEST-{uuid.uuid4().hex[:8]}"
        ts1 = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc)

        ok1, result1, _ = await processor.process_flight_event(
            wrap_flight_event(_flight(unique_id, ts1), source=EventSource.MOCK)
        )
        ok2, result2, _ = await processor.process_flight_event(
            wrap_flight_event(_flight(unique_id, ts2), source=EventSource.MOCK)
        )

        assert ok1 and ok2
        assert result1.idempotent_skip is False
        assert result2.idempotent_skip is False
        assert processor.idempotent_skips == 0
    finally:
        await producer.stop()
