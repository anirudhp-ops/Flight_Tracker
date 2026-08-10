"""
Unit tests for flight_tracker/events/dlq_utils.py (fetch_dlq_events).
Runs against the real local Kafka — consistent with this project's
no-mocks testing convention. Publishes real failures via FailureHandler
(the same producer path server.py's GET /health/dlq and
scripts/inspect_dlq.py rely on) rather than hand-crafting DLQ payloads.

Run: pytest flight_tracker/tests/test_dlq_utils.py -v
"""
import uuid

import pytest
from aiokafka import AIOKafkaProducer

from flight_tracker.config import settings
from flight_tracker.events.dlq_utils import fetch_dlq_events
from flight_tracker.workers.failure_handler import FailureHandler


@pytest.fixture
async def dlq_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    yield producer
    await producer.stop()


async def test_fetch_dlq_events_finds_a_published_failure(dlq_producer):
    handler = FailureHandler(dlq_producer)
    unique_flight_id = f"DLQUTEST-{uuid.uuid4().hex[:8]}"

    await handler.handle_failure(
        event_id="evt-1", flight_id=unique_flight_id, raw_payload=b'{"x":1}',
        error=RuntimeError("boom"), worker_id="w-0",
        original_topic="flight-events", original_partition=0, original_offset=1,
    )

    events = await fetch_dlq_events(since_hours=1.0)
    matching = [e for e in events if e.get("flight_id") == unique_flight_id]

    assert len(matching) == 1
    assert matching[0]["error_type"] == "RuntimeError"
    assert matching[0]["worker_id"] == "w-0"


async def test_fetch_dlq_events_since_hours_excludes_old_events(dlq_producer):
    """since_hours=0 (an instant cutoff in the past) must exclude every
    event, including one published moments ago."""
    handler = FailureHandler(dlq_producer)
    unique_flight_id = f"DLQUTEST-{uuid.uuid4().hex[:8]}"
    await handler.handle_failure(
        event_id="evt-2", flight_id=unique_flight_id, raw_payload=None,
        error=ValueError("old"), worker_id="w-1",
        original_topic="flight-events", original_partition=0, original_offset=2,
    )

    events = await fetch_dlq_events(since_hours=0.0)
    matching = [e for e in events if e.get("flight_id") == unique_flight_id]
    assert matching == []


async def test_fetch_dlq_events_respects_limit(dlq_producer):
    handler = FailureHandler(dlq_producer)
    tag = uuid.uuid4().hex[:8]
    for i in range(3):
        await handler.handle_failure(
            event_id=f"evt-{tag}-{i}", flight_id=f"DLQUTEST-{tag}-{i}", raw_payload=None,
            error=ValueError(f"err{i}"), worker_id="w-2",
            original_topic="flight-events", original_partition=0, original_offset=i,
        )

    events = await fetch_dlq_events(since_hours=1.0, limit=2)
    assert len(events) <= 2
