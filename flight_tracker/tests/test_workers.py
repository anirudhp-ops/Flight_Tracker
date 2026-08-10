"""
Unit tests for flight_tracker/workers/failure_handler.py (dead-letter
publishing). Runs against the real local Kafka (no mocks) — consistent
with how every other phase of this project has been verified.

Retry logic lives in test_retry.py; idempotency (DB constraint + Redis
cache dedup) lives in test_idempotency.py — both were originally here and
were split out into their own files for Phase I.

Requires: a running Kafka broker with the topics from
scripts/create_kafka_topics.sh already created (this test publishes to
dead-letter-events).

Run: pytest flight_tracker/tests/test_workers.py -v
"""
import time
import uuid

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from flight_tracker.config import settings
from flight_tracker.workers.failure_handler import FailureHandler


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
