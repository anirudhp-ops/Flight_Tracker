"""Shared DLQ-reading logic for scripts/inspect_dlq.py and GET /health/dlq."""
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from aiokafka import AIOKafkaConsumer

from flight_tracker.config import settings

POLL_TIMEOUT_MS = 3000


async def fetch_dlq_events(since_hours: float | None = None, limit: int | None = None) -> list[dict]:
    """
    Reads every message currently on dead-letter-events (optionally only
    those newer than `since_hours` ago), using a fresh, never-reused
    consumer group each call so this never resumes from — or advances —
    any other reader's committed offset. Fine for this app's expected DLQ
    volume (low, by design); re-scanning the whole topic on every call
    would not scale to a high-failure-rate topic, which is a known
    limitation worth knowing before reusing this for something busier.
    """
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_dead_letter,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"dlq-reader-{uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=since_hours)
        if since_hours is not None
        else None
    )
    events = []
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=POLL_TIMEOUT_MS)
            if not batches:
                break
            for records in batches.values():
                for record in records:
                    try:
                        payload = json.loads(record.value)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    if cutoff is not None:
                        failed_at = payload.get("failed_at")
                        if failed_at and datetime.fromisoformat(failed_at) < cutoff:
                            continue
                    events.append(payload)
    finally:
        await consumer.stop()

    events.sort(key=lambda p: p.get("failed_at", ""))
    if limit is not None:
        events = events[-limit:]
    return events
