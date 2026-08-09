"""
On a worker's processing failure: log loudly, publish full context to
dead-letter-events, and let the caller (worker_pool.py) commit the offset
anyway — retrying forever isn't the goal, moving on is (see
flight_tracker/workers/CONCURRENCY.md, "Failure handling"). This mirrors
flight_tracker/events/kafka_consumer.py's _send_to_dlq but as a standalone
class the worker pool owns, with worker_id/event_id attached — identity
that only exists at the worker-pool level, not the generic single-consumer
level Phase D's version operates at.
"""
import json
import traceback
from datetime import datetime, timezone
from typing import Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from flight_tracker.config import settings


class FailureHandler:
    def __init__(self, dlq_producer: AIOKafkaProducer):
        self._dlq_producer = dlq_producer
        self.errors_processed = 0

    async def handle_failure(
        self,
        *,
        event_id: Optional[str],
        flight_id: Optional[str],
        raw_payload: Optional[bytes],
        error: BaseException,
        worker_id: str,
        original_topic: str,
        original_partition: int,
        original_offset: int,
    ) -> None:
        self.errors_processed += 1
        failed_at = datetime.now(timezone.utc).isoformat()
        print(
            f"WORKER FAILURE [{worker_id}] event_id={event_id} flight_id={flight_id} "
            f"error={error!r} at {failed_at} "
            f"(source: {original_topic}[{original_partition}]@{original_offset})"
        )
        payload = {
            "event_id": event_id,
            "flight_id": flight_id,
            "value": raw_payload.decode("utf-8", errors="replace") if raw_payload else None,
            "error": repr(error),
            "error_type": type(error).__name__,
            "traceback": traceback.format_exc(),
            "failed_at": failed_at,
            "worker_id": worker_id,
            "original_topic": original_topic,
            "original_partition": original_partition,
            "original_offset": original_offset,
        }
        try:
            await self._dlq_producer.send_and_wait(
                settings.kafka_topic_dead_letter,
                value=json.dumps(payload).encode("utf-8"),
                key=flight_id.encode("utf-8") if flight_id else None,
            )
        except KafkaError as dlq_error:
            # Nothing left to hand this off to — at least make it visible.
            print(f"FailureHandler: FAILED to publish to dead-letter-events: {dlq_error!r}")
