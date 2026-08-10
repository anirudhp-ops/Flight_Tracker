"""
AsyncEventProcessor: the per-event pipeline. One instance per worker (each
worker in worker_pool.py constructs its own, tagged with its worker_id) so
each has its own metrics (events_processed, events_failed, retries_attempted,
latencies_ms) — but all instances share the same underlying resources (DB
pool, forward producer), since those need to be shared/coordinated across
workers anyway, not duplicated per worker.

Orchestrates, per event: idempotency check -> DB write -> forward to
processed-flights. Graph mutation, delay propagation, gate-conflict
resolution, and ML prediction used to live here too (absorbed from Phase
D's two separate consumers — ingestion/consumer_runner.py,
events/delay_prediction_consumer.py); Phase F moved all of that out to
flight_tracker/workers/delay_propagation_worker.py's single-instance
DelayPropagationWorker, since GraphEngine is process-wide, in-memory,
non-shardable state (see flight_tracker/graph/STATE_MANAGEMENT.md) that N
concurrent workers sharing this pipeline could never safely mutate — the
asyncio.Lock this class used to hold around every graph mutation was a
correctness patch for that mismatch, not a real fix; removing the graph
from this pipeline removes the need for the lock entirely, rather than
papering over it further. This pipeline is now pure persistence: nothing
here needs coordinating across workers beyond what the DB pool and Kafka
producer already provide.
"""
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import redis.asyncio as aioredis

from flight_tracker.config import settings
from flight_tracker.db.writer import write_events
from flight_tracker.events.event_model import FlightEventEnvelope
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.metrics import event_processing_latency, events_failed, events_processed
from flight_tracker.models.events import FlightEvent
from flight_tracker.workers.retry import retry_with_backoff

logger = logging.getLogger(__name__)


@dataclass
class ProcessResult:
    event: FlightEvent
    idempotent_skip: bool = False


class AsyncEventProcessor:
    def __init__(
        self,
        worker_id: str,
        pool,
        redis_client: aioredis.Redis,
        processed_producer: KafkaEventProducer,
        airport_code: str,
    ):
        self.worker_id = worker_id
        self._pool = pool
        self._redis = redis_client
        self._processed_producer = processed_producer
        self._airport_code = airport_code

        # Per-worker metrics (task 8) — deliberately per-instance, not
        # shared/aggregated here; worker_pool.py sums across all N
        # processors when it needs a pool-wide total.
        self.events_processed = 0
        self.events_failed = 0
        self.idempotent_skips = 0
        self.retries_attempted = 0
        self.latencies_ms: list[float] = []

        self._write_to_db_retrying = retry_with_backoff(on_retry=self._bump_retries)(
            self._write_to_db_once
        )

    def _bump_retries(self) -> None:
        self.retries_attempted += 1

    @staticmethod
    def _idempotency_key(event: FlightEvent) -> str:
        return f"processed:{event.flight_id}:{event.timestamp.isoformat()}"

    async def process_flight_event(
        self, envelope: FlightEventEnvelope
    ) -> tuple[bool, Optional[ProcessResult], Optional[BaseException]]:
        """
        Never raises — returns (success, result, error) so the caller
        (worker_pool.py) branches on the tuple rather than try/except, per
        the phase spec. A returned error still means "this specific event
        failed"; the worker pool decides what to do about it (dead-letter
        it, per flight_tracker/workers/failure_handler.py).
        """
        start = time.perf_counter()
        try:
            event = envelope.flight_event

            # --- Idempotency check (task 3) ---------------------------------
            # A cache HIT means some worker already fully processed this
            # exact (flight_id, timestamp) pair — skip re-running the
            # DB-write pipeline for it. This is an optimization on top of,
            # not a replacement for, the DB-level guarantee: the
            # UNIQUE(flight_id, captured_at) index on flight_events (see
            # db/writer.py, IDEMPOTENCY.md) is what actually prevents a
            # duplicate row if this cache is ever stale, evicted, or simply
            # never populated (e.g. right after a Redis restart).
            idem_key = self._idempotency_key(event)
            already_processed = await self._redis.get(idem_key)
            if already_processed:
                self.idempotent_skips += 1
                logger.info(
                    "Idempotent skip",
                    extra={
                        "request_id": str(envelope.event_id),
                        "flight_id": event.flight_id,
                        "worker_id": self.worker_id,
                    },
                )
                return True, ProcessResult(event=event, idempotent_skip=True), None

            await self._write_to_db_retrying(event)
            await self._processed_producer.publish(settings.kafka_topic_processed_flights, envelope)

            await self._redis.set(idem_key, "1", ex=settings.worker_idempotency_cache_ttl_seconds)

            elapsed_seconds = time.perf_counter() - start
            self.events_processed += 1
            self.latencies_ms.append(elapsed_seconds * 1000)
            events_processed.labels(worker_id=self.worker_id).inc()
            event_processing_latency.observe(elapsed_seconds)
            logger.info(
                "Event processed",
                extra={
                    "request_id": str(envelope.event_id),
                    "flight_id": event.flight_id,
                    "worker_id": self.worker_id,
                    "latency_ms": round(elapsed_seconds * 1000, 3),
                },
            )

            return True, ProcessResult(event=event), None
        except Exception as e:
            self.events_failed += 1
            events_failed.labels(reason=type(e).__name__).inc()
            logger.error(
                "Event processing failed",
                extra={
                    "request_id": str(envelope.event_id),
                    "flight_id": envelope.flight_id,
                    "worker_id": self.worker_id,
                },
                exc_info=e,
            )
            return False, None, e

    async def _write_to_db_once(self, event: FlightEvent) -> None:
        await write_events(self._pool, [event], airport_code=self._airport_code)
