"""
AsyncEventProcessor: the per-event pipeline. One instance per worker (each
worker in worker_pool.py constructs its own, tagged with its worker_id) so
each has its own metrics (events_processed, events_failed, retries_attempted,
latencies_ms) — but all instances share the same underlying resources (DB
pool, graph engine + lock, predictor, producers), since those need to be
shared/coordinated across workers anyway, not duplicated per worker.

Orchestrates, per event: idempotency check -> graph propagation ->
prediction -> DB write -> forward. This absorbs what were two separate
Phase D consumers (ingestion/consumer_runner.py, events/delay_prediction_consumer.py)
into one per-event pipeline — see flight_tracker/workers/CONCURRENCY.md for
why, and for the tradeoffs of doing so.
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

import redis.asyncio as aioredis

from flight_tracker.config import settings
from flight_tracker.db.writer import write_events
from flight_tracker.events.event_model import EventSource, FlightEventEnvelope, wrap_flight_event
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.models.events import FlightEvent
from flight_tracker.workers.retry import retry_with_backoff
from ml.predictor import DelayPredictor


@dataclass
class ProcessResult:
    event: FlightEvent
    predicted_delay_minutes: Optional[float] = None
    propagated: list = field(default_factory=list)
    reassigned: list = field(default_factory=list)
    idempotent_skip: bool = False


class AsyncEventProcessor:
    def __init__(
        self,
        worker_id: str,
        pool,
        redis_client: aioredis.Redis,
        graph_engine: GraphEngine,
        graph_lock: asyncio.Lock,
        predictor: DelayPredictor,
        processed_producer: KafkaEventProducer,
        prediction_producer: KafkaEventProducer,
        airport_code: str,
    ):
        self.worker_id = worker_id
        self._pool = pool
        self._redis = redis_client
        self._graph_engine = graph_engine
        self._graph_lock = graph_lock
        self._predictor = predictor
        self._processed_producer = processed_producer
        self._prediction_producer = prediction_producer
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
            # exact (flight_id, timestamp) pair — skip re-running the graph/
            # ML/DB-write pipeline for it. This is an optimization on top of,
            # not a replacement for, the DB-level guarantee: the
            # UNIQUE(flight_id, captured_at) index on flight_events (see
            # db/writer.py, IDEMPOTENCY.md) is what actually prevents a
            # duplicate row if this cache is ever stale, evicted, or simply
            # never populated (e.g. right after a Redis restart).
            idem_key = self._idempotency_key(event)
            already_processed = await self._redis.get(idem_key)
            if already_processed:
                self.idempotent_skips += 1
                return True, ProcessResult(event=event, idempotent_skip=True), None

            async with self._graph_lock:
                # Graph mutation is not safe for N concurrent workers
                # without this lock — see CONCURRENCY.md, "Concurrency
                # model." Everything below the lock (DB writes, Kafka
                # publishes) is fine to run concurrently across workers;
                # only the graph itself needs to be serialized.
                self._graph_engine.process_event(event)

                propagated: list[FlightEvent] = []
                predicted = None
                if event.delay_minutes > 0:
                    predicted = self._predictor.predict(
                        airline_code=event.airline_code,
                        origin=event.origin,
                        destination=event.destination,
                        dep_delay=event.delay_minutes,
                        air_time=event.estimated_air_time_minutes,
                        distance=event.estimated_distance_miles,
                    )
                    propagated = self._graph_engine.propagate_delay(
                        event.flight_key, event.delay_minutes
                    )

                reassignments = self._graph_engine.resolve_gate_conflicts()
                reassigned = [r["event"] for r in reassignments if r.get("event")]

            await self._write_to_db_retrying(event)

            await self._processed_producer.publish(settings.kafka_topic_processed_flights, envelope)
            for pe in propagated + reassigned:
                await self._prediction_producer.publish(
                    settings.kafka_topic_delay_predictions,
                    wrap_flight_event(pe, source=EventSource.INTERNAL),
                )
            await self._prediction_producer.publish(
                settings.kafka_topic_delay_predictions,
                wrap_flight_event(event, source=envelope.source),
            )

            await self._redis.set(idem_key, "1", ex=settings.worker_idempotency_cache_ttl_seconds)

            self.events_processed += 1
            self.latencies_ms.append((time.perf_counter() - start) * 1000)

            result = ProcessResult(
                event=event,
                predicted_delay_minutes=predicted,
                propagated=propagated,
                reassigned=reassigned,
            )
            return True, result, None
        except Exception as e:
            self.events_failed += 1
            return False, None, e

    async def _write_to_db_once(self, event: FlightEvent) -> None:
        await write_events(self._pool, [event], airport_code=self._airport_code)
