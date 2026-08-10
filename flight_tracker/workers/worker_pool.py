"""
WorkerPool: WORKER_COUNT concurrent Worker instances, each an independent
member of the same Kafka consumer group (settings.kafka_consumer_group_worker_pool)
consuming flight-events directly. Kafka's normal consumer-group mechanics
split the topic's partitions across them automatically — with 3 partitions
(scripts/create_kafka_topics.sh) that means at most 3 of these are ever
concurrently active; see CONCURRENCY.md, "Known limitations."

This module owns starting/stopping workers and their Kafka consumers, and
aggregating their metrics. It does NOT run or supervise them — that's
flight_tracker/workers/supervisor.py's job, deliberately kept separate so
"how workers execute" and "what happens when one crashes" aren't tangled
into one class.
"""
import asyncio
import logging

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.abc import ConsumerRebalanceListener

from flight_tracker.config import settings
from flight_tracker.events.event_model import FlightEventEnvelope
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.metrics import events_received
from flight_tracker.workers.event_processor import AsyncEventProcessor
from flight_tracker.workers.failure_handler import FailureHandler

logger = logging.getLogger(__name__)


class _RebalanceLogger(ConsumerRebalanceListener):
    def __init__(self, label: str):
        self._label = label

    async def on_partitions_revoked(self, revoked):
        if revoked:
            print(f"INFO: [{self._label}] partitions revoked (rebalance starting): {list(revoked)}")

    async def on_partitions_assigned(self, assigned):
        if assigned:
            print(f"INFO: [{self._label}] partitions assigned (rebalance complete): {list(assigned)}")


class Worker:
    """One consuming asyncio coroutine (run()) + its own AsyncEventProcessor
    + its own Kafka consumer/DLQ-producer pair. Multiple Workers share the
    same group_id, so Kafka handles partition assignment across them — this
    class doesn't coordinate with its siblings itself."""

    def __init__(self, worker_id: str, processor: AsyncEventProcessor):
        self.worker_id = worker_id
        self.processor = processor
        self.restarts = 0  # bumped by the Supervisor, not this class
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self.failure_handler: FailureHandler | None = None
        self._stopping = False

    async def start(self) -> None:
        self._stopping = False
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group_worker_pool,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._consumer.subscribe(
            topics=[settings.kafka_topic_flight_events],
            listener=_RebalanceLogger(
                f"{settings.kafka_topic_flight_events}/{settings.kafka_consumer_group_worker_pool}/{self.worker_id}"
            ),
        )
        await self._consumer.start()
        self._dlq_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await self._dlq_producer.start()
        self.failure_handler = FailureHandler(self._dlq_producer)
        print(f"Worker {self.worker_id} started")

    async def stop(self) -> None:
        self._stopping = True
        if self._consumer is not None:
            await self._consumer.stop()
        if self._dlq_producer is not None:
            await self._dlq_producer.stop()
        print(
            f"Worker {self.worker_id} stopped "
            f"(processed={self.processor.events_processed}, failed={self.processor.events_failed}, "
            f"idempotent_skips={self.processor.idempotent_skips}, "
            f"retries={self.processor.retries_attempted}, restarts={self.restarts})"
        )

    async def lag(self) -> int:
        if self._consumer is None:
            return 0
        total = 0
        for tp in self._consumer.assignment():
            highwater = self._consumer.highwater(tp)
            if highwater is None:
                continue
            position = await self._consumer.position(tp)
            total += max(0, highwater - position)
        return total

    async def run(self) -> None:
        """
        Consume -> process -> (dead-letter on failure) -> commit, forever,
        until stop() is called or this coroutine's task is cancelled.
        Left to raise on an unexpected error (e.g. the Kafka connection
        itself failing) — flight_tracker/workers/supervisor.py is what
        catches that and restarts this worker; this method doesn't
        self-heal, by design (single responsibility).
        """
        assert self._consumer is not None, "call start() before run()"
        try:
            async for message in self._consumer:
                if self._stopping:
                    break

                events_received.labels(topic=message.topic).inc()

                # --- Backpressure (task 5) ---------------------------------
                current_lag = await self.lag()
                if current_lag > settings.worker_backpressure_lag_threshold:
                    logger.warning(
                        "Consumer lag high, reducing throughput",
                        extra={"worker_id": self.worker_id, "lag": current_lag},
                    )
                    await asyncio.sleep(settings.worker_backpressure_throttle_seconds)
                # lag < worker_backpressure_low_lag_threshold: no throttle —
                # that's already the default fast path for every message;
                # there's no batch size to grow further in a one-message-at-
                # a-time consumer loop (see CONCURRENCY.md, "Backpressure").

                # Parsing lives in this same try/except as processing, not
                # before it: a message that isn't even valid
                # FlightEventEnvelope JSON must dead-letter exactly like a
                # processing failure would, not escape run() entirely. If
                # it escaped here, the Supervisor would restart this worker
                # (worker_pool.py's run() is left to raise on unexpected
                # errors on purpose — see this method's docstring), but the
                # poison message's offset was never committed, so the
                # restarted worker would immediately re-fetch the same
                # unparseable message and crash again — forever, never
                # advancing. Caught this via testing, not assumed correct.
                event_id = None
                flight_id = None
                try:
                    envelope = FlightEventEnvelope.from_json(message.value)
                    event_id = str(envelope.event_id)
                    flight_id = envelope.flight_id
                    success, result, error = await self.processor.process_flight_event(envelope)
                except Exception as parse_error:
                    success, error = False, parse_error

                if not success:
                    await self.failure_handler.handle_failure(
                        event_id=event_id,
                        flight_id=flight_id,
                        raw_payload=message.value,
                        error=error,
                        worker_id=self.worker_id,
                        original_topic=message.topic,
                        original_partition=message.partition,
                        original_offset=message.offset,
                    )
                # Commit either way: success or dead-lettered failure both
                # count as "handled" — don't retry the same message forever.
                await self._consumer.commit()
        except asyncio.CancelledError:
            print(f"Worker {self.worker_id} cancelled, shutting down")
            raise


class WorkerPool:
    def __init__(self, workers: list[Worker]):
        self.workers = workers

    async def start(self) -> None:
        for w in self.workers:
            await w.start()

    async def stop(self) -> None:
        for w in self.workers:
            await w.stop()

    @property
    def total_events_processed(self) -> int:
        return sum(w.processor.events_processed for w in self.workers)

    @property
    def total_events_failed(self) -> int:
        """
        Uses each worker's FailureHandler.errors_processed, not
        processor.events_failed — the latter only counts failures inside
        process_flight_event(), missing envelope-parse failures that never
        reach it (see worker_pool.py's run(), and the malformed-message
        test that caught this undercounting).
        """
        return sum(
            w.failure_handler.errors_processed for w in self.workers if w.failure_handler is not None
        )

    @property
    def total_idempotent_skips(self) -> int:
        return sum(w.processor.idempotent_skips for w in self.workers)

    @property
    def total_retries_attempted(self) -> int:
        return sum(w.processor.retries_attempted for w in self.workers)

    @property
    def total_restarts(self) -> int:
        return sum(w.restarts for w in self.workers)

    async def total_lag(self) -> int:
        lags = [await w.lag() for w in self.workers]
        return sum(lags)

    def all_latencies_ms(self) -> list[float]:
        out: list[float] = []
        for w in self.workers:
            out.extend(w.processor.latencies_ms)
        return out


def build_worker_pool(
    pool,
    redis_client: aioredis.Redis,
    processed_producer: KafkaEventProducer,
    airport_code: str,
    worker_count: int | None = None,
) -> WorkerPool:
    """
    Constructs WORKER_COUNT Workers, each with its own AsyncEventProcessor
    (own metrics) but all sharing one asyncpg pool and the given producer —
    the shared-vs-per-worker split described in event_processor.py's
    docstring. No GraphEngine/lock/predictor here anymore (Phase F): this
    pool is pure persistence, and the graph-owning work moved to
    flight_tracker/workers/delay_propagation_worker.py's single-instance
    DelayPropagationWorker.
    """
    n = worker_count if worker_count is not None else settings.worker_count
    workers = []
    for i in range(n):
        worker_id = f"worker-{i}"
        processor = AsyncEventProcessor(
            worker_id=worker_id,
            pool=pool,
            redis_client=redis_client,
            processed_producer=processed_producer,
            airport_code=airport_code,
        )
        workers.append(Worker(worker_id=worker_id, processor=processor))
    return WorkerPool(workers)
