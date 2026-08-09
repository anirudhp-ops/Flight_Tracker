"""
Generic Kafka consumer wrapper. Subscribes to one topic under one consumer
group, runs each message through a caller-supplied async handler, and
commits the offset only after the handler returns successfully — "commit
offsets AFTER processing, not before" from the phase brief, and the
foundation of the at-least-once delivery this app relies on (see
flight_tracker/events/IDEMPOTENCY.md).

Both concrete consumers in this app (ingestion/consumer_runner.py's
flight-processor, events/delay_prediction_consumer.py's delay-predictor)
are thin: they each provide a handler function and let this class own the
loop/commit/DLQ/shutdown/lag mechanics, so that machinery exists in exactly
one place.
"""
import asyncio
import json
import traceback
from datetime import datetime, timezone
from typing import Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.abc import ConsumerRebalanceListener
from aiokafka.errors import KafkaError
from aiokafka.structs import ConsumerRecord

from flight_tracker.config import settings

MessageHandler = Callable[[ConsumerRecord], Awaitable[None]]


class _RebalanceLogger(ConsumerRebalanceListener):
    """Kafka moves partitions between consumers in a group whenever
    membership changes (a consumer joins/leaves/crashes) — visible here as
    "INFO when partition rebalancing occurs" from the phase brief."""

    def __init__(self, label: str):
        self._label = label

    async def on_partitions_revoked(self, revoked):
        if revoked:
            print(f"INFO: [{self._label}] partitions revoked (rebalance starting): {list(revoked)}")

    async def on_partitions_assigned(self, assigned):
        if assigned:
            print(f"INFO: [{self._label}] partitions assigned (rebalance complete): {list(assigned)}")


class KafkaEventConsumer:
    def __init__(
        self,
        topic: str,
        group_id: str,
        handler: MessageHandler,
        bootstrap_servers: str | None = None,
    ):
        self.topic = topic
        self.group_id = group_id
        self._handler = handler
        self._bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._stopping = False

        self.events_received = 0
        self.events_processed = 0
        self.dlq_events = 0

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,  # commit only after the handler succeeds
            auto_offset_reset="earliest",
        )
        # subscribe() with a listener instead of passing the topic to the
        # constructor — the constructor form has no rebalance-callback hook.
        self._consumer.subscribe(
            topics=[self.topic], listener=_RebalanceLogger(f"{self.topic}/{self.group_id}")
        )
        await self._consumer.start()
        # A separate raw producer for DLQ writes, not the typed
        # KafkaEventProducer the rest of the app uses — a failed message's
        # DLQ record wraps arbitrary bytes plus error metadata, not a
        # FlightEventEnvelope, so it doesn't fit that producer's publish()
        # contract.
        self._dlq_producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await self._dlq_producer.start()
        print(f"KafkaEventConsumer started: topic={self.topic} group_id={self.group_id}")

    async def stop(self) -> None:
        self._stopping = True
        if self._consumer is not None:
            await self._consumer.stop()
        if self._dlq_producer is not None:
            await self._dlq_producer.stop()
        print(
            f"KafkaEventConsumer stopped: topic={self.topic} group_id={self.group_id} "
            f"(received={self.events_received}, processed={self.events_processed}, "
            f"dlq={self.dlq_events})"
        )

    async def run(self) -> None:
        """
        Runs until stop() is called or this coroutine's task is cancelled.
        CancelledError is caught, not swallowed — re-raised after logging so
        the caller's task-cancellation flow (server.py's shutdown handler)
        still sees it complete, but only after whatever message was
        in-flight has been committed or DLQ'd first: "graceful shutdown:
        finish in-flight messages before exiting" from the phase brief.
        """
        assert self._consumer is not None, "call start() before run()"
        try:
            async for message in self._consumer:
                if self._stopping:
                    break
                self.events_received += 1
                try:
                    await self._handler(message)
                    await self._consumer.commit()
                    self.events_processed += 1
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    await self._send_to_dlq(message, e)
                    # Commit even on failure: this message has been "handled"
                    # (as a failure, routed to the DLQ) — not committing
                    # would make the consumer re-read and re-DLQ the same
                    # poison message forever on every restart.
                    await self._consumer.commit()
        except asyncio.CancelledError:
            print(f"KafkaEventConsumer for {self.topic}/{self.group_id} cancelled, shutting down")
            raise

    async def _send_to_dlq(self, message: ConsumerRecord, error: Exception) -> None:
        self.dlq_events += 1
        payload = {
            "original_topic": message.topic,
            "original_partition": message.partition,
            "original_offset": message.offset,
            "key": message.key.decode("utf-8", errors="replace") if message.key else None,
            "value": message.value.decode("utf-8", errors="replace") if message.value else None,
            "error": repr(error),
            "error_type": type(error).__name__,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "consumer_group": self.group_id,
        }
        print(
            f"KafkaEventConsumer: routing message from "
            f"{message.topic}[{message.partition}]@{message.offset} to dead-letter-events: {error!r}"
        )
        try:
            await self._dlq_producer.send_and_wait(
                settings.kafka_topic_dead_letter,
                value=json.dumps(payload).encode("utf-8"),
                key=message.key,
            )
        except KafkaError as dlq_error:
            # Nothing left to hand this off to — at least make it visible.
            print(f"KafkaEventConsumer: FAILED to publish to dead-letter-events: {dlq_error!r}")

    async def lag(self) -> int:
        """
        Total lag (unconsumed messages) across this consumer's currently
        assigned partitions: highwater mark (latest offset in the log) minus
        this consumer's current position (next offset it will read).
        """
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

    async def log_metrics(self) -> None:
        """
        kafka_events_received (by topic), kafka_events_processed (by
        consumer group), kafka_consumer_lag, dlq_events — the counters this
        phase asks for, printed on a timer (see settings.kafka_metrics_log_interval_seconds)
        rather than exported anywhere yet. A real metrics backend (Phase J,
        per the brief) would scrape/push these instead of grepping logs.
        """
        current_lag = await self.lag()
        print(
            f"METRICS [{self.topic}/{self.group_id}] "
            f"kafka_events_received={self.events_received} "
            f"kafka_events_processed={self.events_processed} "
            f"kafka_consumer_lag={current_lag} "
            f"dlq_events={self.dlq_events}"
        )
