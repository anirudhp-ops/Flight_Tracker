"""
Kafka producer for flight_tracker/events/. Wraps aiokafka's AIOKafkaProducer
with the durability settings an event bus needs that the Redis pub/sub it
replaces never had: acks="all" (don't consider a write successful until
every in-sync replica has it — with replication-factor=1 in this single-
broker dev setup that's really "the one broker has it," but the setting is
what actually matters for a real cluster), idempotent producer (Kafka
dedupes retried sends server-side, so a network blip retry can't double-
publish), capped in-flight requests (required for idempotence to also
guarantee ordering — Kafka's own constraint, not a made-up number).
"""
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from flight_tracker.config import settings
from flight_tracker.events.event_model import FlightEventEnvelope
from flight_tracker.workers.retry import retry_with_backoff


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str | None = None):
        self._bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self._producer: AIOKafkaProducer | None = None
        self.events_sent = 0
        self.send_failures = 0

    async def start(self) -> None:
        # aiokafka 0.14.0's AIOKafkaProducer has no
        # max_in_flight_requests_per_connection parameter (checked against
        # its actual __init__ signature, not assumed from the Java client's
        # API) — it manages in-flight request ordering internally once
        # enable_idempotence=True, rather than exposing the knob. The
        # constraint that motivated it (a real Kafka producer must run
        # in-flight <= 5 for idempotence to also guarantee ordering) is
        # still satisfied; there's just nothing to set here for it.
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            retry_backoff_ms=200,
        )
        await self._producer.start()
        print(f"KafkaEventProducer started (bootstrap_servers={self._bootstrap_servers})")

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            print(
                f"KafkaEventProducer stopped (events_sent={self.events_sent}, "
                f"send_failures={self.send_failures})"
            )

    @retry_with_backoff(exceptions=(KafkaError,))
    async def _send(self, topic: str, value: bytes, key: bytes) -> None:
        await self._producer.send_and_wait(topic, value=value, key=key)

    async def publish(self, topic: str, event: FlightEventEnvelope):
        """
        Publishes `event` to `topic`, keyed by flight_id so every event for
        a given flight lands on the same partition (aiokafka's default
        partitioner hashes the key) — same-flight events stay in order.
        Retries transient producer errors (broker unavailable, timeout) via
        @retry_with_backoff (flight_tracker/workers/retry.py — settings.worker_retry_*),
        the same retry mechanism Phase E's DB writes use, rather than a
        second, differently-tuned bespoke retry loop. Raises if every
        attempt fails. Returns event.event_id on success.
        """
        if self._producer is None:
            raise RuntimeError("KafkaEventProducer.start() must be called before publish()")

        key = event.flight_id.encode("utf-8")
        value = event.to_json().encode("utf-8")

        try:
            await self._send(topic, value, key)
        except KafkaError:
            self.send_failures += 1
            raise
        self.events_sent += 1
        return event.event_id
