"""
Consumes flight-events, persists each event to Postgres, and forwards it to
processed-flights for the delay-prediction consumer to pick up. This is
where ingestion (worker.py — no DB access at all now) and persistence meet:
a slow or failing DB write backs up this consumer's lag, not the producer
loop pulling from FlightAware/mock data.

"Validate": FlightEventEnvelope.from_json() runs full pydantic validation —
a malformed payload raises here, before write_events() is ever called, and
KafkaEventConsumer routes it to the dead-letter-events topic instead of
this handler swallowing or crashing on it. "Enrich": the envelope's own
event_type/timestamp/source metadata already is the enrichment over a bare
FlightEvent; this handler doesn't invent additional fields with no reader.
"""
import asyncio
from datetime import datetime, timezone

from aiokafka.structs import ConsumerRecord

from flight_tracker.config import settings
from flight_tracker.db.writer import (
    cleanup_stale_active_flights,
    create_pool,
    ensure_schema,
    write_events,
)
from flight_tracker.events.event_model import FlightEventEnvelope
from flight_tracker.events.kafka_consumer import KafkaEventConsumer
from flight_tracker.events.kafka_producer import KafkaEventProducer


async def run(airport_code: str) -> None:
    pool = await create_pool()
    await ensure_schema(pool)

    processed_producer = KafkaEventProducer()
    await processed_producer.start()

    async def handle_message(message: ConsumerRecord) -> None:
        envelope = FlightEventEnvelope.from_json(message.value)
        await write_events(pool, [envelope.flight_event], airport_code=airport_code)
        await processed_producer.publish(settings.kafka_topic_processed_flights, envelope)

    consumer = KafkaEventConsumer(
        topic=settings.kafka_topic_flight_events,
        group_id=settings.kafka_consumer_group_processor,
        handler=handle_message,
    )
    await consumer.start()
    consume_task = asyncio.create_task(consumer.run())

    try:
        last_cleanup = datetime.now(timezone.utc)
        while True:
            await asyncio.sleep(settings.kafka_metrics_log_interval_seconds)

            await consumer.log_metrics()
            lag = await consumer.lag()
            if lag > settings.kafka_consumer_lag_warning_threshold:
                print(
                    f"WARNING: consumer group '{consumer.group_id}' lag is {lag} "
                    f"messages (threshold {settings.kafka_consumer_lag_warning_threshold}) "
                    f"on topic '{consumer.topic}'"
                )

            now = datetime.now(timezone.utc)
            if (now - last_cleanup).total_seconds() >= settings.db_cleanup_interval_seconds:
                deleted = await cleanup_stale_active_flights(
                    pool, max_age_hours=settings.db_cleanup_max_age_hours
                )
                async with pool.acquire() as conn:
                    events_count = await conn.fetchval("SELECT count(*) FROM flight_events")
                print(
                    f"DB cleanup: deleted {deleted} landed flights older than "
                    f"{settings.db_cleanup_max_age_hours}h from active_flights "
                    f"(flight_events now has {events_count} rows, not archived)"
                )
                last_cleanup = now
    except asyncio.CancelledError:
        consume_task.cancel()
        try:
            await consume_task
        except asyncio.CancelledError:
            pass
        await consumer.stop()
        await processed_producer.stop()
        await pool.close()
        raise
