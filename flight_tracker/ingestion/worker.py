import asyncio

from flight_tracker.config import settings
from flight_tracker.events.event_model import wrap_flight_event
from flight_tracker.events.kafka_producer import KafkaEventProducer


async def run(client, producer: KafkaEventProducer, airport_code: str) -> None:
    """
    Fetches flights from `client` (mock or live) and publishes each one to
    the flight-events Kafka topic. That's the whole job now — this worker no
    longer touches Postgres or Redis directly. Persistence, graph
    processing, and delay prediction all happen downstream, in the two
    supervised worker pools server.py starts (flight_tracker/workers/
    worker_pool.py's persistence pool, flight_tracker/workers/
    delay_propagation_worker.py's single-instance DelayPropagationWorker)
    that consume from the topics this worker writes to and forwards to. See
    flight_tracker/events/KAFKA_ARCHITECTURE.md for the full picture.
    """
    poll_interval = settings.poll_interval_seconds

    while True:
        try:
            snapshot = await client.get_airport_flights(airport_code)
            published = 0
            for event in snapshot.flights:
                envelope = wrap_flight_event(event, source=client.source)
                await producer.publish(settings.kafka_topic_flight_events, envelope)
                published += 1
            print(
                f"Ingestion: published {published} flights for {airport_code} "
                f"to Kafka topic '{settings.kafka_topic_flight_events}' "
                f"(source={client.source.value})"
            )
        except Exception as e:
            print(f"Error in ingestion worker: {e}")

        await asyncio.sleep(poll_interval)
