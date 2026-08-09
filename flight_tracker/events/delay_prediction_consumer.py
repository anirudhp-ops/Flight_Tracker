"""
Consumes processed-flights, feeds each event into the shared GraphEngine
(delay propagation, gate-conflict resolution), runs the ML predictor when a
flight is delayed, and publishes results to delay-predictions for the
WebSocket handler to stream out. This is the same graph/prediction logic
that used to run inline inside server.py's websocket_endpoint on every
Redis pub/sub message — moved here so it runs once per event regardless of
how many browser tabs are connected, instead of once per connection.
"""
import asyncio
from datetime import datetime, timezone

from aiokafka.structs import ConsumerRecord

from flight_tracker.config import settings
from flight_tracker.events.event_model import EventSource, FlightEventEnvelope, wrap_flight_event
from flight_tracker.events.kafka_consumer import KafkaEventConsumer
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.graph.engine import GraphEngine
from ml.predictor import DelayPredictor

PRUNE_INTERVAL_SECONDS = 600  # how often to actually check for expired flights (10 min)
PRUNE_MAX_AGE_HOURS = 24


async def run(graph_engine: GraphEngine, predictor: DelayPredictor) -> None:
    predictions_producer = KafkaEventProducer()
    await predictions_producer.start()

    async def publish_prediction(event, source: EventSource) -> None:
        await predictions_producer.publish(
            settings.kafka_topic_delay_predictions, wrap_flight_event(event, source=source)
        )

    async def handle_message(message: ConsumerRecord) -> None:
        envelope = FlightEventEnvelope.from_json(message.value)
        event = envelope.flight_event

        graph_engine.process_event(event)

        if event.delay_minutes > 0:
            air_time = event.estimated_air_time_minutes
            distance = event.estimated_distance_miles
            predicted = predictor.predict(
                airline_code=event.airline_code,
                origin=event.origin,
                destination=event.destination,
                dep_delay=event.delay_minutes,
                air_time=air_time,
                distance=distance,
            )
            print(
                f"{event.flight_key} — predicted arrival delay: {predicted:.1f} min "
                f"(inputs: dep_delay={event.delay_minutes}min, "
                f"air_time={air_time:.0f}min, distance={distance:.0f}mi)"
            )
            propagated_events = graph_engine.propagate_delay(event.flight_key, event.delay_minutes)
            for pe in propagated_events:
                await publish_prediction(pe, EventSource.INTERNAL)

        reassignments = graph_engine.resolve_gate_conflicts()
        if reassignments:
            print(f"Gate reassignments: {reassignments}")
            for r in reassignments:
                updated_event = r.get("event")
                if updated_event:
                    await publish_prediction(updated_event, EventSource.INTERNAL)

        # The triggering event itself, last — matches the order the old
        # inline websocket handler sent messages in (propagated/reassigned
        # events, then the event that caused them).
        await publish_prediction(event, envelope.source)

    consumer = KafkaEventConsumer(
        topic=settings.kafka_topic_processed_flights,
        group_id=settings.kafka_consumer_group_predictor,
        handler=handle_message,
    )
    await consumer.start()
    consume_task = asyncio.create_task(consumer.run())

    try:
        last_prune = datetime.now(timezone.utc)
        while True:
            await asyncio.sleep(settings.kafka_metrics_log_interval_seconds)
            await consumer.log_metrics()
            now = datetime.now(timezone.utc)
            if (now - last_prune).total_seconds() >= PRUNE_INTERVAL_SECONDS:
                removed = graph_engine.prune_expired_flights(max_age_hours=PRUNE_MAX_AGE_HOURS)
                if removed:
                    print(
                        f"Pruned {len(removed)} flights landed >{PRUNE_MAX_AGE_HOURS}h "
                        f"ago from the graph"
                    )
                last_prune = now
    except asyncio.CancelledError:
        consume_task.cancel()
        try:
            await consume_task
        except asyncio.CancelledError:
            pass
        await consumer.stop()
        await predictions_producer.stop()
        raise
