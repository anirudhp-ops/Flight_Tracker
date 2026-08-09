import asyncio
from pathlib import Path
from uuid import uuid4

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from flight_tracker.cache.redis_cache import CacheLayer
from flight_tracker.config import settings
from flight_tracker.db import reader as db_reader
from flight_tracker.db.writer import create_pool
from flight_tracker.events.dlq_utils import fetch_dlq_events
from flight_tracker.events.event_model import FlightEventEnvelope
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.events import delay_prediction_consumer
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.ingestion import consumer_runner
from flight_tracker.ingestion.worker import run as worker_run
from flight_tracker.ingestion.client import MockFlightAwareClient, FlightAwareClient
from ml.predictor import DelayPredictor

app = FastAPI()

# allow React frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/config")
async def get_config():
    return {"target_airport": settings.target_airport}


@app.get("/health/db")
async def health_db():
    result = {
        "active_flights_rows": None,
        "flight_events_rows": None,
        "redis": "unknown",
        "pool": None,
    }

    if db_pool is not None:
        try:
            async with db_pool.acquire() as conn:
                result["active_flights_rows"] = await conn.fetchval("SELECT count(*) FROM active_flights")
                result["flight_events_rows"] = await conn.fetchval("SELECT count(*) FROM flight_events")
        except Exception as e:
            result["db_error"] = repr(e)
        result["pool"] = {
            "size": db_pool.get_size(),
            "min_size": db_pool.get_min_size(),
            "max_size": db_pool.get_max_size(),
            "idle": db_pool.get_idle_size(),
        }
    else:
        result["db_error"] = "pool not initialized"

    try:
        pong = await redis_client.ping()
        result["redis"] = "ok" if pong else "no response"
    except Exception as e:
        result["redis"] = f"error: {e!r}"

    return result


@app.get("/health/dlq")
async def health_dlq():
    """
    Count of dead-letter-events in the last hour, with a warning flag past
    settings.kafka_dlq_warning_threshold (default 10). Reads the DLQ topic
    directly on every call (see dlq_utils.fetch_dlq_events) rather than
    tracking a running counter — correct and simple at this app's expected
    DLQ volume, not something to point at a high-failure-rate topic as-is.
    """
    events = await fetch_dlq_events(since_hours=1.0)
    count = len(events)
    return {
        "dead_letter_events_last_hour": count,
        "warning_threshold": settings.kafka_dlq_warning_threshold,
        "warning": count > settings.kafka_dlq_warning_threshold,
    }

graph_engine = GraphEngine()
redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
predictor = DelayPredictor(str(Path(__file__).resolve().parent.parent / "ml" / "model.pkl"))
cache = CacheLayer(redis_client)
# Shared by the ingestion worker to publish to flight-events. Kafka replaces
# Redis pub/sub entirely for event streaming as of this phase — redis_client
# above is used for the cache-aside layer only now (get_cached/set_cached).
kafka_producer = KafkaEventProducer()

db_pool = None
worker_task = None
consumer_runner_task = None
prediction_consumer_task = None


@app.get("/api/flights/{flight_id}")
async def get_flight_status(flight_id: str):
    """Cache-aside: current status of one flight. TTL 5 min (settings.cache_flight_ttl_seconds)."""
    async def loader():
        return await db_reader.get_flight_status(db_pool, flight_id)

    result = await cache.get_or_set(cache.key_flight(flight_id), settings.cache_flight_ttl_seconds, loader)
    if result is None:
        raise HTTPException(status_code=404, detail=f"No flight found for flight_id={flight_id}")
    return result


@app.get("/api/airports/{airport_code}/snapshot")
async def get_airport_snapshot(airport_code: str):
    """Cache-aside: all active flights for one airport. TTL 10 min (settings.cache_airport_ttl_seconds)."""
    async def loader():
        return await db_reader.get_airport_snapshot(db_pool, airport_code)

    return await cache.get_or_set(cache.key_airport(airport_code), settings.cache_airport_ttl_seconds, loader)


@app.get("/api/flights/{flight_id}/delays")
async def get_flight_delays(flight_id: str):
    """Cache-aside: recent delay events for one flight. TTL 2 min (settings.cache_delays_ttl_seconds)."""
    async def loader():
        return await db_reader.get_recent_delays(db_pool, flight_id)

    return await cache.get_or_set(cache.key_delays(flight_id), settings.cache_delays_ttl_seconds, loader)


def _crash_logger(name: str):
    """Returns a done-callback that logs loudly if `name`'s task dies from
    an unhandled exception — without this, background tasks fail silently."""
    def _on_done(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            print(f"FATAL: {name} task crashed and is not running anymore: {exc!r}")
    return _on_done


@app.on_event("startup")
async def startup():
    global db_pool, worker_task, consumer_runner_task, prediction_consumer_task
    db_pool = await create_pool()
    await graph_engine.load_from_db(db_pool, airport_code=settings.target_airport)
    await kafka_producer.start()

    if settings.live_api_enabled:
        client = FlightAwareClient(settings.flightaware_api_key)
        print("Ingestion client: real FlightAware API Client started.")
    else:
        client = MockFlightAwareClient(settings.target_airport)
        print("Ingestion client: Mock FlightAware Client started (paid API disabled).")

    # Three independent pipeline stages, each its own task — see
    # flight_tracker/events/KAFKA_ARCHITECTURE.md for the full data flow.
    # Keep references: an unreferenced asyncio.Task is only weakly held by
    # the event loop and can be garbage-collected mid-run.
    worker_task = asyncio.create_task(
        worker_run(client, kafka_producer, settings.target_airport)
    )
    worker_task.add_done_callback(_crash_logger("ingestion worker"))

    consumer_runner_task = asyncio.create_task(consumer_runner.run(settings.target_airport))
    consumer_runner_task.add_done_callback(_crash_logger("flight-processor consumer"))

    prediction_consumer_task = asyncio.create_task(
        delay_prediction_consumer.run(graph_engine, predictor)
    )
    prediction_consumer_task.add_done_callback(_crash_logger("delay-predictor consumer"))


@app.on_event("shutdown")
async def shutdown():
    for task in (worker_task, consumer_runner_task, prediction_consumer_task):
        if task is not None:
            task.cancel()
    for task in (worker_task, consumer_runner_task, prediction_consumer_task):
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass
    await kafka_producer.stop()
    if db_pool is not None:
        await db_pool.close()
    await redis_client.aclose()
    print("Shutdown complete: all Kafka tasks stopped, producer/DB pool/Redis client closed.")


@app.websocket("/ws/{airport_code}")
async def websocket_endpoint(websocket: WebSocket, airport_code: str):
    await websocket.accept()

    # Send all currently active flights in graph_engine to the client upon connection
    for node, attrs in graph_engine.graph.nodes(data=True):
        event_obj = attrs.get("event")
        if event_obj:
            await websocket.send_text(event_obj.model_dump_json())

    # Each connection gets its own throwaway consumer group so every
    # connected browser tab sees the full delay-predictions stream
    # independently — in the same group, multiple consumers *split* the
    # partitions instead of each seeing everything. auto_offset_reset
    # "latest": the graph dump above already represents current state, so a
    # newly-connected client doesn't need (and shouldn't get) a replay of
    # the topic's whole retained history — only what happens from here on.
    group_id = f"{settings.kafka_consumer_group_websocket}-{uuid4()}"
    kafka_consumer = AIOKafkaConsumer(
        settings.kafka_topic_delay_predictions,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await kafka_consumer.start()

    async def watch_for_disconnect():
        # stream_events() below only ever writes to the socket — it never
        # reads from it, so without this, a closed browser tab is invisible
        # to the server until the next send_text() happens to fail.
        # receive_text() raises WebSocketDisconnect the moment the client
        # actually closes.
        while True:
            await websocket.receive_text()

    async def stream_events():
        async for message in kafka_consumer:
            envelope = FlightEventEnvelope.from_json(message.value)
            await websocket.send_text(envelope.flight_event.model_dump_json())

    receiver_task = asyncio.create_task(watch_for_disconnect())
    streamer_task = asyncio.create_task(stream_events())
    try:
        done, _ = await asyncio.wait(
            {receiver_task, streamer_task}, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        for task in (receiver_task, streamer_task):
            if not task.done():
                task.cancel()
        results = await asyncio.gather(receiver_task, streamer_task, return_exceptions=True)
        await kafka_consumer.stop()

    for result in results:
        if isinstance(result, Exception) and not isinstance(
            result, (WebSocketDisconnect, asyncio.CancelledError)
        ):
            print(f"WebSocket handler for {airport_code} exited due to: {result!r}")
