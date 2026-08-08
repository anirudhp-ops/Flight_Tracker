import asyncio
from pathlib import Path

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from flight_tracker.cache.redis_cache import CacheLayer
from flight_tracker.config import settings
from flight_tracker.db import reader as db_reader
from flight_tracker.db.writer import create_pool
from flight_tracker.models.events import FlightEvent
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.ingestion.worker import run as worker_run
from flight_tracker.ingestion.publisher import RedisPublisher
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

graph_engine = GraphEngine()
redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
predictor = DelayPredictor(str(Path(__file__).resolve().parent.parent / "ml" / "model.pkl"))
cache = CacheLayer(redis_client)

db_pool = None
worker_task = None


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


def _on_worker_done(task: asyncio.Task) -> None:
    """Without this, an unhandled exception in the worker loop dies silently."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        print(f"FATAL: ingestion worker task crashed and is not running anymore: {exc!r}")


@app.on_event("startup")
async def startup():
    global db_pool, worker_task
    db_pool = await create_pool()
    await graph_engine.load_from_db(db_pool, airport_code=settings.target_airport)

    if settings.live_api_enabled:
        client = FlightAwareClient(settings.flightaware_api_key)
        print("Ingestion client: real FlightAware API Client started.")
    else:
        client = MockFlightAwareClient(settings.target_airport)
        print("Ingestion client: Mock FlightAware Client started (paid API disabled).")

    publisher = RedisPublisher(redis_client)
    # Keep a reference: an unreferenced asyncio.Task is only weakly held by
    # the event loop and can be garbage-collected mid-run.
    worker_task = asyncio.create_task(
        worker_run(client, publisher, settings.target_airport, graph_engine)
    )
    worker_task.add_done_callback(_on_worker_done)


@app.on_event("shutdown")
async def shutdown():
    if worker_task is not None:
        worker_task.remove_done_callback(_on_worker_done)
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    if db_pool is not None:
        await db_pool.close()
    await redis_client.aclose()
    print("Shutdown complete: worker stopped, DB pool closed, Redis client closed.")


@app.websocket("/ws/{airport_code}")
async def websocket_endpoint(websocket: WebSocket, airport_code: str):
    await websocket.accept()

    # Send all currently active flights in graph_engine to the client upon connection
    for node, attrs in graph_engine.graph.nodes(data=True):
        event_obj = attrs.get("event")
        if event_obj:
            await websocket.send_text(event_obj.model_dump_json())

    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"flights:{airport_code}")

    async def watch_for_disconnect():
        # stream_events() below only ever writes to the socket — it never
        # reads from it, so without this, a closed browser tab is invisible
        # to the server until the next send_text() happens to fail (up to
        # POLL_INTERVAL_SECONDS later, or never, if no more events arrive).
        # receive_text() raises WebSocketDisconnect the moment the client
        # actually closes.
        while True:
            await websocket.receive_text()

    async def stream_events():
        async for message in pubsub.listen():
            if message["type"] == "message":
                event = FlightEvent.model_validate_json(message["data"])

                # Add or update the flight event in the graph engine
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
                    propagated_events = graph_engine.propagate_delay(event.flight_key, event.delay_minutes)
                    for pe in propagated_events:
                        await websocket.send_text(pe.model_dump_json())
                    print(
                        f"{event.flight_key} — predicted arrival delay: {predicted:.1f} min "
                        f"(inputs: dep_delay={event.delay_minutes}min, "
                        f"air_time={air_time:.0f}min, distance={distance:.0f}mi)"
                    )

                # resolve any gate conflicts after propagation
                reassignments = graph_engine.resolve_gate_conflicts()
                if reassignments:
                    print(f"Gate reassignments: {reassignments}")
                    for r in reassignments:
                        updated_event = r.get("event")
                        if updated_event:
                            await websocket.send_text(updated_event.model_dump_json())

                await websocket.send_text(event.model_dump_json())

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
        await pubsub.unsubscribe(f"flights:{airport_code}")

    for result in results:
        if isinstance(result, Exception) and not isinstance(
            result, (WebSocketDisconnect, asyncio.CancelledError)
        ):
            print(f"WebSocket handler for {airport_code} exited due to: {result!r}")
