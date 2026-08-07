import asyncio
from pathlib import Path

import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from flight_tracker.config import settings
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

graph_engine = GraphEngine()
redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
predictor = DelayPredictor(str(Path(__file__).resolve().parent.parent / "ml" / "model.pkl"))

db_pool = None
worker_task = None


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
    await graph_engine.load_from_db(db_pool)

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
    try:
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

    except WebSocketDisconnect:
        await pubsub.unsubscribe(f"flights:{airport_code}")
