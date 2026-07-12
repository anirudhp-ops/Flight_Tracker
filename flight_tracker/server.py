import os
import asyncio
import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from flight_tracker.models.events import FlightEvent
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.ingestion.worker import run as worker_run
from flight_tracker.ingestion.publisher import RedisPublisher
from flight_tracker.ingestion.client import MockFlightAwareClient, FlightAwareClient
from ml.predictor import DelayPredictor

load_dotenv()

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
    return {"target_airport": os.getenv("TARGET_AIRPORT", "KJFK")}

graph_engine = GraphEngine()
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", "6379"))
redis_client = aioredis.from_url(f"redis://{redis_host}:{redis_port}")
predictor = DelayPredictor(os.path.join(os.path.dirname(__file__), "../ml/model.pkl"))


@app.on_event("startup")
async def startup():
    import asyncpg
    pool = await asyncpg.create_pool(
        database=os.getenv("DB_NAME", "flight_tracker"),
        user=os.getenv("DB_USER", "anirudhparasramouria"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
    )
    await graph_engine.load_from_db(pool)
    airport = os.getenv("TARGET_AIRPORT", "KJFK")
    api_key = os.getenv("FLIGHTAWARE_API_KEY")
    live_api_enabled = os.getenv("ENABLE_FLIGHTAWARE_API", "false").lower() in {
        "1", "true", "yes", "on"
    }
    
    if live_api_enabled and api_key and api_key.strip() != "" and api_key != "YOUR_API_KEY":
        client = FlightAwareClient(api_key)
        print("Ingestion client: real FlightAware API Client started.")
    else:
        client = MockFlightAwareClient(airport)
        print("Ingestion client: Mock FlightAware Client started (paid API disabled).")

    publisher = RedisPublisher(redis_client)
    asyncio.create_task(worker_run(client, publisher, airport))


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
                    predicted = predictor.predict(
                        airline_code=event.airline_code,
                        origin=event.origin,
                        destination=event.destination,
                        dep_delay=event.delay_minutes,
                        air_time=0,
                        distance=0,
                    )
                    propagated_events = graph_engine.propagate_delay(event.flight_key, event.delay_minutes)
                    for pe in propagated_events:
                        await websocket.send_text(pe.model_dump_json())
                    print(f"{event.flight_key} — predicted arrival delay: {predicted:.1f} min")

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
