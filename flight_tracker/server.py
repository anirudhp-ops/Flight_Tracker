import asyncio
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from flight_tracker.models.events import FlightEvent
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.ingestion.worker import run as worker_run
from flight_tracker.ingestion.publisher import RedisPublisher
from flight_tracker.ingestion.client import MockFlightAwareClient
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

graph_engine = GraphEngine()
redis_client = aioredis.from_url("redis://localhost")
predictor = DelayPredictor("/Users/anirudhparasramouria/Desktop/Flight_Tracker/ml/model.pkl")


@app.on_event("startup")
async def startup():
    import asyncpg
    pool = await asyncpg.create_pool(
        database="flight_tracker",
        user="anirudhparasramouria",
        password=None,
        host="localhost",
    )
    await graph_engine.load_from_db(pool)
    client = MockFlightAwareClient()
    publisher = RedisPublisher(redis_client)
    asyncio.create_task(worker_run(client, publisher, "JFK"))


@app.websocket("/ws/{airport_code}")
async def websocket_endpoint(websocket: WebSocket, airport_code: str):
    await websocket.accept()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"flights:{airport_code}")
    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                event = FlightEvent.model_validate_json(message["data"])

                if event.delay_minutes > 0:
                    predicted = predictor.predict(
                        airline_code=event.airline_code,
                        origin=event.origin,
                        destination=event.destination,
                        dep_delay=event.delay_minutes,
                        air_time=0,
                        distance=0,
                    )
                    graph_engine.propagate_delay(event.flight_key, event.delay_minutes)
                    print(f"{event.flight_key} — predicted arrival delay: {predicted:.1f} min")

                # resolve any gate conflicts after propagation
                reassignments = graph_engine.resolve_gate_conflicts()
                if reassignments:
                    print(f"Gate reassignments: {reassignments}")

                await websocket.send_text(message["data"])

    except WebSocketDisconnect:
        await pubsub.unsubscribe(f"flights:{airport_code}")