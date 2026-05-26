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


