import asyncio
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.ingestion.worker import run as worker_run
from flight_tracker.ingestion.publisher import RedisPublisher
from flight_tracker.ingestion.client import MockFlightAwareClient

app = FastAPI()
graph_engine = GraphEngine()
redis_client = aioredis.from_url("redis://localhost")

@app.on_event("startup")
async def startup():
    pool = await aioredis.from_url("redis://localhost")
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
                await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        await pubsub.unsubscribe(f"flights:{airport_code}")