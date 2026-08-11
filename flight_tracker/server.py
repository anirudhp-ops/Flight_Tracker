import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from flight_tracker.cache.redis_cache import CacheLayer
from flight_tracker.config import settings
from flight_tracker.db import reader as db_reader
from flight_tracker.db.writer import create_pool, ensure_schema
from flight_tracker.events.dlq_utils import fetch_dlq_events
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.ingestion.worker import run as worker_run
from flight_tracker.ingestion.client import MockFlightAwareClient, FlightAwareClient
from flight_tracker.logging_config import configure_logging
from flight_tracker.metrics import active_websocket_connections, websocket_message_latency
from flight_tracker.middleware.request_id import add_request_id
from flight_tracker.models.prediction_event import PredictionEvent
from flight_tracker.websocket.messages import (
    WSMessageType,
    classify_prediction_event,
    heartbeat_message,
    snapshot_message,
)
from flight_tracker.workers.delay_propagation_worker import build_delay_propagation_worker
from flight_tracker.workers.supervisor import Supervisor
from flight_tracker.workers.worker_pool import WorkerPool, build_worker_pool
from ml.predictor import DelayPredictor

# Before any logger.*() call below (or in any module this one imports and
# whose code path runs at request time) — attaches the JSON-formatting
# handler to the root logger once, at process import time. See
# flight_tracker/logging_config.py.
configure_logging()
logger = logging.getLogger(__name__)
SERVER_START_TIME = time.monotonic()

WS_HEARTBEAT_INTERVAL_SECONDS = 30
WS_HISTORY_MAX_MESSAGES = 100

app = FastAPI()

# allow React frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.middleware("http")(add_request_id)


@app.get("/metrics")
async def metrics():
    """Prometheus scrape target — see prometheus.yml's flight-backend job."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

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


@app.get("/health")
async def health():
    """
    Aggregated liveness/metrics snapshot (Phase J task 7) — a single call a
    dashboard or uptime check can hit, unlike /health/db and /health/dlq
    above (kept as-is: more detail than this summary needs on every poll).
    events_per_second is a cumulative average since process start, not a
    trailing window — the periodic per-second number lives in Prometheus
    (rate(events_processed_total[1m]) via /metrics), which is the right
    tool for a real rate; this endpoint just needs to say "alive and
    roughly how busy," not replace Grafana.
    """
    services = {"database": "unknown", "redis": "unknown", "kafka": "unknown"}

    if db_pool is not None:
        try:
            async with db_pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            services["database"] = "ok"
        except Exception as e:
            services["database"] = f"error: {e!r}"
    else:
        services["database"] = "not initialized"

    try:
        services["redis"] = "ok" if await redis_client.ping() else "no response"
    except Exception as e:
        services["redis"] = f"error: {e!r}"

    services["kafka"] = "ok" if kafka_producer._producer is not None else "not started"

    processed = 0
    failed = 0
    latencies_ms: list[float] = []
    consumer_lag = 0
    for sup in (supervisor, delay_propagation_supervisor):
        if sup is None:
            continue
        processed += sup.pool.total_events_processed
        failed += sup.pool.total_events_failed
        latencies_ms.extend(sup.pool.all_latencies_ms())
        consumer_lag += await sup.pool.total_lag()

    elapsed = max(time.monotonic() - SERVER_START_TIME, 1e-9)
    total = processed + failed

    return {
        "status": "healthy" if all(v == "ok" for v in services.values()) else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": services,
        "metrics": {
            "events_per_second": round(processed / elapsed, 2),
            "avg_latency_ms": round(sum(latencies_ms) / len(latencies_ms), 2) if latencies_ms else 0.0,
            "error_rate": round(failed / total, 4) if total else 0.0,
            "consumer_lag": consumer_lag,
        },
    }


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

graph_engine = GraphEngine(airport_code=settings.target_airport)
redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
predictor = DelayPredictor(str(Path(__file__).resolve().parent.parent / "ml" / "model.pkl"))
cache = CacheLayer(redis_client)
# Shared by the ingestion worker to publish to flight-events. Kafka replaces
# Redis pub/sub entirely for event streaming as of this phase — redis_client
# above is used for the cache-aside layer only now (get_cached/set_cached).
kafka_producer = KafkaEventProducer()

db_pool = None
worker_task = None
# Two independent, separately-supervised pipelines consume flight-events'
# descendants — see flight_tracker/workers/CONCURRENCY.md and
# flight_tracker/graph/STATE_MANAGEMENT.md for why they're split this way
# rather than combined into one pipeline (as Phase D's single-consumer
# ingestion/consumer_runner.py + events/delay_prediction_consumer.py did;
# both files are deleted as of Phase F, fully superseded):
#   - `supervisor`: Phase E's WORKER_COUNT concurrent Workers, persistence
#     only (idempotency check -> DB write -> forward to processed-flights).
#   - `delay_propagation_supervisor`: Phase F's single-instance
#     DelayPropagationWorker, the only thing that owns/mutates graph_engine
#     (processed-flights -> graph mutation/propagation/gate-conflicts/ML
#     prediction -> delay-predictions). Single-instance because GraphEngine
#     is process-wide in-memory state that can't be safely sharded across
#     concurrent consumers — see delay_propagation_worker.py's docstring.
supervisor: Supervisor | None = None
metrics_log_task = None
delay_propagation_supervisor: Supervisor | None = None
delay_propagation_metrics_log_task = None

# Shared across all /ws connections (this app tracks exactly one airport —
# see settings.target_airport — so one buffer, not one per airport, is
# correct here; multi-airport support is explicitly out of Phase G's scope).
# A late-connecting client gets this replayed right after its SNAPSHOT, so
# it doesn't miss whatever happened in the gap between "server started
# streaming" and "this client's WebSocket handshake completed" — the
# "server keeps event history for late-connecting clients" option from the
# Phase G brief (the alternative it offered, client-requested replay, would
# need its own request/response sub-protocol for no real benefit over just
# always replaying a bounded, small history on connect).
recent_ws_messages: deque = deque(maxlen=WS_HISTORY_MAX_MESSAGES)


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
            logger.critical(f"{name} task crashed and is not running anymore", exc_info=exc)
    return _on_done


@app.on_event("startup")
async def startup():
    global db_pool, worker_task, supervisor, metrics_log_task
    global delay_propagation_supervisor, delay_propagation_metrics_log_task
    db_pool = await create_pool()
    # CREATE TABLE IF NOT EXISTS — safe to run on every startup. Previously
    # only test_api.py called this, so the schema only ever existed because
    # a developer's local Postgres had accumulated it over past runs; a
    # genuinely fresh database (a new docker-compose volume, RDS, ...) hit
    # UndefinedTableError here instead.
    await ensure_schema(db_pool)
    await graph_engine.load_from_db(db_pool, airport_code=settings.target_airport)
    await kafka_producer.start()

    if settings.live_api_enabled:
        client = FlightAwareClient(settings.flightaware_api_key)
        print("Ingestion client: real FlightAware API Client started.")
    else:
        client = MockFlightAwareClient(settings.target_airport)
        print("Ingestion client: Mock FlightAware Client started (paid API disabled).")

    # Keep references: an unreferenced asyncio.Task is only weakly held by
    # the event loop and can be garbage-collected mid-run.
    worker_task = asyncio.create_task(
        worker_run(client, kafka_producer, settings.target_airport)
    )
    worker_task.add_done_callback(_crash_logger("ingestion worker"))

    # WORKER_COUNT concurrent workers consuming flight-events, supervised
    # (crash -> log -> wait -> restart), pure persistence as of Phase F —
    # see flight_tracker/workers/CONCURRENCY.md for the full data flow.
    processed_producer = KafkaEventProducer()
    prediction_producer = KafkaEventProducer()
    await processed_producer.start()
    await prediction_producer.start()
    pool_obj = build_worker_pool(
        pool=db_pool,
        redis_client=redis_client,
        processed_producer=processed_producer,
        airport_code=settings.target_airport,
    )
    supervisor = Supervisor(pool_obj, consumer_group=settings.kafka_consumer_group_worker_pool)
    await supervisor.start()
    supervisor_task = asyncio.create_task(supervisor.run_forever())
    supervisor_task.add_done_callback(_crash_logger("worker pool supervisor"))
    metrics_log_task = asyncio.create_task(supervisor.run_periodic_metrics_logging())

    # Single-instance DelayPropagationWorker, wrapped in its own
    # WorkerPool/Supervisor for the same crash-restart supervision as the
    # persistence pool above — see delay_propagation_worker.py's docstring
    # for why this one must stay single-instance rather than joining the
    # WORKER_COUNT pool.
    delay_propagation_worker = build_delay_propagation_worker(
        graph_engine=graph_engine,
        predictor=predictor,
        prediction_producer=prediction_producer,
    )
    delay_propagation_pool = WorkerPool([delay_propagation_worker])
    delay_propagation_supervisor = Supervisor(
        delay_propagation_pool, consumer_group=settings.kafka_consumer_group_predictor
    )
    await delay_propagation_supervisor.start()
    delay_propagation_supervisor_task = asyncio.create_task(delay_propagation_supervisor.run_forever())
    delay_propagation_supervisor_task.add_done_callback(_crash_logger("delay propagation supervisor"))
    delay_propagation_metrics_log_task = asyncio.create_task(
        delay_propagation_supervisor.run_periodic_metrics_logging()
    )


@app.on_event("shutdown")
async def shutdown():
    if worker_task is not None:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    if metrics_log_task is not None:
        metrics_log_task.cancel()
        try:
            await metrics_log_task
        except asyncio.CancelledError:
            pass

    if delay_propagation_metrics_log_task is not None:
        delay_propagation_metrics_log_task.cancel()
        try:
            await delay_propagation_metrics_log_task
        except asyncio.CancelledError:
            pass

    if supervisor is not None:
        # "Finish in-flight events (timeout: 30s)" — Worker.run() already
        # finishes whatever message it's mid-processing before honoring
        # _stopping (see worker_pool.py), so this timeout is a backstop
        # against something hanging (a wedged DB/Kafka call outlasting its
        # own retry budget), not the normal-case wait.
        try:
            await asyncio.wait_for(
                supervisor.stop(), timeout=settings.worker_shutdown_timeout_seconds
            )
        except asyncio.TimeoutError:
            print(
                f"WARNING: worker pool did not shut down within "
                f"{settings.worker_shutdown_timeout_seconds}s — proceeding with shutdown anyway"
            )

    if delay_propagation_supervisor is not None:
        try:
            await asyncio.wait_for(
                delay_propagation_supervisor.stop(), timeout=settings.worker_shutdown_timeout_seconds
            )
        except asyncio.TimeoutError:
            print(
                f"WARNING: delay propagation worker did not shut down within "
                f"{settings.worker_shutdown_timeout_seconds}s — proceeding with shutdown anyway"
            )

    await kafka_producer.stop()
    if db_pool is not None:
        await db_pool.close()
    await redis_client.aclose()
    print(
        "Shutdown complete: worker pool + delay propagation worker + ingestion worker stopped, "
        "producer/DB pool/Redis client closed."
    )


@app.websocket("/ws/{airport_code}")
async def websocket_endpoint(websocket: WebSocket, airport_code: str):
    """
    Phase G protocol: every message sent is a WSMessage (see
    flight_tracker/websocket/messages.py) — type + timestamp + optional
    flight_id + a data dict, not the bare FlightEvent JSON this endpoint
    used to send. On connect: one SNAPSHOT, then a replay of
    `recent_ws_messages` (bounded history for late joiners), then the live
    stream. A single `outbox` queue is the only thing that ever calls
    websocket.send_text() — the heartbeat, the Kafka stream, and (indirectly,
    via history replay) connection setup would otherwise be up to three
    concurrent writers on the same socket, which FastAPI's WebSocket does
    not guarantee is safe.
    """
    await websocket.accept()
    request_id = str(uuid4())
    active_websocket_connections.inc()
    logger.info(
        "WebSocket connected",
        extra={"request_id": request_id, "flight_id": airport_code},
    )

    try:
        outbox: asyncio.Queue = asyncio.Queue()
        # Mutable holder (not a bare variable) so both the receiver task
        # (writes, on a client "subscribe"/"unsubscribe" message) and the
        # stream task (reads, to decide what to forward) can share it via
        # closure without a `nonlocal` per task.
        sub_state = {"flight_id": None}

        # SNAPSHOT first: current graph_engine state for this airport, built
        # fresh per connection rather than cached — matches the old dump-on-
        # connect behavior, just now typed and batched into one message
        # instead of one send_text() per flight.
        snapshot_flights = [
            attrs["event"].model_dump(mode="json")
            for _, attrs in graph_engine.graph.nodes(data=True)
            if attrs.get("event") is not None
        ]
        await websocket.send_text(snapshot_message(snapshot_flights).to_json())

        # Bounded history replay for late joiners — see recent_ws_messages'
        # own comment for why this is a shared buffer, not per-connection.
        for historical_msg in list(recent_ws_messages):
            await websocket.send_text(historical_msg.to_json())

        # Each connection gets its own throwaway consumer group so every
        # connected browser tab sees the full delay-predictions stream
        # independently — in the same group, multiple consumers *split* the
        # partitions instead of each seeing everything. auto_offset_reset
        # "latest": the snapshot+history above already cover current/recent
        # state, so a newly-connected client doesn't need (and shouldn't get) a
        # replay of the topic's whole retained history too.
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
            # Also the only reader of client-sent frames, so this is where an
            # optional {"action":"subscribe","flight_id":...}/{"action":
            # "unsubscribe"} message (task 1's "optional: subscribe to a
            # specific flight_id") gets applied. Malformed/unrecognized frames
            # are ignored, not fatal — a stray or future-versioned client
            # message shouldn't kill the connection. receive_text() raising
            # WebSocketDisconnect the moment the client actually closes is
            # still what detects a dead client — without a reader at all, a
            # closed tab is invisible to the server until the next
            # send_text() happens to fail.
            while True:
                raw = await websocket.receive_text()
                try:
                    msg = json.loads(raw)
                except (TypeError, ValueError):
                    continue
                action = msg.get("action")
                if action == "subscribe" and msg.get("flight_id"):
                    sub_state["flight_id"] = msg["flight_id"]
                elif action == "unsubscribe":
                    sub_state["flight_id"] = None

        def _passes_filter(ws_msg) -> bool:
            flight_id = sub_state["flight_id"]
            if flight_id is None:
                return True
            if ws_msg.flight_id == flight_id:
                return True
            # A client watching one flight should still see cascades that
            # originate from it, not just direct updates to it.
            return ws_msg.data.get("propagation_source") == flight_id

        async def stream_events():
            # delay-predictions carries PredictionEvent (Phase F); each maps to
            # exactly one typed WSMessage (Phase G) via classify_prediction_event.
            async for message in kafka_consumer:
                prediction = PredictionEvent.from_json(message.value)
                ws_msg = classify_prediction_event(prediction)
                recent_ws_messages.append(ws_msg)
                if _passes_filter(ws_msg):
                    await outbox.put((time.perf_counter(), ws_msg))

        async def send_heartbeats():
            # App-level heartbeat, not a raw WS ping/pong frame: the frontend
            # needs something it can observe through the same onmessage handler
            # it already has, to detect "socket claims open but nothing — not
            # even a heartbeat — has arrived in N seconds" without a separate
            # low-level ping/pong API.
            while True:
                await asyncio.sleep(WS_HEARTBEAT_INTERVAL_SECONDS)
                await outbox.put((time.perf_counter(), heartbeat_message()))

        async def drain_outbox():
            # The single writer — see this function's own docstring above for
            # why nothing else calls websocket.send_text() directly.
            while True:
                enqueued_at, ws_msg = await outbox.get()
                websocket_message_latency.observe(time.perf_counter() - enqueued_at)
                await websocket.send_text(ws_msg.to_json())

        receiver_task = asyncio.create_task(watch_for_disconnect())
        streamer_task = asyncio.create_task(stream_events())
        heartbeat_task = asyncio.create_task(send_heartbeats())
        sender_task = asyncio.create_task(drain_outbox())
        tasks = (receiver_task, streamer_task, heartbeat_task, sender_task)
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            await kafka_consumer.stop()

        for result in results:
            if isinstance(result, Exception) and not isinstance(
                result, (WebSocketDisconnect, asyncio.CancelledError)
            ):
                logger.warning(
                    f"WebSocket handler for {airport_code} exited due to: {result!r}",
                    extra={"request_id": request_id, "flight_id": airport_code},
                )
    finally:
        active_websocket_connections.dec()
        logger.info(
            "WebSocket disconnected",
            extra={"request_id": request_id, "flight_id": airport_code},
        )
