"""
Unit tests for flight_tracker/workers/worker_pool.py and
flight_tracker/workers/supervisor.py. Runs against the real local
Postgres/Redis/Kafka (no mocks) — consistent with this project's testing
convention (see test_workers.py's docstring).

Run: pytest flight_tracker/tests/test_worker_pool.py -v
"""
import asyncio
import uuid
from datetime import datetime, timezone

import pytest
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer

from flight_tracker.config import settings
from flight_tracker.db.writer import create_pool, ensure_schema
from flight_tracker.events.event_model import EventSource, wrap_flight_event
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus
from flight_tracker.workers.supervisor import Supervisor
from flight_tracker.workers.worker_pool import Worker, WorkerPool, build_worker_pool


@pytest.fixture
async def pool():
    p = await create_pool()
    # CI's Postgres service container is fresh every run — a local
    # Postgres normally already has the schema from a prior server.py
    # startup (ensure_schema() runs there too), which is why this was
    # only ever caught in CI, not locally.
    await ensure_schema(p)
    yield p
    await p.close()


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}")
    yield client
    await client.aclose()


@pytest.fixture
async def processed_producer():
    producer = KafkaEventProducer()
    await producer.start()
    yield producer
    await producer.stop()


def _flight(flight_id):
    now = datetime.now(timezone.utc)
    return FlightEvent(
        flight_id=flight_id, event_type=EventType.DEPARTURE, airline_code="UT", flight_number="1",
        origin="KJFK", destination="KLAX", scheduled_departure=now, scheduled_arrival=now,
        delay_minutes=0, status=FlightStatus.SCHEDULED, timestamp=now,
    )


# --- build_worker_pool --------------------------------------------------------

def test_build_worker_pool_constructs_requested_worker_count(pool, redis_client, processed_producer):
    worker_pool = build_worker_pool(
        pool=pool, redis_client=redis_client, processed_producer=processed_producer,
        airport_code="KJFK", worker_count=3,
    )
    assert len(worker_pool.workers) == 3
    assert [w.worker_id for w in worker_pool.workers] == ["worker-0", "worker-1", "worker-2"]


def test_build_worker_pool_defaults_to_settings_worker_count(pool, redis_client, processed_producer):
    worker_pool = build_worker_pool(
        pool=pool, redis_client=redis_client, processed_producer=processed_producer, airport_code="KJFK",
    )
    assert len(worker_pool.workers) == settings.worker_count


# --- WorkerPool lifecycle + metrics aggregation --------------------------------

async def test_worker_pool_start_stop_lifecycle(pool, redis_client, processed_producer):
    worker_pool = build_worker_pool(
        pool=pool, redis_client=redis_client, processed_producer=processed_producer,
        airport_code="KJFK", worker_count=2,
    )
    await worker_pool.start()
    try:
        for w in worker_pool.workers:
            assert w._consumer is not None
            assert w.failure_handler is not None
    finally:
        await worker_pool.stop()


def test_worker_pool_metrics_aggregate_across_workers(pool, redis_client, processed_producer):
    worker_pool = build_worker_pool(
        pool=pool, redis_client=redis_client, processed_producer=processed_producer,
        airport_code="KJFK", worker_count=2,
    )
    worker_pool.workers[0].processor.events_processed = 5
    worker_pool.workers[1].processor.events_processed = 3
    assert worker_pool.total_events_processed == 8

    worker_pool.workers[0].processor.idempotent_skips = 1
    worker_pool.workers[1].processor.idempotent_skips = 2
    assert worker_pool.total_idempotent_skips == 3

    worker_pool.workers[0].processor.retries_attempted = 4
    assert worker_pool.total_retries_attempted == 4

    worker_pool.workers[0].restarts = 1
    worker_pool.workers[1].restarts = 2
    assert worker_pool.total_restarts == 3

    worker_pool.workers[0].processor.latencies_ms = [1.0, 2.0]
    worker_pool.workers[1].processor.latencies_ms = [3.0]
    assert worker_pool.all_latencies_ms() == [1.0, 2.0, 3.0]


# --- End-to-end: Worker.run() actually processes a real flight-events message ---

async def test_worker_consumes_and_processes_a_real_event_end_to_end(pool, redis_client, processed_producer, monkeypatch):
    # A dedicated, never-reused consumer group: reusing
    # settings.kafka_consumer_group_worker_pool (the default) means this
    # test's join immediately follows other tests' leaves in the same
    # group in the same run, which can stall on the broker's rebalance
    # timeout — a real but infra-timing quirk, not something about the
    # Worker/WorkerPool code itself being tested here.
    monkeypatch.setattr(settings, "kafka_consumer_group_worker_pool", f"test-pool-{uuid.uuid4().hex[:8]}")
    worker_pool = build_worker_pool(
        pool=pool, redis_client=redis_client, processed_producer=processed_producer,
        airport_code="KJFK", worker_count=1,
    )
    worker = worker_pool.workers[0]
    await worker.start()
    # flight-events has years of accumulated backlog from prior test runs;
    # skip straight to "now" rather than replaying it all under
    # auto_offset_reset="earliest" (a deliberate production choice in
    # Worker.start(), left unchanged here).
    await worker._consumer.seek_to_end()
    run_task = asyncio.create_task(worker.run())
    try:
        raw_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await raw_producer.start()
        flight_id = f"WPTEST-{uuid.uuid4().hex[:8]}"
        envelope = wrap_flight_event(_flight(flight_id), source=EventSource.MOCK)
        try:
            await raw_producer.send_and_wait(
                settings.kafka_topic_flight_events,
                value=envelope.to_json().encode("utf-8"),
                key=flight_id.encode("utf-8"),
            )
        finally:
            await raw_producer.stop()

        deadline = asyncio.get_event_loop().time() + 20
        while asyncio.get_event_loop().time() < deadline:
            row = await pool.fetchval(
                "SELECT count(*) FROM flight_events WHERE flight_id = $1", flight_id
            )
            if row == 1:
                break
            await asyncio.sleep(0.5)

        assert worker.processor.events_processed >= 1

        row = await pool.fetchval(
            "SELECT count(*) FROM flight_events WHERE flight_id = $1", flight_id
        )
        assert row == 1
    finally:
        worker._stopping = True
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass
        await worker.stop()


# --- Supervisor: restart-on-crash ------------------------------------------------

class _CrashOnceWorker(Worker):
    """A Worker whose run() raises once, then behaves — exercises
    Supervisor._supervised_loop's catch -> log -> wait -> restart path
    without needing an actual Kafka-level failure to trigger it."""

    def __init__(self, worker_id):
        self._crashed = False
        self.worker_id = worker_id
        self.restarts = 0
        self._consumer = None
        self._dlq_producer = None
        self.failure_handler = None
        self._stopping = False
        self.processor = _StubProcessor()

    async def start(self):
        self._stopping = False

    async def stop(self):
        self._stopping = True

    async def run(self):
        if not self._crashed:
            self._crashed = True
            raise RuntimeError("simulated crash")
        # After the simulated restart, behave: block until stopped.
        while not self._stopping:
            await asyncio.sleep(0.05)


class _StubProcessor:
    events_processed = 0
    events_failed = 0
    idempotent_skips = 0
    retries_attempted = 0


async def test_supervisor_restarts_a_crashed_worker():
    worker = _CrashOnceWorker("crash-test-worker")
    supervisor = Supervisor(WorkerPool([worker]))
    # Speed the test up — settings default is 5s between crash and restart.
    settings.worker_supervisor_restart_delay_seconds = 0.05

    await supervisor.start()
    try:
        deadline = asyncio.get_event_loop().time() + 5
        while asyncio.get_event_loop().time() < deadline:
            if supervisor.restarts_per_worker["crash-test-worker"] >= 1:
                break
            await asyncio.sleep(0.05)

        assert supervisor.restarts_per_worker["crash-test-worker"] == 1
        assert supervisor.failures_per_worker["crash-test-worker"] == 1
        assert worker.restarts == 1
    finally:
        await supervisor.stop()
