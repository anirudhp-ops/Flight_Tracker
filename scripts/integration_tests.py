#!/usr/bin/env python3
"""
Integration test suite: exercises the real, deployed pipeline end to end —
publish a FlightEvent to Kafka, watch it come out the other end as a
prediction, hit the real database, kill and restart the process — instead
of unit-testing individual classes in-process (see flight_tracker/tests/
for those).

Deviation from the literal Phase I brief, stated up front (same convention
the rest of this codebase uses for documented deviations — see e.g.
graph/engine.py, docker-compose.yml): the brief describes "start
docker-compose (all services)" and "stop one worker, verify others
continue." Neither maps cleanly onto this project's actual architecture:

  - This app has no per-service docker-compose entry for itself (only
    Kafka/Postgres/Redis — see docker-compose.yml's own docstring on why:
    the app runs on the host against those, not containerized). This
    script therefore runs against the same native Postgres/Redis/Kafka
    every other test in this project uses (flight_tracker/config.py's
    defaults), and starts the *app* itself as a subprocess
    (`uvicorn flight_tracker.server:app`) rather than via docker-compose.
  - "Workers" here are not separate OS processes — WORKER_COUNT persistence
    workers and the single DelayPropagationWorker are asyncio tasks inside
    ONE uvicorn process (see server.py's startup()), individually
    crash-restarted in-process by flight_tracker/workers/supervisor.py
    (covered directly by flight_tracker/tests/test_worker_pool.py's
    test_supervisor_restarts_a_crashed_worker). There is no external lever
    to kill "one worker" without killing the whole process. What IS
    externally observable and worth proving here is the coarser,
    equally-real case: the whole app process dies (crash, deploy, OOM-kill)
    and comes back — does a message published while it was down still get
    processed once it restarts (Kafka's committed-offset catch-up), and
    does the pipeline resume working afterward. That's what
    scenario_process_restart_catchup() below actually tests.

Requires: local Postgres/Redis reachable via config.py's settings, and a
Kafka broker with the topics from scripts/create_kafka_topics.sh already
created. Does not require FlightAware API access (ENABLE_FLIGHTAWARE_API
stays off — the app runs its normal mock ingestion client in the
background, which is harmless noise here since every event this script
publishes uses a unique, script-generated flight_id prefix).

Run: python scripts/integration_tests.py
"""
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from flight_tracker.config import settings  # noqa: E402
from flight_tracker.db.writer import create_pool  # noqa: E402
from flight_tracker.events.event_model import EventSource, wrap_flight_event  # noqa: E402
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus  # noqa: E402

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8123  # non-default: don't collide with a dev server on 8000
BASE_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"

RESULTS: list[dict] = []


def record(name: str, passed: bool, detail: str = "", **metrics):
    RESULTS.append({"name": name, "passed": passed, "detail": detail, "metrics": metrics})
    status = "PASS" if passed else "FAIL"
    metric_str = f" ({', '.join(f'{k}={v}' for k, v in metrics.items())})" if metrics else ""
    print(f"[{status}] {name}{metric_str}{' - ' + detail if detail else ''}")


# --- process lifecycle -----------------------------------------------------

def start_server() -> subprocess.Popen:
    env = os.environ.copy()
    env["ENABLE_FLIGHTAWARE_API"] = "false"
    env["TARGET_AIRPORT"] = "KJFK"
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "flight_tracker.server:app",
            "--host", SERVER_HOST, "--port", str(SERVER_PORT), "--log-level", "warning",
        ],
        cwd=str(REPO_ROOT), env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    return proc


async def wait_for_health(timeout_s: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout_s
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(f"{BASE_URL}/health/db", timeout=2.0)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("redis") == "ok" and data.get("active_flights_rows") is not None:
                        return True
            except (httpx.ConnectError, httpx.ReadTimeout):
                pass
            await asyncio.sleep(0.5)
    return False


def stop_server(proc: subprocess.Popen, *, force: bool = False, timeout_s: float = 15.0):
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGKILL if force else signal.SIGTERM)
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


# --- fixtures ----------------------------------------------------------------

def make_flight(flight_id: str, *, delay_minutes: int = 0, aircraft_id: str | None = None,
                 gate_id: str | None = None, dep_offset_min: int = 0, arr_offset_min: int = 120,
                 timestamp: datetime | None = None) -> FlightEvent:
    now = datetime.now(timezone.utc)
    return FlightEvent(
        flight_id=flight_id, event_type=EventType.DEPARTURE, airline_code="IT", flight_number=flight_id,
        origin="KJFK", destination="KBOS", aircraft_id=aircraft_id, gate_id=gate_id,
        scheduled_departure=now + timedelta(minutes=dep_offset_min),
        scheduled_arrival=now + timedelta(minutes=arr_offset_min),
        delay_minutes=delay_minutes, status=FlightStatus.SCHEDULED, timestamp=timestamp or now,
    )


async def publish(producer: AIOKafkaProducer, flight: FlightEvent) -> None:
    envelope = wrap_flight_event(flight, source=EventSource.INTERNAL)
    await producer.send_and_wait(
        settings.kafka_topic_flight_events,
        value=envelope.to_json().encode("utf-8"),
        key=flight.flight_id.encode("utf-8"),
    )


async def start_prediction_watcher(group_suffix: str) -> AIOKafkaConsumer:
    """Starts and positions a delay-predictions consumer at the current
    end of the topic (skipping years of backlog from prior test runs —
    see the worker-pool unit tests for the same issue/fix). Must be
    called and fully started BEFORE publishing whatever event you're
    about to watch for: publishing first and only then starting the
    consumer loses the race against a fast pipeline outright — this was
    caught by an early run of this exact script (an event genuinely
    processed within ~200ms still showed up as "no prediction" because
    this consumer hadn't subscribed yet), not assumed."""
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_delay_predictions,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"itest-{group_suffix}-{uuid.uuid4().hex[:8]}",
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await consumer.start()
    await consumer.seek_to_end()
    return consumer


async def collect_predictions(consumer: AIOKafkaConsumer, flight_ids: set[str], timeout_s: float) -> dict:
    """Reads from an already-started/positioned watcher (see
    start_prediction_watcher) until every id in `flight_ids` has been seen
    or the timeout elapses. Returns {flight_id: seconds_since_start}."""
    seen: dict[str, float] = {}
    start = time.monotonic()
    deadline = start + timeout_s
    while time.monotonic() < deadline and len(seen) < len(flight_ids):
        remaining_ms = max(100, int((deadline - time.monotonic()) * 1000))
        batches = await consumer.getmany(timeout_ms=min(1000, remaining_ms))
        for records in batches.values():
            for record in records:
                payload = json.loads(record.value)
                fid = payload.get("flight_id")
                if fid in flight_ids and fid not in seen:
                    seen[fid] = time.monotonic() - start
    return seen


# --- scenarios -----------------------------------------------------------------

async def scenario_single_event_latency(producer, pool):
    """Publish one FlightEvent -> verify a delay-predictions message and an
    active_flights row appear, and report how long each took."""
    flight_id = f"ITEST-SINGLE-{uuid.uuid4().hex[:8]}"
    flight = make_flight(flight_id, delay_minutes=25)

    watcher = await start_prediction_watcher("single")
    t0 = time.monotonic()
    await publish(producer, flight)

    try:
        seen = await collect_predictions(watcher, {flight_id}, timeout_s=5.0)
    finally:
        await watcher.stop()
    prediction_latency = seen.get(flight_id)

    deadline = time.monotonic() + 5.0
    db_row = None
    while time.monotonic() < deadline:
        db_row = await pool.fetchrow(
            "SELECT * FROM active_flights WHERE flight_id = $1", flight_id
        )
        if db_row is not None:
            break
        await asyncio.sleep(0.2)
    db_latency = time.monotonic() - t0

    passed = prediction_latency is not None and db_row is not None
    record(
        "single event: prediction + DB write within 5s", passed,
        detail="" if passed else "prediction or DB row never appeared",
        prediction_latency_s=round(prediction_latency, 3) if prediction_latency is not None else None,
        db_write_latency_s=round(db_latency, 3) if db_row is not None else None,
    )
    return passed


async def scenario_idempotency(producer, pool):
    """Publish the exact same (flight_id, timestamp) event twice -> exactly
    one flight_events row, proving the DB-level UNIQUE constraint + ON
    CONFLICT DO NOTHING guard (flight_tracker/db/writer.py) survives a real
    round trip through Kafka and the persistence worker pool, not just a
    direct write_events() call (already covered in-process by
    flight_tracker/tests/test_idempotency.py)."""
    flight_id = f"ITEST-DUP-{uuid.uuid4().hex[:8]}"
    ts = datetime.now(timezone.utc)
    flight = make_flight(flight_id, delay_minutes=5, timestamp=ts)

    await publish(producer, flight)
    await publish(producer, flight)  # exact duplicate: same flight_id + timestamp

    deadline = time.monotonic() + 8.0
    count = 0
    while time.monotonic() < deadline:
        count = await pool.fetchval(
            "SELECT count(*) FROM flight_events WHERE flight_id = $1", flight_id
        )
        if count >= 1:
            # give the (already-committed) duplicate a moment to land too,
            # if it were ever going to.
            await asyncio.sleep(1.5)
            count = await pool.fetchval(
                "SELECT count(*) FROM flight_events WHERE flight_id = $1", flight_id
            )
            break
        await asyncio.sleep(0.2)

    passed = count == 1
    record(
        "idempotency: duplicate (flight_id, timestamp) produces exactly one DB row",
        passed, detail=f"flight_events row count = {count}",
    )
    return passed


async def scenario_cascade(producer):
    """Build a 51-flight aircraft-turn star (one trigger + 50 flights
    sharing its aircraft_id, so each gets a direct graph edge from the
    trigger — see this file's own module docstring pattern in
    flight_tracker/tests/test_graph.py for why aircraft_turn, not
    gate_reuse, is what survives as a stable graph edge), then delay the
    trigger and measure how long it takes for all 50 propagated
    predictions to arrive on delay-predictions."""
    tag = uuid.uuid4().hex[:8]
    aircraft_id = f"ITEST-AC-{tag}"
    trigger_id = f"ITEST-CASCADE-TRIGGER-{tag}"
    neighbor_ids = [f"ITEST-CASCADE-{tag}-{i}" for i in range(50)]

    # GraphEngine.add_edges_for_flight() draws an aircraft_turn edge from
    # every already-existing same-aircraft node TO the newly-inserted one
    # (see graph/engine.py) — so for the trigger to end up with an
    # OUTGOING edge to a neighbor (the only direction propagate_delay's
    # BFS walks), the trigger must already be in the graph before that
    # neighbor is inserted. There's no cross-partition Kafka ordering
    # guarantee between the trigger's own messages and 50 independently-
    # keyed neighbors' messages, so publishing them all in a burst and
    # merely waiting for persistence (active_flights) to finish is a real
    # race — an earlier run of this exact script lost 14/50 propagations
    # to neighbors that happened to be graph-inserted before the trigger,
    # getting the edge backwards. The fix is ordering, not a longer wait:
    # confirm the trigger is graph-ready FIRST (its own delay-prediction
    # published — DelayPropagationProcessor.process() always emits one,
    # even for a non-delayed flight), and only then publish the neighbors.
    setup_watcher = await start_prediction_watcher("cascade-setup")
    trigger = make_flight(trigger_id, delay_minutes=0, aircraft_id=aircraft_id)
    await publish(producer, trigger)
    try:
        trigger_seen = await collect_predictions(setup_watcher, {trigger_id}, timeout_s=10.0)
    finally:
        await setup_watcher.stop()

    if trigger_id not in trigger_seen:
        record("cascade: trigger flight graph-ready before neighbors", False,
               detail="trigger's own setup prediction never arrived within 10s")
        return False

    neighbors_watcher = await start_prediction_watcher("cascade-neighbors")
    for nid in neighbor_ids:
        await publish(producer, make_flight(nid, delay_minutes=0, aircraft_id=aircraft_id))
    try:
        setup_seen = await collect_predictions(neighbors_watcher, set(neighbor_ids), timeout_s=20.0)
    finally:
        await neighbors_watcher.stop()

    if len(setup_seen) != len(neighbor_ids):
        record("cascade: 50-flight setup graph-ready before triggering", False,
               detail=f"only {len(setup_seen)}/{len(neighbor_ids)} neighbor flights confirmed processed after 20s")
        return False

    watcher = await start_prediction_watcher("cascade")
    trigger_delayed = make_flight(trigger_id, delay_minutes=180, aircraft_id=aircraft_id)
    t0 = time.monotonic()
    await publish(producer, trigger_delayed)

    try:
        seen = await collect_predictions(watcher, set(neighbor_ids), timeout_s=10.0)
    finally:
        await watcher.stop()
    elapsed = time.monotonic() - t0
    missing = set(neighbor_ids) - set(seen.keys())
    if missing:
        print(f"  ... missing neighbor_ids ({len(missing)}): {sorted(missing)[:10]}")

    passed = len(seen) == len(neighbor_ids)
    record(
        "cascade: 50-flight delay propagation", passed,
        detail=f"{len(seen)}/{len(neighbor_ids)} propagated predictions received",
        total_propagation_time_s=round(elapsed, 3),
    )
    return passed


async def scenario_process_restart_catchup(producer_factory):
    """Kill the whole app process, publish a FlightEvent while it's down,
    restart it, and verify the event still gets processed — Kafka's
    committed-offset catch-up (auto_offset_reset="earliest" +
    enable_auto_commit=False in Worker.start()), not lost. See this file's
    module docstring for why this replaces the brief's literal
    "stop one worker" for this project's single-process architecture."""
    global server_proc

    stop_server(server_proc, force=True)
    print("  ... process killed (SIGKILL)")

    flight_id = f"ITEST-RESTART-{uuid.uuid4().hex[:8]}"
    flight = make_flight(flight_id, delay_minutes=15)
    producer = await producer_factory()
    await publish(producer, flight)
    print(f"  ... published {flight_id} while the process was down")

    server_proc = start_server()
    healthy = await wait_for_health(timeout_s=30.0)
    if not healthy:
        record("worker resilience: process restarts and becomes healthy", False,
               detail="server did not report healthy within 30s of restart")
        return False
    print("  ... process restarted and healthy")

    pool = await create_pool()
    try:
        deadline = time.monotonic() + 20.0
        row = None
        while time.monotonic() < deadline:
            row = await pool.fetchrow("SELECT * FROM active_flights WHERE flight_id = $1", flight_id)
            if row is not None:
                break
            await asyncio.sleep(0.3)
    finally:
        await pool.close()

    passed = row is not None
    record(
        "worker resilience: message published during downtime is processed after restart",
        passed, detail="" if passed else "flight never appeared in active_flights after restart",
    )
    return passed


# --- main ----------------------------------------------------------------------

server_proc: subprocess.Popen | None = None


async def main():
    global server_proc

    print(f"Starting server subprocess on {BASE_URL} ...")
    server_proc = start_server()
    healthy = await wait_for_health(timeout_s=30.0)
    if not healthy:
        print("Server never became healthy — dumping recent output:")
        if server_proc.stdout:
            print(server_proc.stdout.read(4000))
        stop_server(server_proc, force=True)
        sys.exit(1)
    print("Server healthy.\n")

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    pool = await create_pool()

    try:
        await scenario_single_event_latency(producer, pool)
        await scenario_idempotency(producer, pool)
        await scenario_cascade(producer)
    finally:
        await producer.stop()
        await pool.close()

    # The restart scenario owns the producer for its "publish while down"
    # step, since the original producer/process context is gone once the
    # server (and this script's other connections) get torn down around it.
    async def make_producer():
        p = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await p.start()
        return p

    restart_producer = None
    try:
        async def factory():
            nonlocal restart_producer
            restart_producer = await make_producer()
            return restart_producer

        await scenario_process_restart_catchup(factory)
    finally:
        if restart_producer is not None:
            await restart_producer.stop()
        stop_server(server_proc)

    print("\n" + "=" * 70)
    print("INTEGRATION TEST SUMMARY")
    print("=" * 70)
    n_passed = sum(1 for r in RESULTS if r["passed"])
    for r in RESULTS:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] {r['name']}")
        if r["metrics"]:
            for k, v in r["metrics"].items():
                print(f"           {k}: {v}")
    print(f"\n{n_passed}/{len(RESULTS)} scenarios passed.")

    Path(REPO_ROOT / "scripts" / "integration_test_results.json").write_text(
        json.dumps(RESULTS, indent=2)
    )
    sys.exit(0 if n_passed == len(RESULTS) else 1)


if __name__ == "__main__":
    asyncio.run(main())
