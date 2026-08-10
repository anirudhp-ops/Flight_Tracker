#!/usr/bin/env python3
"""
Verifies Phase J's observability stack actually works end to end, against
the real running system — same "no mocks, real infra" convention as every
other script in this directory (see flight_tracker/TESTING.md). Five things
get checked, in order:

  1. GET /metrics exposes the Prometheus counters/histograms/gauges from
     flight_tracker/metrics/, and they move under real load.
  2. GET /health reports "healthy" with real service/metrics data.
  3. The backend's own log lines (flight_tracker.* loggers) are valid JSON
     with the fields flight_tracker/logging_config.py's JSONFormatter
     promises.
  4. A request_id (== FlightEventEnvelope.event_id — see
     flight_tracker/middleware/request_id.py's docstring for why the
     Kafka pipeline reuses that instead of minting a second id) traces
     across pipeline stages: the same id shows up in both
     event_processor.py's "Event processed" line and
     delay_propagation_worker.py's "Delay propagation processed" line for
     the same event.
  5. Best-effort: if `docker compose up -d prometheus grafana` is already
     running, confirms Prometheus is actually scraping flight-backend
     (up{job="flight-backend"} == 1) and Grafana is reachable. Skipped
     (not failed) if that stack isn't up — it's optional infra, not a
     prerequisite for the app itself.

Starts its own throwaway `uvicorn flight_tracker.server:app` subprocess
(same pattern as scripts/integration_tests.py) rather than targeting an
already-running one like scripts/load_test_kafka.py does — checks 3 and 4
need this script's own handle on that process's stdout, which an
already-running instance doesn't offer.

Requires: local Postgres/Redis reachable via config.py's settings, and a
Kafka broker with the topics from scripts/create_kafka_topics.sh already
created. Check `ps aux | grep uvicorn` first — a second instance on a
different port still joins the same Kafka consumer groups and splits
partitions with this script's own throwaway instance (see TESTING.md).

Run:
  python scripts/test_observability.py
  python scripts/test_observability.py --duration 20
"""
import argparse
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
from aiokafka import AIOKafkaProducer

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from flight_tracker.config import settings  # noqa: E402
from flight_tracker.events.event_model import EventSource, wrap_flight_event  # noqa: E402
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus  # noqa: E402

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 8125  # distinct from integration_tests.py's 8123 / load_test's defaults
BASE_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"
LOG_PATH = REPO_ROOT / "scripts" / "observability_test_server.log"
RESULTS_PATH = REPO_ROOT / "scripts" / "observability_test_results.json"

RESULTS: list[dict] = []


def record(name: str, status: str, detail: str = "", **metrics):
    """status is PASS/FAIL/SKIP — SKIP for the optional Prometheus/Grafana
    checks when that stack simply isn't running, not treated as failure."""
    RESULTS.append({"name": name, "status": status, "detail": detail, "metrics": metrics})
    metric_str = f" ({', '.join(f'{k}={v}' for k, v in metrics.items())})" if metrics else ""
    print(f"[{status}] {name}{metric_str}{' - ' + detail if detail else ''}")


# --- server lifecycle (mirrors scripts/integration_tests.py) ---------------

def start_server() -> subprocess.Popen:
    env = os.environ.copy()
    env["ENABLE_FLIGHTAWARE_API"] = "false"
    env["TARGET_AIRPORT"] = "KJFK"
    log_file = open(LOG_PATH, "w")
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "flight_tracker.server:app",
            "--host", SERVER_HOST, "--port", str(SERVER_PORT), "--log-level", "info",
        ],
        cwd=str(REPO_ROOT), env=env,
        stdout=log_file, stderr=subprocess.STDOUT, text=True,
    )
    return proc


async def wait_for_health(timeout_s: float = 45.0) -> bool:
    deadline = time.monotonic() + timeout_s
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(f"{BASE_URL}/health", timeout=2.0)
                if resp.status_code == 200:
                    return True
            except (httpx.ConnectError, httpx.ReadTimeout):
                pass
            await asyncio.sleep(0.5)
    return False


def stop_server(proc: subprocess.Popen, timeout_s: float = 15.0):
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


# --- load generation ---------------------------------------------------

def make_flight(flight_id: str, *, delay_minutes: int, aircraft_id: str) -> FlightEvent:
    now = datetime.now(timezone.utc)
    return FlightEvent(
        flight_id=flight_id, event_type=EventType.DEPARTURE, airline_code="OB", flight_number=flight_id,
        origin="KJFK", destination="KBOS", aircraft_id=aircraft_id, gate_id=f"B{(hash(flight_id) % 14) + 1}",
        scheduled_departure=now, scheduled_arrival=now + timedelta(hours=2),
        delay_minutes=delay_minutes, status=FlightStatus.SCHEDULED, timestamp=now,
    )


async def generate_load(duration_s: float, rate_per_sec: float) -> int:
    """
    Publishes directly to flight-events, like scripts/load_test_kafka.py's
    throughput scenario — exercises the whole downstream pipeline (worker
    pool -> DelayPropagationWorker -> delay-predictions) that check 1's
    metrics and check 4's request-id trace both depend on having actually
    run. Roughly a third of events carry a delay > 0 so propagation/
    prediction/gate-reassignment metrics and log lines (not just the
    always-on persistence path) get exercised too.
    """
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    published = 0
    interval = 1.0 / rate_per_sec
    deadline = time.monotonic() + duration_s
    try:
        while time.monotonic() < deadline:
            flight_id = f"OBS-{uuid.uuid4().hex[:10]}"
            aircraft_id = f"OBSAC-{published % 20}"  # shared across some flights -> aircraft_turn edges
            delay = 20 if published % 3 == 0 else 0
            envelope = wrap_flight_event(
                make_flight(flight_id, delay_minutes=delay, aircraft_id=aircraft_id),
                source=EventSource.INTERNAL,
            )
            await producer.send_and_wait(
                settings.kafka_topic_flight_events,
                value=envelope.to_json().encode("utf-8"),
                key=flight_id.encode("utf-8"),
            )
            published += 1
            await asyncio.sleep(interval)
    finally:
        await producer.stop()
    return published


# --- checks --------------------------------------------------------------

REQUIRED_METRIC_FAMILIES = [
    "events_received_total",
    "events_processed_total",
    "event_processing_latency_seconds_count",
    "predictions_generated_total",
    "graph_node_count",
    "graph_edge_count",
    "database_connection_pool_size",
    "kafka_consumer_lag",
]


async def check_metrics_endpoint(client: httpx.AsyncClient) -> str:
    resp = await client.get(f"{BASE_URL}/metrics", timeout=5.0)
    if resp.status_code != 200:
        record("metrics_endpoint_reachable", "FAIL", f"status={resp.status_code}")
        return ""
    body = resp.text
    missing = [m for m in REQUIRED_METRIC_FAMILIES if m not in body]
    if missing:
        record("metrics_endpoint_families_present", "FAIL", f"missing={missing}")
    else:
        record("metrics_endpoint_families_present", "PASS", metrics={"families_checked": len(REQUIRED_METRIC_FAMILIES)})

    events_processed_total = sum(
        float(line.rsplit(" ", 1)[1])
        for line in body.splitlines()
        if line.startswith("events_processed_total{")
    )
    record(
        "metrics_move_under_load", "PASS" if events_processed_total > 0 else "FAIL",
        metrics={"events_processed_total": events_processed_total},
    )
    return body


async def check_cache_metrics(client: httpx.AsyncClient) -> None:
    """
    GET the airport snapshot endpoint twice for a throwaway, never-before-
    seen airport code: first is a guaranteed cache miss (and populates
    Redis with `[]`, since get_or_set caches any non-None result — see
    CacheLayer.get_or_set), second is a guaranteed cache hit. A run-unique
    code avoids a false negative from a previous run's still-live TTL on
    the real target_airport's key.
    """
    fake_airport = f"OBSTEST-{uuid.uuid4().hex[:8]}"
    await client.get(f"{BASE_URL}/api/airports/{fake_airport}/snapshot", timeout=5.0)
    await client.get(f"{BASE_URL}/api/airports/{fake_airport}/snapshot", timeout=5.0)
    resp = await client.get(f"{BASE_URL}/metrics", timeout=5.0)
    body = resp.text
    hits = next((l for l in body.splitlines() if l.startswith("cache_hits_total")), "")
    misses = next((l for l in body.splitlines() if l.startswith("cache_misses_total")), "")
    hit_val = float(hits.rsplit(" ", 1)[1]) if hits else 0.0
    miss_val = float(misses.rsplit(" ", 1)[1]) if misses else 0.0
    record(
        "cache_hit_rate_tracked", "PASS" if hit_val > 0 and miss_val > 0 else "FAIL",
        metrics={"cache_hits_total": hit_val, "cache_misses_total": miss_val},
    )


async def check_health_endpoint(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"{BASE_URL}/health", timeout=5.0)
    if resp.status_code != 200:
        record("health_endpoint_reachable", "FAIL", f"status={resp.status_code}")
        return
    data = resp.json()
    services_ok = all(v == "ok" for v in data.get("services", {}).values())
    has_metrics_keys = {"events_per_second", "avg_latency_ms", "error_rate", "consumer_lag"} <= data.get(
        "metrics", {}
    ).keys()
    passed = data.get("status") == "healthy" and services_ok and has_metrics_keys
    record("health_endpoint_shape", "PASS" if passed else "FAIL", detail=json.dumps(data))


def check_structured_logs() -> list[dict]:
    lines = LOG_PATH.read_text(errors="replace").splitlines()
    parsed = []
    for line in lines:
        try:
            parsed.append(json.loads(line))
        except (json.JSONDecodeError, ValueError):
            continue  # uvicorn's own startup banner isn't JSON — expected, not in scope (see module docstring)

    app_lines = [p for p in parsed if str(p.get("logger", "")).startswith("flight_tracker.")]
    required_keys = {"timestamp", "level", "logger", "message", "request_id", "flight_id", "worker_id"}
    well_formed = [p for p in app_lines if required_keys <= p.keys()]

    record(
        "structured_json_logging",
        "PASS" if app_lines and len(well_formed) == len(app_lines) else "FAIL",
        metrics={"total_lines": len(lines), "json_lines": len(parsed), "flight_tracker_lines": len(app_lines)},
    )
    return parsed


def check_request_id_tracing(parsed_lines: list[dict]) -> None:
    processed_ids = {
        p["request_id"]
        for p in parsed_lines
        if p.get("logger") == "flight_tracker.workers.event_processor" and p.get("message") == "Event processed"
    }
    propagated_ids = {
        p["request_id"]
        for p in parsed_lines
        if p.get("logger") == "flight_tracker.workers.delay_propagation_worker"
        and p.get("message") == "Delay propagation processed"
    }
    traced = processed_ids & propagated_ids
    record(
        "request_id_traces_across_pipeline_stages",
        "PASS" if traced else "FAIL",
        metrics={"processed_ids": len(processed_ids), "propagated_ids": len(propagated_ids), "traced": len(traced)},
    )


async def check_prometheus_and_grafana() -> None:
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                "http://localhost:9090/api/v1/query",
                params={"query": 'up{job="flight-backend"}'},
                timeout=3.0,
            )
            results = resp.json().get("data", {}).get("result", [])
            up = bool(results) and results[0]["value"][1] == "1"
            record("prometheus_scraping_backend", "PASS" if up else "FAIL", metrics={"result": str(results)})
        except (httpx.ConnectError, httpx.ReadTimeout):
            record(
                "prometheus_scraping_backend", "SKIP",
                "Prometheus not reachable at localhost:9090 — run `docker compose up -d prometheus grafana` first",
            )

        try:
            resp = await client.get("http://localhost:3001/api/health", timeout=3.0)
            record(
                "grafana_reachable", "PASS" if resp.status_code == 200 else "FAIL",
                metrics={"status_code": resp.status_code},
            )
        except (httpx.ConnectError, httpx.ReadTimeout):
            record(
                "grafana_reachable", "SKIP",
                "Grafana not reachable at localhost:3001 — run `docker compose up -d prometheus grafana` first",
            )


# --- main ------------------------------------------------------------------

async def main(duration: float, rate: float) -> int:
    print(f"Starting throwaway server on {BASE_URL} (log: {LOG_PATH})...")
    proc = start_server()
    try:
        if not await wait_for_health():
            record("server_startup", "FAIL", "server did not become healthy within timeout")
            print(LOG_PATH.read_text(errors="replace")[-4000:])
            return 1
        record("server_startup", "PASS")

        print(f"Generating load for {duration}s at ~{rate} events/sec...")
        published = await generate_load(duration, rate)
        print(f"Published {published} events. Waiting 5s for the pipeline to drain...")
        await asyncio.sleep(5.0)

        async with httpx.AsyncClient() as client:
            await check_metrics_endpoint(client)
            await check_cache_metrics(client)
            await check_health_endpoint(client)

        parsed_lines = check_structured_logs()
        check_request_id_tracing(parsed_lines)
        await check_prometheus_and_grafana()
    finally:
        print("Stopping server...")
        stop_server(proc)

    RESULTS_PATH.write_text(json.dumps(RESULTS, indent=2))
    failed = [r for r in RESULTS if r["status"] == "FAIL"]
    skipped = [r for r in RESULTS if r["status"] == "SKIP"]
    print(f"\n{len(RESULTS) - len(failed) - len(skipped)}/{len(RESULTS)} passed, "
          f"{len(skipped)} skipped, {len(failed)} failed. Results: {RESULTS_PATH}")
    return 1 if failed else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=60.0, help="Load generation duration in seconds")
    parser.add_argument("--rate", type=float, default=20.0, help="Events published per second")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.duration, args.rate)))
