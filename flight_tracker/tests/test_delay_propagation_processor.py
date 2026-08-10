"""
Additional coverage for flight_tracker/workers/delay_propagation_worker.py
beyond test_delay_propagation_worker.py's gate-conflict-focused tests:
the delay>0 prediction+propagation path, the DelayPropagationWorker
consumer lifecycle, and a real end-to-end run() against Kafka. Real
GraphEngine + real trained DelayPredictor + real Kafka, only the
prediction producer is faked for the pure-processor tests (matching
test_delay_propagation_worker.py's own convention).

Run: pytest flight_tracker/tests/test_delay_propagation_processor.py -v
"""
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from aiokafka import AIOKafkaProducer

from flight_tracker.config import settings
from flight_tracker.events.event_model import EventSource, wrap_flight_event
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus
from flight_tracker.workers.delay_propagation_worker import (
    DelayPropagationProcessor,
    build_delay_propagation_worker,
)
from ml.predictor import DelayPredictor

MODEL_PATH = str(Path(__file__).resolve().parent.parent.parent / "ml" / "model.pkl")


class _FakeProducer:
    def __init__(self):
        self.published = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


@pytest.fixture(scope="module")
def predictor():
    return DelayPredictor(MODEL_PATH)


def _flight(flight_id, *, delay_minutes=0, aircraft_id=None, dep_offset_min=0, arr_offset_min=120):
    now = datetime.now(timezone.utc)
    return FlightEvent(
        flight_id=flight_id, event_type=EventType.DEPARTURE, airline_code="AA", flight_number=flight_id,
        origin="KJFK", destination="KBOS", aircraft_id=aircraft_id,
        scheduled_departure=now + timedelta(minutes=dep_offset_min),
        scheduled_arrival=now + timedelta(minutes=arr_offset_min),
        delay_minutes=delay_minutes, status=FlightStatus.SCHEDULED, timestamp=now,
    )


async def test_process_delayed_flight_predicts_and_propagates_to_neighbor(predictor):
    graph_engine = GraphEngine(airport_code="KJFK")
    producer = _FakeProducer()
    processor = DelayPropagationProcessor(graph_engine, predictor, producer)

    # add_edges_for_flight() only draws an edge from an already-existing
    # node to the newly-added one (see engine.py) — so the source flight
    # must enter the graph FIRST (edge: source -> neighbor), and only then
    # does a later "delay" event for the same flight_key give
    # propagate_delay() an outgoing edge to actually walk.
    source_calm = _flight("DP-SOURCE", delay_minutes=0, aircraft_id="N999")
    ok, err = await processor.process(wrap_flight_event(source_calm, source=EventSource.MOCK))
    assert ok, err

    neighbor = _flight("DP-NEIGHBOR", aircraft_id="N999")
    ok, err = await processor.process(wrap_flight_event(neighbor, source=EventSource.MOCK))
    assert ok, err

    source_delayed = _flight("DP-SOURCE", delay_minutes=60, aircraft_id="N999")
    ok, err = await processor.process(wrap_flight_event(source_delayed, source=EventSource.MOCK))
    assert ok, err

    # One publish for the triggering (delayed) flight, one for its propagated neighbor.
    published_ids = [pe.flight_id for _, pe in producer.published]
    assert "DP-SOURCE" in published_ids
    assert "DP-NEIGHBOR" in published_ids

    # DP-NEIGHBOR is published twice: once for its own (non-delayed)
    # process() call, and again when DP-SOURCE's delay propagates to it —
    # the second is the one carrying propagation_source.
    neighbor_pub = next(
        pe for _, pe in producer.published
        if pe.flight_id == "DP-NEIGHBOR" and pe.propagation_source is not None
    )
    assert neighbor_pub.propagation_source == "DP-SOURCE"
    assert neighbor_pub.propagation_hops == 1
    assert neighbor_pub.predicted_delay_minutes >= 0

    source_pub = next(pe for _, pe in producer.published if pe.flight_id == "DP-SOURCE")
    assert source_pub.propagation_source is None


async def test_process_non_delayed_flight_publishes_with_full_confidence(predictor):
    graph_engine = GraphEngine(airport_code="KJFK")
    producer = _FakeProducer()
    processor = DelayPropagationProcessor(graph_engine, predictor, producer)

    flight = _flight("DP-CALM", delay_minutes=0)
    ok, err = await processor.process(wrap_flight_event(flight, source=EventSource.MOCK))
    assert ok, err

    assert len(producer.published) == 1
    _, pe = producer.published[0]
    assert pe.predicted_delay_minutes == 0
    assert pe.model_confidence == 1.0
    assert pe.propagation_source is None


async def test_process_counts_events_processed_and_records_latency(predictor):
    graph_engine = GraphEngine(airport_code="KJFK")
    producer = _FakeProducer()
    processor = DelayPropagationProcessor(graph_engine, predictor, producer)

    await processor.process(wrap_flight_event(_flight("DP-1"), source=EventSource.MOCK))
    await processor.process(wrap_flight_event(_flight("DP-2"), source=EventSource.MOCK))

    assert processor.events_processed == 2
    assert processor.events_failed == 0
    assert len(processor.latencies_ms) == 2
    assert all(ms >= 0 for ms in processor.latencies_ms)


async def test_process_returns_error_tuple_on_internal_failure(predictor, monkeypatch):
    graph_engine = GraphEngine(airport_code="KJFK")
    producer = _FakeProducer()
    processor = DelayPropagationProcessor(graph_engine, predictor, producer)

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated graph failure")

    monkeypatch.setattr(graph_engine, "process_event", _boom)

    ok, err = await processor.process(wrap_flight_event(_flight("DP-FAIL"), source=EventSource.MOCK))

    assert ok is False
    assert isinstance(err, RuntimeError)
    assert processor.events_failed == 1


# --- DelayPropagationWorker lifecycle + real end-to-end run() -------------------

async def test_build_delay_propagation_worker_returns_configured_instance(predictor):
    graph_engine = GraphEngine(airport_code="KJFK")
    kafka_producer = KafkaEventProducer()
    worker = build_delay_propagation_worker(
        graph_engine=graph_engine, predictor=predictor, prediction_producer=kafka_producer,
    )
    assert worker.worker_id == "delay-propagation-0"
    assert worker.processor._graph_engine is graph_engine


async def test_delay_propagation_worker_processes_a_real_processed_flights_message(predictor, monkeypatch):
    monkeypatch.setattr(settings, "kafka_consumer_group_predictor", f"test-predictor-{uuid.uuid4().hex[:8]}")

    graph_engine = GraphEngine(airport_code="KJFK")
    prediction_producer = KafkaEventProducer()
    await prediction_producer.start()
    worker = build_delay_propagation_worker(
        graph_engine=graph_engine, predictor=predictor, prediction_producer=prediction_producer,
    )
    await worker.start()
    # processed-flights has years of accumulated backlog from prior test
    # runs; skip straight to "now" instead of replaying it all under
    # auto_offset_reset="earliest" (a real, deliberate choice in
    # DelayPropagationWorker.start() for production catch-up-on-restart
    # behavior, not something to change here) — same technique server.py's
    # own WebSocket handler uses (auto_offset_reset="latest") for the same
    # "don't replay ancient history" reason.
    await worker._consumer.seek_to_end()
    run_task = asyncio.create_task(worker.run())
    try:
        raw_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await raw_producer.start()
        flight_id = f"DPWTEST-{uuid.uuid4().hex[:8]}"
        envelope = wrap_flight_event(_flight(flight_id, delay_minutes=10), source=EventSource.MOCK)
        try:
            await raw_producer.send_and_wait(
                settings.kafka_topic_processed_flights,
                value=envelope.to_json().encode("utf-8"),
                key=flight_id.encode("utf-8"),
            )
        finally:
            await raw_producer.stop()

        # processed-flights already has a large backlog from earlier test
        # runs (this worker's group starts at auto_offset_reset="earliest"),
        # so events_processed alone would go >0 from stale backlog messages
        # long before our own new message is reached — poll for OUR
        # flight_key actually landing in the graph instead of a generic
        # counter.
        target_key = envelope.flight_event.flight_key
        deadline = asyncio.get_event_loop().time() + 30
        while asyncio.get_event_loop().time() < deadline:
            if target_key in graph_engine.graph:
                break
            await asyncio.sleep(0.5)

        assert target_key in graph_engine.graph
        assert worker.processor.events_processed >= 1
    finally:
        worker._stopping = True
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass
        await worker.stop()
        await prediction_producer.stop()
