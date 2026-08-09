"""
Real end-to-end test of DelayPropagationProcessor against a real GraphEngine
and a real trained DelayPredictor (ml/model.pkl) — only the Kafka producer
is faked, since this test's purpose is verifying the processor's own logic
(gate-conflict detection -> GateReassignmentDetail threading -> a
GATE_REASSIGNMENT-classifiable PredictionEvent), not Kafka itself (already
covered live and in test_websocket_messages.py).

Run: pytest flight_tracker/tests/test_delay_propagation_worker.py -v
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from flight_tracker.graph.engine import GraphEngine
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus
from flight_tracker.events.event_model import EventSource, wrap_flight_event
from flight_tracker.websocket.messages import WSMessageType, classify_prediction_event
from flight_tracker.workers.delay_propagation_worker import DelayPropagationProcessor
from ml.predictor import DelayPredictor

MODEL_PATH = str(Path(__file__).resolve().parent.parent.parent / "ml" / "model.pkl")


class _FakeProducer:
    def __init__(self):
        self.published = []

    async def publish(self, topic, event):
        self.published.append((topic, event))


def _flight(flight_id, *, gate_id, dep_offset_min, arr_offset_min, aircraft_id=None):
    now = datetime.now(timezone.utc)
    return FlightEvent(
        flight_id=flight_id,
        event_type=EventType.DEPARTURE,
        airline_code="AA",
        flight_number=flight_id,
        origin="KJFK",
        destination="KBOS",
        aircraft_id=aircraft_id,
        gate_id=gate_id,
        scheduled_departure=now + timedelta(minutes=dep_offset_min),
        scheduled_arrival=now + timedelta(minutes=arr_offset_min),
        delay_minutes=0,
        status=FlightStatus.SCHEDULED,
        timestamp=now,
    )


@pytest.mark.asyncio
async def test_real_gate_conflict_produces_a_gate_reassignment_message():
    """Two flights with genuinely overlapping schedules at the same gate
    forces GraphEngine.resolve_gate_conflicts() to actually reassign one —
    real graph logic, not a mocked outcome."""
    graph_engine = GraphEngine(airport_code="KJFK")
    predictor = DelayPredictor(MODEL_PATH)
    producer = _FakeProducer()
    processor = DelayPropagationProcessor(graph_engine, predictor, producer)

    first = _flight("GC001", gate_id="A1", dep_offset_min=60, arr_offset_min=180)
    second = _flight("GC002", gate_id="A1", dep_offset_min=90, arr_offset_min=210)  # overlaps first

    ok, err = await processor.process(wrap_flight_event(first, source=EventSource.MOCK))
    assert ok, err
    ok, err = await processor.process(wrap_flight_event(second, source=EventSource.MOCK))
    assert ok, err

    reassignment_publishes = [
        (topic, pe) for topic, pe in producer.published if pe.gate_reassignment is not None
    ]
    assert len(reassignment_publishes) == 1, (
        f"expected exactly one gate-reassignment publish, got {len(reassignment_publishes)}"
    )
    _, pe = reassignment_publishes[0]
    assert pe.gate_reassignment.old_gate == "A1"
    assert pe.gate_reassignment.new_gate != "A1"
    assert pe.gate_reassignment.new_gate is not None

    ws_msg = classify_prediction_event(pe)
    assert ws_msg.type == WSMessageType.GATE_REASSIGNMENT
    assert ws_msg.data["gate_reassignment"]["new_gate"] == pe.gate_reassignment.new_gate


@pytest.mark.asyncio
async def test_non_overlapping_same_gate_flights_do_not_reassign():
    graph_engine = GraphEngine(airport_code="KJFK")
    predictor = DelayPredictor(MODEL_PATH)
    producer = _FakeProducer()
    processor = DelayPropagationProcessor(graph_engine, predictor, producer)

    first = _flight("GC101", gate_id="B1", dep_offset_min=60, arr_offset_min=120)
    second = _flight("GC102", gate_id="B1", dep_offset_min=300, arr_offset_min=360)  # no overlap

    await processor.process(wrap_flight_event(first, source=EventSource.MOCK))
    await processor.process(wrap_flight_event(second, source=EventSource.MOCK))

    assert all(pe.gate_reassignment is None for _, pe in producer.published)
