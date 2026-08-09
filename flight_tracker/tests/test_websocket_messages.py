"""
Unit tests for flight_tracker/websocket/messages.py — pure classification
logic, no I/O, so no real infra needed here (unlike test_workers.py).

Run: pytest flight_tracker/tests/test_websocket_messages.py -v
"""
from datetime import datetime, timezone

from flight_tracker.models.events import EventType, FlightEvent, FlightStatus
from flight_tracker.models.prediction_event import GateReassignmentDetail, PredictionEvent
from flight_tracker.websocket.messages import (
    WSMessageType,
    classify_prediction_event,
    heartbeat_message,
    snapshot_message,
)


def _flight_event(**overrides) -> FlightEvent:
    now = datetime.now(timezone.utc)
    defaults = dict(
        flight_id="AA100-test",
        event_type=EventType.DELAY,
        airline_code="AA",
        flight_number="100",
        origin="KJFK",
        destination="KLAX",
        scheduled_departure=now,
        scheduled_arrival=now,
        delay_minutes=0,
        status=FlightStatus.ACTIVE,
        timestamp=now,
    )
    defaults.update(overrides)
    return FlightEvent(**defaults)


def _prediction_event(**overrides) -> PredictionEvent:
    fe = overrides.pop("flight_event", None) or _flight_event()
    now = datetime.now(timezone.utc)
    defaults = dict(
        flight_id=fe.flight_id,
        flight_event=fe,
        predicted_delay_minutes=0,
        predicted_arrival_time=now,
        model_confidence=1.0,
    )
    defaults.update(overrides)
    return PredictionEvent(**defaults)


def test_trivial_non_delayed_flight_classifies_as_flight_update():
    pe = _prediction_event(flight_event=_flight_event(delay_minutes=0))
    msg = classify_prediction_event(pe)
    assert msg.type == WSMessageType.FLIGHT_UPDATE
    assert msg.flight_id == "AA100-test"
    assert msg.data["delay_minutes"] == 0


def test_delayed_flight_with_no_propagation_or_reassignment_is_delay_prediction():
    pe = _prediction_event(
        flight_event=_flight_event(delay_minutes=40),
        predicted_delay_minutes=30,
        model_confidence=0.7,
    )
    msg = classify_prediction_event(pe)
    assert msg.type == WSMessageType.DELAY_PREDICTION
    assert msg.data["predicted_delay_minutes"] == 30
    assert msg.data["model_confidence"] == 0.7


def test_propagated_flight_is_propagation_event_even_with_zero_delay_source_flag():
    pe = _prediction_event(
        flight_event=_flight_event(delay_minutes=20),
        propagation_source="SOURCE-1",
        propagation_hops=2,
    )
    msg = classify_prediction_event(pe)
    assert msg.type == WSMessageType.PROPAGATION_EVENT
    assert msg.data["propagation_source"] == "SOURCE-1"
    assert msg.data["propagation_hops"] == 2


def test_gate_reassignment_takes_priority_over_delay_status():
    # A gate reassignment can co-occur with a real delay on the same
    # flight — gate_reassignment must still win the classification, since
    # it's what actually changed about this specific publish.
    pe = _prediction_event(
        flight_event=_flight_event(delay_minutes=15, gate_id="B2"),
        gate_reassignment=GateReassignmentDetail(old_gate="A1", new_gate="B2"),
    )
    msg = classify_prediction_event(pe)
    assert msg.type == WSMessageType.GATE_REASSIGNMENT
    assert msg.data["gate_reassignment"] == {"old_gate": "A1", "new_gate": "B2"}


def test_gate_reassignment_with_no_previous_gate():
    pe = _prediction_event(
        flight_event=_flight_event(gate_id="C4"),
        gate_reassignment=GateReassignmentDetail(old_gate=None, new_gate="C4"),
    )
    msg = classify_prediction_event(pe)
    assert msg.type == WSMessageType.GATE_REASSIGNMENT
    assert msg.data["gate_reassignment"]["old_gate"] is None


def test_snapshot_message_carries_a_flights_list():
    msg = snapshot_message([{"flight_id": "AA1"}, {"flight_id": "AA2"}])
    assert msg.type == WSMessageType.SNAPSHOT
    assert msg.flight_id is None
    assert len(msg.data["flights"]) == 2


def test_heartbeat_message_has_no_flight_id_or_data():
    msg = heartbeat_message()
    assert msg.type == WSMessageType.HEARTBEAT
    assert msg.flight_id is None
    assert msg.data == {}


def test_wsmessage_round_trips_through_json():
    pe = _prediction_event(
        flight_event=_flight_event(delay_minutes=10),
        propagation_source="SRC",
        propagation_hops=1,
    )
    msg = classify_prediction_event(pe)
    raw = msg.to_json()
    rebuilt = type(msg).from_json(raw)
    assert rebuilt.type == msg.type
    assert rebuilt.flight_id == msg.flight_id
    assert rebuilt.data["propagation_source"] == "SRC"
