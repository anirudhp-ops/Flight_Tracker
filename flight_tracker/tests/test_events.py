"""
Unit tests for flight_tracker/models/events.py (FlightEvent, EventType,
FlightStatus) and flight_tracker/events/event_model.py
(FlightEventEnvelope). Pure model/validation logic, no I/O — no infra
required.

Run: pytest flight_tracker/tests/test_events.py -v
"""
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from flight_tracker.events.event_model import (
    EnvelopeEventType,
    EventSource,
    FlightEventEnvelope,
    envelope_event_type_for,
    wrap_flight_event,
)
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus


def _flight(**overrides) -> FlightEvent:
    now = datetime.now(timezone.utc)
    defaults = dict(
        flight_id="AA100",
        event_type=EventType.DEPARTURE,
        airline_code="AA",
        flight_number="100",
        origin="KJFK",
        destination="KLAX",
        scheduled_departure=now,
        scheduled_arrival=now + timedelta(hours=5),
        delay_minutes=0,
        status=FlightStatus.SCHEDULED,
        timestamp=now,
    )
    defaults.update(overrides)
    return FlightEvent(**defaults)


# --- FlightEvent validation: valid cases -------------------------------------

def test_valid_flight_event_constructs():
    event = _flight()
    assert event.flight_id == "AA100"
    assert event.status == FlightStatus.SCHEDULED


def test_valid_flight_event_with_all_optional_fields():
    now = datetime.now(timezone.utc)
    event = _flight(
        aircraft_id="N12345",
        gate_id="A1",
        estimated_departure=now,
        actual_departure=now,
        estimated_arrival=now,
        actual_arrival=now,
        passenger_count=180,
        air_time=300.0,
        distance=2475.0,
    )
    assert event.aircraft_id == "N12345"
    assert event.passenger_count == 180


# --- FlightEvent validation: invalid / edge cases ----------------------------

def test_missing_required_field_raises():
    with pytest.raises(ValidationError):
        FlightEvent(
            flight_id="AA100",
            event_type=EventType.DEPARTURE,
            airline_code="AA",
            flight_number="100",
            origin="KJFK",
            destination="KLAX",
            # scheduled_departure missing
            scheduled_arrival=datetime.now(timezone.utc),
            status=FlightStatus.SCHEDULED,
            timestamp=datetime.now(timezone.utc),
        )


def test_invalid_event_type_raises():
    with pytest.raises(ValidationError):
        _flight(event_type="not-a-real-event-type")


def test_invalid_status_raises():
    with pytest.raises(ValidationError):
        _flight(status="not-a-real-status")


def test_negative_delay_minutes_clamped_to_zero():
    """delay_not_negative validator: negative input is clamped, not rejected."""
    event = _flight(delay_minutes=-15)
    assert event.delay_minutes == 0


def test_zero_delay_minutes_stays_zero():
    event = _flight(delay_minutes=0)
    assert event.delay_minutes == 0


# --- FlightEvent computed properties -----------------------------------------

def test_is_delayed_true_when_positive_delay():
    assert _flight(delay_minutes=10).is_delayed is True


def test_is_delayed_false_when_zero_delay():
    assert _flight(delay_minutes=0).is_delayed is False


def test_flight_key_format():
    dep = datetime(2026, 3, 15, 8, 0, tzinfo=timezone.utc)
    event = _flight(airline_code="UA", flight_number="42", scheduled_departure=dep)
    assert event.flight_key == "UA42-20260315"


def test_estimated_air_time_minutes_uses_real_value_when_present():
    event = _flight(air_time=123.0)
    assert event.estimated_air_time_minutes == 123.0


def test_estimated_air_time_minutes_falls_back_to_schedule_delta():
    dep = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    arr = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    event = _flight(scheduled_departure=dep, scheduled_arrival=arr, air_time=None)
    assert event.estimated_air_time_minutes == 120.0


def test_estimated_air_time_minutes_never_negative():
    """An (invalid, but not schema-rejected) arrival-before-departure pair
    must not produce a negative estimated air time."""
    dep = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    arr = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    event = _flight(scheduled_departure=dep, scheduled_arrival=arr, air_time=None)
    assert event.estimated_air_time_minutes == 0.0


def test_estimated_distance_miles_uses_real_value_when_present():
    event = _flight(distance=999.5)
    assert event.estimated_distance_miles == 999.5


def test_estimated_distance_miles_falls_back_to_speed_estimate():
    dep = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    arr = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)  # 2 hours
    event = _flight(scheduled_departure=dep, scheduled_arrival=arr, distance=None)
    assert event.estimated_distance_miles == 900.0  # 2h * 450mph


# --- EventType / FlightStatus transitions ------------------------------------

def test_event_type_values_are_stable_strings():
    """These strings are on the wire (Kafka JSON, DB TEXT columns) — a
    changed value would silently break stored/in-flight data."""
    assert EventType.DEPARTURE.value == "departure"
    assert EventType.ARRIVAL.value == "arrival"
    assert EventType.DELAY.value == "delay"
    assert EventType.GATE_CHANGE.value == "gate_change"
    assert EventType.CANCELLATION.value == "cancellation"
    assert EventType.DIVERSION.value == "diversion"


def test_flight_status_values_are_stable_strings():
    assert FlightStatus.SCHEDULED.value == "scheduled"
    assert FlightStatus.ACTIVE.value == "active"
    assert FlightStatus.LANDED.value == "landed"
    assert FlightStatus.CANCELLED.value == "cancelled"
    assert FlightStatus.DIVERTED.value == "diverted"


def test_legal_status_transition_scheduled_to_active():
    event = _flight(status=FlightStatus.SCHEDULED)
    event.status = FlightStatus.ACTIVE
    assert event.status == FlightStatus.ACTIVE


def test_legal_status_transition_active_to_landed_with_delay():
    event = _flight(status=FlightStatus.ACTIVE, delay_minutes=0)
    event.status = FlightStatus.LANDED
    event.delay_minutes = 45
    event.event_type = EventType.DELAY
    assert event.status == FlightStatus.LANDED
    assert event.is_delayed is True


def test_reassigning_status_to_invalid_value_raises():
    """Pydantic v2 models are validate_assignment=False by default, but
    assigning a raw string that FlightStatus can't coerce still raises —
    exercising that this isn't silently accepted."""
    event = _flight()
    with pytest.raises(ValueError):
        FlightStatus("not-a-status")


# --- AirportSnapshot.delayed_flights -----------------------------------------

def test_airport_snapshot_delayed_flights_filters_correctly():
    from flight_tracker.models.events import AirportSnapshot

    flights = [_flight(flight_id="A1", delay_minutes=0), _flight(flight_id="A2", delay_minutes=30)]
    snapshot = AirportSnapshot(airport="KJFK", captured_at=datetime.now(timezone.utc), flights=flights)
    assert [f.flight_id for f in snapshot.delayed_flights] == ["A2"]


# --- FlightEventEnvelope serialization ---------------------------------------

def test_envelope_round_trips_through_json():
    event = _flight()
    envelope = wrap_flight_event(event, source=EventSource.MOCK)
    raw = envelope.to_json()
    rebuilt = FlightEventEnvelope.from_json(raw)
    assert rebuilt.event_id == envelope.event_id
    assert rebuilt.flight_id == event.flight_id
    assert rebuilt.flight_event.flight_number == event.flight_number
    assert rebuilt.source == EventSource.MOCK
    assert rebuilt.schema_version == 1


def test_envelope_flight_id_property_delegates_to_flight_event():
    event = _flight(flight_id="ZZ999")
    envelope = wrap_flight_event(event, source=EventSource.INTERNAL)
    assert envelope.flight_id == "ZZ999"


def test_envelope_event_id_defaults_to_unique_uuid():
    event = _flight()
    e1 = wrap_flight_event(event, source=EventSource.MOCK)
    e2 = wrap_flight_event(event, source=EventSource.MOCK)
    assert e1.event_id != e2.event_id


@pytest.mark.parametrize(
    "status,delay,expected",
    [
        (FlightStatus.CANCELLED, 0, EnvelopeEventType.CANCELLED),
        (FlightStatus.DIVERTED, 0, EnvelopeEventType.CANCELLED),
        (FlightStatus.LANDED, 0, EnvelopeEventType.LANDED),
        (FlightStatus.LANDED, 30, EnvelopeEventType.LANDED),  # LANDED wins over DELAYED
        (FlightStatus.ACTIVE, 15, EnvelopeEventType.DELAYED),
        (FlightStatus.ACTIVE, 0, EnvelopeEventType.DEPARTED),
        (FlightStatus.SCHEDULED, 0, EnvelopeEventType.SCHEDULED),
    ],
)
def test_envelope_event_type_for_maps_status_and_delay(status, delay, expected):
    event = _flight(status=status, delay_minutes=delay)
    assert envelope_event_type_for(event) == expected


def test_wrap_flight_event_explicit_event_type_overrides_inference():
    event = _flight(status=FlightStatus.SCHEDULED, delay_minutes=0)
    envelope = wrap_flight_event(event, source=EventSource.MOCK, event_type=EnvelopeEventType.BOARDING)
    assert envelope.event_type == EnvelopeEventType.BOARDING


def test_envelope_invalid_json_raises():
    with pytest.raises(ValidationError):
        FlightEventEnvelope.from_json(b"not json at all")
