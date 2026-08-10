"""
Unit tests for flight_tracker/ingestion/client.py — pure parsing/generation
logic (_compute_delay, _parse_fa_datetime, _raw_flight_to_event,
MockFlightAwareClient). No network calls: FlightAwareClient itself (the
httpx-backed live client) is intentionally not exercised here — hitting
the real AeroAPI is exactly what settings.enable_flightaware_api guards
against, and this project's test_api.py (a manual, cost-gated script) is
the appropriate place for that, not an automated unit test.

Run: pytest flight_tracker/tests/test_ingestion_client.py -v
"""
from datetime import datetime, timezone

import pytest

from flight_tracker.ingestion.client import (
    MockFlightAwareClient,
    _compute_delay,
    _parse_fa_datetime,
    _raw_flight_to_event,
)
from flight_tracker.models.events import EventType, FlightStatus


# --- _compute_delay --------------------------------------------------------------

def test_compute_delay_none_estimated_is_zero():
    assert _compute_delay(datetime(2026, 1, 1, tzinfo=timezone.utc), None) == 0


def test_compute_delay_positive_delta():
    sched = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    est = datetime(2026, 1, 1, 10, 25, tzinfo=timezone.utc)
    assert _compute_delay(sched, est) == 25


def test_compute_delay_early_arrival_clamped_to_zero():
    sched = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    est = datetime(2026, 1, 1, 9, 45, tzinfo=timezone.utc)
    assert _compute_delay(sched, est) == 0


# --- _parse_fa_datetime -----------------------------------------------------------

def test_parse_fa_datetime_none_returns_none():
    assert _parse_fa_datetime(None) is None


def test_parse_fa_datetime_empty_string_returns_none():
    assert _parse_fa_datetime("") is None


def test_parse_fa_datetime_parses_z_suffix():
    result = _parse_fa_datetime("2026-01-01T10:00:00Z")
    assert result == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)


# --- _raw_flight_to_event ----------------------------------------------------------

def _raw(**overrides):
    base = {
        "scheduled_out": "2026-01-01T10:00:00Z",
        "scheduled_in": "2026-01-01T14:00:00Z",
        "fa_flight_id": "FA123",
        "ident": "AA100",
        "operator_iata": "AA",
        "flight_number": "100",
        "origin": {"code_icao": "KJFK"},
        "destination": {"code_icao": "KLAX"},
        "status": "Scheduled",
    }
    base.update(overrides)
    return base


def test_raw_flight_to_event_missing_scheduled_out_returns_none():
    raw = _raw(scheduled_out=None)
    assert _raw_flight_to_event(raw, datetime.now(timezone.utc)) is None


def test_raw_flight_to_event_missing_scheduled_in_returns_none():
    raw = _raw(scheduled_in=None)
    assert _raw_flight_to_event(raw, datetime.now(timezone.utc)) is None


def test_raw_flight_to_event_parses_scheduled_flight():
    event = _raw_flight_to_event(_raw(), datetime.now(timezone.utc))
    assert event is not None
    assert event.flight_id == "FA123"
    assert event.origin == "KJFK"
    assert event.destination == "KLAX"
    assert event.status == FlightStatus.SCHEDULED
    assert event.event_type == EventType.DEPARTURE


def test_raw_flight_to_event_falls_back_to_ident_when_fa_flight_id_missing():
    event = _raw_flight_to_event(_raw(fa_flight_id=""), datetime.now(timezone.utc))
    assert event.flight_id == "AA100"


def test_raw_flight_to_event_cancelled_status():
    event = _raw_flight_to_event(_raw(status="Cancelled"), datetime.now(timezone.utc))
    assert event.status == FlightStatus.CANCELLED
    assert event.event_type == EventType.CANCELLATION


def test_raw_flight_to_event_diverted_status():
    event = _raw_flight_to_event(_raw(status="Diverted"), datetime.now(timezone.utc))
    assert event.status == FlightStatus.DIVERTED
    assert event.event_type == EventType.DIVERSION


def test_raw_flight_to_event_landed_when_actual_in_present():
    event = _raw_flight_to_event(_raw(actual_in="2026-01-01T14:05:00Z"), datetime.now(timezone.utc))
    assert event.status == FlightStatus.LANDED
    assert event.event_type == EventType.ARRIVAL


def test_raw_flight_to_event_active_when_actual_out_present():
    event = _raw_flight_to_event(_raw(actual_out="2026-01-01T10:05:00Z"), datetime.now(timezone.utc))
    assert event.status == FlightStatus.ACTIVE
    assert event.event_type == EventType.DEPARTURE


def test_raw_flight_to_event_delay_from_departure_delay_seconds():
    event = _raw_flight_to_event(_raw(departure_delay=1800), datetime.now(timezone.utc))  # 30 min
    assert event.delay_minutes == 30
    assert event.event_type == EventType.DELAY


def test_raw_flight_to_event_uses_route_distance_when_present():
    event = _raw_flight_to_event(_raw(route_distance=2475), datetime.now(timezone.utc))
    assert event.distance == 2475.0


def test_raw_flight_to_event_gate_id_combines_terminal_and_gate():
    event = _raw_flight_to_event(
        _raw(terminal_origin="4", gate_origin="12"), datetime.now(timezone.utc)
    )
    assert event.gate_id == "4-12"


def test_raw_flight_to_event_gate_id_none_when_both_missing():
    event = _raw_flight_to_event(_raw(), datetime.now(timezone.utc))
    assert event.gate_id is None


def test_raw_flight_to_event_malformed_record_returns_none_not_raises():
    """A record so broken it can't even be indexed as expected (e.g.
    origin is a string, not a dict) is skipped, not fatal to the poll."""
    raw = _raw(origin="not-a-dict")
    assert _raw_flight_to_event(raw, datetime.now(timezone.utc)) is None


# --- MockFlightAwareClient ---------------------------------------------------------

async def test_mock_client_seeds_twenty_flights():
    client = MockFlightAwareClient("KJFK")
    assert len(client._flights) == 20


async def test_mock_client_get_airport_flights_returns_snapshot():
    client = MockFlightAwareClient("KJFK")
    snapshot = await client.get_airport_flights("KJFK")
    assert snapshot.airport == "KJFK"
    assert len(snapshot.flights) == 20


async def test_mock_client_seeded_flights_originate_at_target_airport():
    client = MockFlightAwareClient("KBOS")
    snapshot = await client.get_airport_flights("KBOS")
    assert all(f.origin == "KBOS" for f in snapshot.flights)


async def test_mock_client_repeated_polls_return_same_flight_ids():
    """Drift simulation mutates existing flights' delays in place; it must
    not add or remove flights between polls."""
    client = MockFlightAwareClient("KJFK")
    first = {f.flight_id for f in (await client.get_airport_flights("KJFK")).flights}
    second = {f.flight_id for f in (await client.get_airport_flights("KJFK")).flights}
    assert first == second


async def test_mock_client_source_is_mock():
    assert MockFlightAwareClient.source.value == "MOCK"
