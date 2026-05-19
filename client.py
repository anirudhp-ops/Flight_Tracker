import random
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from flight_tracker.models.events import (
    AirportSnapshot,
    EventType,
    FlightEvent,
    FlightStatus,
)


AEROAPI_BASE_URL = "https://aeroapi.flightaware.com/aeroapi"


def _compute_delay(scheduled: datetime, estimated: datetime | None) -> int:
    """Returns delay in whole minutes, clamped to zero for early arrivals."""
    if estimated is None:
        return 0
    delta = (estimated - scheduled).total_seconds() / 60
    return max(0, int(delta))


def _parse_fa_datetime(value: str | None) -> datetime | None:
    """FlightAware returns ISO-8601 strings with Z suffix."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _raw_flight_to_event(raw: dict[str, Any], captured_at: datetime) -> FlightEvent | None:
    """
    Converts a single flight dict from the AeroAPI response into a FlightEvent.
    Returns None if required fields are missing (we skip incomplete records).
    """
    try:
        scheduled_out = _parse_fa_datetime(raw.get("scheduled_out"))
        scheduled_in = _parse_fa_datetime(raw.get("scheduled_in"))
        if not scheduled_out or not scheduled_in:
            return None

        estimated_out = _parse_fa_datetime(raw.get("estimated_out"))
        actual_out = _parse_fa_datetime(raw.get("actual_out"))
        estimated_in = _parse_fa_datetime(raw.get("estimated_in"))
        actual_in = _parse_fa_datetime(raw.get("actual_in"))

        delay_minutes = int(raw.get("departure_delay", 0) or 0) // 60
        # Map FlightAware status strings to our internal enum
        fa_status = raw.get("status", "").lower()
        if "cancelled" in fa_status:
            status = FlightStatus.CANCELLED
            event_type = EventType.CANCELLATION
        elif "diverted" in fa_status:
            status = FlightStatus.DIVERTED
            event_type = EventType.DIVERSION
        elif actual_in:
            status = FlightStatus.LANDED
            event_type = EventType.ARRIVAL
        elif actual_out:
            status = FlightStatus.ACTIVE
            event_type = EventType.DEPARTURE
        elif delay_minutes > 0:
            status = FlightStatus.SCHEDULED
            event_type = EventType.DELAY
        else:
            status = FlightStatus.SCHEDULED
            event_type = EventType.DEPARTURE

        fa_id = raw.get("fa_flight_id", "")
        ident = raw.get("ident", "")
        airline_code = raw.get("operator_iata", "XX")
        flight_number = raw.get("flight_number", "")
        return FlightEvent(
            flight_id=fa_id or ident,
            event_type=event_type,
            airline_code=airline_code,
            flight_number=flight_number,
            origin=raw.get("origin", {}).get("code_icao", ""),
            destination=raw.get("destination", {}).get("code_icao", ""),
            aircraft_id=raw.get("registration"),
            gate_id=f"{raw.get('terminal_origin', '')}-{raw.get('gate_origin', '')}".strip("-"),
            scheduled_departure=scheduled_out,
            estimated_departure=estimated_out,
            actual_departure=actual_out,
            scheduled_arrival=scheduled_in,
            estimated_arrival=estimated_in,
            actual_arrival=actual_in,
            delay_minutes=delay_minutes,
            status=status,
            passenger_count=None,  # AeroAPI doesn't provide this directly
            timestamp=captured_at,
        )
    except Exception:
        # Individual bad records get skipped, not crash the whole poll
        return None


class FlightAwareClient:
    """
    Thin async wrapper around the AeroAPI flights endpoint.
    Use MockFlightAwareClient below during development.
    """

    def __init__(self, api_key: str):
        self._headers = {
            "x-apikey": api_key,
            "Accept": "application/json; charset=UTF-8",
        }

    async def get_airport_flights(self, airport_icao: str) -> AirportSnapshot:
        captured_at = datetime.now(timezone.utc)
        url = f"{AEROAPI_BASE_URL}/airports/{airport_icao}/flights"

        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(url, headers=self._headers)
            response.raise_for_status()
            data = response.json()

        raw_flights = data.get("arrivals", []) + data.get("departures", [])
        events = [e for raw in raw_flights if (e := _raw_flight_to_event(raw, captured_at))]

        return AirportSnapshot(
            airport=airport_icao,
            captured_at=captured_at,
            flights=events,
        )


class MockFlightAwareClient:
    """
    Generates realistic-looking fake flight data so you can develop
    the rest of the system without API credentials.
    Swap this out for FlightAwareClient once you have a key.
    """

    AIRLINES = [("UA", "United"), ("AA", "American"), ("DL", "Delta"), ("WN", "Southwest")]
    AIRPORTS = ["KLAX", "KORD", "KJFK", "KATL", "KDFW", "KDEN", "KLAS", "KSEA"]

    def __init__(self, airport_icao: str):
        self._airport = airport_icao
        self._flights: dict[str, FlightEvent] = {}
        self._seed_flights()

    def _seed_flights(self) -> None:
        """Create an initial set of flights on first call."""
        now = datetime.now(timezone.utc)
        for i in range(20):
            airline_code, _ = random.choice(self.AIRLINES)
            flight_number = str(random.randint(100, 9999))
            scheduled_dep = now + timedelta(minutes=random.randint(-60, 240))
            scheduled_arr = scheduled_dep + timedelta(hours=random.randint(1, 5))
            delay = random.choices([0, random.randint(10, 180)], weights=[0.7, 0.3])[0]
            estimated_dep = scheduled_dep + timedelta(minutes=delay) if delay else None
            status = random.choices(
                [FlightStatus.SCHEDULED, FlightStatus.ACTIVE, FlightStatus.LANDED],
                weights=[0.5, 0.35, 0.15],
            )[0]

            flight_id = f"{airline_code}{flight_number}-mock-{i}"
            self._flights[flight_id] = FlightEvent(
                flight_id=flight_id,
                event_type=EventType.DELAY if delay else EventType.DEPARTURE,
                airline_code=airline_code,
                flight_number=flight_number,
                origin=self._airport,
                destination=random.choice(self.AIRPORTS),
                aircraft_id=f"N{random.randint(10000, 99999)}",
                gate_id=f"{random.choice('ABCDE')}{random.randint(1, 30)}",
                scheduled_departure=scheduled_dep,
                estimated_departure=estimated_dep,
                actual_departure=None,
                scheduled_arrival=scheduled_arr,
                estimated_arrival=None,
                actual_arrival=None,
                delay_minutes=delay,
                status=status,
                passenger_count=random.randint(50, 220),
                timestamp=datetime.now(timezone.utc),
            )

    async def get_airport_flights(self, airport_icao: str) -> AirportSnapshot:
        """Simulate drift: randomly update delays on existing flights each poll."""
        now = datetime.now(timezone.utc)

        for flight in self._flights.values():
            if random.random() < 0.15:  # 15% chance of a delay change per poll
                extra = random.randint(5, 30)
                flight.delay_minutes += extra
                flight.event_type = EventType.DELAY
                flight.timestamp = now

        return AirportSnapshot(
            airport=airport_icao,
            captured_at=now,
            flights=list(self._flights.values()),
        )
