from enum import Enum
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, field_validator


class EventType(str, Enum):
    DEPARTURE = "departure"
    ARRIVAL = "arrival"
    DELAY = "delay"
    GATE_CHANGE = "gate_change"
    CANCELLATION = "cancellation"
    DIVERSION = "diversion"


class FlightStatus(str, Enum):
    SCHEDULED = "scheduled"
    ACTIVE = "active"
    LANDED = "landed"
    CANCELLED = "cancelled"
    DIVERTED = "diverted"


class FlightEvent(BaseModel):
    flight_id: str
    event_type: EventType
    airline_code: str
    flight_number: str
    origin: str
    destination: str
    aircraft_id: Optional[str] = None
    gate_id: Optional[str] = None
    scheduled_departure: datetime
    estimated_departure: Optional[datetime] = None
    actual_departure: Optional[datetime] = None
    scheduled_arrival: datetime
    estimated_arrival: Optional[datetime] = None
    actual_arrival: Optional[datetime] = None
    delay_minutes: int = 0
    status: FlightStatus
    passenger_count: Optional[int] = None
    timestamp: datetime

    # Real values when the ingestion source provides them (e.g. an AeroAPI
    # field the client parsed). Left None when the source doesn't have them —
    # use estimated_air_time_minutes / estimated_distance_miles below rather
    # than defaulting to 0, since a duration/distance of exactly zero is
    # never a real flight and skews DelayPredictor's output.
    air_time: Optional[float] = None  # minutes
    distance: Optional[float] = None  # statute miles

    @field_validator("delay_minutes")
    @classmethod
    def delay_not_negative(cls, v: int) -> int:
        return max(0, v)

    @property
    def is_delayed(self) -> bool:
        return self.delay_minutes > 0

    @property
    def flight_key(self) -> str:
        date_str = self.scheduled_departure.strftime("%Y%m%d")
        return f"{self.airline_code}{self.flight_number}-{date_str}"

    @property
    def estimated_air_time_minutes(self) -> float:
        """Falls back to the scheduled block time (always available) when
        `air_time` wasn't supplied by the ingestion source."""
        if self.air_time is not None:
            return self.air_time
        delta = (self.scheduled_arrival - self.scheduled_departure).total_seconds() / 60
        return max(0.0, delta)

    @property
    def estimated_distance_miles(self) -> float:
        """Falls back to a rough distance-from-duration estimate (~450 mph
        average commercial cruise speed) when `distance` wasn't supplied.
        This is a coarse placeholder, not real geodata — good enough to
        avoid feeding the model a distance of exactly 0, not good enough to
        replace an actual route-distance lookup."""
        if self.distance is not None:
            return self.distance
        AVG_CRUISE_MPH = 450.0
        return round(self.estimated_air_time_minutes / 60 * AVG_CRUISE_MPH, 1)


class AirportSnapshot(BaseModel):
    airport: str
    captured_at: datetime
    flights: list[FlightEvent]

    @property
    def delayed_flights(self) -> list[FlightEvent]:
        return [f for f in self.flights if f.is_delayed]