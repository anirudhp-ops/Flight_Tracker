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


class AirportSnapshot(BaseModel):
    airport: str
    captured_at: datetime
    flights: list[FlightEvent]

    @property
    def delayed_flights(self) -> list[FlightEvent]:
        return [f for f in self.flights if f.is_delayed]