"""
The envelope every Kafka message on flight-events/processed-flights carries.

Deliberately a separate model from FlightEvent (flight_tracker/models/events.py),
not a replacement for it: FlightEvent is "what do we know about this flight
right now" (used everywhere already — the graph engine, the DB writer, the
WebSocket JSON). FlightEventEnvelope is "what kind of thing happened, when,
and from where" wrapped around one — the metadata Kafka's log needs that a
Postgres row or a WebSocket push never did.
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from flight_tracker.models.events import FlightEvent, FlightStatus


class EnvelopeEventType(str, Enum):
    SCHEDULED = "SCHEDULED"
    BOARDING = "BOARDING"
    DEPARTED = "DEPARTED"
    DELAYED = "DELAYED"
    LANDED = "LANDED"
    CANCELLED = "CANCELLED"


class EventSource(str, Enum):
    FLIGHTAWARE = "FLIGHTAWARE"
    MOCK = "MOCK"
    INTERNAL = "INTERNAL"  # events this system generates itself (e.g. predictions)


class FlightEventEnvelope(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EnvelopeEventType
    timestamp: datetime
    source: EventSource
    flight_event: FlightEvent
    schema_version: int = 1

    @property
    def flight_id(self) -> str:
        """Kafka partition key — see kafka_producer.py."""
        return self.flight_event.flight_id

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: "str | bytes") -> "FlightEventEnvelope":
        """
        Only schema_version=1 exists today. A future breaking change adds a
        branch here keyed on the incoming payload's schema_version — kept as
        a single `model_validate_json` call now rather than pre-building
        version-dispatch logic with nothing on the other end of it yet.
        """
        return cls.model_validate_json(data)


def envelope_event_type_for(flight_event: FlightEvent) -> EnvelopeEventType:
    """
    Maps FlightEvent's own status/delay onto the envelope's coarser
    lifecycle vocabulary. Not a perfect mapping — two known gaps, both
    because nothing in this codebase's actual data sources produces the
    concept:
      - BOARDING has no source anywhere (mock client and the AeroAPI parser
        in ingestion/client.py only ever produce scheduled/active/landed/
        cancelled/diverted) — this function will never emit it.
      - DIVERTED flights map to CANCELLED (closest "not completing as
        planned" analog among the six allowed values), not a perfect fit.
    """
    if flight_event.status in (FlightStatus.CANCELLED, FlightStatus.DIVERTED):
        return EnvelopeEventType.CANCELLED
    if flight_event.status == FlightStatus.LANDED:
        return EnvelopeEventType.LANDED
    if flight_event.delay_minutes > 0:
        return EnvelopeEventType.DELAYED
    if flight_event.status == FlightStatus.ACTIVE:
        return EnvelopeEventType.DEPARTED
    return EnvelopeEventType.SCHEDULED


def wrap_flight_event(
    flight_event: FlightEvent,
    source: EventSource,
    event_type: Optional[EnvelopeEventType] = None,
) -> FlightEventEnvelope:
    """Convenience constructor used by the ingestion worker and consumers."""
    return FlightEventEnvelope(
        event_type=event_type or envelope_event_type_for(flight_event),
        timestamp=datetime.now(timezone.utc),
        source=source,
        flight_event=flight_event,
    )
