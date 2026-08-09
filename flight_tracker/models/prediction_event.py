"""
What DelayPropagationWorker (flight_tracker/workers/delay_propagation_worker.py)
publishes to kafka_topic_delay_predictions for every flight it touches —
the triggering flight itself plus any it propagated a delay to or
reassigned a gate for.

Deliberately carries a full `flight_event: FlightEvent`, not just
`flight_id` (the Phase F spec's literal field list omits it): the
WebSocket handler in server.py forwards this topic's messages straight to
the frontend, and the frontend has only ever known how to render a
FlightEvent-shaped payload. Dropping it would mean the handler doing a
DB or in-memory lookup per message just to reconstruct what the producer
already had in hand — a real deviation from the literal spec, made to
keep frontend compatibility, same pattern as the airport_code/gate_pool
and hop-count deviations documented in engine.py and STATE_MANAGEMENT.md.
"""
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from flight_tracker.models.events import FlightEvent


class PredictionEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    flight_id: str
    flight_event: FlightEvent
    predicted_delay_minutes: int
    predicted_arrival_time: datetime
    model_confidence: float = Field(ge=0.0, le=1.0)

    # Set only when this prediction is the result of BFS propagation from
    # another flight's delay (GraphEngine.propagate_delay) rather than a
    # direct prediction for the triggering flight itself.
    propagation_source: Optional[str] = None  # the source flight's flight_key
    propagation_hops: Optional[int] = None

    schema_version: int = 1

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, data: "str | bytes") -> "PredictionEvent":
        return cls.model_validate_json(data)
