"""
Typed WebSocket message envelope (Phase G). Before this, /ws/{airport_code}
forwarded bare FlightEvent JSON with no type discrimination — the frontend
had no way to tell a plain status update apart from a cascaded propagation
result or a gate reassignment, and predicted_delay_minutes/model_confidence/
propagation_source/propagation_hops (all already computed by
flight_tracker/workers/delay_propagation_worker.py since Phase F) never
reached the browser at all. Every message the server now sends is a
WSMessage; every PredictionEvent consumed off delay-predictions maps to
exactly one via classify_prediction_event().

GRAPH_UPDATE (node/edge diff messages for a future advanced graph view) is
deliberately not implemented here — the Phase G task list itself calls it
"for advanced viz," and diffing GraphEngine's networkx structure over the
wire is a materially bigger feature than everything else in this phase
combined, with no current UI consumer for it. Left out rather than built
as an unused placeholder; flagged in PHASE_G_REPORT.md as a real scope cut,
not a silent omission.
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field

from flight_tracker.models.prediction_event import PredictionEvent


class WSMessageType(str, Enum):
    SNAPSHOT = "SNAPSHOT"
    FLIGHT_UPDATE = "FLIGHT_UPDATE"
    DELAY_PREDICTION = "DELAY_PREDICTION"
    PROPAGATION_EVENT = "PROPAGATION_EVENT"
    GATE_REASSIGNMENT = "GATE_REASSIGNMENT"
    HEARTBEAT = "HEARTBEAT"


class WSMessage(BaseModel):
    type: WSMessageType
    timestamp: datetime
    # None only for SNAPSHOT (many flights) and HEARTBEAT (no flight at all).
    flight_id: Optional[str] = None
    data: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: "str | bytes") -> "WSMessage":
        return cls.model_validate_json(raw)


def classify_prediction_event(pe: PredictionEvent) -> WSMessage:
    """
    Maps one PredictionEvent (what DelayPropagationWorker actually
    publishes to delay-predictions, one per touched flight — see that
    worker's own docstring) to exactly one typed WSMessage:

      - GATE_REASSIGNMENT: pe.gate_reassignment is set — this publish
        exists because resolve_gate_conflicts() moved this flight's gate,
        independent of delay/propagation status.
      - PROPAGATION_EVENT: pe.propagation_source is set — this flight's
        delay is a BFS-propagated result of another flight's delay, not
        its own primary update.
      - DELAY_PREDICTION: delay_minutes > 0 and neither of the above — a
        real model prediction was computed for this flight's own delay
        (see ml/predictor.py's predict_with_confidence).
      - FLIGHT_UPDATE: none of the above — an ordinary status update
        (including the trivial non-delayed case DelayPropagationWorker
        always publishes so the frontend keeps seeing every flight, not
        just delayed ones).

    `data` always carries the full flight_event fields merged with the
    prediction/propagation/gate-reassignment fields flattened in, so the
    frontend gets one object with everything a given message type needs
    rather than having to cross-reference by flight_id.
    """
    fe = pe.flight_event
    data: dict[str, Any] = fe.model_dump(mode="json")
    data.update(
        predicted_delay_minutes=pe.predicted_delay_minutes,
        predicted_arrival_time=pe.predicted_arrival_time.isoformat(),
        model_confidence=pe.model_confidence,
        propagation_source=pe.propagation_source,
        propagation_hops=pe.propagation_hops,
        gate_reassignment=(
            pe.gate_reassignment.model_dump(mode="json") if pe.gate_reassignment else None
        ),
    )

    if pe.gate_reassignment is not None:
        msg_type = WSMessageType.GATE_REASSIGNMENT
    elif pe.propagation_source is not None:
        msg_type = WSMessageType.PROPAGATION_EVENT
    elif pe.flight_event.delay_minutes > 0:
        msg_type = WSMessageType.DELAY_PREDICTION
    else:
        msg_type = WSMessageType.FLIGHT_UPDATE

    return WSMessage(type=msg_type, timestamp=datetime.now(timezone.utc), flight_id=pe.flight_id, data=data)


def snapshot_message(flights: list[dict]) -> WSMessage:
    """One SNAPSHOT sent right after connect (and to a late-joining client
    instead of/alongside replay — see server.py). `flights` is a list of
    flight_event-shaped dicts (same shape FLIGHT_UPDATE's `data` carries,
    minus the prediction fields — a snapshot is "what do we know right
    now," not a re-run of the ML pipeline for every flight in the graph)."""
    return WSMessage(
        type=WSMessageType.SNAPSHOT,
        timestamp=datetime.now(timezone.utc),
        data={"flights": flights},
    )


def heartbeat_message() -> WSMessage:
    return WSMessage(type=WSMessageType.HEARTBEAT, timestamp=datetime.now(timezone.utc))
