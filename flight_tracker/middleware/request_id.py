"""
Per-HTTP-request UUID, generated once at the edge and threaded through the
rest of that request's handling. Two things use it:

  - Every log line emitted while handling the request can carry it via
    `extra={"request_id": ...}` (see flight_tracker/logging_config.py),
    letting one request's log lines be grep'd/joined across a busy server's
    interleaved output.
  - The response carries it back as X-Request-ID, so a client (or a human
    correlating a bug report against the logs) has the same id to search for.

Kafka's own event-driven pipeline (ingestion -> worker pool ->
DelayPropagationWorker -> WebSocket stream) doesn't route through this
middleware at all — it has no inbound HTTP request to attach an id to. That
pipeline already carries its own natural per-event correlation id,
FlightEventEnvelope.event_id (flight_tracker/events/event_model.py),
end-to-end from publish through every downstream consumer; the
instrumentation added in Phase J logs that under the same `request_id` JSON
field rather than minting a second, redundant id — see event_processor.py
and delay_propagation_worker.py.
"""
import uuid

from starlette.requests import Request
from starlette.responses import Response


async def add_request_id(request: Request, call_next) -> Response:
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response
