"""
Structured JSON logging for flight_tracker. Every log line becomes one JSON
object — timestamp, level, logger, message, request_id/flight_id/worker_id
(always present, None when not applicable), plus whatever else a call site
passed via `extra=` (e.g. latency_ms) — so logs stay machine-parseable and
joinable by request_id across the ingestion worker, the persistence pool,
DelayPropagationWorker, and the WebSocket handler, instead of the free-text
print() calls used before Phase J. See flight_tracker/OBSERVABILITY.md for
the logging conventions (levels, what to log where).
"""
import json
import logging

# Every attribute a bare LogRecord already carries (see logging.LogRecord's
# own __init__) — anything else on the record came from a caller's `extra=`
# dict and belongs in the JSON payload as its own field, not just the three
# the Phase J brief's pseudocode names explicitly.
_STANDARD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()) | {
    "message",
    "asctime",
}
_ALWAYS_PRESENT = ("request_id", "flight_id", "worker_id")


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in _ALWAYS_PRESENT:
            log_data[key] = getattr(record, key, None)
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and key not in _ALWAYS_PRESENT:
                log_data[key] = value
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """
    Call once, at process startup (server.py's module import), before any
    other module's `logging.getLogger(__name__)` emits its first line —
    handlers attach to the root logger here, so every module's logger
    propagates up to this one JSON-formatted StreamHandler without needing
    its own per-module setup.

    Idempotent: pytest can import server.py more than once across a test
    session, and this guards against stacking a second handler (which would
    duplicate every subsequent log line) if configure_logging() is ever
    called twice in one process.
    """
    root = logging.getLogger()
    root.setLevel(level)
    if any(isinstance(h, logging.StreamHandler) and isinstance(h.formatter, JSONFormatter) for h in root.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)
