"""
Process-wide Prometheus metric objects for flight_tracker. Every metric the
backend exposes on GET /metrics (see server.py) is defined here, once, so
instrumentation call sites elsewhere (event_processor.py, worker_pool.py,
delay_propagation_worker.py, redis_cache.py, db/reader.py, db/writer.py,
server.py itself) just import and use them rather than each defining its
own Counter/Histogram/Gauge — a metric name registered twice under
prometheus_client's default REGISTRY raises at import time, so this module
being the single definition point isn't just tidiness, it's required.
"""
from prometheus_client import Counter, Gauge, Histogram

# --- Counters (only go up) --------------------------------------------------
events_received = Counter("events_received_total", "Total events received", ["topic"])
events_processed = Counter("events_processed_total", "Total events processed", ["worker_id"])
events_failed = Counter("events_failed_total", "Total events failed", ["reason"])
predictions_generated = Counter("predictions_generated_total", "Total predictions")
propagations_triggered = Counter("propagations_triggered_total", "Cascades triggered")
cache_hits = Counter("cache_hits_total", "Redis cache hits")
cache_misses = Counter("cache_misses_total", "Redis cache misses")

# --- Histograms (measure latencies) -----------------------------------------
event_processing_latency = Histogram(
    "event_processing_latency_seconds", "Event processing time",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0],
)
propagation_latency = Histogram("propagation_latency_seconds", "Cascade propagation time")
database_query_latency = Histogram(
    "database_query_latency_seconds", "Database query time", ["query_type"]
)
websocket_message_latency = Histogram(
    "websocket_message_latency_seconds", "Message send to frontend"
)

# --- Gauges (current value) -------------------------------------------------
active_websocket_connections = Gauge("active_websocket_connections", "Current WS connections")
graph_node_count = Gauge("graph_node_count", "Flights in graph")
graph_edge_count = Gauge("graph_edge_count", "Edges in graph")
kafka_consumer_lag = Gauge("kafka_consumer_lag", "Consumer lag", ["consumer_group"])
database_connection_pool_size = Gauge("database_connection_pool_size", "Active DB connections")
