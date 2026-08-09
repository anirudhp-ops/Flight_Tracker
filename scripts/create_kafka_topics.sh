#!/usr/bin/env bash
# Idempotent — safe to re-run. Uses --if-not-exists, so an already-created
# topic is a no-op rather than an error.
#
# Usage:
#   scripts/create_kafka_topics.sh                          # localhost:9092 (native broker or docker-compose)
#   KAFKA_BOOTSTRAP_SERVERS=host:9092 scripts/create_kafka_topics.sh
#
# Needs the Kafka CLI tools on PATH. On macOS: brew install kafka. Set
# KAFKA_BIN_DIR below if kafka-topics isn't already on PATH (e.g. the brew
# keg's bin/ dir isn't symlinked).

set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-}"

if [ -n "$KAFKA_BIN_DIR" ]; then
    KAFKA_TOPICS="$KAFKA_BIN_DIR/kafka-topics"
elif command -v kafka-topics >/dev/null 2>&1; then
    KAFKA_TOPICS="kafka-topics"
elif command -v kafka-topics.sh >/dev/null 2>&1; then
    KAFKA_TOPICS="kafka-topics.sh"
else
    # Common Homebrew keg location when the versioned bin/ isn't on PATH.
    FOUND=$(ls -d /opt/homebrew/Cellar/kafka/*/bin/kafka-topics 2>/dev/null | sort -V | tail -1 || true)
    if [ -n "$FOUND" ]; then
        KAFKA_TOPICS="$FOUND"
    else
        echo "kafka-topics not found on PATH and no Homebrew keg detected." >&2
        echo "Install with 'brew install kafka' or set KAFKA_BIN_DIR." >&2
        exit 1
    fi
fi

echo "Using: $KAFKA_TOPICS"
echo "Bootstrap servers: $BOOTSTRAP_SERVERS"
echo

create_topic() {
    local name="$1" partitions="$2" retention_ms="$3"
    echo "Creating topic '$name' (partitions=$partitions, retention.ms=$retention_ms)..."
    "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --create --if-not-exists \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config "retention.ms=$retention_ms"
}

SEVEN_DAYS_MS=$((7 * 24 * 60 * 60 * 1000))
THIRTY_DAYS_MS=$((30 * 24 * 60 * 60 * 1000))

# Raw ingested events, keyed by flight_id (see kafka_producer.py). 3
# partitions: enough to parallelize the flight-processor consumer group
# across a few instances later without being wasteful for a single-broker
# dev setup.
create_topic "flight-events" 3 "$SEVEN_DAYS_MS"

# Validated/enriched output of the flight-processor consumer.
create_topic "processed-flights" 3 "$SEVEN_DAYS_MS"

# ML delay predictions, consumed by the WebSocket handler.
create_topic "delay-predictions" 3 "$SEVEN_DAYS_MS"

# Failed events for debugging (scripts/inspect_dlq.py). 1 partition: DLQ
# volume should be low, and a single partition keeps failures in one
# reviewable, roughly-time-ordered stream instead of scattered across three.
# Longer retention (30d) than the happy-path topics — you need time to
# actually notice and investigate a failure, not just replay it same-day.
create_topic "dead-letter-events" 1 "$THIRTY_DAYS_MS"

echo
echo "Topics:"
"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVERS" --list
