"""
DelayPropagationWorker: the single instance that owns the one GraphEngine.
Consumes processed-flights, mutates the graph, resolves gate conflicts,
propagates delays, generates ML predictions (with real per-prediction
confidence — see ml/predictor.py's predict_with_confidence), and publishes
one PredictionEvent per touched flight to delay-predictions.

Deliberately NOT part of the Phase E WorkerPool (N concurrent instances
sharing one consumer group). GraphEngine is single, in-process, in-memory
state (see flight_tracker/graph/STATE_MANAGEMENT.md) — N instances in the
same consumer group would each receive a different, incomplete slice of
processed-flights (Kafka splits partitions across group members), and each
would build a different fragment of the graph. A flight's aircraft-turn/
gate-reuse neighbors could easily land on a different worker's shard than
the flight itself, silently corrupting propagation and gate-conflict
resolution. This worker therefore runs under its own consumer group
(settings.kafka_consumer_group_predictor — reused from Phase D's now-
deleted events/delay_prediction_consumer.py, since this worker supersedes
exactly that role) so it always receives every partition, i.e. the
complete picture.

Still gets crash-restart supervision for free despite being single-
instance: this module's DelayPropagationWorker duck-types
flight_tracker/workers/worker_pool.py's Worker interface (worker_id,
start()/stop()/run(), a .processor with the same counters, .restarts,
.failure_handler, an async lag()) closely enough to be wrapped in
`WorkerPool([this])` + `Supervisor(...)` unmodified — see server.py's
wiring (Phase F task #74).
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.abc import ConsumerRebalanceListener

from flight_tracker.config import settings
from flight_tracker.events.event_model import FlightEventEnvelope
from flight_tracker.events.kafka_producer import KafkaEventProducer
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.models.events import FlightEvent
from flight_tracker.models.prediction_event import GateReassignmentDetail, PredictionEvent
from flight_tracker.workers.failure_handler import FailureHandler
from ml.predictor import DelayPredictor


class _RebalanceLogger(ConsumerRebalanceListener):
    """Identical to worker_pool.py's — duplicated rather than imported
    because worker_pool.py is a sibling module for a different consumer
    group, not a shared-utilities module; importing across them for one
    tiny listener class isn't worth the coupling."""

    def __init__(self, label: str):
        self._label = label

    async def on_partitions_revoked(self, revoked):
        if revoked:
            print(f"INFO: [{self._label}] partitions revoked (rebalance starting): {list(revoked)}")

    async def on_partitions_assigned(self, assigned):
        if assigned:
            print(f"INFO: [{self._label}] partitions assigned (rebalance complete): {list(assigned)}")


class DelayPropagationProcessor:
    """
    Per-message pipeline: graph mutation -> gate-conflict resolution ->
    (only if delayed) propagation + ML prediction -> publish one
    PredictionEvent per touched flight.

    No DB write here — GraphEngine is the only state this worker owns;
    persisting flight state to Postgres stays entirely the Phase E pool's
    job, upstream of this one (flight_tracker/workers/event_processor.py).

    No Redis idempotency guard either, unlike AsyncEventProcessor. That
    guard exists there because multiple concurrent workers share one
    consumer group and could each independently pick up a redelivered
    message. This worker is single-instance, so Kafka's own commit-after-
    process offset handling (see run(), below) is the only replay guard
    needed: a redelivered processed-flights message just re-derives the
    same graph state. add_flight/add_edges_for_flight overwrite/re-derive
    rather than accumulate, and propagate_delay's max()-merge means
    re-applying an already-applied propagation never double-counts a
    delay. idempotent_skips therefore always stays 0 — kept only for
    interface parity with worker_pool.py's Worker/AsyncEventProcessor
    (WorkerPool's total_idempotent_skips sums across all wrapped workers),
    not because this processor ever actually skips anything.

    retries_attempted also always stays 0: the only retry-wrapped call this
    processor makes is KafkaEventProducer.publish() (retried internally via
    @retry_with_backoff), and — matching event_processor.py's own
    convention, where retries_attempted only counts the DB-write retry
    wrapper it built with on_retry=self._bump_retries — publish() failures
    were never counted there either. Kept for the same interface-parity
    reason as idempotent_skips.
    """

    def __init__(self, graph_engine: GraphEngine, predictor: DelayPredictor,
                 prediction_producer: KafkaEventProducer):
        self._graph_engine = graph_engine
        self._predictor = predictor
        self._prediction_producer = prediction_producer

        self.events_processed = 0
        self.events_failed = 0
        self.idempotent_skips = 0
        self.retries_attempted = 0
        self.latencies_ms: list[float] = []

    async def process(self, envelope: FlightEventEnvelope) -> tuple[bool, Optional[BaseException]]:
        """Never raises — returns (success, error), same calling convention
        as AsyncEventProcessor.process_flight_event() so run() below can
        branch on it the same way worker_pool.py's Worker.run() does."""
        start = time.perf_counter()
        try:
            event = envelope.flight_event

            # Unconditional — preserves the old event_processor.py's
            # behavior of building aircraft_turn/gate_reuse edges for every
            # flight, not just delayed ones. A later flight's delay may
            # need to propagate across an edge from an earlier, never-
            # delayed flight, so the graph must stay complete regardless of
            # whether *this* event is itself a delay.
            self._graph_engine.process_event(event)

            # Also unconditional, and also matching preserved behavior —
            # this deviates from the task brief's pseudocode, which nests
            # gate-conflict resolution inside the delay>0 branch. Gate
            # conflicts aren't inherently delay-related: two flights can
            # collide on a gate purely from a schedule overlap with no
            # delay involved, so this must run for every event.
            reassignments = self._graph_engine.resolve_gate_conflicts()

            propagated: list[tuple[FlightEvent, int]] = []
            # Trivial case for a non-delayed triggering flight: no model
            # ran, so there's nothing to be uncertain about — confidence=1.0
            # is "certain," not "the model is confident."
            source_predicted: float = float(event.delay_minutes)
            source_confidence = 1.0

            if event.delay_minutes > 0:
                source_predicted, source_confidence = self._predictor.predict_with_confidence(
                    airline_code=event.airline_code,
                    origin=event.origin,
                    destination=event.destination,
                    dep_delay=event.delay_minutes,
                    air_time=event.estimated_air_time_minutes,
                    distance=event.estimated_distance_miles,
                )
                propagated = self._graph_engine.propagate_delay(event.flight_key, event.delay_minutes)

            # Always publish for the triggering flight itself, even a
            # non-delayed one, so the frontend keeps seeing every flight on
            # delay-predictions, not just delayed ones — the same
            # frontend-compatibility reasoning behind PredictionEvent
            # carrying a full flight_event (see prediction_event.py).
            await self._publish(
                event,
                predicted_delay_minutes=int(round(source_predicted)),
                model_confidence=source_confidence,
            )

            for propagated_event, hops in propagated:
                predicted, confidence = self._predictor.predict_with_confidence(
                    airline_code=propagated_event.airline_code,
                    origin=propagated_event.origin,
                    destination=propagated_event.destination,
                    dep_delay=propagated_event.delay_minutes,
                    air_time=propagated_event.estimated_air_time_minutes,
                    distance=propagated_event.estimated_distance_miles,
                )
                await self._publish(
                    propagated_event,
                    predicted_delay_minutes=int(round(predicted)),
                    model_confidence=confidence,
                    # event.flight_id, not event.flight_key: flight_key is
                    # only meaningful inside GraphEngine's own graph (its
                    # nodes are keyed by it, which is why propagate_delay()
                    # itself is called with event.flight_key above) — every
                    # externally-visible identifier (this PredictionEvent's
                    # own top-level flight_id, every other message's
                    # flight_id) is the ingestion-assigned flight_id, so
                    # propagation_source has to match that or a consumer
                    # (Phase G's frontend, keying its flights map by
                    # flight_id) could never actually resolve "the source
                    # flight" this propagated from. Found via building that
                    # consumer, not assumed correct beforehand.
                    propagation_source=event.flight_id,
                    propagation_hops=hops,
                )

            for reassignment in reassignments:
                reassigned_event = reassignment.get("event")
                if reassigned_event is None:
                    continue
                # A gate reassignment isn't a model output — delay_minutes
                # is whatever it already was, confidence=1.0 for the same
                # "certain, not model-confident" reason as the trivial
                # source case above. old_gate/new_gate come straight from
                # resolve_gate_conflicts()'s own reassignment dict (Phase F)
                # — reassigned_event.gate_id is already the *new* gate by
                # this point, so old_gate has to come from the dict, not
                # the event.
                await self._publish(
                    reassigned_event,
                    predicted_delay_minutes=reassigned_event.delay_minutes,
                    model_confidence=1.0,
                    gate_reassignment=GateReassignmentDetail(
                        old_gate=reassignment.get("old_gate"),
                        new_gate=reassignment["new_gate"],
                    ),
                )

            self.events_processed += 1
            self.latencies_ms.append((time.perf_counter() - start) * 1000)
            return True, None
        except Exception as e:
            self.events_failed += 1
            return False, e

    async def _publish(
        self,
        event: FlightEvent,
        *,
        predicted_delay_minutes: int,
        model_confidence: float,
        propagation_source: Optional[str] = None,
        propagation_hops: Optional[int] = None,
        gate_reassignment: Optional[GateReassignmentDetail] = None,
    ) -> None:
        prediction = PredictionEvent(
            flight_id=event.flight_id,
            flight_event=event,
            predicted_delay_minutes=predicted_delay_minutes,
            predicted_arrival_time=event.estimated_arrival or event.scheduled_arrival,
            model_confidence=model_confidence,
            gate_reassignment=gate_reassignment,
            propagation_source=propagation_source,
            propagation_hops=propagation_hops,
        )
        await self._prediction_producer.publish(settings.kafka_topic_delay_predictions, prediction)


class DelayPropagationWorker:
    """Single instance — see module docstring for why this must never be
    horizontally scaled the way worker_pool.py's Worker is."""

    def __init__(
        self,
        worker_id: str,
        graph_engine: GraphEngine,
        predictor: DelayPredictor,
        prediction_producer: KafkaEventProducer,
    ):
        self.worker_id = worker_id
        self.processor = DelayPropagationProcessor(graph_engine, predictor, prediction_producer)
        self.restarts = 0  # bumped by the Supervisor, not this class — matches Worker
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self.failure_handler: FailureHandler | None = None
        self._stopping = False

    async def start(self) -> None:
        self._stopping = False
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group_predictor,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._consumer.subscribe(
            topics=[settings.kafka_topic_processed_flights],
            listener=_RebalanceLogger(
                f"{settings.kafka_topic_processed_flights}/"
                f"{settings.kafka_consumer_group_predictor}/{self.worker_id}"
            ),
        )
        await self._consumer.start()
        self._dlq_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
        await self._dlq_producer.start()
        self.failure_handler = FailureHandler(self._dlq_producer)
        print(f"DelayPropagationWorker {self.worker_id} started")

    async def stop(self) -> None:
        self._stopping = True
        if self._consumer is not None:
            await self._consumer.stop()
        if self._dlq_producer is not None:
            await self._dlq_producer.stop()
        print(
            f"DelayPropagationWorker {self.worker_id} stopped "
            f"(processed={self.processor.events_processed}, failed={self.processor.events_failed}, "
            f"restarts={self.restarts})"
        )

    async def lag(self) -> int:
        if self._consumer is None:
            return 0
        total = 0
        for tp in self._consumer.assignment():
            highwater = self._consumer.highwater(tp)
            if highwater is None:
                continue
            position = await self._consumer.position(tp)
            total += max(0, highwater - position)
        return total

    async def run(self) -> None:
        """
        Consume -> process -> (dead-letter on failure) -> commit, forever,
        until stop() is called or this coroutine's task is cancelled.
        Mirrors worker_pool.py's Worker.run() exactly, including parsing
        living inside the same try/except as processing (a malformed
        processed-flights message must dead-letter, not escape run() and
        trigger an infinite crash-restart loop on the same un-committed
        offset — see that file's docstring for the real bug this pattern
        was built to fix). Left to raise on a genuinely unexpected error
        (e.g. the Kafka connection itself failing) — supervisor.py is what
        catches that and restarts this worker.
        """
        assert self._consumer is not None, "call start() before run()"
        try:
            async for message in self._consumer:
                if self._stopping:
                    break

                event_id = None
                flight_id = None
                try:
                    envelope = FlightEventEnvelope.from_json(message.value)
                    event_id = str(envelope.event_id)
                    flight_id = envelope.flight_id
                    success, error = await self.processor.process(envelope)
                except Exception as parse_error:
                    success, error = False, parse_error

                if not success:
                    await self.failure_handler.handle_failure(
                        event_id=event_id,
                        flight_id=flight_id,
                        raw_payload=message.value,
                        error=error,
                        worker_id=self.worker_id,
                        original_topic=message.topic,
                        original_partition=message.partition,
                        original_offset=message.offset,
                    )
                await self._consumer.commit()
        except asyncio.CancelledError:
            print(f"DelayPropagationWorker {self.worker_id} cancelled, shutting down")
            raise


def build_delay_propagation_worker(
    graph_engine: GraphEngine,
    predictor: DelayPredictor,
    prediction_producer: KafkaEventProducer,
) -> DelayPropagationWorker:
    """
    One worker, always — see module docstring. Kept as a factory function
    (rather than constructing DelayPropagationWorker directly in server.py)
    only for symmetry with worker_pool.py's build_worker_pool(), which
    server.py also calls; there's no hidden fan-out here to configure.
    """
    return DelayPropagationWorker(
        worker_id="delay-propagation-0",
        graph_engine=graph_engine,
        predictor=predictor,
        prediction_producer=prediction_producer,
    )
