"""
Unit tests for flight_tracker/graph/engine.py (GraphEngine). Pure in-memory
NetworkX logic — no DB/Redis/Kafka needed (load_from_db, the one method that
touches Postgres, is exercised live in test_workers.py-style infra tests,
not here).

Run: pytest flight_tracker/tests/test_graph.py -v
"""
from datetime import datetime, timedelta, timezone

import pytest

from flight_tracker.config import settings
from flight_tracker.graph.engine import GraphEngine
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus


def _flight(flight_id, *, gate_id=None, aircraft_id=None, dep_offset_min=0,
            arr_offset_min=60, delay_minutes=0, status=FlightStatus.SCHEDULED):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return FlightEvent(
        flight_id=flight_id,
        event_type=EventType.DEPARTURE,
        airline_code="AA",
        flight_number=flight_id,
        origin="KJFK",
        destination="KBOS",
        aircraft_id=aircraft_id,
        gate_id=gate_id,
        scheduled_departure=now + timedelta(minutes=dep_offset_min),
        scheduled_arrival=now + timedelta(minutes=arr_offset_min),
        delay_minutes=delay_minutes,
        status=status,
        timestamp=now,
    )


# --- add_edges_for_flight: aircraft turns ------------------------------------

def test_aircraft_turn_edge_created_for_shared_aircraft():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", aircraft_id="N100", dep_offset_min=0, arr_offset_min=60)
    second = _flight("F2", aircraft_id="N100", dep_offset_min=90, arr_offset_min=150)

    engine.process_event(first)
    engine.process_event(second)

    assert engine.graph.has_edge(first.flight_key, second.flight_key)
    assert engine.graph.edges[first.flight_key, second.flight_key]["type"] == "aircraft_turn"


def test_no_aircraft_turn_edge_for_different_aircraft():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", aircraft_id="N100")
    second = _flight("F2", aircraft_id="N200")

    engine.process_event(first)
    engine.process_event(second)

    assert not engine.graph.has_edge(first.flight_key, second.flight_key)
    assert not engine.graph.has_edge(second.flight_key, first.flight_key)


def test_no_aircraft_turn_edge_when_aircraft_id_missing():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", aircraft_id=None)
    second = _flight("F2", aircraft_id=None)

    engine.process_event(first)
    engine.process_event(second)

    assert engine.graph.number_of_edges() == 0


# --- add_edges_for_flight: gate reuse -----------------------------------------

def test_gate_reuse_edge_created_for_overlapping_same_gate_flights():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", gate_id="A1", dep_offset_min=0, arr_offset_min=120)
    second = _flight("F2", gate_id="A1", dep_offset_min=60, arr_offset_min=180)  # overlaps

    engine.process_event(first)
    engine.process_event(second)

    assert engine.graph.has_edge(first.flight_key, second.flight_key)
    assert engine.graph.edges[first.flight_key, second.flight_key]["type"] == "gate_reuse"


def test_no_gate_reuse_edge_for_non_overlapping_same_gate_flights():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", gate_id="A1", dep_offset_min=0, arr_offset_min=60)
    second = _flight("F2", gate_id="A1", dep_offset_min=300, arr_offset_min=360)  # no overlap

    engine.process_event(first)
    engine.process_event(second)

    assert not engine.graph.has_edge(first.flight_key, second.flight_key)


def test_no_gate_reuse_edge_for_different_gates():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", gate_id="A1", dep_offset_min=0, arr_offset_min=120)
    second = _flight("F2", gate_id="B1", dep_offset_min=60, arr_offset_min=180)

    engine.process_event(first)
    engine.process_event(second)

    assert engine.graph.number_of_edges() == 0


def test_no_self_edge_for_same_flight_key():
    engine = GraphEngine(airport_code="KJFK")
    flight = _flight("F1", gate_id="A1", aircraft_id="N100")
    engine.process_event(flight)
    # process_event on the identical flight_key again must not self-loop
    engine.process_event(flight)
    assert flight.flight_key not in engine.graph.successors(flight.flight_key)


# --- propagate_delay: decay formula -------------------------------------------

def test_propagate_delay_single_hop_decays_by_075():
    engine = GraphEngine(airport_code="KJFK")
    source = _flight("F1", aircraft_id="N100")
    neighbor = _flight("F2", aircraft_id="N100")
    engine.process_event(source)
    engine.process_event(neighbor)

    updated = engine.propagate_delay(source.flight_key, 100)

    assert len(updated) == 1
    event_obj, hops = updated[0]
    assert event_obj.flight_id == "F2"
    assert event_obj.delay_minutes == 75  # int(100 * 0.75)
    assert hops == 1


def test_propagate_delay_two_hop_chain_decays_100_75_56():
    """100 -> 75 (hop 1) -> 56 (hop 2), matching int(x * 0.75) at each hop."""
    engine = GraphEngine(airport_code="KJFK")
    f1 = _flight("F1", aircraft_id="N100")
    f2 = _flight("F2", aircraft_id="N100")
    f3 = _flight("F3", aircraft_id="N100")
    engine.process_event(f1)
    engine.process_event(f2)
    engine.process_event(f3)
    # aircraft_turn edges connect every pair sharing N100 (f1->f2, f1->f3, f2->f3);
    # BFS from f1 still reaches f3 at hop 1 directly AND via f2 at hop 2 —
    # use a manually chained graph instead so hop counting is unambiguous.
    engine2 = GraphEngine(airport_code="KJFK")
    engine2.graph.add_node(f1.flight_key, event=f1, aircraft_id=None, gate_id=None,
                            scheduled_departure=f1.scheduled_departure,
                            scheduled_arrival=f1.scheduled_arrival,
                            delay_minutes=0, status=f1.status.value, flight_id=f1.flight_id,
                            airline_code=f1.airline_code, flight_number=f1.flight_number)
    engine2.graph.add_node(f2.flight_key, event=f2, aircraft_id=None, gate_id=None,
                            scheduled_departure=f2.scheduled_departure,
                            scheduled_arrival=f2.scheduled_arrival,
                            delay_minutes=0, status=f2.status.value, flight_id=f2.flight_id,
                            airline_code=f2.airline_code, flight_number=f2.flight_number)
    engine2.graph.add_node(f3.flight_key, event=f3, aircraft_id=None, gate_id=None,
                            scheduled_departure=f3.scheduled_departure,
                            scheduled_arrival=f3.scheduled_arrival,
                            delay_minutes=0, status=f3.status.value, flight_id=f3.flight_id,
                            airline_code=f3.airline_code, flight_number=f3.flight_number)
    engine2.graph.add_edge(f1.flight_key, f2.flight_key, type="aircraft_turn")
    engine2.graph.add_edge(f2.flight_key, f3.flight_key, type="aircraft_turn")

    updated = engine2.propagate_delay(f1.flight_key, 100)
    by_id = {event.flight_id: (event.delay_minutes, hops) for event, hops in updated}

    assert by_id["F2"] == (75, 1)
    assert by_id["F3"] == (56, 2)  # int(75 * 0.75) = 56


def test_propagate_delay_never_regresses_a_larger_existing_delay():
    engine = GraphEngine(airport_code="KJFK")
    source = _flight("F1", aircraft_id="N100")
    neighbor = _flight("F2", aircraft_id="N100", delay_minutes=90)  # already worse than 75
    engine.process_event(source)
    engine.process_event(neighbor)

    updated = engine.propagate_delay(source.flight_key, 100)

    assert updated == []  # max(90, 75) == 90, no change
    assert engine.graph.nodes[neighbor.flight_key]["delay_minutes"] == 90


def test_propagate_delay_sets_event_type_delay_when_new_delay_positive():
    engine = GraphEngine(airport_code="KJFK")
    source = _flight("F1", aircraft_id="N100")
    neighbor = _flight("F2", aircraft_id="N100", delay_minutes=0)
    engine.process_event(source)
    engine.process_event(neighbor)

    updated = engine.propagate_delay(source.flight_key, 20)
    event_obj, _ = updated[0]
    assert event_obj.event_type == EventType.DELAY


def test_propagate_delay_from_unknown_flight_key_returns_empty():
    engine = GraphEngine(airport_code="KJFK")
    assert engine.propagate_delay("NONEXISTENT-20260101", 100) == []


def test_propagate_delay_does_not_revisit_nodes():
    """A diamond (F1 -> F2, F1 -> F3, F2 -> F4, F3 -> F4) must visit F4
    exactly once, not twice."""
    engine = GraphEngine(airport_code="KJFK")
    for fid in ("F1", "F2", "F3", "F4"):
        engine.graph.add_node(fid, event=_flight(fid), aircraft_id=None, gate_id=None,
                               scheduled_departure=datetime.now(timezone.utc),
                               scheduled_arrival=datetime.now(timezone.utc),
                               delay_minutes=0, status="scheduled", flight_id=fid,
                               airline_code="AA", flight_number=fid)
    engine.graph.add_edge("F1", "F2", type="aircraft_turn")
    engine.graph.add_edge("F1", "F3", type="aircraft_turn")
    engine.graph.add_edge("F2", "F4", type="aircraft_turn")
    engine.graph.add_edge("F3", "F4", type="aircraft_turn")

    updated = engine.propagate_delay("F1", 100)
    f4_updates = [u for u in updated if u[0].flight_id == "F4"]
    assert len(f4_updates) == 1


# --- resolve_gate_conflicts ----------------------------------------------------

def test_resolve_gate_conflicts_reassigns_overlapping_flight():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", gate_id="A1", dep_offset_min=0, arr_offset_min=120)
    second = _flight("F2", gate_id="A1", dep_offset_min=60, arr_offset_min=180)
    engine.process_event(first)
    engine.process_event(second)

    reassignments = engine.resolve_gate_conflicts()

    assert len(reassignments) == 1
    r = reassignments[0]
    assert r["flight_key"] == second.flight_key
    assert r["old_gate"] == "A1"
    assert r["new_gate"] != "A1"
    assert engine.graph.nodes[second.flight_key]["gate_id"] == r["new_gate"]
    assert not engine.graph.has_edge(first.flight_key, second.flight_key)


def test_resolve_gate_conflicts_no_conflicts_returns_empty():
    engine = GraphEngine(airport_code="KJFK")
    first = _flight("F1", gate_id="A1", dep_offset_min=0, arr_offset_min=60)
    second = _flight("F2", gate_id="B1", dep_offset_min=0, arr_offset_min=60)
    engine.process_event(first)
    engine.process_event(second)

    assert engine.resolve_gate_conflicts() == []


def test_resolve_gate_conflicts_picks_a_gate_not_already_in_use():
    engine = GraphEngine(airport_code="KJFK")
    gate_pool = engine._gate_pool()
    # Occupy every gate except the last one so the reassignment is forced
    # onto a specific, predictable free gate.
    occupied = gate_pool[:-1]
    free_gate = gate_pool[-1]

    now_offset = 0
    for i, gate in enumerate(occupied):
        f = _flight(f"OCC{i}", gate_id=gate, dep_offset_min=0, arr_offset_min=500)
        engine.process_event(f)

    first = _flight("F1", gate_id="A1", dep_offset_min=0, arr_offset_min=120)
    # A1 is in `occupied` too (first gate) — reuse it deliberately to force a conflict.
    second = _flight("F2", gate_id="A1", dep_offset_min=30, arr_offset_min=150)
    engine.process_event(first)
    engine.process_event(second)

    reassignments = engine.resolve_gate_conflicts()
    assert len(reassignments) == 1
    assert reassignments[0]["new_gate"] == free_gate


def test_resolve_gate_conflicts_no_free_gate_leaves_conflict_unresolved():
    engine = GraphEngine(airport_code="KJFK")
    gate_pool = engine._gate_pool()
    for i, gate in enumerate(gate_pool):
        f = _flight(f"OCC{i}", gate_id=gate, dep_offset_min=0, arr_offset_min=60)
        engine.process_event(f)

    # Force one more overlapping pair on an already-fully-occupied pool.
    first = _flight("F1", gate_id=gate_pool[0], dep_offset_min=0, arr_offset_min=60)
    second = _flight("F2", gate_id=gate_pool[0], dep_offset_min=30, arr_offset_min=90)
    engine.process_event(first)
    engine.process_event(second)

    assert engine.resolve_gate_conflicts() == []


# --- graph pruning --------------------------------------------------------------

def test_prune_expired_flights_removes_old_landed_flights():
    engine = GraphEngine(airport_code="KJFK")
    old_landed = _flight("F1", status=FlightStatus.LANDED,
                          arr_offset_min=-48 * 60)  # landed 48h before "now" reference point
    old_landed.actual_arrival = datetime.now(timezone.utc) - timedelta(hours=48)
    engine.process_event(old_landed)

    removed = engine.prune_expired_flights(max_age_hours=24)

    assert removed == [old_landed.flight_key]
    assert old_landed.flight_key not in engine.graph


def test_prune_expired_flights_keeps_recent_landed_flights():
    engine = GraphEngine(airport_code="KJFK")
    recent_landed = _flight("F1", status=FlightStatus.LANDED)
    recent_landed.actual_arrival = datetime.now(timezone.utc) - timedelta(hours=1)
    engine.process_event(recent_landed)

    removed = engine.prune_expired_flights(max_age_hours=24)

    assert removed == []
    assert recent_landed.flight_key in engine.graph


def test_prune_expired_flights_keeps_non_landed_flights_regardless_of_age():
    engine = GraphEngine(airport_code="KJFK")
    old_active = _flight("F1", status=FlightStatus.ACTIVE)
    old_active.scheduled_arrival = datetime.now(timezone.utc) - timedelta(hours=100)
    engine.process_event(old_active)

    removed = engine.prune_expired_flights(max_age_hours=24)

    assert removed == []
    assert old_active.flight_key in engine.graph


def test_prune_expired_flights_falls_back_to_scheduled_arrival_when_no_actual():
    engine = GraphEngine(airport_code="KJFK")
    old_landed = _flight("F1", status=FlightStatus.LANDED, arr_offset_min=-48 * 60)
    # actual_arrival left None deliberately
    engine.process_event(old_landed)

    removed = engine.prune_expired_flights(max_age_hours=24)
    assert removed == [old_landed.flight_key]


# --- gate pool ------------------------------------------------------------------

def test_gate_pool_generated_from_settings_when_no_override():
    engine = GraphEngine(airport_code="KJFK")
    pool = engine._gate_pool()
    expected_count = len(settings.gate_pool_terminals) * settings.gate_pool_gates_per_terminal
    assert len(pool) == expected_count
    assert pool[0] == f"{settings.gate_pool_terminals[0]}1"


def test_gate_pool_uses_override_when_present(monkeypatch):
    monkeypatch.setitem(settings.gate_pool_overrides, "KTEST", ["X1", "X2"])
    engine = GraphEngine(airport_code="KTEST")
    assert engine._gate_pool() == ["X1", "X2"]
