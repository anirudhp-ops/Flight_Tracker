import networkx as nx
from flight_tracker.models.events import FlightEvent
from collections import deque


class GraphEngine:
    def __init__(self):
        self.graph = nx.DiGraph()
    
    def add_flight(self, flight: FlightEvent) -> None:
        self.graph.add_node(
            flight.flight_key,
            flight_id=flight.flight_id,
            airline_code=flight.airline_code,
            flight_number=flight.flight_number,
            aircraft_id=flight.aircraft_id,
            gate_id=flight.gate_id,
            scheduled_departure=flight.scheduled_departure,
            scheduled_arrival=flight.scheduled_arrival,
            delay_minutes=flight.delay_minutes,
            status=flight.status.value,
    )
    def add_edges_for_flight(self, new_flight: FlightEvent) -> None:
        for node_key, attrs in self.graph.nodes(data=True):
            if node_key == new_flight.flight_key:
                continue

            # aircraft_turn: same physical plane
            if attrs["aircraft_id"] and new_flight.aircraft_id:
                if attrs["aircraft_id"] == new_flight.aircraft_id:
                    self.graph.add_edge(node_key, new_flight.flight_key, type="aircraft_turn")

            # gate_reuse: same gate, overlapping time windows
            if attrs["gate_id"] and new_flight.gate_id:
                if attrs["gate_id"] == new_flight.gate_id:
                    a_dep = attrs["scheduled_departure"]
                    a_arr = attrs["scheduled_arrival"]
                    b_dep = new_flight.scheduled_departure
                    b_arr = new_flight.scheduled_arrival
                    overlaps = not (a_arr < b_dep or b_arr < a_dep)
                    if overlaps:
                         self.graph.add_edge(node_key, new_flight.flight_key, type="gate_reuse")
    
    async def load_from_db(self, pool) -> None:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM active_flights")
            for row in rows:
                event = FlightEvent(
                    flight_id=row["flight_id"],
                    event_type=EventType(row["event_type"]),
                    airline_code=row["airline_code"],
                    flight_number=row["flight_number"],
                    origin=row["origin"],
                    destination=row["destination"],
                    aircraft_id=row["aircraft_id"],
                    gate_id=row["gate_id"],
                    scheduled_departure=row["scheduled_departure"],
                    estimated_departure=row["estimated_departure"],
                    actual_departure=row["actual_departure"],
                    scheduled_arrival=row["scheduled_arrival"],
                    estimated_arrival=row["estimated_arrival"],
                    actual_arrival=row["actual_arrival"],
                    delay_minutes=row["delay_minutes"],
                    status=FlightStatus(row["status"]),
                    passenger_count=row["passenger_count"],
                    timestamp=row["timestamp"],
                )
                self.add_flight(event)
                self.add_edges_for_flight(event)

    def process_event(self, event: FlightEvent) -> None:
        self.add_flight(event)
        self.add_edges_for_flight(event)
    

    def propagate_delay(self, flight_key: str, delay_minutes: int) -> None:
        queue = deque()
        queue.append((flight_key, delay_minutes))
        visited = set()

        while queue:
            current_key, current_delay = queue.popleft()
            if current_key in visited:
                continue
            visited.add(current_key)

            for neighbor_key in self.graph.neighbors(current_key):
                if neighbor_key in visited:
                    continue
                neighbor_delay = self.graph.nodes[neighbor_key]["delay_minutes"]
                propagated = int(current_delay * 0.75)
                new_delay = max(neighbor_delay, propagated)
                self.graph.nodes[neighbor_key]["delay_minutes"] = new_delay
                queue.append((neighbor_key, new_delay))
                
    def resolve_gate_conflicts(self) -> list[dict]:
        gate_pool = [f"{terminal}{num}" for terminal in ["A","B","C","T"] for num in range(1,15)]
        used_gates = {attrs["gate_id"] for _, attrs in self.graph.nodes(data=True) if attrs["gate_id"]}
        reassignments = []

        for src, dst, data in self.graph.edges(data=True):
            if data.get("type") != "gate_reuse":
                continue

            src_attrs = self.graph.nodes[src]
            dst_attrs = self.graph.nodes[dst]

            a_dep = src_attrs["scheduled_departure"]
            a_arr = src_attrs["scheduled_arrival"]
            b_dep = dst_attrs["scheduled_departure"]
            b_arr = dst_attrs["scheduled_arrival"]
            overlaps = not (a_arr < b_dep or b_arr < a_dep)

            if not overlaps:
                continue

            free_gates = [g for g in gate_pool if g not in used_gates]
            if not free_gates:
                continue

            new_gate = free_gates[0]
            used_gates.add(new_gate)
            self.graph.nodes[dst]["gate_id"] = new_gate
            self.graph.remove_edge(src, dst)

            reassignments.append({
                "flight_key": dst,
                "old_gate": dst_attrs["gate_id"],
                "new_gate": new_gate,
            })

        return reassignments            