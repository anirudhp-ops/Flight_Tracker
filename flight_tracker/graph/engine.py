import networkx as nx
from flight_tracker.models.events import FlightEvent


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
                    self.graph.add_edge(node_key, new_flight.flight_key, type="gate_reuse")
                    