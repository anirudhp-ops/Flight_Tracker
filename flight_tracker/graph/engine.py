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