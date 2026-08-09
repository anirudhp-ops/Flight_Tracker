# GraphEngine state management: ephemeral, by decision

**The in-memory graph is ephemeral.** It is never persisted, and a process
restart loses it completely. This has been true since it was introduced
and is confirmed here as a deliberate decision, not an oversight to fix.

## What "ephemeral" means concretely

- `GraphEngine.__init__()` starts with an empty `networkx.DiGraph()` —
  nothing loads automatically.
- **On startup**: `server.py` calls `GraphEngine.load_from_db(pool,
  airport_code=...)`, which rebuilds the graph from `active_flights`
  (Postgres) — every row for the tracked airport becomes a node, and
  `add_edges_for_flight` re-derives every `aircraft_turn`/`gate_reuse`
  edge from scratch by re-comparing all loaded flights against each
  other. The graph's edges are not stored anywhere; they're always
  recomputed from node attributes.
- **While running**: `DelayPropagationWorker` (Phase F) is the only thing
  that mutates the graph after startup — `process_event()` for every
  event, `propagate_delay()` / `resolve_gate_conflicts()` for delayed
  ones. `prune_expired_flights()` (Phase B/C) removes landed flights older
  than `PRUNE_MAX_AGE_HOURS` so the graph doesn't grow unbounded for the
  life of the process.
- **On shutdown**: the graph is simply discarded along with the process's
  memory. Nothing writes it anywhere.

## Why this is the right call, not just the current implementation

**Flights are transient, and Postgres is already the durable record.**
`active_flights` (current state) and `flight_events` (full history) are
both persisted and both already reconstructable — see
`flight_tracker/db/README.md`. The graph is a *derived* view over
`active_flights` (which flights exist, plus the aircraft/gate
relationships between them) — recomputing a derived view from its source
of truth on restart is a normal, standard pattern, not a gap. There is
nothing in the graph that couldn't be rebuilt from Postgres.

**Staleness would be worse than emptiness.** If the graph *were*
persisted and reloaded verbatim, a long-stopped process resuming would
reload delay-propagation state and gate assignments that may no longer
reflect reality — flights that landed while the process was down, gates
that were manually reassigned, aircraft that flew different routes than
last known. Rebuilding from `active_flights` on every startup means the
graph always reflects whatever Postgres's current state is *right now*,
not whatever was true when the process last wrote a snapshot.

**The cost of rebuilding is known and small at current scale.** Phase C's
`OPTIMIZATION.md` measured the raw `active_flights` query itself at 4.35ms
for ~1,000 rows — the real cost of `load_from_db()` is the O(n²)
`add_edges_for_flight` re-comparison during the rebuild (documented
precisely in that same file: ~796ms for 998 nodes, i.e., graph
construction, not the database, dominates). That cost is paid once, at
startup, not on every event — an acceptable tradeoff for not having to
maintain a second, independently-serialized copy of graph state.

## What this decision does NOT cover

**In-flight propagation results between a delay event landing and its
next Postgres write can theoretically be lost** if the process crashes in
that exact window — the graph's `delay_minutes` for a propagated flight
updates in memory immediately, but that flight's own next
`flight-events` message (which would re-persist its updated state via the
Phase E worker pool) hasn't necessarily happened yet. In practice this
self-heals: the next real update for that flight re-triggers
`process_event()`/propagation once the graph is rebuilt, and Postgres's
`active_flights` row for it isn't wrong, just not yet caught up to the
graph's latest propagation — no permanent inconsistency, just a bounded
window of the derived graph being briefly ahead of its own source of
truth. Not a new risk introduced by "ephemeral" — the same window exists
with a persisted graph unless propagation writes were made transactional
with the graph snapshot, which they are not.

## Alternative considered: Redis persistence (future work, not built)

A future option, if graph rebuild cost ever becomes a real startup-latency
problem: serialize the graph's nodes/edges to Redis (e.g., on a timer, or
on clean shutdown) and attempt to reload from that snapshot before falling
back to `load_from_db()`. Not built now because:

- It would need its own staleness handling (how old is "too old" for a
  Redis snapshot to trust over rebuilding from Postgres?) — genuine new
  complexity, not a free win.
- `networkx.DiGraph` isn't natively JSON/Redis-friendly; serialization
  would need `networkx.node_link_data`/`node_link_graph` or similar,
  reconstructing `FlightEvent` objects on the way back in (they're stored
  as node attributes, not simple dicts).
- No current measurement suggests startup latency is actually a problem
  worth solving this way — see "cost of rebuilding" above.

If graph rebuild time becomes a real bottleneck at larger scale, this is
the documented next step, not a surprise decision made silently later.
