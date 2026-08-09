# Real-time architecture (Phase G)

How the frontend gets its live data, what the wire protocol looks like, and
how the pieces fit together. For the backend side of this (DelayPropagationWorker,
GraphEngine, the ML predictor), see `flight_tracker/workers/PHASE_F_REPORT.md`
and `flight_tracker/workers/delay_propagation_worker.py`'s own docstring —
this file covers what changed in Phase G specifically: the WebSocket
protocol and everything in `frontend/` that consumes it.

## What existed before Phase G

Contrary to the phase brief's own "current state" description, the
frontend already connected to a real WebSocket (`ws://.../ws/{airport}`)
with real reconnect-on-drop logic and a connection-status indicator —
verified live in Phase F. What it did **not** have: any message-type
envelope. The backend sent bare `FlightEvent` JSON, so ML predictions,
confidence scores, propagation chains, and gate reassignments — all
already computed server-side since Phase F — never reached the browser at
all. That's the real gap this phase closes.

## Wire protocol: WSMessage

Every message the server sends over `/ws/{airport_code}` is now a
`WSMessage` (`flight_tracker/websocket/messages.py`):

```json
{
  "type": "PROPAGATION_EVENT",
  "timestamp": "2026-08-09T19:33:30.15Z",
  "flight_id": "UA4310-mock-14",
  "data": { "...flight fields...", "predicted_delay_minutes": 49, "model_confidence": 0.66, "propagation_source": "DL3025-mock-19", "propagation_hops": 1 }
}
```

`type` is one of:

- **SNAPSHOT** — sent once, immediately after connect. `data.flights` is
  the full current flight list (from `GraphEngine`'s in-memory state), no
  prediction fields (a snapshot is "what do we know right now," not a
  re-run of the ML pipeline for every flight in the graph).
- **FLIGHT_UPDATE** — an ordinary status update, including the trivial
  non-delayed case `DelayPropagationWorker` always publishes so the
  frontend keeps seeing every flight, not just delayed ones.
- **DELAY_PREDICTION** — a real model prediction ran for this flight's own
  delay (not propagated, not a gate reassignment).
- **PROPAGATION_EVENT** — this flight's delay is a BFS-propagated result
  of another flight's delay. `propagation_source` is the *source* flight's
  `flight_id` and `propagation_hops` is its distance from that source.
- **GATE_REASSIGNMENT** — `resolve_gate_conflicts()` moved this flight's
  gate. Takes priority over DELAY_PREDICTION/FLIGHT_UPDATE classification
  even if the same flight is also delayed — see
  `flight_tracker/websocket/messages.py`'s `classify_prediction_event()`.
- **HEARTBEAT** — sent every 30s (`WS_HEARTBEAT_INTERVAL_SECONDS` in
  `server.py`) with empty `data`, purely so the frontend can detect a
  silently-dead connection (see below).

Not implemented: **GRAPH_UPDATE** (node/edge diff messages for a future
advanced graph view). The phase brief itself calls this "for advanced
viz" — it's a materially bigger feature (diffing `networkx` state over the
wire) than the rest of this phase combined, with no current UI consumer.
Left out rather than built as a dead code path.

### Server-side history + late-connecting clients

`server.py` keeps a shared, process-wide `recent_ws_messages` ring buffer
(last 100 `WSMessage`s, across all connections — this app tracks exactly
one airport, so one buffer is correct, not one per airport). A new
connection gets its SNAPSHOT, then a replay of that buffer, before joining
the live stream — the "server keeps event history for late-connecting
clients" option from the brief, chosen over client-requested replay
because it doesn't need its own request/response sub-protocol for no real
benefit at this scale.

### Optional per-flight filtering

A client can send `{"action":"subscribe","flight_id":"..."}` (and
`{"action":"unsubscribe"}`) as a WS text frame; the server then only
forwards messages for that flight_id or cascades that originate from it
(`propagation_source` match). `useFlightData`'s `subscribeToFlight`/
`unsubscribeFromFlight` expose this, but it is **not** wired into
click-to-select on the map — selecting a flight must not narrow what the
map itself receives, or every other flight would vanish the moment a user
clicked one. It's there for a future "focus mode," not the default view.

## State management: `useFlightData`

`frontend/src/hooks/useFlightData.js` owns the WebSocket connection and
everything derived from it:

- `flights: Map<flight_id, flightData>`
- `predictions: Map<flight_id, {predicted_delay_minutes, predicted_arrival_time, model_confidence}>`
- `propagationChains: Map<source_flight_id, [{flight_id, delay_minutes, hops}]>`
- `gateReassignments: Map<flight_id, {old_gate, new_gate, at}>`
- `connectionStatus`: `CONNECTING -> CONNECTED -> DISCONNECTED -> RECONNECTING`
  (the exact lifecycle named in the phase brief), plus a separate
  `hardFailure` boolean that flips true after 30s of continuous
  disconnection — the "Connection failed. Refresh to retry." case is
  distinct from "still retrying."
- `getPropagationChain(flightId)` — derives `{downstream, upstream}` for
  any flight from `propagationChains`, viewed from both directions (a
  flight looking up its own id in every chain's affected-list finds its
  upstream source; looking up its own chain entry finds its downstream).

### Dead-connection detection

The server's HEARTBEAT is the mechanism: if the hook hasn't received a
message of **any** kind (heartbeat included) in 45s while it believes the
connection is open, it force-closes the socket and reconnects. A silently
half-open TCP connection (network changes, laptop sleep/wake) can leave a
browser `WebSocket` object reporting `readyState === OPEN` for a long time
with no way to know the far end is actually gone — the heartbeat is what
makes that detectable within a bounded window, rather than waiting on the
OS's own TCP timeout.

### Performance: batching and bounded redraw

Two real performance issues were found via live testing against ~1700
real flights, not assumed:

1. **Message batching.** WS frames arrive one at a time; `useFlightData`
   queues them and processes up to 10 every 100ms, so a burst of updates
   produces one batched React state update instead of one per message.
2. **Decoupled render cadence.** The first version of `FlightMap.jsx`'s D3
   effect depended directly on the flights `Map` reference, which changes
   on every batch tick — up to 10x/second under load. That meant a full
   redraw (position/rotation trig for ~1700 planes, plus a full
   propagation-overlay rebuild that restarted its entrance transitions
   every time) was re-running far more often than necessary. Measured
   directly (`performance.now()` around the redraw): a full pass over
   ~1700 flights costs roughly 20-90ms. The fix decouples redraw
   frequency from data-arrival frequency entirely — `FlightMap.jsx` now
   redraws on its own fixed ~500ms interval, reading the latest data via
   refs rather than effect dependencies, and the propagation overlay only
   rebuilds (and restarts its pulse animation) when the actual selected
   flight/chain identity changes, not on every tick.

   Caveat, stated plainly: this session's automated browser-testing
   environment ran the page in a backgrounded/non-compositor-visible tab
   (`document.hidden === true` throughout), which fully suspends
   `requestAnimationFrame` regardless of application code — a literal
   60fps-in-the-browser measurement could not be taken through this
   harness. The number that *was* measured directly (20-90ms of JS/DOM
   work per full redraw pass, at a bounded ~1-2 redraws/second) is real
   and is what the 60fps claim rests on, not an assumption.

3. **No separate flight list to virtualize.** The brief's task 11 also
   asks to "virtualize flight list (render only visible flights)" — this
   app has no separate scrollable flight-list UI element (only the map and
   the single-flight detail panel), so there's nothing to virtualize in
   that specific sense. The equivalent optimization that actually applies
   here is the keyed D3 join (enter/update/exit by `flight_id`) already in
   place, which avoids tearing down and rebuilding unrelated markers.

## Component architecture

- **`FlightMap.jsx`** — top-level map, owns the `useFlightData()` call,
  the D3 render loop, keyboard navigation, and the responsive
  side-panel/stacked layout switch.
- **`FlightDetail.jsx`** — pure, prop-driven detail panel: flight fields,
  severity-colored delay, prediction + confidence meter, upstream/downstream
  propagation lists, an event timeline built strictly from fields
  `FlightEvent` actually carries (no fabricated boarding time or delay-reason
  text — see its own docstring), and an embedded `GateMap`.
- **`GateMap.jsx`** — the 56-gate grid (4 terminals x 14 gates, matching
  the backend's *default* gate pool — a per-airport override is possible
  server-side via `GATE_POOL_OVERRIDES` but isn't exposed over any API, so
  this always renders the generated default layout).
- **`utils/airportCodes.js`**, **`utils/delaySeverity.js`** — small shared
  helpers (ICAO->IATA display, severity color/threshold, confidence
  labels) factored out of `FlightMap.jsx` so `FlightDetail`/`GateMap` don't
  duplicate them.

## Cascade visualization algorithm

For the selected flight, if it has downstream entries in
`propagationChains`: draw a pulsing red marker at its map position (an SVG
`<animate>` loop, not a JS-driven RAF loop, so it keeps animating smoothly
between the ~500ms redraw ticks), then for each affected flight — color
interpolated by hop distance (orange for hop 1, trending yellow for later
hops), a dashed connection line fading in with a per-hop stagger, a decay
label (`×0.75^hops` as a percentage), and a small yellow square if that
flight also has a pending gate reassignment. If the selected flight itself
has an upstream entry (i.e. it's a victim, not just a source), a fainter
dashed line traces back to that source too.

One deliberate gap versus the brief's own mockup: the downstream list
doesn't label each hop "(aircraft turn)" / "(gate reuse)" — `GraphEngine.propagate_delay()`'s
BFS doesn't track which edge type was traversed at each hop, only the hop
count, so that label would be fabricated. Hop count and decay percentage
are shown instead, since those are real.
