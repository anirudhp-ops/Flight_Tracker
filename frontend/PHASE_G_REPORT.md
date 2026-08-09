# Phase G final report: real-time WebSocket system & frontend integration

Summary of what was built, what the brief got wrong about the starting
point, what was found and fixed via live testing, and what's still open.
See `frontend/REAL_TIME_ARCHITECTURE.md` for the technical reference and
`frontend/USER_GUIDE.md` for how to use the dashboard — this file is the
narrative wrap-up.

## The brief's "current state" was wrong, and that changed the actual work

The brief described the frontend as "disconnected D3 map with mock
flights only." Reading `frontend/src/components/FlightMap.jsx` before
touching anything showed that was already false — it connected to the
real `/ws/{airport}` WebSocket, had reconnect-with-backoff, and a live
connection-status indicator (all verified working in Phase F's own
browser check). The real gap was narrower and more specific: the backend
sent bare `FlightEvent` JSON with no message-type envelope, so ML
predictions, confidence, propagation chains, and gate reassignments —
already fully computed server-side since Phase F — never reached the
browser. That's what this phase actually needed to close, and it reframed
several tasks (1, 3, 9) from "build from scratch" to "extend what's
already correct."

## What was built

- **`flight_tracker/websocket/messages.py`** — the `WSMessage` envelope
  (`type`/`timestamp`/`flight_id`/`data`) and `classify_prediction_event()`,
  which maps every `PredictionEvent` off `delay-predictions` to exactly
  one of SNAPSHOT / FLIGHT_UPDATE / DELAY_PREDICTION / PROPAGATION_EVENT /
  GATE_REASSIGNMENT / HEARTBEAT.
- **`server.py`'s WebSocket handler rewritten**: SNAPSHOT on connect, a
  shared 100-message ring buffer replayed to late-joining clients, a 30s
  app-level heartbeat, and an optional per-flight subscribe filter — all
  routed through a single `outbox` queue/writer task so the three
  concurrent sources (heartbeat, Kafka stream, connection setup) can't
  race on `websocket.send_text()`.
- **`PredictionEvent` gained `gate_reassignment`** (old/new gate), threaded
  through from `DelayPropagationWorker`'s real `resolve_gate_conflicts()`
  call — not synthesized.
- **`frontend/src/hooks/useFlightData.js`** — owns the WebSocket
  connection and all derived state (flights, predictions, propagation
  chains, gate reassignments, connection lifecycle), with message
  batching and heartbeat-based dead-connection detection.
- **`FlightDetail.jsx`**, **`GateMap.jsx`** — new components: severity-colored
  delay, prediction + confidence meter, upstream/downstream propagation
  lists, an event timeline built strictly from real `FlightEvent` fields,
  and a 56-gate occupancy grid.
- **Propagation cascade overlay** in `FlightMap.jsx` — pulsing source,
  hop-colored fading connections to affected flights, decay labels,
  gate-reassignment markers, hover highlight, click-to-jump.
- **Loading/error states, responsive layout, accessibility** — skeleton
  load, disconnected/reconnecting/hard-failure states (the last after 30s,
  distinct from "still retrying"), a narrow-viewport layout switch, ARIA
  labels + keyboard navigation on every flight marker and gate cell,
  color-blind-safe palette (blue/amber/orange/red, not red/green).
- **39 automated tests** (17 backend `pytest`, 22 frontend `Jest`),
  covering the message classifier, a real gate-conflict end-to-end path,
  the hook's message handling and propagation-chain resolution, and the
  detail/gate components.

## Explicit scope cuts (not silent omissions)

- **`GRAPH_UPDATE` message type**: not built. The brief itself calls it
  "for advanced viz" — diffing `GraphEngine`'s `networkx` state over the
  wire is a materially bigger feature than the rest of this phase
  combined, with no current UI that would consume it.
- **Multiple airports**: not built. The brief marks this "Optional,
  Stretch" itself.
- **Cypress/Playwright E2E**: not built. The brief marks this "optional"
  itself; real end-to-end coverage was instead done via live browser
  testing against the actual running stack (see below), which is how
  every phase of this project has been verified.
- **Aircraft-turn / gate-reuse edge-type labels on the downstream list**:
  the brief's mockup shows `UA456: +22 min (aircraft turn)`, but
  `GraphEngine.propagate_delay()`'s BFS only tracks hop count, not which
  edge type was traversed at each hop — labeling this would be
  fabricated. Hop count and decay percentage are shown instead, since
  those are real.
- **Per-airport gate-pool overrides in `GateMap`**: the backend supports
  `GATE_POOL_OVERRIDES` per airport, but nothing exposes that over an
  API, so the frontend always renders the generated default 56-gate
  layout. Documented in the component's own comment, not silently wrong.

## Four real bugs found via live testing, all fixed

Every one of these was caught by actually running the system against
real Kafka/Postgres/Redis and a real browser — not by reading the code
and assuming it was correct:

1. **`propagation_source` used the wrong identifier.**
   `delay_propagation_worker.py` published `event.flight_key` (the
   internal graph-node id, e.g. `"AA6259-20260809"`), but every other
   identifier in the system — including the frontend's own `flights` Map
   keys — is `event.flight_id` (e.g. `"AA6259-mock-9"`). The frontend
   could never have resolved "the source flight" of a cascade. Fixed to
   publish `flight_id`.
2. **`useFlightData`'s upstream lookup showed the wrong flight.**
   `{ flight_id: sourceId, ...match }` — object-spread order meant
   `match.flight_id` (the *affected* flight's own id) silently overwrote
   `sourceId`. Confirmed live: selecting an affected flight showed its
   own id as its "upstream impact" instead of the flight that actually
   caused its delay. Fixed by spreading first, overriding after.
3. **Decay display rendered as `×0.751`** (a `<sup>` tag's text flattened
   inline, reading like a decimal). Replaced with the actual computed
   decay percentage, matching the map overlay's own label.
4. **`GraphEngine.resolve_gate_conflicts()`'s `old_gate` always equalled
   `new_gate`.** A pre-existing bug from when gate-conflict resolution was
   first built (Phase B) — `dst_attrs` is a live view into the node's
   attribute dict, not a snapshot, so reading `dst_attrs["gate_id"]`
   *after* already writing the new gate into that same dict just returned
   the new value again. Invisible until Phase G's `GateReassignmentDetail`
   became the first real caller of `old_gate` — caught by a test that
   forces a genuine schedule-overlapping gate conflict through the real
   `GraphEngine` and asserts on the actual before/after values, not a
   mocked outcome.

## A real performance investigation, including a false alarm

Live testing against ~1700 real flights surfaced a genuine issue: the
first version of the D3 render effect depended directly on the
`flights` Map reference, which changes on every 100ms batch tick under
load — so a full redraw (position/rotation for every plane, plus a full
propagation-overlay rebuild that restarted its transitions) was
re-running up to 10x/second. Fixed by decoupling redraw cadence from
data-arrival cadence entirely: `FlightMap.jsx` now redraws on its own
fixed ~500ms interval via refs, and the cascade overlay only rebuilds
when the actual selected-flight/chain identity changes.

Chasing this down also produced a false alarm worth recording honestly:
`requestAnimationFrame`-based timing measurements repeatedly failed
against the live tab, which first looked like a frozen renderer.
Instrumenting the actual redraw with `performance.now()` directly (not
rAF) showed real per-frame cost of 20-90ms for ~1700 flights — reasonable,
not a freeze. The rAF measurements were failing because this session's
browser-automation tab ran with `document.hidden === true` throughout
(never the OS-visible/compositor-active tab), which is standard Chrome
background-tab throttling of `requestAnimationFrame`, unrelated to the
app. The real, direct measurement (20-90ms/redraw at ~1-2 redraws/sec) is
what the "smooth at 1000+ flights" claim rests on — a literal
frames-per-second number in a foreground browser could not be captured
through this harness, and that limitation is stated here rather than
implied away.

## What was verified live (not asserted)

- **Full protocol, live**: captured real SNAPSHOT, DELAY_PREDICTION,
  PROPAGATION_EVENT, and HEARTBEAT messages off the running server with
  real model confidence scores and real propagation hop/decay data.
- **Propagation chain, end to end**: selected a real cascading flight in
  the browser — downstream list, upstream reverse-lookup, and the D3
  cascade overlay (pulsing source + hop-colored connections + decay
  labels) all confirmed against real backend data, including catching
  and fixing bugs 1-3 above mid-verification.
- **Gate reassignment, end to end**: a dedicated test forces a real
  schedule-overlapping gate conflict through `GraphEngine` and confirms
  the resulting `GATE_REASSIGNMENT` message carries correct before/after
  gate values (bug 4 above, caught this way).
- **Reconnection resilience, twice**: once deliberately (backend stopped
  for 40+ seconds — confirmed DISCONNECTED, then the "Connection failed"
  hard-failure state after 30s, then full recovery with a fresh snapshot
  once the backend returned) and once unplanned (a genuine transient drop
  occurred mid-testing-session and the app recovered on its own within
  the expected reconnect window, without intervention).
- **Frontend + backend regression**: 39/39 automated tests passing,
  zero DLQ entries beyond the two expected ones from the DLQ test itself,
  zero browser console errors across the whole session.

## Known limitations (stated plainly)

- 60fps-in-a-visible-browser was not literally measured (see the
  performance section above) — the underlying per-frame JS/DOM cost was,
  and is well within budget at the current bounded redraw rate.
- The responsive breakpoint (mobile-width stacked layout) could not be
  exercised below ~900px in this session's browser-automation
  environment (window resize appeared to have a floor there); it's
  covered by straightforward, low-risk conditional logic and is the kind
  of thing worth a real-device check before shipping.
- `old_gate`/`GraphEngine.resolve_gate_conflicts()`'s fix (bug 4) changes
  behavior any other caller of that method would see too — none exist
  today besides `DelayPropagationWorker`, but worth knowing if that
  changes.
