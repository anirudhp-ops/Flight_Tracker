# FlightTracker dashboard — user guide

## Overview

The dashboard shows every flight currently tracked for the configured
airport (top-left label, e.g. "FlightTracker — JFK"), streamed live from
the backend — no manual refresh needed.

## The header

- **N flights** — total flights currently known.
- **N delayed** — how many of those have a delay greater than zero.
- **Connection badge**:
  - 🟡 **Connecting…** — first-time connection in progress.
  - 🟢 **Live** — connected and receiving updates.
  - 🔴 **Disconnected** — the connection just dropped; a reconnect attempt
    is about to start.
  - 🟡 **Reconnecting…** — actively retrying.
  - If the connection stays down for 30+ seconds, the map dims and shows
    **"Connection failed. Refresh to retry."**

## The map

Each triangle is a flight, positioned along its route based on how far
through its scheduled flight time it currently is. Colors:

- **Blue** — on time.
- **Amber** — minor delay (up to 10 minutes).
- **Orange** — moderate delay (10-30 minutes).
- **Red** — severe delay (30+ minutes).
- **Light blue outline** — currently selected.

A small red circle with "!" marks any delayed flight. A small yellow
square marks a flight whose gate was just reassigned.

**Click a flight** (or use the arrow keys to cycle through flights and
select one) to open its detail panel and, if it has any cascading impact,
its propagation view on the map.

## Propagation view

When you select a flight that has caused downstream delays:

- The selected flight **pulses red** — the source of the cascade.
- Lines fan out to every flight it affected, colored orange (closer, more
  affected) trending to yellow (farther away in the chain).
- Each line is labeled with the affected flight's added delay and the
  decay percentage (delays shrink by 25% per hop — a flight 2 hops away
  keeps roughly 56% of the original delay, `0.75 × 0.75`).
- If the selected flight was itself affected by another flight's delay, a
  fainter dashed line traces back to that source.
- Hover over an affected flight's marker to highlight it; click it to jump
  the detail panel to that flight.

## The detail panel

- **Flight / Route / Aircraft / Gate / Status / Delay** — the basics.
- **Predicted arrival + Confidence** — appears once a prediction exists
  for this flight. The confidence label ("high confidence," "moderate
  confidence," "low confidence") reflects how much the underlying model's
  individual decision trees agree with each other, not a fixed accuracy
  percentage — tight agreement across trees means high confidence, wide
  disagreement means low confidence, on that specific prediction.
- **Upstream impact** — if this flight's own delay was caused by another
  flight's cascade, that source flight and how many hops away it is.
- **Downstream impact** — every flight this one's delay propagated to,
  with hop distance and decay percentage. Click any entry to jump there.
- **Timeline** — scheduled departure, actual/estimated departure (with a
  DELAYED badge and minutes-late if applicable), a gate-reassignment entry
  if one happened, and predicted or actual arrival.
- **Gates** — a small grid of every gate at the airport. Gray means empty,
  green means occupied and on time, amber means occupied and delayed, red
  means it was just reassigned. Click any gate to see which flight(s) are
  using it.

## Keyboard and accessibility

- **Arrow keys** (with the map focused) cycle through flights and select
  one, no mouse required.
- **Enter / Space** on a focused flight marker or gate also selects it.
- Every flight marker, gate cell, and the map itself have descriptive
  labels for screen readers (flight number, route, delay status).
- Colors are chosen to stay distinguishable under red-green color
  blindness (blue/amber/orange/red rather than plain red/green).

## If something looks wrong

- **Map stuck on "Loading flight snapshot…"** — the backend hasn't sent
  its initial state yet; this resolves itself once the connection opens.
- **"Waiting for flights from the backend…"** — connected, but the
  backend currently has zero tracked flights.
- **"Connection failed. Refresh to retry."** — the backend has been
  unreachable for 30+ seconds. It will keep retrying in the background;
  refreshing the page is only needed if you want to force an immediate
  reconnect attempt.
