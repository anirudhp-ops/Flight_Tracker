import { useMemo, useState } from "react";

// Matches the backend's default gate pool
// (flight_tracker/config.py's GATE_POOL_TERMINALS/GATE_POOL_GATES_PER_TERMINAL,
// flight_tracker/graph/engine.py's _gate_pool()) — 4 terminals x 14 gates =
// 56 gates. A per-airport GATE_POOL_OVERRIDES is possible on the backend
// (see config.py) but isn't exposed over any API, so this always renders
// the generated default layout; a real override wouldn't be reflected
// here. Documented in PHASE_G_REPORT.md as a known gap, not silently
// assumed away.
const TERMINALS = ["A", "B", "C", "T"];
const GATES_PER_TERMINAL = 14;
// How long a just-reassigned gate stays flagged red before falling back
// to its occupancy-based color — long enough to notice, short enough to
// not look stuck.
const REASSIGNMENT_HIGHLIGHT_MS = 8000;

const COLORS = {
  empty: "#30363d",
  onTime: "#2ea043",
  delayed: "#d29922",
  conflict: "#f85149",
};

function buildGatePool() {
  const gates = [];
  for (const terminal of TERMINALS) {
    for (let n = 1; n <= GATES_PER_TERMINAL; n++) gates.push(`${terminal}${n}`);
  }
  return gates;
}
const GATE_POOL = buildGatePool();

export default function GateMap({ flights, highlightGate, gateReassignments, now }) {
  const [selectedGate, setSelectedGate] = useState(null);
  const nowMs = now ?? Date.now();

  const occupancy = useMemo(() => {
    const map = new Map();
    const flightList = flights instanceof Map ? Array.from(flights.values()) : flights || [];
    for (const f of flightList) {
      if (!f.gate_id) continue;
      const list = map.get(f.gate_id) || [];
      list.push(f);
      map.set(f.gate_id, list);
    }
    return map;
  }, [flights]);

  const recentlyReassignedGates = useMemo(() => {
    const set = new Set();
    if (!gateReassignments) return set;
    const entries = gateReassignments instanceof Map ? gateReassignments.values() : gateReassignments;
    for (const r of entries) {
      if (!r?.new_gate || !r?.at) continue;
      const at = new Date(r.at).getTime();
      if (Number.isFinite(at) && nowMs - at < REASSIGNMENT_HIGHLIGHT_MS) {
        set.add(r.new_gate);
      }
    }
    return set;
  }, [gateReassignments, nowMs]);

  function gateColor(gateId) {
    if (recentlyReassignedGates.has(gateId)) return COLORS.conflict;
    const occupants = occupancy.get(gateId);
    if (!occupants || occupants.length === 0) return COLORS.empty;
    return occupants.some((f) => f.delay_minutes > 0) ? COLORS.delayed : COLORS.onTime;
  }

  const selectedOccupants = selectedGate ? occupancy.get(selectedGate) || [] : [];

  return (
    <div>
      <div
        role="grid"
        aria-label="Gate map"
        style={{ display: "grid", gridTemplateColumns: `repeat(${GATES_PER_TERMINAL}, 1fr)`, gap: 2 }}
      >
        {GATE_POOL.map((gateId) => {
          const isHighlighted = gateId === highlightGate;
          return (
            <div
              key={gateId}
              role="gridcell"
              tabIndex={0}
              aria-label={`Gate ${gateId}, ${(occupancy.get(gateId) || []).length} flight(s)`}
              title={gateId}
              onClick={() => setSelectedGate((g) => (g === gateId ? null : gateId))}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  setSelectedGate((g) => (g === gateId ? null : gateId));
                }
              }}
              style={{
                width: 12,
                height: 12,
                borderRadius: 2,
                background: gateColor(gateId),
                cursor: "pointer",
                outline: isHighlighted ? "1.5px solid #79c0ff" : selectedGate === gateId ? "1.5px solid #e6edf3" : "none",
                outlineOffset: 1,
              }}
            />
          );
        })}
      </div>
      {selectedGate && (
        <div style={{ marginTop: 6, fontSize: 10, color: "#8b949e" }}>
          <strong style={{ color: "#e6edf3" }}>{selectedGate}</strong>
          {selectedOccupants.length === 0
            ? " — empty"
            : selectedOccupants.map((f) => (
                <span key={f.flight_id}>
                  {" · "}
                  {f.flight_id}
                  {f.delay_minutes > 0 ? ` (+${f.delay_minutes}m)` : ""}
                </span>
              ))}
        </div>
      )}
    </div>
  );
}
