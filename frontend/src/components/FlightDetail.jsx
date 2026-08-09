import { getIATACode } from "../utils/airportCodes";
import { delaySeverity, confidenceLabel } from "../utils/delaySeverity";
import GateMap from "./GateMap";

const STATUS_LABEL = {
  scheduled: "SCHEDULED",
  active: "DEPARTED",
  landed: "LANDED",
  cancelled: "CANCELLED",
  diverted: "DIVERTED",
};

function fmtTime(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function Row({ label, children }) {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        fontSize: 12,
        padding: "3px 0",
        borderBottom: "0.5px solid #21262d",
        gap: 8,
      }}
    >
      <span style={{ color: "#8b949e" }}>{label}</span>
      <span style={{ fontWeight: 500, textAlign: "right" }}>{children}</span>
    </div>
  );
}

/** Small horizontal meter, 0-1, used for ML confidence. Not a progress bar
 * (nothing is "loading") — a static fill-proportional-to-value indicator. */
function ConfidenceMeter({ confidence }) {
  const pct = Math.round(Math.max(0, Math.min(1, confidence ?? 0)) * 100);
  const { label } = confidenceLabel(confidence);
  return (
    <div
      role="meter"
      aria-label={`Model confidence: ${label}, ${pct}%`}
      aria-valuenow={pct}
      aria-valuemin={0}
      aria-valuemax={100}
      style={{ display: "flex", alignItems: "center", gap: 6 }}
    >
      <div style={{ width: 46, height: 6, background: "#21262d", borderRadius: 3, overflow: "hidden" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: "#58a6ff" }} />
      </div>
      <span style={{ fontSize: 10, color: "#8b949e" }}>{label}</span>
    </div>
  );
}

/** Vertical timeline built strictly from fields FlightEvent actually
 * carries. Deliberately has no BOARDING entry and no per-event "delay
 * reason" text — flight_tracker/events/event_model.py documents that
 * nothing in this app's ingestion sources (mock or live AeroAPI) ever
 * produces a boarding timestamp, and FlightEvent has no delay-reason
 * field at all (carrier/weather/nas/late-aircraft delay are optional
 * predictor *inputs*, not ingested/observed data) — showing either would
 * be fabricated, not real. */
function EventTimeline({ flight, prediction, gateReassignment }) {
  const events = [];
  events.push({ label: "SCHEDULED", time: flight.scheduled_departure, tag: null });

  const departedAt = flight.actual_departure || flight.estimated_departure;
  if (departedAt) {
    const sched = new Date(flight.scheduled_departure).getTime();
    const dep = new Date(departedAt).getTime();
    const deltaMin = Number.isFinite(sched) && Number.isFinite(dep) ? Math.round((dep - sched) / 60000) : null;
    events.push({
      label: flight.actual_departure ? "DEPARTED" : "ESTIMATED DEPARTURE",
      time: departedAt,
      tag: deltaMin && deltaMin > 0 ? `+${deltaMin} min` : deltaMin === 0 ? "on time" : null,
      badge: deltaMin && deltaMin > 0 ? "DELAYED" : null,
    });
  }

  if (gateReassignment) {
    events.push({
      label: "GATE REASSIGNED",
      time: gateReassignment.at,
      tag: `${gateReassignment.old_gate || "unassigned"} → ${gateReassignment.new_gate}`,
      badge: "GATE",
    });
  }

  if (flight.actual_arrival) {
    events.push({ label: "ARRIVED", time: flight.actual_arrival, tag: null });
  } else if (prediction?.predicted_arrival_time) {
    events.push({
      label: "ARRIVAL (PREDICTED)",
      time: prediction.predicted_arrival_time,
      tag: null,
      badge: "PREDICTION",
    });
  } else if (flight.estimated_arrival || flight.scheduled_arrival) {
    events.push({ label: "ESTIMATED ARRIVAL", time: flight.estimated_arrival || flight.scheduled_arrival, tag: null });
  }

  return (
    <div style={{ position: "relative", paddingLeft: 14 }}>
      <div style={{ position: "absolute", left: 3, top: 4, bottom: 4, width: 1, background: "#30363d" }} />
      {events.map((e, i) => (
        <div key={i} style={{ position: "relative", marginBottom: 10 }}>
          <div
            style={{
              position: "absolute",
              left: -14,
              top: 3,
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: e.badge === "PREDICTION" ? "#58a6ff" : "#8b949e",
              border: "2px solid #0d1117",
            }}
          />
          <div style={{ fontSize: 11, color: "#e6edf3", fontWeight: 500 }}>
            {e.label}
            {e.badge && (
              <span
                style={{
                  marginLeft: 6,
                  fontSize: 9,
                  padding: "1px 5px",
                  borderRadius: 8,
                  background: e.badge === "DELAYED" ? "#3d1515" : e.badge === "GATE" ? "#2d2410" : "#0d1f3c",
                  color: e.badge === "DELAYED" ? "#ff7b72" : e.badge === "GATE" ? "#e3b341" : "#79c0ff",
                }}
              >
                {e.badge}
              </span>
            )}
          </div>
          <div style={{ fontSize: 10, color: "#8b949e" }}>
            {fmtTime(e.time) || "—"}
            {e.tag ? ` · ${e.tag}` : ""}
          </div>
        </div>
      ))}
    </div>
  );
}

export default function FlightDetail({
  flight,
  prediction,
  upstream,
  downstream,
  gateReassignment,
  flights,
  gateReassignments,
  onSelectFlight,
}) {
  if (!flight) {
    return <div style={{ fontSize: 12, color: "#8b949e" }}>Click a plane to inspect.</div>;
  }

  const severity = delaySeverity(flight.delay_minutes);
  const statusLabel = STATUS_LABEL[flight.status] || flight.status?.toUpperCase();

  return (
    <div>
      <Row label="Flight">{`${flight.airline_code}${flight.flight_number}`}</Row>
      <Row label="Route">{`${getIATACode(flight.origin)} → ${getIATACode(flight.destination)}`}</Row>
      <Row label="Aircraft">{flight.aircraft_id || "N/A"}</Row>
      <Row label="Gate">{flight.gate_id || "N/A"}</Row>
      <Row label="Status">{statusLabel}</Row>
      <Row label="Delay">
        <span style={{ color: severity.color }}>
          {flight.delay_minutes > 0 ? `+${flight.delay_minutes} min` : "On time"}
        </span>
      </Row>

      {prediction && (
        <>
          <Row label="Predicted arrival">{fmtTime(prediction.predicted_arrival_time) || "—"}</Row>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "5px 0" }}>
            <span style={{ fontSize: 12, color: "#8b949e" }}>Confidence</span>
            <ConfidenceMeter confidence={prediction.model_confidence} />
          </div>
          <div style={{ fontSize: 9, color: "#6e7681", marginBottom: 6, lineHeight: 1.4 }}>
            Confidence reflects agreement across the Random Forest model's individual trees, not a fixed accuracy score.
          </div>
        </>
      )}

      {upstream && (
        <div style={{ marginTop: 8, fontSize: 11 }}>
          <div style={{ color: "#8b949e", marginBottom: 2 }}>Upstream impact</div>
          <div
            role="button"
            tabIndex={0}
            onClick={() => onSelectFlight?.(upstream.flight_id)}
            onKeyDown={(e) => e.key === "Enter" && onSelectFlight?.(upstream.flight_id)}
            style={{ cursor: onSelectFlight ? "pointer" : "default", color: "#e3b341" }}
          >
            {upstream.flight_id}: +{upstream.delay_minutes} min ({upstream.hops} hop{upstream.hops === 1 ? "" : "s"} away)
          </div>
        </div>
      )}

      {downstream && downstream.length > 0 && (
        <div style={{ marginTop: 8, fontSize: 11 }}>
          <div style={{ color: "#8b949e", marginBottom: 2 }}>
            Downstream impact ({downstream.length} flight{downstream.length === 1 ? "" : "s"})
          </div>
          {downstream
            .slice()
            .sort((a, b) => (a.hops ?? 0) - (b.hops ?? 0))
            .map((d) => (
              <div
                key={d.flight_id}
                role="button"
                tabIndex={0}
                onClick={() => onSelectFlight?.(d.flight_id)}
                onKeyDown={(e) => e.key === "Enter" && onSelectFlight?.(d.flight_id)}
                style={{ cursor: onSelectFlight ? "pointer" : "default", padding: "1px 0", color: "#ff7b72" }}
              >
                {d.flight_id}: +{d.delay_minutes} min ({d.hops} hop{d.hops === 1 ? "" : "s"} away, {Math.round(0.75 ** (d.hops || 1) * 100)}% decay)
              </div>
            ))}
        </div>
      )}

      <div style={{ marginTop: 10 }}>
        <div style={{ color: "#8b949e", fontSize: 11, marginBottom: 4 }}>Timeline</div>
        <EventTimeline flight={flight} prediction={prediction} gateReassignment={gateReassignment} />
      </div>

      {flights && (
        <div style={{ marginTop: 10 }}>
          <div style={{ color: "#8b949e", fontSize: 11, marginBottom: 4 }}>Gates</div>
          <GateMap flights={flights} highlightGate={flight.gate_id} gateReassignments={gateReassignments} />
        </div>
      )}
    </div>
  );
}
