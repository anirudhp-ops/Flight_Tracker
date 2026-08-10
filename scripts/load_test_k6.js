/*
 * k6 load test for the flight_tracker WebSocket API (/ws/{airport_code}).
 *
 * Covers Scenario 1 (Baseline) and Scenario 4 (Spike) from the Phase I
 * brief. Scenarios 2 (Kafka event throughput) and 3 (cascade load) are
 * deliberately NOT here — stated up front, same convention the rest of
 * this codebase uses for documented deviations (see e.g. graph/engine.py,
 * docker-compose.yml, scripts/integration_tests.py): k6 has no built-in
 * Kafka producer, and this project has no HTTP endpoint that publishes to
 * flight-events (only /ws/{airport_code}, which is read-only from a
 * client's perspective — see server.py's websocket_endpoint). Adding one
 * purely to give k6 something to POST to would be new production surface
 * area built only to serve a load test, which is worse than just using
 * the right tool: scripts/load_test_kafka.py runs Scenarios 2 and 3
 * directly against Kafka with aiokafka, the same client this app's own
 * ingestion path uses.
 *
 * Requires a running flight_tracker server (the normal
 * `uvicorn flight_tracker.server:app` from README.md, or the throwaway
 * instance scripts/integration_tests.py knows how to start) reachable at
 * WS_URL — this script does not start one itself, matching how k6 load
 * tests normally target an already-deployed system rather than owning
 * its lifecycle.
 *
 * Run:
 *   k6 run scripts/load_test_k6.js
 *   k6 run --summary-export=k6_summary.json scripts/load_test_k6.js
 *   WS_URL=ws://127.0.0.1:8123 AIRPORT=KJFK k6 run scripts/load_test_k6.js
 */
import ws from "k6/ws";
import { check } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const WS_URL = __ENV.WS_URL || "ws://127.0.0.1:8000";
const AIRPORT = __ENV.AIRPORT || "KJFK";

// Scenario 1 (Baseline) holds one connection per VU for the whole 60s;
// Scenario 4 (Spike) intentionally churns shorter-lived connections so
// ramping VU counts actually produce connect/disconnect traffic instead
// of one connection per VU for the test's whole lifetime.
const BASELINE_HOLD_S = 60;
const SPIKE_HOLD_S = 8;

const wsConnectSuccess = new Rate("ws_connect_success");
const wsConnectLatency = new Trend("ws_connect_latency_ms");
const wsFirstMessageLatency = new Trend("ws_first_message_latency_ms");
const wsMessagesReceived = new Counter("ws_messages_received");
const wsErrors = new Counter("ws_errors");

// k6 ignores CLI-level -u/-d/--exec once options.scenarios is set (which
// scenario(s) run has to be decided in the script itself), so CI selects
// baseline-only ("Run load test (baseline only, not full spike)" — the
// Phase I brief) via SCENARIOS=baseline rather than a CLI flag k6 would
// silently ignore here.
const ENABLED_SCENARIOS = (__ENV.SCENARIOS || "baseline,spike").split(",").map((s) => s.trim());

const ALL_SCENARIOS = {
  // Scenario 1: Baseline (Normal Load) — 10 concurrent users, 60s.
  baseline: {
    executor: "constant-vus",
    vus: 10,
    duration: `${BASELINE_HOLD_S}s`,
    exec: "baselineScenario",
    tags: { scenario: "baseline" },
  },
  // Scenario 4: Spike Test — 0 -> 100 over 30s, hold 100 for 60s, 100 -> 0 over 30s.
  // Offset to start after baseline finishes so the two don't compete
  // for the same server capacity and confound each other's numbers
  // (only matters when both are enabled in the same run).
  spike: {
    executor: "ramping-vus",
    startVUs: 0,
    stages: [
      { duration: "30s", target: 100 },
      { duration: "60s", target: 100 },
      { duration: "30s", target: 0 },
    ],
    exec: "spikeScenario",
    startTime: ENABLED_SCENARIOS.includes("baseline") ? `${BASELINE_HOLD_S + 10}s` : "0s",
    gracefulRampDown: "10s",
    tags: { scenario: "spike" },
  },
};

const ALL_THRESHOLDS = {
  baseline: { "ws_connect_success{scenario:baseline}": ["rate>0.99"] },
  // Spike target from the brief: <1% error rate, i.e. >99% connect success.
  spike: { "ws_connect_success{scenario:spike}": ["rate>0.99"] },
};

export const options = {
  scenarios: Object.fromEntries(
    Object.entries(ALL_SCENARIOS).filter(([name]) => ENABLED_SCENARIOS.includes(name))
  ),
  thresholds: Object.assign(
    {},
    ...ENABLED_SCENARIOS.filter((s) => ALL_THRESHOLDS[s]).map((s) => ALL_THRESHOLDS[s])
  ),
};

function runConnection(holdSeconds, scenarioTag) {
  const url = `${WS_URL}/ws/${AIRPORT}`;
  const connectStart = Date.now();
  let firstMessageAt = null;

  const res = ws.connect(url, {}, function (socket) {
    socket.on("open", () => {
      wsConnectSuccess.add(true, { scenario: scenarioTag });
      wsConnectLatency.add(Date.now() - connectStart, { scenario: scenarioTag });
    });

    socket.on("message", (_data) => {
      wsMessagesReceived.add(1, { scenario: scenarioTag });
      if (firstMessageAt === null) {
        firstMessageAt = Date.now();
        wsFirstMessageLatency.add(firstMessageAt - connectStart, { scenario: scenarioTag });
      }
    });

    socket.on("error", (_e) => {
      wsErrors.add(1, { scenario: scenarioTag });
    });

    // Every /ws/{airport_code} connection gets a SNAPSHOT immediately on
    // connect (server.py's websocket_endpoint), so a connection that
    // opened but never received anything within its hold window is a
    // real signal, not just "no traffic yet."
    socket.setTimeout(() => socket.close(), holdSeconds * 1000);
  });

  const connected = res && res.status === 101;
  if (!connected) {
    wsConnectSuccess.add(false, { scenario: scenarioTag });
  }
  check(res, {
    "connected (HTTP 101)": (r) => r && r.status === 101,
  });
}

export function baselineScenario() {
  runConnection(BASELINE_HOLD_S, "baseline");
}

export function spikeScenario() {
  runConnection(SPIKE_HOLD_S, "spike");
}
