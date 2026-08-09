import { useCallback, useEffect, useRef, useState } from "react";

// Same-origin (CRA dev server) can't reach the FastAPI backend directly —
// it runs on a different port. Override with REACT_APP_BACKEND_URL if the
// backend isn't at the default uvicorn address.
const BACKEND_HTTP_URL = process.env.REACT_APP_BACKEND_URL || "http://localhost:8000";
const BACKEND_WS_URL = BACKEND_HTTP_URL.replace(/^http/, "ws");

const RECONNECT_DELAY_MS = 3000;
// Server sends a HEARTBEAT every 30s (flight_tracker/server.py's
// WS_HEARTBEAT_INTERVAL_SECONDS) even with zero flight traffic — so "no
// message of ANY kind, including heartbeats, in this long" is a reliable
// dead-connection signal independent of whether the browser's TCP stack
// has noticed the socket is actually gone yet.
const HEARTBEAT_TIMEOUT_MS = 45000;
// Task 9: distinguish "still retrying" from "connection failed" only after
// a sustained outage, not on the first dropped frame.
const HARD_FAILURE_MS = 30000;
// Task 11: batch incoming messages instead of one React state update (and
// one full D3 re-render) per WebSocket frame.
const BATCH_INTERVAL_MS = 100;
const BATCH_MAX_PER_TICK = 10;

export const ConnectionStatus = {
  CONNECTING: "CONNECTING",
  CONNECTED: "CONNECTED",
  DISCONNECTED: "DISCONNECTED",
  RECONNECTING: "RECONNECTING",
};

function upsertFlight(prevFlights, flightId, data) {
  const next = new Map(prevFlights);
  next.set(flightId, { ...next.get(flightId), ...data });
  return next;
}

/**
 * Owns the WebSocket connection to /ws/{airport} and the client-side view
 * of everything it streams: flights, ML predictions, propagation chains
 * (source flight_id -> affected flights, each with hops/delay), and gate
 * reassignments. See flight_tracker/websocket/messages.py for the wire
 * protocol this parses (WSMessage: type/timestamp/flight_id/data).
 *
 * Deliberately keeps the click-to-select `selectedFlightId` UI concept
 * separate from the server's optional per-flight subscribe filter
 * (subscribeToFlight/unsubscribeFromFlight below): selecting a flight on
 * the map must not narrow what the map itself receives, or every other
 * flight would vanish the moment the user clicked one. The subscribe
 * filter is exposed for a future "focus mode" that intentionally wants
 * less traffic, not wired into selection by default.
 */
export function useFlightData(airportCodeOverride) {
  const [targetAirport, setTargetAirport] = useState(airportCodeOverride || null);
  const [connectionStatus, setConnectionStatus] = useState(ConnectionStatus.CONNECTING);
  const [hardFailure, setHardFailure] = useState(false);
  const [snapshotLoaded, setSnapshotLoaded] = useState(false);
  const [flights, setFlights] = useState(new Map());
  const [predictions, setPredictions] = useState(new Map());
  const [propagationChains, setPropagationChains] = useState(new Map());
  const [gateReassignments, setGateReassignments] = useState(new Map());
  const [selectedFlightId, setSelectedFlightId] = useState(null);

  const socketRef = useRef(null);
  const statusRef = useRef(connectionStatus);
  const everConnectedRef = useRef(false);
  const lastMessageAtRef = useRef(Date.now());
  const disconnectedSinceRef = useRef(null);
  const pendingRef = useRef([]);
  const reconnectTimerRef = useRef(null);
  const subscribedFlightRef = useRef(null);

  useEffect(() => {
    statusRef.current = connectionStatus;
  }, [connectionStatus]);

  // Discover which airport the backend is tracking, unless the caller
  // pinned one explicitly (airportCodeOverride).
  useEffect(() => {
    if (airportCodeOverride) return;
    fetch(`${BACKEND_HTTP_URL}/api/config`)
      .then((r) => r.json())
      .then((cfg) => setTargetAirport(cfg.target_airport))
      .catch((err) => {
        console.error("Failed to load backend config:", err);
        setConnectionStatus(ConnectionStatus.DISCONNECTED);
      });
  }, [airportCodeOverride]);

  const applyMessage = useCallback((msg) => {
    lastMessageAtRef.current = Date.now();
    switch (msg.type) {
      case "SNAPSHOT": {
        const map = new Map();
        for (const f of msg.data.flights || []) {
          if (f && f.flight_id) map.set(f.flight_id, f);
        }
        setFlights(map);
        setSnapshotLoaded(true);
        return;
      }
      case "HEARTBEAT":
        return;
      case "FLIGHT_UPDATE":
      case "DELAY_PREDICTION":
      case "PROPAGATION_EVENT":
      case "GATE_REASSIGNMENT":
        break;
      default:
        return;
    }

    const d = msg.data;
    const flightId = msg.flight_id;
    if (!flightId) return;

    setFlights((prev) => upsertFlight(prev, flightId, d));

    if (typeof d.predicted_delay_minutes === "number") {
      setPredictions((prev) => {
        const next = new Map(prev);
        next.set(flightId, {
          predicted_delay_minutes: d.predicted_delay_minutes,
          predicted_arrival_time: d.predicted_arrival_time,
          model_confidence: d.model_confidence,
        });
        return next;
      });
    }

    if (msg.type === "PROPAGATION_EVENT" && d.propagation_source) {
      setPropagationChains((prev) => {
        const next = new Map(prev);
        const existing = next.get(d.propagation_source) || [];
        const idx = existing.findIndex((e) => e.flight_id === flightId);
        const entry = {
          flight_id: flightId,
          delay_minutes: d.delay_minutes,
          hops: d.propagation_hops,
        };
        const list = idx >= 0 ? existing.map((e, i) => (i === idx ? entry : e)) : [...existing, entry];
        next.set(d.propagation_source, list);
        return next;
      });
    }

    if (msg.type === "GATE_REASSIGNMENT" && d.gate_reassignment) {
      setGateReassignments((prev) => {
        const next = new Map(prev);
        next.set(flightId, { ...d.gate_reassignment, at: msg.timestamp });
        return next;
      });
    }
  }, []);

  // Batched flush: at most BATCH_MAX_PER_TICK messages processed every
  // BATCH_INTERVAL_MS, so a burst of WS frames doesn't trigger one React
  // re-render (and downstream D3 redraw) per message.
  useEffect(() => {
    const timer = setInterval(() => {
      if (pendingRef.current.length === 0) return;
      const batch = pendingRef.current.splice(0, BATCH_MAX_PER_TICK);
      batch.forEach(applyMessage);
    }, BATCH_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [applyMessage]);

  const subscribeToFlight = useCallback((flightId) => {
    subscribedFlightRef.current = flightId;
    const socket = socketRef.current;
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ action: "subscribe", flight_id: flightId }));
    }
  }, []);

  const unsubscribeFromFlight = useCallback(() => {
    subscribedFlightRef.current = null;
    const socket = socketRef.current;
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ action: "unsubscribe" }));
    }
  }, []);

  // Connection lifecycle: connect, reconnect with a fixed backoff on
  // drop, and force-reconnect if the connection goes silent (no message
  // of any kind, including heartbeats) for HEARTBEAT_TIMEOUT_MS.
  useEffect(() => {
    const airport = airportCodeOverride || targetAirport;
    if (!airport) return undefined;

    let stopped = false;

    function connect() {
      if (stopped) return;
      setConnectionStatus(
        everConnectedRef.current ? ConnectionStatus.RECONNECTING : ConnectionStatus.CONNECTING
      );
      const socket = new WebSocket(`${BACKEND_WS_URL}/ws/${airport}`);
      socketRef.current = socket;

      socket.onopen = () => {
        if (stopped) return;
        everConnectedRef.current = true;
        disconnectedSinceRef.current = null;
        lastMessageAtRef.current = Date.now();
        setHardFailure(false);
        setConnectionStatus(ConnectionStatus.CONNECTED);
        if (subscribedFlightRef.current) {
          socket.send(JSON.stringify({ action: "subscribe", flight_id: subscribedFlightRef.current }));
        }
      };

      socket.onmessage = (evt) => {
        if (stopped) return;
        try {
          pendingRef.current.push(JSON.parse(evt.data));
        } catch (err) {
          console.error("Failed to parse WS message:", err);
        }
      };

      socket.onclose = () => {
        if (stopped) return;
        if (disconnectedSinceRef.current === null) disconnectedSinceRef.current = Date.now();
        setConnectionStatus(ConnectionStatus.DISCONNECTED);
        reconnectTimerRef.current = setTimeout(connect, RECONNECT_DELAY_MS);
      };

      socket.onerror = () => {
        if (stopped) return;
        socket.close();
      };
    }

    connect();

    const watchdog = setInterval(() => {
      if (stopped) return;
      if (
        statusRef.current === ConnectionStatus.CONNECTED &&
        Date.now() - lastMessageAtRef.current > HEARTBEAT_TIMEOUT_MS
      ) {
        socketRef.current?.close();
      }
      if (disconnectedSinceRef.current && Date.now() - disconnectedSinceRef.current > HARD_FAILURE_MS) {
        setHardFailure(true);
      }
    }, 2000);

    return () => {
      stopped = true;
      clearTimeout(reconnectTimerRef.current);
      clearInterval(watchdog);
      socketRef.current?.close();
    };
  }, [airportCodeOverride, targetAirport]);

  /** Downstream flights this one's delay propagated to, plus (best-effort,
   * first match) the upstream flight this one's own delay was propagated
   * from, if any — derived from propagationChains rather than stored
   * separately, since it's the same data viewed from two directions. */
  const getPropagationChain = useCallback(
    (flightId) => {
      const downstream = propagationChains.get(flightId) || [];
      let upstream = null;
      for (const [sourceId, affected] of propagationChains.entries()) {
        const match = affected.find((e) => e.flight_id === flightId);
        if (match) {
          // ...match spread FIRST: match.flight_id === flightId itself
          // (the affected flight, not its source) — putting flight_id:
          // sourceId after the spread is what actually overrides it to
          // the source's id, not the other way around. Reversing this
          // order silently shows the wrong flight (found via live testing:
          // "Upstream impact" displayed the selected flight's own id).
          upstream = { ...match, flight_id: sourceId };
          break;
        }
      }
      return { downstream, upstream };
    },
    [propagationChains]
  );

  return {
    targetAirport,
    connectionStatus,
    hardFailure,
    snapshotLoaded,
    flights,
    predictions,
    propagationChains,
    gateReassignments,
    selectedFlightId,
    setSelectedFlightId,
    subscribeToFlight,
    unsubscribeFromFlight,
    getPropagationChain,
  };
}
