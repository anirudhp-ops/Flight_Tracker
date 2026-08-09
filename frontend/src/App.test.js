import { render, screen, waitFor, act } from '@testing-library/react';
import App from './App';

// Empty-but-valid topojson topology: enough for topojson.feature() and
// d3.geoPath() to run without throwing, with zero rendered countries.
const mockWorldTopology = {
  type: 'Topology',
  arcs: [],
  objects: { countries: { type: 'GeometryCollection', geometries: [] } },
};

class MockWebSocket {
  static instances = [];

  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
    MockWebSocket.instances.push(this);
  }

  send() {}

  close() {
    this.readyState = 3;
    if (this.onclose) this.onclose({});
  }
}

function mockFlightEvent(overrides = {}) {
  const now = Date.now();
  return {
    flight_id: 'AA100-test',
    event_type: 'departure',
    airline_code: 'AA',
    flight_number: '100',
    origin: 'KJFK',
    destination: 'KLAX',
    aircraft_id: 'N123AA',
    gate_id: 'B12',
    scheduled_departure: new Date(now - 3600_000).toISOString(),
    scheduled_arrival: new Date(now + 3600_000).toISOString(),
    delay_minutes: 0,
    status: 'active',
    passenger_count: 150,
    timestamp: new Date(now).toISOString(),
    ...overrides,
  };
}

// Matches flight_tracker/websocket/messages.py's WSMessage envelope
// (Phase G): every message the server sends now has this shape, not a
// bare FlightEvent.
function wsMessage(type, flightId, data) {
  return { type, timestamp: new Date().toISOString(), flight_id: flightId, data };
}

beforeEach(() => {
  MockWebSocket.instances = [];
  global.WebSocket = MockWebSocket;
  global.fetch = jest.fn((url) => {
    const href = String(url);
    if (href.includes('/api/config')) {
      return Promise.resolve({ json: () => Promise.resolve({ target_airport: 'KJFK' }) });
    }
    if (href.includes('countries-110m.json')) {
      return Promise.resolve({ json: () => Promise.resolve(mockWorldTopology) });
    }
    return Promise.reject(new Error(`Unexpected fetch in test: ${href}`));
  });
});

afterEach(() => {
  jest.restoreAllMocks();
});

test('fetches backend config and opens a websocket to the reported airport', async () => {
  render(<App />);

  await waitFor(() => expect(screen.getByText('FlightTracker — JFK')).toBeInTheDocument());
  await waitFor(() => expect(MockWebSocket.instances).toHaveLength(1));
  expect(MockWebSocket.instances[0].url).toBe('ws://localhost:8000/ws/KJFK');
});

test('shows a loading skeleton before the snapshot arrives, then a waiting message once it does', async () => {
  render(<App />);
  await waitFor(() => expect(MockWebSocket.instances).toHaveLength(1));

  expect(screen.getByText('Loading flight snapshot…')).toBeInTheDocument();

  const socket = MockWebSocket.instances[0];
  act(() => {
    socket.onopen({});
    socket.onmessage({ data: JSON.stringify(wsMessage('SNAPSHOT', null, { flights: [] })) });
  });

  await waitFor(() => expect(screen.getByText('Waiting for flights from the backend…')).toBeInTheDocument());
});

test('renders a flight received over the websocket and updates the delay count', async () => {
  render(<App />);
  await waitFor(() => expect(MockWebSocket.instances).toHaveLength(1));
  const socket = MockWebSocket.instances[0];

  act(() => {
    socket.onopen({});
    socket.onmessage({
      data: JSON.stringify(wsMessage('SNAPSHOT', null, { flights: [mockFlightEvent({ delay_minutes: 0 })] })),
    });
  });

  await waitFor(() => expect(screen.getByText('● Live')).toBeInTheDocument());
  // useFlightData batches incoming messages (processes up to 10 every
  // 100ms — task 11's performance work), so the snapshot's flight isn't
  // necessarily in state yet just because the socket is open.
  await waitFor(() => expect(screen.getByText('1 flights')).toBeInTheDocument());
  expect(screen.getByText('0 delayed')).toBeInTheDocument();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMessage('DELAY_PREDICTION', 'DL200-test', mockFlightEvent({ flight_id: 'DL200-test', delay_minutes: 45 }))
      ),
    });
  });

  await waitFor(() => expect(screen.getByText('2 flights')).toBeInTheDocument());
  expect(screen.getByText('1 delayed')).toBeInTheDocument();
});

test('shows a disconnected badge immediately on drop, then reconnecting once a retry begins', async () => {
  render(<App />);
  await waitFor(() => expect(MockWebSocket.instances).toHaveLength(1));
  const socket = MockWebSocket.instances[0];

  act(() => {
    socket.onopen({});
  });
  await waitFor(() => expect(screen.getByText('● Live')).toBeInTheDocument());

  act(() => {
    socket.onclose({});
  });
  // Immediately after a drop the hook reports DISCONNECTED — RECONNECTING
  // only appears once an actual retry attempt begins (useFlightData.js's
  // RECONNECT_DELAY_MS later), matching the CONNECTING -> CONNECTED ->
  // DISCONNECTED -> RECONNECTING lifecycle from the Phase G brief.
  await waitFor(() => expect(screen.getByText('● Disconnected')).toBeInTheDocument());

  // Real timers (not faked): RECONNECT_DELAY_MS is 3s, so this waits for
  // an actual retry attempt rather than simulating one.
  await waitFor(() => expect(screen.getByText('● Reconnecting…')).toBeInTheDocument(), { timeout: 5000 });
}, 10000);
