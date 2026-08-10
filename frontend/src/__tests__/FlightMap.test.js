import { render, screen, waitFor, act, fireEvent } from '@testing-library/react';
import FlightMap from '../components/FlightMap';

// Empty-but-valid topojson topology: enough for topojson.feature() and
// d3.geoPath() to run without throwing, with zero rendered countries —
// same fixture App.test.js uses.
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
  // FlightMap wraps its map in a ResizeObserver; jsdom has no native
  // implementation.
  global.ResizeObserver = class {
    observe() {}
    disconnect() {}
  };
});

afterEach(() => {
  jest.restoreAllMocks();
});

async function renderConnectedMap() {
  const { container } = render(<FlightMap />);
  await waitFor(() => expect(MockWebSocket.instances).toHaveLength(1));
  const socket = MockWebSocket.instances[0];
  act(() => {
    socket.onopen({});
  });
  return { container, socket };
}

// The D3 render loop redraws on a fixed 500ms interval (see FlightMap.jsx's
// own comment on why — decoupling redraw cadence from data-arrival
// cadence), not on every React re-render, so tests that depend on the SVG
// reflecting new data/selection have to wait out at least one tick.
//
// Deliberately NOT wrapped in act(async () => ...): React defers flushing
// state updates (and the effects that mirror them into the refs
// FlightMap's D3 loop reads) until an enclosing async act() callback's
// promise resolves, so a real setInterval tick firing *during* that
// callback would still read stale refs — the D3 interval would run several
// times against pre-SNAPSHOT data before ever seeing the update. A bare
// awaited setTimeout lets React's normal scheduling (and this real
// interval) interleave normally. Found via instrumenting FlightMap's own
// render loop with logging, not assumed.
async function waitForNextRedraw() {
  await new Promise((r) => setTimeout(r, 700));
}

test('renders an SVG map with an aria-label reflecting zero tracked flights', async () => {
  await renderConnectedMap();
  await waitFor(() =>
    expect(screen.getByRole('img', { name: /Map of 0 tracked flights/ })).toBeInTheDocument()
  );
});

test('renders one plane marker per flight with resolvable origin/destination airports', async () => {
  const { container, socket } = await renderConnectedMap();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMessage('SNAPSHOT', null, {
          flights: [
            mockFlightEvent({ flight_id: 'AA100-test', origin: 'KJFK', destination: 'KLAX' }),
            // Unresolvable airport codes: FlightMap's own `airports` lookup
            // table has no entry for these, so project() returns null and
            // this flight must be filtered out of the plane layer entirely.
            mockFlightEvent({ flight_id: 'ZZ999-test', origin: 'ZZZZ', destination: 'YYYY' }),
          ],
        })
      ),
    });
  });
  await waitForNextRedraw();

  const planes = container.querySelectorAll('g.plane');
  expect(planes).toHaveLength(1);
});

test('clicking a plane selects it and shows its details in the side panel', async () => {
  const { container, socket } = await renderConnectedMap();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMessage('SNAPSHOT', null, { flights: [mockFlightEvent({ flight_id: 'AA100-test' })] })
      ),
    });
  });
  await waitForNextRedraw();

  expect(screen.getByText('Click a plane to inspect.')).toBeInTheDocument();

  const plane = container.querySelector('g.plane');
  fireEvent.click(plane);

  await waitFor(() => expect(screen.getByText('AA100')).toBeInTheDocument());
});

test('clicking an already-selected plane deselects it', async () => {
  const { container, socket } = await renderConnectedMap();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMessage('SNAPSHOT', null, { flights: [mockFlightEvent({ flight_id: 'AA100-test' })] })
      ),
    });
  });
  await waitForNextRedraw();

  const plane = container.querySelector('g.plane');
  fireEvent.click(plane);
  await waitFor(() => expect(screen.getByText('AA100')).toBeInTheDocument());

  fireEvent.click(plane);
  await waitFor(() => expect(screen.getByText('Click a plane to inspect.')).toBeInTheDocument());
});

test('arrow-key navigation on the map cycles the selected flight', async () => {
  const { socket } = await renderConnectedMap();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMessage('SNAPSHOT', null, {
          flights: [
            mockFlightEvent({ flight_id: 'AA100-test', airline_code: 'AA', flight_number: '100' }),
            mockFlightEvent({ flight_id: 'BB200-test', airline_code: 'BB', flight_number: '200' }),
          ],
        })
      ),
    });
  });
  await waitFor(() => expect(screen.getByText('2 flights')).toBeInTheDocument());

  const svg = screen.getByRole('img');
  fireEvent.keyDown(svg, { key: 'ArrowRight' });

  await waitFor(() => expect(screen.getByText('AA100')).toBeInTheDocument());

  fireEvent.keyDown(svg, { key: 'ArrowRight' });
  await waitFor(() => expect(screen.getByText('BB200')).toBeInTheDocument());
});

test('selecting a flight with a propagated delay renders a cascade overlay', async () => {
  const { container, socket } = await renderConnectedMap();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMessage('SNAPSHOT', null, {
          flights: [
            mockFlightEvent({ flight_id: 'SRC-test', delay_minutes: 40 }),
            mockFlightEvent({ flight_id: 'DST-test', origin: 'KJFK', destination: 'KLAX', delay_minutes: 30 }),
          ],
        })
      ),
    });
    socket.onmessage({
      data: JSON.stringify(
        wsMessage('PROPAGATION_EVENT', 'DST-test', {
          ...mockFlightEvent({ flight_id: 'DST-test', delay_minutes: 30 }),
          predicted_delay_minutes: 30,
          predicted_arrival_time: new Date().toISOString(),
          model_confidence: 0.6,
          propagation_source: 'SRC-test',
          propagation_hops: 1,
        })
      ),
    });
  });
  await waitFor(() => expect(screen.getByText('2 flights')).toBeInTheDocument());
  await waitForNextRedraw();

  // Plane join order follows SNAPSHOT insertion order — SRC-test first.
  const planes = container.querySelectorAll('g.plane');
  expect(planes).toHaveLength(2);
  fireEvent.click(planes[0]);
  await waitFor(() => expect(screen.getByText('AA100')).toBeInTheDocument());
  await waitForNextRedraw();

  const cascadeGroup = container.querySelector('g.cascade');
  expect(cascadeGroup.children.length).toBeGreaterThan(0);
  expect(cascadeGroup.querySelector('line')).not.toBeNull();
});
