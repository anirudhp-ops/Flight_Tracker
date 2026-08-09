import { renderHook, act, waitFor } from '@testing-library/react';
import { useFlightData, ConnectionStatus } from '../hooks/useFlightData';

class MockWebSocket {
  static instances = [];
  static OPEN = 1;
  static CLOSED = 3;
  constructor(url) {
    this.url = url;
    this.readyState = 0;
    this.sent = [];
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
    MockWebSocket.instances.push(this);
  }
  send(data) {
    this.sent.push(data);
  }
  close() {
    this.readyState = MockWebSocket.CLOSED;
    if (this.onclose) this.onclose({});
  }
}

function flightPayload(overrides = {}) {
  const now = Date.now();
  return {
    flight_id: 'AA100-test',
    event_type: 'delay',
    airline_code: 'AA',
    flight_number: '100',
    origin: 'KJFK',
    destination: 'KLAX',
    aircraft_id: 'N1',
    gate_id: 'A1',
    scheduled_departure: new Date(now).toISOString(),
    scheduled_arrival: new Date(now + 3600_000).toISOString(),
    delay_minutes: 0,
    status: 'active',
    timestamp: new Date(now).toISOString(),
    ...overrides,
  };
}

function wsMsg(type, flightId, data = {}) {
  return { type, timestamp: new Date().toISOString(), flight_id: flightId, data };
}

beforeEach(() => {
  MockWebSocket.instances = [];
  global.WebSocket = MockWebSocket;
});

async function openSocket() {
  await waitFor(() => expect(MockWebSocket.instances).toHaveLength(1));
  const socket = MockWebSocket.instances[0];
  act(() => {
    socket.readyState = MockWebSocket.OPEN;
    socket.onopen({});
  });
  return socket;
}

test('applies a SNAPSHOT message as the initial flight set', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(wsMsg('SNAPSHOT', null, { flights: [flightPayload(), flightPayload({ flight_id: 'BB2' })] })),
    });
  });

  await waitFor(() => expect(result.current.flights.size).toBe(2));
  expect(result.current.snapshotLoaded).toBe(true);
  expect(result.current.connectionStatus).toBe(ConnectionStatus.CONNECTED);
});

test('DELAY_PREDICTION populates both the flight and its prediction', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMsg('DELAY_PREDICTION', 'AA100-test', {
          ...flightPayload({ delay_minutes: 40 }),
          predicted_delay_minutes: 30,
          predicted_arrival_time: '2026-01-01T00:00:00Z',
          model_confidence: 0.82,
        })
      ),
    });
  });

  await waitFor(() => expect(result.current.flights.get('AA100-test')?.delay_minutes).toBe(40));
  expect(result.current.predictions.get('AA100-test')).toEqual({
    predicted_delay_minutes: 30,
    predicted_arrival_time: '2026-01-01T00:00:00Z',
    model_confidence: 0.82,
  });
});

test('PROPAGATION_EVENT records the affected flight under its propagation_source', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMsg('PROPAGATION_EVENT', 'BB2', {
          ...flightPayload({ flight_id: 'BB2', delay_minutes: 20 }),
          predicted_delay_minutes: 20,
          predicted_arrival_time: '2026-01-01T00:00:00Z',
          model_confidence: 0.5,
          propagation_source: 'AA100-test',
          propagation_hops: 1,
        })
      ),
    });
  });

  await waitFor(() => expect(result.current.propagationChains.get('AA100-test')).toBeDefined());
  expect(result.current.propagationChains.get('AA100-test')).toEqual([
    { flight_id: 'BB2', delay_minutes: 20, hops: 1 },
  ]);
});

test('getPropagationChain resolves downstream and upstream from opposite ends of the same chain', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMsg('PROPAGATION_EVENT', 'BB2', {
          ...flightPayload({ flight_id: 'BB2', delay_minutes: 20 }),
          predicted_delay_minutes: 20,
          predicted_arrival_time: '2026-01-01T00:00:00Z',
          model_confidence: 0.5,
          propagation_source: 'AA100-test',
          propagation_hops: 1,
        })
      ),
    });
  });
  await waitFor(() => expect(result.current.propagationChains.size).toBe(1));

  // Downstream, viewed from the source's own perspective.
  expect(result.current.getPropagationChain('AA100-test').downstream).toEqual([
    { flight_id: 'BB2', delay_minutes: 20, hops: 1 },
  ]);
  expect(result.current.getPropagationChain('AA100-test').upstream).toBeNull();

  // Upstream, viewed from the affected flight's own perspective — this is
  // the exact case a live-testing session caught as a real bug (object
  // spread order silently showing the affected flight's own id instead of
  // its source's).
  const upstream = result.current.getPropagationChain('BB2').upstream;
  expect(upstream.flight_id).toBe('AA100-test');
  expect(upstream.hops).toBe(1);
  expect(result.current.getPropagationChain('BB2').downstream).toEqual([]);
});

test('GATE_REASSIGNMENT records old/new gate for the reassigned flight', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();

  act(() => {
    socket.onmessage({
      data: JSON.stringify(
        wsMsg('GATE_REASSIGNMENT', 'AA100-test', {
          ...flightPayload({ gate_id: 'B2' }),
          predicted_delay_minutes: 0,
          predicted_arrival_time: '2026-01-01T00:00:00Z',
          model_confidence: 1.0,
          gate_reassignment: { old_gate: 'A1', new_gate: 'B2' },
        })
      ),
    });
  });

  await waitFor(() => expect(result.current.gateReassignments.get('AA100-test')).toBeDefined());
  expect(result.current.gateReassignments.get('AA100-test')).toMatchObject({ old_gate: 'A1', new_gate: 'B2' });
});

test('HEARTBEAT does not create a phantom flight entry', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();

  act(() => {
    socket.onmessage({ data: JSON.stringify(wsMsg('HEARTBEAT', null, {})) });
  });

  await new Promise((r) => setTimeout(r, 150));
  expect(result.current.flights.size).toBe(0);
});

test('subscribeToFlight sends a subscribe action over the live socket', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();

  act(() => result.current.subscribeToFlight('AA100-test'));

  expect(socket.sent).toEqual([JSON.stringify({ action: 'subscribe', flight_id: 'AA100-test' })]);
});

test('goes DISCONNECTED on close, without requiring the caller to poll the raw socket', async () => {
  const { result } = renderHook(() => useFlightData('KJFK'));
  const socket = await openSocket();
  expect(result.current.connectionStatus).toBe(ConnectionStatus.CONNECTED);

  act(() => socket.close());

  await waitFor(() => expect(result.current.connectionStatus).toBe(ConnectionStatus.DISCONNECTED));
});
