import { render, screen } from '@testing-library/react';
import FlightDetail from '../components/FlightDetail';

function baseFlight(overrides = {}) {
  return {
    flight_id: 'AA100-test',
    airline_code: 'AA',
    flight_number: '100',
    origin: 'KJFK',
    destination: 'KLAX',
    aircraft_id: 'N1AA',
    gate_id: 'A1',
    scheduled_departure: '2026-01-01T12:00:00Z',
    scheduled_arrival: '2026-01-01T18:00:00Z',
    delay_minutes: 0,
    status: 'scheduled',
    ...overrides,
  };
}

test('shows a prompt when no flight is selected', () => {
  render(<FlightDetail flight={null} />);
  expect(screen.getByText('Click a plane to inspect.')).toBeInTheDocument();
});

test('renders core flight fields and an on-time delay label', () => {
  render(<FlightDetail flight={baseFlight()} />);
  expect(screen.getByText('AA100')).toBeInTheDocument();
  expect(screen.getByText('JFK → LAX')).toBeInTheDocument();
  expect(screen.getByText('N1AA')).toBeInTheDocument();
  expect(screen.getByText('A1')).toBeInTheDocument();
  expect(screen.getByText('On time')).toBeInTheDocument();
});

test('shows the delay in minutes when delayed', () => {
  render(<FlightDetail flight={baseFlight({ delay_minutes: 47 })} />);
  expect(screen.getByText('+47 min')).toBeInTheDocument();
});

test('shows prediction and a confidence label when a prediction is provided', () => {
  render(
    <FlightDetail
      flight={baseFlight({ delay_minutes: 47 })}
      prediction={{
        predicted_delay_minutes: 40,
        predicted_arrival_time: '2026-01-01T18:40:00Z',
        model_confidence: 0.85,
      }}
    />
  );
  expect(screen.getByText('high confidence')).toBeInTheDocument();
});

test('lists downstream impact with hop count and decay percentage', () => {
  render(
    <FlightDetail
      flight={baseFlight({ delay_minutes: 60 })}
      downstream={[{ flight_id: 'UA200-test', delay_minutes: 45, hops: 1 }]}
    />
  );
  expect(screen.getByText(/UA200-test: \+45 min \(1 hop away, 75% decay\)/)).toBeInTheDocument();
});

test('shows upstream impact using the source flight id, not the selected flight', () => {
  render(
    <FlightDetail
      flight={baseFlight({ flight_id: 'BB2-test', delay_minutes: 20 })}
      upstream={{ flight_id: 'AA100-test', delay_minutes: 20, hops: 1 }}
    />
  );
  expect(screen.getByText(/AA100-test: \+20 min \(1 hop away\)/)).toBeInTheDocument();
});

test('renders a GATE badge on the timeline when a gate reassignment is present', () => {
  render(
    <FlightDetail
      flight={baseFlight({ gate_id: 'B7' })}
      gateReassignment={{ old_gate: 'A1', new_gate: 'B7', at: '2026-01-01T12:30:00Z' }}
    />
  );
  expect(screen.getByText('GATE')).toBeInTheDocument();
  expect(screen.getByText(/A1 → B7/)).toBeInTheDocument();
});
