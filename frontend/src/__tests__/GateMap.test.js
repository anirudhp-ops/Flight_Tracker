import { render, screen, fireEvent } from '@testing-library/react';
import GateMap from '../components/GateMap';

function flight(id, gate, delay = 0) {
  return { flight_id: id, gate_id: gate, delay_minutes: delay };
}

test('clicking an occupied gate lists the flight(s) using it', () => {
  const flights = new Map([
    ['AA1', flight('AA1', 'A1', 0)],
    ['AA2', flight('AA2', 'A1', 15)],
  ]);
  render(<GateMap flights={flights} />);

  fireEvent.click(screen.getByTitle('A1'));

  expect(screen.getByText('A1')).toBeInTheDocument();
  expect(screen.getByText(/AA1/)).toBeInTheDocument();
  expect(screen.getByText(/AA2 \(\+15m\)/)).toBeInTheDocument();
});

test('clicking an empty gate reports it as empty', () => {
  render(<GateMap flights={new Map()} />);

  fireEvent.click(screen.getByTitle('B5'));

  expect(screen.getByText(/empty/)).toBeInTheDocument();
});

test('a recently reassigned gate is flagged even without knowing occupancy yet', () => {
  const gateReassignments = new Map([['AA1', { old_gate: 'A1', new_gate: 'A2', at: new Date().toISOString() }]]);
  const now = Date.now();
  render(<GateMap flights={new Map()} gateReassignments={gateReassignments} now={now} />);

  const gateA2 = screen.getByTitle('A2');
  expect(gateA2).toHaveStyle({ background: 'rgb(248, 81, 73)' });
});
