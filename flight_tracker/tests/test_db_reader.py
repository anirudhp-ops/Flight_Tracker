"""
Unit tests for flight_tracker/db/reader.py. Runs against the real local
Postgres — consistent with this project's no-mocks testing convention.

Run: pytest flight_tracker/tests/test_db_reader.py -v
"""
import uuid
from datetime import datetime, timezone

import pytest

from flight_tracker.db import reader as db_reader
from flight_tracker.db.writer import create_pool, ensure_schema, write_events
from flight_tracker.models.events import EventType, FlightEvent, FlightStatus


@pytest.fixture
async def pool():
    p = await create_pool()
    # CI's Postgres service container is fresh every run — a local
    # Postgres normally already has the schema from a prior server.py
    # startup (ensure_schema() runs there too), which is why this was
    # only ever caught in CI, not locally.
    await ensure_schema(p)
    yield p
    await p.close()


def _flight(flight_id, **overrides):
    now = datetime.now(timezone.utc)
    defaults = dict(
        flight_id=flight_id, event_type=EventType.DEPARTURE, airline_code="UT", flight_number="1",
        origin="KJFK", destination="KLAX", scheduled_departure=now, scheduled_arrival=now,
        delay_minutes=0, status=FlightStatus.SCHEDULED, timestamp=now,
    )
    defaults.update(overrides)
    return FlightEvent(**defaults)


async def test_get_flight_status_returns_none_for_unknown_flight(pool):
    result = await db_reader.get_flight_status(pool, f"NONEXISTENT-{uuid.uuid4().hex[:8]}")
    assert result is None


async def test_get_flight_status_returns_latest_row(pool):
    flight_id = f"UTEST-{uuid.uuid4().hex[:8]}"
    await write_events(pool, [_flight(flight_id, delay_minutes=10)], airport_code="KJFK")

    result = await db_reader.get_flight_status(pool, flight_id)

    assert result is not None
    assert result["flight_id"] == flight_id
    assert result["delay_minutes"] == 10


async def test_get_airport_snapshot_filters_by_airport_code(pool):
    tag = uuid.uuid4().hex[:8]
    jfk_id = f"JFKTEST-{tag}"
    lax_id = f"LAXTEST-{tag}"
    # Distinct airline_code/flight_number so the two rows get different
    # flight_keys (active_flights' primary key) — same defaults for both
    # would collide and the second write would overwrite the first.
    await write_events(pool, [_flight(jfk_id, airline_code="J1", flight_number="1")], airport_code="KJFK")
    await write_events(pool, [_flight(lax_id, airline_code="L2", flight_number="2")], airport_code="KLAX")

    snapshot = await db_reader.get_airport_snapshot(pool, "KJFK")
    flight_ids = {row["flight_id"] for row in snapshot}

    assert jfk_id in flight_ids
    assert lax_id not in flight_ids


async def test_get_airport_snapshot_empty_for_unknown_airport(pool):
    result = await db_reader.get_airport_snapshot(pool, f"UNKNOWN-{uuid.uuid4().hex[:8]}")
    assert result == []


async def test_get_recent_delays_only_returns_delayed_events(pool):
    flight_id = f"UTEST-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    await write_events(
        pool,
        [_flight(flight_id, delay_minutes=0, timestamp=now)],
        airport_code="KJFK",
    )
    await write_events(
        pool,
        [_flight(flight_id, delay_minutes=20, status=FlightStatus.ACTIVE,
                  timestamp=now.replace(microsecond=0))],
        airport_code="KJFK",
    )

    results = await db_reader.get_recent_delays(pool, flight_id)

    assert len(results) >= 1
    assert all(r["delay_minutes"] > 0 for r in results)


async def test_get_recent_delays_respects_limit(pool):
    flight_id = f"UTEST-{uuid.uuid4().hex[:8]}"
    from datetime import timedelta
    base = datetime.now(timezone.utc)
    for i in range(5):
        await write_events(
            pool,
            [_flight(flight_id, delay_minutes=10 + i, timestamp=base + timedelta(seconds=i))],
            airport_code="KJFK",
        )

    results = await db_reader.get_recent_delays(pool, flight_id, limit=2)
    assert len(results) == 2
