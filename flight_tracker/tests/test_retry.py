"""
Unit tests for flight_tracker/workers/retry.py (@retry_with_backoff). Pure
async logic, no infra needed.

Run: pytest flight_tracker/tests/test_retry.py -v
"""
import time

import pytest

from flight_tracker.workers.retry import retry_with_backoff


async def test_retry_succeeds_after_transient_failures():
    """Fails twice, succeeds on the 3rd attempt."""
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=3, initial_delay_ms=10, backoff_factor=2.0)
    async def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionError(f"transient failure {calls['count']}")
        return "recovered"

    result = await flaky()
    assert result == "recovered"
    assert calls["count"] == 3


async def test_retry_exhausts_and_raises_last_error():
    """max_attempts exceeded -> raises the last exception, not fewer/more tries."""
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=3, initial_delay_ms=10)
    async def always_fails():
        calls["count"] += 1
        raise ValueError(f"permanent failure {calls['count']}")

    with pytest.raises(ValueError, match="permanent failure 3"):
        await always_fails()
    assert calls["count"] == 3


async def test_retry_succeeds_on_first_attempt_no_retry_needed():
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=3, initial_delay_ms=10)
    async def works_first_try():
        calls["count"] += 1
        return "ok"

    result = await works_first_try()
    assert result == "ok"
    assert calls["count"] == 1


async def test_retry_only_catches_specified_exception_types():
    @retry_with_backoff(max_attempts=3, initial_delay_ms=10, exceptions=(ConnectionError,))
    async def raises_unlisted_type():
        raise ValueError("not a ConnectionError, should not be retried")

    with pytest.raises(ValueError):
        await raises_unlisted_type()


async def test_retry_backoff_grows_exponentially():
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=3, initial_delay_ms=50, backoff_factor=2.0, jitter_fraction=0.0)
    async def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ConnectionError("fail")
        return "ok"

    t0 = time.perf_counter()
    await flaky()
    elapsed = time.perf_counter() - t0
    # no jitter: delays are exactly 50ms then 100ms = 150ms total
    assert 0.13 < elapsed < 0.25, f"expected ~0.15s of backoff delay, got {elapsed:.3f}s"


async def test_retry_delay_is_capped_at_max_delay_ms():
    calls = {"count": 0}

    @retry_with_backoff(max_attempts=4, initial_delay_ms=1000, max_delay_ms=1100,
                         backoff_factor=10.0, jitter_fraction=0.0)
    async def flaky():
        calls["count"] += 1
        if calls["count"] < 4:
            raise ConnectionError("fail")
        return "ok"

    t0 = time.perf_counter()
    await flaky()
    elapsed = time.perf_counter() - t0
    # Uncapped this would be 1s + 10s + 100s; capped at 1.1s per retry * 3 = 3.3s.
    assert elapsed < 4.0, f"max_delay_ms cap was not applied, took {elapsed:.2f}s"


async def test_retry_jitter_produces_varying_delays_not_a_constant():
    """Runs the same flaky() several times with jitter enabled and checks
    that observed elapsed retry time is not identical every run — the
    whole point of jitter (see retry.py's module docstring: avoiding a
    thundering-herd of perfectly-synchronized retries)."""
    elapsed_times = []
    for _ in range(6):
        calls = {"count": 0}

        @retry_with_backoff(max_attempts=2, initial_delay_ms=200, jitter_fraction=0.3)
        async def flaky():
            calls["count"] += 1
            if calls["count"] < 2:
                raise ConnectionError("fail")
            return "ok"

        t0 = time.perf_counter()
        await flaky()
        elapsed_times.append(time.perf_counter() - t0)

    assert len(set(round(t, 4) for t in elapsed_times)) > 1, (
        f"all retry delays were identical despite jitter_fraction=0.3: {elapsed_times}"
    )
    # Sanity: jitter is bounded, not unbounded — every run stays near 200ms +/- 30%.
    for t in elapsed_times:
        assert 0.13 < t < 0.30


async def test_retry_on_retry_callback_fires_once_per_retry_not_per_attempt():
    retries = {"count": 0}
    calls = {"count": 0}

    @retry_with_backoff(
        max_attempts=4, initial_delay_ms=5,
        on_retry=lambda: retries.__setitem__("count", retries["count"] + 1),
    )
    async def flaky():
        calls["count"] += 1
        if calls["count"] < 4:
            raise ConnectionError("fail")
        return "ok"

    await flaky()
    assert retries["count"] == 3  # 4 attempts total, 3 retries between them


async def test_retry_on_retry_not_called_when_first_attempt_succeeds():
    retries = {"count": 0}

    @retry_with_backoff(
        max_attempts=3, initial_delay_ms=5,
        on_retry=lambda: retries.__setitem__("count", retries["count"] + 1),
    )
    async def works_first_try():
        return "ok"

    await works_first_try()
    assert retries["count"] == 0
