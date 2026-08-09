"""
@retry_with_backoff — exponential backoff with jitter for transient
failures in DB writes and Kafka publishes.

delay = min(max_delay_ms, initial_delay_ms * backoff_factor ** (attempt-1))
      * (1 + uniform(-jitter_fraction, +jitter_fraction))

The jitter exists specifically because this app has WORKER_COUNT concurrent
workers: without it, N workers hitting the same transient failure (a brief
Postgres blip, a broker hiccup) at close to the same moment would all sleep
for the *exact* same duration and retry in a synchronized burst — a
thundering herd hitting the just-recovering dependency at once. ±10%
spreads that burst out.
"""
import asyncio
import functools
import random
from typing import Callable, Optional, Type

from flight_tracker.config import settings


def retry_with_backoff(
    max_attempts: int | None = None,
    initial_delay_ms: int | None = None,
    max_delay_ms: int | None = None,
    backoff_factor: float | None = None,
    jitter_fraction: float | None = None,
    exceptions: tuple[Type[BaseException], ...] = (Exception,),
    on_retry: Optional[Callable[[], None]] = None,
):
    """
    Decorator factory for async functions — always used as
    @retry_with_backoff(...), parameters optional (fall back to
    settings.worker_retry_*). Retries on `exceptions`; re-raises the last
    exception if every attempt is exhausted. `max_attempts` is the total
    number of tries including the first (matches WORKER_RETRY_MAX_ATTEMPTS's
    name — 3 means 3 attempts, 2 retries after the first failure, not 3
    retries on top of a first try).
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            attempts = max_attempts if max_attempts is not None else settings.worker_retry_max_attempts
            delay_ms = initial_delay_ms if initial_delay_ms is not None else settings.worker_retry_initial_delay_ms
            cap_ms = max_delay_ms if max_delay_ms is not None else settings.worker_retry_max_delay_ms
            factor = backoff_factor if backoff_factor is not None else settings.worker_retry_backoff_factor
            jitter = jitter_fraction if jitter_fraction is not None else settings.worker_retry_jitter_fraction

            last_exc: BaseException | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt >= attempts:
                        break
                    base_ms = min(cap_ms, delay_ms * (factor ** (attempt - 1)))
                    jittered_ms = base_ms * (1 + random.uniform(-jitter, jitter))
                    sleep_s = max(0.0, jittered_ms) / 1000
                    print(
                        f"retry_with_backoff: {func.__qualname__} attempt {attempt}/{attempts} "
                        f"failed ({e!r}), retrying in {sleep_s * 1000:.0f}ms"
                    )
                    if on_retry is not None:
                        on_retry()
                    await asyncio.sleep(sleep_s)
            raise last_exc

        return wrapper
    return decorator
