"""
Supervisor: runs a WorkerPool's workers and restarts any that crash. Kept
separate from worker_pool.py deliberately — WorkerPool knows how to start/
stop/aggregate workers; this class is the only thing that decides what
happens when one dies unexpectedly.
"""
import asyncio

from flight_tracker.config import settings
from flight_tracker.workers.worker_pool import Worker, WorkerPool


class Supervisor:
    def __init__(self, pool: WorkerPool):
        self.pool = pool
        self.restarts_per_worker: dict[str, int] = {w.worker_id: 0 for w in pool.workers}
        self.failures_per_worker: dict[str, int] = {w.worker_id: 0 for w in pool.workers}
        self._stopping = False
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        await self.pool.start()
        self._tasks = [
            asyncio.create_task(self._supervised_loop(w), name=f"supervisor-{w.worker_id}")
            for w in self.pool.workers
        ]

    async def _supervised_loop(self, worker: Worker) -> None:
        """
        Runs worker.run() forever, restarting it on any unexpected crash.
        worker.run() itself never self-heals (single responsibility — see
        worker_pool.py); this is the only place that decides "log loudly,
        wait, restart."
        """
        while not self._stopping:
            try:
                await worker.run()
                return  # run() returned normally (stop() flipped _stopping) — done
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.failures_per_worker[worker.worker_id] += 1
                print(
                    f"FATAL: worker {worker.worker_id} crashed: {e!r} — restarting in "
                    f"{settings.worker_supervisor_restart_delay_seconds}s "
                    f"(failure #{self.failures_per_worker[worker.worker_id]} for this worker)"
                )
                try:
                    await worker.stop()
                except Exception as stop_err:
                    print(f"Supervisor: error stopping crashed worker {worker.worker_id}: {stop_err!r}")

                await asyncio.sleep(settings.worker_supervisor_restart_delay_seconds)
                if self._stopping:
                    return

                self.restarts_per_worker[worker.worker_id] += 1
                worker.restarts += 1
                await worker.start()
                print(f"Supervisor: worker {worker.worker_id} restarted (restart #{worker.restarts})")

    async def run_forever(self):
        """
        Awaits until stop() cancels every supervised loop. Uses
        asyncio.gather(..., return_exceptions=True) per the phase spec: the
        crash-catch-restart logic lives in _supervised_loop above, so in
        normal operation these tasks only ever end via cancellation
        (stop()) — gather() here just runs the whole pool concurrently as
        one awaitable unit, with return_exceptions=True as a safety net in
        case a supervised loop's own bug ever escapes its try/except.
        """
        return await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        self._stopping = True
        for t in self._tasks:
            if not t.done():
                t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        await self.pool.stop()

    def log_metrics(self) -> None:
        total_restarts = sum(self.restarts_per_worker.values())
        total_failures = sum(self.failures_per_worker.values())
        print(
            f"Supervisor: {len(self.pool.workers)} workers managed, "
            f"total restarts={total_restarts}, total worker-crashes={total_failures}"
        )

    async def run_periodic_metrics_logging(self) -> None:
        """
        Logs the one-line pool-wide summary every
        settings.kafka_metrics_log_interval_seconds, in the exact format
        the phase brief specifies: "Workers: N, events/sec: X, lag: Y,
        errors: Z, restarts: W". Per-worker detail (latency histogram,
        individual counters) is available via WorkerPool/each
        AsyncEventProcessor directly — see scripts/load_test_workers.py for
        where that gets used (percentiles, per-worker breakdown), kept out
        of this line so it stays readable as a steady heartbeat.
        """
        last_processed = self.pool.total_events_processed
        try:
            while not self._stopping:
                await asyncio.sleep(settings.kafka_metrics_log_interval_seconds)
                if self._stopping:
                    break
                current_processed = self.pool.total_events_processed
                events_per_sec = (
                    (current_processed - last_processed) / settings.kafka_metrics_log_interval_seconds
                )
                last_processed = current_processed
                lag = await self.pool.total_lag()
                errors = self.pool.total_events_failed
                restarts = sum(self.restarts_per_worker.values())
                print(
                    f"Workers: {len(self.pool.workers)}, events/sec: {events_per_sec:.1f}, "
                    f"lag: {lag}, errors: {errors}, restarts: {restarts}"
                )
        except asyncio.CancelledError:
            pass
