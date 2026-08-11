# Development Workflow

## Setup

Follow [docs/DEPLOYMENT.md § Local development: host processes](DEPLOYMENT.md#local-development-host-processes-no-docker) for hot-reload backend/frontend development, or [§ docker-compose](DEPLOYMENT.md#local-development-docker-compose) to run the whole stack in containers.

## How to add a feature

There's no formal template, but the pattern this codebase already follows consistently (visible in every `flight_tracker/**/*.md` file) is worth matching:

1. **Read the relevant subsystem's own doc first** — `flight_tracker/graph/STATE_MANAGEMENT.md`, `flight_tracker/workers/CONCURRENCY.md`, `flight_tracker/events/KAFKA_ARCHITECTURE.md`, `flight_tracker/db/DATABASE_DESIGN.md`, etc. Several past changes in this project's history were driven by a brief's assumption about the current state turning out to be wrong (see `frontend/PHASE_G_REPORT.md`'s opening section for a real example) — reading the code and its doc first avoids redoing that.
2. **Write the test first, or alongside** — this project has a strict "no mocks for its own components" convention (real Postgres/Redis/Kafka, not fakes — see `flight_tracker/tests/test_workers.py`'s own docstring, and `flight_tracker/TESTING.md`). A new feature touching the DB, cache, or Kafka pipeline should be tested against the real thing, not a mock of it.
3. **Comment the *why*, not the *what***, only where a design decision, a workaround, or a non-obvious constraint would otherwise be invisible to the next reader — see [Code comment conventions](#code-comment-conventions) below. This codebase's existing comments are the best reference for the style to match.
4. **Update the subsystem doc if the design decision changes**, in place, rather than leaving a stale doc that contradicts the code (see [docs/ARCHITECTURE.md](ARCHITECTURE.md)'s staleness note on `KAFKA_ARCHITECTURE.md` for what happens when this doesn't happen — a real example already in this repo).
5. **Run the relevant test layer(s)** — see [Testing](#testing) below.

## Testing

Four layers, in order — see [`flight_tracker/TESTING.md`](../flight_tracker/TESTING.md) for the full reference; summarized here:

```bash
# 1. Unit tests — is each component correct in isolation?
pytest flight_tracker/tests/ -v
pytest flight_tracker/tests/ --cov --cov-config=.coveragerc --cov-report=term-missing
cd frontend && npm test -- --watchAll=false --coverage

# 2. Integration tests — does the real, deployed pipeline work end to end?
python scripts/integration_tests.py

# 3. Load tests — does it hold up under realistic concurrent load?
k6 run scripts/load_test_k6.js
python scripts/load_test_kafka.py all

# 4. Benchmarks — how does latency scale with data size?
python scripts/benchmark.py all
```

**Coverage gates actually enforced in CI**: 70% backend (`.coveragerc`'s `fail_under = 70`; `server.py` and `ingestion/worker.py` are excluded by design — thin orchestration, integration-tested instead of unit-tested, and their 0% coverage would otherwise pull the number down from the ~91% actually achieved on unit-testable code), 70%/70%/70%/60% frontend (statements/lines/functions/branches, `frontend/package.json`'s `coverageThreshold`).

**Before trusting a load/integration test number**, confirm no other instance of the app is already running (`ps aux | grep uvicorn`, `docker compose ps`) — a second process joins the same Kafka consumer groups and silently splits partitions with the one your test started. This is not a theoretical warning; it happened during this project's own Phase I development and cost 30/50 apparently-missing cascade propagations before being traced to a stray process.

## Debugging

- **Structured logs first**: every log line is JSON with `request_id`, `flight_id`, `worker_id` fields (`flight_tracker/logging_config.py`). For the Kafka pipeline, `request_id` is the envelope's `event_id`, which flows unchanged from `flight-events` through to the WebSocket layer — grep for one `request_id` across `docker compose logs backend` (or your terminal) to trace a single event's entire path through the system. See [`flight_tracker/OBSERVABILITY.md`](../flight_tracker/OBSERVABILITY.md#logging).
- **`GET /health/dlq` and `scripts/inspect_dlq.py`** for anything that failed processing — every dead-lettered message carries its original payload, error type/message/traceback, and timestamp.
- **Standard `pdb`/IDE breakpoints work as normal** against a host-run `uvicorn --reload` process; they don't work against the Docker Compose backend without extra setup (attaching a debugger to a container) — use the host-process workflow when you need to actually step through code.
- **The Grafana dashboard** (`http://localhost:3001`) for anything latency/throughput/error-rate related — see [docs/TROUBLESHOOTING.md](TROUBLESHOOTING.md) for specific symptom → panel mappings.

## Profiling / load testing

```bash
python scripts/benchmark.py graph   # or: db | cache | ml | all
python scripts/load_test_kafka.py throughput --rate <N> --duration <S>
python scripts/load_test_kafka.py cascade --count <N> --fanout <N>
k6 run scripts/load_test_k6.js
```

See [docs/BENCHMARKS.md](BENCHMARKS.md) for what each isolates and the last real results, and [docs/PERFORMANCE.md](PERFORMANCE.md) for how to interpret a regression against the documented baseline.

## Code comment conventions

This codebase's dominant style, worth matching in new code: comments explain **why**, not what — a hidden constraint, the reasoning behind a non-default choice, a workaround for a specific bug that was actually found, or a tradeoff that was deliberately made one way over another. A comment that just restates what the next line of code obviously does is not this project's style and should be left out. Good examples to model new comments on: `flight_tracker/graph/engine.py`'s `resolve_gate_conflicts()` (explains *why* `old_gate` is captured before mutation, referencing the actual test that caught the bug), `flight_tracker/workers/retry.py`'s module docstring (explains why jitter exists, not just what the formula is).

## Commit conventions

This repo's history mixes two styles: early development used short, plain descriptions (`add bfs`, `websocket activate`); once formal project phases began, commits switched to `Phase <letter>: <what changed>` (e.g. `Phase F: delay propagation integration`) for phase-level work. For a feature or fix that isn't a whole phase, a plain, specific, present/imperative-tense summary of the change (not the task or ticket it came from) is the right style — match whichever convention your change actually fits, don't force a `Phase X:` prefix onto a small fix.

## PR review checklist

- [ ] Tests added/updated for the actual change (not just happy-path — see this project's "no mocks for its own components" convention)
- [ ] `pytest flight_tracker/tests/` and `npm test` both pass locally
- [ ] Coverage didn't drop below the enforced gates (70% backend, 70/70/70/60% frontend)
- [ ] Any new/changed env var is added to `.env.example` and [docs/DEPLOYMENT.md](DEPLOYMENT.md#environment-variables)
- [ ] Any new/changed REST or WebSocket surface is reflected in [docs/API.md](API.md)
- [ ] A subsystem doc (`flight_tracker/**/*.md`) is updated if the change alters a documented design decision, not left to silently contradict the code
- [ ] Comments explain *why*, not *what* (see above) — and only where genuinely non-obvious
- [ ] No secrets, API keys, or hardcoded local paths committed
