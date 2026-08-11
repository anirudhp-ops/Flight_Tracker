# Contributing

Bug reports, feature ideas, and pull requests are welcome. This is a prototype-stage personal project, not a large open-source project with a formal governance process — the guidelines below are about keeping the codebase consistent with itself, not bureaucracy for its own sake.

## Reporting bugs

Open a GitHub issue on this repository. Include:

- What you did, what you expected, what actually happened
- Whether it reproduces against `docker compose up` (the tested path — see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)) or only in a host-process setup
- Relevant log output — this app's structured JSON logs carry a `request_id` that traces one event through the whole pipeline; grepping for it (see [docs/DEVELOPMENT.md § Debugging](docs/DEVELOPMENT.md#debugging)) is usually more useful than a raw log dump

## Feature requests

Open a GitHub issue describing the use case, not just the implementation — several past design decisions in this project were revisited after the actual requirement turned out to differ from the original assumption (see `flight_tracker/events/IDEMPOTENCY.md`'s Phase E section for a real example of a decision that changed for good reason). Context on *why* you need something helps more than a prescribed API.

## Pull request process

1. Fork/branch, make your change.
2. Follow [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) — especially "How to add a feature" and the PR review checklist.
3. Make sure tests pass locally (see [Testing requirements](#testing-requirements) below) before opening the PR — CI (`.github/workflows/test.yml`) runs the same suites against real Postgres/Redis/Kafka service containers on every PR, and will fail if they don't.
4. Open the PR against `main` with a description of *what* changed and *why* — the why matters more here than usual, given how much of this codebase's own documentation exists specifically to record design rationale.
5. Address review feedback; once approved and green, it gets merged.

## Code style

- **Python**: no linter is currently wired into CI or configured in this repo (no `pylintrc`/`ruff`/`flake8` config exists yet) — match the existing code's style by reading nearby files, particularly its comment conventions (see below).
- **JavaScript/React**: ESLint via Create React App's built-in `react-app`/`react-app/jest` config (`frontend/package.json`), which runs automatically as part of `npm start`/`npm test`/`npm run build` — no separate lint command to remember to run.
- **Comments**: explain *why*, not *what* — see [docs/DEVELOPMENT.md § Code comment conventions](docs/DEVELOPMENT.md#code-comment-conventions). This is the single most consistent stylistic convention in this codebase; new code should match it.

## Testing requirements

- **New code touching the DB, cache, or Kafka pipeline should be tested against the real thing**, not a mock — this project deliberately tests against real local Postgres/Redis/Kafka throughout (see `flight_tracker/tests/test_workers.py`'s own docstring for why).
- **Coverage gates actually enforced in CI**: 70% backend (`.coveragerc`), 70%/70%/70%/60% frontend (statements/lines/functions/branches, `frontend/package.json`). A PR that drops either below its gate will fail CI.
- Run `pytest flight_tracker/tests/ -v` and `cd frontend && npm test -- --watchAll=false --coverage` before opening a PR — see [docs/DEVELOPMENT.md § Testing](docs/DEVELOPMENT.md#testing) for the full four-layer testing approach (unit → integration → load → benchmark) and when each one matters for your change.

## Commit message format

See [docs/DEVELOPMENT.md § Commit conventions](docs/DEVELOPMENT.md#commit-conventions) — a plain, specific, present/imperative-tense summary of the actual change.
