# Quick Reference

A cheat sheet of commands actually used in this project — see the linked doc for context on any of these.

## Local development

```bash
docker compose up -d                    # start everything (see docs/DEPLOYMENT.md)
docker compose logs -f backend          # follow backend logs
docker compose ps                       # container status/health
docker compose restart backend          # after an env var or code change
docker compose down                     # stop everything (-v to also drop volumes)
docker compose build backend            # rebuild after requirements.txt/source change

uvicorn flight_tracker.server:app --reload   # host-run backend, hot reload
cd frontend && npm start                     # host-run frontend, hot reload
```

## Testing

```bash
pytest flight_tracker/tests/ -v                                          # backend unit tests
pytest flight_tracker/tests/ --cov --cov-config=.coveragerc --cov-report=term-missing
cd frontend && npm test -- --watchAll=false --coverage                    # frontend unit tests

python scripts/integration_tests.py                                      # real pipeline, real infra
k6 run scripts/load_test_k6.js                                           # WebSocket load
python scripts/load_test_kafka.py all                                    # Kafka throughput + cascade
python scripts/benchmark.py all                                          # component benchmarks

./scripts/smoke-tests.sh                                                 # health + API + WebSocket
```

## Health & debugging

```bash
curl http://localhost:8000/health              # aggregated status
curl http://localhost:8000/health/db            # DB row counts + pool stats
curl http://localhost:8000/health/dlq           # dead-letter count
python scripts/inspect_dlq.py                   # inspect actual failed events
curl http://localhost:8000/metrics              # Prometheus text format
```

## Connect to infrastructure

**If running via `docker compose up`** (non-default host ports for Postgres/Redis — see docs/DEPLOYMENT.md):

```bash
psql -h localhost -p 5433 -U postgres -d flight_tracker
redis-cli -p 6380
kafka-topics.sh --list --bootstrap-server localhost:9092   # Kafka's host port isn't remapped
```

**If running natively / host processes** (default ports):

```bash
psql -h localhost -U postgres -d flight_tracker
redis-cli
kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Common ad-hoc commands

```bash
redis-cli FLUSHALL                                          # clear the cache (safe — cache-aside, see DATA_MODEL.md)
scripts/create_kafka_topics.sh                               # idempotent — create/fix topic partition counts
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dead-letter-events --from-beginning
psql -d flight_tracker -c "SELECT count(*) FROM active_flights;"
psql -d flight_tracker -c "EXPLAIN ANALYZE SELECT * FROM active_flights WHERE airport_code = 'KJFK';"
ps aux | grep uvicorn                                         # check for a stray second backend instance
```

## Monitoring

| | URL |
|---|---|
| Frontend | http://localhost:3000 |
| Backend API | http://localhost:8000 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 (`admin`/`admin`) |

## Docs map

| Need to... | See |
|---|---|
| Understand the system | [docs/ARCHITECTURE.md](ARCHITECTURE.md) |
| Call an endpoint | [docs/API.md](API.md) |
| Understand the schema/models | [docs/DATA_MODEL.md](DATA_MODEL.md) |
| Deploy or configure | [docs/DEPLOYMENT.md](DEPLOYMENT.md) |
| Add a feature | [docs/DEVELOPMENT.md](DEVELOPMENT.md) |
| Fix something broken | [docs/TROUBLESHOOTING.md](TROUBLESHOOTING.md) |
| Check real performance numbers | [docs/BENCHMARKS.md](BENCHMARKS.md), [docs/PERFORMANCE.md](PERFORMANCE.md) |
