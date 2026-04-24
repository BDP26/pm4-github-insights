# Contributing

## Prerequisites

| Tool | Minimum version | Purpose |
|---|---|---|
| Docker + Compose plugin | Docker ≥ 24 | Run the full stack |
| Node.js | ≥ 18 | Frontend development |
| npm | ≥ 9 | Frontend package management |
| Python | 3.12 | API / producer / consumer development |

---

## Running the full stack

The recommended way to (re)start the production stack is `prod-run.sh` from the project root:

```bash
./prod-run.sh
```

This script: pulls latest code, stops any running stack (data volumes preserved), removes the Grafana volume so dashboards always reload from provisioning, rebuilds all images with `--no-cache`, and starts the `multi-consumer` profile in detached mode.

For manual control:

```bash
# Single consumer instance
docker compose --profile single-consumer up --build

# Three partition-pinned consumer instances (higher throughput)
docker compose --profile multi-consumer up --build
```

See [docs/RUNBOOK.md](RUNBOOK.md) for details on multi-instance mode and deployment procedures.

The FastAPI service (`api/`, port 8000) and Next.js frontend (`frontend/`, port 3000) are included in `docker-compose.yml` and start automatically as part of the full stack. They can also be run standalone (see below).

---

## Frontend development (standalone)

```bash
cd frontend
npm install --legacy-peer-deps
npm run dev
```

The app is served at http://localhost:3000.
The `--legacy-peer-deps` flag is required because Tremor v3 declares a React 18 peer dep while the project uses React 19.

### Available scripts

<!-- AUTO-GENERATED — source: frontend/package.json -->
| Command | Description |
|---|---|
| `npm run dev` | Start Next.js dev server with hot reload |
| `npm run build` | Production build (runs TypeScript type check) |
| `npm start` | Start the compiled production server |
<!-- /AUTO-GENERATED -->

---

## API development (standalone)

```bash
cd api
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Interactive docs: http://localhost:8000/docs

The API requires a running TimescaleDB. Start only the DB service:

```bash
docker compose up timescaledb
```

---

## Running tests

### Unit tests

Cover consumer enrichment, partition assignment, geocoder claim logic, db-writer message routing, and producer/consumer rate-limit helpers. No running infrastructure required — all external calls are mocked.

```bash
# From the project root
pip install pytest
python -m pytest tests/ -v
```

Key test files:

| File | What it covers |
|---|---|
| `tests/test_enrich_user.py` | `enrich_user` claim-before-fetch pattern, all GitHub actor types |
| `tests/test_enrich_repo.py` | `enrich_repo` claim-before-fetch pattern |
| `tests/test_geocoder.py` | Geocoder claim SQL, Nominatim response parsing |
| `tests/test_db_writer.py` | DB-writer routing of status and ratelimit messages |
| `tests/test_ratelimit.py` | `extract_ratelimit` helper, `_publish_ratelimit` |
| `tests/test_producer_ratelimit.py` | Producer `extract_ratelimit` helper |
| `tests/test_partition_config.py` | Multi-instance partition assignment logic |

### Integration tests

Spin up a real Kafka + TimescaleDB stack and verify that 3 partition-pinned consumer instances start correctly, consume all events exactly once, and produce no `NOT_COORDINATOR` errors.

```bash
# Requires: Docker, confluent-kafka Python package
pip install confluent-kafka pytest
python -m pytest tests/integration/test_multi_consumer.py -v -s
```

The integration test uses non-conflicting ports (Kafka: 19094, TimescaleDB: 15432) so it can run alongside the production stack. It starts and tears down its own isolated Docker Compose stack automatically. Allow ~2 minutes.

---

## Code style

- **TypeScript:** strict mode enabled (`tsconfig.json`). Zero type errors expected — `npm run build` must pass clean.
- **Tailwind CSS:** v4 syntax — use `@import "tailwindcss"` in CSS files, not the v3 `@tailwind` directives.
- **Python:** no linter is enforced; follow the style of the existing files (standard library imports first, then third-party).

---

## PR checklist

- [ ] `npm run build` passes with zero errors (frontend)
- [ ] `docker compose build api frontend` succeeds
- [ ] `docker compose build` succeeds (all services: consumer, producer, geocoder, db-writer, api, frontend)
- [ ] `docker compose --profile single-consumer up --build` starts without errors
- [ ] `python -m pytest tests/ -v` passes (unit tests, 47+ tests, no infrastructure needed)
- [ ] `python -m pytest api/tests/ -v` passes (scheduler unit tests)
- [ ] `python -m pytest tests/integration/ -v -s` passes (consumer integration tests, requires Docker)
- [ ] New environment variables are documented in `docs/ENV.md`
- [ ] Any new API endpoints are listed in the root `README.md`
