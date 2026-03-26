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

```bash
docker compose up --build
```

This starts: Kafka (KRaft), Kafka-UI, TimescaleDB, producer, consumer, Grafana.

The FastAPI service and Next.js frontend have individual Dockerfiles (`api/` and `frontend/`) and can be added to the compose file or run standalone (see below).

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

## Code style

- **TypeScript:** strict mode enabled (`tsconfig.json`). Zero type errors expected — `npm run build` must pass clean.
- **Tailwind CSS:** v4 syntax — use `@import "tailwindcss"` in CSS files, not the v3 `@tailwind` directives.
- **Python:** no linter is enforced; follow the style of the existing files (standard library imports first, then third-party).

---

## PR checklist

- [ ] `npm run build` passes with zero errors (frontend)
- [ ] `docker build -t test-api ./api` succeeds (API)
- [ ] `docker compose up --build` starts without errors
- [ ] New environment variables are documented in `docs/ENV.md`
- [ ] Any new API endpoints are listed in the root `README.md`
