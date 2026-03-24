# Environment Variables

All variables are configured in `.env` at the project root (copy from `.env.example`).
Docker Compose reads this file automatically.

---

## Producer

<!-- AUTO-GENERATED — source: docker-compose.yml producer.environment -->
| Variable | Required | Default | Description |
|---|---|---|---|
| `GITHUB_TOKEN` | No | _(empty)_ | GitHub personal access token. Raises rate limit from 60 to 5 000 req/h. Strongly recommended. |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `kafka:9092` | Kafka broker address. Use `kafka:9092` inside Docker; `localhost:9094` from the host. |
| `POLL_INTERVAL_SECONDS` | No | `10` | Seconds between GitHub public events API polls. |
| `MAX_PAGES` | No | `3` | Number of pages to fetch per poll. Each page contains ~30 events. |
<!-- /AUTO-GENERATED -->

---

## Consumer

<!-- AUTO-GENERATED — source: docker-compose.yml consumer.environment -->
| Variable | Required | Default | Description |
|---|---|---|---|
| `GITHUB_TOKEN` | No | _(empty)_ | GitHub PAT — used to fetch user profile data without hitting the 60 req/h unauthenticated limit. |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `kafka:9092` | Kafka broker address. |
| `DB_HOST` | Yes | `timescaledb` | TimescaleDB hostname. |
| `DB_PORT` | No | `5432` | TimescaleDB port. |
| `DB_NAME` | Yes | `github_events` | Database name. |
| `DB_USER` | Yes | `github` | Database user. |
| `DB_PASSWORD` | Yes | `github_secret` | Database password. |
<!-- /AUTO-GENERATED -->

---

## API (`api/`)

The API reads `DB_*` variables at runtime.

<!-- AUTO-GENERATED — source: api/main.py DB connection pool config -->
| Variable | Required | Default | Description |
|---|---|---|---|
| `DB_HOST` | Yes | `timescaledb` | TimescaleDB hostname. |
| `DB_PORT` | No | `5432` | TimescaleDB port. |
| `DB_NAME` | Yes | `github_events` | Database name. |
| `DB_USER` | Yes | `github` | Database user. |
| `DB_PASSWORD` | Yes | `github_secret` | Database password. |
<!-- /AUTO-GENERATED -->

---

## Frontend (`frontend/`)

The frontend uses two URL patterns to avoid browser/server network mismatches.

<!-- AUTO-GENERATED — source: frontend/Dockerfile ARG declarations -->
| Variable | Required | When resolved | Default | Description |
|---|---|---|---|---|
| `NEXT_PUBLIC_API_URL` | Yes | **Build time** (baked into client bundle) | `http://localhost:8000` | Base URL used by the browser SSE hook and any client-side fetches. Must be reachable from the user's browser. |
| `API_URL` | Yes | **Runtime** (server components only) | `http://api:8000` | Base URL used by Next.js server-side fetch calls. Must be reachable from the Next.js container, not from the browser. |
<!-- /AUTO-GENERATED -->

> **Why two variables?**
> `NEXT_PUBLIC_API_URL` is embedded into the JavaScript bundle at build time. It must resolve from the **user's browser** (e.g. `http://localhost:8000`).
> `API_URL` is read at request time by server components inside the container where Docker DNS resolves `api` to the API container.

---

## TimescaleDB

These variables are consumed by the official `timescale/timescaledb` Docker image.

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_DB` | `github_events` | Database to create on first start. |
| `POSTGRES_USER` | `github` | Superuser login. |
| `POSTGRES_PASSWORD` | `github_secret` | Superuser password. |

---

## Grafana

| Variable | Default | Description |
|---|---|---|
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Grafana admin password. Change in production. |
