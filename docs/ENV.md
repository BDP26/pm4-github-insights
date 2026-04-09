# Environment Variables

All variables are configured in `.env` at the project root (copy from `.env.example`).
Docker Compose reads this file automatically.

---

## Producer

<!-- AUTO-GENERATED — source: docker-compose.yml producer.environment -->
| Variable | Required | Default | Description |
|---|---|---|---|
| `GITHUB_TOKEN_EVENTS` | No | _(empty)_ | GitHub PAT used to poll the public events API. Raises rate limit from 60 to 5 000 req/h. Strongly recommended. |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `kafka:9092` | Kafka broker address. Use `kafka:9092` inside Docker; `localhost:9094` from the host. |
| `POLL_INTERVAL_SECONDS` | No | `10` | Seconds between GitHub public events API polls. |
| `MAX_PAGES` | No | `3` | Number of pages to fetch per poll. Each page contains ~30 events. |
<!-- /AUTO-GENERATED -->

---

## Consumer

<!-- AUTO-GENERATED — source: docker-compose.yml consumer.environment -->
| Variable | Required | Default | Description |
|---|---|---|---|
| `GITHUB_TOKEN_USER` | No | _(empty)_ | GitHub PAT used to fetch user profile data (`/users/:login`). Raises rate limit from 60 to 5 000 req/h. |
| `GITHUB_TOKEN_REPO` | No | _(empty)_ | GitHub PAT used to fetch repository metadata (`/repos/:owner/:repo`). Separate token allows independent rate-limit budgets. |
| `GITHUB_TOKENS_USER` | No | _(empty)_ | Comma-separated list of GitHub PATs for user enrichment. Takes precedence over `GITHUB_TOKEN_USER`. Each token has its own 5,000 req/hour budget. |
| `GITHUB_TOKENS_REPO` | No | _(empty)_ | Comma-separated list of GitHub PATs for repo enrichment. Takes precedence over `GITHUB_TOKEN_REPO`. |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `kafka:9092` | Kafka broker address. |
| `DB_HOST` | Yes | `timescaledb` | TimescaleDB hostname. |
| `DB_PORT` | No | `5432` | TimescaleDB port. |
| `DB_NAME` | Yes | `github_events` | Database name. |
| `DB_USER` | Yes | `github` | Database user. |
| `DB_PASSWORD` | Yes | `github_secret` | Database password. |
| `KAFKA_MULTI_INSTANCE_ENABLED` | No | `false` | Set to `true` to enable partition-aware multi-instance mode. When enabled, each instance pins itself to a specific partition subset via `assign()` instead of `subscribe()`. |
| `KAFKA_INSTANCE_INDEX` | Conditional | — | **Required when `KAFKA_MULTI_INSTANCE_ENABLED=true`.** 0-based index of this consumer instance (e.g. `0`, `1`, `2`). Consumer exits with a clear error if omitted when multi-instance is enabled. |
| `KAFKA_TOTAL_INSTANCES` | No | `3` | Total number of consumer instances running in parallel. Used together with `KAFKA_INSTANCE_INDEX` to calculate the deterministic partition assignment: `partition % total == index`. |
<!-- /AUTO-GENERATED -->

---

## Geocoder (`geocoder/`)

<!-- AUTO-GENERATED — source: docker-compose.yml geocoder.environment -->
| Variable | Required | Default | Description |
|---|---|---|---|
| `DB_HOST` | Yes | `timescaledb` | TimescaleDB hostname. |
| `DB_PORT` | No | `5432` | TimescaleDB port. |
| `DB_NAME` | Yes | `github_events` | Database name. |
| `DB_USER` | Yes | `github` | Database user. |
| `DB_PASSWORD` | Yes | `github_secret` | Database password. |
<!-- /AUTO-GENERATED -->

No GitHub API key or Nominatim key is required. The geocoder calls the free OpenStreetMap Nominatim API (1 req/s rate limit enforced internally).

---

## DB Writer (`db-writer/`)

<!-- AUTO-GENERATED — source: docker-compose.yml db-writer.environment -->
| Variable | Required | Default | Description |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | `kafka:9092` | Kafka broker address. |
| `DB_HOST` | Yes | `timescaledb` | TimescaleDB hostname. |
| `DB_PORT` | No | `5432` | TimescaleDB port. |
| `DB_NAME` | Yes | `github_events` | Database name. |
| `DB_USER` | Yes | `github` | Database user. |
| `DB_PASSWORD` | Yes | `github_secret` | Database password. |
<!-- /AUTO-GENERATED -->

The db-writer consumes `github.events.status` and `github.ratelimit` topics and inserts rows into `request_logs` and `rate_limit_snapshots` respectively.

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

<!-- AUTO-GENERATED — source: frontend/Dockerfile ARG declarations, frontend/src/lib/api.ts -->
| Variable | Required | When resolved | Default | Description |
|---|---|---|---|---|
| `NEXT_PUBLIC_API_URL` | Yes | **Build time** (baked into client bundle) | `http://localhost:8000` | Base URL used by the browser SSE hook and any client-side fetches. Must be reachable from the user's browser. |
| `API_URL` | No | **Runtime** (server-side only) | `http://localhost:8000` | Base URL used by Next.js Server Components and Route Handlers to reach the API over the internal network. Never exposed to the browser. Set to the internal Docker hostname (e.g. `http://api:8000`) when running in Docker. |
<!-- /AUTO-GENERATED -->

> **Why this variable?**
> `NEXT_PUBLIC_API_URL` is embedded into the JavaScript bundle at build time. It must resolve from the **user's browser** (e.g. `http://localhost:8000`).

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
