# GitHub Events Streaming Stack

Real-time pipeline that ingests the GitHub public events API into Kafka,
enriches each event with geographic + profile data, stores everything in
TimescaleDB, and visualises it live in a Next.js dashboard and Grafana.

---

## Architecture

```
GitHub API
    │  (poll every 10 s)
    ▼
┌─────────────────────────────────────┐
│  Producer  (producer/producer.py)   │
│  • Deduplicates by event ID         │
│  • Publishes raw JSON to Kafka      │
└──────────────┬──────────────────────┘
               │  topic: github.events.raw
               ▼
┌─────────────────────────────────────┐
│  Kafka  (KRaft, no Zookeeper)       │
│  • Broker + Controller in one node  │
│  • Snappy compression               │
│  • 48 h retention / 512 MB cap      │
└──────────────┬──────────────────────┘
               │  consumer group: github-events-enricher
               ▼
┌─────────────────────────────────────┐
│  Consumer  (consumer/consumer.py)   │
│  • Fetches GitHub user profiles     │
│  • Geocodes location strings        │
│    (Nominatim, 1 req/s, cached)     │
│  • Writes enriched rows to DB       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  TimescaleDB  (PostgreSQL + TSE)    │
│                                     │
│  events          ← hypertable       │
│                    1-day chunks     │
│                    7-day compress   │
│                    90-day retention │
│                                     │
│  event_stats_5m  ← continuous agg  │
│  country_stats_5m← continuous agg  │
│  actor_stats_1h  ← continuous agg  │
└──────────┬─────────────┬────────────┘
           │             │
           ▼             ▼
┌──────────────┐  ┌──────────────────┐
│  FastAPI     │  │  Grafana  :3001  │
│  :8000       │  │  • Time-series   │
│  REST + SSE  │  │  • World-map     │
└──────┬───────┘  │  • Top repos     │
       │          └──────────────────┘
       ▼
┌──────────────────────────────────────┐
│  Next.js Frontend  :3000             │
│  • KPI cards + commit trend chart    │
│  • Top repos bar chart               │
│  • Live event stream (SSE)           │
└──────────────────────────────────────┘

Debug UI: Kafka-UI  :8080
```

```mermaid
erDiagram
    users ||--o{ events : "triggers"
    repos ||--o{ events : "is target of"
    organizations ||--o{ organization_members : "has"
    users ||--o{ organization_members : "belongs to"

    users {
        TEXT username PK "login"
        TEXT company
        TEXT location
        TEXT country
        CHAR country_code "2-letter ISO"
        DOUBLE_PRECISION lat
        DOUBLE_PRECISION lng
        INTEGER public_repos
        TIMESTAMPTZ last_active
    }

    organizations {
        TEXT login PK
        TEXT name
        TEXT location
        BOOLEAN is_verified
        INTEGER public_repos
    }

    organization_members {
        TEXT org_login FK
        TEXT user_username FK
        TEXT role
    }

    repos {
        INTEGER repo_id PK
        TEXT name
        TEXT full_name
        TEXT owner_login
        TEXT description
        TEXT language "Primary language"
        TEXT license_spdx "e.g. MIT, Apache-2.0"
        TEXT[] topics "Array of tags"
        INTEGER stargazers_count
        INTEGER forks_count
        INTEGER open_issues_count
        TIMESTAMPTZ created_at
        TIMESTAMPTZ pushed_at
    }

    events {
        TIMESTAMPTZ time PK
        TEXT event_id PK
        TEXT event_type
        TEXT actor_username FK "References users.username"
        INTEGER repo_id FK "References repos.repo_id"
        TEXT detail "Extracted summary"
        JSONB payload "Raw data fallback"
    }
```

---

## Services overview

| Service | Port | Description |
|---|---|---|
| **frontend** | 3000 | Next.js dashboard with live event stream |
| **api** | 8000 | FastAPI — REST endpoints + SSE stream |
| **grafana** | 3001 | Grafana dashboards (admin / admin) |
| **kafka-ui** | 8080 | Kafka topic browser |
| **timescaledb** | 5432 | TimescaleDB (PostgreSQL 16) |
| **kafka** | 9092 / 9094 | Kafka broker (KRaft) |

---

## Quick start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with the Compose plugin (`docker compose version`)

### 1. Clone and enter the repo

```bash
git clone <repo-url>
cd pm4-github-insights
```

### 2. Configure environment

```bash
cp .env.example .env
```

Open `.env` and optionally add a GitHub token (strongly recommended — increases rate limit from 60 to 5 000 req/h):

```dotenv
GITHUB_TOKEN=ghp_your_token_here   # optional but recommended
POLL_INTERVAL_SECONDS=10            # how often to poll the GitHub API
MAX_PAGES=3                         # pages per poll (30 events/page)
```

### 3. Build and start all services

```bash
docker compose up --build
```

First start takes a few minutes while images are pulled and built.
Kafka topic creation and DB schema initialisation happen automatically.

### 4. Open the UIs

| URL | What you see |
|---|---|
| http://localhost:3000 | Next.js live dashboard |
| http://localhost:8000/docs | FastAPI interactive docs |
| http://localhost:3001 | Grafana (admin / admin) |
| http://localhost:8080 | Kafka-UI |

### 5. Stop everything

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop containers AND delete all data
```

---

## API endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check |
| GET | `/api/kpis` | Key metrics (total events, active repos, …) |
| GET | `/api/commits-over-time?days=30` | Commit trend (daily buckets) |
| GET | `/api/top-repos?limit=10` | Most active repositories |
| GET | `/api/recent-events?limit=20` | Latest events |
| GET | `/stream/events` | Server-Sent Events live stream |

---

## Useful SQL queries

```bash
# Open a psql session inside the container
docker exec -it timescaledb psql -U github -d github_events
```

```sql
-- Most active repos in the last 24 hours
SELECT * FROM v_top_repos_24h LIMIT 10;

-- Geographic distribution of events
SELECT * FROM v_geo_events LIMIT 20;

-- Most active actor today
SELECT actor_username, sum(event_count)
FROM actor_stats_1h
WHERE bucket > now() - INTERVAL '24 hours'
GROUP BY actor_username
ORDER BY sum DESC
LIMIT 10;

-- Raw payload inspection
SELECT time, actor_username, event_type, payload->'commits'->0->>'message' AS commit_msg
FROM events
WHERE event_type = 'PushEvent'
ORDER BY time DESC
LIMIT 20;
```

---

## Why these technology choices?

### Kafka (message bus)

- Decouples the rate-limited GitHub poller from the slow geo-enrichment step
- Allows multiple consumers (e.g. add a Flink job later without touching the producer)
- Acts as a durable replay buffer — if the consumer crashes, it picks up where it left off

### TimescaleDB (storage) — not plain Postgres, not Neo4j

| Option | Verdict |
|---|---|
| Plain PostgreSQL | Missing time-series indexes & compression — gets slow at scale |
| **TimescaleDB** ✅ | Hypertables = automatic chunking by time. 10-20× compression. Continuous aggregates pre-compute rollups. First-class Grafana support |
| InfluxDB | Good for pure metrics but no SQL, weak JOIN support for enrichment queries |
| Graph DB (Neo4j) | Excellent for "actor→repo→org" relationship queries but overkill here; you'd need a second DB for time-series anyway |
| Cassandra | Good for write-heavy scale, but complex ops and no aggregation |

**Verdict**: TimescaleDB gives you all of PostgreSQL (rich SQL, JOINs, JSONB for payloads) plus time-series superpowers.

### Grafana (visualisation)

- Native TimescaleDB/PostgreSQL data source
- World-map panel for geo distribution
- Auto-refresh every 10 s matches the poll interval
- No extra backend needed — Grafana queries the continuous aggregates directly

---

## Scaling up

| Need | Solution |
|---|---|
| More throughput | Add Kafka broker nodes, increase partitions |
| Faster enrichment | Run multiple consumer containers (same group.id = auto-balanced) |
| Stream processing | Add Apache Flink or Spark Structured Streaming consuming from Kafka |
| Long-term archival | Add a Kafka connector to dump to S3/GCS (Parquet) |
| Alerts | Use Grafana alerting rules on the continuous aggregates |
