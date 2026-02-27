# GitHub Events Streaming Stack

Real-time pipeline that ingests the GitHub public events API into Kafka,
enriches each event with geographic + profile data, stores everything in
TimescaleDB, and visualises it live in Grafana.

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
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Grafana  :3000                     │
│  • Events/min time-series           │
│  • World-map country heatmap        │
│  • Top repos table                  │
│  • Recent events log panel          │
└─────────────────────────────────────┘

Debug UI: Kafka-UI  :8080
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

**Verdict**: TimescaleDB gives you all of PostgreSQL (rich SQL, JOINs, JSONB for payloads) plus time-series superpowers. If you later want graph queries (e.g. "which developers contributed to the same repos?"), you can add a Neo4j container and feed it from the same Kafka topic.

### Grafana (visualisation)
- Native TimescaleDB/PostgreSQL data source
- `grafana-worldmap-panel` for the geo distribution you already have in the CLI script
- Auto-refresh every 10 s matches the poll interval
- No extra backend needed — Grafana queries the continuous aggregates directly

---

## Quick start

```bash
# 1. Clone / enter the project directory
cd github-kafka

# 2. Create your .env with optional GitHub token
cp .env.example .env
# edit .env and add your GITHUB_TOKEN

# 3. Build and start all services
docker compose up --build

# 4. Open Grafana
open http://localhost:3000          # admin / admin

# 5. Inspect Kafka topics / messages
open http://localhost:8080          # Kafka-UI

# 6. Query TimescaleDB directly
docker exec -it timescaledb psql -U github -d github_events
```

### Useful SQL queries

```sql
-- Live event rate (events per minute, last 30 min)
SELECT * FROM v_event_rate_1m;

-- Top countries in the last hour
SELECT country, events FROM v_top_countries_24h LIMIT 10;

-- Most active actor today
SELECT actor, sum(event_count)
FROM actor_stats_1h
WHERE bucket > now() - INTERVAL '24 hours'
GROUP BY actor
ORDER BY sum DESC
LIMIT 10;

-- Raw payload inspection
SELECT time, actor, event_type, payload->'commits'->0->>'message' AS commit_msg
FROM events
WHERE event_type = 'PushEvent'
ORDER BY time DESC
LIMIT 20;
```

---

## Scaling up

| Need | Solution |
|---|---|
| More throughput | Add Kafka broker nodes, increase partitions |
| Faster enrichment | Run multiple consumer containers (same group.id = auto-balanced) |
| Stream processing | Add Apache Flink or Spark Structured Streaming consuming from Kafka |
| Long-term archival | Add a Kafka connector to dump to S3/GCS (Parquet) |
| Alerts | Use Grafana alerting rules on the continuous aggregates |
