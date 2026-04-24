# Runbook

Operational reference for the GitHub Events Streaming Stack.

---

## Deployment

### Standard deployment (recommended)

Run `prod-run.sh` from the project root. It handles stopping, volume cleanup, cache-busting, and restart in one step:

```bash
./prod-run.sh
```

What it does:
1. `git pull` — fetch latest code
2. `docker compose --profile multi-consumer down` — stop running stack (data volumes preserved)
3. `docker volume rm pm4-github-insights_grafana-data` — remove Grafana volume so provisioned dashboards reload
4. `docker compose --profile multi-consumer build --no-cache` — rebuild all images without cache
5. `docker compose --profile multi-consumer up -d` — start in detached mode

### Manual start

```bash
# Single consumer instance
docker compose --profile single-consumer up --build

# Three partition-pinned consumer instances (higher throughput, production default)
docker compose --profile multi-consumer up --build
```

First start pulls images and builds the producer/consumer containers. Allow ~2–3 minutes.

### Startup order

Compose health checks enforce this order automatically:

```
kafka (healthy) → kafka-init → timescaledb (healthy) → consumer / consumer-0,1,2
                                                      → geocoder
                                                      → db-writer
                                                      → grafana
                                                      → api (healthy) → frontend
```

The `api` and `frontend` services are part of the standard compose file and start with the rest of the stack. To start them standalone:

```bash
# API only
docker compose up api

# Frontend only (requires API to be reachable)
docker compose up frontend
```

### Stop and clean up

```bash
docker compose down          # stop containers, keep volumes (data survives)
docker compose down -v       # stop containers AND delete all data
```

---

## Health checks

<!-- AUTO-GENERATED — source: docker-compose.yml, api/main.py -->
| Service | Check | Expected response |
|---|---|---|
| API | `curl http://localhost:8000/health` | `{"status":"ok"}` |
| Frontend | `curl -I http://localhost:3000` | `HTTP/1.1 200 OK` |
| Grafana | http://localhost:3001 | Login page |
| Kafka-UI | http://localhost:8080 | Topic browser |
| FastAPI docs | http://localhost:8000/docs | Swagger UI |
| TimescaleDB | `docker exec timescaledb pg_isready -U github` | `accepting connections` |
| Kafka | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` | lists topics |
| Geocoder | `docker logs geocoder` | periodic `Geocoded …` or `Nothing to geocode, sleeping` lines |
| DB Writer | `docker logs db-writer` | consuming from `github.events.status` and `github.ratelimit` |
<!-- /AUTO-GENERATED -->

---

## Common issues

### API container exits immediately on start
**Cause:** TimescaleDB is not yet accepting connections.
**Fix:** `main.py` has a retry loop — the API will reconnect automatically. Wait 30 s, then check logs:
```bash
docker logs github-api
```

### Producer logs rate-limit warnings (`403 Forbidden` or `X-RateLimit-Remaining: 0`)
**Cause:** No GitHub token — unauthenticated limit is 60 req/h.
**Fix:** Add a GitHub personal access token to `.env`:
```dotenv
GITHUB_TOKEN=ghp_your_token_here
```
Then restart the producer: `docker compose restart producer`

### Consumer lag building up in Kafka
**Check:** Open Kafka-UI at http://localhost:8080 → select `github.events.raw` → check consumer group `github-events-enricher` lag.
**Fix options:**
- Check `docker logs github-consumer` for DB connection issues or GitHub API errors.
- Geocoding (`lat`/`lng` enrichment) runs in the dedicated `geocoder` container — it does not affect consumer throughput.
- Switch to 3 partition-pinned instances for 3× parallel throughput:
  ```bash
  docker compose stop consumer
  docker compose --profile multi-consumer up -d
  ```
  Each instance (`consumer-0`, `consumer-1`, `consumer-2`) owns exactly one of the 3 `github.events.raw` partitions.

### Hidden Gems snapshot returns empty data (`repo_count: 0`)
**Cause:** The DB functions (`hidden_gem_repo_scores`, `hidden_gem_user_scores`, `hidden_gem_org_scores`) exist but return no rows because there is no ingested event data yet.
**Fix:** This is normal on a fresh stack. Wait for the producer/consumer pipeline to populate the `events` table. Snapshots are captured automatically at each interval (24h, 168h, 730h by default). To force an immediate capture once data exists:
```bash
curl -X POST "http://localhost:8000/api/hidden-gems/snapshots/trigger?interval_hours=24"
```

### API scheduler logs `UndefinedFunctionError` on startup
**Cause:** Migration 007 (`007_hidden_gem_snapshots.sql`) or the earlier hidden gem functions migration (`004_hidden_gem_functions.sql`) have not been applied.
**Fix:** The migrations are mounted into the `timescaledb` container's init directory and apply automatically on **first start**. On an existing volume they must be applied manually:
```bash
docker cp db/migrations/007_hidden_gem_snapshots.sql $(docker compose ps -q timescaledb):/tmp/007.sql
docker compose exec timescaledb psql -U github -d github_events -f /tmp/007.sql
```

### TimescaleDB compression job fails
**Cause:** Compression is configured to kick in after 7 days. Chunks younger than 7 days cannot be compressed manually.
**Fix:** This is expected behaviour — no action required. Check retention policy:
```sql
SELECT * FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression';
```

### Kafka topic missing (`github.events.raw`, `github.events.status`, or `github.ratelimit`)
**Fix:** Re-run the init container:
```bash
docker compose run --rm kafka-init
```

---

## Database access

```bash
# Open an interactive psql session
docker exec -it timescaledb psql -U github -d github_events
```

Useful queries are in the root `README.md` under **Useful SQL queries**.

---

## Rollback

To reset all state and start fresh:

```bash
docker compose down -v
docker compose up --build
```

This drops all Kafka log data and the entire TimescaleDB database. The schema is re-applied from `db/init.sql` on the next start.

---

## Alerting / monitoring

Grafana alerting rules can be configured against the continuous aggregates:
- `event_stats_5m` — event volume per type (5-minute buckets)
- `country_ids_5m` — geo distribution (5-minute buckets)
- `actor_stats_1h` — per-actor activity (1-hour buckets)

Grafana is available at http://localhost:3001 (credentials: `admin` / `admin`).
