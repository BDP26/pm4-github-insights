# Runbook

Operational reference for the GitHub Events Streaming Stack.

---

## Deployment

### Start all services

```bash
docker compose up --build
```

First start pulls images and builds the producer/consumer containers. Allow ~2–3 minutes.

### Startup order

Compose health checks enforce this order automatically:

```
kafka (healthy) → kafka-init → timescaledb (healthy) → consumer
                                                      → grafana
```

The FastAPI (`api/`) and Next.js (`frontend/`) services have their own Dockerfiles and can be started separately:

```bash
# API
docker build -t github-api ./api
docker run --name github-api -p 8000:8000 --env-file .env github-api

# Frontend (set NEXT_PUBLIC_API_URL at build time)
docker build --build-arg NEXT_PUBLIC_API_URL=http://localhost:8000 -t github-frontend ./frontend
docker run -p 3000:3000 github-frontend
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
- Check `docker logs github-consumer` for geocoding errors or DB connection issues.
- Nominatim geocoding enforces 1 req/s — lag is normal if the event rate spikes.
- Scale consumers: `docker compose up --scale consumer=2` (all share the same `group.id` → Kafka auto-balances).

### TimescaleDB compression job fails
**Cause:** Compression is configured to kick in after 7 days. Chunks younger than 7 days cannot be compressed manually.
**Fix:** This is expected behaviour — no action required. Check retention policy:
```sql
SELECT * FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression';
```

### `github.events.raw` topic missing
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
