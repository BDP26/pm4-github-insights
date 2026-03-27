# Consumer Refactor Design
**Date:** 2026-03-27
**Status:** Approved

## Summary

Four focused improvements to the GitHub Events pipeline:

1. Extract geocoding into a dedicated container (remove from consumer)
2. Track GitHub API rate limit headers in a new Kafka topic and DB table
3. Fix consumer race conditions in multi-instance mode using DB-level claim stubs
4. Extract a dedicated `db-writer` container for `github.events.status` and `github.ratelimit` — pure DB inserts, scales independently of enrichment

---

## Architecture Overview

```
Producer ──► github.events.raw    ──► Consumer (1 or 3 instances)
         ├──► github.events.status ──► DB-Writer ──► request_logs
         └──► github.ratelimit    ──►           └──► rate_limit_snapshots
                                       Consumer ──► users / repos / orgs
                                                        │
                                             (lat IS NULL + geo_claimed_at IS NULL)
                                                        ▼
                                               Geocoder container
                                               (Nominatim, 1 req/s)
```

**New containers:** `geocoder`, `db-writer`
**New Kafka topics:** `github.ratelimit` (3 partitions)
**New DB objects:** `rate_limit_snapshots` table; `geo_claimed_at` column on `users` and `organizations`
**Removed from consumer:** geocoding thread, `geocode()`, `NOMINATIM_URL`, `threading` import, `RateLimiter` class, `user_limiter`, `repo_limiter`, `TOPIC_STATUS` handling, `insert_request_meta()`, priority-ordering logic (`status_msgs` / `raw_msgs` split)

---

## 1. Database Changes

Delivered as a new migration file `db/migrations/002_geocoder_and_ratelimit.sql`.

### 1a. `geo_claimed_at` column

```sql
ALTER TABLE users         ADD COLUMN IF NOT EXISTS geo_claimed_at TIMESTAMPTZ;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS geo_claimed_at TIMESTAMPTZ;
```

Semantics:
- `geo_claimed_at IS NULL` + `location IS NOT NULL` + `lat IS NULL` → pending geocoding
- `geo_claimed_at IS NOT NULL` + `lat IS NULL` → claimed (in-progress or permanently failed)
- `lat IS NOT NULL` → geocoded successfully

Failed geocodes keep `geo_claimed_at IS NOT NULL` and are not retried (location strings that fail Nominatim are unlikely to succeed on retry).

### 1b. `rate_limit_snapshots` table

```sql
CREATE TABLE IF NOT EXISTS rate_limit_snapshots (
    id           SERIAL PRIMARY KEY,
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source       TEXT NOT NULL,    -- 'producer' or 'consumer'
    resource     TEXT,             -- 'core', 'search', etc.
    limit_       INTEGER,
    used         INTEGER,
    remaining    INTEGER,
    reset_at     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_ratelimit_recorded ON rate_limit_snapshots (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_ratelimit_source   ON rate_limit_snapshots (source, recorded_at DESC);
```

`reset_at` is derived from `X-RateLimit-Reset` (Unix timestamp → `TIMESTAMPTZ`).

---

## 2. New Geocoder Container

### Files

```
geocoder/
├── Dockerfile
└── geocoder.py
```

### geocoder.py logic

```
Loop:
  Claim one pending user (FOR UPDATE SKIP LOCKED):
    UPDATE users SET geo_claimed_at = NOW()
    WHERE username = (
      SELECT username FROM users
      WHERE lat IS NULL AND location IS NOT NULL
        AND geo_claimed_at IS NULL AND is_bot = FALSE
      ORDER BY fetched_at DESC LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    RETURNING username, location

  If claimed:
    → GET Nominatim /search?q={location}
    → sleep(1)  -- Nominatim policy: 1 req/s
    → UPDATE users SET lat=, lng=, country=, country_code= WHERE username=
  Else:
    → Try organizations table with same pattern

  If nothing to do: sleep(30)
```

### Dockerfile

Base image: `python:3.12-slim`
Dependencies: `psycopg2-binary`, `requests`

### API keys

The geocoder calls **Nominatim** (OpenStreetMap), which is free and requires **no API key**. It only needs a valid `User-Agent` header (e.g. `"ZHAW-Explorer/2.0"`) and must respect the 1 req/s rate limit. The geocoder never calls the GitHub API.

### docker-compose.yml additions

```yaml
geocoder:
  build: ./geocoder
  container_name: geocoder
  networks: [github-stream]
  depends_on:
    timescaledb:
      condition: service_healthy
  environment:
    DB_HOST: timescaledb
    DB_PORT: "5432"
    DB_NAME: github_events
    DB_USER: github
    DB_PASSWORD: github_secret
  restart: unless-stopped
```

No Kafka dependency, no API tokens — the geocoder only needs DB access.

---

## 3. New DB-Writer Container

### Purpose

Handles all fast DB-insert topics (`github.events.status`, `github.ratelimit`) independently of the enrichment consumer. This removes the priority-ordering workaround from the consumer and allows the two concerns to scale separately.

### Files

```
db-writer/
├── Dockerfile
└── db_writer.py
```

### db_writer.py logic

```
Subscribe to: github.events.status, github.ratelimit

Loop (consume batch):
  For each message on github.events.status:
    → INSERT INTO request_logs (...) — same as current insert_request_meta()
    → commit offset

  For each message on github.ratelimit:
    → INSERT INTO rate_limit_snapshots (source, resource, limit_, used, remaining, reset_at, recorded_at)
    → commit offset
```

No GitHub API calls. No enrichment. Stateless — can run as many replicas as needed.

### Dockerfile

Base image: `python:3.12-slim`
Dependencies: `psycopg2-binary`, `confluent-kafka`

### docker-compose.yml additions

```yaml
db-writer:
  build: ./db-writer
  container_name: db-writer
  networks: [github-stream]
  depends_on:
    kafka:
      condition: service_healthy
    timescaledb:
      condition: service_healthy
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    DB_HOST: timescaledb
    DB_PORT: "5432"
    DB_NAME: github_events
    DB_USER: github
    DB_PASSWORD: github_secret
  restart: unless-stopped
```

---

## 4. Consumer Changes (enrichment only)

### 4a. Remove geocoding

Delete from `consumer.py`:
- `_geocode_pending_users()` function
- `geocode()` function
- `NOMINATIM_URL` constant
- `geo_thread` startup in `main()`
- `import threading`
- `RateLimiter` class, `user_limiter`, `repo_limiter` instances

### 4b. Race condition fix — claim-before-fetch pattern

**For `enrich_user()`:**

```
1. Fast path:
   SELECT username FROM users WHERE username=%s AND fetched_at IS NOT NULL
   → if found: return True (fully enriched)

2. Claim stub:
   INSERT INTO users (username) VALUES (%s) ON CONFLICT DO NOTHING RETURNING username
   → if no RETURNING row: stub or complete record exists → return False (skip API call)

3. API call → parse response
   - 404: UPDATE users SET fetched_at=NOW() WHERE username=%s (finalize stub)
   - 200 User: UPDATE users SET fetched_at=NOW(), company=, location=, ... WHERE username=%s
   - 200 Bot:  UPDATE users SET fetched_at=NOW(), is_bot=TRUE WHERE username=%s
   - 200 Org:  INSERT INTO organizations (...) ON CONFLICT DO NOTHING
               DELETE FROM users WHERE username=%s AND fetched_at IS NULL
               (org actors should live in organizations, not users)
```

**For `enrich_repo()`:**

```
1. Fast path:
   SELECT repo_id FROM repos WHERE repo_id=%s AND fetched_at IS NOT NULL
   → if found: return True

2. Claim stub:
   INSERT INTO repos (repo_id, name, full_name, owner_login, owner_type)
   VALUES (%s, '', %s, '', '') ON CONFLICT DO NOTHING RETURNING repo_id
   → if no RETURNING row: skip

3. API call → 200:
   UPDATE repos SET fetched_at=NOW(), name=, full_name=, ... WHERE repo_id=%s
```

### 4c. Rate limit publishing

Extract a helper `extract_ratelimit(headers, source)` used in `logged_request()`:

```python
def extract_ratelimit(headers: dict, source: str) -> dict | None:
    remaining = headers.get("X-RateLimit-Remaining")
    if remaining is None:
        return None
    reset_ts = headers.get("X-RateLimit-Reset")
    return {
        "source": source,
        "resource": headers.get("X-RateLimit-Resource", "core"),
        "limit": int(headers.get("X-RateLimit-Limit", 0)),
        "used": int(headers.get("X-RateLimit-Used", 0)),
        "remaining": int(remaining),
        "reset_at": datetime.fromtimestamp(int(reset_ts), tz=timezone.utc).isoformat() if reset_ts else None,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
```

After every successful GitHub API response in `logged_request()`, publish `extract_ratelimit(r.headers, "consumer")` to `github.ratelimit`.

The consumer needs its own `confluent_kafka.Producer` instance (initialized once in `main()` and passed into `enrich_user`/`enrich_repo`/`logged_request`). This is a new addition — the consumer currently has no producer.

The consumer's Kafka loop subscribes to `github.ratelimit` in addition to `github.events.raw` and `github.events.status`. In multi-instance mode, `TOPIC_RATELIMIT` partitions are added to the `tp_list` alongside the existing topics. Rate limit messages are processed in the fast-insert path (same as `github.events.status`).

New `TOPIC_RATELIMIT = "github.ratelimit"` constant.

---

## 5. Producer Changes

### 5a. Rate limit publishing

Add `TOPIC_RATELIMIT = "github.ratelimit"` constant.

In `fetch_events()`, after every successful response:
```python
rl = extract_ratelimit(resp.headers, "producer")
if rl:
    publish_ratelimit(producer, rl)
```

`extract_ratelimit()` is the same logic as in the consumer (can be a shared utility or duplicated given they are separate services).

`publish_ratelimit()` mirrors `publish_status()`:
```python
def publish_ratelimit(producer: Producer, data: dict) -> None:
    producer.produce(
        topic=TOPIC_RATELIMIT,
        key=f"ratelimit-{int(time.time())}",
        value=json.dumps(data),
        callback=delivery_report,
    )
```

### 5b. kafka-init topic creation

Add to `kafka-init` command:
```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic github.ratelimit \
  --partitions 3 --replication-factor 1
```

---

## 6. Error Handling

- **Geocoder DB disconnect:** reconnect loop (same pattern as consumer)
- **Geocoder Nominatim failure:** log warning, `geo_claimed_at` stays set (won't retry), processing continues
- **Consumer stub orphan:** if a consumer dies after inserting a stub but before the API call, the stub stays with `fetched_at IS NULL`. On next consumer restart, the claim INSERT returns no row → the stub is treated as "someone else owns it" and the record is never fully enriched. Mitigation: a periodic cleanup job (`UPDATE users SET geo_claimed_at=NULL WHERE fetched_at IS NULL AND created_at < NOW() - INTERVAL '1 hour'`) is out of scope for this iteration.
- **Rate limit topic failure:** non-blocking; if publish to `github.ratelimit` fails, log warning and continue. Rate limit tracking is observational, not critical path.

---

## 7. Kafka Topics Summary

| Topic | Partitions | Producer | Consumer |
|---|---|---|---|
| `github.events.raw` | 3 | producer | consumer |
| `github.events.status` | 3 | producer | consumer |
| `github.ratelimit` | 3 | producer + consumer | consumer |

---

## 8. Files Affected

| File | Change |
|---|---|
| `consumer/consumer.py` | Remove geocoding, TOPIC_STATUS handling, priority-ordering; fix enrich_user/repo; add ratelimit publish; add confluent Producer |
| `producer/producer.py` | Add extract_ratelimit + publish_ratelimit |
| `geocoder/geocoder.py` | New file |
| `geocoder/Dockerfile` | New file |
| `db-writer/db_writer.py` | New file |
| `db-writer/Dockerfile` | New file |
| `db/migrations/002_geocoder_and_ratelimit.sql` | geo_claimed_at columns + rate_limit_snapshots table |
| `docker-compose.yml` | Add geocoder + db-writer services; add github.ratelimit to kafka-init |
