# Consumer Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract geocoding and DB-write concerns into dedicated containers, add GitHub API rate-limit tracking, and eliminate multi-consumer race conditions using DB-level claim stubs.

**Architecture:** A new `geocoder` container polls TimescaleDB for users/orgs with `location` but no `lat`, claims rows via `FOR UPDATE SKIP LOCKED`, and calls Nominatim. A new `db-writer` container is the sole consumer of `github.events.status` and `github.ratelimit`, inserting rows into `request_logs` and `rate_limit_snapshots`. The enrichment consumer handles only `github.events.raw`, uses an INSERT-stub-first claim pattern to prevent duplicate GitHub API calls, and publishes rate limit snapshots after each API response.

**Tech Stack:** Python 3.12, psycopg2-binary, confluent-kafka, requests, pytest, TimescaleDB/PostgreSQL, Apache Kafka, Docker Compose.

---

## File Map

| File | Status | Responsibility |
|---|---|---|
| `db/migrations/002_geocoder_and_ratelimit.sql` | Create | `geo_claimed_at` columns + `rate_limit_snapshots` table |
| `geocoder/geocoder.py` | Create | DB polling, Nominatim HTTP, claim/update loop |
| `geocoder/Dockerfile` | Create | Python 3.12-slim image for geocoder |
| `geocoder/requirements.txt` | Create | psycopg2-binary, requests |
| `db-writer/db_writer.py` | Create | Consume status + ratelimit topics, insert to DB |
| `db-writer/Dockerfile` | Create | Python 3.12-slim image for db-writer |
| `db-writer/requirements.txt` | Create | psycopg2-binary, confluent-kafka |
| `consumer/consumer.py` | Modify | Remove geocoding/status handling; claim-before-fetch; ratelimit publish |
| `producer/producer.py` | Modify | Add extract_ratelimit + publish_ratelimit |
| `docker-compose.yml` | Modify | Add geocoder + db-writer services; add github.ratelimit to kafka-init |
| `tests/test_enrich_user.py` | Rewrite | Remove rate-limiter/geocode patches; test claim pattern |
| `tests/test_enrich_repo.py` | Create | Test claim-before-fetch for repos |
| `tests/test_ratelimit.py` | Create | Test extract_ratelimit helper (pure function) |
| `tests/test_db_writer.py` | Create | Test db_writer message routing and DB inserts |
| `tests/test_geocoder.py` | Create | Test geocoder claim SQL + Nominatim parsing |

---

## Task 1: DB Migration

**Files:**
- Create: `db/migrations/002_geocoder_and_ratelimit.sql`

- [ ] **Step 1: Write the migration file**

```sql
-- Migration 002: geocoder claim column + rate limit snapshot table

-- Allow geocoder to claim rows atomically (prevents duplicate Nominatim requests)
ALTER TABLE users         ADD COLUMN IF NOT EXISTS geo_claimed_at TIMESTAMPTZ;
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS geo_claimed_at TIMESTAMPTZ;

-- Store GitHub API rate limit snapshots from producer and consumer
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

CREATE INDEX IF NOT EXISTS idx_ratelimit_recorded
    ON rate_limit_snapshots (recorded_at DESC);

CREATE INDEX IF NOT EXISTS idx_ratelimit_source
    ON rate_limit_snapshots (source, recorded_at DESC);
```

- [ ] **Step 2: Commit**

```bash
git add db/migrations/002_geocoder_and_ratelimit.sql
git commit -m "feat: add geo_claimed_at columns and rate_limit_snapshots migration"
```

---

## Task 2: extract_ratelimit Helper + Tests (Consumer)

**Files:**
- Modify: `consumer/consumer.py` — add `extract_ratelimit` function only
- Create: `tests/test_ratelimit.py`

This is a pure function — easiest thing to TDD first.

- [ ] **Step 1: Write the failing test**

Create `tests/test_ratelimit.py`:

```python
"""Unit tests for extract_ratelimit helper in consumer.py."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

from datetime import timezone
import pytest


def test_extract_ratelimit_parses_all_headers():
    from consumer import extract_ratelimit
    headers = {
        "X-RateLimit-Limit": "5000",
        "X-RateLimit-Used": "729",
        "X-RateLimit-Remaining": "4271",
        "X-RateLimit-Reset": "1774601860",
        "X-RateLimit-Resource": "core",
    }
    result = extract_ratelimit(headers, "consumer")
    assert result["limit"] == 5000
    assert result["used"] == 729
    assert result["remaining"] == 4271
    assert result["resource"] == "core"
    assert result["source"] == "consumer"
    assert result["reset_at"] is not None
    assert "recorded_at" in result


def test_extract_ratelimit_returns_none_when_no_header():
    from consumer import extract_ratelimit
    result = extract_ratelimit({}, "consumer")
    assert result is None


def test_extract_ratelimit_handles_missing_reset():
    from consumer import extract_ratelimit
    headers = {"X-RateLimit-Remaining": "100"}
    result = extract_ratelimit(headers, "producer")
    assert result["reset_at"] is None
    assert result["remaining"] == 100


def test_extract_ratelimit_defaults_resource_to_core():
    from consumer import extract_ratelimit
    headers = {"X-RateLimit-Remaining": "50"}
    result = extract_ratelimit(headers, "consumer")
    assert result["resource"] == "core"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /path/to/pm4-github-insights
pytest tests/test_ratelimit.py -v
```

Expected: `ImportError` or `AttributeError: module 'consumer' has no attribute 'extract_ratelimit'`

- [ ] **Step 3: Add `extract_ratelimit` to consumer.py**

Add after the `_redact_headers` function (around line 206 in the current file):

```python
def extract_ratelimit(headers: dict, source: str) -> dict | None:
    """Extract GitHub rate limit fields from response headers.

    Returns None if X-RateLimit-Remaining is absent (non-GitHub response).
    """
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
        "reset_at": (
            datetime.fromtimestamp(int(reset_ts), tz=timezone.utc).isoformat()
            if reset_ts else None
        ),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_ratelimit.py -v
```

Expected: 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add consumer/consumer.py tests/test_ratelimit.py
git commit -m "feat: add extract_ratelimit helper to consumer"
```

---

## Task 3: Producer — Rate Limit Publishing

**Files:**
- Modify: `producer/producer.py`

- [ ] **Step 1: Add `TOPIC_RATELIMIT`, `extract_ratelimit`, and `publish_ratelimit` to producer.py**

Add `TOPIC_RATELIMIT` with the other topic constants (after line 36):

```python
TOPIC_RATELIMIT      = "github.ratelimit"
```

Add `extract_ratelimit` after the `_redact_headers` function (same logic as consumer, duplicated intentionally — these are separate services):

```python
def extract_ratelimit(headers: dict, source: str) -> dict | None:
    """Extract GitHub rate limit fields from response headers.

    Returns None if X-RateLimit-Remaining is absent (non-GitHub response).
    """
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
        "reset_at": (
            datetime.fromtimestamp(int(reset_ts), tz=timezone.utc).isoformat()
            if reset_ts else None
        ),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
```

Add `publish_ratelimit` after the existing `publish_status` function:

```python
def publish_ratelimit(producer: Producer, data: dict) -> None:
    """Publish a rate limit snapshot to github.ratelimit."""
    producer.produce(
        topic=TOPIC_RATELIMIT,
        key=f"ratelimit-{int(time.time())}",
        value=json.dumps(data),
        callback=delivery_report,
    )
```

- [ ] **Step 2: Call `publish_ratelimit` in `fetch_events`**

In `fetch_events`, after the existing `publish_status(producer=producer, meta=meta)` call (around line 179), add:

```python
if resp is not None:
    rl = extract_ratelimit(dict(resp.headers), "producer")
    if rl:
        publish_ratelimit(producer, rl)
```

Place this block right after `publish_status` and before `producer.poll(0)`.

- [ ] **Step 3: Write a focused test for `extract_ratelimit` in producer**

Create `tests/test_producer_ratelimit.py`:

```python
"""Unit tests for extract_ratelimit in producer.py."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producer'))


def test_producer_extract_ratelimit_source_is_producer():
    from producer import extract_ratelimit
    headers = {"X-RateLimit-Remaining": "4000", "X-RateLimit-Limit": "5000"}
    result = extract_ratelimit(headers, "producer")
    assert result["source"] == "producer"
    assert result["limit"] == 5000


def test_producer_extract_ratelimit_returns_none_without_header():
    from producer import extract_ratelimit
    assert extract_ratelimit({}, "producer") is None
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_producer_ratelimit.py -v
```

Expected: 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add producer/producer.py tests/test_producer_ratelimit.py
git commit -m "feat: add rate limit publishing to producer"
```

---

## Task 4: db-writer Container

**Files:**
- Create: `db-writer/db_writer.py`
- Create: `db-writer/Dockerfile`
- Create: `db-writer/requirements.txt`
- Create: `tests/test_db_writer.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_db_writer.py`:

```python
"""Unit tests for db_writer.py message routing and DB inserts."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'db-writer'))

import json
from unittest.mock import MagicMock, patch, call
import pytest


def make_cursor():
    return MagicMock()


def make_conn():
    return MagicMock()


def make_message(topic: str, payload: dict):
    """Return a mock Kafka message."""
    m = MagicMock()
    m.error.return_value = None
    m.topic.return_value = topic
    m.value.return_value = json.dumps(payload).encode()
    return m


# ── github.events.status → request_logs ──────────────────────────

def test_status_message_inserts_into_request_logs():
    from db_writer import handle_status_message
    cur = make_cursor()
    conn = make_conn()
    payload = {
        "request_success": True,
        "sent_at": "2026-03-27T08:26:05+00:00",
        "received_at": "2026-03-27T08:26:05+00:00",
        "elapsed_s": 0.3,
        "method": "GET",
        "url": "https://api.github.com/events",
        "status_code": 200,
        "reason": "OK",
        "response_bytes": 1024,
        "redirects": 0,
        "final_url": "https://api.github.com/events",
        "http_version": 11,
        "encoding": "utf-8",
        "request_headers": {},
        "response_headers": {},
        "error": None,
    }
    handle_status_message(cur, conn, payload)
    sql_calls = [str(c) for c in cur.execute.call_args_list]
    assert any("INSERT INTO request_logs" in s for s in sql_calls)
    conn.commit.assert_called_once()


# ── github.ratelimit → rate_limit_snapshots ───────────────────────

def test_ratelimit_message_inserts_into_rate_limit_snapshots():
    from db_writer import handle_ratelimit_message
    cur = make_cursor()
    conn = make_conn()
    payload = {
        "source": "producer",
        "resource": "core",
        "limit": 5000,
        "used": 729,
        "remaining": 4271,
        "reset_at": "2026-03-27T09:17:40+00:00",
        "recorded_at": "2026-03-27T08:26:05+00:00",
    }
    handle_ratelimit_message(cur, conn, payload)
    sql_calls = [str(c) for c in cur.execute.call_args_list]
    assert any("INSERT INTO rate_limit_snapshots" in s for s in sql_calls)
    conn.commit.assert_called_once()


def test_ratelimit_message_stores_correct_remaining():
    from db_writer import handle_ratelimit_message
    cur = make_cursor()
    conn = make_conn()
    payload = {
        "source": "consumer",
        "resource": "core",
        "limit": 5000,
        "used": 100,
        "remaining": 4900,
        "reset_at": None,
        "recorded_at": "2026-03-27T08:26:05+00:00",
    }
    handle_ratelimit_message(cur, conn, payload)
    # Inspect the params passed to the INSERT
    insert_calls = [c for c in cur.execute.call_args_list
                    if "INSERT INTO rate_limit_snapshots" in str(c)]
    assert insert_calls, "No INSERT INTO rate_limit_snapshots found"
    params = insert_calls[0][0][1]  # positional args[1] = params tuple
    assert 4900 in params, f"remaining=4900 must be in INSERT params, got: {params}"


# ── error handling ────────────────────────────────────────────────

def test_status_message_rollback_on_db_error():
    from db_writer import handle_status_message
    cur = make_cursor()
    conn = make_conn()
    cur.execute.side_effect = Exception("DB error")
    payload = {"request_success": True, "sent_at": None, "received_at": None,
               "elapsed_s": None, "method": "GET", "url": "x", "status_code": 200,
               "reason": "OK", "response_bytes": 0, "redirects": 0,
               "final_url": "x", "http_version": 11, "encoding": "utf-8",
               "request_headers": {}, "response_headers": {}, "error": None}
    handle_status_message(cur, conn, payload)  # must not raise
    conn.rollback.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_db_writer.py -v
```

Expected: `ModuleNotFoundError: No module named 'db_writer'`

- [ ] **Step 3: Create db-writer/requirements.txt**

```
psycopg2-binary
confluent-kafka
```

- [ ] **Step 4: Implement db-writer/db_writer.py**

```python
import json
import logging
import os
import time

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DB-WRITER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_STATUS      = "github.events.status"
TOPIC_RATELIMIT   = "github.ratelimit"
GROUP_ID          = "github-db-writer"

DB_DSN = (
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'github_events')} "
    f"user={os.getenv('DB_USER', 'github')} "
    f"password={os.getenv('DB_PASSWORD', 'github_secret')}"
)


def db_connect():
    while True:
        try:
            conn = psycopg2.connect(DB_DSN)
            log.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("DB not ready (%s), retrying in 3s...", e)
            time.sleep(3)


def handle_status_message(cur, conn, payload: dict) -> None:
    """Insert a request_logs row from a github.events.status message."""
    try:
        cur.execute("""
            INSERT INTO request_logs (
                request_success, sent_at, received_at, elapsed_s,
                method, url, status_code, reason, response_bytes,
                redirects, final_url, http_version, encoding,
                request_headers, response_headers, error
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            payload.get("request_success"),
            payload.get("sent_at"),
            payload.get("received_at"),
            payload.get("elapsed_s"),
            payload.get("method"),
            payload.get("url"),
            payload.get("status_code"),
            payload.get("reason"),
            payload.get("response_bytes"),
            payload.get("redirects"),
            payload.get("final_url"),
            payload.get("http_version"),
            payload.get("encoding"),
            psycopg2.extras.Json(payload.get("request_headers")),
            psycopg2.extras.Json(payload.get("response_headers")),
            payload.get("error"),
        ))
        conn.commit()
    except Exception as e:
        log.error("Failed to insert request_log: %s", e)
        conn.rollback()


def handle_ratelimit_message(cur, conn, payload: dict) -> None:
    """Insert a rate_limit_snapshots row from a github.ratelimit message."""
    try:
        cur.execute("""
            INSERT INTO rate_limit_snapshots
                (source, resource, limit_, used, remaining, reset_at, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            payload.get("source"),
            payload.get("resource"),
            payload.get("limit"),
            payload.get("used"),
            payload.get("remaining"),
            payload.get("reset_at"),
            payload.get("recorded_at"),
        ))
        conn.commit()
    except Exception as e:
        log.error("Failed to insert rate_limit_snapshot: %s", e)
        conn.rollback()


def main():
    log.info("Starting DB Writer")
    conn = db_connect()
    cur = conn.cursor()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_STATUS, TOPIC_RATELIMIT])

    try:
        while True:
            batch = consumer.consume(num_messages=100, timeout=1.0)
            if not batch:
                continue
            for msg in batch:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error: %s", msg.error())
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    if msg.topic() == TOPIC_STATUS:
                        handle_status_message(cur, conn, payload)
                    elif msg.topic() == TOPIC_RATELIMIT:
                        handle_ratelimit_message(cur, conn, payload)
                    consumer.commit(message=msg, asynchronous=False)
                except Exception as e:
                    log.error("Message processing error: %s", e)
                    if "connection" in str(e).lower():
                        conn = db_connect()
                        cur = conn.cursor()
    except KeyboardInterrupt:
        log.info("DB Writer stopped")
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_db_writer.py -v
```

Expected: 4 tests PASS

- [ ] **Step 6: Create db-writer/Dockerfile**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY db_writer.py .
CMD ["python", "db_writer.py"]
```

- [ ] **Step 7: Commit**

```bash
git add db-writer/ tests/test_db_writer.py
git commit -m "feat: add db-writer container for status and ratelimit topics"
```

---

## Task 5: Geocoder Container

**Files:**
- Create: `geocoder/geocoder.py`
- Create: `geocoder/Dockerfile`
- Create: `geocoder/requirements.txt`
- Create: `tests/test_geocoder.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_geocoder.py`:

```python
"""Unit tests for geocoder.py — claim pattern and Nominatim parsing."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'geocoder'))

from unittest.mock import MagicMock, patch
import pytest


def make_cursor(claimed_row=None):
    """claimed_row: what RETURNING returns after UPDATE claim."""
    cur = MagicMock()
    cur.fetchone.return_value = claimed_row
    return cur


def make_nominatim_response(status_code=200, body=None):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = body or []
    return r


# ── parse_nominatim_result ────────────────────────────────────────

def test_parse_nominatim_result_extracts_country_and_coords():
    from geocoder import parse_nominatim_result
    data = [{
        "lat": "37.7749",
        "lon": "-122.4194",
        "address": {
            "country": "United States",
            "country_code": "us",
        }
    }]
    result = parse_nominatim_result(data)
    assert result["country"] == "United States"
    assert result["country_code"] == "US"
    assert result["lat"] == 37.7749
    assert result["lng"] == -122.4194


def test_parse_nominatim_result_returns_none_on_empty():
    from geocoder import parse_nominatim_result
    assert parse_nominatim_result([]) is None


def test_parse_nominatim_result_returns_none_on_none():
    from geocoder import parse_nominatim_result
    assert parse_nominatim_result(None) is None


# ── claim_pending_user ────────────────────────────────────────────

def test_claim_pending_user_returns_row_when_claimed():
    from geocoder import claim_pending_user
    cur = make_cursor(claimed_row=("octocat", "San Francisco, CA"))
    result = claim_pending_user(cur)
    assert result == ("octocat", "San Francisco, CA")
    sql = cur.execute.call_args[0][0]
    assert "geo_claimed_at" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_claim_pending_user_returns_none_when_nothing_pending():
    from geocoder import claim_pending_user
    cur = make_cursor(claimed_row=None)
    result = claim_pending_user(cur)
    assert result is None


# ── claim_pending_org ─────────────────────────────────────────────

def test_claim_pending_org_returns_row_when_claimed():
    from geocoder import claim_pending_org
    cur = make_cursor(claimed_row=("github", "San Francisco, CA"))
    result = claim_pending_org(cur)
    assert result == ("github", "San Francisco, CA")
    sql = cur.execute.call_args[0][0]
    assert "organizations" in sql
    assert "geo_claimed_at" in sql
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_geocoder.py -v
```

Expected: `ModuleNotFoundError: No module named 'geocoder'`

- [ ] **Step 3: Create geocoder/requirements.txt**

```
psycopg2-binary
requests
```

- [ ] **Step 4: Implement geocoder/geocoder.py**

```python
import logging
import os
import time

import psycopg2
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GEOCODER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "ZHAW-Explorer/2.0",
    "Accept-Language": "en",
}

DB_DSN = (
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'github_events')} "
    f"user={os.getenv('DB_USER', 'github')} "
    f"password={os.getenv('DB_PASSWORD', 'github_secret')}"
)


def db_connect():
    while True:
        try:
            conn = psycopg2.connect(DB_DSN)
            log.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("DB not ready (%s), retrying in 3s...", e)
            time.sleep(3)


def parse_nominatim_result(data: list | None) -> dict | None:
    """Parse the first Nominatim result into {country, country_code, lat, lng}.

    Returns None if data is empty or None.
    """
    if not data:
        return None
    h = data[0]
    adr = h.get("address", {})
    return {
        "country": adr.get("country"),
        "country_code": (adr.get("country_code") or "").upper()[:2],
        "lat": float(h["lat"]),
        "lng": float(h["lon"]),
    }


def claim_pending_user(cur):
    """Claim one user pending geocoding. Returns (username, location) or None."""
    cur.execute("""
        UPDATE users
        SET geo_claimed_at = NOW()
        WHERE username = (
            SELECT username FROM users
            WHERE lat IS NULL
              AND location IS NOT NULL
              AND geo_claimed_at IS NULL
              AND is_bot = FALSE
            ORDER BY fetched_at DESC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING username, location
    """)
    return cur.fetchone()


def claim_pending_org(cur):
    """Claim one organization pending geocoding. Returns (login, location) or None."""
    cur.execute("""
        UPDATE organizations
        SET geo_claimed_at = NOW()
        WHERE login = (
            SELECT login FROM organizations
            WHERE lat IS NULL
              AND location IS NOT NULL
              AND geo_claimed_at IS NULL
            ORDER BY fetched_at DESC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING login, location
    """)
    return cur.fetchone()


def geocode_location(location: str) -> dict | None:
    """Call Nominatim to resolve a location string. Sleeps 1s (rate limit)."""
    try:
        r = requests.get(
            NOMINATIM_URL,
            params={"q": location, "format": "json", "limit": 1, "addressdetails": 1},
            headers=NOMINATIM_HEADERS,
            timeout=5,
        )
        time.sleep(1)  # Nominatim policy: max 1 req/s
        if r.status_code == 200:
            return parse_nominatim_result(r.json())
    except Exception as e:
        log.warning("Nominatim request failed for %r: %s", location, e)
    return None


def apply_geo_to_user(cur, conn, username: str, geo: dict) -> None:
    cur.execute("""
        UPDATE users
        SET country=%s, country_code=%s, lat=%s, lng=%s
        WHERE username=%s
    """, (geo["country"], geo["country_code"], geo["lat"], geo["lng"], username))
    conn.commit()


def apply_geo_to_org(cur, conn, login: str, geo: dict) -> None:
    cur.execute("""
        UPDATE organizations
        SET lat=%s, lng=%s
        WHERE login=%s
    """, (geo["lat"], geo["lng"], login))
    conn.commit()


def main():
    log.info("Starting Geocoder")
    conn = db_connect()
    cur = conn.cursor()

    while True:
        try:
            row = claim_pending_user(cur)
            conn.commit()
            if row:
                username, location = row
                geo = geocode_location(location)
                if geo:
                    apply_geo_to_user(cur, conn, username, geo)
                    log.info("Geocoded user %s → %s (%s)", username, location, geo["country_code"])
                else:
                    log.warning("Geocode failed for user %s location %r (claim kept)", username, location)
                continue

            row = claim_pending_org(cur)
            conn.commit()
            if row:
                login, location = row
                geo = geocode_location(location)
                if geo:
                    apply_geo_to_org(cur, conn, login, geo)
                    log.info("Geocoded org %s → %s (%s)", login, location, geo["country_code"])
                else:
                    log.warning("Geocode failed for org %s location %r (claim kept)", login, location)
                continue

            time.sleep(30)

        except Exception as e:
            log.error("Geocoder error: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
            if "connection" in str(e).lower():
                conn = db_connect()
                cur = conn.cursor()
            time.sleep(10)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_geocoder.py -v
```

Expected: 6 tests PASS

- [ ] **Step 6: Create geocoder/Dockerfile**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY geocoder.py .
CMD ["python", "geocoder.py"]
```

- [ ] **Step 7: Commit**

```bash
git add geocoder/ tests/test_geocoder.py
git commit -m "feat: add geocoder container with FOR UPDATE SKIP LOCKED claim pattern"
```

---

## Task 6: Consumer — Remove Dead Code

**Files:**
- Modify: `consumer/consumer.py`
- Modify: `tests/test_enrich_user.py`

Remove all geocoding and status-handling code. The existing tests patch `consumer.geocode` and `consumer.user_limiter` — those patches must be removed.

- [ ] **Step 1: Delete geocoding and rate limiter from consumer.py**

Remove the following from `consumer/consumer.py`:

1. `import threading` (line 5)
2. `NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"` constant
3. The entire `RateLimiter` class (lines 49–64)
4. `user_limiter = RateLimiter(2300)` and `repo_limiter = RateLimiter(2300)` module-level instances
5. The entire `geocode(location)` function
6. The entire `_geocode_pending_users()` function
7. In `main()`: the `geo_thread = threading.Thread(...)` and `geo_thread.start()` lines
8. `TOPIC_STATUS` constant and all status-message handling:
   - In `consumer.subscribe([TOPIC_RAW, TOPIC_STATUS])` → change to `consumer.subscribe([TOPIC_RAW])`
   - In multi-instance mode, remove `[TopicPartition(TOPIC_STATUS, p) for p in assigned]` from `tp_list`
   - Remove the `status_msgs = []` / `raw_msgs = []` split
   - Remove the `for m in status_msgs:` loop
   - Remove the `insert_request_meta` function definition
   - Inside `logged_request`: remove the `insert_request_meta(cur, meta)` call (the function body still builds `meta` and returns it — just delete that one line)
   - `cur` and `conn` parameters on `logged_request` become unused after removing `insert_request_meta`. Leave them in place for now (callers still pass them; removing them is a separate cleanup).
9. `GITHUB_TOKEN_USER` / `GITHUB_HEADERS_USER` / `GITHUB_TOKEN_REPO` / `GITHUB_HEADERS_REPO` — keep these, they're still needed for `enrich_user` and `enrich_repo`

After these deletions the consumer should still import and the non-geocoding tests should still pass. Do NOT yet change `enrich_user` or `enrich_repo` logic.

- [ ] **Step 2: Update test_enrich_user.py — remove geocode/user_limiter patches**

The existing tests patch `consumer.geocode` and `consumer.user_limiter` which no longer exist. Replace the contents of `tests/test_enrich_user.py` with a version that removes those patches and drops the rate-limiter test. The behavior tests (User/Bot/Org/404 inserts) remain but will be updated in Task 7 for the new claim pattern.

Replace the entire file with:

```python
"""Unit tests for enrich_user in consumer.py.

Tests cover:
- Fast path: returns True when user already fully enriched (fetched_at IS NOT NULL)
- Fast path: returns True when org already fully enriched
- Claim stub: returns False when another consumer already inserted a stub
- GitHub type=User: inserts with is_bot=FALSE
- GitHub type=Bot: inserts with is_bot=TRUE, no geocode
- GitHub type=Organization: inserts into organizations
- 404: inserts stub row to prevent future API calls
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

from unittest.mock import MagicMock, patch
import pytest


def make_cursor(fast_path_row=None, claim_row=None):
    """Return a mock cursor.

    call order for fetchone():
      1st → fast-path SELECT (fetched_at IS NOT NULL check)
      2nd → claim INSERT RETURNING
    """
    cur = MagicMock()
    cur.fetchone.side_effect = [fast_path_row, claim_row, None, None]
    return cur


def make_conn():
    return MagicMock()


def make_response(status_code, json_body):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = json_body
    r.ok = status_code < 400
    r.elapsed.total_seconds.return_value = 0.1
    r.request.method = "GET"
    r.request.url = "https://api.github.com/users/test"
    r.request.headers = {}
    r.headers = {}
    r.content = b""
    r.history = []
    r.url = "https://api.github.com/users/test"
    r.raw.version = 11
    r.encoding = "utf-8"
    r.reason = "OK"
    return r


def get_insert_calls(cur, table):
    return [c for c in cur.execute.call_args_list if f"INSERT INTO {table}" in str(c)]


def get_update_calls(cur, table):
    return [c for c in cur.execute.call_args_list if f"UPDATE {table}" in str(c)]


# ── Fast path: already fully enriched ────────────────────────────

def test_fully_enriched_user_returns_true_without_api_call():
    """fetched_at IS NOT NULL in users → return True, no API call."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=("octocat",))
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "octocat")
    assert result is True
    mock_req.assert_not_called()


def test_fully_enriched_org_returns_true_without_api_call():
    """fetched_at IS NOT NULL in organizations → return True, no API call."""
    from consumer import enrich_user
    # fast_path_row=None (not in users), then org fast-path row
    cur = MagicMock()
    cur.fetchone.side_effect = [None, ("github",), None, None]
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "github")
    assert result is True
    mock_req.assert_not_called()


# ── Claim stub: already claimed by another consumer ───────────────

def test_claim_returns_no_row_skips_api_call():
    """INSERT RETURNING no row → another consumer owns it → return False."""
    from consumer import enrich_user
    # fast_path=None (not enriched), claim=None (already claimed)
    cur = make_cursor(fast_path_row=None, claim_row=None)
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "someuser")
    assert result is False
    mock_req.assert_not_called()


# ── GitHub type == "User" ─────────────────────────────────────────

def test_user_response_updates_users_with_is_bot_false():
    """200 type=User → UPDATE users, is_bot=FALSE in params."""
    from consumer import enrich_user
    # fast_path=None, claim=("octocat",) → we own it
    cur = make_cursor(fast_path_row=None, claim_row=("octocat",))
    conn = make_conn()
    api_response = make_response(200, {
        "login": "octocat", "type": "User",
        "company": "GitHub", "location": "San Francisco, CA",
        "public_repos": 10, "followers": 500,
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "octocat")
    assert result is True
    updates = get_update_calls(cur, "users")
    assert updates, "Must UPDATE users after API response"
    all_params = [str(c) for c in cur.execute.call_args_list]
    assert any("FALSE" in s.upper() or "false" in s for s in all_params), \
        "is_bot=False must appear in UPDATE"


def test_user_response_does_not_insert_into_orgs():
    """200 type=User → no INSERT INTO organizations."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("octocat",))
    conn = make_conn()
    api_response = make_response(200, {
        "login": "octocat", "type": "User",
        "company": None, "location": None,
        "public_repos": 5, "followers": 100,
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        enrich_user(cur, conn, "octocat")
    assert len(get_insert_calls(cur, "organizations")) == 0


# ── GitHub type == "Bot" ──────────────────────────────────────────

def test_bot_response_updates_users_with_is_bot_true():
    """200 type=Bot → UPDATE users with is_bot=True in params."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("dependabot[bot]",))
    conn = make_conn()
    api_response = make_response(200, {"login": "dependabot[bot]", "type": "Bot"})
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "dependabot[bot]")
    assert result is True
    updates = get_update_calls(cur, "users")
    assert updates, "Must UPDATE users for bot"
    params_flat = [p for c in updates for p in (c[0][1] if len(c[0]) > 1 else [])]
    assert True in params_flat, "is_bot=True must appear in UPDATE params"


def test_bot_does_not_insert_into_orgs():
    """200 type=Bot → no INSERT INTO organizations."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("bot[bot]",))
    conn = make_conn()
    api_response = make_response(200, {"login": "bot[bot]", "type": "Bot"})
    with patch("consumer.logged_request", return_value=(api_response, {})):
        enrich_user(cur, conn, "bot[bot]")
    assert len(get_insert_calls(cur, "organizations")) == 0


# ── GitHub type == "Organization" ────────────────────────────────

def test_org_response_inserts_into_organizations():
    """200 type=Organization → INSERT INTO organizations."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("github",))
    conn = make_conn()
    api_response = make_response(200, {
        "login": "github", "type": "Organization",
        "name": "GitHub", "description": "Where software is built",
        "location": "San Francisco, CA", "public_repos": 400,
        "created_at": "2008-05-11T04:37:31Z",
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "github")
    assert result is True
    assert len(get_insert_calls(cur, "organizations")) == 1


# ── 404 stub ─────────────────────────────────────────────────────

def test_404_finalises_stub_to_prevent_future_api_calls():
    """404 → UPDATE stub row with fetched_at=NOW() so fast-path catches it next time."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("ghost",))
    conn = make_conn()
    api_response = make_response(404, {"message": "Not Found"})
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "ghost")
    assert result is False
    updates = get_update_calls(cur, "users")
    assert updates, "Must UPDATE stub row on 404 to finalise it"


# ── _is_non_bot_user ──────────────────────────────────────────────

def test_is_non_bot_user_returns_false_for_bot():
    from consumer import _is_non_bot_user
    cur = MagicMock()
    cur.fetchone.return_value = None
    assert _is_non_bot_user(cur, "bot[bot]") is False
    assert "is_bot = FALSE" in cur.execute.call_args[0][0]


def test_is_non_bot_user_returns_true_for_human():
    from consumer import _is_non_bot_user
    cur = MagicMock()
    cur.fetchone.return_value = ("alice",)
    assert _is_non_bot_user(cur, "alice") is True
```

- [ ] **Step 3: Run updated tests to verify they fail (expected — enrich_user still uses old logic)**

```bash
pytest tests/test_enrich_user.py -v
```

Expected: several failures because `enrich_user` still uses the old SELECT-then-request pattern.

- [ ] **Step 4: Commit dead code removal (consumer changes so far)**

```bash
git add consumer/consumer.py tests/test_enrich_user.py
git commit -m "refactor: remove geocoding and status-handling from consumer"
```

---

## Task 7: Consumer — Rewrite enrich_user with Claim Pattern

**Files:**
- Modify: `consumer/consumer.py` — rewrite `enrich_user`

- [ ] **Step 1: Rewrite `enrich_user` in consumer.py**

Replace the entire `enrich_user` function with:

```python
def enrich_user(cur, conn, username):
    """Enrich a GitHub actor using the claim-before-fetch pattern.

    1. Fast path: already fully enriched → return True without API call.
    2. Claim stub via INSERT ... ON CONFLICT DO NOTHING RETURNING.
       If no row returned, another consumer owns this username → return False.
    3. Call GitHub API, then UPDATE the stub row with full data.
    """
    # Fast path: fully enriched user
    cur.execute(
        "SELECT username FROM users WHERE username = %s AND fetched_at IS NOT NULL",
        (username,),
    )
    if cur.fetchone():
        return True

    # Fast path: fully enriched org
    cur.execute(
        "SELECT login FROM organizations WHERE login = %s AND fetched_at IS NOT NULL",
        (username,),
    )
    if cur.fetchone():
        return True

    # Claim the slot — only one consumer wins
    cur.execute(
        "INSERT INTO users (username) VALUES (%s) ON CONFLICT DO NOTHING RETURNING username",
        (username,),
    )
    if not cur.fetchone():
        # Another consumer already inserted a stub or full row
        return False
    conn.commit()  # make stub visible to other consumers immediately

    try:
        r, _ = logged_request(
            cur, conn, "GET",
            f"https://api.github.com/users/{username}",
            headers=GITHUB_HEADERS_USER,
            timeout=5,
        )
        if r is None:
            return False

        if r.status_code == 404:
            log.info("Stored 404 stub for unresolvable actor: %s", username)
            cur.execute(
                "UPDATE users SET fetched_at = NOW() WHERE username = %s",
                (username,),
            )
            conn.commit()
            return False

        if r.status_code == 200:
            d = r.json()
            actor_type = d.get("type")

            if actor_type == "User":
                cur.execute("""
                    UPDATE users
                    SET fetched_at    = NOW(),
                        company       = %s,
                        location      = %s,
                        public_repos  = %s,
                        followers     = %s,
                        is_bot        = FALSE
                    WHERE username = %s
                """, (d.get("company"), d.get("location"),
                      d.get("public_repos"), d.get("followers"),
                      d["login"]))
                conn.commit()
                return True

            elif actor_type == "Bot":
                log.info("Stored bot actor: %s", d["login"])
                cur.execute(
                    "UPDATE users SET fetched_at = NOW(), is_bot = TRUE WHERE username = %s",
                    (d["login"],),
                )
                conn.commit()
                return True

            else:
                # Organization: insert into organizations, remove stub from users
                cur.execute("""
                    INSERT INTO organizations
                        (login, fetched_at, name, description, location,
                         public_repos, created_at)
                    VALUES (%s, NOW(), %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (d["login"], d.get("name"), d.get("description"),
                      d.get("location"), d.get("public_repos"), d.get("created_at")))
                # Remove the users stub for this org actor
                cur.execute(
                    "DELETE FROM users WHERE username = %s AND fetched_at IS NULL",
                    (username,),
                )
                conn.commit()
                return True

    except Exception as e:
        log.error("User API Error: %s", e)
    return False
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
pytest tests/test_enrich_user.py -v
```

Expected: all tests PASS

- [ ] **Step 3: Run all tests to check for regressions**

```bash
pytest tests/ -v --ignore=tests/integration
```

Expected: all non-integration tests PASS

- [ ] **Step 4: Commit**

```bash
git add consumer/consumer.py
git commit -m "feat: rewrite enrich_user with claim-before-fetch pattern"
```

---

## Task 8: Consumer — Rewrite enrich_repo with Claim Pattern

**Files:**
- Modify: `consumer/consumer.py` — rewrite `enrich_repo`, update caller signature
- Create: `tests/test_enrich_repo.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_enrich_repo.py`:

```python
"""Unit tests for enrich_repo in consumer.py."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

from unittest.mock import MagicMock, patch
import pytest


def make_cursor(fast_path_row=None, claim_row=None):
    cur = MagicMock()
    cur.fetchone.side_effect = [fast_path_row, claim_row, None, None]
    return cur


def make_conn():
    return MagicMock()


def make_response(status_code, json_body):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = json_body
    r.ok = status_code < 400
    r.elapsed.total_seconds.return_value = 0.1
    r.request.method = "GET"
    r.request.url = "https://api.github.com/repos/octocat/hello"
    r.request.headers = {}
    r.headers = {}
    r.content = b""
    r.history = []
    r.url = "https://api.github.com/repos/octocat/hello"
    r.raw.version = 11
    r.encoding = "utf-8"
    r.reason = "OK"
    return r


def get_update_calls(cur):
    return [c for c in cur.execute.call_args_list if "UPDATE repos" in str(c)]


# ── Fast path ────────────────────────────────────────────────────

def test_fully_enriched_repo_returns_true_without_api_call():
    """fetched_at IS NOT NULL in repos → return True, no API call."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=(42,))
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is True
    mock_req.assert_not_called()


# ── Claim: already owned ──────────────────────────────────────────

def test_claim_returns_no_row_skips_api_call():
    """INSERT RETURNING no row → another consumer owns it → False."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=None, claim_row=None)
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is False
    mock_req.assert_not_called()


# ── Successful enrichment ─────────────────────────────────────────

def test_successful_repo_response_updates_repos():
    """200 response → UPDATE repos with full data."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=None, claim_row=(42,))
    conn = make_conn()
    api_response = make_response(200, {
        "id": 42,
        "name": "hello",
        "full_name": "octocat/hello",
        "owner": {"login": "octocat", "type": "User"},
        "description": "My first repo",
        "language": "Python",
        "license": {"spdx_id": "MIT"},
        "topics": ["python", "demo"],
        "stargazers_count": 100,
        "forks_count": 20,
        "size": 500,
        "created_at": "2020-01-01T00:00:00Z",
        "pushed_at": "2026-01-01T00:00:00Z",
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is True
    updates = get_update_calls(cur)
    assert updates, "Must UPDATE repos after 200 response"


def test_repo_api_failure_returns_false():
    """logged_request returns None (network error) → return False."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=None, claim_row=(42,))
    conn = make_conn()
    with patch("consumer.logged_request", return_value=(None, {})):
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_enrich_repo.py -v
```

Expected: failures — `enrich_repo` still uses old signature `(cur, conn, full_name)` and old logic.

- [ ] **Step 3: Rewrite `enrich_repo` in consumer.py**

Replace the entire `enrich_repo` function with:

```python
def enrich_repo(cur, conn, repo_id: int, full_name: str):
    """Enrich a repository using the claim-before-fetch pattern.

    Signature change: now takes repo_id (int) as well as full_name.
    The claim stub uses repo_id as the conflict key (PRIMARY KEY).
    """
    # Fast path
    cur.execute(
        "SELECT repo_id FROM repos WHERE repo_id = %s AND fetched_at IS NOT NULL",
        (repo_id,),
    )
    if cur.fetchone():
        return True

    # Claim stub (repo_id is the PK; name/full_name/owner_login satisfy NOT NULL)
    cur.execute(
        """INSERT INTO repos (repo_id, name, full_name, owner_login, owner_type)
           VALUES (%s, '', %s, '', '')
           ON CONFLICT DO NOTHING
           RETURNING repo_id""",
        (repo_id, full_name),
    )
    if not cur.fetchone():
        return False
    conn.commit()

    try:
        r, _ = logged_request(
            cur, conn, "GET",
            f"https://api.github.com/repos/{full_name}",
            headers=GITHUB_HEADERS_REPO,
            timeout=5,
        )
        if r is None:
            return False
        if r.status_code == 200:
            d = r.json()
            cur.execute("""
                UPDATE repos
                SET fetched_at        = NOW(),
                    name              = %s,
                    full_name         = %s,
                    owner_login       = %s,
                    owner_type        = %s,
                    description       = %s,
                    language          = %s,
                    license_spdx      = %s,
                    topics            = %s,
                    stargazers_count  = %s,
                    forks_count       = %s,
                    size              = %s,
                    created_at        = %s,
                    pushed_at         = %s
                WHERE repo_id = %s
            """, (
                d["name"], d["full_name"],
                d["owner"]["login"], d["owner"]["type"],
                d.get("description"),
                d.get("language"),
                (d.get("license") or {}).get("spdx_id"),
                d.get("topics", []),
                d.get("stargazers_count"), d.get("forks_count"),
                d.get("size"), d.get("created_at"), d.get("pushed_at"),
                d["id"],
            ))
            conn.commit()
            return True
    except Exception as e:
        log.error("Repo API Error: %s", e)
    return False
```

- [ ] **Step 4: Update the caller in `main()` to pass `repo_id`**

In the main consumer loop, find the call to `enrich_repo(cur, conn, repo_name)` and change it to:

```python
enrich_repo(cur, conn, repo_id, repo_name)
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_enrich_repo.py tests/test_enrich_user.py -v
```

Expected: all tests PASS

- [ ] **Step 6: Run full test suite**

```bash
pytest tests/ -v --ignore=tests/integration
```

Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add consumer/consumer.py tests/test_enrich_repo.py
git commit -m "feat: rewrite enrich_repo with claim-before-fetch pattern"
```

---

## Task 9: Consumer — Rate Limit Publishing

**Files:**
- Modify: `consumer/consumer.py` — add Kafka producer, `_publish_ratelimit`, call after API responses

- [ ] **Step 1: Add Kafka producer and TOPIC_RATELIMIT to consumer.py**

Add at the top of consumer.py with the other imports:

```python
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
```

Add with the other topic constants:

```python
TOPIC_RATELIMIT = "github.ratelimit"
```

Add a module-level producer variable after the topic constants:

```python
_kafka_producer: Producer | None = None
```

- [ ] **Step 2: Add `_publish_ratelimit` function**

Add after `extract_ratelimit`:

```python
def _publish_ratelimit(headers: dict) -> None:
    """Publish a rate limit snapshot to github.ratelimit if producer is available."""
    if _kafka_producer is None:
        return
    rl = extract_ratelimit(dict(headers), "consumer")
    if not rl:
        return
    try:
        _kafka_producer.produce(
            topic=TOPIC_RATELIMIT,
            key=f"ratelimit-{int(time.time())}",
            value=json.dumps(rl),
        )
        _kafka_producer.poll(0)
    except Exception as e:
        log.warning("Failed to publish rate limit snapshot: %s", e)
```

- [ ] **Step 3: Call `_publish_ratelimit` inside `logged_request`**

In `logged_request`, after the successful response (after `insert_request_meta(cur, meta)` — but wait, `insert_request_meta` was removed in Task 6). After the `meta` dict is built and before `return r, meta`, add:

```python
_publish_ratelimit(r.headers)
```

The final `return` line in the success branch of `logged_request` should look like:

```python
        _publish_ratelimit(r.headers)
        return r, meta
```

- [ ] **Step 4: Initialize `_kafka_producer` in `main()`**

At the start of `main()`, after the existing log statements, add:

```python
global _kafka_producer
_kafka_producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "1",
    "linger.ms": 100,
})
log.info("Kafka producer initialized for rate limit publishing")
```

- [ ] **Step 5: Write a test for `_publish_ratelimit`**

Add to `tests/test_ratelimit.py`:

```python
def test_publish_ratelimit_calls_produce_with_ratelimit_headers():
    import consumer
    from unittest.mock import MagicMock, patch
    mock_producer = MagicMock()
    original = consumer._kafka_producer
    try:
        consumer._kafka_producer = mock_producer
        headers = {
            "X-RateLimit-Remaining": "4000",
            "X-RateLimit-Limit": "5000",
            "X-RateLimit-Resource": "core",
        }
        consumer._publish_ratelimit(headers)
        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs[1]["topic"] == "github.ratelimit" or \
               (call_kwargs[0] and call_kwargs[0][0] == "github.ratelimit")
    finally:
        consumer._kafka_producer = original


def test_publish_ratelimit_does_nothing_when_no_producer():
    import consumer
    original = consumer._kafka_producer
    try:
        consumer._kafka_producer = None
        # Should not raise
        consumer._publish_ratelimit({"X-RateLimit-Remaining": "100"})
    finally:
        consumer._kafka_producer = original
```

- [ ] **Step 6: Run tests**

```bash
pytest tests/test_ratelimit.py -v
```

Expected: all 6 tests PASS

- [ ] **Step 7: Commit**

```bash
git add consumer/consumer.py tests/test_ratelimit.py
git commit -m "feat: add rate limit publishing to consumer"
```

---

## Task 10: docker-compose.yml — Wire Everything Together

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add `github.ratelimit` to kafka-init**

In `docker-compose.yml`, find the `kafka-init` `command` block and add:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic github.ratelimit --partitions 3 --replication-factor 1
```

The full kafka-init command block becomes:

```yaml
    command: |
      "
      echo '--- Creating Kafka topics ---'
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic github.events.raw --partitions 3 --replication-factor 1
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic github.events.status --partitions 3 --replication-factor 1
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic github.ratelimit --partitions 3 --replication-factor 1

      echo '--- Topics ready ---'
      "
```

- [ ] **Step 2: Add geocoder service**

Add after the `consumer-2` service definition:

```yaml
  # ── Geocoder ───────────────────────────────────────────────────
  geocoder:
    build: ./geocoder
    pull_policy: build
    container_name: geocoder
    networks: [github-stream]
    depends_on:
      timescaledb:
        condition: service_healthy
    environment:
      DB_HOST:     timescaledb
      DB_PORT:     "5432"
      DB_NAME:     github_events
      DB_USER:     github
      DB_PASSWORD: github_secret
    restart: unless-stopped
```

- [ ] **Step 3: Add db-writer service**

Add after the geocoder service:

```yaml
  # ── DB Writer (status + ratelimit topics) ─────────────────────
  db-writer:
    build: ./db-writer
    pull_policy: build
    container_name: db-writer
    networks: [github-stream]
    depends_on:
      kafka:
        condition: service_healthy
      timescaledb:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      DB_HOST:                 timescaledb
      DB_PORT:                 "5432"
      DB_NAME:                 github_events
      DB_USER:                 github
      DB_PASSWORD:             github_secret
    restart: unless-stopped
```

- [ ] **Step 4: Update consumer environment — add KAFKA_BOOTSTRAP_SERVERS for producer init**

The consumer already has `KAFKA_BOOTSTRAP_SERVERS: kafka:9092` in all three consumer service definitions. No change needed — the `_kafka_producer` in the consumer uses `BOOTSTRAP_SERVERS` which reads from that env var.

- [ ] **Step 5: Run final test suite**

```bash
pytest tests/ -v --ignore=tests/integration
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add geocoder and db-writer services, add github.ratelimit topic"
```

---

## Task 11: Final Smoke Test

- [ ] **Step 1: Build all containers**

```bash
docker compose build
```

Expected: all images build without error

- [ ] **Step 2: Start the stack (single-consumer mode)**

```bash
docker compose --profile single-consumer up -d
```

- [ ] **Step 3: Verify all services healthy**

```bash
docker compose ps
```

Expected: `kafka`, `timescaledb`, `kafka-ui`, `producer`, `consumer`, `geocoder`, `db-writer`, `grafana` all show `running` or `healthy`.

- [ ] **Step 4: Apply the DB migration**

```bash
docker exec -i timescaledb psql -U github -d github_events \
  < db/migrations/002_geocoder_and_ratelimit.sql
```

Expected: `ALTER TABLE` × 2, `CREATE TABLE`, `CREATE INDEX` × 2

- [ ] **Step 5: Verify rate_limit_snapshots receives data**

```bash
docker exec timescaledb psql -U github -d github_events \
  -c "SELECT source, remaining, recorded_at FROM rate_limit_snapshots ORDER BY id DESC LIMIT 5;"
```

Expected: rows appearing from both `producer` and `consumer` sources within 30s

- [ ] **Step 6: Verify db-writer is consuming status messages**

```bash
docker logs db-writer --tail 20
```

Expected: log lines showing `INSERT INTO request_logs` or no errors

- [ ] **Step 7: Verify geocoder is running**

```bash
docker logs geocoder --tail 20
```

Expected: `Connected to TimescaleDB` and either geocoding log lines or sleep messages

- [ ] **Step 8: Commit final state**

```bash
git add .
git commit -m "chore: verify smoke test passes for consumer refactor"
```

---

## Summary of Kafka Topics After Refactor

| Topic | Partitions | Publisher(s) | Consumer |
|---|---|---|---|
| `github.events.raw` | 3 | producer | consumer (enrichment) |
| `github.events.status` | 3 | producer | db-writer |
| `github.ratelimit` | 3 | producer + consumer | db-writer |
