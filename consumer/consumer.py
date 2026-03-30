import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = "github.events.raw"
TOPIC_STATUS = "github.events.status"
TOPIC_RATELIMIT = "github.ratelimit"
GROUP_ID = "github-events-enricher"

DB_DSN = (
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'github_events')} "
    f"user={os.getenv('DB_USER', 'github')} "
    f"password={os.getenv('DB_PASSWORD', 'github_secret')}"
)

GITHUB_TOKEN_USER = os.getenv("GITHUB_TOKEN_USER", "")
GITHUB_TOKEN_REPO = os.getenv("GITHUB_TOKEN_REPO", "")

GITHUB_HEADERS_USER = {"Accept": "application/vnd.github+json", "User-Agent": "ZHAW-Explorer/2.0"}
if GITHUB_TOKEN_USER:
    GITHUB_HEADERS_USER["Authorization"] = f"Bearer {GITHUB_TOKEN_USER}"

GITHUB_HEADERS_REPO = {"Accept": "application/vnd.github+json", "User-Agent": "ZHAW-Explorer/2.0"}
if GITHUB_TOKEN_REPO:
    GITHUB_HEADERS_REPO["Authorization"] = f"Bearer {GITHUB_TOKEN_REPO}"

_kafka_producer: Producer | None = None
_ratelimit_reset_user: float = 0  # UTC timestamp when user token rate limit resets
_ratelimit_reset_repo: float = 0  # UTC timestamp when repo token rate limit resets

# ── Helpers ─────────────────────────────────────────────────────

def db_connect():
    """Verbindet zur DB mit Retry-Logik, falls die DB noch startet."""
    while True:
        try:
            conn = psycopg2.connect(DB_DSN)
            log.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("DB not ready (%s), retrying in 3s...", e)
            time.sleep(3)


def extract_detail(event: dict) -> str:
    etype = event.get("type", "")
    p = event.get("payload", {})
    if etype == "PushEvent": return f"{len(p.get('commits', []))} commits"
    if etype == "WatchEvent": return "starred"
    if etype == "CreateEvent": return f"created {p.get('ref_type')}"
    if etype == "ForkEvent": return f"forked to {p.get('forkee', {}).get('full_name')}"
    return ""


# ── Enrichment ──────────────────────────────────────────────────

def _delete_user_stub(cur, conn, username: str) -> None:
    """Delete an unclaimed stub row so other consumers can retry enrichment."""
    try:
        cur.execute(
            "DELETE FROM users WHERE username = %s AND fetched_at IS NULL",
            (username,),
        )
        conn.commit()
    except Exception as cleanup_err:
        log.warning("Could not clean up stub for %s: %s", username, cleanup_err)


def _delete_repo_stub(cur, conn, repo_id: int) -> None:
    """Delete an unclaimed repo stub so other consumers can retry enrichment."""
    try:
        cur.execute(
            "DELETE FROM repos WHERE repo_id = %s AND fetched_at IS NULL",
            (repo_id,),
        )
        conn.commit()
    except Exception as cleanup_err:
        log.warning("Could not clean up repo stub for %s: %s", repo_id, cleanup_err)


def enrich_user(cur, conn, username):
    """Enrich a GitHub actor using the claim-before-fetch pattern.

    1. Fast path: already fully enriched → return True without API call.
    2. Claim stub via INSERT ... ON CONFLICT DO NOTHING RETURNING.
       If no row returned, another consumer owns this username → return False.
    3. Call GitHub API, then UPDATE the stub row with full data.
    """
    # Skip if rate-limited
    if _ratelimit_reset_user > datetime.now(timezone.utc).timestamp():
        return False
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
            # Network error — delete stub so another consumer can retry
            _delete_user_stub(cur, conn, username)
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
                    "UPDATE users SET fetched_at = NOW(), is_bot = %s WHERE username = %s",
                    (True, d["login"]),
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

        else:
            log.warning("User API %s returned %s for %s", r.status_code, r.reason, username)
            _delete_user_stub(cur, conn, username)
            return False

    except Exception as e:
        log.error("User API Error for %s: %s", username, e)
        _delete_user_stub(cur, conn, username)
    return False


def enrich_repo(cur, conn, repo_id: int, full_name: str):
    """Enrich a repository using the claim-before-fetch pattern.

    Signature change: now takes repo_id (int) as well as full_name.
    The claim stub uses repo_id as the conflict key (PRIMARY KEY).
    """
    # Skip if rate-limited
    if _ratelimit_reset_repo > datetime.now(timezone.utc).timestamp():
        return False
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
            # Network error — delete stub so another consumer can retry
            _delete_repo_stub(cur, conn, repo_id)
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
                    watchers_count    = %s,
                    has_issues        = %s,
                    open_issues_count = %s,
                    has_projects      = %s,
                    has_downloads     = %s,
                    archived          = %s,
                    disabled          = %s,
                    homepage          = %s,
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
                d.get("stargazers_count"), d.get("forks_count"),d.get("watchers_count"),
                d.get("has_issues"), d.get("open_issues_count"),
                d.get("has_projects"), d.get("has_downloads"),d.get("archived"), d.get("disabled"),
                d.get("homepage"),
                d.get("size"), d.get("created_at"), d.get("pushed_at"),
                d["id"],
            ))
            conn.commit()
            return True
        else:
            log.warning("Repo API %s returned %s for %s", r.status_code, r.reason, full_name)
            _delete_repo_stub(cur, conn, repo_id)
            return False
    except Exception as e:
        log.error("Repo API Error for %s: %s", full_name, e)
        _delete_repo_stub(cur, conn, repo_id)
    return False

def _redact_headers(headers):
    """Return a copy of headers with sensitive values redacted."""
    if not isinstance(headers, dict):
        return headers

    sensitive_header_names = {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-api-key",
        "x-api-token",
        "x-auth-token",
        "x-access-token",
    }

    redacted = {}
    for name, value in headers.items():
        if isinstance(name, str) and name.lower() in sensitive_header_names:
            redacted[name] = "[REDACTED]"
        else:
            redacted[name] = value
    return redacted


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


def _publish_status(meta: dict) -> None:
    """Publish HTTP request metadata to github.events.status if producer is available."""
    if _kafka_producer is None:
        return
    try:
        _kafka_producer.produce(
            topic=TOPIC_STATUS,
            key=f"status-{int(time.time())}",
            value=json.dumps(meta),
        )
        _kafka_producer.poll(0)
    except Exception as e:
        log.warning("Failed to publish request status: %s", e)


def logged_request(cur, conn, method, url, **kwargs):
    """Perform an HTTP request and log metadata directly to request_logs."""
    sent_at = datetime.now(timezone.utc)
    try:
        r = requests.request(method, url, **kwargs)
        received_at = datetime.now(timezone.utc)
        meta = {
            "request_success": r.ok,
            "sent_at":         sent_at.isoformat(),
            "received_at":     received_at.isoformat(),
            "elapsed_s":       r.elapsed.total_seconds(),
            "method":          r.request.method,
            "url":             r.request.url,
            "request_headers": _redact_headers(dict(r.request.headers)),
            "status_code":     r.status_code,
            "reason":          r.reason,
            "response_bytes":  len(r.content),
            "response_headers": _redact_headers(dict(r.headers)),
            "redirects":       len(r.history),
            "final_url":       r.url,
            "http_version":    r.raw.version,
            "encoding":        r.encoding,
        }
        _publish_ratelimit(r.headers)
        _publish_status(meta)

        # Track rate-limit reset timestamps (no blocking sleep)
        if r.status_code in (403, 429):
            reset_ts = r.headers.get("X-RateLimit-Reset")
            if reset_ts:
                global _ratelimit_reset_user, _ratelimit_reset_repo
                reset_at = int(reset_ts)
                if "/users/" in url:
                    _ratelimit_reset_user = reset_at
                else:
                    _ratelimit_reset_repo = reset_at
                log.warning("Rate-limited (%s). Skipping enrichment until reset at %s.",
                            r.status_code, datetime.fromtimestamp(reset_at, tz=timezone.utc).isoformat())

        return r, meta
    except requests.RequestException as exc:
        error_meta = {
            "request_success": False,
            "sent_at":         sent_at.isoformat(),
            "received_at":     datetime.now(timezone.utc).isoformat(),
            "error":           str(exc),
            "method":          method,
            "url":             url,
            "status_code":     None,
            "reason":          None,
            "response_bytes":  None,
            "response_headers": None,
            "redirects":       None,
            "final_url":       None,
            "http_version":    None,
            "encoding":        None,
        }
        log.warning("GitHub API request failed: %s", exc)
        return None, error_meta


# ── Kafka Startup Helpers ────────────────────────────────────────

def _wait_for_coordinator(consumer: Consumer, tp_list: list, max_wait_s: int = 60) -> None:
    """Block until the Kafka group coordinator is ready.

    Fetching committed offsets is the lightest operation that requires an
    active coordinator.  Retries with exponential backoff so consumers do
    not start processing before the coordinator is elected.
    """
    delay = 2.0
    elapsed = 0.0
    while elapsed < max_wait_s:
        try:
            consumer.committed(tp_list, timeout=5)
            log.info("Group coordinator is ready")
            return
        except KafkaException as e:
            log.warning("Group coordinator not ready (%s), retrying in %.0fs…", e, delay)
        except Exception as e:
            log.warning("Coordinator check error (%s), retrying in %.0fs…", e, delay)
        time.sleep(delay)
        elapsed += delay
        delay = min(delay * 2, 15.0)
    log.warning("Coordinator readiness check timed out after %.0fs — proceeding", max_wait_s)


# ── Multi-Instance Partition Config ─────────────────────────────

def get_multi_instance_config():
    """Parse multi-instance env vars.

    Returns:
        (enabled: bool, instance_index: int|None, total_instances: int|None)

    Raises:
        SystemExit: if enabled but KAFKA_INSTANCE_INDEX is missing.
    """
    enabled = os.getenv("KAFKA_MULTI_INSTANCE_ENABLED", "false").lower() == "true"
    if not enabled:
        return False, None, None

    index_str = os.getenv("KAFKA_INSTANCE_INDEX")
    if index_str is None:
        raise SystemExit(
            "KAFKA_MULTI_INSTANCE_ENABLED=true but KAFKA_INSTANCE_INDEX is not set. "
            "Set KAFKA_INSTANCE_INDEX to 0, 1, or 2 (or up to KAFKA_TOTAL_INSTANCES-1)."
        )

    total_instances = int(os.getenv("KAFKA_TOTAL_INSTANCES", "3"))
    return True, int(index_str), total_instances


def calculate_assigned_partitions(
    instance_index: int,
    total_instances: int,
    topic_partitions: list,
) -> list:
    """Deterministically assign partitions to this instance.

    Formula: assigned = [p for p in topic_partitions if p % total_instances == instance_index]

    Args:
        instance_index:   0-based index of this consumer instance.
        total_instances:  total number of consumer instances.
        topic_partitions: list of actual partition IDs for the topic.
    """
    if total_instances > len(topic_partitions):
        log.warning(
            "KAFKA_TOTAL_INSTANCES (%d) > actual partitions (%d); "
            "some instances will receive no partitions.",
            total_instances, len(topic_partitions),
        )
    return [p for p in topic_partitions if p % total_instances == instance_index]


# ── Main ────────────────────────────────────────────────────────

def _is_non_bot_user(cur, username: str) -> bool:
    """Return True iff username exists in users and is not a bot."""
    cur.execute(
        "SELECT username FROM users WHERE username = %s AND is_bot = FALSE",
        (username,),
    )
    return cur.fetchone() is not None


def main():
    global _kafka_producer
    _kafka_producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "1",
        "linger.ms": 100,
    })
    log.info("Kafka producer initialized for rate limit publishing")
    log.info("Starting GitHub Events Consumer (Star Schema Mode)")

    # 1. Kafka Consumer Initialisierung
    multi_enabled, instance_index, total_instances = get_multi_instance_config()

    # Each instance needs a unique group ID when using assign().
    # Sharing a group ID across assign()-based consumers bypasses Kafka's rebalance
    # protocol while still registering with the group coordinator, which causes
    # NOT_COORDINATOR errors. Per-instance group IDs give each consumer its own
    # independent offset tracking for its pinned partition.
    effective_group_id = f"{GROUP_ID}-p{instance_index}" if multi_enabled else GROUP_ID

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": effective_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    tp_list = []  # populated in multi-instance mode; kept in scope for reconnect loop
    if multi_enabled:
        # Query actual partition count from broker metadata, then pin to our slice.
        # Retry until the topic exists — kafka-init may not have run yet.
        from confluent_kafka import TopicPartition
        actual_partitions = []
        for attempt in range(10):
            metadata = consumer.list_topics(TOPIC_RAW, timeout=10)
            actual_partitions = sorted(metadata.topics[TOPIC_RAW].partitions.keys())
            if actual_partitions:
                break
            log.warning("Topic %s has no partitions yet, retrying in 3s (%d/10)…", TOPIC_RAW, attempt + 1)
            time.sleep(3)
        if not actual_partitions:
            raise SystemExit(f"Topic {TOPIC_RAW} has no partitions after retries — is kafka-init running?")
        assigned = calculate_assigned_partitions(instance_index, total_instances, actual_partitions)
        tp_list = [TopicPartition(TOPIC_RAW, p) for p in assigned]
        consumer.assign(tp_list)
        log.info(
            "Multi-instance mode: instance %d/%d, assigned partitions %s on topic %s",
            instance_index, total_instances, assigned, TOPIC_RAW,
        )
        _wait_for_coordinator(consumer, tp_list)
    else:
        consumer.subscribe([TOPIC_RAW])
        _wait_for_coordinator(consumer, [])

    # 2. Datenbank-Verbindung (mit Retry-Logik)
    conn = db_connect()
    cur = conn.cursor()

    _coord_backoff = 1.0  # exponential backoff for NOT_COORDINATOR errors

    try:
        while True:
            # Batch-poll: drain all available messages
            batch = consumer.consume(num_messages=100, timeout=1.0)
            if not batch:
                continue

            raw_msgs = []
            coord_error_seen = False
            for m in batch:
                if m.error():
                    err = m.error()
                    if err.code() == KafkaError.NOT_COORDINATOR:
                        coord_error_seen = True
                        log.warning(
                            "Group coordinator not ready (transient), backing off %.1fs…",
                            _coord_backoff,
                        )
                    else:
                        log.error("Kafka error: %s", err)
                    continue
                raw_msgs.append(m)

            if coord_error_seen:
                time.sleep(_coord_backoff)
                _coord_backoff = min(_coord_backoff * 2, 30.0)
                # Sleeping alone is not enough: librdkafka's internal group-membership
                # state is broken after NOT_COORDINATOR.  Subsequent consume() calls
                # return empty batches indefinitely without logging anything.
                # Force a full group re-join by tearing down and rebuilding the
                # partition assignment / subscription.
                log.info("Forcing Kafka group re-join after NOT_COORDINATOR…")
                if multi_enabled:
                    consumer.unassign()
                    _wait_for_coordinator(consumer, tp_list)
                    consumer.assign(tp_list)
                else:
                    consumer.unsubscribe()
                    _wait_for_coordinator(consumer, [])
                    consumer.subscribe([TOPIC_RAW])
            elif raw_msgs:
                _coord_backoff = 1.0  # reset on successful messages

            # Process RAW events
            for msg in raw_msgs:
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    actor = event.get("actor", {}).get("login")
                    repo_id = event.get("repo", {}).get("id")
                    repo_name = event.get("repo", {}).get("name")

                    if not actor or not repo_id:
                        continue

                    # --- STUFE 1: Event Fact-Table befüllen (immer zuerst) ---
                    ts_str = event.get("created_at", datetime.now(timezone.utc).isoformat())
                    ts = datetime.strptime(ts_str.replace("Z", "+00:00"), "%Y-%m-%dT%H:%M:%S%z")

                    cur.execute("""
                        INSERT INTO events (time, event_id, event_type, actor_username, repo_id, detail, payload)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (time, event_id) DO NOTHING
                    """, (
                        ts,
                        event['id'],
                        event['type'],
                        actor,
                        repo_id,
                        extract_detail(event),
                        psycopg2.extras.Json(event.get('payload'))
                    ))

                    conn.commit()
                    consumer.commit(message=msg, asynchronous=False)

                    # --- STUFE 2: Stammdaten-Anreicherung (Enrichment) ---
                    enrich_user(cur, conn, actor)
                    enrich_repo(cur, conn, repo_id, repo_name)

                    # --- STUFE 3: Relationen-Learning (Membership) ---
                    cur.execute("SELECT owner_login, owner_type FROM repos WHERE repo_id = %s", (repo_id,))
                    res = cur.fetchone()

                    if res:
                        owner_login, owner_type = res

                        if owner_type == 'Organization':
                            enrich_user(cur, conn, owner_login)
                            if _is_non_bot_user(cur, actor):
                                cur.execute("""
                                    INSERT INTO organization_members (org_login, user_username, role)
                                    VALUES (%s, %s, 'contributor') ON CONFLICT DO NOTHING
                                """, (owner_login, actor))
                                conn.commit()

                except Exception as e:
                    conn.rollback()
                    log.error(f"Processing Error: {e}")
                    if "connection" in str(e).lower():
                        conn = db_connect()
                        cur = conn.cursor()

    except KeyboardInterrupt:
        log.info("Consumer stopped by user")
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
