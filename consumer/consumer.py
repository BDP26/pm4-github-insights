import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from enricher import Enricher, BATCH_FLUSH_INTERVAL_S, parse_token_pool

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = "github.events.raw"
GROUP_ID = "github-events-enricher"

DB_DSN = (
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'github_events')} "
    f"user={os.getenv('DB_USER', 'github')} "
    f"password={os.getenv('DB_PASSWORD', 'github_secret')}"
)

GITHUB_TOKENS_USER = parse_token_pool("GITHUB_TOKENS_USER", "GITHUB_TOKEN_USER")
GITHUB_TOKENS_REPO = parse_token_pool("GITHUB_TOKENS_REPO", "GITHUB_TOKEN_REPO")

_kafka_producer: Producer | None = None


# ── Helpers ──────────────────────────────────────────────────────

def db_connect():
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


def _is_non_bot_user(cur, username: str) -> bool:
    """Return True if the user exists and is not a bot."""
    cur.execute(
        "SELECT username FROM users WHERE username = %s AND is_bot = FALSE",
        (username,),
    )
    return cur.fetchone() is not None


# ── Kafka Startup Helpers ────────────────────────────────────────

def _wait_for_coordinator(consumer: Consumer, tp_list: list, max_wait_s: int = 60) -> None:
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
    enabled = os.getenv("KAFKA_MULTI_INSTANCE_ENABLED", "false").lower() == "true"
    if not enabled:
        return False, None, None
    index_str = os.getenv("KAFKA_INSTANCE_INDEX")
    if index_str is None:
        raise SystemExit(
            "KAFKA_MULTI_INSTANCE_ENABLED=true but KAFKA_INSTANCE_INDEX is not set."
        )
    total_instances = int(os.getenv("KAFKA_TOTAL_INSTANCES", "3"))
    return True, int(index_str), total_instances


def calculate_assigned_partitions(instance_index, total_instances, topic_partitions):
    if total_instances > len(topic_partitions):
        log.warning(
            "KAFKA_TOTAL_INSTANCES (%d) > actual partitions (%d); "
            "some instances will receive no partitions.",
            total_instances, len(topic_partitions),
        )
    return [p for p in topic_partitions if p % total_instances == instance_index]


# ── Main ────────────────────────────────────────────────────────

def main():
    global _kafka_producer
    _kafka_producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "1",
        "linger.ms": 100,
    })
    log.info("Starting GitHub Events Consumer (GraphQL Batch Mode)")
    log.info(
        "Token pools — user: %d token(s), repo: %d token(s)",
        len(GITHUB_TOKENS_USER), len(GITHUB_TOKENS_REPO),
    )

    multi_enabled, instance_index, total_instances = get_multi_instance_config()
    # Per-instance group IDs ensure each instance gets independent offset tracking
    # when manually assigning partitions (librdkafka requires this for assign() mode).
    effective_group_id = f"{GROUP_ID}-p{instance_index}" if multi_enabled else GROUP_ID

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": effective_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    tp_list = []
    if multi_enabled:
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
            raise SystemExit(f"Topic {TOPIC_RAW} has no partitions after retries")
        assigned = calculate_assigned_partitions(instance_index, total_instances, actual_partitions)
        tp_list = [TopicPartition(TOPIC_RAW, p) for p in assigned]
        consumer.assign(tp_list)
        log.info(
            "Multi-instance mode: instance %d/%d, assigned partitions %s",
            instance_index, total_instances, assigned,
        )
        _wait_for_coordinator(consumer, tp_list)
    else:
        consumer.subscribe([TOPIC_RAW])
        _wait_for_coordinator(consumer, [])

    conn = db_connect()
    cur = conn.cursor()
    enricher = Enricher(GITHUB_TOKENS_USER, GITHUB_TOKENS_REPO, conn, cur, _kafka_producer)
    last_flush_ts = time.monotonic()
    _coord_backoff = 1.0

    try:
        while True:
            # Time-based flush trigger
            if time.monotonic() - last_flush_ts >= BATCH_FLUSH_INTERVAL_S:
                enricher.flush(force=True)
                last_flush_ts = time.monotonic()

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
                # NOT_COORDINATOR is transient — librdkafka requires a full unsubscribe/reassign
                # cycle to rediscover the new coordinator after a Kafka broker failover.
                time.sleep(_coord_backoff)
                _coord_backoff = min(_coord_backoff * 2, 30.0)
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
                _coord_backoff = 1.0

            for msg in raw_msgs:
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    actor = event.get("actor", {}).get("login")
                    repo_id = event.get("repo", {}).get("id")
                    repo_name = event.get("repo", {}).get("name")

                    if not actor or not repo_id:
                        continue

                    # Stage 1: Insert event fact
                    ts_str = event.get("created_at", datetime.now(timezone.utc).isoformat())
                    ts = datetime.strptime(ts_str.replace("Z", "+00:00"), "%Y-%m-%dT%H:%M:%S%z")
                    cur.execute("""
                        INSERT INTO events (time, event_id, event_type, actor_username, repo_id, detail, payload)
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (time, event_id) DO NOTHING
                    """, (
                        ts, event["id"], event["type"], actor, repo_id,
                        extract_detail(event),
                        psycopg2.extras.Json(event.get("payload")),
                    ))
                    conn.commit()
                    consumer.commit(message=msg, asynchronous=False)

                    # Stage 2: Queue enrichment
                    enricher.add_user(actor)
                    enricher.add_repo(repo_id, repo_name)

                    # Stage 3: Org membership (best-effort; requires org already enriched)
                    cur.execute(
                        "SELECT owner_login, owner_type FROM repos WHERE repo_id = %s", (repo_id,)
                    )
                    res = cur.fetchone()
                    if res:
                        owner_login, owner_type = res
                        if owner_type == "Organization":
                            enricher.add_user(owner_login)
                            # Flush now so the org row is available for the membership check
                            enricher.flush(force=True)
                            cur.execute(
                                "SELECT 1 FROM organizations WHERE login = %s AND fetched_at IS NOT NULL",
                                (owner_login,),
                            )
                            if cur.fetchone() and _is_non_bot_user(cur, actor):
                                cur.execute("""
                                    INSERT INTO organization_members (org_login, user_username, role)
                                    VALUES (%s, %s, 'contributor') ON CONFLICT DO NOTHING
                                """, (owner_login, actor))
                                conn.commit()

                except Exception as e:
                    conn.rollback()
                    log.error("Processing Error: %s", e)
                    if "connection" in str(e).lower():
                        conn = db_connect()
                        cur = conn.cursor()
                        enricher.set_db(conn, cur)

    except KeyboardInterrupt:
        log.info("Consumer stopped by user")
    finally:
        enricher.flush(force=True)
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
