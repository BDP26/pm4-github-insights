import json
import logging
import os
import time

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError, KafkaException

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


def db_connect() -> psycopg2.extensions.connection:
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
                request_headers, response_headers, error,
                request_type, batch_size, token_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
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
            payload.get("request_type", "rest"),
            payload.get("batch_size"),
            payload.get("token_id"),
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
                (source, resource, limit_, used, remaining, reset_at, recorded_at, token_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            payload.get("source"),
            payload.get("resource"),
            payload.get("limit"),
            payload.get("used"),
            payload.get("remaining"),
            payload.get("reset_at"),
            payload.get("recorded_at"),
            payload.get("token_id"),
        ))
        conn.commit()
    except Exception as e:
        log.error("Failed to insert rate_limit_snapshot: %s", e)
        conn.rollback()


def _wait_for_kafka(consumer: Consumer, max_wait_s: int = 60) -> None:
    """Block until the Kafka group coordinator is ready.

    Uses the same exponential-backoff pattern as consumer.py so that
    db-writer does not start processing before the coordinator is elected.
    """
    delay = 2.0
    elapsed = 0.0
    while elapsed < max_wait_s:
        try:
            consumer.committed([], timeout=5)
            log.info("Kafka group coordinator is ready")
            return
        except KafkaException as e:
            log.warning("Kafka not ready (%s), retrying in %.0fs…", e, delay)
        except Exception as e:
            log.warning("Kafka coordinator check error (%s), retrying in %.0fs…", e, delay)
        time.sleep(delay)
        elapsed += delay
        delay = min(delay * 2, 15.0)
    log.warning("Kafka readiness check timed out after %.0fs — proceeding", max_wait_s)


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
    _wait_for_kafka(consumer)

    _coord_backoff = 1.0

    try:
        while True:
            batch = consumer.consume(num_messages=100, timeout=1.0)
            if not batch:
                continue

            coord_error_seen = False
            for msg in batch:
                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError.NOT_COORDINATOR:
                        coord_error_seen = True
                        log.warning(
                            "Group coordinator not ready (transient), backing off %.1fs…",
                            _coord_backoff,
                        )
                    elif err.code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error: %s", err)
                    continue
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    if msg.topic() == TOPIC_STATUS:
                        handle_status_message(cur, conn, payload)
                    elif msg.topic() == TOPIC_RATELIMIT:
                        handle_ratelimit_message(cur, conn, payload)
                    consumer.commit(message=msg, asynchronous=False)
                except psycopg2.OperationalError as e:
                    log.error("Database connection error while processing message: %s", e)
                    # Message offset is not committed — Kafka will re-deliver it
                    # on the next poll (at-least-once delivery guarantee).
                    conn = db_connect()
                    cur = conn.cursor()
                # Let all other exceptions propagate so that we do not commit
                # Kafka offsets for messages that failed to be written to the DB.

            if coord_error_seen:
                time.sleep(_coord_backoff)
                _coord_backoff = min(_coord_backoff * 2, 30.0)
                # Force group re-join: sleeping alone leaves librdkafka in a broken
                # state where consume() returns empty batches indefinitely.
                log.info("Forcing Kafka group re-join after NOT_COORDINATOR…")
                consumer.unsubscribe()
                _wait_for_kafka(consumer)
                consumer.subscribe([TOPIC_STATUS, TOPIC_RATELIMIT])
            else:
                _coord_backoff = 1.0
    except KeyboardInterrupt:
        log.info("DB Writer stopped")
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
