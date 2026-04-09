import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extensions
import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GEOCODER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_REQUESTS = "geocoder.requests"

PHOTON_URL = os.getenv("PHOTON_URL", "http://photon:2322/api")

DB_DSN = (
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'github_events')} "
    f"user={os.getenv('DB_USER', 'github')} "
    f"password={os.getenv('DB_PASSWORD', 'github_secret')}"
)

_kafka_producer: Producer | None = None


def db_connect() -> psycopg2.extensions.connection:
    while True:
        try:
            conn = psycopg2.connect(DB_DSN)
            log.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("DB not ready (%s), retrying in 3s...", e)
            time.sleep(3)


def parse_nominatim_result(data: dict | None) -> dict | None:
    """Parse the first Photon result into {country, country_code, lat, lng}.

    Returns None if data is empty or None.
    """
    if not data:
        return None
    features = data.get("features") or []
    if not features:
        return None

    h = features[0]
    props = h.get("properties", {})
    coords = (h.get("geometry") or {}).get("coordinates") or []
    if len(coords) < 2:
        return None

    return {
        "country": props.get("country"),
        "country_code": (props.get("countrycode") or "").upper()[:2],
        "lat": float(coords[1]),
        "lng": float(coords[0]),
    }


def claim_pending_user(cur: psycopg2.extensions.cursor) -> tuple | None:
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


def claim_pending_org(cur: psycopg2.extensions.cursor) -> tuple | None:
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



def apply_geo_to_user(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection, username: str, geo: dict) -> None:
    cur.execute("""
        UPDATE users
        SET country=%s, country_code=%s, lat=%s, lng=%s
        WHERE username=%s
    """, (geo["country"], geo["country_code"], geo["lat"], geo["lng"], username))
    conn.commit()


def apply_geo_to_org(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection, login: str, geo: dict) -> None:
    cur.execute("""
        UPDATE organizations
        SET lat=%s, lng=%s
        WHERE login=%s
    """, (geo["lat"], geo["lng"], login))
    conn.commit()


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


def _publish_request_log(location: str, meta: dict) -> None:
    """Publish geocoding request metadata to geocoder.requests topic if producer is available."""
    if _kafka_producer is None:
        return
    meta["location_query"] = location
    try:
        _kafka_producer.produce(
            topic=TOPIC_REQUESTS,
            key=f"geocoder-{int(time.time())}",
            value=json.dumps(meta),
        )
        _kafka_producer.poll(0)
    except Exception as e:
        log.warning("Failed to publish geocoder request log: %s", e)


def geocode_location_with_logging(location: str) -> dict | None:
    """Call Photon and log request metadata to Kafka."""
    sent_at = datetime.now(timezone.utc)
    try:
        r = requests.get(
            PHOTON_URL,
            params={"q": location, "limit": 1},
            timeout=5,
        )
        received_at = datetime.now(timezone.utc)
        meta = {
            "request_success": r.ok,
            "sent_at": sent_at.isoformat(),
            "received_at": received_at.isoformat(),
            "elapsed_s": r.elapsed.total_seconds(),
            "method": "GET",
            "url": r.request.url,
            "request_headers": _redact_headers(dict(r.request.headers)),
            "status_code": r.status_code,
            "reason": r.reason,
            "response_bytes": len(r.content),
            "response_headers": _redact_headers(dict(r.headers)),
            "redirects": len(r.history),
            "final_url": r.url,
            "http_version": r.raw.version if hasattr(r.raw, 'version') else None,
            "encoding": r.encoding,
        }
        _publish_request_log(location, meta)
        r.close()

        if r.status_code == 200:
            return parse_nominatim_result(r.json())
        return None
    except requests.RequestException as exc:
        received_at = datetime.now(timezone.utc)
        error_meta = {
            "request_success": False,
            "sent_at": sent_at.isoformat(),
            "received_at": received_at.isoformat(),
            "error": str(exc),
            "method": "GET",
            "url": PHOTON_URL,
            "status_code": None,
            "reason": None,
            "response_bytes": None,
            "response_headers": None,
            "redirects": None,
            "final_url": None,
            "http_version": None,
            "encoding": None,
        }
        _publish_request_log(location, error_meta)
        log.warning("Photon request failed for %r: %s", location, exc)
        return None


def main() -> None:
    global _kafka_producer
    _kafka_producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "1",
        "linger.ms": 100,
    })
    log.info("Kafka producer initialized for geocoder request logging")
    log.info("Starting Geocoder")
    conn = db_connect()
    cur = conn.cursor()

    while True:
        try:
            row = claim_pending_user(cur)
            conn.commit()
            if row:
                username, location = row
                geo = geocode_location_with_logging(location)
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
                geo = geocode_location_with_logging(location)
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
