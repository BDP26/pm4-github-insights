"""
GitHub Events Consumer
──────────────────────
Reads raw events from  github.events.raw,
enriches each actor with geographic + profile data,
and writes rows to TimescaleDB.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS
  DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from confluent_kafka import Consumer, KafkaError

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW         = "github.events.raw"
GROUP_ID          = "github-events-enricher"

DB_DSN = (
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'github_events')} "
    f"user={os.getenv('DB_USER', 'github')} "
    f"password={os.getenv('DB_PASSWORD', 'github_secret')}"
)

GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN", "")
NOMINATIM_URL  = "https://nominatim.openstreetmap.org/search"
GITHUB_API_BASE = "https://api.github.com/users"

GITHUB_HEADERS = {
    "Accept":     "application/vnd.github+json",
    "User-Agent": "ZHAW-BigData-Explorer/1.0",
}
if GITHUB_TOKEN:
    GITHUB_HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

NOMINATIM_HEADERS = {
    "User-Agent":      "ZHAW-BigData-Explorer/1.0 (educational project)",
    "Accept-Language": "en",
}

# ── In-memory caches (survive within one container lifetime) ─────
user_cache:    dict[str, dict] = {}
geocode_cache: dict[str, dict] = {}


# ── Enrichment helpers ────────────────────────────────────────────

def geocode(location: str) -> dict:
    if not location:
        return {}
    key = location.lower().strip()
    if key in geocode_cache:
        return geocode_cache[key]

    result: dict = {}
    try:
        r = requests.get(
            NOMINATIM_URL,
            headers=NOMINATIM_HEADERS,
            params={"q": location, "format": "json", "limit": 1, "addressdetails": 1},
            timeout=8,
        )
        if r.status_code == 200:
            hits = r.json()
            if hits:
                h   = hits[0]
                adr = h.get("address", {})
                result = {
                    "country":      adr.get("country"),
                    "country_code": (adr.get("country_code") or "").upper()[:2] or None,
                    "lat":          float(h.get("lat", 0)) or None,
                    "lng":          float(h.get("lon", 0)) or None,
                }
    except requests.RequestException:
        pass

    geocode_cache[key] = result
    time.sleep(1.1)   # Nominatim policy: ≤ 1 req/s
    return result


def fetch_profile(username: str) -> dict:
    if username in user_cache:
        return user_cache[username]

    profile: dict = {
        "location": None, "company": None, "public_repos": None,
        "country": None, "country_code": None,
        "lat": None, "lng": None,
    }

    try:
        r = requests.get(
            f"{GITHUB_API_BASE}/{username}",
            headers=GITHUB_HEADERS,
            timeout=8,
        )
        if r.status_code == 200:
            d = r.json()
            profile["location"]     = d.get("location") or None
            profile["company"]      = (d.get("company") or "").strip("@ ").strip() or None
            profile["public_repos"] = d.get("public_repos")
    except requests.RequestException:
        pass

    if profile["location"]:
        geo = geocode(profile["location"])
        profile.update({k: geo.get(k) for k in ("country", "country_code", "lat", "lng")})

    user_cache[username] = profile
    return profile


# ── Detail extractor (mirrors the producer-side logic) ───────────

def extract_detail(event: dict) -> str:
    etype   = event.get("type", "")
    payload = event.get("payload", {})
    if etype == "PushEvent":
        return f"{len(payload.get('commits', []))} commit(s)"
    if etype in ("PullRequestEvent", "IssuesEvent"):
        return payload.get("action", "")
    if etype == "CreateEvent":
        return f"{payload.get('ref_type', '')} '{payload.get('ref', '')}'"
    if etype == "WatchEvent":
        return payload.get("action", "starred")
    if etype == "ReleaseEvent":
        return f"tag {payload.get('release', {}).get('tag_name', '')}"
    return ""


# ── DB writer ─────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO events (
    time, event_id, event_type, actor, repo, detail,
    location, country, country_code, lat, lng,
    company, public_repos, payload
) VALUES (
    %(time)s, %(event_id)s, %(event_type)s, %(actor)s, %(repo)s, %(detail)s,
    %(location)s, %(country)s, %(country_code)s, %(lat)s, %(lng)s,
    %(company)s, %(public_repos)s, %(payload)s
)
ON CONFLICT DO NOTHING;
"""

def db_connect():
    while True:
        try:
            conn = psycopg2.connect(DB_DSN)
            log.info("Connected to TimescaleDB")
            return conn
        except psycopg2.OperationalError as e:
            log.warning("DB not ready (%s), retrying in 3s…", e)
            time.sleep(3)


def write_event(cur, event: dict, profile: dict):
    ts_raw = event.get("created_at", "")
    try:
        ts = datetime.strptime(ts_raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        ts = datetime.now(timezone.utc)

    cur.execute(INSERT_SQL, {
        "time":         ts,
        "event_id":     event.get("id"),
        "event_type":   event.get("type"),
        "actor":        event.get("actor", {}).get("login"),
        "repo":         event.get("repo",  {}).get("name"),
        "detail":       extract_detail(event),
        "location":     profile.get("location"),
        "country":      profile.get("country"),
        "country_code": profile.get("country_code"),
        "lat":          profile.get("lat"),
        "lng":          profile.get("lng"),
        "company":      profile.get("company"),
        "public_repos": profile.get("public_repos"),
        "payload":      psycopg2.extras.Json(event.get("payload", {})),
    })


# ── Main consumer loop ────────────────────────────────────────────

def main():
    log.info("Starting GitHub Events Consumer")
    log.info("  Bootstrap servers : %s", BOOTSTRAP_SERVERS)
    log.info("  Topic             : %s", TOPIC_RAW)
    log.info("  Consumer group    : %s", GROUP_ID)
    log.info("  TimescaleDB       : %s", DB_DSN.split("password=")[0])

    consumer = Consumer({
        "bootstrap.servers":        BOOTSTRAP_SERVERS,
        "group.id":                 GROUP_ID,
        "auto.offset.reset":        "earliest",
        "enable.auto.commit":       False,       # manual commit after DB write
        "max.poll.interval.ms":     300_000,
        "session.timeout.ms":       30_000,
    })
    consumer.subscribe([TOPIC_RAW])

    conn = db_connect()
    cur  = conn.cursor()

    processed = 0
    errors    = 0

    try:
        while True:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                errors += 1
                continue

            try:
                event   = json.loads(msg.value().decode("utf-8"))
                actor   = event.get("actor", {}).get("login", "")
                profile = fetch_profile(actor)

                write_event(cur, event, profile)
                conn.commit()
                consumer.commit(asynchronous=False)

                processed += 1
                if processed % 50 == 0:
                    log.info(
                        "Processed %d events | users cached: %d | geocodes: %d | errors: %d",
                        processed, len(user_cache), len(geocode_cache), errors,
                    )

            except (json.JSONDecodeError, KeyError) as exc:
                log.warning("Skipping malformed message: %s", exc)
                conn.rollback()
                errors += 1
            except psycopg2.Error as exc:
                log.error("DB write failed: %s", exc)
                conn.rollback()
                # Re-connect if connection was lost
                try:
                    conn.close()
                except Exception:
                    pass
                conn = db_connect()
                cur  = conn.cursor()
                errors += 1

    except KeyboardInterrupt:
        log.info("Shutting down…")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
