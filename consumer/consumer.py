import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

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
TOPIC_RAW = "github.events.raw"
TOPIC_STATUS = "github.events.status"
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

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


# ── Quota Tracking ──────────────────────────────────────────────
class RateLimiter:
    def __init__(self, max_per_hour):
        self.max_per_hour = max_per_hour
        self.calls = []

    def can_call(self):
        now = datetime.now()
        self.calls = [t for t in self.calls if t > now - timedelta(hours=1)]
        return len(self.calls) < self.max_per_hour

    def record_call(self):
        self.calls.append(datetime.now())


user_limiter = RateLimiter(2300)
repo_limiter = RateLimiter(2300)


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


def geocode(location: str) -> dict:
    if not location: return {}
    try:
        r = requests.get(NOMINATIM_URL, params={"q": location, "format": "json", "limit": 1, "addressdetails": 1},
                         headers={"User-Agent": "ZHAW-Explorer/2.0",
                                  "Accept-Language": "en"},
                         timeout=5)
        if r.status_code == 200 and r.json():
            h = r.json()[0]
            adr = h.get("address", {})
            time.sleep(1)  # Nominatim policy: 1 req/s
            return {
                "country": adr.get("country"),
                "country_code": (adr.get("country_code") or "").upper()[:2],
                "lat": float(h.get("lat")), "lng": float(h.get("lon"))
            }
    except:
        pass
    return {}


def extract_detail(event: dict) -> str:
    etype = event.get("type", "")
    p = event.get("payload", {})
    if etype == "PushEvent": return f"{len(p.get('commits', []))} commits"
    if etype == "WatchEvent": return "starred"
    if etype == "CreateEvent": return f"created {p.get('ref_type')}"
    if etype == "ForkEvent": return f"forked to {p.get('forkee', {}).get('full_name')}"
    return ""


# ── Enrichment ──────────────────────────────────────────────────

def enrich_user(cur, conn, username):
    if username.endswith("[bot]"):
        return False  # skip bots
    cur.execute("SELECT username FROM users WHERE username = %s", (username,))
    if cur.fetchone(): return True
    cur.execute("SELECT login FROM organizations WHERE login = %s", (username,))
    if cur.fetchone(): return True

    if not user_limiter.can_call(): return False

    try:
        r, meta = logged_request(cur, conn, "GET", f"https://api.github.com/users/{username}", headers=GITHUB_HEADERS_USER, timeout=5)
        user_limiter.record_call()
        if r is None: return False
        if r.status_code == 200:
            d = r.json()
            if d.get("type") == "User":
                geo = geocode(d.get("location"))
                cur.execute("""
                            INSERT INTO users (username, fetched_at, company, location, country, country_code, lat, lng,
                                               public_repos, followers)
                            VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                            """, (d['login'], d.get('company'), d.get('location'), geo.get('country'),
                                  geo.get('country_code'),
                                  geo.get('lat'), geo.get('lng'), d.get('public_repos'), d.get('followers')))
            else:
                cur.execute("""
                            INSERT INTO organizations (login, fetched_at, name, description, location, public_repos,
                                                       created_at)
                            VALUES (%s, NOW(), %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                            """,
                            (d['login'], d.get('name'), d.get('description'), d.get('location'), d.get('public_repos'),
                             d.get('created_at')))
            return True
    except Exception as e:
        log.error(f"User API Error: {e}")
    return False


def enrich_repo(cur, conn, full_name):
    cur.execute("SELECT repo_id FROM repos WHERE full_name = %s", (full_name,))
    if cur.fetchone(): return True
    if not repo_limiter.can_call(): return False

    try:
        r, meta = logged_request(cur, conn, "GET", f"https://api.github.com/repos/{full_name}", headers=GITHUB_HEADERS_REPO, timeout=5)
        repo_limiter.record_call()
        if r is None: return False
        if r.status_code == 200:
            d = r.json()
            cur.execute("""
                        INSERT INTO repos (repo_id, fetched_at, name, full_name, owner_login, owner_type, description,
                                           language, license_spdx, topics, stargazers_count, forks_count, size,
                                           created_at, pushed_at)
                        VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                        """, (d['id'], d['name'], d['full_name'], d['owner']['login'], d['owner']['type'],
                              d.get('description'),
                              d.get('language'), (d.get('license') or {}).get('spdx_id'), d.get('topics', []),
                              d.get('stargazers_count'), d.get('forks_count'), d.get('size'), d.get('created_at'),
                              d.get('pushed_at')))
            return True
    except Exception as e:
        log.error(f"Repo API Error: {e}")
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
        insert_request_meta(cur, meta)
        conn.commit()
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
        insert_request_meta(cur, error_meta)
        conn.commit()
        log.warning("GitHub API request failed: %s", exc)
        return None, error_meta


def insert_request_meta(cur, meta: dict) -> None:
    """Insert a request metadata record into request_logs."""
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
        meta.get("request_success"),
        meta.get("sent_at"),
        meta.get("received_at"),
        meta.get("elapsed_s"),
        meta.get("method"),
        meta.get("url"),
        meta.get("status_code"),
        meta.get("reason"),
        meta.get("response_bytes"),
        meta.get("redirects"),
        meta.get("final_url"),
        meta.get("http_version"),
        meta.get("encoding"),
        psycopg2.extras.Json(_redact_headers(meta.get("request_headers"))),
        psycopg2.extras.Json(_redact_headers(meta.get("response_headers"))),
        meta.get("error"),
    ))

# ── Main ────────────────────────────────────────────────────────

def main():
    log.info("Starting GitHub Events Consumer (Star Schema Mode)")

    # 1. Kafka Consumer Initialisierung
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC_RAW, TOPIC_STATUS])

    # 2. Datenbank-Verbindung (mit Retry-Logik)
    conn = db_connect()
    cur = conn.cursor()

    try:
        while True:
            # Batch-poll: drain all available messages, process STATUS first
            batch = consumer.consume(num_messages=100, timeout=1.0)
            if not batch:
                continue

            # Separate STATUS from RAW so status/request_logs are never blocked by slow enrichment
            status_msgs = []
            raw_msgs = []
            for m in batch:
                if m.error():
                    log.error(f"Kafka error: {m.error()}")
                    continue
                if m.topic() == TOPIC_STATUS:
                    status_msgs.append(m)
                else:
                    raw_msgs.append(m)

            # Process all STATUS messages first (fast inserts)
            for m in status_msgs:
                try:
                    meta = json.loads(m.value().decode("utf-8"))
                    insert_request_meta(cur, meta)
                    conn.commit()
                    consumer.commit(message=m, asynchronous=False)
                except Exception as e:
                    conn.rollback()
                    log.error(f"Status Processing Error: {e}")
                    if "connection" in str(e).lower():
                        conn = db_connect()
                        cur = conn.cursor()

            # Then process RAW events
            for msg in raw_msgs:
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    actor = event.get("actor", {}).get("login")
                    repo_id = event.get("repo", {}).get("id")
                    repo_name = event.get("repo", {}).get("name")

                    if not actor or not repo_id:
                        continue

                    # --- STUFE 1: Stammdaten-Anreicherung (Enrichment) ---
                    enrich_user(cur, conn, actor)
                    enrich_repo(cur, conn, repo_name)

                    # --- STUFE 2: Relationen-Learning (Membership) ---
                    cur.execute("SELECT owner_login, owner_type FROM repos WHERE repo_id = %s", (repo_id,))
                    res = cur.fetchone()

                    if res:
                        owner_login, owner_type = res

                        if owner_type == 'Organization':
                            enrich_user(cur, conn, owner_login)
                            cur.execute("SELECT username FROM users WHERE username = %s", (actor,))
                            if cur.fetchone():
                                cur.execute("""
                                    INSERT INTO organization_members (org_login, user_username, role)
                                    VALUES (%s, %s, 'contributor') ON CONFLICT DO NOTHING
                                """, (owner_login, actor))

                    # --- STUFE 3: Event Fact-Table befüllen ---
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
