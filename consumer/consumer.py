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
GROUP_ID = "github-events-enricher"

DB_DSN = (
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')} "
    f"dbname={os.getenv('DB_NAME', 'github_events')} "
    f"user={os.getenv('DB_USER', 'github')} "
    f"password={os.getenv('DB_PASSWORD', 'github_secret')}"
)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_HEADERS = {"Accept": "application/vnd.github+json", "User-Agent": "ZHAW-Explorer/2.0"}
if GITHUB_TOKEN:
    GITHUB_HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

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
            log.warning("DB not ready, retrying in 3s...")
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

def enrich_user(cur, username):
    cur.execute("SELECT username FROM users WHERE username = %s", (username,))
    if cur.fetchone(): return True
    cur.execute("SELECT login FROM organizations WHERE login = %s", (username,))
    if cur.fetchone(): return True

    if not user_limiter.can_call(): return False

    try:
        r = requests.get(f"https://api.github.com/users/{username}", headers=GITHUB_HEADERS, timeout=5)
        user_limiter.record_call()
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


def enrich_repo(cur, full_name):
    cur.execute("SELECT repo_id FROM repos WHERE full_name = %s", (full_name,))
    if cur.fetchone(): return True
    if not repo_limiter.can_call(): return False

    try:
        r = requests.get(f"https://api.github.com/repos/{full_name}", headers=GITHUB_HEADERS, timeout=5)
        repo_limiter.record_call()
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
    consumer.subscribe([TOPIC_RAW])

    # 2. Datenbank-Verbindung (mit Retry-Logik)
    conn = db_connect()
    cur = conn.cursor()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                # Daten aus Kafka-Message parsen
                event = json.loads(msg.value().decode("utf-8"))
                actor = event.get("actor", {}).get("login")
                repo_id = event.get("repo", {}).get("id")
                repo_name = event.get("repo", {}).get("name")

                if not actor or not repo_id:
                    continue

                # --- STUFE 1: Stammdaten-Anreicherung (Enrichment) ---
                # Actor (User) anlegen/anreichern
                enrich_user(cur, actor)

                # Repository anlegen/anreichern
                enrich_repo(cur, repo_name)

                # --- STUFE 2: Relationen-Learning (Membership) ---
                # Wir prüfen, wer der Besitzer des Repos ist
                cur.execute("SELECT owner_login, owner_type FROM repos WHERE repo_id = %s", (repo_id,))
                res = cur.fetchone()

                if res:
                    owner_login, owner_type = res

                    if owner_type == 'Organization':
                        # KRITISCH: Damit der Foreign Key in organization_members nicht knallt,
                        # müssen wir sicherstellen, dass die Org in 'organizations' existiert.
                        enrich_user(cur, owner_login)

                        # Jetzt können wir die Verbindung gefahrlos speichern
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

                # Transaktion abschließen
                conn.commit()
                consumer.commit(asynchronous=False)

            except Exception as e:
                conn.rollback()
                log.error(f"Processing Error: {e}")
                # Bei DB-Verbindungsabbruch neu verbinden
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
