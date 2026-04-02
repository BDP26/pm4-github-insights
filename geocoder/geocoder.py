import logging
import os
import time

import psycopg2
import psycopg2.extensions
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


def db_connect() -> psycopg2.extensions.connection:
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


def main() -> None:
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
