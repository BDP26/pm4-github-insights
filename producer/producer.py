"""
GitHub Events Producer
─────────────────────
Polls the GitHub public events API and publishes raw JSON events
to the Kafka topic  github.events.raw

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS  (default: localhost:9092)
  GITHUB_TOKEN             (optional, raises rate limit to 5000 req/h)
  POLL_INTERVAL_SECONDS    (default: 10)
  MAX_PAGES                (default: 3)
"""

import json
import os
import time
import logging
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────
BOOTSTRAP_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW            = "github.events.raw"
POLL_INTERVAL        = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
MAX_PAGES            = int(os.getenv("MAX_PAGES", "3"))
TOKEN                = os.getenv("GITHUB_TOKEN", "")

GITHUB_HEADERS = {
    "Accept":     "application/vnd.github+json",
    "User-Agent": "ZHAW-BigData-Explorer/1.0",
}
if TOKEN:
    GITHUB_HEADERS["Authorization"] = f"Bearer {TOKEN}"

GITHUB_API_URL = "https://api.github.com/events"

# ── Kafka setup ───────────────────────────────────────────────────

def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers":            BOOTSTRAP_SERVERS,
        "acks":                         "all",
        "retries":                      5,
        "retry.backoff.ms":             500,
        "compression.type":             "snappy",
        "batch.num.messages":           500,
        "linger.ms":                    200,          # small batching window
        "queue.buffering.max.messages": 100_000,
    })


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for event %s: %s", msg.key(), err)


# ── State ─────────────────────────────────────────────────────────
seen_ids:  set  = set()
last_etag: str  = ""
poll_count: int = 0
sent_total: int = 0


def fetch_events() -> list[dict]:
    """Fetch new events from the GitHub API, skipping already-seen IDs."""
    global last_etag, sent_total

    new_events: list[dict] = []
    etag_headers = {"If-None-Match": last_etag} if last_etag else {}

    for page in range(1, MAX_PAGES + 1):
        try:
            resp = requests.get(
                GITHUB_API_URL,
                headers={**GITHUB_HEADERS, **etag_headers},
                params={"per_page": 30, "page": page},
                timeout=10,
            )

            if resp.status_code == 304:
                log.debug("304 Not Modified — no new events on page %d", page)
                break

            if resp.status_code == 403:
                reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
                wait     = max(0, reset_ts - int(time.time()))
                log.warning("Rate-limited by GitHub. Sleeping %ds. Set GITHUB_TOKEN!", wait)
                time.sleep(wait + 1)
                break

            resp.raise_for_status()

            if page == 1:
                last_etag = resp.headers.get("ETag", "")

            for event in resp.json():
                eid = event.get("id")
                if eid and eid not in seen_ids:
                    seen_ids.add(eid)
                    new_events.append(event)

        except requests.RequestException as exc:
            log.warning("GitHub API request failed: %s", exc)
            break

    return new_events


def publish_events(producer: Producer, events: list[dict]) -> int:
    """Serialize and publish events to Kafka; returns count sent."""
    global sent_total

    for event in events:
        event["_ingested_at"] = datetime.now(timezone.utc).isoformat()

        producer.produce(
            topic     = TOPIC_RAW,
            key       = event.get("id", ""),
            value     = json.dumps(event, default=str),
            callback  = delivery_report,
        )
        producer.poll(0)   # serve delivery callbacks without blocking

    producer.flush(timeout=10)
    sent_total += len(events)
    return len(events)


def main():
    global poll_count

    log.info("Starting GitHub Events Producer")
    log.info("  Bootstrap servers : %s", BOOTSTRAP_SERVERS)
    log.info("  Topic             : %s", TOPIC_RAW)
    log.info("  Poll interval     : %ds", POLL_INTERVAL)
    log.info("  GitHub auth       : %s", "yes (token)" if TOKEN else "no (60 req/h limit)")

    producer = create_producer()

    while True:
        poll_count += 1
        events = fetch_events()

        if events:
            sent = publish_events(producer, events)
            log.info(
                "Poll #%d → %d new events sent | total sent: %d | seen IDs: %d",
                poll_count, sent, sent_total, len(seen_ids),
            )
        else:
            log.info("Poll #%d → no new events", poll_count)

        # Trim seen_ids to avoid unbounded growth (keep last 10 k)
        if len(seen_ids) > 10_000:
            surplus = len(seen_ids) - 8_000
            for _ in range(surplus):
                seen_ids.pop()

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
