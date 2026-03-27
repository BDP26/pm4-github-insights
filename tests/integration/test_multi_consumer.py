"""Integration tests: multi-instance consumer partition assignment.

Verifies that running 3 consumer instances with KAFKA_MULTI_INSTANCE_ENABLED=true:
  1. Starts without NOT_COORDINATOR errors
  2. Each instance logs its correct partition assignment
  3. All produced events are consumed exactly once across the 3 instances

Requirements:
  pip install confluent-kafka pytest

The test stack runs on non-conflicting ports (Kafka: 19094, TimescaleDB: 15432)
so it can run alongside a production stack without port clashes.

Run:
  python -m pytest tests/integration/test_multi_consumer.py -v -s
  (takes ~2-3 minutes: stack start + kafka-init + consumption)
"""

import json
import os
import subprocess
import time

import pytest

confluent_kafka = pytest.importorskip(
    "confluent_kafka",
    reason="confluent_kafka not installed — pip install confluent-kafka",
)
from confluent_kafka import Producer  # noqa: E402

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

INTEGRATION_DIR = os.path.dirname(__file__)
COMPOSE_FILE = os.path.join(INTEGRATION_DIR, "docker-compose.integration-test.yml")
PROJECT_NAME = "pm4-integtest"

# External Kafka port in the test compose (avoids clash with prod 9094)
KAFKA_EXTERNAL = "localhost:19094"
RAW_TOPIC = "github.events.raw"

# One event per partition × 3 partitions
EVENTS_PER_PARTITION = 5
TOTAL_EVENTS = EVENTS_PER_PARTITION * 3

# Consumer container names: Docker Compose generates them as
# {project}-{service}-{replica}, e.g. pm4-integtest-consumer-0-1
CONSUMER_CONTAINERS = [f"{PROJECT_NAME}-consumer-{i}-1" for i in range(3)]
KAFKA_CONTAINER = f"{PROJECT_NAME}-kafka-1"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compose(*args, check=True):
    return subprocess.run(
        ["docker", "compose",
         "-f", COMPOSE_FILE,
         "-p", PROJECT_NAME,
         *args],
        capture_output=True, text=True, check=check,
    )


def _container_logs(container: str) -> str:
    result = subprocess.run(
        ["docker", "logs", container],
        capture_output=True, text=True, check=False,
    )
    return result.stdout + result.stderr


def _wait_for_log(container: str, pattern: str, timeout: int = 60) -> bool:
    """Poll container logs until pattern appears or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pattern in _container_logs(container):
            return True
        time.sleep(2)
    return False


def _consumer_group_lag(partition_index: int) -> int:
    """Return total lag for github-events-enricher-p{partition_index}."""
    group_id = f"github-events-enricher-p{partition_index}"
    result = subprocess.run(
        [
            "docker", "exec", KAFKA_CONTAINER,
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--group", group_id,
        ],
        capture_output=True, text=True, check=False,
    )
    total = 0
    for line in result.stdout.splitlines():
        parts = line.split()
        # kafka-consumer-groups.sh --describe columns (0-indexed):
        #   0:GROUP  1:TOPIC  2:PARTITION  3:CURRENT-OFFSET  4:LOG-END-OFFSET  5:LAG  ...
        if len(parts) >= 6 and parts[5].lstrip("-").isdigit():
            lag = int(parts[5])
            if lag >= 0:
                total += lag
    return total


def _wait_for_all_lag_zero(timeout: int = 90) -> list[int] | None:
    """Poll until all 3 consumer groups reach lag=0, or return None on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lags = [_consumer_group_lag(i) for i in range(3)]
        print(f"\n    Lags per partition group: {lags}")
        if all(lag == 0 for lag in lags):
            return lags
        time.sleep(4)
    return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module", autouse=True)
def stack():
    """Start the integration test stack; tear it down after all tests in this module."""
    print("\n[stack] Starting integration test stack…")
    _compose("up", "--build", "-d")

    # Wait for Kafka and TimescaleDB to be healthy
    print("[stack] Waiting for Kafka and TimescaleDB to be healthy…")
    _compose("up", "--wait", "--timeout", "120", check=False)

    # Wait for each consumer to log its partition assignment
    print("[stack] Waiting for consumers to assign partitions…")
    for container in CONSUMER_CONTAINERS:
        ok = _wait_for_log(container, "Multi-instance mode:", timeout=60)
        if not ok:
            logs = _container_logs(container)
            pytest.fail(
                f"{container} never logged partition assignment within 60s.\n"
                f"Last logs:\n{logs[-3000:]}"
            )

    print("[stack] All consumers running.")
    yield

    print("\n[stack] Tearing down integration test stack…")
    _compose("down", "-v", check=False)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMultiConsumerIntegration:

    def test_no_not_coordinator_error_in_any_consumer(self):
        """No consumer instance may log a NOT_COORDINATOR error after startup."""
        for container in CONSUMER_CONTAINERS:
            logs = _container_logs(container)
            offending = [l for l in logs.splitlines() if "NOT_COORDINATOR" in l]
            assert not offending, (
                f"{container} logged NOT_COORDINATOR error(s):\n"
                + "\n".join(offending)
            )

    def test_each_consumer_assigned_its_own_partition(self):
        """consumer-N must log that it owns partition N (and only N)."""
        for i, container in enumerate(CONSUMER_CONTAINERS):
            logs = _container_logs(container)
            assert f"assigned partitions [{i}]" in logs, (
                f"{container} did not log correct partition assignment.\n"
                f"Expected: 'assigned partitions [{i}]'\n"
                f"Last logs:\n{logs[-2000:]}"
            )

    def test_all_events_consumed_exactly_once(self):
        """Produce events directly to each partition; lag must reach 0 on all groups."""
        p = Producer({"bootstrap.servers": KAFKA_EXTERNAL})
        delivered: list[tuple[int, int]] = []

        def on_delivery(err, msg):
            assert err is None, f"Delivery failed: {err}"
            delivered.append((msg.partition(), msg.offset()))

        # Produce EVENTS_PER_PARTITION events pinned to each partition explicitly
        for partition in range(3):
            for seq in range(EVENTS_PER_PARTITION):
                event = {
                    "id": f"inttest-p{partition}-{seq:04d}",
                    "type": "WatchEvent",
                    "actor": {"login": f"integtest-user-{partition}-{seq}"},
                    "repo": {
                        "id": 900000 + partition * 1000 + seq,
                        "name": f"integtest/repo-{partition}-{seq}",
                    },
                    "created_at": "2026-03-27T00:00:00Z",
                    "payload": {},
                }
                p.produce(
                    RAW_TOPIC,
                    value=json.dumps(event).encode(),
                    partition=partition,  # pin explicitly to test partition ownership
                    on_delivery=on_delivery,
                )

        p.flush(timeout=15)
        assert len(delivered) == TOTAL_EVENTS, (
            f"Only {len(delivered)}/{TOTAL_EVENTS} messages were acknowledged by Kafka"
        )
        partitions_hit = {p for p, _ in delivered}
        assert partitions_hit == {0, 1, 2}, f"Not all partitions received events: {partitions_hit}"

        # Wait for all consumer groups to drain
        lags = _wait_for_all_lag_zero(timeout=90)
        assert lags is not None, (
            "Consumer lag did not reach 0 within 90s. "
            f"Final lags: {[_consumer_group_lag(i) for i in range(3)]}\n"
            + "\n".join(f"--- {c} ---\n{_container_logs(c)[-1500:]}" for c in CONSUMER_CONTAINERS)
        )
        assert all(lag == 0 for lag in lags), f"Non-zero lag after drain: {lags}"
