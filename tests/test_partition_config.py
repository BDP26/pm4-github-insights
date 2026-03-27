"""Unit tests for multi-instance partition assignment.

Tests cover:
- Config parsing: single-instance mode (flag unset or false)
- Config parsing: multi-instance mode with valid index
- Config parsing: multi-instance mode with missing index → fail-fast
- Partition calculation: 3 instances × 3 partitions (balanced)
- Partition calculation: 2 instances × 3 partitions (uneven)
- Partition calculation: KAFKA_TOTAL_INSTANCES > actual partitions → warning logged
"""
import importlib
import os
import sys
import logging
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def reload_consumer():
    """Re-import consumer so module-level os.getenv() reads fresh env vars."""
    import consumer
    importlib.reload(consumer)
    return consumer


# ---------------------------------------------------------------------------
# Tests: Config parsing — get_multi_instance_config()
# ---------------------------------------------------------------------------

def test_single_instance_mode_when_flag_unset(monkeypatch):
    """KAFKA_MULTI_INSTANCE_ENABLED unset → disabled, no index/total returned."""
    monkeypatch.delenv("KAFKA_MULTI_INSTANCE_ENABLED", raising=False)
    from consumer import get_multi_instance_config
    enabled, idx, total = get_multi_instance_config()
    assert enabled is False
    assert idx is None
    assert total is None


def test_single_instance_mode_when_flag_false(monkeypatch):
    """KAFKA_MULTI_INSTANCE_ENABLED=false → disabled."""
    monkeypatch.setenv("KAFKA_MULTI_INSTANCE_ENABLED", "false")
    from consumer import get_multi_instance_config
    enabled, idx, total = get_multi_instance_config()
    assert enabled is False


def test_multi_instance_valid_config(monkeypatch):
    """KAFKA_MULTI_INSTANCE_ENABLED=true + valid index + total → enabled."""
    monkeypatch.setenv("KAFKA_MULTI_INSTANCE_ENABLED", "true")
    monkeypatch.setenv("KAFKA_INSTANCE_INDEX", "1")
    monkeypatch.setenv("KAFKA_TOTAL_INSTANCES", "3")
    from consumer import get_multi_instance_config
    enabled, idx, total = get_multi_instance_config()
    assert enabled is True
    assert idx == 1
    assert total == 3


def test_multi_instance_defaults_total_to_3(monkeypatch):
    """KAFKA_TOTAL_INSTANCES unset → defaults to 3."""
    monkeypatch.setenv("KAFKA_MULTI_INSTANCE_ENABLED", "true")
    monkeypatch.setenv("KAFKA_INSTANCE_INDEX", "0")
    monkeypatch.delenv("KAFKA_TOTAL_INSTANCES", raising=False)
    from consumer import get_multi_instance_config
    _, _, total = get_multi_instance_config()
    assert total == 3


def test_multi_instance_missing_index_raises_with_clear_message(monkeypatch):
    """KAFKA_MULTI_INSTANCE_ENABLED=true but KAFKA_INSTANCE_INDEX missing → SystemExit."""
    monkeypatch.setenv("KAFKA_MULTI_INSTANCE_ENABLED", "true")
    monkeypatch.delenv("KAFKA_INSTANCE_INDEX", raising=False)
    from consumer import get_multi_instance_config
    with pytest.raises(SystemExit) as exc_info:
        get_multi_instance_config()
    assert "KAFKA_INSTANCE_INDEX" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Tests: Partition calculation — calculate_assigned_partitions()
# ---------------------------------------------------------------------------

def test_partition_calc_3_instances_3_partitions_instance_0():
    """3 instances × 3 partitions: instance 0 → [0]."""
    from consumer import calculate_assigned_partitions
    assert calculate_assigned_partitions(0, 3, [0, 1, 2]) == [0]


def test_partition_calc_3_instances_3_partitions_instance_1():
    """3 instances × 3 partitions: instance 1 → [1]."""
    from consumer import calculate_assigned_partitions
    assert calculate_assigned_partitions(1, 3, [0, 1, 2]) == [1]


def test_partition_calc_3_instances_3_partitions_instance_2():
    """3 instances × 3 partitions: instance 2 → [2]."""
    from consumer import calculate_assigned_partitions
    assert calculate_assigned_partitions(2, 3, [0, 1, 2]) == [2]


def test_partition_calc_no_overlap_3_instances():
    """3 instances × 3 partitions: no partition appears twice."""
    from consumer import calculate_assigned_partitions
    all_parts = [0, 1, 2]
    assigned = [
        calculate_assigned_partitions(i, 3, all_parts)
        for i in range(3)
    ]
    flat = [p for group in assigned for p in group]
    assert sorted(flat) == all_parts, "Every partition must be assigned exactly once"


def test_partition_calc_2_instances_3_partitions_instance_0():
    """2 instances × 3 partitions: instance 0 → [0, 2]."""
    from consumer import calculate_assigned_partitions
    assert calculate_assigned_partitions(0, 2, [0, 1, 2]) == [0, 2]


def test_partition_calc_2_instances_3_partitions_instance_1():
    """2 instances × 3 partitions: instance 1 → [1]."""
    from consumer import calculate_assigned_partitions
    assert calculate_assigned_partitions(1, 2, [0, 1, 2]) == [1]


def test_partition_calc_total_greater_than_actual_logs_warning(caplog):
    """KAFKA_TOTAL_INSTANCES > actual partitions → warning logged, still assigns."""
    from consumer import calculate_assigned_partitions
    with caplog.at_level(logging.WARNING, logger="consumer"):
        result = calculate_assigned_partitions(0, 5, [0, 1, 2])
    assert result == [0]  # 0 % 5 == 0
    assert any("warn" in r.levelname.lower() for r in caplog.records), (
        "A warning must be logged when total_instances > len(topic_partitions)"
    )
