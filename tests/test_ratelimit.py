"""Unit tests for extract_ratelimit helper in consumer.py."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

from datetime import timezone
import pytest


def test_extract_ratelimit_parses_all_headers():
    from consumer import extract_ratelimit
    headers = {
        "X-RateLimit-Limit": "5000",
        "X-RateLimit-Used": "729",
        "X-RateLimit-Remaining": "4271",
        "X-RateLimit-Reset": "1774601860",
        "X-RateLimit-Resource": "core",
    }
    result = extract_ratelimit(headers, "consumer")
    assert result["limit"] == 5000
    assert result["used"] == 729
    assert result["remaining"] == 4271
    assert result["resource"] == "core"
    assert result["source"] == "consumer"
    assert result["reset_at"] is not None
    assert "recorded_at" in result


def test_extract_ratelimit_returns_none_when_no_header():
    from consumer import extract_ratelimit
    result = extract_ratelimit({}, "consumer")
    assert result is None


def test_extract_ratelimit_handles_missing_reset():
    from consumer import extract_ratelimit
    headers = {"X-RateLimit-Remaining": "100"}
    result = extract_ratelimit(headers, "producer")
    assert result["reset_at"] is None
    assert result["remaining"] == 100


def test_extract_ratelimit_defaults_resource_to_core():
    from consumer import extract_ratelimit
    headers = {"X-RateLimit-Remaining": "50"}
    result = extract_ratelimit(headers, "consumer")
    assert result["resource"] == "core"
