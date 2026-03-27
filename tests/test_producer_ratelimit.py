"""Unit tests for extract_ratelimit in producer.py."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producer'))


def test_producer_extract_ratelimit_source_is_producer():
    from producer import extract_ratelimit
    headers = {"X-RateLimit-Remaining": "4000", "X-RateLimit-Limit": "5000"}
    result = extract_ratelimit(headers, "producer")
    assert result["source"] == "producer"
    assert result["limit"] == 5000


def test_producer_extract_ratelimit_returns_none_without_header():
    from producer import extract_ratelimit
    assert extract_ratelimit({}, "producer") is None
