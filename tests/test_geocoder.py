"""Unit tests for geocoder.py — claim pattern and Nominatim parsing."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'geocoder'))

from unittest.mock import MagicMock, patch
import pytest


def make_cursor(claimed_row=None):
    """claimed_row: what RETURNING returns after UPDATE claim."""
    cur = MagicMock()
    cur.fetchone.return_value = claimed_row
    return cur


def make_nominatim_response(status_code=200, body=None):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = body or []
    return r


# ── parse_nominatim_result ────────────────────────────────────────

def test_parse_nominatim_result_extracts_country_and_coords():
    from geocoder import parse_nominatim_result
    data = [{
        "lat": "37.7749",
        "lon": "-122.4194",
        "address": {
            "country": "United States",
            "country_code": "us",
        }
    }]
    result = parse_nominatim_result(data)
    assert result["country"] == "United States"
    assert result["country_code"] == "US"
    assert result["lat"] == 37.7749
    assert result["lng"] == -122.4194


def test_parse_nominatim_result_returns_none_on_empty():
    from geocoder import parse_nominatim_result
    assert parse_nominatim_result([]) is None


def test_parse_nominatim_result_returns_none_on_none():
    from geocoder import parse_nominatim_result
    assert parse_nominatim_result(None) is None


# ── claim_pending_user ────────────────────────────────────────────

def test_claim_pending_user_returns_row_when_claimed():
    from geocoder import claim_pending_user
    cur = make_cursor(claimed_row=("octocat", "San Francisco, CA"))
    result = claim_pending_user(cur)
    assert result == ("octocat", "San Francisco, CA")
    sql = cur.execute.call_args[0][0]
    assert "geo_claimed_at" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_claim_pending_user_returns_none_when_nothing_pending():
    from geocoder import claim_pending_user
    cur = make_cursor(claimed_row=None)
    result = claim_pending_user(cur)
    assert result is None


# ── claim_pending_org ─────────────────────────────────────────────

def test_claim_pending_org_returns_row_when_claimed():
    from geocoder import claim_pending_org
    cur = make_cursor(claimed_row=("github", "San Francisco, CA"))
    result = claim_pending_org(cur)
    assert result == ("github", "San Francisco, CA")
    sql = cur.execute.call_args[0][0]
    assert "organizations" in sql
    assert "geo_claimed_at" in sql
    assert "FOR UPDATE SKIP LOCKED" in sql
