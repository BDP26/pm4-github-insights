"""Unit tests for enrich_repo in consumer.py."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

from unittest.mock import MagicMock, patch
import pytest


def make_cursor(fast_path_row=None, claim_row=None):
    cur = MagicMock()
    cur.fetchone.side_effect = [fast_path_row, claim_row, None, None]
    return cur


def make_conn():
    return MagicMock()


def make_response(status_code, json_body):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = json_body
    r.ok = status_code < 400
    r.elapsed.total_seconds.return_value = 0.1
    r.request.method = "GET"
    r.request.url = "https://api.github.com/repos/octocat/hello"
    r.request.headers = {}
    r.headers = {}
    r.content = b""
    r.history = []
    r.url = "https://api.github.com/repos/octocat/hello"
    r.raw.version = 11
    r.encoding = "utf-8"
    r.reason = "OK"
    return r


def get_update_calls(cur):
    return [c for c in cur.execute.call_args_list if "UPDATE repos" in str(c)]


# ── Fast path ────────────────────────────────────────────────────

def test_fully_enriched_repo_returns_true_without_api_call():
    """fetched_at IS NOT NULL in repos → return True, no API call."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=(42,))
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is True
    mock_req.assert_not_called()


# ── Claim: already owned ──────────────────────────────────────────

def test_claim_returns_no_row_skips_api_call():
    """INSERT RETURNING no row → another consumer owns it → False."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=None, claim_row=None)
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is False
    mock_req.assert_not_called()


# ── Successful enrichment ─────────────────────────────────────────

def test_successful_repo_response_updates_repos():
    """200 response → UPDATE repos with full data."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=None, claim_row=(42,))
    conn = make_conn()
    api_response = make_response(200, {
        "id": 42,
        "name": "hello",
        "full_name": "octocat/hello",
        "owner": {"login": "octocat", "type": "User"},
        "description": "My first repo",
        "language": "Python",
        "license": {"spdx_id": "MIT"},
        "topics": ["python", "demo"],
        "stargazers_count": 100,
        "forks_count": 20,
        "size": 500,
        "created_at": "2020-01-01T00:00:00Z",
        "pushed_at": "2026-01-01T00:00:00Z",
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is True
    updates = get_update_calls(cur)
    assert updates, "Must UPDATE repos after 200 response"


def test_repo_api_failure_returns_false():
    """logged_request returns None (network error) → return False and delete stub."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=None, claim_row=(42,))
    conn = make_conn()
    with patch("consumer.logged_request", return_value=(None, {})):
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is False
    delete_calls = [c for c in cur.execute.call_args_list if "DELETE FROM repos" in str(c)]
    assert delete_calls, "Must DELETE stub on network failure so other consumers can retry"


def test_exception_during_api_call_deletes_repo_stub():
    """Exception inside try block → stub is deleted so other consumers can retry."""
    from consumer import enrich_repo
    cur = make_cursor(fast_path_row=None, claim_row=(42,))
    conn = make_conn()
    with patch("consumer.logged_request", side_effect=RuntimeError("connection reset")):
        result = enrich_repo(cur, conn, 42, "octocat/hello")
    assert result is False
    delete_calls = [c for c in cur.execute.call_args_list if "DELETE FROM repos" in str(c)]
    assert delete_calls, "Must DELETE stub on exception so other consumers can retry"
