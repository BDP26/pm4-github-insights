"""Unit tests for enrich_user in consumer.py.

Tests cover:
- Fast path: returns True when user already fully enriched (fetched_at IS NOT NULL)
- Fast path: returns True when org already fully enriched
- Claim stub: returns False when another consumer already inserted a stub
- GitHub type=User: inserts with is_bot=FALSE
- GitHub type=Bot: inserts with is_bot=TRUE, no geocode
- GitHub type=Organization: inserts into organizations
- 404: inserts stub row to prevent future API calls
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

from unittest.mock import MagicMock, patch
import pytest


def make_cursor(fast_path_row=None, claim_row=None):
    """Return a mock cursor.

    call order for fetchone():
      1st → fast-path SELECT (fetched_at IS NOT NULL check)
      2nd → claim INSERT RETURNING
    """
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
    r.request.url = "https://api.github.com/users/test"
    r.request.headers = {}
    r.headers = {}
    r.content = b""
    r.history = []
    r.url = "https://api.github.com/users/test"
    r.raw.version = 11
    r.encoding = "utf-8"
    r.reason = "OK"
    return r


def get_insert_calls(cur, table):
    return [c for c in cur.execute.call_args_list if f"INSERT INTO {table}" in str(c)]


def get_update_calls(cur, table):
    return [c for c in cur.execute.call_args_list if f"UPDATE {table}" in str(c)]


# ── Fast path: already fully enriched ────────────────────────────

def test_fully_enriched_user_returns_true_without_api_call():
    """fetched_at IS NOT NULL in users → return True, no API call."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=("octocat",))
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "octocat")
    assert result is True
    mock_req.assert_not_called()


def test_fully_enriched_org_returns_true_without_api_call():
    """fetched_at IS NOT NULL in organizations → return True, no API call."""
    from consumer import enrich_user
    # fast_path_row=None (not in users), then org fast-path row
    cur = MagicMock()
    cur.fetchone.side_effect = [None, ("github",), None, None]
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "github")
    assert result is True
    mock_req.assert_not_called()


# ── Claim stub: already claimed by another consumer ───────────────

def test_claim_returns_no_row_skips_api_call():
    """INSERT RETURNING no row → another consumer owns it → return False."""
    from consumer import enrich_user
    # fast_path=None (not enriched), claim=None (already claimed)
    cur = make_cursor(fast_path_row=None, claim_row=None)
    conn = make_conn()
    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "someuser")
    assert result is False
    mock_req.assert_not_called()


# ── GitHub type == "User" ─────────────────────────────────────────

def test_user_response_updates_users_with_is_bot_false():
    """200 type=User → UPDATE users, is_bot=FALSE in params."""
    from consumer import enrich_user
    # fast_path=None, claim=("octocat",) → we own it
    cur = make_cursor(fast_path_row=None, claim_row=("octocat",))
    conn = make_conn()
    api_response = make_response(200, {
        "login": "octocat", "type": "User",
        "company": "GitHub", "location": "San Francisco, CA",
        "public_repos": 10, "followers": 500,
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "octocat")
    assert result is True
    updates = get_update_calls(cur, "users")
    assert updates, "Must UPDATE users after API response"
    all_params = [str(c) for c in cur.execute.call_args_list]
    assert any("FALSE" in s.upper() or "false" in s for s in all_params), \
        "is_bot=False must appear in UPDATE"


def test_user_response_does_not_insert_into_orgs():
    """200 type=User → no INSERT INTO organizations."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("octocat",))
    conn = make_conn()
    api_response = make_response(200, {
        "login": "octocat", "type": "User",
        "company": None, "location": None,
        "public_repos": 5, "followers": 100,
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        enrich_user(cur, conn, "octocat")
    assert len(get_insert_calls(cur, "organizations")) == 0


# ── GitHub type == "Bot" ──────────────────────────────────────────

def test_bot_response_updates_users_with_is_bot_true():
    """200 type=Bot → UPDATE users with is_bot=True in params."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("dependabot[bot]",))
    conn = make_conn()
    api_response = make_response(200, {"login": "dependabot[bot]", "type": "Bot"})
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "dependabot[bot]")
    assert result is True
    updates = get_update_calls(cur, "users")
    assert updates, "Must UPDATE users for bot"
    params_flat = [p for c in updates for p in (c[0][1] if len(c[0]) > 1 else [])]
    assert True in params_flat, "is_bot=True must appear in UPDATE params"


def test_bot_does_not_insert_into_orgs():
    """200 type=Bot → no INSERT INTO organizations."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("bot[bot]",))
    conn = make_conn()
    api_response = make_response(200, {"login": "bot[bot]", "type": "Bot"})
    with patch("consumer.logged_request", return_value=(api_response, {})):
        enrich_user(cur, conn, "bot[bot]")
    assert len(get_insert_calls(cur, "organizations")) == 0


# ── GitHub type == "Organization" ────────────────────────────────

def test_org_response_inserts_into_organizations():
    """200 type=Organization → INSERT INTO organizations."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("github",))
    conn = make_conn()
    api_response = make_response(200, {
        "login": "github", "type": "Organization",
        "name": "GitHub", "description": "Where software is built",
        "location": "San Francisco, CA", "public_repos": 400,
        "created_at": "2008-05-11T04:37:31Z",
    })
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "github")
    assert result is True
    assert len(get_insert_calls(cur, "organizations")) == 1


# ── 404 stub ─────────────────────────────────────────────────────

def test_404_finalises_stub_to_prevent_future_api_calls():
    """404 → UPDATE stub row with fetched_at=NOW() so fast-path catches it next time."""
    from consumer import enrich_user
    cur = make_cursor(fast_path_row=None, claim_row=("ghost",))
    conn = make_conn()
    api_response = make_response(404, {"message": "Not Found"})
    with patch("consumer.logged_request", return_value=(api_response, {})):
        result = enrich_user(cur, conn, "ghost")
    assert result is False
    updates = get_update_calls(cur, "users")
    assert updates, "Must UPDATE stub row on 404 to finalise it"


# ── _is_non_bot_user ──────────────────────────────────────────────

def test_is_non_bot_user_returns_false_for_bot():
    from consumer import _is_non_bot_user
    cur = MagicMock()
    cur.fetchone.return_value = None
    assert _is_non_bot_user(cur, "bot[bot]") is False
    assert "is_bot = FALSE" in cur.execute.call_args[0][0]


def test_is_non_bot_user_returns_true_for_human():
    from consumer import _is_non_bot_user
    cur = MagicMock()
    cur.fetchone.return_value = ("alice",)
    assert _is_non_bot_user(cur, "alice") is True
