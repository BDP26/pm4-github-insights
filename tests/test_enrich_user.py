"""Unit tests for enrich_user in consumer.py.

Tests cover:
- Returning True immediately when user/bot already in users table (cache hit)
- Returning True immediately when org already in organizations table (cache hit)
- Inserting a real GitHub User with is_bot=FALSE
- Inserting a GitHub Bot with is_bot=TRUE (no geo, no company)
- Inserting a GitHub Organization (unchanged behavior)
- Returning False when rate limiter is exhausted
- Inserting a stub row with is_bot=FALSE when GitHub returns 404
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

from unittest.mock import MagicMock, patch, call
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_cursor(users_row=None, orgs_row=None):
    """Return a mock psycopg2 cursor.

    fetchone() returns values in call order:
      1st call → users lookup
      2nd call → orgs lookup (only if users_row is None)
    """
    cur = MagicMock()
    cur.fetchone.side_effect = [users_row, orgs_row, None, None, None]
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
    """Return execute calls that contain INSERT INTO <table>."""
    return [c for c in cur.execute.call_args_list if f"INSERT INTO {table}" in str(c)]


def get_insert_params(cur, table):
    """Return the params tuple of the first INSERT INTO <table> call."""
    inserts = get_insert_calls(cur, table)
    assert inserts, f"No INSERT INTO {table} found"
    # call_args is (args, kwargs); args[0] is sql, args[1] is params tuple
    return inserts[0][0][1]


# ---------------------------------------------------------------------------
# Tests: cache hits (no API call)
# ---------------------------------------------------------------------------

def test_already_in_users_returns_true_without_api_call():
    """If username already in users table → return True, no API call."""
    from consumer import enrich_user
    cur = make_cursor(users_row=("octocat",))
    conn = make_conn()

    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "octocat")

    assert result is True
    mock_req.assert_not_called()


def test_already_in_orgs_returns_true_without_api_call():
    """If login already in organizations table → return True, no API call."""
    from consumer import enrich_user
    cur = make_cursor(users_row=None, orgs_row=("github",))
    conn = make_conn()

    with patch("consumer.logged_request") as mock_req:
        result = enrich_user(cur, conn, "github")

    assert result is True
    mock_req.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: rate limiting
# ---------------------------------------------------------------------------

def test_rate_limited_returns_false():
    """If rate limiter exhausted → return False, no API call."""
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    with patch.object(user_limiter, "can_call", return_value=False):
        with patch("consumer.logged_request") as mock_req:
            result = enrich_user(cur, conn, "newuser")

    assert result is False
    mock_req.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: GitHub type == "User"
# ---------------------------------------------------------------------------

def test_real_user_inserts_into_users_with_is_bot_false():
    """GitHub type=User → INSERT SQL must contain literal FALSE for is_bot.

    is_bot=FALSE is embedded as a SQL literal (not a %s parameter) in the
    User branch. We therefore inspect the SQL string, not the params tuple.
    """
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    api_response = make_response(200, {
        "login": "octocat",
        "type": "User",
        "company": "GitHub",
        "location": "San Francisco, CA",
        "public_repos": 10,
        "followers": 500,
    })

    with patch.object(user_limiter, "can_call", return_value=True):
        with patch.object(user_limiter, "record_call"):
            with patch("consumer.logged_request", return_value=(api_response, {})):
                with patch("consumer.geocode", return_value={
                    "country": "United States",
                    "country_code": "US",
                    "lat": 37.7749,
                    "lng": -122.4194,
                }):
                    result = enrich_user(cur, conn, "octocat")

    assert result is True
    inserts = get_insert_calls(cur, "users")
    assert len(inserts) == 1
    sql_str = inserts[0][0][0]  # first positional arg to execute() = the SQL string
    assert "FALSE" in sql_str.upper(), (
        f"is_bot=FALSE literal must be present in User INSERT SQL, got: {sql_str}"
    )


def test_real_user_does_not_insert_into_orgs():
    """GitHub type=User → must NOT insert into organizations."""
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    api_response = make_response(200, {
        "login": "octocat",
        "type": "User",
        "company": None,
        "location": None,
        "public_repos": 5,
        "followers": 100,
    })

    with patch.object(user_limiter, "can_call", return_value=True):
        with patch.object(user_limiter, "record_call"):
            with patch("consumer.logged_request", return_value=(api_response, {})):
                with patch("consumer.geocode", return_value={}):
                    enrich_user(cur, conn, "octocat")

    assert len(get_insert_calls(cur, "organizations")) == 0, \
        "User must not be inserted into organizations"


# ---------------------------------------------------------------------------
# Tests: GitHub type == "Bot"
# ---------------------------------------------------------------------------

def test_bot_inserts_into_users_with_is_bot_true():
    """GitHub type=Bot → INSERT INTO users with is_bot=TRUE as parameter."""
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    api_response = make_response(200, {
        "login": "dependabot[bot]",
        "type": "Bot",
    })

    with patch.object(user_limiter, "can_call", return_value=True):
        with patch.object(user_limiter, "record_call"):
            with patch("consumer.logged_request", return_value=(api_response, {})):
                with patch("consumer.geocode") as mock_geo:
                    result = enrich_user(cur, conn, "dependabot[bot]")

    assert result is True
    mock_geo.assert_not_called()  # bots have no location → no geocoding

    params = get_insert_params(cur, "users")
    # Bot INSERT: VALUES (%s, NOW(), %s) → (login, is_bot)
    # is_bot=TRUE is passed as Python True in params
    assert True in params, f"is_bot=TRUE expected in INSERT params, got: {params}"


def test_bot_does_not_insert_into_orgs():
    """GitHub type=Bot → must NOT insert into organizations."""
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    api_response = make_response(200, {
        "login": "dependabot[bot]",
        "type": "Bot",
    })

    with patch.object(user_limiter, "can_call", return_value=True):
        with patch.object(user_limiter, "record_call"):
            with patch("consumer.logged_request", return_value=(api_response, {})):
                with patch("consumer.geocode", return_value={}):
                    enrich_user(cur, conn, "dependabot[bot]")

    assert len(get_insert_calls(cur, "organizations")) == 0, \
        "Bot must not be inserted into organizations"


def test_bot_no_geocode_called():
    """Geocode must never be called for a bot — they have no location."""
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    api_response = make_response(200, {"login": "renovate[bot]", "type": "Bot"})

    with patch.object(user_limiter, "can_call", return_value=True):
        with patch.object(user_limiter, "record_call"):
            with patch("consumer.logged_request", return_value=(api_response, {})):
                with patch("consumer.geocode") as mock_geo:
                    enrich_user(cur, conn, "renovate[bot]")

    mock_geo.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: GitHub type == "Organization"
# ---------------------------------------------------------------------------

def test_org_inserts_into_organizations():
    """GitHub type=Organization → insert into organizations (unchanged behavior)."""
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    api_response = make_response(200, {
        "login": "github",
        "type": "Organization",
        "name": "GitHub",
        "description": "Where software is built",
        "location": "San Francisco, CA",
        "public_repos": 400,
        "created_at": "2008-05-11T04:37:31Z",
    })

    with patch.object(user_limiter, "can_call", return_value=True):
        with patch.object(user_limiter, "record_call"):
            with patch("consumer.logged_request", return_value=(api_response, {})):
                result = enrich_user(cur, conn, "github")

    assert result is True
    assert len(get_insert_calls(cur, "organizations")) == 1


# ---------------------------------------------------------------------------
# Tests: 404 stub row
# ---------------------------------------------------------------------------

def test_404_inserts_stub_row_to_prevent_repeated_api_calls():
    """GitHub returns 404 → insert stub row in users so it is cached.

    Without this, every event for an unresolvable username burns a rate-limit
    API call. The stub row (is_bot=FALSE by default) is cached on the next
    enrich_user call via the fast-path SELECT.
    """
    from consumer import enrich_user, user_limiter
    cur = make_cursor()
    conn = make_conn()

    api_response = make_response(404, {"message": "Not Found"})

    with patch.object(user_limiter, "can_call", return_value=True):
        with patch.object(user_limiter, "record_call"):
            with patch("consumer.logged_request", return_value=(api_response, {})):
                result = enrich_user(cur, conn, "ghost")

    # Returns False (could not enrich) but stub was inserted
    assert result is False
    stub_inserts = get_insert_calls(cur, "users")
    assert len(stub_inserts) == 1, "Stub row must be inserted on 404 to prevent repeated API calls"
    params = stub_inserts[0][0][1]
    assert "ghost" in params, f"Stub row must use the username passed to enrich_user, got: {params}"


def test_is_non_bot_user_returns_false_for_bot():
    """_is_non_bot_user returns False when no non-bot row is found (actor is a bot)."""
    from consumer import _is_non_bot_user
    cur = MagicMock()
    cur.fetchone.return_value = None
    result = _is_non_bot_user(cur, "bot[bot]")
    assert result is False
    sql = cur.execute.call_args[0][0]
    assert "is_bot = FALSE" in sql


def test_is_non_bot_user_returns_true_for_human():
    """_is_non_bot_user returns True when a non-bot row exists."""
    from consumer import _is_non_bot_user
    cur = MagicMock()
    cur.fetchone.return_value = ("alice",)
    result = _is_non_bot_user(cur, "alice")
    assert result is True
