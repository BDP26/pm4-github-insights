"""HTTP-mocked integration tests for the Enricher class."""
import json
import sys
import pathlib
import time
import responses as resp_lib

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

from enricher import Enricher, GRAPHQL_ENDPOINT, GRAPHQL_BATCH_SIZE
from unittest.mock import MagicMock


def _graphql_user_response(usernames: list[str]) -> dict:
    return {
        "data": {
            f"u{i}": {
                "__typename": "User",
                "login": name,
                "databaseId": i + 1,
                "company": "ACME",
                "location": "Zurich",
                "followers": {"totalCount": 10},
                "repositories": {"totalCount": 5},
            }
            for i, name in enumerate(usernames)
        }
    }


@resp_lib.activate
def test_single_user_graphql_batch():
    cur = MagicMock()
    conn = MagicMock()
    # is_enriched checks: user→None, org→None; claim→("alice",)
    cur.fetchone.side_effect = [None, None, ("alice",)]

    resp_lib.add(
        resp_lib.POST, GRAPHQL_ENDPOINT,
        json=_graphql_user_response(["alice"]),
        status=200,
        headers={
            "X-RateLimit-Remaining": "4999",
            "X-RateLimit-Reset": str(int(time.time()) + 3600),
        },
    )

    enricher = Enricher(["tok_a"], ["tok_r"], conn, cur, None)
    enricher.add_user("alice")
    enricher.flush(force=True)

    calls = [c[0][0] for c in cur.execute.call_args_list]
    assert any("UPDATE users" in sql for sql in calls)


@resp_lib.activate
def test_single_repo_graphql_batch():
    cur = MagicMock()
    conn = MagicMock()
    # is_enriched(repo)→None; claim→(1,)
    cur.fetchone.side_effect = [None, (1,)]

    resp_lib.add(
        resp_lib.POST, GRAPHQL_ENDPOINT,
        json={
            "data": {
                "r0": {
                    "databaseId": 1,
                    "name": "linux",
                    "nameWithOwner": "torvalds/linux",
                    "owner": {"login": "torvalds", "__typename": "User"},
                    "description": None,
                    "primaryLanguage": {"name": "C"},
                    "licenseInfo": None,
                    "repositoryTopics": {"nodes": []},
                    "stargazerCount": 100,
                    "forkCount": 10,
                    "watchers": {"totalCount": 5},
                    "hasIssuesEnabled": True,
                    "issues": {"totalCount": 0},
                    "hasProjectsEnabled": False,
                    "isArchived": False,
                    "isDisabled": False,
                    "homepageUrl": None,
                    "diskUsage": 1000,
                    "createdAt": "2011-09-04T22:14:16Z",
                    "pushedAt": "2026-04-01T10:00:00Z",
                }
            }
        },
        status=200,
        headers={
            "X-RateLimit-Remaining": "4999",
            "X-RateLimit-Reset": str(int(time.time()) + 3600),
        },
    )

    enricher = Enricher(["tok_a"], ["tok_r"], conn, cur, None)
    enricher.add_repo(1, "torvalds/linux")
    enricher.flush(force=True)

    calls = [c[0][0] for c in cur.execute.call_args_list]
    assert any("UPDATE repos" in sql for sql in calls)


@resp_lib.activate
def test_size_trigger_flushes_at_batch_size():
    cur = MagicMock()
    conn = MagicMock()

    # Each user needs: is_enriched×2 (None,None) + claim→("uX",)
    side_effects = []
    for _ in range(GRAPHQL_BATCH_SIZE):
        side_effects += [None, None, ("u",)]
    cur.fetchone.side_effect = side_effects

    usernames = [f"user{i}" for i in range(GRAPHQL_BATCH_SIZE)]
    resp_lib.add(
        resp_lib.POST, GRAPHQL_ENDPOINT,
        json=_graphql_user_response(usernames),
        status=200,
        headers={
            "X-RateLimit-Remaining": "4980",
            "X-RateLimit-Reset": str(int(time.time()) + 3600),
        },
    )

    enricher = Enricher(["tok_a"], [], conn, cur, None)
    for name in usernames:
        enricher.add_user(name)

    # The 20th add_user must have triggered an automatic flush
    assert len(resp_lib.calls) == 1


@resp_lib.activate
def test_null_alias_triggers_rest_fallback():
    cur = MagicMock()
    conn = MagicMock()
    cur.fetchone.side_effect = [None, None, ("alice",)]

    # GraphQL returns null for u0
    resp_lib.add(
        resp_lib.POST, GRAPHQL_ENDPOINT,
        json={"data": {"u0": None}},
        status=200,
        headers={
            "X-RateLimit-Remaining": "4999",
            "X-RateLimit-Reset": str(int(time.time()) + 3600),
        },
    )
    # REST fallback returns 200
    resp_lib.add(
        resp_lib.GET, "https://api.github.com/users/alice",
        json={
            "type": "User", "login": "alice", "company": None,
            "location": None, "followers": 0, "public_repos": 0,
        },
        status=200,
        headers={
            "X-RateLimit-Remaining": "4998",
            "X-RateLimit-Reset": str(int(time.time()) + 3600),
        },
    )

    enricher = Enricher(["tok_a"], [], conn, cur, None)
    enricher.add_user("alice")
    enricher.flush(force=True)

    assert len(resp_lib.calls) == 2  # 1 GraphQL + 1 REST


def test_all_tokens_rate_limited_deletes_stubs():
    cur = MagicMock()
    conn = MagicMock()
    cur.fetchone.side_effect = [None, None, ("alice",)]

    enricher = Enricher(["tok_a"], [], conn, cur, None)
    enricher._user_pool.mark_rate_limited("user_pool[0]", time.time() + 3600)
    enricher.add_user("alice")
    enricher.flush(force=True)

    calls = [c[0][0] for c in cur.execute.call_args_list]
    assert any("DELETE FROM users" in sql for sql in calls)


@resp_lib.activate
def test_backward_compat_single_token():
    """Enricher with a single token behaves like the old single-token path."""
    cur = MagicMock()
    conn = MagicMock()
    cur.fetchone.side_effect = [None, None, ("alice",)]

    resp_lib.add(
        resp_lib.POST, GRAPHQL_ENDPOINT,
        json=_graphql_user_response(["alice"]),
        status=200,
        headers={
            "X-RateLimit-Remaining": "4999",
            "X-RateLimit-Reset": str(int(time.time()) + 3600),
        },
    )

    enricher = Enricher(["single_tok"], [], conn, cur, None)
    enricher.add_user("alice")
    enricher.flush(force=True)

    assert len(resp_lib.calls) == 1
    assert "single_tok" in resp_lib.calls[0].request.headers.get("Authorization", "")


@resp_lib.activate
def test_graphql_500_deletes_stubs():
    cur = MagicMock()
    conn = MagicMock()
    cur.fetchone.side_effect = [None, None, ("alice",)]

    resp_lib.add(
        resp_lib.POST, GRAPHQL_ENDPOINT,
        json={"message": "Internal Server Error"},
        status=500,
        headers={"X-RateLimit-Remaining": "5000", "X-RateLimit-Reset": str(int(time.time()) + 3600)},
    )

    enricher = Enricher(["tok_a"], [], conn, cur, None)
    enricher.add_user("alice")
    enricher.flush(force=True)

    # Verify DELETE FROM users was called (stub cleanup)
    calls = [c[0][0] for c in cur.execute.call_args_list]
    assert any("DELETE FROM users" in sql for sql in calls)


@resp_lib.activate
def test_flush_without_force_respects_interval():
    """flush() without force=True should not trigger HTTP call before interval elapses."""
    cur = MagicMock()
    conn = MagicMock()
    cur.fetchone.side_effect = [None, None, ("alice",)]

    enricher = Enricher(["tok_a"], [], conn, cur, None)
    enricher.add_user("alice")
    # Call flush() without force — interval hasn't elapsed yet
    enricher.flush(force=False)

    # No HTTP calls should have been made
    assert len(resp_lib.calls) == 0
