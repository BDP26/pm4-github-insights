"""Unit tests for _write_user, _write_repo, and REST shape mappers."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

from unittest.mock import MagicMock
from enricher import (
    _write_user,
    _write_repo,
    _rest_user_to_graphql_shape,
    _rest_repo_to_graphql_shape,
)


def _mock_db():
    cur = MagicMock()
    conn = MagicMock()
    cur.fetchone.return_value = None
    return cur, conn


def test_write_user_updates_user_row():
    cur, conn = _mock_db()
    node = {
        "__typename": "User", "login": "alice", "company": "ZHAW",
        "location": "Zurich", "followers": {"totalCount": 42},
        "repositories": {"totalCount": 10},
    }
    _write_user(cur, conn, "alice", node)
    cur.execute.assert_called_once()
    sql, params = cur.execute.call_args[0]
    assert "UPDATE users" in sql
    assert "ZHAW" in params
    assert 42 in params
    assert 10 in params
    assert params[-1] == "alice"
    conn.commit.assert_called_once()


def test_write_user_bot_sets_is_bot_true():
    cur, conn = _mock_db()
    node = {"__typename": "Bot", "login": "dependabot"}
    _write_user(cur, conn, "dependabot", node)
    _, params = cur.execute.call_args[0]
    assert params[-2] is True  # is_bot is second-to-last param


def test_write_user_org_inserts_org_deletes_stub():
    cur, conn = _mock_db()
    node = {
        "__typename": "Organization", "login": "my-org",
        "description": "Cool org", "location": "Berlin",
        "repositories": {"totalCount": 5},
    }
    _write_user(cur, conn, "my-org", node)
    calls = [c[0][0] for c in cur.execute.call_args_list]
    assert any("INSERT INTO organizations" in sql for sql in calls)
    assert any("DELETE FROM users" in sql for sql in calls)
    conn.commit.assert_called_once()


def test_write_user_unknown_typename_deletes_stub():
    cur, conn = _mock_db()
    node = {"__typename": "Unknown", "login": "x"}
    _write_user(cur, conn, "x", node)
    _, params = cur.execute.call_args[0]
    assert "x" in params


def test_write_repo_updates_repo_row():
    cur, conn = _mock_db()
    node = {
        "name": "linux", "nameWithOwner": "torvalds/linux",
        "owner": {"login": "torvalds", "__typename": "User"},
        "description": "Linux kernel", "primaryLanguage": {"name": "C"},
        "licenseInfo": {"spdxId": "GPL-2.0"},
        "repositoryTopics": {"nodes": [{"topic": {"name": "os"}}]},
        "stargazerCount": 100000, "forkCount": 5000,
        "watchers": {"totalCount": 3000}, "hasIssuesEnabled": True,
        "issues": {"totalCount": 200}, "hasProjectsEnabled": False,
        "isArchived": False, "isDisabled": False,
        "homepageUrl": "https://kernel.org", "diskUsage": 5000000,
        "createdAt": "2011-09-04T22:14:16Z", "pushedAt": "2026-04-01T10:00:00Z",
    }
    _write_repo(cur, conn, 123, node)
    sql, params = cur.execute.call_args[0]
    assert "UPDATE repos" in sql
    assert "torvalds/linux" in params
    assert ["os"] in params
    assert params[-1] == 123
    conn.commit.assert_called_once()


def test_write_repo_handles_null_optional_fields():
    cur, conn = _mock_db()
    node = {
        "name": "empty", "nameWithOwner": "user/empty",
        "owner": {"login": "user", "__typename": "User"},
        "description": None, "primaryLanguage": None, "licenseInfo": None,
        "repositoryTopics": {"nodes": []}, "stargazerCount": 0, "forkCount": 0,
        "watchers": {"totalCount": 0}, "hasIssuesEnabled": False,
        "issues": {"totalCount": 0}, "hasProjectsEnabled": False,
        "isArchived": False, "isDisabled": False, "homepageUrl": None,
        "diskUsage": 0, "createdAt": "2020-01-01T00:00:00Z",
        "pushedAt": "2020-01-01T00:00:00Z",
    }
    _write_repo(cur, conn, 99, node)  # must not raise
    conn.commit.assert_called_once()


def test_rest_user_to_graphql_shape_user():
    data = {
        "type": "User", "login": "alice", "company": "ZHAW",
        "location": "Zurich", "followers": 42, "public_repos": 10,
    }
    shape = _rest_user_to_graphql_shape(data)
    assert shape["__typename"] == "User"
    assert shape["followers"]["totalCount"] == 42
    assert shape["repositories"]["totalCount"] == 10


def test_rest_user_to_graphql_shape_org():
    data = {"type": "Organization", "login": "my-org", "description": "Org", "location": None}
    shape = _rest_user_to_graphql_shape(data)
    assert shape["__typename"] == "Organization"


def test_rest_user_to_graphql_shape_bot():
    data = {"type": "Bot", "login": "bot[bot]"}
    shape = _rest_user_to_graphql_shape(data)
    assert shape["__typename"] == "Bot"


def test_rest_repo_to_graphql_shape_maps_fields():
    data = {
        "name": "linux", "full_name": "torvalds/linux",
        "owner": {"login": "torvalds", "type": "User"},
        "description": "kernel", "language": "C",
        "license": {"spdx_id": "GPL-2.0"}, "topics": ["os", "kernel"],
        "stargazers_count": 100, "forks_count": 50, "watchers_count": 30,
        "has_issues": True, "open_issues_count": 5, "has_projects": False,
        "archived": False, "disabled": False, "homepage": None, "size": 1000,
        "created_at": "2011-09-04T22:14:16Z", "pushed_at": "2026-04-01T10:00:00Z",
    }
    shape = _rest_repo_to_graphql_shape(data)
    assert shape["nameWithOwner"] == "torvalds/linux"
    assert shape["primaryLanguage"]["name"] == "C"
    assert shape["repositoryTopics"]["nodes"][0]["topic"]["name"] == "os"
    assert shape["stargazerCount"] == 100
