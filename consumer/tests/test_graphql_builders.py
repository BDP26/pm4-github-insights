"""Unit tests for GraphQL query builder functions."""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

from enricher import build_user_query, build_repo_query


def test_user_query_single():
    q = build_user_query(["alice"])
    assert "u0: repositoryOwner" in q
    assert 'login: "alice"' in q
    assert "... on User" in q
    assert "... on Organization" in q
    assert "followers" in q


def test_user_query_multiple():
    q = build_user_query(["alice", "bob", "carol"])
    assert "u0: repositoryOwner" in q
    assert "u1: repositoryOwner" in q
    assert "u2: repositoryOwner" in q
    assert "u3:" not in q


def test_user_query_escapes_quotes():
    q = build_user_query(['bad"name'])
    assert '"bad\\"name"' in q


def test_user_query_empty_list():
    q = build_user_query([])
    assert "query BatchUsers" in q
    assert "repositoryOwner" not in q


def test_repo_query_single():
    q = build_repo_query([(123, "torvalds/linux")])
    assert "r0: repository" in q
    assert 'owner: "torvalds"' in q
    assert 'name: "linux"' in q
    assert "stargazerCount" in q
    assert "nameWithOwner" in q


def test_repo_query_multiple():
    repos = [(1, "a/b"), (2, "c/d"), (3, "e/f")]
    q = build_repo_query(repos)
    assert "r0: repository" in q
    assert "r1: repository" in q
    assert "r2: repository" in q
    assert "r3:" not in q


def test_repo_query_full_name_with_slash():
    """owner/name split must handle exactly one slash."""
    q = build_repo_query([(42, "my-org/my-repo")])
    assert 'owner: "my-org"' in q
    assert 'name: "my-repo"' in q
