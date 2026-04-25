"""Tests for the activity router (heatmap + leaderboard)."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from routers.activity import router, _leaderboard_cache, _heatmap_cache


@pytest.fixture(autouse=True)
def clear_caches():
    _leaderboard_cache.clear()
    _heatmap_cache.clear()
    yield
    _leaderboard_cache.clear()
    _heatmap_cache.clear()


@pytest.fixture
def mock_conn():
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])
    return conn


@pytest.fixture
def client(mock_conn):
    app = FastAPI()
    app.include_router(router)
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire.return_value = acquire_ctx
    app.state.pool = pool
    return TestClient(app), mock_conn


# ── /api/overview/heatmap ─────────────────────────────────────────────────────

def test_heatmap_returns_list(client):
    test_client, conn = client
    from datetime import date
    conn.fetch.return_value = [
        {"date": date(2026, 4, 1), "event_type": "PushEvent", "count": 42}
    ]
    resp = test_client.get("/api/overview/heatmap")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert data[0]["event_type"] == "PushEvent"
    assert data[0]["count"] == 42


def test_heatmap_default_weeks_is_52(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/overview/heatmap")
    _sql, weeks_arg, _types = conn.fetch.call_args[0]
    assert weeks_arg == 52


def test_heatmap_cache_hit_skips_db(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/overview/heatmap")
    test_client.get("/api/overview/heatmap")
    assert conn.fetch.call_count == 1


# ── /api/activity/leaderboard ─────────────────────────────────────────────────

def test_leaderboard_repos_returns_scope_and_items(client):
    test_client, conn = client
    conn.fetch.return_value = [
        {
            "repo_id": 1, "full_name": "a/b", "owner_login": "a",
            "language": "Python", "total_stars": 100, "total_forks": 10,
            "impact_score": 120.0,
        }
    ]
    resp = test_client.get("/api/activity/leaderboard?scope=repos")
    assert resp.status_code == 200
    body = resp.json()
    assert body["scope"] == "repos"
    assert len(body["items"]) == 1
    assert body["items"][0]["full_name"] == "a/b"


def test_leaderboard_users_returns_scope(client):
    test_client, conn = client
    conn.fetch.return_value = [
        {
            "username": "torvalds", "location": "Portland",
            "total_repos": 5, "total_stars": 1000, "total_forks": 200,
            "impact_score": 1400.0,
        }
    ]
    resp = test_client.get("/api/activity/leaderboard?scope=users")
    assert resp.status_code == 200
    body = resp.json()
    assert body["scope"] == "users"
    assert body["items"][0]["username"] == "torvalds"
    assert body["items"][0]["location"] == "Portland"


def test_leaderboard_orgs_returns_scope(client):
    test_client, conn = client
    conn.fetch.return_value = [
        {
            "org_login": "microsoft", "total_repos": 200,
            "total_stars": 50000, "total_forks": 8000,
            "impact_score": 66000.0,
        }
    ]
    resp = test_client.get("/api/activity/leaderboard?scope=orgs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["scope"] == "orgs"
    assert body["items"][0]["org_login"] == "microsoft"


def test_leaderboard_invalid_scope_returns_422(client):
    test_client, _ = client
    resp = test_client.get("/api/activity/leaderboard?scope=invalid")
    assert resp.status_code == 422


def test_leaderboard_cache_hit_skips_db(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/activity/leaderboard?scope=repos")
    test_client.get("/api/activity/leaderboard?scope=repos")
    assert conn.fetch.call_count == 1
