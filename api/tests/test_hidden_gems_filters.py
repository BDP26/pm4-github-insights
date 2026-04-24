"""Tests for hidden gems filter endpoints (languages, licenses, topics)."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from routers.hidden_gems import router, _filter_cache


@pytest.fixture(autouse=True)
def clear_filter_cache():
    _filter_cache.clear()
    yield
    _filter_cache.clear()


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


def _row(value: str):
    r = MagicMock()
    r.__getitem__ = lambda self, k: value
    return r


def test_get_languages_returns_list_of_strings(client):
    test_client, conn = client
    conn.fetch.return_value = [_row("Python"), _row("TypeScript")]
    resp = test_client.get("/api/hidden-gems/filters/languages")
    assert resp.status_code == 200
    assert resp.json() == ["Python", "TypeScript"]


def test_get_languages_forwards_hours(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/hidden-gems/filters/languages?hours=24")
    _sql, hours_arg, limit_arg = conn.fetch.call_args[0]
    assert hours_arg == 24


def test_get_languages_default_limit_is_50(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/hidden-gems/filters/languages")
    _sql, _hours, limit_arg = conn.fetch.call_args[0]
    assert limit_arg == 50


def test_get_languages_custom_limit(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/hidden-gems/filters/languages?limit=10")
    _sql, _hours, limit_arg = conn.fetch.call_args[0]
    assert limit_arg == 10


def test_get_languages_cache_hit_skips_db(client):
    test_client, conn = client
    conn.fetch.return_value = [_row("Go")]
    test_client.get("/api/hidden-gems/filters/languages")
    test_client.get("/api/hidden-gems/filters/languages")
    assert conn.fetch.call_count == 1


def test_get_licenses_default_limit_is_20(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/hidden-gems/filters/licenses")
    _sql, _hours, limit_arg = conn.fetch.call_args[0]
    assert limit_arg == 20


def test_get_topics_default_limit_is_200(client):
    test_client, conn = client
    conn.fetch.return_value = []
    test_client.get("/api/hidden-gems/filters/topics")
    _sql, _hours, limit_arg = conn.fetch.call_args[0]
    assert limit_arg == 200
