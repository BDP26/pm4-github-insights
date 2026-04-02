"""Unit tests for db_writer.py message routing and DB inserts."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'db-writer'))

import json
from unittest.mock import MagicMock, patch, call
import pytest


def make_cursor():
    return MagicMock()


def make_conn():
    return MagicMock()


# ── github.events.status → request_logs ──────────────────────────

def test_status_message_inserts_into_request_logs():
    from db_writer import handle_status_message
    cur = make_cursor()
    conn = make_conn()
    payload = {
        "request_success": True,
        "sent_at": "2026-03-27T08:26:05+00:00",
        "received_at": "2026-03-27T08:26:05+00:00",
        "elapsed_s": 0.3,
        "method": "GET",
        "url": "https://api.github.com/events",
        "status_code": 200,
        "reason": "OK",
        "response_bytes": 1024,
        "redirects": 0,
        "final_url": "https://api.github.com/events",
        "http_version": 11,
        "encoding": "utf-8",
        "request_headers": {},
        "response_headers": {},
        "error": None,
    }
    handle_status_message(cur, conn, payload)
    sql_calls = [str(c) for c in cur.execute.call_args_list]
    assert any("INSERT INTO request_logs" in s for s in sql_calls)
    conn.commit.assert_called_once()


# ── github.ratelimit → rate_limit_snapshots ───────────────────────

def test_ratelimit_message_inserts_into_rate_limit_snapshots():
    from db_writer import handle_ratelimit_message
    cur = make_cursor()
    conn = make_conn()
    payload = {
        "source": "producer",
        "resource": "core",
        "limit": 5000,
        "used": 729,
        "remaining": 4271,
        "reset_at": "2026-03-27T09:17:40+00:00",
        "recorded_at": "2026-03-27T08:26:05+00:00",
    }
    handle_ratelimit_message(cur, conn, payload)
    sql_calls = [str(c) for c in cur.execute.call_args_list]
    assert any("INSERT INTO rate_limit_snapshots" in s for s in sql_calls)
    conn.commit.assert_called_once()


def test_ratelimit_message_stores_correct_remaining():
    from db_writer import handle_ratelimit_message
    cur = make_cursor()
    conn = make_conn()
    payload = {
        "source": "consumer",
        "resource": "core",
        "limit": 5000,
        "used": 100,
        "remaining": 4900,
        "reset_at": None,
        "recorded_at": "2026-03-27T08:26:05+00:00",
    }
    handle_ratelimit_message(cur, conn, payload)
    # Inspect the params passed to the INSERT
    insert_calls = [c for c in cur.execute.call_args_list
                    if "INSERT INTO rate_limit_snapshots" in str(c)]
    assert insert_calls, "No INSERT INTO rate_limit_snapshots found"
    params = insert_calls[0][0][1]  # positional args[1] = params tuple
    assert 4900 in params, f"remaining=4900 must be in INSERT params, got: {params}"


# ── error handling ────────────────────────────────────────────────

def test_status_message_rollback_on_db_error():
    from db_writer import handle_status_message
    cur = make_cursor()
    conn = make_conn()
    cur.execute.side_effect = Exception("DB error")
    payload = {"request_success": True, "sent_at": None, "received_at": None,
               "elapsed_s": None, "method": "GET", "url": "x", "status_code": 200,
               "reason": "OK", "response_bytes": 0, "redirects": 0,
               "final_url": "x", "http_version": 11, "encoding": "utf-8",
               "request_headers": {}, "response_headers": {}, "error": None}
    handle_status_message(cur, conn, payload)  # must not raise
    conn.rollback.assert_called_once()
