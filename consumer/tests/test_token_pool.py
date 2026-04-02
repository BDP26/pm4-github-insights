"""Unit tests for TokenPool — no network, no DB."""
import time
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))

from unittest.mock import MagicMock
from enricher import TokenPool


def test_single_token_returns_it():
    pool = TokenPool(["tok_a"], "user_pool")
    token, token_id = pool.next_token()
    assert token == "tok_a"
    assert token_id == "user_pool[0]"


def test_round_robin_two_tokens():
    pool = TokenPool(["tok_a", "tok_b"], "user_pool")
    _, id0 = pool.next_token()
    _, id1 = pool.next_token()
    _, id2 = pool.next_token()
    assert id0 == "user_pool[0]"
    assert id1 == "user_pool[1]"
    assert id2 == "user_pool[0]"


def test_rate_limited_token_is_skipped():
    pool = TokenPool(["tok_a", "tok_b"], "user_pool")
    # mark tok_a as rate-limited far in the future
    pool.mark_rate_limited("user_pool[0]", time.time() + 3600)
    token, token_id = pool.next_token()
    assert token == "tok_b"
    assert token_id == "user_pool[1]"


def test_all_rate_limited_returns_none():
    pool = TokenPool(["tok_a", "tok_b"], "user_pool")
    pool.mark_rate_limited("user_pool[0]", time.time() + 3600)
    pool.mark_rate_limited("user_pool[1]", time.time() + 3600)
    token, token_id = pool.next_token()
    assert token is None
    assert token_id is None


def test_update_from_response_marks_exhausted_token():
    pool = TokenPool(["tok_a"], "user_pool")
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(int(time.time()) + 3600),
    }
    pool.update_from_response("user_pool[0]", resp)
    token, _ = pool.next_token()
    assert token is None


def test_update_from_response_on_429():
    pool = TokenPool(["tok_a"], "user_pool")
    resp = MagicMock()
    resp.status_code = 429
    resp.headers = {
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(int(time.time()) + 3600),
    }
    pool.update_from_response("user_pool[0]", resp)
    token, _ = pool.next_token()
    assert token is None


def test_update_from_response_none_is_safe():
    pool = TokenPool(["tok_a"], "user_pool")
    pool.update_from_response("user_pool[0]", None)  # must not raise
    token, _ = pool.next_token()
    assert token == "tok_a"


def test_empty_pool_returns_none():
    pool = TokenPool([], "user_pool")
    token, token_id = pool.next_token()
    assert token is None
    assert token_id is None
