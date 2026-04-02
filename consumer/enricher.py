"""GitHub enrichment: token pools, GraphQL batching, REST fallback, logging."""
# NOTE: json, time, requests used by GraphQL/REST functions added in later tasks
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────
GRAPHQL_BATCH_SIZE = 20
BATCH_FLUSH_INTERVAL_S = 2.0
GRAPHQL_ENDPOINT = "https://api.github.com/graphql"
TOPIC_STATUS = "github.events.status"
TOPIC_RATELIMIT = "github.ratelimit"


# ── TokenPool ────────────────────────────────────────────────────

class TokenPool:
    """Round-robin token pool that skips rate-limited tokens."""

    def __init__(self, tokens: list[str], pool_name: str):
        self._tokens = tokens
        self._name = pool_name
        self._index = 0
        self._reset_times: dict[int, float] = {}

    def next_token(self) -> tuple[Optional[str], Optional[str]]:
        """Return (token, token_id) for the next available token.

        Returns (None, None) if the pool is empty or all tokens are rate-limited.
        """
        if not self._tokens:
            return None, None
        now = datetime.now(timezone.utc).timestamp()
        for _ in range(len(self._tokens)):
            i = self._index % len(self._tokens)
            self._index += 1
            if self._reset_times.get(i, 0) <= now:
                return self._tokens[i], f"{self._name}[{i}]"
        return None, None

    def mark_rate_limited(self, token_id: str, reset_at: float) -> None:
        """Mark token as unavailable until reset_at (UTC timestamp)."""
        try:
            idx = int(token_id.split("[")[1].rstrip("]"))
        except (IndexError, ValueError):
            log.error("Invalid token_id format: %s", token_id)
            return
        self._reset_times[idx] = reset_at

    def update_from_response(self, token_id: str, response: Optional["requests.Response"]) -> None:
        """Parse rate-limit headers and mark token if exhausted or 429/403."""
        if response is None:
            return
        reset_ts = response.headers.get("X-RateLimit-Reset")
        remaining = response.headers.get("X-RateLimit-Remaining")
        if response.status_code in (403, 429) and reset_ts:
            self.mark_rate_limited(token_id, int(reset_ts))
        elif remaining is not None and int(remaining) == 0 and reset_ts:
            self.mark_rate_limited(token_id, int(reset_ts))
