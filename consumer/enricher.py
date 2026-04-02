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


# ── GraphQL Query Builders ───────────────────────────────────────

def build_user_query(usernames: list[str]) -> str:
    """Build a GraphQL alias query for up to GRAPHQL_BATCH_SIZE users."""
    aliases = []
    for i, login in enumerate(usernames):
        escaped = login.replace("\\", "\\\\").replace('"', '\\"')
        aliases.append(f"""
  u{i}: repositoryOwner(login: "{escaped}") {{
    login
    __typename
    ... on User {{
      databaseId
      company
      location
      followers {{ totalCount }}
      repositories(privacy: PUBLIC) {{ totalCount }}
    }}
    ... on Organization {{
      databaseId
      description
      location
      repositories {{ totalCount }}
    }}
  }}""")
    return "query BatchUsers {" + "".join(aliases) + "\n}"


def build_repo_query(repos: list[tuple[int, str]]) -> str:
    """Build a GraphQL alias query for up to GRAPHQL_BATCH_SIZE repos.

    repos: list of (repo_id, full_name) where full_name is 'owner/name'.
    """
    aliases = []
    for i, (repo_id, full_name) in enumerate(repos):
        owner, _, name = full_name.partition("/")
        owner_esc = owner.replace("\\", "\\\\").replace('"', '\\"')
        name_esc = name.replace("\\", "\\\\").replace('"', '\\"')
        aliases.append(f"""
  r{i}: repository(owner: "{owner_esc}", name: "{name_esc}") {{
    databaseId
    name
    nameWithOwner
    description
    primaryLanguage {{ name }}
    licenseInfo {{ spdxId }}
    repositoryTopics(first: 10) {{ nodes {{ topic {{ name }} }} }}
    stargazerCount
    forkCount
    watchers {{ totalCount }}
    hasIssuesEnabled
    issues(states: OPEN) {{ totalCount }}
    hasProjectsEnabled
    isArchived
    isDisabled
    homepageUrl
    diskUsage
    createdAt
    pushedAt
    owner {{ login __typename }}
  }}""")
    return "query BatchRepos {" + "".join(aliases) + "\n}"


# ── DB Write Helpers ─────────────────────────────────────────────

def _write_user(cur, conn, username: str, node: dict) -> None:
    """Write a GraphQL repositoryOwner node to the DB."""
    typename = node.get("__typename")
    if typename in ("User", "Bot"):
        cur.execute("""
            UPDATE users
            SET fetched_at   = NOW(),
                company      = %s,
                location     = %s,
                public_repos = %s,
                followers    = %s,
                is_bot       = %s
            WHERE username = %s
        """, (
            node.get("company"),
            node.get("location"),
            (node.get("repositories") or {}).get("totalCount"),
            (node.get("followers") or {}).get("totalCount"),
            typename == "Bot",
            username,
        ))
        conn.commit()
    elif typename == "Organization":
        cur.execute("""
            INSERT INTO organizations
                (login, fetched_at, description, location, public_repos)
            VALUES (%s, NOW(), %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            node.get("login", username),
            node.get("description"),
            node.get("location"),
            (node.get("repositories") or {}).get("totalCount"),
        ))
        cur.execute(
            "DELETE FROM users WHERE username = %s AND fetched_at IS NULL",
            (username,),
        )
        conn.commit()
    else:
        log.warning("Unexpected __typename %r for %s; deleting stub", typename, username)
        _delete_user_stub(cur, conn, username)


def _write_repo(cur, conn, repo_id: int, node: dict) -> None:
    """Write a GraphQL repository node to the DB."""
    topics = [
        t["topic"]["name"]
        for t in (node.get("repositoryTopics") or {}).get("nodes", [])
    ]
    cur.execute("""
        UPDATE repos
        SET fetched_at        = NOW(),
            name              = %s,
            full_name         = %s,
            owner_login       = %s,
            owner_type        = %s,
            description       = %s,
            language          = %s,
            license_spdx      = %s,
            topics            = %s,
            stargazers_count  = %s,
            forks_count       = %s,
            watchers_count    = %s,
            has_issues        = %s,
            open_issues_count = %s,
            has_projects      = %s,
            archived          = %s,
            disabled          = %s,
            homepage          = %s,
            size              = %s,
            created_at        = %s,
            pushed_at         = %s
        WHERE repo_id = %s
    """, (
        node.get("name"),
        node.get("nameWithOwner"),
        (node.get("owner") or {}).get("login"),
        (node.get("owner") or {}).get("__typename"),
        node.get("description"),
        (node.get("primaryLanguage") or {}).get("name"),
        (node.get("licenseInfo") or {}).get("spdxId"),
        topics,
        node.get("stargazerCount"),
        node.get("forkCount"),
        (node.get("watchers") or {}).get("totalCount"),
        node.get("hasIssuesEnabled"),
        (node.get("issues") or {}).get("totalCount"),
        node.get("hasProjectsEnabled"),
        node.get("isArchived"),
        node.get("isDisabled"),
        node.get("homepageUrl"),
        node.get("diskUsage"),
        node.get("createdAt"),
        node.get("pushedAt"),
        repo_id,
    ))
    conn.commit()


def _delete_user_stub(cur, conn, username: str) -> None:
    try:
        cur.execute(
            "DELETE FROM users WHERE username = %s AND fetched_at IS NULL",
            (username,),
        )
        conn.commit()
    except Exception as e:
        log.warning("Could not clean up user stub for %s: %s", username, e)


def _delete_repo_stub(cur, conn, repo_id: int) -> None:
    try:
        cur.execute(
            "DELETE FROM repos WHERE repo_id = %s AND fetched_at IS NULL",
            (repo_id,),
        )
        conn.commit()
    except Exception as e:
        log.warning("Could not clean up repo stub for %s: %s", repo_id, e)


# ── REST Shape Adapters ──────────────────────────────────────────

def _rest_user_to_graphql_shape(data: dict) -> dict:
    """Map REST /users/:login response to the shape _write_user expects."""
    typename = data.get("type", "User")
    if typename == "Bot":
        return {"__typename": "Bot", "login": data.get("login")}
    return {
        "__typename": "User" if typename == "User" else "Organization",
        "login": data.get("login"),
        "company": data.get("company"),
        "location": data.get("location"),
        "followers": {"totalCount": data.get("followers", 0)},
        "repositories": {"totalCount": data.get("public_repos", 0)},
        "description": data.get("description"),
    }


def _rest_repo_to_graphql_shape(d: dict) -> dict:
    """Map REST /repos/:owner/:name response to the shape _write_repo expects."""
    return {
        "name": d.get("name"),
        "nameWithOwner": d.get("full_name"),
        "owner": {
            "login": (d.get("owner") or {}).get("login"),
            "__typename": (d.get("owner") or {}).get("type"),
        },
        "description": d.get("description"),
        "primaryLanguage": {"name": d["language"]} if d.get("language") else None,
        "licenseInfo": {"spdxId": (d["license"] or {}).get("spdx_id")} if d.get("license") else None,
        "repositoryTopics": {
            "nodes": [{"topic": {"name": t}} for t in d.get("topics", [])]
        },
        "stargazerCount": d.get("stargazers_count"),
        "forkCount": d.get("forks_count"),
        "watchers": {"totalCount": d.get("watchers_count")},
        "hasIssuesEnabled": d.get("has_issues"),
        "issues": {"totalCount": d.get("open_issues_count")},
        "hasProjectsEnabled": d.get("has_projects"),
        "isArchived": d.get("archived"),
        "isDisabled": d.get("disabled"),
        "homepageUrl": d.get("homepage"),
        "diskUsage": d.get("size"),
        "createdAt": d.get("created_at"),
        "pushedAt": d.get("pushed_at"),
    }


# ── Auth & Redaction ─────────────────────────────────────────────

def _auth_headers(token: str) -> dict:
    return {
        "Authorization": f"bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "ZHAW-Explorer/2.0",
    }


def _redact_headers(headers: dict) -> dict:
    sensitive = {
        "authorization", "proxy-authorization", "cookie",
        "set-cookie", "x-api-key", "x-api-token", "x-auth-token", "x-access-token",
    }
    return {
        k: "[REDACTED]" if isinstance(k, str) and k.lower() in sensitive else v
        for k, v in headers.items()
    }


# ── Kafka Publishing ─────────────────────────────────────────────

def _publish_ratelimit(kafka_producer, headers: dict, token_id: Optional[str]) -> None:
    if kafka_producer is None:
        return
    remaining = headers.get("X-RateLimit-Remaining")
    if remaining is None:
        return
    reset_ts = headers.get("X-RateLimit-Reset")
    payload = {
        "source": "consumer",
        "resource": headers.get("X-RateLimit-Resource", "core"),
        "limit": int(headers.get("X-RateLimit-Limit", 0)),
        "used": int(headers.get("X-RateLimit-Used", 0)),
        "remaining": int(remaining),
        "reset_at": (
            datetime.fromtimestamp(int(reset_ts), tz=timezone.utc).isoformat()
            if reset_ts else None
        ),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "token_id": token_id,
    }
    try:
        kafka_producer.produce(
            topic=TOPIC_RATELIMIT,
            key=f"ratelimit-{int(time.time())}",
            value=json.dumps(payload),
        )
        kafka_producer.poll(0)
    except Exception as e:
        log.warning("Failed to publish rate limit: %s", e)


def _publish_status(kafka_producer, meta: dict) -> None:
    if kafka_producer is None:
        return
    try:
        kafka_producer.produce(
            topic=TOPIC_STATUS,
            key=f"status-{int(time.time())}",
            value=json.dumps(meta),
        )
        kafka_producer.poll(0)
    except Exception as e:
        log.warning("Failed to publish request status: %s", e)


# ── HTTP Request Logger ──────────────────────────────────────────

def logged_request(
    cur, conn, method: str, url: str,
    *,
    token_id: Optional[str],
    request_type: str,
    batch_size: Optional[int],
    kafka_producer,
    **kwargs,
):
    """Perform an HTTP request and publish status + rate-limit events to Kafka."""
    sent_at = datetime.now(timezone.utc)
    try:
        r = requests.request(method, url, **kwargs)
        received_at = datetime.now(timezone.utc)
        meta = {
            "request_success": r.ok,
            "sent_at": sent_at.isoformat(),
            "received_at": received_at.isoformat(),
            "elapsed_s": r.elapsed.total_seconds(),
            "method": r.request.method,
            "url": r.request.url,
            "request_headers": _redact_headers(dict(r.request.headers)),
            "status_code": r.status_code,
            "reason": r.reason,
            "response_bytes": len(r.content),
            "response_headers": _redact_headers(dict(r.headers)),
            "redirects": len(r.history),
            "final_url": r.url,
            "http_version": r.raw.version,
            "encoding": r.encoding,
            "request_type": request_type,
            "batch_size": batch_size,
            "token_id": token_id,
        }
        _publish_status(kafka_producer, meta)
        _publish_ratelimit(kafka_producer, dict(r.headers), token_id)
        return r, meta
    except requests.RequestException as exc:
        error_meta = {
            "request_success": False,
            "sent_at": sent_at.isoformat(),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
            "method": method,
            "url": url,
            "status_code": None,
            "reason": None,
            "response_bytes": None,
            "response_headers": None,
            "redirects": None,
            "final_url": None,
            "http_version": None,
            "encoding": None,
            "request_type": request_type,
            "batch_size": batch_size,
            "token_id": token_id,
        }
        log.warning("GitHub API request failed: %s", exc)
        return None, error_meta


# ── REST Fallback Functions ──────────────────────────────────────

def _rest_fallback_user(cur, conn, username: str, pool: "TokenPool", kafka_producer) -> None:
    """Single-item REST fallback for a user alias that returned null in GraphQL."""
    token, token_id = pool.next_token()
    if token is None:
        _delete_user_stub(cur, conn, username)
        return
    r, _ = logged_request(
        cur, conn, "GET",
        f"https://api.github.com/users/{username}",
        headers=_auth_headers(token),
        token_id=token_id,
        request_type="rest",
        batch_size=None,
        kafka_producer=kafka_producer,
        timeout=5,
    )
    pool.update_from_response(token_id, r)
    if r is None:
        _delete_user_stub(cur, conn, username)
        return
    if r.status_code == 404:
        cur.execute("UPDATE users SET fetched_at = NOW() WHERE username = %s", (username,))
        conn.commit()
        return
    if r.status_code == 200:
        _write_user(cur, conn, username, _rest_user_to_graphql_shape(r.json()))
    else:
        log.warning("REST fallback user %s: %s %s", username, r.status_code, r.reason)
        _delete_user_stub(cur, conn, username)


def _rest_fallback_repo(cur, conn, repo_id: int, full_name: str, pool: "TokenPool", kafka_producer) -> None:
    """Single-item REST fallback for a repo alias that returned null in GraphQL."""
    token, token_id = pool.next_token()
    if token is None:
        _delete_repo_stub(cur, conn, repo_id)
        return
    r, _ = logged_request(
        cur, conn, "GET",
        f"https://api.github.com/repos/{full_name}",
        headers=_auth_headers(token),
        token_id=token_id,
        request_type="rest",
        batch_size=None,
        kafka_producer=kafka_producer,
        timeout=5,
    )
    pool.update_from_response(token_id, r)
    if r is None:
        _delete_repo_stub(cur, conn, repo_id)
        return
    if r.status_code == 200:
        _write_repo(cur, conn, repo_id, _rest_repo_to_graphql_shape(r.json()))
    else:
        log.warning("REST fallback repo %s: %s %s", full_name, r.status_code, r.reason)
        _delete_repo_stub(cur, conn, repo_id)
