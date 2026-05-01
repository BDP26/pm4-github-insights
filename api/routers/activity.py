"""
Activity Router
───────────────
GET /api/overview/heatmap?weeks=52
GET /api/overview/globe-heatmap?weeks=52&limit=250
GET /api/activity/leaderboard?scope=repos|users|orgs&limit=20
"""

import asyncio
from typing import Any, Literal, Optional

import asyncpg
from cachetools import TTLCache
from fastapi import APIRouter, Query, Request

_heatmap_cache: TTLCache = TTLCache(maxsize=32, ttl=300)  # 5 min
_globe_heatmap_cache: TTLCache = TTLCache(maxsize=32, ttl=300)  # 5 min
_leaderboard_cache: TTLCache = TTLCache(maxsize=64, ttl=60)  # 60 s

# ── Cache stampede locks (double-checked locking) ──────────────────────────────
_heatmap_lock: asyncio.Lock = asyncio.Lock()
_globe_heatmap_lock: asyncio.Lock = asyncio.Lock()
_leaderboard_lock: asyncio.Lock = asyncio.Lock()

router = APIRouter(tags=["activity"])

_HEATMAP_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
]


def _pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


# ── Heatmap ───────────────────────────────────────────────────────────────────

@router.get("/api/overview/heatmap")
async def get_heatmap(
    request: Request,
    weeks: int = Query(52, ge=1, le=104),
) -> list[dict[str, Any]]:
    cache_key = ("heatmap", weeks)
    if cache_key in _heatmap_cache:
        return _heatmap_cache[cache_key]

    async with _heatmap_lock:
        if cache_key in _heatmap_cache:  # double-check after acquiring lock
            return _heatmap_cache[cache_key]
        async with _pool(request).acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    time_bucket('1 week', time)::date AS date,
                    event_type,
                    COUNT(*)::int                     AS count
                FROM events
                WHERE time >= NOW() - make_interval(weeks => $1::int)
                  AND event_type = ANY($2::text[])
                GROUP BY date, event_type
                ORDER BY date, event_type
                """,
                weeks,
                _HEATMAP_EVENT_TYPES,
            )

        result = [
            {"date": str(r["date"]), "event_type": r["event_type"], "count": r["count"]}
            for r in rows
        ]
        _heatmap_cache[cache_key] = result
    return result


# ── Globe heatmap ────────────────────────────────────────────────────────────

@router.get("/api/overview/globe-heatmap")
async def get_globe_heatmap(
    request: Request,
    hours: Optional[int] = Query(None, ge=1, le=87600),
    limit: int = Query(2000, ge=1, le=5000),
) -> list[dict[str, Any]]:
    cache_key = ("globe_heatmap", hours, limit)
    if cache_key in _globe_heatmap_cache:
        return _globe_heatmap_cache[cache_key]

    async with _globe_heatmap_lock:
        if cache_key in _globe_heatmap_cache:
            return _globe_heatmap_cache[cache_key]
        async with _pool(request).acquire() as conn:
            if hours is not None:
                rows = await conn.fetch(
                    """
                    SELECT
                        u.lat,
                        u.lng,
                        u.country,
                        COUNT(*)::int AS count
                    FROM events e
                    JOIN users u ON e.actor_username = u.username
                    WHERE e.time >= NOW() - make_interval(hours => $1::int)
                      AND u.lat IS NOT NULL
                      AND u.lng IS NOT NULL
                      AND u.is_bot = FALSE
                    GROUP BY u.lat, u.lng, u.country
                    ORDER BY count DESC, u.country NULLS LAST, u.lat, u.lng
                    LIMIT $2
                    """,
                    hours,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT
                        u.lat,
                        u.lng,
                        u.country,
                        COUNT(*)::int AS count
                    FROM events e
                    JOIN users u ON e.actor_username = u.username
                    WHERE u.lat IS NOT NULL
                      AND u.lng IS NOT NULL
                      AND u.is_bot = FALSE
                    GROUP BY u.lat, u.lng, u.country
                    ORDER BY count DESC, u.country NULLS LAST, u.lat, u.lng
                    LIMIT $1
                    """,
                    limit,
                )

        result = [
            {
                "lat": float(r["lat"]),
                "lng": float(r["lng"]),
                "country": r["country"],
                "count": r["count"],
            }
            for r in rows
        ]
        _globe_heatmap_cache[cache_key] = result
    return result


# ── Leaderboard ───────────────────────────────────────────────────────────────

@router.get("/api/activity/leaderboard")
async def get_leaderboard(
    request: Request,
    scope: Literal["repos", "users", "orgs"] = Query("repos"),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    cache_key = (scope, limit)
    if cache_key in _leaderboard_cache:
        return _leaderboard_cache[cache_key]

    async with _leaderboard_lock:
        if cache_key in _leaderboard_cache:  # double-check after acquiring lock
            return _leaderboard_cache[cache_key]
        async with _pool(request).acquire() as conn:
            if scope == "users":
                rows = await conn.fetch(
                    """
                    WITH config AS (
                        SELECT 1.0::FLOAT AS star_weight,
                               2.0::FLOAT AS fork_weight
                    ),
                    user_portfolio AS (
                        SELECT
                            owner_login,
                            COUNT(repo_id)                      AS total_repos,
                            COALESCE(SUM(stargazers_count), 0)  AS total_stars,
                            COALESCE(SUM(forks_count),      0)  AS total_forks
                        FROM repos
                        WHERE owner_type = 'User'
                        GROUP BY owner_login
                    )
                    SELECT
                        u.username,
                        u.location,
                        p.total_repos,
                        p.total_stars,
                        p.total_forks,
                        (p.total_stars * cfg.star_weight
                         + p.total_forks * cfg.fork_weight)  AS impact_score
                    FROM user_portfolio p
                    JOIN users u ON p.owner_login = u.username
                    CROSS JOIN config cfg
                    ORDER BY impact_score DESC NULLS LAST
                    LIMIT $1
                    """,
                    limit,
                )
                result: dict[str, Any] = {
                    "scope": "users",
                    "items": [dict(r) for r in rows],
                }

            elif scope == "orgs":
                rows = await conn.fetch(
                    """
                    WITH config AS (
                        SELECT 1.0::FLOAT AS star_weight,
                               2.0::FLOAT AS fork_weight
                    ),
                    org_portfolio AS (
                        SELECT
                            owner_login,
                            COUNT(repo_id)                      AS total_repos,
                            COALESCE(SUM(stargazers_count), 0)  AS total_stars,
                            COALESCE(SUM(forks_count),      0)  AS total_forks
                        FROM repos
                        WHERE owner_type = 'Organization'
                        GROUP BY owner_login
                    )
                    SELECT
                        p.owner_login  AS org_login,
                        p.total_repos,
                        p.total_stars,
                        p.total_forks,
                        (p.total_stars * cfg.star_weight
                         + p.total_forks * cfg.fork_weight)  AS impact_score
                    FROM org_portfolio p
                    CROSS JOIN config cfg
                    ORDER BY impact_score DESC NULLS LAST
                    LIMIT $1
                    """,
                    limit,
                )
                result = {
                    "scope": "orgs",
                    "items": [dict(r) for r in rows],
                }

            else:  # repos
                rows = await conn.fetch(
                    """
                    SELECT
                        r.repo_id,
                        r.full_name,
                        r.owner_login,
                        r.language,
                        r.stargazers_count                          AS total_stars,
                        r.forks_count                               AS total_forks,
                        (r.stargazers_count * 1.0
                         + r.forks_count * 2.0)                    AS impact_score
                    FROM repos r
                    WHERE r.stargazers_count > 0 OR r.forks_count > 0
                    ORDER BY impact_score DESC NULLS LAST
                    LIMIT $1
                    """,
                    limit,
                )
                result = {
                    "scope": "repos",
                    "items": [dict(r) for r in rows],
                }

        _leaderboard_cache[cache_key] = result
    return result
