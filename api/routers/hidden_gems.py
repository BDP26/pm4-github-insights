"""
Hidden Gems Router
──────────────────
All /api/hidden-gems/* endpoints.
"""
import asyncio
import logging
from typing import Any

import asyncpg
from cachetools import TTLCache
from fastapi import APIRouter, HTTPException, Query, Request

# ── Caches ────────────────────────────────────────────────────────────────────
_filter_cache: TTLCache = TTLCache(maxsize=128, ttl=300)   # 5 minutes
_live_cache:   TTLCache = TTLCache(maxsize=256, ttl=30)    # 30 seconds — wired in get_live

# ── Filter locks (one per endpoint — double-checked locking) ──────────────────
_filter_lock_languages = asyncio.Lock()
_filter_lock_licenses  = asyncio.Lock()
_filter_lock_topics    = asyncio.Lock()

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/hidden-gems", tags=["hidden-gems"])


def _pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


# ── Filter helpers ────────────────────────────────────────────────────────────

@router.get("/filters/languages")
async def get_languages(
    request: Request,
    hours: int = Query(168, ge=1, le=8760),
    limit: int = Query(50, ge=1, le=500),
) -> list[str]:
    cache_key = ("languages", hours, limit)
    if cache_key in _filter_cache:
        return _filter_cache[cache_key]
    async with _filter_lock_languages:
        if cache_key in _filter_cache:   # double-check after acquiring lock
            return _filter_cache[cache_key]
        async with _pool(request).acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.language AS value
                FROM events e
                JOIN repos r ON r.repo_id = e.repo_id
                WHERE e.time >= NOW() - make_interval(hours => $1::int)
                  AND r.language IS NOT NULL
                GROUP BY r.language
                ORDER BY COUNT(*) DESC
                LIMIT $2
                """,
                hours, limit,
            )
        result = [r["value"] for r in rows]
        _filter_cache[cache_key] = result
    return result


@router.get("/filters/licenses")
async def get_licenses(
    request: Request,
    hours: int = Query(168, ge=1, le=8760),
    limit: int = Query(20, ge=1, le=500),
) -> list[str]:
    cache_key = ("licenses", hours, limit)
    if cache_key in _filter_cache:
        return _filter_cache[cache_key]
    async with _filter_lock_licenses:
        if cache_key in _filter_cache:   # double-check after acquiring lock
            return _filter_cache[cache_key]
        async with _pool(request).acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT r.license_spdx AS value
                FROM events e
                JOIN repos r ON r.repo_id = e.repo_id
                WHERE e.time >= NOW() - make_interval(hours => $1::int)
                  AND r.license_spdx IS NOT NULL
                GROUP BY r.license_spdx
                ORDER BY COUNT(*) DESC
                LIMIT $2
                """,
                hours, limit,
            )
        result = [r["value"] for r in rows]
        _filter_cache[cache_key] = result
    return result


@router.get("/filters/topics")
async def get_topics(
    request: Request,
    hours: int = Query(168, ge=1, le=8760),
    limit: int = Query(200, ge=1, le=1000),
) -> list[str]:
    cache_key = ("topics", hours, limit)
    if cache_key in _filter_cache:
        return _filter_cache[cache_key]
    async with _filter_lock_topics:
        if cache_key in _filter_cache:   # double-check after acquiring lock
            return _filter_cache[cache_key]
        async with _pool(request).acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT t.value
                FROM events e
                JOIN repos r ON r.repo_id = e.repo_id
                JOIN LATERAL unnest(r.topics) AS t(value) ON true
                WHERE e.time >= NOW() - make_interval(hours => $1::int)
                  AND r.topics IS NOT NULL
                GROUP BY t.value
                ORDER BY COUNT(*) DESC
                LIMIT $2
                """,
                hours, limit,
            )
        result = [r["value"] for r in rows]
        _filter_cache[cache_key] = result
    return result


# ── Live ranking ──────────────────────────────────────────────────────────────

@router.get("/live")
async def get_live(
    request: Request,
    hours: int = Query(168, ge=1, le=8760),
    scope: str = Query("repos"),
    language: list[str] = Query(default=[]),
    license: list[str] = Query(default=[]),
    topic: list[str] = Query(default=[]),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    if scope not in {"repos", "users", "orgs"}:
        raise HTTPException(status_code=422, detail="scope must be one of: repos, users, orgs")

    offset = (page - 1) * limit
    lang_arr  = language or None
    lic_arr   = license  or None
    topic_arr = topic    or None

    async with _pool(request).acquire() as conn:
        if scope == "users":
            rows = await conn.fetch(
                """
                SELECT username, total_score, best_repo_score, best_repo,
                       hidden_gem_count, active_repos_in_window
                FROM hidden_gem_user_scores($1::float, $2::float, $3::int,
                                            $4::text[], $5::text[], $6::text[])
                ORDER BY total_score DESC NULLS LAST
                LIMIT $7 OFFSET $8
                """,
                1.0, 1.0, hours, lang_arr, lic_arr, topic_arr, limit, offset,
            )
            return {"scope": "users", "page": page, "limit": limit,
                    "items": [dict(r) for r in rows]}

        if scope == "orgs":
            rows = await conn.fetch(
                """
                SELECT org_login, org_repos_total_score, org_repos_best_score,
                       org_active_repos, org_hidden_gem_count,
                       member_repos_total_score, member_repos_best_score,
                       member_active_repos, member_active_users,
                       member_hidden_gem_count
                FROM hidden_gem_org_scores($1::float, $2::float, $3::int,
                                           $4::text[], $5::text[], $6::text[])
                ORDER BY org_repos_total_score DESC NULLS LAST
                LIMIT $7 OFFSET $8
                """,
                1.0, 1.0, hours, lang_arr, lic_arr, topic_arr, limit, offset,
            )
            return {"scope": "orgs", "page": page, "limit": limit,
                    "items": [dict(r) for r in rows]}

        # default: repos
        rows = await conn.fetch(
            """
            SELECT repo_id, full_name, name, owner_login, language,
                   license_spdx, topics, sig_score,
                   count_stars_interval, count_forks_interval,
                   total_stars, total_forks
            FROM hidden_gem_repo_scores(
                $1::float, $2::float, $3::int,
                $4::text[], $5::text[], $6::text[],
                5, 1, 10000
            )
            WHERE sig_score IS NOT NULL
            ORDER BY sig_score DESC NULLS LAST
            LIMIT $7 OFFSET $8
            """,
            1.0, 1.0, hours, lang_arr, lic_arr, topic_arr, limit, offset,
        )
        return {"scope": "repos", "page": page, "limit": limit,
                "items": [
                    {**dict(r), "topics": list(r["topics"] or [])}
                    for r in rows
                ]}


# ── Global search ─────────────────────────────────────────────────────────────

@router.get("/search")
async def search(
    request: Request,
    q: str = Query(..., min_length=1),
    scope: str = Query("all"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    offset = (page - 1) * limit
    safe_q = q.replace("%", "\\%").replace("_", "\\_")
    pattern = f"%{safe_q}%"

    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT type, name, score FROM (
                SELECT * FROM (
                    SELECT DISTINCT ON (full_name)
                        'repo'    AS type,
                        full_name AS name,
                        sig_score AS score
                    FROM hidden_gem_snapshot_repos sr
                    JOIN hidden_gem_snapshot_runs  rn ON rn.id = sr.snapshot_id
                    WHERE sr.full_name ILIKE $1 ESCAPE '\\'
                    ORDER BY full_name, rn.run_at DESC
                ) repos

                UNION ALL

                SELECT * FROM (
                    SELECT DISTINCT ON (username)
                        'user'      AS type,
                        username    AS name,
                        total_score AS score
                    FROM hidden_gem_snapshot_users su
                    JOIN hidden_gem_snapshot_runs  rn ON rn.id = su.snapshot_id
                    WHERE su.username ILIKE $1 ESCAPE '\\'
                    ORDER BY username, rn.run_at DESC
                ) users

                UNION ALL

                SELECT * FROM (
                    SELECT DISTINCT ON (org_login)
                        'org'                 AS type,
                        org_login             AS name,
                        org_repos_total_score AS score
                    FROM hidden_gem_snapshot_orgs so
                    JOIN hidden_gem_snapshot_runs rn ON rn.id = so.snapshot_id
                    WHERE so.org_login ILIKE $1 ESCAPE '\\'
                    ORDER BY org_login, rn.run_at DESC
                ) orgs
            ) combined
            WHERE ($2 = 'all' OR type = $2)
            ORDER BY score DESC NULLS LAST
            LIMIT $3 OFFSET $4
            """,
            pattern, scope, limit, offset,
        )

    return {
        "items": [{"type": r["type"], "name": r["name"], "score": r["score"]}
                  for r in rows],
        "page": page,
        "limit": limit,
    }


# ── Detail: repo ──────────────────────────────────────────────────────────────

@router.get("/repos/{full_name:path}")
async def get_repo_detail(
    request: Request,
    full_name: str,
    interval_hours: int = Query(168, ge=1, le=8760),
) -> dict[str, Any]:
    async with _pool(request).acquire() as conn:
        live_rows = await conn.fetch(
            """
            SELECT full_name, name, owner_login, language, license_spdx,
                   topics, sig_score, count_stars_interval,
                   count_forks_interval, total_stars, total_forks
            FROM hidden_gem_repo_scores(1.0, 1.0, $1, NULL, NULL, NULL, 1, 1, 10000)
            WHERE full_name = $2
            LIMIT 1
            """,
            interval_hours, full_name,
        )
        current = dict(live_rows[0]) if live_rows else None
        if current:
            current["topics"] = list(current.get("topics") or [])

        history = await conn.fetch(
            """
            SELECT rn.run_at, rn.interval_hours, sr.sig_score, sr.rank,
                   sr.count_stars_interval, sr.count_forks_interval,
                   sr.total_stars, sr.total_forks
            FROM hidden_gem_snapshot_runs rn
            JOIN hidden_gem_snapshot_repos sr ON sr.snapshot_id = rn.id
            WHERE sr.full_name = $1
              AND rn.interval_hours = $2
            ORDER BY rn.run_at ASC
            """,
            full_name, interval_hours,
        )

    if current is None and not history:
        raise HTTPException(status_code=404, detail=f"Repo '{full_name}' not found")

    return {
        "full_name": full_name,
        "current": current,
        "history": [
            {
                "run_at": r["run_at"].isoformat(),
                "interval_hours": r["interval_hours"],
                "sig_score": r["sig_score"],
                "rank": r["rank"],
                "count_stars_interval": r["count_stars_interval"],
                "count_forks_interval": r["count_forks_interval"],
                "total_stars": r["total_stars"],
                "total_forks": r["total_forks"],
            }
            for r in history
        ],
    }


# ── Detail: user ──────────────────────────────────────────────────────────────

@router.get("/users/{username}")
async def get_user_detail(
    request: Request,
    username: str,
    interval_hours: int = Query(168, ge=1, le=8760),
) -> dict[str, Any]:
    async with _pool(request).acquire() as conn:
        live_rows = await conn.fetch(
            """
            SELECT username, total_score, best_repo_score, best_repo,
                   hidden_gem_count, active_repos_in_window
            FROM hidden_gem_user_scores(1.0, 1.0, $1, NULL, NULL, NULL)
            WHERE username = $2
            LIMIT 1
            """,
            interval_hours, username,
        )
        current = dict(live_rows[0]) if live_rows else None

        repo_rows = await conn.fetch(
            """
            SELECT full_name, language, sig_score, total_stars,
                   count_stars_interval, count_forks_interval
            FROM hidden_gem_repo_scores(1.0, 1.0, $1, NULL, NULL, NULL,
                                        1, 1, 10000)
            WHERE owner_login = $2 AND owner_type = 'User'
              AND sig_score IS NOT NULL
            ORDER BY sig_score DESC NULLS LAST
            LIMIT 20
            """,
            interval_hours, username,
        )

        history = await conn.fetch(
            """
            SELECT rn.run_at, su.total_score, su.hidden_gem_count,
                   su.best_repo_score, su.best_repo
            FROM hidden_gem_snapshot_runs rn
            JOIN hidden_gem_snapshot_users su ON su.snapshot_id = rn.id
            WHERE su.username = $1
              AND rn.interval_hours = $2
            ORDER BY rn.run_at ASC
            """,
            username, interval_hours,
        )

    if current is None and not history:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")

    return {
        "username": username,
        "current": current,
        "repos": [dict(r) for r in repo_rows],
        "history": [
            {
                "run_at": r["run_at"].isoformat(),
                "total_score": r["total_score"],
                "hidden_gem_count": r["hidden_gem_count"],
                "best_repo_score": r["best_repo_score"],
                "best_repo": r["best_repo"],
            }
            for r in history
        ],
    }


# ── Detail: org ───────────────────────────────────────────────────────────────

@router.get("/orgs/{org_login}")
async def get_org_detail(
    request: Request,
    org_login: str,
    interval_hours: int = Query(168, ge=1, le=8760),
) -> dict[str, Any]:
    async with _pool(request).acquire() as conn:
        live_rows = await conn.fetch(
            """
            SELECT org_login, org_repos_total_score, org_repos_best_score,
                   org_active_repos, org_hidden_gem_count,
                   member_repos_total_score, member_repos_best_score,
                   member_active_repos, member_active_users,
                   member_hidden_gem_count
            FROM hidden_gem_org_scores(1.0, 1.0, $1, NULL, NULL, NULL)
            WHERE org_login = $2
            LIMIT 1
            """,
            interval_hours, org_login,
        )
        current = dict(live_rows[0]) if live_rows else None

        history = await conn.fetch(
            """
            SELECT rn.run_at, so.org_repos_total_score, so.org_hidden_gem_count,
                   so.member_repos_total_score, so.member_hidden_gem_count
            FROM hidden_gem_snapshot_runs rn
            JOIN hidden_gem_snapshot_orgs so ON so.snapshot_id = rn.id
            WHERE so.org_login = $1
              AND rn.interval_hours = $2
            ORDER BY rn.run_at ASC
            """,
            org_login, interval_hours,
        )

    if current is None and not history:
        raise HTTPException(status_code=404, detail=f"Org '{org_login}' not found")

    return {
        "org_login": org_login,
        "current": current,
        "history": [
            {
                "run_at": r["run_at"].isoformat(),
                "org_repos_total_score": r["org_repos_total_score"],
                "org_hidden_gem_count": r["org_hidden_gem_count"],
                "member_repos_total_score": r["member_repos_total_score"],
                "member_hidden_gem_count": r["member_hidden_gem_count"],
            }
            for r in history
        ],
    }


# ── Snapshots list ────────────────────────────────────────────────────────────

@router.get("/snapshots")
async def list_snapshots(
    request: Request,
    interval_hours: int = Query(168),
    limit: int = Query(20, ge=1, le=100),
) -> list[dict[str, Any]]:
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, run_at, interval_hours, alpha, beta,
                   repo_count, user_count, org_count
            FROM hidden_gem_snapshot_runs
            WHERE interval_hours = $1
            ORDER BY run_at DESC
            LIMIT $2
            """,
            interval_hours, limit,
        )
    return [
        {**dict(r), "run_at": r["run_at"].isoformat()}
        for r in rows
    ]


# ── Cohort evaluation ─────────────────────────────────────────────────────────

@router.get("/snapshots/{snapshot_id}/cohort")
async def get_cohort(
    request: Request,
    snapshot_id: int,
) -> dict[str, Any]:
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            """
            WITH current_run AS (
                SELECT interval_hours, run_at
                FROM hidden_gem_snapshot_runs
                WHERE id = $1
            ),
            next_run AS (
                SELECT r.id
                FROM hidden_gem_snapshot_runs r, current_run cr
                WHERE r.interval_hours = cr.interval_hours
                  AND r.run_at > cr.run_at
                ORDER BY r.run_at ASC
                LIMIT 1
            )
            SELECT
                cur.repo_id,
                cur.full_name,
                cur.sig_score        AS prev_score,
                nxt.sig_score        AS current_score,
                CASE
                    WHEN (SELECT id FROM next_run) IS NULL THEN 'pending'
                    WHEN nxt.sig_score IS NULL OR nxt.sig_score < 1.5 THEN 'false_positive'
                    ELSE 'true_positive'
                END AS classification
            FROM hidden_gem_snapshot_repos cur
            LEFT JOIN next_run          nr  ON true
            LEFT JOIN hidden_gem_snapshot_repos nxt
                ON nxt.snapshot_id = nr.id AND nxt.repo_id = cur.repo_id
            WHERE cur.snapshot_id = $1
            ORDER BY cur.sig_score DESC NULLS LAST
            """,
            snapshot_id,
        )

    if not rows:
        async with _pool(request).acquire() as conn2:
            exists = await conn2.fetchval(
                "SELECT 1 FROM hidden_gem_snapshot_runs WHERE id = $1", snapshot_id
            )
        if not exists:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")

    total     = len(rows)
    sustained = sum(1 for r in rows if r["classification"] == "true_positive")
    dropped   = sum(1 for r in rows if r["classification"] == "false_positive")
    pending   = sum(1 for r in rows if r["classification"] == "pending")

    return {
        "snapshot_id": snapshot_id,
        "summary": {
            "total":     total,
            "sustained": sustained,
            "dropped":   dropped,
            "pending":   pending,
        },
        "repos": [
            {
                "repo_id":        r["repo_id"],
                "full_name":      r["full_name"],
                "prev_score":     r["prev_score"],
                "current_score":  r["current_score"],
                "classification": r["classification"],
            }
            for r in rows
        ],
    }


# ── Manual trigger ────────────────────────────────────────────────────────────

@router.post("/snapshots/trigger")
async def trigger_snapshot(
    request: Request,
    interval_hours: int = Query(168),
) -> dict[str, Any]:
    scheduler = getattr(request.app.state, "snapshot_scheduler", None)
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialised")
    await scheduler.trigger(interval_hours)
    return {"status": "triggered", "interval_hours": interval_hours}
