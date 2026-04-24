"""
GitHub Insights API
───────────────────
FastAPI service that reads from TimescaleDB and exposes:

  REST
    GET /health
    GET /api/kpis
    GET /api/commits-over-time?days=30
    GET /api/top-repos?limit=10
    GET /api/recent-events?limit=20

  Server-Sent Events
    GET /stream/events   — pushes new events as they arrive (poll interval: 3 s)

Environment variables:
  DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator

import asyncpg
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from routers.hidden_gems import router as hidden_gems_router
from scheduler import SnapshotConfig, SnapshotScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [API] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME", "github_events")
DB_USER     = os.getenv("DB_USER", "github")
DB_PASSWORD = os.getenv("DB_PASSWORD", "github_secret")

pool: asyncpg.Pool | None = None


def _snapshot_config_from_env() -> SnapshotConfig:
    raw = os.getenv("SNAPSHOT_INTERVALS", "24,168,730")
    intervals = [int(x.strip()) for x in raw.split(",") if x.strip()]
    return SnapshotConfig(
        interval_hours=intervals,
        alpha=float(os.getenv("SNAPSHOT_ALPHA", "1.0")),
        beta=float(os.getenv("SNAPSHOT_BETA", "1.0")),
        min_stars=int(os.getenv("SNAPSHOT_MIN_STARS", "5")),
        min_forks=int(os.getenv("SNAPSHOT_MIN_FORKS", "1")),
        top_n=int(os.getenv("SNAPSHOT_TOP_N", "1000")),
    )


# ── DB pool with retry ────────────────────────────────────────────────────────
async def create_pool_with_retry() -> asyncpg.Pool:
    while True:
        try:
            p: asyncpg.Pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                min_size=2,
                max_size=10,
                command_timeout=30,
            )
            log.info("Connected to TimescaleDB at %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
            return p
        except Exception as exc:
            log.warning("DB not ready (%s), retrying in 3 s…", exc)
            await asyncio.sleep(3)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global pool
    pool = await create_pool_with_retry()
    app.state.pool = pool

    scheduler = SnapshotScheduler(pool, _snapshot_config_from_env())
    app.state.snapshot_scheduler = scheduler
    await scheduler.start()

    yield

    await scheduler.stop()
    await pool.close()


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="GitHub Insights API", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(hidden_gems_router)


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ── KPIs ──────────────────────────────────────────────────────────────────────
@app.get("/api/kpis")
async def get_kpis() -> dict[str, Any]:
    assert pool is not None
    now = datetime.now(timezone.utc)
    since_30d = now - timedelta(days=30)
    since_60d = now - timedelta(days=60)
    since_7d  = now - timedelta(days=7)

    async with pool.acquire() as conn:
        # ── Total push events (commit proxy) — current & previous 30-day window
        commits_now: int = await conn.fetchval(
            """
            SELECT COALESCE(sum(event_count), 0)
            FROM   event_stats_5m
            WHERE  bucket >= $1 AND event_type = 'PushEvent'
            """,
            since_30d,
        )
        commits_prev: int = await conn.fetchval(
            """
            SELECT COALESCE(sum(event_count), 0)
            FROM   event_stats_5m
            WHERE  bucket >= $1 AND bucket < $2 AND event_type = 'PushEvent'
            """,
            since_60d,
            since_30d,
        )

        # ── Open PRs: opened minus closed in last 30 days (approximation)
        prs_opened: int = await conn.fetchval(
            """
            SELECT count(*) FROM events
            WHERE  time >= $1
              AND  event_type = 'PullRequestEvent'
              AND  payload->>'action' = 'opened'
            """,
            since_30d,
        )
        prs_closed: int = await conn.fetchval(
            """
            SELECT count(*) FROM events
            WHERE  time >= $1
              AND  event_type = 'PullRequestEvent'
              AND  payload->>'action' = 'closed'
            """,
            since_30d,
        )
        open_prs = max(int(prs_opened or 0) - int(prs_closed or 0), 0)

        # ── Active contributors — current & previous 30-day window
        contributors_now: int = await conn.fetchval(
            "SELECT count(DISTINCT actor) FROM events WHERE time >= $1",
            since_30d,
        )
        contributors_prev: int = await conn.fetchval(
            "SELECT count(DISTINCT actor) FROM events WHERE time >= $1 AND time < $2",
            since_60d,
            since_30d,
        )

        # ── Avg review time (hours) for PRs that were opened and closed in last 7 d
        avg_review_hours: float | None = await conn.fetchval(
            """
            WITH pr_times AS (
                SELECT
                    repo,
                    payload->>'number'                                               AS pr_num,
                    MIN(CASE WHEN payload->>'action' = 'opened' THEN time END)      AS opened_at,
                    MAX(CASE WHEN payload->>'action' = 'closed' THEN time END)      AS closed_at
                FROM   events
                WHERE  event_type = 'PullRequestEvent'
                  AND  time >= $1
                GROUP  BY repo, payload->>'number'
            )
            SELECT ROUND(
                AVG(EXTRACT(EPOCH FROM (closed_at - opened_at)) / 3600)::numeric,
                1
            )
            FROM pr_times
            WHERE opened_at IS NOT NULL AND closed_at IS NOT NULL
            """,
            since_7d,
        )

    def pct_delta(curr: int, prev: int) -> float | None:
        if not prev:
            return None
        return round((curr - prev) / prev * 100, 1)

    return {
        "totalCommits": {
            "value": int(commits_now),
            "delta": pct_delta(int(commits_now), int(commits_prev)),
        },
        "openPRs": {
            "value": open_prs,
            "delta": None,
        },
        "activeContributors": {
            "value": int(contributors_now),
            "delta": pct_delta(int(contributors_now), int(contributors_prev)),
        },
        "avgReviewHours": {
            "value": float(avg_review_hours) if avg_review_hours is not None else None,
            "delta": None,
        },
    }


# ── Commits over time ─────────────────────────────────────────────────────────
@app.get("/api/commits-over-time")
async def get_commits_over_time(
    days: int = Query(30, ge=1, le=365),
) -> list[dict[str, Any]]:
    assert pool is not None
    since = datetime.now(timezone.utc) - timedelta(days=days)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                time_bucket('1 day', bucket)::date AS day,
                sum(event_count)                   AS commits
            FROM   event_stats_5m
            WHERE  bucket >= $1
              AND  event_type = 'PushEvent'
            GROUP  BY day
            ORDER  BY day
            """,
            since,
        )

    return [{"date": str(r["day"]), "commits": int(r["commits"])} for r in rows]


# ── Top repos ─────────────────────────────────────────────────────────────────
@app.get("/api/top-repos")
async def get_top_repos(
    limit: int = Query(10, ge=1, le=50),
) -> list[dict[str, Any]]:
    assert pool is not None

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT repo, events FROM v_top_repos_24h LIMIT $1",
            limit,
        )

    return [{"repo": r["repo"], "events": int(r["events"])} for r in rows]


# ── Recent events ─────────────────────────────────────────────────────────────
@app.get("/api/recent-events")
async def get_recent_events(
    limit: int = Query(20, ge=1, le=100),
) -> list[dict[str, Any]]:
    assert pool is not None

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT time, event_id, event_type, actor, repo, detail
            FROM   v_recent_events
            LIMIT  $1
            """,
            limit,
        )

    return [
        {
            "id":        r["event_id"],
            "repo":      r["repo"],
            "eventType": r["event_type"],
            "actor":     r["actor"],
            "timestamp": r["time"].isoformat(),
        }
        for r in rows
    ]


# ── SSE: live event stream ────────────────────────────────────────────────────
@app.get("/stream/events")
async def stream_events() -> StreamingResponse:
    assert pool is not None

    async def generator() -> AsyncGenerator[str, None]:
        last_seen: datetime = datetime.now(timezone.utc)

        # Immediate heartbeat so the browser EventSource registers as open
        yield ": heartbeat\n\n"

        while True:
            await asyncio.sleep(3)
            try:
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT time, event_id, event_type, actor, repo, detail
                        FROM   events
                        WHERE  time > $1
                        ORDER  BY time ASC
                        LIMIT  50
                        """,
                        last_seen,
                    )

                if rows:
                    last_seen = rows[-1]["time"]
                    for row in rows:
                        data = json.dumps(
                            {
                                "id":        row["event_id"],
                                "repo":      row["repo"],
                                "eventType": row["event_type"],
                                "actor":     row["actor"],
                                "timestamp": row["time"].isoformat(),
                                "detail":    row["detail"],
                            }
                        )
                        yield f"data: {data}\n\n"
                else:
                    # Keep-alive comment (browsers drop idle SSE connections)
                    yield ": keep-alive\n\n"

            except Exception as exc:
                log.error("SSE query error: %s", exc)
                yield ": error — retrying\n\n"
                await asyncio.sleep(5)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering if present
        },
    )
