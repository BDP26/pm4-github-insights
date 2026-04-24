# Hidden Gems Scheduler & Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a scheduled hidden gem snapshot system, 8 new FastAPI endpoints, and a full Next.js Hidden Gems page with live ranking, cohort evaluation, global search, and detail pages for repos/users/orgs.

**Architecture:** APScheduler runs inside FastAPI (extractable to standalone container). A new `api/routers/hidden_gems.py` router serves all hidden gem endpoints. The frontend is a new `/hidden-gems` route with client-side view toggling and server-component detail pages.

**Tech Stack:** Python 3.11, FastAPI, asyncpg, APScheduler 3.x, Next.js 16 App Router, React 19, TypeScript strict, Tailwind CSS 4, Tremor v3, Lucide React

---

## File Map

**Create:**
- `db/migrations/007_hidden_gem_snapshots.sql` — snapshot tables + indices
- `api/scheduler/__init__.py` — package exports
- `api/scheduler/snapshot_scheduler.py` — SnapshotConfig + SnapshotScheduler
- `api/routers/__init__.py` — package init
- `api/routers/hidden_gems.py` — all /api/hidden-gems/* endpoints
- `api/tests/__init__.py`
- `api/tests/test_snapshot_scheduler.py` — scheduler unit tests
- `frontend/src/types/hidden_gems.ts` — all hidden-gems TypeScript types
- `frontend/src/lib/hidden_gems_api.ts` — all hidden-gems fetch functions
- `frontend/src/components/HiddenGemFilters.tsx`
- `frontend/src/components/HiddenGemTable.tsx`
- `frontend/src/components/CohortTable.tsx`
- `frontend/src/components/SearchBar.tsx`
- `frontend/src/components/SearchResults.tsx`
- `frontend/src/components/ScoreHistoryChart.tsx`
- `frontend/src/components/RepoDetailCard.tsx`
- `frontend/src/components/UserDetailCard.tsx`
- `frontend/src/components/OrgDetailCard.tsx`
- `frontend/src/app/hidden-gems/page.tsx`
- `frontend/src/app/hidden-gems/repos/[...slug]/page.tsx`
- `frontend/src/app/hidden-gems/users/[username]/page.tsx`
- `frontend/src/app/hidden-gems/orgs/[org_login]/page.tsx`

**Modify:**
- `api/requirements.txt` — add apscheduler, pytest-asyncio
- `api/main.py` — import router + scheduler, wire into lifespan
- `frontend/src/components/Sidebar.tsx` — add Hidden Gems nav item

---

## Task 1: DB Migration — Snapshot Tables

**Files:**
- Create: `db/migrations/007_hidden_gem_snapshots.sql`

- [ ] **Step 1: Write the migration**

```sql
-- ════════════════════════════════════════════════════════════════
--  007 — Hidden Gem Snapshot Tables
-- ════════════════════════════════════════════════════════════════
--  Adds four tables for storing scheduled hidden gem score snapshots.
--  Idempotent: safe to re-apply.
-- ════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_runs (
    id              SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    interval_hours  INT         NOT NULL,
    alpha           FLOAT       NOT NULL DEFAULT 1.0,
    beta            FLOAT       NOT NULL DEFAULT 1.0,
    repo_count      INT,
    user_count      INT,
    org_count       INT
);

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_repos (
    snapshot_id          INT  NOT NULL REFERENCES hidden_gem_snapshot_runs(id) ON DELETE CASCADE,
    repo_id              INT  NOT NULL,
    full_name            TEXT NOT NULL,
    name                 TEXT,
    owner_login          TEXT,
    language             TEXT,
    license_spdx         TEXT,
    topics               TEXT[],
    sig_score            FLOAT,
    rank                 INT,
    count_stars_interval INT,
    count_forks_interval INT,
    total_stars          INT,
    total_forks          INT,
    PRIMARY KEY (snapshot_id, repo_id)
);

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_users (
    snapshot_id            INT  NOT NULL REFERENCES hidden_gem_snapshot_runs(id) ON DELETE CASCADE,
    username               TEXT NOT NULL,
    total_score            FLOAT,
    best_repo_score        FLOAT,
    best_repo              TEXT,
    hidden_gem_count       INT,
    active_repos_in_window INT,
    PRIMARY KEY (snapshot_id, username)
);

CREATE TABLE IF NOT EXISTS hidden_gem_snapshot_orgs (
    snapshot_id                INT  NOT NULL REFERENCES hidden_gem_snapshot_runs(id) ON DELETE CASCADE,
    org_login                  TEXT NOT NULL,
    org_repos_total_score      FLOAT,
    org_repos_best_score       FLOAT,
    org_active_repos           INT,
    org_hidden_gem_count       INT,
    member_repos_total_score   FLOAT,
    member_repos_best_score    FLOAT,
    member_active_repos        INT,
    member_active_users        INT,
    member_hidden_gem_count    INT,
    PRIMARY KEY (snapshot_id, org_login)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_runs_interval
    ON hidden_gem_snapshot_runs(interval_hours, run_at DESC);

CREATE INDEX IF NOT EXISTS idx_snapshot_repos_fullname
    ON hidden_gem_snapshot_repos(full_name, snapshot_id);

CREATE INDEX IF NOT EXISTS idx_snapshot_users_username
    ON hidden_gem_snapshot_users(username, snapshot_id);

CREATE INDEX IF NOT EXISTS idx_snapshot_orgs_login
    ON hidden_gem_snapshot_orgs(org_login, snapshot_id);

DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_runs TO grafana_reader';
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_repos TO grafana_reader';
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_users TO grafana_reader';
    EXECUTE 'GRANT SELECT ON hidden_gem_snapshot_orgs TO grafana_reader';
  END IF;
END $$;
```

- [ ] **Step 2: Apply migration to running DB**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights
docker compose exec timescaledb psql -U github -d github_events \
  -f /docker-entrypoint-initdb.d/migrations/007_hidden_gem_snapshots.sql
```

If migration path differs, copy the file in first:
```bash
docker cp db/migrations/007_hidden_gem_snapshots.sql \
  $(docker compose ps -q timescaledb):/tmp/007.sql
docker compose exec timescaledb psql -U github -d github_events -f /tmp/007.sql
```

Expected output: `CREATE TABLE` × 4, `CREATE INDEX` × 4, `DO`

- [ ] **Step 3: Verify tables exist**

```bash
docker compose exec timescaledb psql -U github -d github_events -c "\dt hidden_gem_snapshot*"
```

Expected: 4 rows listing the four new tables.

- [ ] **Step 4: Commit**

```bash
git add db/migrations/007_hidden_gem_snapshots.sql
git commit -m "feat: add hidden gem snapshot tables (migration 007)"
```

---

## Task 2: Scheduler Package

**Files:**
- Create: `api/scheduler/__init__.py`
- Create: `api/scheduler/snapshot_scheduler.py`
- Modify: `api/requirements.txt`

- [ ] **Step 1: Add APScheduler to requirements**

Replace contents of `api/requirements.txt`:

```
fastapi>=0.115.0
uvicorn[standard]>=0.30.0
asyncpg>=0.29.0
apscheduler>=3.10.0
pytest>=8.0.0
pytest-asyncio>=0.23.0
```

- [ ] **Step 2: Write the scheduler package init**

`api/scheduler/__init__.py`:
```python
from .snapshot_scheduler import SnapshotConfig, SnapshotScheduler

__all__ = ["SnapshotConfig", "SnapshotScheduler"]
```

- [ ] **Step 3: Write the SnapshotScheduler class**

`api/scheduler/snapshot_scheduler.py`:
```python
"""
SnapshotScheduler
─────────────────
Periodically captures hidden gem scores from the DB and writes them to
snapshot tables. Designed to be extracted to a standalone container:
the class has zero FastAPI imports — it only needs an asyncpg.Pool.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapshotConfig:
    interval_hours: list[int] = field(default_factory=lambda: [24, 168, 730])
    alpha: float = 1.0
    beta: float = 1.0
    min_stars: int = 5
    min_forks: int = 1
    top_n: int = 1000


class SnapshotScheduler:
    def __init__(self, pool: asyncpg.Pool, config: SnapshotConfig) -> None:
        self._pool = pool
        self._config = config
        self._scheduler = AsyncIOScheduler()

    async def start(self) -> None:
        """Register one APScheduler job per interval and start the scheduler."""
        for hours in self._config.interval_hours:
            self._scheduler.add_job(
                self._run_snapshot,
                "interval",
                hours=hours,
                args=[hours],
                id=f"snapshot_{hours}h",
                next_run_time=datetime.now(),  # run immediately on startup
                replace_existing=True,
            )
            log.info("Registered snapshot job: every %sh", hours)
        self._scheduler.start()
        log.info("SnapshotScheduler started with intervals: %s", self._config.interval_hours)

    async def stop(self) -> None:
        """Gracefully shut down APScheduler."""
        self._scheduler.shutdown(wait=False)
        log.info("SnapshotScheduler stopped")

    async def trigger(self, interval_hours: int) -> None:
        """Manually trigger a snapshot for a given interval (e.g. from an API endpoint)."""
        await self._run_snapshot(interval_hours)

    async def _run_snapshot(self, interval_hours: int) -> None:
        """Core snapshot logic: call DB scoring functions and persist results."""
        log.info("Starting snapshot: interval=%sh", interval_hours)
        async with self._pool.acquire() as conn:
            # 1. Insert the run record and get its ID
            run_id: int = await conn.fetchval(
                """
                INSERT INTO hidden_gem_snapshot_runs (run_at, interval_hours, alpha, beta)
                VALUES (NOW(), $1, $2, $3)
                RETURNING id
                """,
                interval_hours,
                self._config.alpha,
                self._config.beta,
            )

            # 2. Capture repo scores
            repo_rows = await conn.fetch(
                """
                SELECT
                    repo_id, full_name, name, owner_login, language,
                    license_spdx, topics,
                    sig_score,
                    ROW_NUMBER() OVER (ORDER BY sig_score DESC NULLS LAST) AS rank,
                    count_stars_interval, count_forks_interval,
                    total_stars, total_forks
                FROM hidden_gem_repo_scores(
                    $1::float, $2::float, $3::int,
                    NULL, NULL, NULL,
                    $4::int, $5::int, $6::int
                )
                WHERE sig_score IS NOT NULL
                """,
                self._config.alpha,
                self._config.beta,
                interval_hours,
                self._config.min_stars,
                self._config.min_forks,
                self._config.top_n,
            )

            if repo_rows:
                await conn.executemany(
                    """
                    INSERT INTO hidden_gem_snapshot_repos
                        (snapshot_id, repo_id, full_name, name, owner_login,
                         language, license_spdx, topics, sig_score, rank,
                         count_stars_interval, count_forks_interval,
                         total_stars, total_forks)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
                    ON CONFLICT (snapshot_id, repo_id) DO NOTHING
                    """,
                    [
                        (
                            run_id,
                            r["repo_id"], r["full_name"], r["name"], r["owner_login"],
                            r["language"], r["license_spdx"], r["topics"],
                            r["sig_score"], r["rank"],
                            r["count_stars_interval"], r["count_forks_interval"],
                            r["total_stars"], r["total_forks"],
                        )
                        for r in repo_rows
                    ],
                )

            # 3. Capture user scores
            user_rows = await conn.fetch(
                """
                SELECT username, total_score, best_repo_score, best_repo,
                       hidden_gem_count, active_repos_in_window
                FROM hidden_gem_user_scores($1::float, $2::float, $3::int,
                                            NULL, NULL, NULL)
                """,
                self._config.alpha,
                self._config.beta,
                interval_hours,
            )

            if user_rows:
                await conn.executemany(
                    """
                    INSERT INTO hidden_gem_snapshot_users
                        (snapshot_id, username, total_score, best_repo_score,
                         best_repo, hidden_gem_count, active_repos_in_window)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (snapshot_id, username) DO NOTHING
                    """,
                    [
                        (
                            run_id,
                            r["username"], r["total_score"], r["best_repo_score"],
                            r["best_repo"], r["hidden_gem_count"],
                            r["active_repos_in_window"],
                        )
                        for r in user_rows
                    ],
                )

            # 4. Capture org scores
            org_rows = await conn.fetch(
                """
                SELECT org_login, org_repos_total_score, org_repos_best_score,
                       org_active_repos, org_hidden_gem_count,
                       member_repos_total_score, member_repos_best_score,
                       member_active_repos, member_active_users,
                       member_hidden_gem_count
                FROM hidden_gem_org_scores($1::float, $2::float, $3::int,
                                           NULL, NULL, NULL)
                """,
                self._config.alpha,
                self._config.beta,
                interval_hours,
            )

            if org_rows:
                await conn.executemany(
                    """
                    INSERT INTO hidden_gem_snapshot_orgs
                        (snapshot_id, org_login, org_repos_total_score,
                         org_repos_best_score, org_active_repos,
                         org_hidden_gem_count, member_repos_total_score,
                         member_repos_best_score, member_active_repos,
                         member_active_users, member_hidden_gem_count)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (snapshot_id, org_login) DO NOTHING
                    """,
                    [
                        (
                            run_id,
                            r["org_login"],
                            r["org_repos_total_score"], r["org_repos_best_score"],
                            r["org_active_repos"], r["org_hidden_gem_count"],
                            r["member_repos_total_score"], r["member_repos_best_score"],
                            r["member_active_repos"], r["member_active_users"],
                            r["member_hidden_gem_count"],
                        )
                        for r in org_rows
                    ],
                )

            # 5. Update run record with counts
            await conn.execute(
                """
                UPDATE hidden_gem_snapshot_runs
                SET repo_count = $2, user_count = $3, org_count = $4
                WHERE id = $1
                """,
                run_id,
                len(repo_rows),
                len(user_rows),
                len(org_rows),
            )

        log.info(
            "Snapshot complete: id=%s interval=%sh repos=%s users=%s orgs=%s",
            run_id, interval_hours, len(repo_rows), len(user_rows), len(org_rows),
        )
```

- [ ] **Step 4: Write scheduler unit tests**

`api/tests/__init__.py`: empty file.

`api/tests/test_snapshot_scheduler.py`:
```python
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from scheduler import SnapshotConfig, SnapshotScheduler


@pytest.fixture
def config() -> SnapshotConfig:
    return SnapshotConfig(interval_hours=[24], alpha=1.0, beta=1.0)


@pytest.fixture
def mock_pool() -> AsyncMock:
    pool = AsyncMock()
    conn = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(return_value=[])
    conn.executemany = AsyncMock()
    conn.execute = AsyncMock()
    return pool


def test_snapshot_config_defaults() -> None:
    cfg = SnapshotConfig()
    assert cfg.interval_hours == [24, 168, 730]
    assert cfg.alpha == 1.0
    assert cfg.beta == 1.0
    assert cfg.min_stars == 5
    assert cfg.min_forks == 1
    assert cfg.top_n == 1000


def test_snapshot_config_custom() -> None:
    cfg = SnapshotConfig(interval_hours=[48], alpha=2.0, beta=0.5)
    assert cfg.interval_hours == [48]
    assert cfg.alpha == 2.0


@pytest.mark.asyncio
async def test_run_snapshot_inserts_run_record(
    mock_pool: AsyncMock, config: SnapshotConfig
) -> None:
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, config)
        await scheduler._run_snapshot(24)

    conn = mock_pool.acquire.return_value.__aenter__.return_value
    # Should insert a run record
    conn.fetchval.assert_awaited_once()
    insert_sql = conn.fetchval.call_args[0][0]
    assert "INSERT INTO hidden_gem_snapshot_runs" in insert_sql


@pytest.mark.asyncio
async def test_run_snapshot_updates_counts(
    mock_pool: AsyncMock, config: SnapshotConfig
) -> None:
    with patch("scheduler.snapshot_scheduler.AsyncIOScheduler"):
        scheduler = SnapshotScheduler(mock_pool, config)
        await scheduler._run_snapshot(24)

    conn = mock_pool.acquire.return_value.__aenter__.return_value
    conn.execute.assert_awaited_once()
    update_sql = conn.execute.call_args[0][0]
    assert "UPDATE hidden_gem_snapshot_runs" in update_sql
```

- [ ] **Step 5: Run the tests**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights/api
pip install apscheduler>=3.10.0 pytest pytest-asyncio
pytest tests/test_snapshot_scheduler.py -v
```

Expected: 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add api/scheduler/ api/tests/ api/requirements.txt
git commit -m "feat: add SnapshotScheduler with APScheduler (extractable to container)"
```

---

## Task 3: FastAPI Hidden Gems Router

**Files:**
- Create: `api/routers/__init__.py`
- Create: `api/routers/hidden_gems.py`

- [ ] **Step 1: Create router package init**

`api/routers/__init__.py`:
```python
```
(empty file)

- [ ] **Step 2: Write the hidden gems router**

`api/routers/hidden_gems.py`:
```python
"""
Hidden Gems Router
──────────────────
Endpoints:
  GET  /api/hidden-gems/live
  GET  /api/hidden-gems/search
  GET  /api/hidden-gems/repos/{full_name}       (full_name = owner/repo)
  GET  /api/hidden-gems/users/{username}
  GET  /api/hidden-gems/orgs/{org_login}
  GET  /api/hidden-gems/snapshots
  GET  /api/hidden-gems/snapshots/{id}/cohort
  POST /api/hidden-gems/snapshots/trigger
  GET  /api/hidden-gems/filters/languages
  GET  /api/hidden-gems/filters/licenses
  GET  /api/hidden-gems/filters/topics
"""
import logging
from typing import Any

import asyncpg
from fastapi import APIRouter, HTTPException, Query, Request

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/hidden-gems", tags=["hidden-gems"])


def _pool(request: Request) -> asyncpg.Pool:
    """Extract the shared DB pool injected via app.state."""
    return request.app.state.pool


# ── Filter helpers ────────────────────────────────────────────────────────────

@router.get("/filters/languages")
async def get_languages(request: Request) -> list[str]:
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch("SELECT value FROM v_repo_languages ORDER BY value")
    return [r["value"] for r in rows]


@router.get("/filters/licenses")
async def get_licenses(request: Request) -> list[str]:
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch("SELECT value FROM v_repo_licenses ORDER BY value")
    return [r["value"] for r in rows]


@router.get("/filters/topics")
async def get_topics(request: Request) -> list[str]:
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch("SELECT value FROM v_repo_topics ORDER BY value")
    return [r["value"] for r in rows]


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
    offset = (page - 1) * limit
    lang_arr   = language or None
    lic_arr    = license  or None
    topic_arr  = topic    or None

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
    # Sanitize: strip SQL wildcard characters from user input before embedding
    safe_q = q.replace("%", "\\%").replace("_", "\\_")
    pattern = f"%{safe_q}%"

    # Build a fixed UNION across all three entity tables and post-filter by scope.
    # Using a single $1 parameter for the pattern avoids injection.
    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT type, name, score FROM (
                SELECT DISTINCT ON (full_name)
                    'repo'    AS type,
                    full_name AS name,
                    sig_score AS score
                FROM hidden_gem_snapshot_repos sr
                JOIN hidden_gem_snapshot_runs  rn ON rn.id = sr.snapshot_id
                WHERE sr.full_name ILIKE $1 ESCAPE '\\'
                ORDER BY full_name, rn.run_at DESC

                UNION ALL

                SELECT DISTINCT ON (username)
                    'user'      AS type,
                    username    AS name,
                    total_score AS score
                FROM hidden_gem_snapshot_users su
                JOIN hidden_gem_snapshot_runs  rn ON rn.id = su.snapshot_id
                WHERE su.username ILIKE $1 ESCAPE '\\'
                ORDER BY username, rn.run_at DESC

                UNION ALL

                SELECT DISTINCT ON (org_login)
                    'org'                 AS type,
                    org_login             AS name,
                    org_repos_total_score AS score
                FROM hidden_gem_snapshot_orgs so
                JOIN hidden_gem_snapshot_runs rn ON rn.id = so.snapshot_id
                WHERE so.org_login ILIKE $1 ESCAPE '\\'
                ORDER BY org_login, rn.run_at DESC
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
    interval_hours: int = Query(168),
) -> dict[str, Any]:
    async with _pool(request).acquire() as conn:
        # Current live score
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

        # Score history
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
    interval_hours: int = Query(168),
) -> dict[str, Any]:
    async with _pool(request).acquire() as conn:
        # Current aggregate
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

        # Their repos (live)
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

        # Score history
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
    interval_hours: int = Query(168),
) -> dict[str, Any]:
    async with _pool(request).acquire() as conn:
        # Current aggregate
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

        # Score history
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

    total    = len(rows)
    sustained = sum(1 for r in rows if r["classification"] == "true_positive")
    dropped   = sum(1 for r in rows if r["classification"] == "false_positive")
    pending   = sum(1 for r in rows if r["classification"] == "pending")

    return {
        "snapshot_id": snapshot_id,
        "summary": {
            "total":    total,
            "sustained": sustained,
            "dropped":  dropped,
            "pending":  pending,
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
) -> dict[str, str]:
    scheduler = getattr(request.app.state, "snapshot_scheduler", None)
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialised")
    await scheduler.trigger(interval_hours)
    return {"status": "triggered", "interval_hours": str(interval_hours)}
```

- [ ] **Step 3: Commit**

```bash
git add api/routers/
git commit -m "feat: add hidden gems FastAPI router with live, search, detail, cohort endpoints"
```

---

## Task 4: Wire Router + Scheduler into main.py

**Files:**
- Modify: `api/main.py`

- [ ] **Step 1: Read current main.py** (already done in planning — lines 1-333)

- [ ] **Step 2: Add imports at the top of main.py**

After the existing imports block (after `from fastapi.responses import StreamingResponse`), add:

```python
import os
from routers.hidden_gems import router as hidden_gems_router
from scheduler import SnapshotConfig, SnapshotScheduler
```

- [ ] **Step 3: Add config_from_env helper after the DB config block**

After the `pool: asyncpg.Pool | None = None` line, add:

```python
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
```

- [ ] **Step 4: Update the lifespan function**

Replace the existing lifespan:

```python
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
```

- [ ] **Step 5: Register the router**

After `app.add_middleware(...)` block, add:

```python
app.include_router(hidden_gems_router)
```

- [ ] **Step 6: Rebuild and test the API container**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights
docker compose build api
docker compose up -d api
sleep 5
curl -s http://localhost:8000/health
```

Expected: `{"status":"ok"}`

```bash
curl -s "http://localhost:8000/api/hidden-gems/filters/languages" | head -c 200
```

Expected: a JSON array (possibly empty if no data yet).

- [ ] **Step 7: Commit**

```bash
git add api/main.py
git commit -m "feat: wire hidden gems router and SnapshotScheduler into FastAPI lifespan"
```

---

## Task 5: TypeScript Types + API Client

**Files:**
- Create: `frontend/src/types/hidden_gems.ts`
- Create: `frontend/src/lib/hidden_gems_api.ts`

- [ ] **Step 1: Write the TypeScript types**

`frontend/src/types/hidden_gems.ts`:
```typescript
export interface HiddenGemRepo {
  repo_id: number;
  full_name: string;
  name: string;
  owner_login: string;
  language: string | null;
  license_spdx: string | null;
  topics: string[];
  sig_score: number;
  count_stars_interval: number;
  count_forks_interval: number;
  total_stars: number;
  total_forks: number;
}

export interface HiddenGemUser {
  username: string;
  total_score: number;
  best_repo_score: number;
  best_repo: string | null;
  hidden_gem_count: number;
  active_repos_in_window: number;
}

export interface HiddenGemOrg {
  org_login: string;
  org_repos_total_score: number;
  org_repos_best_score: number;
  org_active_repos: number;
  org_hidden_gem_count: number;
  member_repos_total_score: number;
  member_repos_best_score: number;
  member_active_repos: number;
  member_active_users: number;
  member_hidden_gem_count: number;
}

export interface LiveResponse<T> {
  scope: string;
  page: number;
  limit: number;
  items: T[];
}

export interface SearchResult {
  type: "repo" | "user" | "org";
  name: string;
  score: number | null;
}

export interface SearchResponse {
  items: SearchResult[];
  page: number;
  limit: number;
}

export interface ScoreHistoryPoint {
  run_at: string;
  interval_hours: number;
  sig_score: number | null;
  rank: number | null;
  count_stars_interval: number;
  count_forks_interval: number;
  total_stars: number;
  total_forks: number;
}

export interface RepoDetailResponse {
  full_name: string;
  current: HiddenGemRepo | null;
  history: ScoreHistoryPoint[];
}

export interface UserHistoryPoint {
  run_at: string;
  total_score: number;
  hidden_gem_count: number;
  best_repo_score: number | null;
  best_repo: string | null;
}

export interface UserDetailResponse {
  username: string;
  current: HiddenGemUser | null;
  repos: Partial<HiddenGemRepo>[];
  history: UserHistoryPoint[];
}

export interface OrgHistoryPoint {
  run_at: string;
  org_repos_total_score: number;
  org_hidden_gem_count: number;
  member_repos_total_score: number;
  member_hidden_gem_count: number;
}

export interface OrgDetailResponse {
  org_login: string;
  current: HiddenGemOrg | null;
  history: OrgHistoryPoint[];
}

export interface SnapshotRun {
  id: number;
  run_at: string;
  interval_hours: number;
  alpha: number;
  beta: number;
  repo_count: number | null;
  user_count: number | null;
  org_count: number | null;
}

export interface CohortEntry {
  repo_id: number;
  full_name: string;
  prev_score: number;
  current_score: number | null;
  classification: "true_positive" | "false_positive" | "pending";
}

export interface CohortSummary {
  total: number;
  sustained: number;
  dropped: number;
  pending: number;
}

export interface CohortResponse {
  snapshot_id: number;
  summary: CohortSummary;
  repos: CohortEntry[];
}
```

- [ ] **Step 2: Write the API client**

`frontend/src/lib/hidden_gems_api.ts`:
```typescript
/**
 * Hidden Gems API client — import only from Server Components or Route Handlers.
 * Uses the internal Docker hostname (API_URL) to stay in the container network.
 */
import type {
  CohortResponse,
  LiveResponse,
  HiddenGemRepo,
  HiddenGemUser,
  HiddenGemOrg,
  OrgDetailResponse,
  RepoDetailResponse,
  SearchResponse,
  SnapshotRun,
  UserDetailResponse,
} from "@/types/hidden_gems";

const BASE_URL = process.env.API_URL ?? "http://localhost:8000";

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`API ${path} returned ${res.status}`);
  return res.json() as Promise<T>;
}

function qs(params: Record<string, string | number | string[] | undefined>): string {
  const parts: string[] = [];
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined) continue;
    if (Array.isArray(v)) {
      v.forEach((item) => parts.push(`${k}=${encodeURIComponent(item)}`));
    } else {
      parts.push(`${k}=${encodeURIComponent(v)}`);
    }
  }
  return parts.length ? `?${parts.join("&")}` : "";
}

export async function fetchFilterLanguages(): Promise<string[]> {
  return apiFetch<string[]>("/api/hidden-gems/filters/languages");
}

export async function fetchFilterLicenses(): Promise<string[]> {
  return apiFetch<string[]>("/api/hidden-gems/filters/licenses");
}

export async function fetchFilterTopics(): Promise<string[]> {
  return apiFetch<string[]>("/api/hidden-gems/filters/topics");
}

export async function fetchHiddenGemsLive(params: {
  hours?: number;
  scope?: "repos" | "users" | "orgs";
  language?: string[];
  license?: string[];
  topic?: string[];
  page?: number;
  limit?: number;
}): Promise<LiveResponse<HiddenGemRepo | HiddenGemUser | HiddenGemOrg>> {
  return apiFetch(
    `/api/hidden-gems/live${qs({
      hours: params.hours,
      scope: params.scope,
      language: params.language,
      license: params.license,
      topic: params.topic,
      page: params.page,
      limit: params.limit,
    })}`
  );
}

export async function fetchHiddenGemSearch(params: {
  q: string;
  scope?: string;
  page?: number;
  limit?: number;
}): Promise<SearchResponse> {
  return apiFetch(
    `/api/hidden-gems/search${qs({
      q: params.q,
      scope: params.scope,
      page: params.page,
      limit: params.limit,
    })}`
  );
}

export async function fetchRepoDetail(
  fullName: string,
  intervalHours = 168
): Promise<RepoDetailResponse> {
  return apiFetch(
    `/api/hidden-gems/repos/${encodeURIComponent(fullName)}${qs({ interval_hours: intervalHours })}`
  );
}

export async function fetchUserDetail(
  username: string,
  intervalHours = 168
): Promise<UserDetailResponse> {
  return apiFetch(
    `/api/hidden-gems/users/${encodeURIComponent(username)}${qs({ interval_hours: intervalHours })}`
  );
}

export async function fetchOrgDetail(
  orgLogin: string,
  intervalHours = 168
): Promise<OrgDetailResponse> {
  return apiFetch(
    `/api/hidden-gems/orgs/${encodeURIComponent(orgLogin)}${qs({ interval_hours: intervalHours })}`
  );
}

export async function fetchSnapshotRuns(
  intervalHours = 168,
  limit = 20
): Promise<SnapshotRun[]> {
  return apiFetch(
    `/api/hidden-gems/snapshots${qs({ interval_hours: intervalHours, limit })}`
  );
}

export async function fetchSnapshotCohort(
  snapshotId: number
): Promise<CohortResponse> {
  return apiFetch(`/api/hidden-gems/snapshots/${snapshotId}/cohort`);
}
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights/frontend
npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/types/hidden_gems.ts frontend/src/lib/hidden_gems_api.ts
git commit -m "feat: add hidden gems TypeScript types and API client"
```

---

## Task 6: Sidebar + Filter Components

**Files:**
- Modify: `frontend/src/components/Sidebar.tsx`
- Create: `frontend/src/components/HiddenGemFilters.tsx`

- [ ] **Step 1: Add Hidden Gems to Sidebar nav**

In `frontend/src/components/Sidebar.tsx`, replace the navItems array:

```typescript
const navItems: NavItem[] = [
  { label: "Overview", href: "/" },
  { label: "Repositories", href: "/repositories" },
  { label: "Contributors", href: "/contributors" },
  { label: "Activity", href: "/activity" },
  { label: "Hidden Gems", href: "/hidden-gems" },
];
```

- [ ] **Step 2: Write HiddenGemFilters component**

`frontend/src/components/HiddenGemFilters.tsx`:
```typescript
"use client";

import { Filter, Code, Shield, BookOpen, Info } from "lucide-react";

export type Timeframe = "24" | "168" | "730";
export type Scope = "repos" | "users" | "orgs";

interface HiddenGemFiltersProps {
  timeframe: Timeframe;
  scope: Scope;
  language: string;
  license: string;
  topic: string;
  languages: string[];
  licenses: string[];
  topics: string[];
  onTimeframeChange: (t: Timeframe) => void;
  onScopeChange: (s: Scope) => void;
  onLanguageChange: (l: string) => void;
  onLicenseChange: (l: string) => void;
  onTopicChange: (t: string) => void;
}

const TIMEFRAMES: { value: Timeframe; label: string }[] = [
  { value: "24",  label: "Daily (24h)"    },
  { value: "168", label: "Weekly (168h)"  },
  { value: "730", label: "Monthly (730h)" },
];

const SCOPES: { value: Scope; label: string }[] = [
  { value: "repos", label: "Repos" },
  { value: "users", label: "Users" },
  { value: "orgs",  label: "Orgs"  },
];

export default function HiddenGemFilters({
  timeframe,
  scope,
  language,
  license,
  topic,
  languages,
  licenses,
  topics,
  onTimeframeChange,
  onScopeChange,
  onLanguageChange,
  onLicenseChange,
  onTopicChange,
}: HiddenGemFiltersProps) {
  return (
    <div className="space-y-4">
      {/* Timeframe + Scope selector */}
      <div className="flex flex-wrap items-center gap-4 justify-between">
        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {TIMEFRAMES.map((t) => (
            <button
              key={t.value}
              onClick={() => onTimeframeChange(t.value)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
                timeframe === t.value
                  ? "bg-indigo-50 text-indigo-700 shadow-sm"
                  : "text-slate-600 hover:text-slate-900 hover:bg-slate-50"
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {SCOPES.map((s) => (
            <button
              key={s.value}
              onClick={() => onScopeChange(s.value)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
                scope === s.value
                  ? "bg-indigo-50 text-indigo-700 shadow-sm"
                  : "text-slate-600 hover:text-slate-900 hover:bg-slate-50"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </div>

      {/* Attribute filters (only shown for repos) */}
      {scope === "repos" && (
        <div className="bg-white p-4 rounded-xl shadow-sm border border-slate-200 flex flex-wrap gap-4 items-center">
          <div className="flex items-center text-slate-500 mr-2">
            <Filter className="w-5 h-5 mr-2" />
            <span className="font-medium text-sm">Filters:</span>
          </div>

          <div className="flex items-center gap-2">
            <Code className="w-4 h-4 text-slate-400" />
            <select
              value={language}
              onChange={(e) => onLanguageChange(e.target.value)}
              className="pl-2 pr-8 py-1.5 text-sm border border-slate-200 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
            >
              <option value="">All Languages</option>
              {languages.map((l) => (
                <option key={l} value={l}>{l}</option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-2 border-l border-slate-200 pl-4">
            <Shield className="w-4 h-4 text-slate-400" />
            <select
              value={license}
              onChange={(e) => onLicenseChange(e.target.value)}
              className="pl-2 pr-8 py-1.5 text-sm border border-slate-200 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
            >
              <option value="">All Licenses</option>
              {licenses.map((l) => (
                <option key={l} value={l}>{l}</option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-2 border-l border-slate-200 pl-4">
            <BookOpen className="w-4 h-4 text-slate-400" />
            <select
              value={topic}
              onChange={(e) => onTopicChange(e.target.value)}
              className="pl-2 pr-8 py-1.5 text-sm border border-slate-200 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
            >
              <option value="">All Topics</option>
              {topics.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>

          <div className="ml-auto text-xs text-slate-500 flex items-center gap-1 bg-slate-100 px-3 py-1.5 rounded-full">
            <Info className="w-4 h-4" />
            Weights: α=1.0 (Stars), β=1.0 (Forks)
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights/frontend
npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/Sidebar.tsx frontend/src/components/HiddenGemFilters.tsx
git commit -m "feat: add Hidden Gems sidebar link and filter bar component"
```

---

## Task 7: HiddenGemTable + CohortTable

**Files:**
- Create: `frontend/src/components/HiddenGemTable.tsx`
- Create: `frontend/src/components/CohortTable.tsx`

- [ ] **Step 1: Write HiddenGemTable**

`frontend/src/components/HiddenGemTable.tsx`:
```typescript
"use client";

import Link from "next/link";
import {
  Star, GitFork, ArrowUpRight, ArrowDownRight, Minus,
  BookOpen, ChevronLeft, ChevronRight,
} from "lucide-react";
import type { HiddenGemRepo, HiddenGemUser, HiddenGemOrg } from "@/types/hidden_gems";

type Scope = "repos" | "users" | "orgs";

interface HiddenGemTableProps {
  items: (HiddenGemRepo | HiddenGemUser | HiddenGemOrg)[];
  scope: Scope;
  page: number;
  onPageChange: (p: number) => void;
}

function ScoreBadge({ score }: { score: number }) {
  const isSignificant = score >= 3.0;
  return (
    <div className="flex flex-col items-start">
      <span className={`text-lg font-bold ${isSignificant ? "text-emerald-600" : "text-amber-500"}`}>
        {score.toFixed(2)}
      </span>
      <span className="text-xs text-slate-400 mt-0.5">
        {isSignificant ? "> 95% Conf." : "< 95% Conf."}
      </span>
    </div>
  );
}

function RepoRow({ item, rank }: { item: HiddenGemRepo; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors">
      <td className="p-4 text-center font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <div className="flex flex-col">
          <Link
            href={`/hidden-gems/repos/${item.full_name}`}
            className="text-indigo-600 font-semibold hover:underline flex items-center gap-1 text-base"
          >
            <BookOpen className="w-4 h-4 text-slate-400" />
            {item.full_name}
          </Link>
          <div className="flex items-center gap-3 mt-2 text-xs font-medium text-slate-500">
            {item.language && (
              <span className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-blue-500" />
                {item.language}
              </span>
            )}
            {item.license_spdx && (
              <span className="px-1.5 py-0.5 rounded bg-slate-100 border border-slate-200">
                {item.license_spdx}
              </span>
            )}
          </div>
        </div>
      </td>
      <td className="p-4">
        <ScoreBadge score={item.sig_score} />
      </td>
      <td className="p-4">
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <Star className="w-4 h-4 text-amber-400" />
            <span className="text-sm font-semibold text-slate-700">
              {item.total_stars.toLocaleString()}
            </span>
            <span className="text-xs font-medium text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded">
              +{item.count_stars_interval}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <GitFork className="w-4 h-4 text-slate-400" />
            <span className="text-sm font-semibold text-slate-700">
              {item.total_forks.toLocaleString()}
            </span>
            <span className="text-xs font-medium text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded">
              +{item.count_forks_interval}
            </span>
          </div>
        </div>
      </td>
    </tr>
  );
}

function UserRow({ item, rank }: { item: HiddenGemUser; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors">
      <td className="p-4 text-center font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/users/${item.username}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.username}
        </Link>
        {item.best_repo && (
          <p className="text-xs text-slate-500 mt-1">Best: {item.best_repo}</p>
        )}
      </td>
      <td className="p-4">
        <ScoreBadge score={item.total_score} />
      </td>
      <td className="p-4">
        <span className="text-sm text-slate-600">
          {item.hidden_gem_count} gems · {item.active_repos_in_window} active repos
        </span>
      </td>
    </tr>
  );
}

function OrgRow({ item, rank }: { item: HiddenGemOrg; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors">
      <td className="p-4 text-center font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/orgs/${item.org_login}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.org_login}
        </Link>
      </td>
      <td className="p-4">
        <ScoreBadge score={item.org_repos_total_score} />
      </td>
      <td className="p-4">
        <span className="text-sm text-slate-600">
          {item.org_hidden_gem_count} org gems · {item.member_hidden_gem_count} member gems
        </span>
      </td>
    </tr>
  );
}

export default function HiddenGemTable({
  items, scope, page, onPageChange,
}: HiddenGemTableProps) {
  const offset = (page - 1) * 25;

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-50 border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500 font-semibold">
              <th className="p-4 w-12 text-center">Rank</th>
              <th className="p-4">{scope === "repos" ? "Repository" : scope === "users" ? "User" : "Organisation"}</th>
              <th className="p-4">Sig. Score</th>
              <th className="p-4">Activity</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {items.map((item, idx) => {
              const rank = offset + idx + 1;
              if (scope === "repos") return <RepoRow key={(item as HiddenGemRepo).repo_id} item={item as HiddenGemRepo} rank={rank} />;
              if (scope === "users") return <UserRow key={(item as HiddenGemUser).username} item={item as HiddenGemUser} rank={rank} />;
              return <OrgRow key={(item as HiddenGemOrg).org_login} item={item as HiddenGemOrg} rank={rank} />;
            })}
          </tbody>
        </table>
      </div>
      <div className="bg-slate-50 border-t border-slate-200 p-4 flex items-center justify-between text-sm text-slate-600">
        <span>Page {page}</span>
        <div className="flex gap-2">
          <button
            onClick={() => onPageChange(page - 1)}
            disabled={page === 1}
            className="px-3 py-1 bg-white border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-40 flex items-center gap-1"
          >
            <ChevronLeft className="w-4 h-4" /> Prev
          </button>
          <button
            onClick={() => onPageChange(page + 1)}
            disabled={items.length < 25}
            className="px-3 py-1 bg-white border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-40 flex items-center gap-1"
          >
            Next <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Write CohortTable**

`frontend/src/components/CohortTable.tsx`:
```typescript
"use client";

import Link from "next/link";
import {
  BookOpen, CheckCircle, AlertTriangle, Clock,
  ArrowUpRight, ArrowDownRight,
} from "lucide-react";
import type { CohortEntry, CohortSummary } from "@/types/hidden_gems";

interface CohortTableProps {
  summary: CohortSummary;
  repos: CohortEntry[];
  reportLabel: string;
}

function ClassificationBadge({ classification }: { classification: CohortEntry["classification"] }) {
  if (classification === "true_positive") {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-emerald-50 text-emerald-700 border border-emerald-200">
        <CheckCircle className="w-3 h-3" /> Sustained
      </span>
    );
  }
  if (classification === "false_positive") {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-rose-50 text-rose-700 border border-rose-200">
        <AlertTriangle className="w-3 h-3" /> Dropped Off
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-slate-100 text-slate-600 border border-slate-200">
      <Clock className="w-3 h-3" /> Pending
    </span>
  );
}

export default function CohortTable({ summary, repos, reportLabel }: CohortTableProps) {
  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm flex items-center justify-between">
          <div>
            <p className="text-sm font-medium text-slate-500">Total Flagged</p>
            <p className="text-2xl font-bold text-slate-900 mt-1">{summary.total}</p>
          </div>
          <div className="w-10 h-10 rounded-full bg-blue-50 flex items-center justify-center">
            <BookOpen className="w-5 h-5 text-blue-600" />
          </div>
        </div>
        <div className="bg-white p-5 rounded-xl border border-emerald-200 shadow-sm flex items-center justify-between ring-1 ring-emerald-50">
          <div>
            <p className="text-sm font-medium text-emerald-600">Sustained Gems</p>
            <p className="text-2xl font-bold text-emerald-700 mt-1">{summary.sustained}</p>
          </div>
          <div className="w-10 h-10 rounded-full bg-emerald-100 flex items-center justify-center">
            <CheckCircle className="w-5 h-5 text-emerald-600" />
          </div>
        </div>
        <div className="bg-white p-5 rounded-xl border border-rose-200 shadow-sm flex items-center justify-between ring-1 ring-rose-50">
          <div>
            <p className="text-sm font-medium text-rose-600">Dropped Off</p>
            <p className="text-2xl font-bold text-rose-700 mt-1">{summary.dropped}</p>
          </div>
          <div className="w-10 h-10 rounded-full bg-rose-100 flex items-center justify-center">
            <AlertTriangle className="w-5 h-5 text-rose-600" />
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
        <div className="px-6 py-4 border-b border-slate-200 bg-slate-50">
          <h3 className="font-semibold text-slate-800">Cohort Analysis: {reportLabel}</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-white border-b border-slate-100 text-xs uppercase tracking-wider text-slate-500 font-semibold">
                <th className="p-4 pl-6">Repository</th>
                <th className="p-4">Classification</th>
                <th className="p-4">Score Trajectory</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {repos.map((repo) => (
                <tr key={repo.repo_id} className="hover:bg-slate-50 transition-colors">
                  <td className="p-4 pl-6">
                    <Link
                      href={`/hidden-gems/repos/${repo.full_name}`}
                      className="text-indigo-600 font-medium hover:underline flex items-center gap-1.5"
                    >
                      <BookOpen className="w-4 h-4 text-slate-400" />
                      {repo.full_name}
                    </Link>
                  </td>
                  <td className="p-4">
                    <ClassificationBadge classification={repo.classification} />
                  </td>
                  <td className="p-4">
                    <div className="flex items-center gap-3">
                      <span className="font-semibold text-slate-700">
                        {repo.prev_score.toFixed(1)}
                      </span>
                      {repo.current_score === null ? (
                        <Clock className="w-5 h-5 text-slate-400" />
                      ) : repo.current_score >= repo.prev_score ? (
                        <ArrowUpRight className="w-5 h-5 text-emerald-500" />
                      ) : (
                        <ArrowDownRight className="w-5 h-5 text-rose-500" />
                      )}
                      {repo.current_score !== null && (
                        <span className={`font-semibold ${repo.current_score >= 1.5 ? "text-emerald-600" : "text-rose-500"}`}>
                          {repo.current_score.toFixed(1)}
                        </span>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Verify TypeScript**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights/frontend
npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/HiddenGemTable.tsx frontend/src/components/CohortTable.tsx
git commit -m "feat: add HiddenGemTable and CohortTable components"
```

---

## Task 8: Search + ScoreHistoryChart + Detail Cards

**Files:**
- Create: `frontend/src/components/SearchBar.tsx`
- Create: `frontend/src/components/SearchResults.tsx`
- Create: `frontend/src/components/ScoreHistoryChart.tsx`
- Create: `frontend/src/components/RepoDetailCard.tsx`
- Create: `frontend/src/components/UserDetailCard.tsx`
- Create: `frontend/src/components/OrgDetailCard.tsx`

- [ ] **Step 1: Write SearchBar**

`frontend/src/components/SearchBar.tsx`:
```typescript
"use client";

import { Search } from "lucide-react";

interface SearchBarProps {
  value: string;
  scope: string;
  onChange: (q: string) => void;
  onScopeChange: (s: string) => void;
}

export default function SearchBar({ value, scope, onChange, onScopeChange }: SearchBarProps) {
  return (
    <div className="relative flex items-center w-full max-w-2xl">
      <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
        <Search className="h-5 w-5 text-slate-400" />
      </div>
      <input
        type="text"
        className="block w-full pl-10 pr-24 py-2 border border-slate-300 rounded-lg bg-slate-50 placeholder-slate-400 focus:outline-none focus:bg-white focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm transition-all"
        placeholder="Search repositories, users, or organisations…"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      <div className="absolute inset-y-0 right-0 flex items-center">
        <select
          value={scope}
          onChange={(e) => onScopeChange(e.target.value)}
          className="h-full py-0 pl-2 pr-7 border-transparent bg-transparent text-slate-500 sm:text-sm rounded-r-md focus:ring-indigo-500 focus:border-indigo-500"
        >
          <option value="all">All</option>
          <option value="repos">Repos</option>
          <option value="users">Users</option>
          <option value="orgs">Orgs</option>
        </select>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Write SearchResults**

`frontend/src/components/SearchResults.tsx`:
```typescript
"use client";

import Link from "next/link";
import { BookOpen, User, Building2 } from "lucide-react";
import type { SearchResult } from "@/types/hidden_gems";

interface SearchResultsProps {
  results: SearchResult[];
  onClose: () => void;
}

const TYPE_CONFIG = {
  repo:  { icon: BookOpen,   label: "Repo",  href: (n: string) => `/hidden-gems/repos/${n}` },
  user:  { icon: User,       label: "User",  href: (n: string) => `/hidden-gems/users/${n}` },
  org:   { icon: Building2,  label: "Org",   href: (n: string) => `/hidden-gems/orgs/${n}`  },
} as const;

export default function SearchResults({ results, onClose }: SearchResultsProps) {
  if (results.length === 0) {
    return (
      <div className="absolute top-full mt-1 w-full bg-white border border-slate-200 rounded-lg shadow-lg z-50 p-4 text-sm text-slate-500 text-center">
        No results found
      </div>
    );
  }

  return (
    <div className="absolute top-full mt-1 w-full bg-white border border-slate-200 rounded-lg shadow-lg z-50 overflow-hidden">
      <ul className="divide-y divide-slate-100 max-h-80 overflow-y-auto">
        {results.map((r) => {
          const cfg = TYPE_CONFIG[r.type];
          const Icon = cfg.icon;
          return (
            <li key={`${r.type}:${r.name}`}>
              <Link
                href={cfg.href(r.name)}
                onClick={onClose}
                className="flex items-center gap-3 px-4 py-3 hover:bg-slate-50 transition-colors"
              >
                <Icon className="w-4 h-4 text-slate-400 flex-shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-slate-800 truncate">{r.name}</p>
                  <p className="text-xs text-slate-500">{cfg.label}</p>
                </div>
                {r.score !== null && (
                  <span className={`text-xs font-semibold ${r.score >= 3 ? "text-emerald-600" : "text-amber-500"}`}>
                    {r.score.toFixed(2)}
                  </span>
                )}
              </Link>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
```

- [ ] **Step 3: Write ScoreHistoryChart**

`frontend/src/components/ScoreHistoryChart.tsx`:
```typescript
import { LineChart } from "@tremor/react";
import type { ScoreHistoryPoint, UserHistoryPoint, OrgHistoryPoint } from "@/types/hidden_gems";

type HistoryPoint = ScoreHistoryPoint | UserHistoryPoint | OrgHistoryPoint;

interface ScoreHistoryChartProps {
  data: HistoryPoint[];
  scoreKey: string;
  title?: string;
}

export default function ScoreHistoryChart({
  data,
  scoreKey,
  title = "Score History",
}: ScoreHistoryChartProps) {
  const chartData = data.map((p) => ({
    date: new Date(p.run_at).toLocaleDateString("en-CH", {
      month: "short",
      day: "numeric",
    }),
    Score: (p as Record<string, unknown>)[scoreKey] as number ?? 0,
  }));

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
      <h3 className="font-semibold text-slate-800 mb-4">{title}</h3>
      {chartData.length === 0 ? (
        <p className="text-sm text-slate-500 text-center py-8">
          No snapshot history yet. History builds up as the scheduler runs.
        </p>
      ) : (
        <LineChart
          data={chartData}
          index="date"
          categories={["Score"]}
          colors={["indigo"]}
          showLegend={false}
          yAxisWidth={48}
          className="h-48"
        />
      )}
    </div>
  );
}
```

- [ ] **Step 4: Write RepoDetailCard**

`frontend/src/components/RepoDetailCard.tsx`:
```typescript
import { Star, GitFork, Code, Shield } from "lucide-react";
import type { HiddenGemRepo } from "@/types/hidden_gems";

interface RepoDetailCardProps {
  repo: HiddenGemRepo;
}

export default function RepoDetailCard({ repo }: RepoDetailCardProps) {
  const isSignificant = repo.sig_score >= 3.0;

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">{repo.full_name}</h1>
          <div className="flex flex-wrap items-center gap-3 mt-3 text-sm">
            {repo.language && (
              <span className="flex items-center gap-1 text-slate-600">
                <Code className="w-4 h-4" /> {repo.language}
              </span>
            )}
            {repo.license_spdx && (
              <span className="flex items-center gap-1 text-slate-600">
                <Shield className="w-4 h-4" /> {repo.license_spdx}
              </span>
            )}
          </div>
          {repo.topics.length > 0 && (
            <div className="flex flex-wrap gap-2 mt-3">
              {repo.topics.map((t) => (
                <span key={t} className="px-2 py-0.5 text-xs bg-indigo-50 text-indigo-700 rounded-full border border-indigo-100">
                  {t}
                </span>
              ))}
            </div>
          )}
        </div>

        <div className={`text-right px-4 py-3 rounded-xl ${isSignificant ? "bg-emerald-50 border border-emerald-200" : "bg-amber-50 border border-amber-200"}`}>
          <p className={`text-3xl font-bold ${isSignificant ? "text-emerald-700" : "text-amber-600"}`}>
            {repo.sig_score.toFixed(2)}
          </p>
          <p className={`text-xs mt-1 ${isSignificant ? "text-emerald-600" : "text-amber-500"}`}>
            {isSignificant ? "≥ 95% Confidence" : "< 95% Confidence"}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-6">
        {[
          { icon: Star, label: "Total Stars", value: repo.total_stars.toLocaleString(), delta: `+${repo.count_stars_interval}` },
          { icon: GitFork, label: "Total Forks", value: repo.total_forks.toLocaleString(), delta: `+${repo.count_forks_interval}` },
        ].map(({ icon: Icon, label, value, delta }) => (
          <div key={label} className="bg-slate-50 rounded-lg p-3 border border-slate-200">
            <div className="flex items-center gap-2 text-slate-500 text-xs mb-1">
              <Icon className="w-3.5 h-3.5" /> {label}
            </div>
            <p className="font-semibold text-slate-900">{value}</p>
            <p className="text-xs text-emerald-600 font-medium">{delta} this window</p>
          </div>
        ))}
      </div>
    </div>
  );
}
```

- [ ] **Step 5: Write UserDetailCard**

`frontend/src/components/UserDetailCard.tsx`:
```typescript
import Link from "next/link";
import { BookOpen, Star } from "lucide-react";
import type { HiddenGemUser } from "@/types/hidden_gems";

interface UserDetailCardProps {
  user: HiddenGemUser;
  repos: { full_name: string; sig_score: number; language: string | null; total_stars: number }[];
}

export default function UserDetailCard({ user, repos }: UserDetailCardProps) {
  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">{user.username}</h1>
            <p className="text-slate-500 mt-1 text-sm">
              {user.hidden_gem_count} hidden gems · {user.active_repos_in_window} active repos in window
            </p>
            {user.best_repo && (
              <p className="text-sm text-slate-600 mt-2">
                Best repo:{" "}
                <Link href={`/hidden-gems/repos/${user.best_repo}`} className="text-indigo-600 hover:underline">
                  {user.best_repo}
                </Link>
              </p>
            )}
          </div>
          <div className="text-right px-4 py-3 rounded-xl bg-emerald-50 border border-emerald-200">
            <p className="text-3xl font-bold text-emerald-700">{user.total_score.toFixed(2)}</p>
            <p className="text-xs mt-1 text-emerald-600">Total Score</p>
          </div>
        </div>
      </div>

      {repos.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-100 bg-slate-50">
            <h3 className="font-semibold text-slate-800">Scored Repositories</h3>
          </div>
          <ul className="divide-y divide-slate-100">
            {repos.map((r) => (
              <li key={r.full_name} className="flex items-center justify-between px-6 py-3 hover:bg-slate-50">
                <Link
                  href={`/hidden-gems/repos/${r.full_name}`}
                  className="flex items-center gap-2 text-indigo-600 font-medium hover:underline text-sm"
                >
                  <BookOpen className="w-4 h-4 text-slate-400" />
                  {r.full_name}
                </Link>
                <div className="flex items-center gap-4 text-sm text-slate-600">
                  <span className="flex items-center gap-1">
                    <Star className="w-3.5 h-3.5 text-amber-400" />
                    {r.total_stars.toLocaleString()}
                  </span>
                  <span className={`font-semibold ${r.sig_score >= 3 ? "text-emerald-600" : "text-amber-500"}`}>
                    {r.sig_score.toFixed(2)}
                  </span>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 6: Write OrgDetailCard**

`frontend/src/components/OrgDetailCard.tsx`:
```typescript
import type { HiddenGemOrg } from "@/types/hidden_gems";

interface OrgDetailCardProps {
  org: HiddenGemOrg;
}

function StatBox({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="bg-slate-50 rounded-lg p-3 border border-slate-200">
      <p className="text-xs text-slate-500 mb-1">{label}</p>
      <p className="font-semibold text-slate-900">{value}</p>
    </div>
  );
}

export default function OrgDetailCard({ org }: OrgDetailCardProps) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
      <div className="flex items-start justify-between gap-4">
        <h1 className="text-2xl font-bold text-slate-900">{org.org_login}</h1>
        <div className="text-right px-4 py-3 rounded-xl bg-emerald-50 border border-emerald-200">
          <p className="text-3xl font-bold text-emerald-700">
            {org.org_repos_total_score.toFixed(2)}
          </p>
          <p className="text-xs mt-1 text-emerald-600">Org Score</p>
        </div>
      </div>

      <div className="mt-6 space-y-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Org-Owned Repos</p>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatBox label="Total Score"    value={org.org_repos_total_score.toFixed(2)} />
            <StatBox label="Best Score"     value={org.org_repos_best_score.toFixed(2)} />
            <StatBox label="Active Repos"   value={org.org_active_repos} />
            <StatBox label="Gems (≥3.0)"    value={org.org_hidden_gem_count} />
          </div>
        </div>
        <div>
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Member Repos</p>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatBox label="Total Score"    value={org.member_repos_total_score.toFixed(2)} />
            <StatBox label="Best Score"     value={org.member_repos_best_score.toFixed(2)} />
            <StatBox label="Active Repos"   value={org.member_active_repos} />
            <StatBox label="Active Users"   value={org.member_active_users} />
          </div>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 7: Verify TypeScript**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights/frontend
npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 8: Commit**

```bash
git add frontend/src/components/SearchBar.tsx \
        frontend/src/components/SearchResults.tsx \
        frontend/src/components/ScoreHistoryChart.tsx \
        frontend/src/components/RepoDetailCard.tsx \
        frontend/src/components/UserDetailCard.tsx \
        frontend/src/components/OrgDetailCard.tsx
git commit -m "feat: add search, score history, and detail card components"
```

---

## Task 9: Main Hidden Gems Page

**Files:**
- Create: `frontend/src/app/hidden-gems/page.tsx`

- [ ] **Step 1: Write the main page**

`frontend/src/app/hidden-gems/page.tsx`:
```typescript
"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { Clock } from "lucide-react";
import HiddenGemFilters, { type Timeframe, type Scope } from "@/components/HiddenGemFilters";
import HiddenGemTable from "@/components/HiddenGemTable";
import CohortTable from "@/components/CohortTable";
import SearchBar from "@/components/SearchBar";
import SearchResults from "@/components/SearchResults";
import type {
  HiddenGemRepo, HiddenGemUser, HiddenGemOrg,
  SnapshotRun, CohortResponse, SearchResult,
} from "@/types/hidden_gems";

type ActiveView = "dashboard" | "reports";

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json() as Promise<T>;
}

function qs(p: Record<string, string | number | string[] | undefined>): string {
  const parts: string[] = [];
  for (const [k, v] of Object.entries(p)) {
    if (v === undefined) continue;
    if (Array.isArray(v)) v.forEach((i) => parts.push(`${k}=${encodeURIComponent(i)}`));
    else parts.push(`${k}=${encodeURIComponent(v)}`);
  }
  return parts.length ? `?${parts.join("&")}` : "";
}

export default function HiddenGemsPage() {
  const [activeView, setActiveView] = useState<ActiveView>("dashboard");

  // Dashboard state
  const [timeframe, setTimeframe] = useState<Timeframe>("168");
  const [scope, setScope]         = useState<Scope>("repos");
  const [language, setLanguage]   = useState("");
  const [license, setLicense]     = useState("");
  const [topic, setTopic]         = useState("");
  const [page, setPage]           = useState(1);
  const [items, setItems]         = useState<(HiddenGemRepo | HiddenGemUser | HiddenGemOrg)[]>([]);
  const [loading, setLoading]     = useState(false);

  // Filter options (loaded once)
  const [languages, setLanguages] = useState<string[]>([]);
  const [licenses, setLicenses]   = useState<string[]>([]);
  const [topics, setTopics]       = useState<string[]>([]);

  // Search state
  const [searchQuery, setSearchQuery]   = useState("");
  const [searchScope, setSearchScope]   = useState("all");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [showResults, setShowResults]   = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  // Reports state
  const [snapshots, setSnapshots]       = useState<SnapshotRun[]>([]);
  const [selectedSnapshotId, setSelectedSnapshotId] = useState<number | null>(null);
  const [cohort, setCohort]             = useState<CohortResponse | null>(null);

  // Load filter options once
  useEffect(() => {
    Promise.all([
      get<string[]>("/api/hidden-gems/filters/languages"),
      get<string[]>("/api/hidden-gems/filters/licenses"),
      get<string[]>("/api/hidden-gems/filters/topics"),
    ]).then(([langs, lics, tops]) => {
      setLanguages(langs);
      setLicenses(lics);
      setTopics(tops);
    }).catch(() => {/* filters are optional */});
  }, []);

  // Load live ranking
  const loadLive = useCallback(() => {
    setLoading(true);
    const params = {
      hours: timeframe,
      scope,
      page,
      limit: 25,
      ...(language ? { language: [language] } : {}),
      ...(license  ? { license: [license] }   : {}),
      ...(topic    ? { topic: [topic] }        : {}),
    };
    get<{ items: (HiddenGemRepo | HiddenGemUser | HiddenGemOrg)[] }>(
      `/api/hidden-gems/live${qs(params as Record<string, string | number | string[] | undefined>)}`
    )
      .then((d) => setItems(d.items))
      .catch(() => setItems([]))
      .finally(() => setLoading(false));
  }, [timeframe, scope, language, license, topic, page]);

  useEffect(() => {
    if (activeView === "dashboard") loadLive();
  }, [activeView, loadLive]);

  // Load snapshots for reports view
  useEffect(() => {
    if (activeView !== "reports") return;
    get<SnapshotRun[]>(`/api/hidden-gems/snapshots${qs({ interval_hours: timeframe, limit: 20 })}`)
      .then((runs) => {
        setSnapshots(runs);
        if (runs.length > 0 && selectedSnapshotId === null) {
          setSelectedSnapshotId(runs[0].id);
        }
      })
      .catch(() => setSnapshots([]));
  }, [activeView, timeframe, selectedSnapshotId]);

  // Load cohort when snapshot selected
  useEffect(() => {
    if (selectedSnapshotId === null) return;
    get<CohortResponse>(`/api/hidden-gems/snapshots/${selectedSnapshotId}/cohort`)
      .then(setCohort)
      .catch(() => setCohort(null));
  }, [selectedSnapshotId]);

  // Search with debounce
  useEffect(() => {
    if (searchQuery.length < 2) {
      setSearchResults([]);
      setShowResults(false);
      return;
    }
    const timer = setTimeout(() => {
      get<{ items: SearchResult[] }>(
        `/api/hidden-gems/search${qs({ q: searchQuery, scope: searchScope, limit: 10 })}`
      )
        .then((d) => {
          setSearchResults(d.items);
          setShowResults(true);
        })
        .catch(() => setSearchResults([]));
    }, 300);
    return () => clearTimeout(timer);
  }, [searchQuery, searchScope]);

  // Close search on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowResults(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectedSnapshot = snapshots.find((s) => s.id === selectedSnapshotId);

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Hidden Gems</h1>
          <p className="mt-1 text-slate-600">
            Repositories showing statistically significant growth, ranked by{" "}
            <code className="bg-slate-100 px-1.5 py-0.5 rounded text-xs">
              sig_score = -ln(1 − Poisson-CDF)
            </code>
          </p>
        </div>

        {/* View toggle */}
        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {(["dashboard", "reports"] as const).map((v) => (
            <button
              key={v}
              onClick={() => setActiveView(v)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
                activeView === v
                  ? "bg-indigo-50 text-indigo-700 shadow-sm"
                  : "text-slate-600 hover:text-slate-900 hover:bg-slate-50"
              }`}
            >
              {v === "dashboard" ? "Ranking" : "Evaluation Reports"}
            </button>
          ))}
        </div>
      </div>

      {/* Search bar */}
      <div ref={searchRef} className="relative">
        <SearchBar
          value={searchQuery}
          scope={searchScope}
          onChange={setSearchQuery}
          onScopeChange={setSearchScope}
        />
        {showResults && (
          <SearchResults
            results={searchResults}
            onClose={() => { setShowResults(false); setSearchQuery(""); }}
          />
        )}
      </div>

      {activeView === "dashboard" ? (
        <>
          <HiddenGemFilters
            timeframe={timeframe}
            scope={scope}
            language={language}
            license={license}
            topic={topic}
            languages={languages}
            licenses={licenses}
            topics={topics}
            onTimeframeChange={(t) => { setTimeframe(t); setPage(1); }}
            onScopeChange={(s)     => { setScope(s);     setPage(1); }}
            onLanguageChange={(l)  => { setLanguage(l);  setPage(1); }}
            onLicenseChange={(l)   => { setLicense(l);   setPage(1); }}
            onTopicChange={(t)     => { setTopic(t);     setPage(1); }}
          />
          {loading ? (
            <div className="flex items-center justify-center py-16 text-slate-400">Loading…</div>
          ) : (
            <HiddenGemTable
              items={items}
              scope={scope}
              page={page}
              onPageChange={setPage}
            />
          )}
        </>
      ) : (
        <div className="space-y-6">
          {/* Snapshot selector */}
          <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm px-3 py-2 w-fit">
            <Clock className="w-4 h-4 text-slate-400 mr-2" />
            <select
              value={selectedSnapshotId ?? ""}
              onChange={(e) => setSelectedSnapshotId(Number(e.target.value))}
              className="bg-transparent text-sm font-medium text-slate-700 outline-none border-none p-0"
            >
              {snapshots.length === 0 && (
                <option value="">No snapshots yet</option>
              )}
              {snapshots.map((s) => (
                <option key={s.id} value={s.id}>
                  {new Date(s.run_at).toLocaleDateString("en-CH", {
                    day: "numeric", month: "short", year: "numeric",
                  })}
                  {" "}— {s.repo_count ?? 0} repos
                </option>
              ))}
            </select>
          </div>

          {cohort ? (
            <CohortTable
              summary={cohort.summary}
              repos={cohort.repos}
              reportLabel={
                selectedSnapshot
                  ? new Date(selectedSnapshot.run_at).toLocaleDateString("en-CH", {
                      day: "numeric", month: "long", year: "numeric",
                    })
                  : ""
              }
            />
          ) : (
            <div className="flex items-center justify-center py-16 text-slate-400">
              {snapshots.length === 0
                ? "No snapshots yet — the scheduler will capture the first one on startup."
                : "Loading cohort…"}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Verify TypeScript**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights/frontend
npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/hidden-gems/
git commit -m "feat: add main Hidden Gems page with ranking and evaluation report views"
```

---

## Task 10: Detail Pages

**Files:**
- Create: `frontend/src/app/hidden-gems/repos/[...slug]/page.tsx`
- Create: `frontend/src/app/hidden-gems/users/[username]/page.tsx`
- Create: `frontend/src/app/hidden-gems/orgs/[org_login]/page.tsx`

- [ ] **Step 1: Write repo detail page**

`frontend/src/app/hidden-gems/repos/[...slug]/page.tsx`:
```typescript
import Link from "next/link";
import { ChevronLeft } from "lucide-react";
import RepoDetailCard from "@/components/RepoDetailCard";
import ScoreHistoryChart from "@/components/ScoreHistoryChart";
import type { RepoDetailResponse } from "@/types/hidden_gems";

export const dynamic = "force-dynamic";

const BASE_URL = process.env.API_URL ?? "http://localhost:8000";

interface Props {
  params: Promise<{ slug: string[] }>;
  searchParams: Promise<{ interval_hours?: string }>;
}

export default async function RepoDetailPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const { interval_hours = "168" } = await searchParams;
  const fullName = slug.join("/");

  let data: RepoDetailResponse | null = null;
  try {
    const res = await fetch(
      `${BASE_URL}/api/hidden-gems/repos/${encodeURIComponent(fullName)}?interval_hours=${interval_hours}`,
      { cache: "no-store" }
    );
    if (res.ok) data = await res.json();
  } catch {
    // data stays null
  }

  return (
    <div className="space-y-6">
      <Link href="/hidden-gems" className="inline-flex items-center gap-1 text-sm text-slate-500 hover:text-indigo-600 transition-colors">
        <ChevronLeft className="w-4 h-4" /> Back to Hidden Gems
      </Link>

      {data?.current ? (
        <>
          <RepoDetailCard repo={data.current} />
          <ScoreHistoryChart
            data={data.history}
            scoreKey="sig_score"
            title="Significance Score History"
          />
        </>
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-8 text-center text-slate-500">
          <p className="font-medium">No data found for <code>{fullName}</code></p>
          <p className="text-sm mt-2">This repo may not have appeared in any scored window yet.</p>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Write user detail page**

`frontend/src/app/hidden-gems/users/[username]/page.tsx`:
```typescript
import Link from "next/link";
import { ChevronLeft } from "lucide-react";
import UserDetailCard from "@/components/UserDetailCard";
import ScoreHistoryChart from "@/components/ScoreHistoryChart";
import type { UserDetailResponse } from "@/types/hidden_gems";

export const dynamic = "force-dynamic";

const BASE_URL = process.env.API_URL ?? "http://localhost:8000";

interface Props {
  params: Promise<{ username: string }>;
  searchParams: Promise<{ interval_hours?: string }>;
}

export default async function UserDetailPage({ params, searchParams }: Props) {
  const { username } = await params;
  const { interval_hours = "168" } = await searchParams;

  let data: UserDetailResponse | null = null;
  try {
    const res = await fetch(
      `${BASE_URL}/api/hidden-gems/users/${encodeURIComponent(username)}?interval_hours=${interval_hours}`,
      { cache: "no-store" }
    );
    if (res.ok) data = await res.json();
  } catch {
    // data stays null
  }

  return (
    <div className="space-y-6">
      <Link href="/hidden-gems" className="inline-flex items-center gap-1 text-sm text-slate-500 hover:text-indigo-600 transition-colors">
        <ChevronLeft className="w-4 h-4" /> Back to Hidden Gems
      </Link>

      {data?.current ? (
        <>
          <UserDetailCard
            user={data.current}
            repos={data.repos as { full_name: string; sig_score: number; language: string | null; total_stars: number }[]}
          />
          <ScoreHistoryChart
            data={data.history}
            scoreKey="total_score"
            title="Total Score History"
          />
        </>
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-8 text-center text-slate-500">
          <p className="font-medium">No data found for <code>{username}</code></p>
          <p className="text-sm mt-2">This user may not have appeared in any scored window yet.</p>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 3: Write org detail page**

`frontend/src/app/hidden-gems/orgs/[org_login]/page.tsx`:
```typescript
import Link from "next/link";
import { ChevronLeft } from "lucide-react";
import OrgDetailCard from "@/components/OrgDetailCard";
import ScoreHistoryChart from "@/components/ScoreHistoryChart";
import type { OrgDetailResponse } from "@/types/hidden_gems";

export const dynamic = "force-dynamic";

const BASE_URL = process.env.API_URL ?? "http://localhost:8000";

interface Props {
  params: Promise<{ org_login: string }>;
  searchParams: Promise<{ interval_hours?: string }>;
}

export default async function OrgDetailPage({ params, searchParams }: Props) {
  const { org_login } = await params;
  const { interval_hours = "168" } = await searchParams;

  let data: OrgDetailResponse | null = null;
  try {
    const res = await fetch(
      `${BASE_URL}/api/hidden-gems/orgs/${encodeURIComponent(org_login)}?interval_hours=${interval_hours}`,
      { cache: "no-store" }
    );
    if (res.ok) data = await res.json();
  } catch {
    // data stays null
  }

  return (
    <div className="space-y-6">
      <Link href="/hidden-gems" className="inline-flex items-center gap-1 text-sm text-slate-500 hover:text-indigo-600 transition-colors">
        <ChevronLeft className="w-4 h-4" /> Back to Hidden Gems
      </Link>

      {data?.current ? (
        <>
          <OrgDetailCard org={data.current} />
          <ScoreHistoryChart
            data={data.history}
            scoreKey="org_repos_total_score"
            title="Org Score History"
          />
        </>
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-8 text-center text-slate-500">
          <p className="font-medium">No data found for <code>{org_login}</code></p>
          <p className="text-sm mt-2">This organisation may not have appeared in any scored window yet.</p>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 4: Verify TypeScript**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights/frontend
npx tsc --noEmit
```

Expected: zero errors.

- [ ] **Step 5: Build check**

```bash
npm run build 2>&1 | tail -20
```

Expected: `✓ Compiled successfully` with no type errors. Warnings about `export const dynamic` are acceptable.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/app/hidden-gems/
git commit -m "feat: add repo, user, and org detail pages for hidden gems"
```

---

## Task 11: Full Integration Smoke Test

- [ ] **Step 1: Rebuild and start all services**

```bash
cd /Users/schiba/Projects/zhaw/pm4-github-insights
docker compose build
docker compose up -d
```

- [ ] **Step 2: Wait for scheduler first run**

```bash
sleep 10
docker compose logs api | grep -E "Snapshot complete|Snapshot start|ERROR"
```

Expected output contains: `Starting snapshot: interval=24h` and then `Snapshot complete: id=1 interval=24h`

- [ ] **Step 3: Verify API endpoints**

```bash
# Live ranking
curl -s "http://localhost:8000/api/hidden-gems/live?hours=24&scope=repos&limit=5" | python3 -m json.tool | head -30

# Snapshot list
curl -s "http://localhost:8000/api/hidden-gems/snapshots?interval_hours=24" | python3 -m json.tool

# Search (replace 'test' with a real actor from your data)
curl -s "http://localhost:8000/api/hidden-gems/search?q=test&scope=all" | python3 -m json.tool
```

- [ ] **Step 4: Check frontend loads**

Open `http://localhost:3000/hidden-gems` in a browser.

Verify:
- Page loads without errors
- "Hidden Gems" appears in the sidebar nav (active)
- Ranking tab shows a table (may be empty if no data)
- Evaluation Reports tab shows the snapshot selector

- [ ] **Step 5: Final commit**

```bash
git add -A
git status  # verify nothing sensitive is staged
git commit -m "chore: final hidden gems integration — scheduler, API, frontend complete"
```
