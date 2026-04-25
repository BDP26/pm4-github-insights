# Overview & Activity Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove dead Repositories/Contributors pages, upgrade Overview to a 6-KPI + activity heatmap dashboard, and build an Activity page with a Repos/Users/Orgs impact-score leaderboard.

**Architecture:** A new FastAPI router (`api/routers/activity.py`) provides two endpoints consumed by two new React components (`ActivityHeatmap`, `ActivityTable`). The Activity page mirrors the Hidden Gems page structure — `"use client"` with `useTransition`+`useDeferredValue` for instant scope switching. The Overview page stays a server component; the heatmap component is a client island fetching on mount.

**Tech Stack:** Next.js 16 / React 19, Tremor v3, Tailwind CSS 4, FastAPI + asyncpg, cachetools TTLCache, pytest with FastAPI TestClient.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| DELETE | `frontend/src/app/repositories/page.tsx` | Removed |
| DELETE | `frontend/src/app/contributors/page.tsx` | Removed |
| MODIFY | `frontend/src/components/Sidebar.tsx` | Remove 2 nav entries |
| MODIFY | `api/main.py` | Add 2 KPI fields, register activity router |
| MODIFY | `frontend/src/lib/api.ts` | Parse 2 new KPI fields, add `fetchHeatmap()` |
| MODIFY | `frontend/src/app/page.tsx` | 6-col KPI grid + `<ActivityHeatmap />` |
| CREATE | `api/routers/activity.py` | `/api/overview/heatmap` + `/api/activity/leaderboard` |
| CREATE | `api/tests/test_activity.py` | Pytest tests for both new endpoints |
| CREATE | `frontend/src/types/activity.ts` | `ActivityRepoItem`, `ActivityUserItem`, `ActivityOrgItem` |
| CREATE | `frontend/src/components/ActivityHeatmap.tsx` | 52-week × 5-event-type heatmap grid |
| CREATE | `frontend/src/components/ActivityTable.tsx` | Ranked table for all 3 scopes |
| MODIFY | `frontend/src/app/activity/page.tsx` | Full rewrite — scope toggle + leaderboard |

---

## Task 1: Remove Dead Pages and Sidebar Links

**Files:**
- Delete: `frontend/src/app/repositories/page.tsx`
- Delete: `frontend/src/app/contributors/page.tsx`
- Modify: `frontend/src/components/Sidebar.tsx`

- [ ] **Step 1: Delete the two placeholder page files**

```bash
rm frontend/src/app/repositories/page.tsx
rm frontend/src/app/contributors/page.tsx
```

- [ ] **Step 2: Remove nav items from Sidebar**

Open `frontend/src/components/Sidebar.tsx`. The current `navItems` array is:

```typescript
const navItems: NavItem[] = [
  { label: "Overview",      href: "/"             },
  { label: "Repositories",  href: "/repositories" },
  { label: "Contributors",  href: "/contributors" },
  { label: "Activity",      href: "/activity"     },
  { label: "Hidden Gems",   href: "/hidden-gems"  },
];
```

Replace it with:

```typescript
const navItems: NavItem[] = [
  { label: "Overview",    href: "/"           },
  { label: "Activity",    href: "/activity"   },
  { label: "Hidden Gems", href: "/hidden-gems"},
];
```

- [ ] **Step 3: Verify the dev server starts without errors**

```bash
cd frontend && npm run build 2>&1 | tail -20
```

Expected: `✓ Compiled successfully` (no missing-page errors).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "feat: remove Repositories and Contributors pages"
```

---

## Task 2: Extend /api/kpis with Repos Tracked and Total Stars

**Files:**
- Modify: `api/main.py` (lines ~122–228, the `get_kpis` function)
- Modify: `frontend/src/lib/api.ts`
- Modify: `frontend/src/app/page.tsx`

- [ ] **Step 1: Add two new SQL queries inside `get_kpis` in `api/main.py`**

Inside the `async with pool.acquire() as conn:` block in `get_kpis`, after the existing `avg_review_hours` query, add:

```python
        # ── Repos tracked
        repos_tracked: int = await conn.fetchval(
            "SELECT COUNT(*) FROM repos"
        )

        # ── Total stars across all tracked repos
        total_stars: int = await conn.fetchval(
            "SELECT COALESCE(SUM(stargazers_count), 0) FROM repos"
        )
```

- [ ] **Step 2: Add the two new fields to the return dict in `get_kpis`**

The existing return dict ends with `"avgReviewHours": {...}`. Add after it:

```python
        "reposTracked": {
            "value": int(repos_tracked),
            "delta": None,
        },
        "totalStars": {
            "value": int(total_stars),
            "delta": None,
        },
```

- [ ] **Step 3: Parse the two new fields in `frontend/src/lib/api.ts`**

The existing `KpisResponse` interface is:

```typescript
interface KpisResponse {
  totalCommits:        KpiRaw;
  openPRs:             KpiRaw;
  activeContributors:  KpiRaw;
  avgReviewHours:      KpiRaw;
}
```

Replace with:

```typescript
interface KpisResponse {
  totalCommits:        KpiRaw;
  openPRs:             KpiRaw;
  activeContributors:  KpiRaw;
  avgReviewHours:      KpiRaw;
  reposTracked:        KpiRaw;
  totalStars:          KpiRaw;
}
```

In `fetchKpis()`, the returned array currently has 4 items. Append two more:

```typescript
    {
      title:     "Repos Tracked",
      value:     (d.reposTracked.value ?? 0).toLocaleString(),
      delta:     "—",
      deltaType: "unchanged",
    },
    {
      title:     "Total Stars",
      value:     (d.totalStars.value ?? 0).toLocaleString(),
      delta:     "—",
      deltaType: "unchanged",
    },
```

- [ ] **Step 4: Change the KPI grid in `frontend/src/app/page.tsx` from 4 to 6 columns**

Find the grid div:

```tsx
<div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
```

Replace with:

```tsx
<div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
```

- [ ] **Step 5: Verify the build still passes**

```bash
cd frontend && npm run build 2>&1 | tail -20
```

Expected: `✓ Compiled successfully`

- [ ] **Step 6: Commit**

```bash
git add api/main.py frontend/src/lib/api.ts frontend/src/app/page.tsx
git commit -m "feat: add Repos Tracked and Total Stars KPIs to overview"
```

---

## Task 3: Create activity router with heatmap and leaderboard endpoints

**Files:**
- Create: `api/routers/activity.py`
- Create: `api/tests/test_activity.py`
- Modify: `api/main.py` (register router)

- [ ] **Step 1: Write the failing tests first**

Create `api/tests/test_activity.py`:

```python
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
```

- [ ] **Step 2: Run tests — they must fail (module not found)**

```bash
cd api && python -m pytest tests/test_activity.py -v 2>&1 | head -30
```

Expected: `ModuleNotFoundError: No module named 'routers.activity'`

- [ ] **Step 3: Create `api/routers/activity.py`**

```python
"""
Activity Router
───────────────
GET /api/overview/heatmap?weeks=52
GET /api/activity/leaderboard?scope=repos|users|orgs&limit=20
"""
from typing import Any

import asyncpg
from cachetools import TTLCache
from fastapi import APIRouter, HTTPException, Query, Request

_heatmap_cache:     TTLCache = TTLCache(maxsize=32,  ttl=300)   # 5 min
_leaderboard_cache: TTLCache = TTLCache(maxsize=64,  ttl=60)    # 60 s

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

    async with _pool(request).acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                time_bucket('1 week', bucket)::date AS date,
                event_type,
                SUM(event_count)::int               AS count
            FROM event_stats_5m
            WHERE bucket >= NOW() - make_interval(weeks => $1::int)
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


# ── Leaderboard ───────────────────────────────────────────────────────────────

@router.get("/api/activity/leaderboard")
async def get_leaderboard(
    request: Request,
    scope: str = Query("repos"),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    if scope not in {"repos", "users", "orgs"}:
        raise HTTPException(
            status_code=422,
            detail="scope must be one of: repos, users, orgs",
        )

    cache_key = (scope, limit)
    if cache_key in _leaderboard_cache:
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
```

- [ ] **Step 4: Run tests — all must pass**

```bash
cd api && python -m pytest tests/test_activity.py -v
```

Expected:
```
PASSED tests/test_activity.py::test_heatmap_returns_list
PASSED tests/test_activity.py::test_heatmap_default_weeks_is_52
PASSED tests/test_activity.py::test_heatmap_cache_hit_skips_db
PASSED tests/test_activity.py::test_leaderboard_repos_returns_scope_and_items
PASSED tests/test_activity.py::test_leaderboard_users_returns_scope
PASSED tests/test_activity.py::test_leaderboard_orgs_returns_scope
PASSED tests/test_activity.py::test_leaderboard_invalid_scope_returns_422
PASSED tests/test_activity.py::test_leaderboard_cache_hit_skips_db
8 passed
```

- [ ] **Step 5: Register the router in `api/main.py`**

Find this line near the top of `api/main.py`:

```python
from routers.hidden_gems import router as hidden_gems_router
```

Add below it:

```python
from routers.activity import router as activity_router
```

Find:

```python
app.include_router(hidden_gems_router)
```

Add below it:

```python
app.include_router(activity_router)
```

- [ ] **Step 6: Run all API tests to confirm nothing broke**

```bash
cd api && python -m pytest tests/ -v 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add api/routers/activity.py api/tests/test_activity.py api/main.py
git commit -m "feat: add activity router with heatmap and leaderboard endpoints"
```

---

## Task 4: ActivityHeatmap frontend component

**Files:**
- Create: `frontend/src/components/ActivityHeatmap.tsx`

- [ ] **Step 1: Create `frontend/src/components/ActivityHeatmap.tsx`**

```tsx
"use client";

import { useEffect, useState } from "react";

interface HeatmapPoint {
  date: string;        // "YYYY-MM-DD" (week start)
  event_type: string;
  count: number;
}

const EVENT_TYPES = [
  "PushEvent",
  "PullRequestEvent",
  "IssuesEvent",
  "WatchEvent",
  "ForkEvent",
];

const EVENT_LABELS: Record<string, string> = {
  PushEvent:        "Push",
  PullRequestEvent: "PR",
  IssuesEvent:      "Issue",
  WatchEvent:       "Star",
  ForkEvent:        "Fork",
};

function intensityClass(count: number): string {
  if (count === 0)    return "bg-slate-100";
  if (count < 10)     return "bg-indigo-100";
  if (count < 50)     return "bg-indigo-300";
  if (count < 200)    return "bg-indigo-500";
  return "bg-indigo-700";
}

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export default function ActivityHeatmap() {
  const [data, setData] = useState<HeatmapPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API}/api/overview/heatmap?weeks=52`, { cache: "no-store" })
      .then((r) => r.json())
      .then((d: HeatmapPoint[]) => { setData(d); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  // Build a lookup: { "YYYY-MM-DD": { PushEvent: N, ... } }
  const lookup: Record<string, Record<string, number>> = {};
  for (const pt of data) {
    if (!lookup[pt.date]) lookup[pt.date] = {};
    lookup[pt.date][pt.event_type] = pt.count;
  }

  // Collect sorted unique week dates
  const weeks = Array.from(new Set(data.map((p) => p.date))).sort();

  if (loading) {
    return (
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
        <div className="h-4 w-40 bg-slate-200 rounded animate-pulse mb-4" />
        <div className="h-32 bg-slate-100 rounded animate-pulse" />
      </div>
    );
  }

  if (weeks.length === 0) {
    return (
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6 text-slate-400 text-sm">
        No heatmap data available yet.
      </div>
    );
  }

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
      <h2 className="text-base font-semibold text-slate-800 mb-4">
        Activity Heatmap — last 52 weeks
      </h2>
      <div className="overflow-x-auto">
        <div className="min-w-max">
          {EVENT_TYPES.map((et) => (
            <div key={et} className="flex items-center gap-1 mb-1">
              <span className="w-10 text-xs text-slate-500 text-right shrink-0">
                {EVENT_LABELS[et]}
              </span>
              <div className="flex gap-0.5 ml-1">
                {weeks.map((week) => {
                  const count = lookup[week]?.[et] ?? 0;
                  return (
                    <div
                      key={week}
                      title={`${week} · ${EVENT_LABELS[et]}: ${count}`}
                      className={`w-3 h-3 rounded-sm ${intensityClass(count)}`}
                    />
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </div>
      {/* Legend */}
      <div className="flex items-center gap-2 mt-3 text-xs text-slate-500">
        <span>Less</span>
        {["bg-slate-100", "bg-indigo-100", "bg-indigo-300", "bg-indigo-500", "bg-indigo-700"].map((cls) => (
          <div key={cls} className={`w-3 h-3 rounded-sm ${cls}`} />
        ))}
        <span>More</span>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd frontend && npx tsc --noEmit 2>&1 | grep -E "error|ActivityHeatmap"
```

Expected: no output (no errors).

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/ActivityHeatmap.tsx
git commit -m "feat: add ActivityHeatmap component"
```

---

## Task 5: Wire ActivityHeatmap into the Overview page

**Files:**
- Modify: `frontend/src/app/page.tsx`

- [ ] **Step 1: Import and add `<ActivityHeatmap />` to Overview**

Open `frontend/src/app/page.tsx`. Add the import at the top with the other component imports:

```tsx
import ActivityHeatmap from "@/components/ActivityHeatmap";
```

The current return JSX ends with:

```tsx
      {/* Live events table — appends new events via SSE */}
      <LiveEventsTable initialEvents={events} />
    </div>
```

Insert `<ActivityHeatmap />` between the Charts section and the LiveEventsTable:

```tsx
      {/* Charts */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <CommitsChart data={commits} />
        <RepoActivityChart data={repos} />
      </div>

      {/* Activity heatmap — client island, fetches on mount */}
      <ActivityHeatmap />

      {/* Live events table — appends new events via SSE */}
      <LiveEventsTable initialEvents={events} />
    </div>
```

- [ ] **Step 2: Build to confirm no errors**

```bash
cd frontend && npm run build 2>&1 | tail -20
```

Expected: `✓ Compiled successfully`

- [ ] **Step 3: Commit**

```bash
git add frontend/src/app/page.tsx
git commit -m "feat: add ActivityHeatmap to Overview page"
```

---

## Task 6: Activity page types and ActivityTable component

**Files:**
- Create: `frontend/src/types/activity.ts`
- Create: `frontend/src/components/ActivityTable.tsx`

- [ ] **Step 1: Create `frontend/src/types/activity.ts`**

```typescript
export type ActivityScope = "repos" | "users" | "orgs";

export interface ActivityRepoItem {
  repo_id: number;
  full_name: string;
  owner_login: string;
  language: string | null;
  total_stars: number;
  total_forks: number;
  impact_score: number;
}

export interface ActivityUserItem {
  username: string;
  location: string | null;
  total_repos: number;
  total_stars: number;
  total_forks: number;
  impact_score: number;
}

export interface ActivityOrgItem {
  org_login: string;
  total_repos: number;
  total_stars: number;
  total_forks: number;
  impact_score: number;
}

export type ActivityItem = ActivityRepoItem | ActivityUserItem | ActivityOrgItem;

export interface ActivityLeaderboardResponse {
  scope: ActivityScope;
  items: ActivityItem[];
}
```

- [ ] **Step 2: Create `frontend/src/components/ActivityTable.tsx`**

```tsx
"use client";

import Link from "next/link";
import { Star, GitFork, BookOpen } from "lucide-react";
import type {
  ActivityScope,
  ActivityItem,
  ActivityRepoItem,
  ActivityUserItem,
  ActivityOrgItem,
} from "@/types/activity";

interface ActivityTableProps {
  items: ActivityItem[];
  scope: ActivityScope;
}

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}k`;
  return n.toLocaleString();
}

function ScoreCell({ score }: { score: number }) {
  return (
    <span className="font-bold text-indigo-600 tabular-nums">
      {fmt(Math.round(score))}
    </span>
  );
}

function RepoRow({ item, rank }: { item: ActivityRepoItem; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors cursor-pointer">
      <td className="p-4 text-center text-sm font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/repos/${item.full_name}`}
          className="text-indigo-600 font-semibold hover:underline flex items-center gap-1"
        >
          <BookOpen className="w-4 h-4 text-slate-400 shrink-0" />
          {item.full_name}
        </Link>
        {item.language && (
          <span className="text-xs text-slate-500 mt-1 flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-blue-500 inline-block" />
            {item.language}
          </span>
        )}
      </td>
      <td className="p-4 text-sm text-slate-600">{item.owner_login}</td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <Star className="w-3.5 h-3.5 text-amber-400" />
          {fmt(item.total_stars)}
        </div>
      </td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <GitFork className="w-3.5 h-3.5 text-slate-400" />
          {fmt(item.total_forks)}
        </div>
      </td>
      <td className="p-4"><ScoreCell score={item.impact_score} /></td>
    </tr>
  );
}

function UserRow({ item, rank }: { item: ActivityUserItem; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors cursor-pointer">
      <td className="p-4 text-center text-sm font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/users/${item.username}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.username}
        </Link>
        {item.location && (
          <p className="text-xs text-slate-500 mt-0.5">{item.location}</p>
        )}
      </td>
      <td className="p-4 text-sm text-slate-600">{item.total_repos}</td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <Star className="w-3.5 h-3.5 text-amber-400" />
          {fmt(item.total_stars)}
        </div>
      </td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <GitFork className="w-3.5 h-3.5 text-slate-400" />
          {fmt(item.total_forks)}
        </div>
      </td>
      <td className="p-4"><ScoreCell score={item.impact_score} /></td>
    </tr>
  );
}

function OrgRow({ item, rank }: { item: ActivityOrgItem; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors cursor-pointer">
      <td className="p-4 text-center text-sm font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/orgs/${item.org_login}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.org_login}
        </Link>
      </td>
      <td className="p-4 text-sm text-slate-600">{item.total_repos}</td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <Star className="w-3.5 h-3.5 text-amber-400" />
          {fmt(item.total_stars)}
        </div>
      </td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <GitFork className="w-3.5 h-3.5 text-slate-400" />
          {fmt(item.total_forks)}
        </div>
      </td>
      <td className="p-4"><ScoreCell score={item.impact_score} /></td>
    </tr>
  );
}

const HEADERS: Record<ActivityScope, string[]> = {
  repos: ["#", "Repository", "Owner",  "Stars", "Forks", "Score"],
  users: ["#", "Username",   "Repos",  "Stars", "Forks", "Score"],
  orgs:  ["#", "Org",        "Repos",  "Stars", "Forks", "Score"],
};

export default function ActivityTable({ items, scope }: ActivityTableProps) {
  if (items.length === 0) {
    return (
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-12 text-center text-slate-400">
        No data available.
      </div>
    );
  }

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-50 border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500 font-semibold">
              {HEADERS[scope].map((h) => (
                <th key={h} className="p-4">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {items.map((item, idx) => {
              const rank = idx + 1;
              if (scope === "repos") {
                return <RepoRow key={(item as ActivityRepoItem).repo_id} item={item as ActivityRepoItem} rank={rank} />;
              }
              if (scope === "users") {
                return <UserRow key={(item as ActivityUserItem).username} item={item as ActivityUserItem} rank={rank} />;
              }
              return <OrgRow key={(item as ActivityOrgItem).org_login} item={item as ActivityOrgItem} rank={rank} />;
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
```

- [ ] **Step 3: Verify TypeScript**

```bash
cd frontend && npx tsc --noEmit 2>&1 | grep -E "error|ActivityTable|activity"
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/types/activity.ts frontend/src/components/ActivityTable.tsx
git commit -m "feat: add Activity types and ActivityTable component"
```

---

## Task 7: Activity page with scope toggle and instant transitions

**Files:**
- Modify: `frontend/src/app/activity/page.tsx`

- [ ] **Step 1: Rewrite `frontend/src/app/activity/page.tsx`**

```tsx
"use client";

import { useState, useCallback, useEffect, useTransition, useDeferredValue } from "react";
import ActivityTable from "@/components/ActivityTable";
import type { ActivityScope, ActivityItem } from "@/types/activity";

const SCOPES: { value: ActivityScope; label: string }[] = [
  { value: "repos", label: "Repos" },
  { value: "users", label: "Users" },
  { value: "orgs",  label: "Orgs"  },
];

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export default function ActivityPage() {
  const [scope, setScope]   = useState<ActivityScope>("repos");
  const [items, setItems]   = useState<ActivityItem[]>([]);
  const [isPending, startTransition] = useTransition();
  const deferredItems = useDeferredValue(items);

  const load = useCallback((s: ActivityScope) => {
    startTransition(async () => {
      try {
        const res = await fetch(
          `${API}/api/activity/leaderboard?scope=${s}&limit=20`,
          { cache: "no-store" },
        );
        if (!res.ok) throw new Error(`${res.status}`);
        const body = await res.json();
        setItems(body.items);
      } catch {
        setItems([]);
      }
    });
  }, []);

  useEffect(() => { load(scope); }, [scope, load]);

  function handleScope(s: ActivityScope) {
    setScope(s);   // instant — UI switches immediately
    load(s);
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Activity</h1>
          <p className="mt-1 text-slate-600">
            Lifetime impact leaderboard — ranked by{" "}
            <code className="bg-slate-100 px-1.5 py-0.5 rounded text-xs">
              impact = stars × 1 + forks × 2
            </code>
          </p>
        </div>

        {/* Scope toggle — identical style to Hidden Gems */}
        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {SCOPES.map((s) => (
            <button
              key={s.value}
              type="button"
              onClick={() => handleScope(s.value)}
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

      {/* Table with loading overlay */}
      <div className="relative">
        {isPending && (
          <div className="absolute inset-x-0 top-0 z-10 flex items-center justify-center gap-2 bg-indigo-50 border border-indigo-100 rounded-lg py-2 text-sm font-medium text-indigo-600 shadow-sm">
            <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Updating leaderboard…
          </div>
        )}
        <div className={`transition-opacity duration-200 ${isPending ? "opacity-50 pointer-events-none mt-10" : ""}`}>
          <ActivityTable items={deferredItems} scope={scope} />
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Run TypeScript check**

```bash
cd frontend && npx tsc --noEmit 2>&1 | grep error
```

Expected: no output.

- [ ] **Step 3: Full build check**

```bash
cd frontend && npm run build 2>&1 | tail -20
```

Expected: `✓ Compiled successfully`

- [ ] **Step 4: Run all API tests one final time**

```bash
cd api && python -m pytest tests/ -v 2>&1 | tail -15
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/app/activity/page.tsx
git commit -m "feat: implement Activity page with scope toggle and instant transitions"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| Delete /repositories and /contributors | Task 1 |
| Remove sidebar links | Task 1 |
| 6 KPI cards (add Repos Tracked + Total Stars) | Task 2 |
| Heatmap endpoint GET /api/overview/heatmap | Task 3 |
| ActivityHeatmap component (52w × 5 types) | Task 4 |
| ActivityHeatmap wired into Overview page | Task 5 |
| Activity endpoint GET /api/activity/leaderboard | Task 3 |
| Leaderboard types (repos/users/orgs) | Task 6 |
| ActivityTable component | Task 6 |
| Scope toggle (repos/users/orgs) identical to HG | Task 7 |
| Instant transitions via useTransition + useDeferredValue | Task 7 |
| Row links to Hidden Gems detail pages | Task 6 (ActivityTable) |
| TTL caching on new endpoints | Task 3 (activity.py) |

**Placeholder scan:** No TBDs. All steps contain exact code. ✓

**Type consistency:**
- `ActivityScope`, `ActivityItem`, `ActivityRepoItem`, `ActivityUserItem`, `ActivityOrgItem` defined in Task 6 step 1 and used correctly in Task 6 step 2 and Task 7. ✓
- `HeatmapPoint` defined inside `ActivityHeatmap.tsx` (local, not exported — only used there). ✓
- `fmt()` defined and used within `ActivityTable.tsx` only. ✓
