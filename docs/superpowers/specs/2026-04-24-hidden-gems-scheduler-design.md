# Hidden Gems Scheduler & Dashboard — Design Spec

**Date:** 2026-04-24  
**Project:** pm4-github-insights  
**Status:** Approved

---

## Overview

Add a scheduled hidden gem reporting system to the GitHub Insights platform. The system periodically captures hidden gem scores (repos, users, orgs) using the existing PostgreSQL scoring functions and stores them as snapshots for historical tracking, cohort evaluation, and searchable score history.

A new Next.js page exposes this data as an interactive dashboard with a live ranking view, cohort evaluation reports, global search, and detail pages for repos, users, and orgs.

---

## 1. Database Schema

**New migration: `db/migrations/007_hidden_gem_snapshots.sql`**

### `hidden_gem_snapshot_runs`
One row per scheduled execution. Stores metadata about each snapshot run.

| Column | Type | Notes |
|---|---|---|
| `id` | SERIAL PRIMARY KEY | |
| `run_at` | TIMESTAMPTZ | Defaults to NOW() |
| `interval_hours` | INT | 24, 168, or 730 |
| `alpha` | FLOAT | Star weight (default 1.0) |
| `beta` | FLOAT | Fork weight (default 1.0) |
| `repo_count` | INT | Rows captured for repos |
| `user_count` | INT | Rows captured for users |
| `org_count` | INT | Rows captured for orgs |

### `hidden_gem_snapshot_repos`
Repo-level scores captured at each run.

| Column | Type |
|---|---|
| `snapshot_id` | INT FK → `hidden_gem_snapshot_runs.id` ON DELETE CASCADE |
| `repo_id` | INT |
| `full_name` | TEXT |
| `name` | TEXT |
| `owner_login` | TEXT |
| `language` | TEXT |
| `license_spdx` | TEXT |
| `topics` | TEXT[] |
| `sig_score` | FLOAT |
| `rank` | INT |
| `count_stars_interval` | INT |
| `count_forks_interval` | INT |
| `total_stars` | INT |
| `total_forks` | INT |

PRIMARY KEY: `(snapshot_id, repo_id)`

### `hidden_gem_snapshot_users`
User-level aggregates per run.

| Column | Type |
|---|---|
| `snapshot_id` | INT FK |
| `username` | TEXT |
| `total_score` | FLOAT |
| `best_repo_score` | FLOAT |
| `best_repo` | TEXT |
| `hidden_gem_count` | INT |
| `active_repos_in_window` | INT |

PRIMARY KEY: `(snapshot_id, username)`

### `hidden_gem_snapshot_orgs`
Org-level aggregates per run.

| Column | Type |
|---|---|
| `snapshot_id` | INT FK |
| `org_login` | TEXT |
| `org_repos_total_score` | FLOAT |
| `org_repos_best_score` | FLOAT |
| `org_active_repos` | INT |
| `org_hidden_gem_count` | INT |
| `member_repos_total_score` | FLOAT |
| `member_repos_best_score` | FLOAT |
| `member_active_repos` | INT |
| `member_active_users` | INT |
| `member_hidden_gem_count` | INT |

PRIMARY KEY: `(snapshot_id, org_login)`

### Indices
```sql
CREATE INDEX idx_snapshot_runs_interval    ON hidden_gem_snapshot_runs(interval_hours, run_at DESC);
CREATE INDEX idx_snapshot_repos_fullname   ON hidden_gem_snapshot_repos(full_name, snapshot_id);
CREATE INDEX idx_snapshot_users_username   ON hidden_gem_snapshot_users(username, snapshot_id);
CREATE INDEX idx_snapshot_orgs_login       ON hidden_gem_snapshot_orgs(org_login, snapshot_id);
```

---

## 2. Scheduler Architecture

### Design principle: extractable by construction

`SnapshotScheduler` has zero FastAPI imports. Its only external dependencies are an `asyncpg.Pool` and a `SnapshotConfig`. To move it to a standalone container later, create `scheduler/main.py` that builds the pool, instantiates the class, and calls `start()` — no changes to the class itself.

### Files

**`api/scheduler/snapshot_scheduler.py`**

```python
@dataclass(frozen=True)
class SnapshotConfig:
    interval_hours: list[int]   # e.g. [24, 168, 730]
    alpha: float = 1.0
    beta: float  = 1.0
    min_stars: int = 5
    min_forks: int = 1
    top_n: int = 1000

class SnapshotScheduler:
    def __init__(self, pool: asyncpg.Pool, config: SnapshotConfig) -> None
    async def start(self) -> None        # register APScheduler jobs, run first snapshot immediately
    async def stop(self)  -> None        # clean shutdown
    async def trigger(self, interval_hours: int) -> None   # manual trigger
    async def _run_snapshot(self, interval_hours: int) -> None  # core logic
```

**`api/scheduler/__init__.py`** — exports `SnapshotScheduler`, `SnapshotConfig`

### Snapshot execution flow (`_run_snapshot`)

1. Insert a `hidden_gem_snapshot_runs` row, capture `snapshot_id`
2. Call `hidden_gem_repo_scores(alpha, beta, hours, ...)` → bulk insert into `hidden_gem_snapshot_repos` with computed `rank`
3. Call `hidden_gem_user_scores(alpha, beta, hours)` → bulk insert into `hidden_gem_snapshot_users`
4. Call `hidden_gem_org_scores(alpha, beta, hours)` → bulk insert into `hidden_gem_snapshot_orgs`
5. Update `hidden_gem_snapshot_runs` with `repo_count`, `user_count`, `org_count`

### FastAPI wiring (`api/main.py` lifespan)

```python
scheduler = SnapshotScheduler(pool, config_from_env())
await scheduler.start()
# ... yield ...
await scheduler.stop()
```

### Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `SNAPSHOT_INTERVALS` | `24,168,730` | Comma-separated interval hours |
| `SNAPSHOT_ALPHA` | `1.0` | Star weight |
| `SNAPSHOT_BETA` | `1.0` | Fork weight |
| `SNAPSHOT_MIN_STARS` | `5` | Noise guard |
| `SNAPSHOT_MIN_FORKS` | `1` | Noise guard |
| `SNAPSHOT_TOP_N` | `1000` | Max repos per snapshot |

---

## 3. FastAPI Endpoints

All new endpoints live under the `/api/hidden-gems` prefix.

### Live Ranking
```
GET /api/hidden-gems/live
```
Calls the existing DB functions directly (always fresh, no snapshot). Returns paginated repo/user/org results.

Query params: `hours` (24/168/730), `scope` (repos/users/orgs, default repos), `language[]`, `license[]`, `topic[]`, `page`, `limit`

### Global Search
```
GET /api/hidden-gems/search
```
Searches `hidden_gem_snapshot_repos.full_name`, `hidden_gem_snapshot_users.username`, and `hidden_gem_snapshot_orgs.org_login` using `ILIKE`. Returns the latest known score alongside each result. Each result carries a `type` tag (`repo`/`user`/`org`) so the frontend can route to the correct detail page.

Query params: `q` (required), `scope` (all/repos/users/orgs), `page`, `limit`

### Repo Detail
```
GET /api/hidden-gems/repos/{full_name}
```
Returns current sig_score (from live DB function), repo metadata, and full score history across all snapshots grouped by `interval_hours`.

### User Detail
```
GET /api/hidden-gems/users/{username}
```
Returns current user aggregate, list of their currently scored repos, and score history across snapshots.

### Org Detail
```
GET /api/hidden-gems/orgs/{org_login}
```
Returns current org aggregate (org-owned + member breakdown), and score history across snapshots.

### Snapshot Runs List
```
GET /api/hidden-gems/snapshots
```
Query params: `interval_hours`, `limit` (default 20)

### Cohort Evaluation
```
GET /api/hidden-gems/snapshots/{id}/cohort
```
Returns repos from snapshot `id` with auto-classification based on comparison to the next snapshot of the same interval:

- `true_positive`: sig_score in next snapshot ≥ 1.5
- `false_positive`: sig_score drops below 1.5 or repo absent from next snapshot
- `pending`: no subsequent snapshot exists yet

Response also includes summary counts (total flagged, sustained gems, dropped off) for the stats cards.

### Manual Trigger (admin/demo)
```
POST /api/hidden-gems/snapshots/trigger
```
Body: `{ "interval_hours": 24 }`

---

## 4. Frontend

### New Routes

```
/hidden-gems                                     — main page (Dashboard + Evaluation Reports)
/hidden-gems/repos/[...full_name]                — repo detail page (catch-all: owner/repo has a slash)
/hidden-gems/users/[username]                    — user detail page
/hidden-gems/orgs/[org_login]                    — org detail page
```

### Navigation Flows

- Sidebar → `/hidden-gems` → Dashboard view (default)
- Dashboard ranking table row click → `/hidden-gems/repos/[full_name]`
- Header "Evaluation Reports" tab → Evaluation Reports view (same page, state toggle)
- Cohort table repo link → `/hidden-gems/repos/[full_name]`
- Search result click → `/hidden-gems/repos/[name]` or `/hidden-gems/users/[name]` or `/hidden-gems/orgs/[name]` based on `type` tag

### New Files

```
src/app/hidden-gems/page.tsx                         — page shell, owns activeView state
src/app/hidden-gems/repos/[...full_name]/page.tsx    — repo detail (catch-all for owner/repo slug)
src/app/hidden-gems/users/[username]/page.tsx        — user detail
src/app/hidden-gems/orgs/[org_login]/page.tsx        — org detail
src/components/HiddenGemFilters.tsx                  — timeframe + language/license/topic dropdowns
src/components/HiddenGemTable.tsx                    — ranked repo table (Dashboard view)
src/components/CohortTable.tsx                       — cohort analysis table (Evaluation Reports)
src/components/SearchResults.tsx                     — mixed search results list with type-based routing
src/components/ScoreHistoryChart.tsx                 — reusable Tremor LineChart for score over time
src/components/RepoDetailCard.tsx                    — repo detail header + stats
src/components/UserDetailCard.tsx                    — user detail header + repo list
src/components/OrgDetailCard.tsx                     — org detail header + breakdown
```

### Existing Files Updated

```
src/components/Sidebar.tsx     — add Hidden Gems nav link
src/lib/api.ts                 — add fetchHiddenGemsLive, fetchHiddenGemSearch,
                                   fetchHiddenGemRepo, fetchHiddenGemUser, fetchHiddenGemOrg,
                                   fetchSnapshotRuns, fetchSnapshotCohort
src/types/dashboard.ts         — add HiddenGemRepo, HiddenGemUser, HiddenGemOrg,
                                   SnapshotRun, CohortEntry, SearchResult types
```

### Filter Dropdown Data Source

Language, License, and Topic dropdowns are populated from the existing DB views (`v_repo_languages`, `v_repo_licenses`, `v_repo_topics`) built in migration 006. Three new lightweight FastAPI endpoints serve these:

```
GET /api/hidden-gems/filters/languages
GET /api/hidden-gems/filters/licenses
GET /api/hidden-gems/filters/topics
```

---

## 5. Auto-Classification Thresholds

| Classification | Condition |
|---|---|
| `true_positive` | sig_score ≥ 1.5 in the next snapshot of the same interval |
| `false_positive` | sig_score < 1.5 in next snapshot, or absent from next snapshot |
| `pending` | No subsequent snapshot of the same interval exists yet |

Threshold of 1.5 chosen as half of the 3.0 "≥95% confidence" boundary — repos that retain meaningful signal are true positives; repos that collapse completely are false positives.

---

## 6. Out of Scope

- Manual override of true/false positive labels
- Authentication / access control on the trigger endpoint
- Retention policy for old snapshots (no auto-deletion)
- Email/webhook notifications on snapshot completion
