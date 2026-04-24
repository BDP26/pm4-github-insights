# Spec: Overview & Activity Redesign

**Date:** 2026-04-24  
**Status:** Approved

---

## Summary

Remove two placeholder pages (Repositories, Contributors). Redesign the Overview page as a compact command-centre dashboard. Implement a new Activity page — a lifetime impact leaderboard with Repos / Users / Orgs scope, linking rows to existing Hidden Gems detail pages.

---

## 1. Pages Removed

Remove the following completely:
- `frontend/src/app/repositories/page.tsx` — delete file
- `frontend/src/app/contributors/page.tsx` — delete file
- Sidebar nav entries for "Repositories" and "Contributors" in `frontend/src/components/Sidebar.tsx`

After removal the sidebar contains: **Overview · Activity · Hidden Gems**

---

## 2. Overview Page Redesign

**Route:** `/`  
**File:** `frontend/src/app/page.tsx`  
**Render mode:** `force-dynamic` (server component, already set)

### 2a. KPI Cards (6 cards, 3×2 grid on mobile → 6-col on xl)

| Title | Source | Notes |
|---|---|---|
| Total Commits | existing `/api/kpis` | unchanged |
| Open PRs | existing `/api/kpis` | unchanged |
| Active Contributors | existing `/api/kpis` | unchanged |
| Avg Review Time | existing `/api/kpis` | unchanged |
| Repos Tracked | new field in `/api/kpis` | `SELECT COUNT(*) FROM repos` |
| Total Stars | new field in `/api/kpis` | `SELECT COALESCE(SUM(stargazers_count),0) FROM repos` |

The two new metrics are added to the existing `/api/kpis` endpoint response (no delta for either — `delta: null`).

### 2b. Activity Heatmap (full-width)

**New API endpoint:** `GET /api/overview/heatmap?weeks=52`

Returns a list of `{ date: "YYYY-MM-DD", event_type: string, count: number }` rows for the past N weeks, one row per (day, event_type) combination. Event types included: `PushEvent`, `PullRequestEvent`, `IssuesEvent`, `WatchEvent`, `ForkEvent`.

**Query:**
```sql
SELECT
    time_bucket('1 day', bucket)::date AS date,
    event_type,
    SUM(event_count)::int              AS count
FROM event_stats_5m
WHERE bucket >= NOW() - make_interval(weeks => $1::int)
  AND event_type = ANY($2::text[])
GROUP BY date, event_type
ORDER BY date, event_type
```

**Frontend rendering:** A CSS-grid heatmap (52 columns × 5 event-type rows). Each cell is colour-coded by count intensity using Tailwind background-opacity classes (0 → light indigo, high → deep indigo). A colour legend sits below. Built as a new `ActivityHeatmap` client component.

### 2c. Live Events Feed

Unchanged — `<LiveEventsTable initialEvents={events} />` remains below the heatmap.

---

## 3. Activity Page

**Route:** `/activity`  
**File:** `frontend/src/app/activity/page.tsx`  
**Render mode:** `"use client"` (interactive scope toggle + data fetching)

### 3a. Scope Toggle

Identical to Hidden Gems: a compact inline button group with three options — **Repos**, **Users**, **Orgs**. Switching scope resets to page 1 and refetches.

### 3b. Instant Page Transitions (UX requirement)

Scope switches and any future filter changes must feel instant — the UI must never block on a network request before updating. Use the same React 19 pattern already in Hidden Gems:

- `useTransition` — wraps the fetch call so the scope button activates immediately while the network request runs in the background
- `useDeferredValue` — the table renders the previous data at reduced opacity while new data loads, preventing a blank flash
- A small "Updating…" spinner appears above the table during the pending state (identical to the Hidden Gems loading indicator)

This applies to both the scope toggle and any future limit / sort changes added to this page.

### 3b. Leaderboard Table

One table, columns adapt per scope. All rows are clickable and navigate to the corresponding Hidden Gems detail page.

**Repos scope** → `/hidden-gems/repos/<full_name>`

| # | Repository | Owner | Language | ⭐ Stars | 🍴 Forks | Score |
|---|---|---|---|---|---|---|

**Users scope** → `/hidden-gems/users/<username>`

| # | Username | Location | Repos | ⭐ Stars | 🍴 Forks | Score |
|---|---|---|---|---|---|---|

**Orgs scope** → `/hidden-gems/orgs/<org_login>`

| # | Organisation | Repos | ⭐ Stars | 🍴 Forks | Score |
|---|---|---|---|---|---|

Score formula (all scopes): `stars × 1.0 + forks × 2.0` — displayed as a formatted integer.  
A formula badge appears in the page subtitle: `impact = stars × 1 + forks × 2`.

### 3c. New API Endpoint

**`GET /api/activity/leaderboard?scope=repos|users|orgs&limit=20`**

Added to `api/routers/activity.py` (new router, registered in `main.py`).

**Repos query:**
```sql
SELECT
    r.repo_id, r.full_name, r.owner_login, r.language,
    r.stargazers_count                                        AS total_stars,
    r.forks_count                                             AS total_forks,
    (r.stargazers_count * 1.0 + r.forks_count * 2.0)        AS impact_score
FROM repos r
WHERE r.stargazers_count > 0 OR r.forks_count > 0
ORDER BY impact_score DESC NULLS LAST
LIMIT $1
```

**Users query** (as provided by user, parameterised):
```sql
WITH config AS (
    SELECT 1.0::FLOAT AS star_weight, 2.0::FLOAT AS fork_weight
),
user_portfolio AS (
    SELECT
        owner_login,
        COUNT(repo_id)                      AS total_repos,
        COALESCE(SUM(stargazers_count), 0)  AS total_stars,
        COALESCE(SUM(forks_count), 0)       AS total_forks
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
    (p.total_stars * cfg.star_weight + p.total_forks * cfg.fork_weight) AS impact_score
FROM user_portfolio p
JOIN users u ON p.owner_login = u.username
CROSS JOIN config cfg
ORDER BY impact_score DESC NULLS LAST
LIMIT $1
```

**Orgs query:**
```sql
WITH config AS (
    SELECT 1.0::FLOAT AS star_weight, 2.0::FLOAT AS fork_weight
),
org_portfolio AS (
    SELECT
        owner_login,
        COUNT(repo_id)                      AS total_repos,
        COALESCE(SUM(stargazers_count), 0)  AS total_stars,
        COALESCE(SUM(forks_count), 0)       AS total_forks
    FROM repos
    WHERE owner_type = 'Organization'
    GROUP BY owner_login
)
SELECT
    owner_login AS org_login,
    p.total_repos,
    p.total_stars,
    p.total_forks,
    (p.total_stars * cfg.star_weight + p.total_forks * cfg.fork_weight) AS impact_score
FROM org_portfolio p
CROSS JOIN config cfg
ORDER BY impact_score DESC NULLS LAST
LIMIT $1
```

Response shape:
```json
{
  "scope": "users",
  "items": [
    { "username": "torvalds", "location": "Portland", "total_repos": 12,
      "total_stars": 182000, "total_forks": 23000, "impact_score": 228000 }
  ]
}
```

Response caching: TTL 60 seconds (same pattern as hidden-gems live cache).

---

## 4. New Frontend Files

| File | Purpose |
|---|---|
| `frontend/src/components/ActivityHeatmap.tsx` | Heatmap grid component (client) |
| `frontend/src/components/ActivityTable.tsx` | Leaderboard table, renders all 3 scope variants |

---

## 5. New API Files

| File | Purpose |
|---|---|
| `api/routers/activity.py` | New FastAPI router with `/api/activity/leaderboard` and `/api/overview/heatmap` |

---

## 6. What is NOT changing

- Hidden Gems pages, detail pages, filters, snapshots — untouched
- SSE stream endpoint — untouched
- Existing KPIs, commits-over-time, top-repos, recent-events endpoints — `kpis` gets 2 new fields; others untouched
- `mockData.ts` — no changes needed (fallback remains for dev)
