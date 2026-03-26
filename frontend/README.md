# GitHub Insights — Frontend

Real-time analytics dashboard for GitHub event data.
Built with Next.js 16 (App Router), TypeScript, Tailwind CSS 4, and Tremor.

---

## Prerequisites

- Node.js >= 18
- npm >= 9

---

## Running the app

```bash
cd frontend
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

---

## Project structure

```
src/
├── app/
│   ├── layout.tsx            # Root layout — sidebar + main content shell
│   ├── globals.css           # Tailwind v4 import + Tremor source path
│   ├── page.tsx              # / → Overview dashboard (KPIs, charts, table)
│   ├── repositories/page.tsx # /repositories → placeholder
│   ├── contributors/page.tsx # /contributors  → placeholder
│   └── activity/page.tsx     # /activity      → placeholder
├── components/
│   ├── Sidebar.tsx           # Persistent left navigation
│   ├── KpiCard.tsx           # Single KPI metric card (Tremor Card)
│   ├── CommitsChart.tsx      # Commits over time — Tremor AreaChart
│   ├── RepoActivityChart.tsx # Top repos by events — Tremor BarChart
│   ├── RecentEventsTable.tsx # Recent events — Tremor Table
│   └── LiveEventsTable.tsx   # Real-time SSE-driven event table
├── hooks/
│   └── useSSE.ts             # Reusable SSE hook (see below)
├── lib/
│   ├── api.ts                # Server-side fetch wrappers (uses API_URL env var)
│   └── mockData.ts           # Static mock data (fallback / test fixtures)
└── types/
    └── dashboard.ts          # Shared TypeScript interfaces
```

---

## useSSE hook

`src/hooks/useSSE.ts` wraps the browser `EventSource` API into a typed React hook.

### Signature

```ts
function useSSE<T>(url: string): SSEState<T>

interface SSEState<T> {
  data: T | null;       // Latest parsed JSON payload
  error: Event | null;  // Last connection error (if any)
  isConnected: boolean; // Whether the EventSource is open
}
```

### Usage

```tsx
"use client";

import { useSSE } from "@/hooks/useSSE";

interface LiveEvent {
  repo: string;
  eventType: string;
  actor: string;
}

export default function LiveFeed() {
  const { data, isConnected, error } = useSSE<LiveEvent>(
    "http://localhost:8000/stream/events"   // FastAPI SSE endpoint
  );

  if (!isConnected) return <p>Connecting…</p>;
  if (error)        return <p>Connection error.</p>;
  if (!data)        return <p>Waiting for events…</p>;

  return (
    <p>
      {data.actor} pushed to {data.repo}
    </p>
  );
}
```

### Reconnection behaviour

The hook automatically reconnects with a 3-second delay after a connection drop.
The connection is cleanly closed when the component unmounts.

---

## Wiring in real data

`src/lib/api.ts` already contains server-side fetch wrappers for all REST endpoints:

```ts
fetchKpis()              // GET /api/kpis
fetchCommitsOverTime()   // GET /api/commits-over-time
fetchTopRepos()          // GET /api/top-repos
fetchRecentEvents()      // GET /api/recent-events
```

These use the `API_URL` environment variable (server-side only, not baked into the bundle).

For real-time data, use the `useSSE` hook (client components only):

```tsx
const { data } = useSSE<YourType>(
  `${process.env.NEXT_PUBLIC_API_URL}/stream/events`
);
```

`src/lib/mockData.ts` remains as a test fixture / fallback.

---

## Tech choices

| Concern        | Library                        |
| -------------- | ------------------------------ |
| Framework      | Next.js 16 (App Router)        |
| Language       | TypeScript (strict)            |
| Styling        | Tailwind CSS 4                 |
| UI / Charts    | Tremor v3 (Recharts under hood)|
| Real-time      | Browser `EventSource` (SSE)    |
