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
│   └── RecentEventsTable.tsx # Recent events — Tremor Table
├── hooks/
│   └── useSSE.ts             # Reusable SSE hook (see below)
├── lib/
│   └── mockData.ts           # Static mock data (replace with live API calls)
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

1. Start the FastAPI backend (see root `docker-compose.yml`).
2. The SSE endpoint will be available at `http://localhost:8000/stream/events`.
3. Replace the imports from `@/lib/mockData` in `src/app/page.tsx` with
   `useSSE` calls or `fetch` calls against the FastAPI REST endpoints.
4. Remove or keep `src/lib/mockData.ts` as a fallback / test fixture.

---

## Tech choices

| Concern        | Library                        |
| -------------- | ------------------------------ |
| Framework      | Next.js 16 (App Router)        |
| Language       | TypeScript (strict)            |
| Styling        | Tailwind CSS 4                 |
| UI / Charts    | Tremor v3 (Recharts under hood)|
| Real-time      | Browser `EventSource` (SSE)    |
