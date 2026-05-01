import nextDynamic from "next/dynamic";
import { Suspense } from "react";
import { Title, Text } from "@tremor/react";
import KpiCard from "@/components/KpiCard";
import CommitsChart from "@/components/CommitsChart";
import RepoActivityChart from "@/components/RepoActivityChart";
import ActivityHeatmap from "@/components/ActivityHeatmap";
import LiveEventsTable from "@/components/LiveEventsTable";
import {
  fetchKpis,
  fetchCommitsOverTime,
  fetchTopRepos,
  fetchRecentEvents,
} from "@/lib/api";
import {
  kpiMetrics as mockKpis,
  commitsOverTime as mockCommits,
  topRepos as mockRepos,
  recentEvents as mockEvents,
} from "@/lib/mockData";
import type { KpiMetric, CommitDataPoint, RepoActivity, RecentEvent } from "@/types/dashboard";

const ActivityGlobe = nextDynamic(() => import("@/components/ActivityGlobe"), {
  ssr: false,
  loading: () => (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6 animate-pulse">
      <div className="h-4 w-44 bg-slate-200 rounded mb-4" />
      <div className="h-[420px] rounded-2xl bg-slate-100" />
    </div>
  ),
});

// Always render fresh — data is real-time
export const dynamic = "force-dynamic";

// ── Loading skeletons ─────────────────────────────────────────────────────────

function KpiSkeleton() {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
      {Array.from({ length: 6 }).map((_, i) => (
        <div key={i} className="bg-white border border-slate-200 rounded-xl p-5 animate-pulse">
          <div className="h-3 w-24 bg-slate-200 rounded mb-3" />
          <div className="h-7 w-16 bg-slate-200 rounded mb-2" />
          <div className="h-3 w-12 bg-slate-100 rounded" />
        </div>
      ))}
    </div>
  );
}

function ChartSkeleton() {
  return (
    <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
      {[0, 1].map((i) => (
        <div key={i} className="bg-white border border-slate-200 rounded-xl p-5 animate-pulse">
          <div className="h-4 w-40 bg-slate-200 rounded mb-4" />
          <div className="h-48 bg-slate-100 rounded" />
        </div>
      ))}
    </div>
  );
}

function TableSkeleton() {
  return (
    <div className="bg-white border border-slate-200 rounded-xl p-5 animate-pulse">
      <div className="h-4 w-32 bg-slate-200 rounded mb-4" />
      {Array.from({ length: 5 }).map((_, i) => (
        <div key={i} className="flex gap-4 py-3 border-b border-slate-100 last:border-0">
          <div className="h-3 w-20 bg-slate-100 rounded" />
          <div className="h-3 w-32 bg-slate-100 rounded" />
          <div className="h-3 w-16 bg-slate-100 rounded" />
        </div>
      ))}
    </div>
  );
}

// ── Async data sections (each streams independently) ─────────────────────────

async function KpiSection() {
  const kpis: KpiMetric[] = await fetchKpis().catch(() => mockKpis);
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
      {kpis.map((metric) => (
        <KpiCard key={metric.title} metric={metric} />
      ))}
    </div>
  );
}

async function ChartSection() {
  const [commits, repos]: [CommitDataPoint[], RepoActivity[]] = await Promise.all([
    fetchCommitsOverTime().catch(() => mockCommits),
    fetchTopRepos().catch(() => mockRepos),
  ]);
  return (
    <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
      <CommitsChart data={commits} />
      <RepoActivityChart data={repos} />
    </div>
  );
}

async function EventsSection() {
  const events: RecentEvent[] = await fetchRecentEvents().catch(() => mockEvents);
  return <LiveEventsTable initialEvents={events} />;
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default function OverviewPage() {
  return (
    <div className="space-y-8">
      <div>
        <Title>Overview</Title>
        <Text className="mt-1">
          Real-time summary of GitHub activity across tracked repositories.
        </Text>
      </div>

      {/* KPI Cards — stream in independently */}
      <Suspense fallback={<KpiSkeleton />}>
        <KpiSection />
      </Suspense>

      {/* Charts — stream in together (same data fetch group) */}
      <Suspense fallback={<ChartSkeleton />}>
        <ChartSection />
      </Suspense>

      {/* Activity heatmap — client island, fetches on mount (no Suspense needed) */}
      <ActivityHeatmap />

      {/* Globe heatmap — client island, rendered as a continuous intensity layer */}
      <ActivityGlobe />

      {/* Live events table — streams in, then SSE takes over */}
      <Suspense fallback={<TableSkeleton />}>
        <EventsSection />
      </Suspense>
    </div>
  );
}
