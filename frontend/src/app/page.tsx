import { Title, Text } from "@tremor/react";
import KpiCard from "@/components/KpiCard";
import CommitsChart from "@/components/CommitsChart";
import RepoActivityChart from "@/components/RepoActivityChart";
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

// Always render fresh — data is real-time
export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  // Fetch all data in parallel; fall back to mock data if the API is not yet available.
  const [kpis, commits, repos, events] = await Promise.all([
    fetchKpis().catch(() => mockKpis),
    fetchCommitsOverTime().catch(() => mockCommits),
    fetchTopRepos().catch(() => mockRepos),
    fetchRecentEvents().catch(() => mockEvents),
  ]);

  return (
    <div className="space-y-8">
      <div>
        <Title>Overview</Title>
        <Text className="mt-1">
          Real-time summary of GitHub activity across tracked repositories.
        </Text>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4">
        {kpis.map((metric) => (
          <KpiCard key={metric.title} metric={metric} />
        ))}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <CommitsChart data={commits} />
        <RepoActivityChart data={repos} />
      </div>

      {/* Live events table — appends new events via SSE */}
      <LiveEventsTable initialEvents={events} />
    </div>
  );
}
