import type { CommitDataPoint, KpiMetric, RecentEvent, RepoActivity } from "@/types/dashboard";

// ── KPI cards ──────────────────────────────────────────────────────────────
export const kpiMetrics: KpiMetric[] = [
  {
    title: "Total Commits",
    value: "14,832",
    delta: "+12.4%",
    deltaType: "increase",
  },
  {
    title: "Open PRs",
    value: "247",
    delta: "-3.1%",
    deltaType: "decrease",
  },
  {
    title: "Active Contributors",
    value: "1,089",
    delta: "+8.7%",
    deltaType: "increase",
  },
  {
    title: "Avg. Review Time",
    value: "4.2 h",
    delta: "-0.5 h",
    deltaType: "increase", // shorter is better, but deltaType reflects direction
  },
  {
    title: "Repos Tracked",
    value: "0",
    delta: "—",
    deltaType: "unchanged",
  },
  {
    title: "Total Stars",
    value: "0",
    delta: "—",
    deltaType: "unchanged",
  },
];

// ── Commits over time (last 30 days) ───────────────────────────────────────
function daysAgo(n: number): string {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}

export const commitsOverTime: CommitDataPoint[] = Array.from(
  { length: 30 },
  (_, i) => ({
    date: daysAgo(29 - i),
    commits: Math.floor(300 + Math.random() * 700 + Math.sin(i / 3) * 150),
  })
);

// ── Top repos by activity ───────────────────────────────────────────────────
export const topRepos: RepoActivity[] = [
  { repo: "torvalds/linux", events: 4821 },
  { repo: "microsoft/vscode", events: 3502 },
  { repo: "vercel/next.js", events: 2944 },
  { repo: "facebook/react", events: 2711 },
  { repo: "golang/go", events: 2198 },
  { repo: "rust-lang/rust", events: 1987 },
  { repo: "denoland/deno", events: 1543 },
];

// ── Recent events ───────────────────────────────────────────────────────────
export const recentEvents: RecentEvent[] = [
  {
    id: "1",
    repo: "vercel/next.js",
    eventType: "PushEvent",
    actor: "timneutkens",
    timestamp: new Date(Date.now() - 1 * 60 * 1000).toISOString(),
  },
  {
    id: "2",
    repo: "microsoft/vscode",
    eventType: "PullRequestEvent",
    actor: "bpasero",
    timestamp: new Date(Date.now() - 4 * 60 * 1000).toISOString(),
  },
  {
    id: "3",
    repo: "facebook/react",
    eventType: "IssuesEvent",
    actor: "gaearon",
    timestamp: new Date(Date.now() - 9 * 60 * 1000).toISOString(),
  },
  {
    id: "4",
    repo: "torvalds/linux",
    eventType: "PushEvent",
    actor: "torvalds",
    timestamp: new Date(Date.now() - 13 * 60 * 1000).toISOString(),
  },
  {
    id: "5",
    repo: "golang/go",
    eventType: "PullRequestReviewEvent",
    actor: "rsc",
    timestamp: new Date(Date.now() - 21 * 60 * 1000).toISOString(),
  },
  {
    id: "6",
    repo: "rust-lang/rust",
    eventType: "PushEvent",
    actor: "nikomatsakis",
    timestamp: new Date(Date.now() - 35 * 60 * 1000).toISOString(),
  },
  {
    id: "7",
    repo: "denoland/deno",
    eventType: "IssueCommentEvent",
    actor: "ry",
    timestamp: new Date(Date.now() - 48 * 60 * 1000).toISOString(),
  },
];
