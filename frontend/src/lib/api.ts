/**
 * Server-side API client — import only from Server Components or Route Handlers.
 * Uses the internal Docker hostname (API_URL) so it never leaves the container network.
 */
import type { CommitDataPoint, KpiMetric, RecentEvent, RepoActivity } from "@/types/dashboard";

const BASE_URL = process.env.API_URL ?? "http://localhost:8000";

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`API ${path} returned ${res.status}`);
  return res.json() as Promise<T>;
}

// ── Raw shapes returned by the API ───────────────────────────────────────────
interface KpiRaw {
  value: number | null;
  delta: number | null;
}

interface KpisResponse {
  totalCommits:        KpiRaw;
  openPRs:             KpiRaw;
  activeContributors:  KpiRaw;
  avgReviewHours:      KpiRaw;
  reposTracked:        KpiRaw;
  totalStars:          KpiRaw;
}

export interface GeoHeatmapPoint {
  lat: number;
  lng: number;
  country: string | null;
  count: number;
}

// ── Public fetch functions ────────────────────────────────────────────────────
export async function fetchKpis(): Promise<KpiMetric[]> {
  const d = await apiFetch<KpisResponse>("/api/kpis");

  function deltaType(v: number | null): KpiMetric["deltaType"] {
    if (v === null) return "unchanged";
    return v > 0 ? "increase" : v < 0 ? "decrease" : "unchanged";
  }

  function fmtDelta(v: number | null): string {
    if (v === null) return "—";
    return `${v > 0 ? "+" : ""}${v}%`;
  }

  return [
    {
      title:     "Total Commits",
      value:     (d.totalCommits.value ?? 0).toLocaleString(),
      delta:     fmtDelta(d.totalCommits.delta),
      deltaType: deltaType(d.totalCommits.delta),
    },
    {
      title:     "Open PRs",
      value:     (d.openPRs.value ?? 0).toLocaleString(),
      delta:     fmtDelta(d.openPRs.delta),
      deltaType: deltaType(d.openPRs.delta),
    },
    {
      title:     "Known Contributors",
      value:     (d.activeContributors.value ?? 0).toLocaleString(),
      delta:     fmtDelta(d.activeContributors.delta),
      deltaType: deltaType(d.activeContributors.delta),
    },
    {
      title:     "Avg. Review Time",
      value:     d.avgReviewHours.value !== null ? `${d.avgReviewHours.value} h` : "N/A",
      delta:     fmtDelta(d.avgReviewHours.delta),
      deltaType: deltaType(d.avgReviewHours.delta),
    },
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
  ];
}

export async function fetchCommitsOverTime(days = 30): Promise<CommitDataPoint[]> {
  return apiFetch<CommitDataPoint[]>(`/api/commits-over-time?days=${days}`);
}

export async function fetchTopRepos(limit = 10): Promise<RepoActivity[]> {
  return apiFetch<RepoActivity[]>(`/api/top-repos?limit=${limit}`);
}

export async function fetchRecentEvents(limit = 20): Promise<RecentEvent[]> {
  return apiFetch<RecentEvent[]>(`/api/recent-events?limit=${limit}`);
}

export async function fetchGeoHeatmap(weeks = 52, limit = 250): Promise<GeoHeatmapPoint[]> {
  const publicBaseUrl = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";
  const res = await fetch(
    `${publicBaseUrl}/api/overview/globe-heatmap?weeks=${weeks}&limit=${limit}`,
    { cache: "no-store" },
  );
  if (!res.ok) throw new Error(`API /api/overview/globe-heatmap returned ${res.status}`);
  return res.json() as Promise<GeoHeatmapPoint[]>;
}
