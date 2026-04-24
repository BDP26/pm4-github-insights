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
