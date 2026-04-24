export interface HiddenGemRepo {
  repo_id: number;
  full_name: string;
  name: string;
  owner_login: string;
  language: string | null;
  license_spdx: string | null;
  topics: string[];
  sig_score: number;
  count_stars_interval: number;
  count_forks_interval: number;
  total_stars: number;
  total_forks: number;
}

export interface HiddenGemUser {
  username: string;
  total_score: number;
  best_repo_score: number;
  best_repo: string | null;
  hidden_gem_count: number;
  active_repos_in_window: number;
}

export interface HiddenGemOrg {
  org_login: string;
  org_repos_total_score: number;
  org_repos_best_score: number;
  org_active_repos: number;
  org_hidden_gem_count: number;
  member_repos_total_score: number;
  member_repos_best_score: number;
  member_active_repos: number;
  member_active_users: number;
  member_hidden_gem_count: number;
}

export interface LiveResponse<T> {
  scope: string;
  page: number;
  limit: number;
  items: T[];
}

export interface SearchResult {
  type: "repo" | "user" | "org";
  name: string;
  score: number | null;
}

export interface SearchResponse {
  items: SearchResult[];
  page: number;
  limit: number;
}

export interface ScoreHistoryPoint {
  run_at: string;
  interval_hours: number;
  sig_score: number | null;
  rank: number | null;
  count_stars_interval: number;
  count_forks_interval: number;
  total_stars: number;
  total_forks: number;
}

export interface RepoDetailResponse {
  full_name: string;
  current: HiddenGemRepo | null;
  history: ScoreHistoryPoint[];
}

export interface UserHistoryPoint {
  run_at: string;
  total_score: number;
  hidden_gem_count: number;
  best_repo_score: number | null;
  best_repo: string | null;
}

export interface UserDetailResponse {
  username: string;
  current: HiddenGemUser | null;
  repos: Partial<HiddenGemRepo>[];
  history: UserHistoryPoint[];
}

export interface OrgHistoryPoint {
  run_at: string;
  org_repos_total_score: number;
  org_hidden_gem_count: number;
  member_repos_total_score: number;
  member_hidden_gem_count: number;
}

export interface OrgDetailResponse {
  org_login: string;
  current: HiddenGemOrg | null;
  history: OrgHistoryPoint[];
}

export interface SnapshotRun {
  id: number;
  run_at: string;
  interval_hours: number;
  alpha: number;
  beta: number;
  repo_count: number | null;
  user_count: number | null;
  org_count: number | null;
}

export interface CohortEntry {
  repo_id: number;
  full_name: string;
  prev_score: number;
  current_score: number | null;
  classification: "true_positive" | "false_positive" | "pending";
}

export interface CohortSummary {
  total: number;
  sustained: number;
  dropped: number;
  pending: number;
}

export interface CohortResponse {
  snapshot_id: number;
  summary: CohortSummary;
  repos: CohortEntry[];
}
