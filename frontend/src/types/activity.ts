export type ActivityScope = "repos" | "users" | "orgs";

export interface ActivityRepoItem {
  repo_id: number;
  full_name: string;
  owner_login: string;
  language: string | null;
  total_stars: number;
  total_forks: number;
  impact_score: number;
}

export interface ActivityUserItem {
  username: string;
  location: string | null;
  total_repos: number;
  total_stars: number;
  total_forks: number;
  impact_score: number;
}

export interface ActivityOrgItem {
  org_login: string;
  total_repos: number;
  total_stars: number;
  total_forks: number;
  impact_score: number;
}

export type ActivityItem = ActivityRepoItem | ActivityUserItem | ActivityOrgItem;

export interface ActivityLeaderboardResponse {
  scope: ActivityScope;
  items: ActivityItem[];
}
