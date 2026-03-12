export interface KpiMetric {
  title: string;
  value: string;
  delta: string;
  deltaType: "increase" | "decrease" | "unchanged";
}

export interface CommitDataPoint {
  date: string;
  commits: number;
}

export interface RepoActivity {
  repo: string;
  events: number;
}

export interface RecentEvent {
  id: string;
  repo: string;
  eventType: string;
  actor: string;
  timestamp: string;
}

export interface SSEState<T> {
  data: T | null;
  error: Event | null;
  isConnected: boolean;
}
