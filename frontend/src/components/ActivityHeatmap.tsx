"use client";

import { useEffect, useState } from "react";

interface HeatmapPoint {
  date: string;        // "YYYY-MM-DD" (week start)
  event_type: string;
  count: number;
}

const EVENT_TYPES = [
  "PushEvent",
  "PullRequestEvent",
  "IssuesEvent",
  "WatchEvent",
  "ForkEvent",
];

const EVENT_LABELS: Record<string, string> = {
  PushEvent:        "Push",
  PullRequestEvent: "PR",
  IssuesEvent:      "Issue",
  WatchEvent:       "Star",
  ForkEvent:        "Fork",
};

const INTENSITY: Record<string, string> = {
  none:   "bg-slate-100",
  low:    "bg-indigo-100",
  medium: "bg-indigo-300",
  high:   "bg-indigo-500",
  max:    "bg-indigo-700",
};

function intensityKey(count: number): string {
  if (count === 0)  return "none";
  if (count < 10)   return "low";
  if (count < 50)   return "medium";
  if (count < 200)  return "high";
  return "max";
}

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export default function ActivityHeatmap() {
  const [data, setData] = useState<HeatmapPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  useEffect(() => {
    fetch(`${API}/api/overview/heatmap?weeks=52`, { cache: "no-store" })
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json();
      })
      .then((d: HeatmapPoint[]) => { setData(d); setLoading(false); })
      .catch(() => { setError(true); setLoading(false); });
  }, []);

  // Build a lookup: { "YYYY-MM-DD": { PushEvent: N, ... } }
  const lookup: Record<string, Record<string, number>> = {};
  for (const pt of data) {
    if (!lookup[pt.date]) lookup[pt.date] = {};
    lookup[pt.date][pt.event_type] = pt.count;
  }

  // Collect sorted unique week dates
  const weeks = Array.from(new Set(data.map((p) => p.date))).sort();

  if (loading) {
    return (
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
        <div className="h-4 w-40 bg-slate-200 rounded animate-pulse mb-4" />
        <div className="h-32 bg-slate-100 rounded animate-pulse" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6 text-red-500 text-sm">
        Failed to load heatmap data.
      </div>
    );
  }

  if (weeks.length === 0) {
    return (
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6 text-slate-400 text-sm">
        No heatmap data available yet.
      </div>
    );
  }

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-6">
      <h2 className="text-base font-semibold text-slate-800 mb-4">
        Activity Heatmap — last 52 weeks
      </h2>
      <div className="overflow-x-auto">
        <div className="min-w-max">
          {EVENT_TYPES.map((et) => (
            <div key={et} className="flex items-center gap-1 mb-1">
              <span className="w-10 text-xs text-slate-500 text-right shrink-0">
                {EVENT_LABELS[et]}
              </span>
              <div className="flex gap-0.5 ml-1">
                {weeks.map((week) => {
                  const count = lookup[week]?.[et] ?? 0;
                  return (
                    <div
                      key={week}
                      title={`${week} · ${EVENT_LABELS[et]}: ${count}`}
                      className={`w-3 h-3 rounded-sm ${INTENSITY[intensityKey(count)]}`}
                    />
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </div>
      {/* Legend */}
      <div className="flex items-center gap-2 mt-3 text-xs text-slate-500">
        <span>Less</span>
        {(["none", "low", "medium", "high", "max"] as const).map((key) => (
          <div key={key} className={`w-3 h-3 rounded-sm ${INTENSITY[key]}`} />
        ))}
        <span>More</span>
      </div>
    </div>
  );
}
