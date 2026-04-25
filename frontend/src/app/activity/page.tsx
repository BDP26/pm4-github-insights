"use client";

import { useState, useCallback, useEffect, useTransition, useDeferredValue } from "react";
import ActivityTable from "@/components/ActivityTable";
import type { ActivityScope, ActivityItem } from "@/types/activity";

const SCOPES: { value: ActivityScope; label: string }[] = [
  { value: "repos", label: "Repos" },
  { value: "users", label: "Users" },
  { value: "orgs",  label: "Orgs"  },
];

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export default function ActivityPage() {
  const [scope, setScope]   = useState<ActivityScope>("repos");
  const [items, setItems]   = useState<ActivityItem[]>([]);
  const [isPending, startTransition] = useTransition();
  const deferredItems = useDeferredValue(items);

  const load = useCallback((s: ActivityScope) => {
    startTransition(async () => {
      try {
        const res = await fetch(
          `${API}/api/activity/leaderboard?scope=${s}&limit=20`,
          { cache: "no-store" },
        );
        if (!res.ok) throw new Error(`${res.status}`);
        const body = await res.json();
        setItems(body.items);
      } catch {
        setItems([]);
      }
    });
  }, []);

  useEffect(() => { load(scope); }, [scope, load]);

  function handleScope(s: ActivityScope) {
    setScope(s);   // instant — UI switches immediately
    load(s);
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Activity</h1>
          <p className="mt-1 text-slate-600">
            Lifetime impact leaderboard — ranked by{" "}
            <code className="bg-slate-100 px-1.5 py-0.5 rounded text-xs">
              impact = stars × 1 + forks × 2
            </code>
          </p>
        </div>

        {/* Scope toggle — identical style to Hidden Gems */}
        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {SCOPES.map((s) => (
            <button
              key={s.value}
              type="button"
              onClick={() => handleScope(s.value)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
                scope === s.value
                  ? "bg-indigo-50 text-indigo-700 shadow-sm"
                  : "text-slate-600 hover:text-slate-900 hover:bg-slate-50"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </div>

      {/* Table with loading overlay */}
      <div className="relative">
        {isPending && (
          <div className="absolute inset-x-0 top-0 z-10 flex items-center justify-center gap-2 bg-indigo-50 border border-indigo-100 rounded-lg py-2 text-sm font-medium text-indigo-600 shadow-sm">
            <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            Updating leaderboard…
          </div>
        )}
        <div className={`transition-opacity duration-200 ${isPending ? "opacity-50 pointer-events-none mt-10" : ""}`}>
          <ActivityTable items={deferredItems} scope={scope} />
        </div>
      </div>
    </div>
  );
}
