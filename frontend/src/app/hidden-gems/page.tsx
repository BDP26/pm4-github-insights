"use client";

import { useState, useEffect, useCallback, useRef, useTransition, useDeferredValue } from "react";
import { Clock } from "lucide-react";
import HiddenGemFilters, { type Timeframe, type Scope } from "@/components/HiddenGemFilters";
import HiddenGemTable from "@/components/HiddenGemTable";
import CohortTable from "@/components/CohortTable";
import SearchBar from "@/components/SearchBar";
import SearchResults from "@/components/SearchResults";
import type {
  HiddenGemRepo, HiddenGemUser, HiddenGemOrg,
  SnapshotRun, CohortResponse, SearchResult,
} from "@/types/hidden_gems";

type ActiveView = "dashboard" | "reports";

const API = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json() as Promise<T>;
}

function qs(p: Record<string, string | number | string[] | undefined>): string {
  const parts: string[] = [];
  for (const [k, v] of Object.entries(p)) {
    if (v === undefined) continue;
    if (Array.isArray(v)) v.forEach((i) => parts.push(`${k}=${encodeURIComponent(i)}`));
    else parts.push(`${k}=${encodeURIComponent(v)}`);
  }
  return parts.length ? `?${parts.join("&")}` : "";
}

export default function HiddenGemsPage() {
  const [activeView, setActiveView] = useState<ActiveView>("dashboard");

  // Dashboard state
  const [timeframe, setTimeframe] = useState<Timeframe>("168");
  const [scope, setScope]         = useState<Scope>("repos");
  const [language, setLanguage]   = useState("");
  const [license, setLicense]     = useState("");
  const [topic, setTopic]         = useState("");
  const [page, setPage]           = useState(1);
  const [items, setItems]         = useState<(HiddenGemRepo | HiddenGemUser | HiddenGemOrg)[]>([]);
  const [isPending, startTransition] = useTransition();
  const deferredItems = useDeferredValue(items);

  // Filter options (loaded once)
  const [languages, setLanguages] = useState<string[]>([]);
  const [licenses, setLicenses]   = useState<string[]>([]);
  const [topics, setTopics]       = useState<string[]>([]);

  // Search state
  const [searchQuery, setSearchQuery]   = useState("");
  const [searchScope, setSearchScope]   = useState("all");
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [showResults, setShowResults]   = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);

  // Reports state
  const [snapshots, setSnapshots]       = useState<SnapshotRun[]>([]);
  const [selectedSnapshotId, setSelectedSnapshotId] = useState<number | null>(null);
  const [cohort, setCohort]             = useState<CohortResponse | null>(null);

  // Reload filter options when timeframe changes
  useEffect(() => {
    Promise.all([
      get<string[]>(`/api/hidden-gems/filters/languages${qs({ hours: timeframe })}`),
      get<string[]>(`/api/hidden-gems/filters/licenses${qs({ hours: timeframe })}`),
      get<string[]>(`/api/hidden-gems/filters/topics${qs({ hours: timeframe })}`),
    ]).then(([langs, lics, tops]) => {
      setLanguages(langs);
      setLicenses(lics);
      setTopics(tops);
    }).catch(() => {/* filters are optional */});
  }, [timeframe]);

  // Load live ranking
  const loadLive = useCallback(() => {
    const params = {
      hours: timeframe,
      scope,
      page,
      limit: 25,
      ...(language ? { language: [language] } : {}),
      ...(license  ? { license: [license] }   : {}),
      ...(topic    ? { topic: [topic] }        : {}),
    };
    startTransition(async () => {
      try {
        const d = await get<{ items: (HiddenGemRepo | HiddenGemUser | HiddenGemOrg)[] }>(
          `/api/hidden-gems/live${qs(params as Record<string, string | number | string[] | undefined>)}`
        );
        setItems(d.items);
      } catch {
        setItems([]);
      }
    });
  }, [timeframe, scope, language, license, topic, page]);

  useEffect(() => {
    if (activeView === "dashboard") loadLive();
  }, [activeView, loadLive]);

  // Load snapshots for reports view
  useEffect(() => {
    if (activeView !== "reports") return;
    get<SnapshotRun[]>(`/api/hidden-gems/snapshots${qs({ interval_hours: timeframe, limit: 20 })}`)
      .then((runs) => {
        setSnapshots(runs);
        if (runs.length > 0 && selectedSnapshotId === null) {
          setSelectedSnapshotId(runs[0].id);
        }
      })
      .catch(() => setSnapshots([]));
  }, [activeView, timeframe, selectedSnapshotId]);

  // Load cohort when snapshot selected
  useEffect(() => {
    if (selectedSnapshotId === null) return;
    get<CohortResponse>(`/api/hidden-gems/snapshots/${selectedSnapshotId}/cohort`)
      .then(setCohort)
      .catch(() => setCohort(null));
  }, [selectedSnapshotId]);

  // Search with debounce
  useEffect(() => {
    if (searchQuery.length < 2) {
      setSearchResults([]);
      setShowResults(false);
      return;
    }
    const timer = setTimeout(() => {
      get<{ items: SearchResult[] }>(
        `/api/hidden-gems/search${qs({ q: searchQuery, scope: searchScope, limit: 10 })}`
      )
        .then((d) => {
          setSearchResults(d.items);
          setShowResults(true);
        })
        .catch(() => setSearchResults([]));
    }, 300);
    return () => clearTimeout(timer);
  }, [searchQuery, searchScope]);

  // Close search on outside click
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowResults(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  const selectedSnapshot = snapshots.find((s) => s.id === selectedSnapshotId);

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight">Hidden Gems</h1>
          <p className="mt-1 text-slate-600">
            Repositories showing statistically significant growth, ranked by{" "}
            <code className="bg-slate-100 px-1.5 py-0.5 rounded text-xs">
              sig_score = -ln(1 − Poisson-CDF)
            </code>
          </p>
        </div>

        {/* View toggle */}
        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {(["dashboard", "reports"] as const).map((v) => (
            <button
              key={v}
              type="button"
              onClick={() => setActiveView(v)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
                activeView === v
                  ? "bg-indigo-50 text-indigo-700 shadow-sm"
                  : "text-slate-600 hover:text-slate-900 hover:bg-slate-50"
              }`}
            >
              {v === "dashboard" ? "Ranking" : "Evaluation Reports"}
            </button>
          ))}
        </div>
      </div>

      {/* Search bar */}
      <div ref={searchRef} className="relative">
        <SearchBar
          value={searchQuery}
          scope={searchScope}
          onChange={setSearchQuery}
          onScopeChange={setSearchScope}
        />
        {showResults && (
          <SearchResults
            results={searchResults}
            onClose={() => { setShowResults(false); setSearchQuery(""); }}
          />
        )}
      </div>

      {activeView === "dashboard" ? (
        <>
          <HiddenGemFilters
            timeframe={timeframe}
            scope={scope}
            language={language}
            license={license}
            topic={topic}
            languages={languages}
            licenses={licenses}
            topics={topics}
            onTimeframeChange={(t) => { setTimeframe(t); setPage(1); }}
            onScopeChange={(s)     => { setScope(s);     setPage(1); }}
            onLanguageChange={(l)  => { setLanguage(l);  setPage(1); }}
            onLicenseChange={(l)   => { setLicense(l);   setPage(1); }}
            onTopicChange={(t)     => { setTopic(t);     setPage(1); }}
          />
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
              <HiddenGemTable
                items={deferredItems}
                scope={scope}
                page={page}
                onPageChange={setPage}
              />
            </div>
          </div>
        </>
      ) : (
        <div className="space-y-6">
          {/* Snapshot selector */}
          <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm px-3 py-2 w-fit">
            <Clock className="w-4 h-4 text-slate-400 mr-2" />
            <select
              value={selectedSnapshotId ?? ""}
              onChange={(e) => setSelectedSnapshotId(Number(e.target.value))}
              className="bg-transparent text-sm font-medium text-slate-700 outline-none border-none p-0"
            >
              {snapshots.length === 0 && (
                <option value="">No snapshots yet</option>
              )}
              {snapshots.map((s) => (
                <option key={s.id} value={s.id}>
                  {new Date(s.run_at).toLocaleDateString("en-CH", {
                    day: "numeric", month: "short", year: "numeric",
                  })}
                  {" "}— {s.repo_count ?? 0} repos
                </option>
              ))}
            </select>
          </div>

          {cohort ? (
            <CohortTable
              summary={cohort.summary}
              repos={cohort.repos}
              reportLabel={
                selectedSnapshot
                  ? new Date(selectedSnapshot.run_at).toLocaleDateString("en-CH", {
                      day: "numeric", month: "long", year: "numeric",
                    })
                  : ""
              }
            />
          ) : (
            <div className="flex items-center justify-center py-16 text-slate-400">
              {snapshots.length === 0
                ? "No snapshots yet — the scheduler will capture the first one on startup."
                : "Loading cohort…"}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
