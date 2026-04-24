"use client";

import { Filter, Code, Shield, BookOpen, Info } from "lucide-react";

export type Timeframe = "24" | "168" | "730";
export type Scope = "repos" | "users" | "orgs";

interface HiddenGemFiltersProps {
  timeframe: Timeframe;
  scope: Scope;
  language: string;
  license: string;
  topic: string;
  languages: string[];
  licenses: string[];
  topics: string[];
  onTimeframeChange: (t: Timeframe) => void;
  onScopeChange: (s: Scope) => void;
  onLanguageChange: (l: string) => void;
  onLicenseChange: (l: string) => void;
  onTopicChange: (t: string) => void;
}

const TIMEFRAMES: { value: Timeframe; label: string }[] = [
  { value: "24",  label: "Daily (24h)"    },
  { value: "168", label: "Weekly (168h)"  },
  { value: "730", label: "Monthly (730h)" },
];

const SCOPES: { value: Scope; label: string }[] = [
  { value: "repos", label: "Repos" },
  { value: "users", label: "Users" },
  { value: "orgs",  label: "Orgs"  },
];

export default function HiddenGemFilters({
  timeframe,
  scope,
  language,
  license,
  topic,
  languages,
  licenses,
  topics,
  onTimeframeChange,
  onScopeChange,
  onLanguageChange,
  onLicenseChange,
  onTopicChange,
}: HiddenGemFiltersProps) {
  return (
    <div className="space-y-4">
      {/* Timeframe + Scope selector */}
      <div className="flex flex-wrap items-center gap-4 justify-between">
        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {TIMEFRAMES.map((t) => (
            <button
              key={t.value}
              onClick={() => onTimeframeChange(t.value)}
              className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${
                timeframe === t.value
                  ? "bg-indigo-50 text-indigo-700 shadow-sm"
                  : "text-slate-600 hover:text-slate-900 hover:bg-slate-50"
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        <div className="flex items-center bg-white border border-slate-200 rounded-lg shadow-sm p-1">
          {SCOPES.map((s) => (
            <button
              key={s.value}
              onClick={() => onScopeChange(s.value)}
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

      {/* Attribute filters (only shown for repos) */}
      {scope === "repos" && (
        <div className="bg-white p-4 rounded-xl shadow-sm border border-slate-200 flex flex-wrap gap-4 items-center">
          <div className="flex items-center text-slate-500 mr-2">
            <Filter className="w-5 h-5 mr-2" />
            <span className="font-medium text-sm">Filters:</span>
          </div>

          <div className="flex items-center gap-2">
            <Code className="w-4 h-4 text-slate-400" />
            <select
              value={language}
              onChange={(e) => onLanguageChange(e.target.value)}
              className="pl-2 pr-8 py-1.5 text-sm border border-slate-200 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
            >
              <option value="">All Languages</option>
              {languages.map((l) => (
                <option key={l} value={l}>{l}</option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-2 border-l border-slate-200 pl-4">
            <Shield className="w-4 h-4 text-slate-400" />
            <select
              value={license}
              onChange={(e) => onLicenseChange(e.target.value)}
              className="pl-2 pr-8 py-1.5 text-sm border border-slate-200 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
            >
              <option value="">All Licenses</option>
              {licenses.map((l) => (
                <option key={l} value={l}>{l}</option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-2 border-l border-slate-200 pl-4">
            <BookOpen className="w-4 h-4 text-slate-400" />
            <select
              value={topic}
              onChange={(e) => onTopicChange(e.target.value)}
              className="pl-2 pr-8 py-1.5 text-sm border border-slate-200 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
            >
              <option value="">All Topics</option>
              {topics.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>

          <div className="ml-auto text-xs text-slate-500 flex items-center gap-1 bg-slate-100 px-3 py-1.5 rounded-full">
            <Info className="w-4 h-4" />
            Weights: α=1.0 (Stars), β=1.0 (Forks)
          </div>
        </div>
      )}
    </div>
  );
}
