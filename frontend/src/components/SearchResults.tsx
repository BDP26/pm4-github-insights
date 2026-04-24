"use client";

import Link from "next/link";
import { BookOpen, User, Building2 } from "lucide-react";
import type { SearchResult } from "@/types/hidden_gems";

interface SearchResultsProps {
  results: SearchResult[];
  onClose: () => void;
}

const TYPE_CONFIG = {
  repo:  { icon: BookOpen,   label: "Repo",  href: (n: string) => `/hidden-gems/repos/${n}` },
  user:  { icon: User,       label: "User",  href: (n: string) => `/hidden-gems/users/${n}` },
  org:   { icon: Building2,  label: "Org",   href: (n: string) => `/hidden-gems/orgs/${n}`  },
} as const;

export default function SearchResults({ results, onClose }: SearchResultsProps) {
  if (results.length === 0) {
    return (
      <div className="absolute top-full mt-1 w-full bg-white border border-slate-200 rounded-lg shadow-lg z-50 p-4 text-sm text-slate-500 text-center">
        No results found
      </div>
    );
  }

  return (
    <div className="absolute top-full mt-1 w-full bg-white border border-slate-200 rounded-lg shadow-lg z-50 overflow-hidden">
      <ul className="divide-y divide-slate-100 max-h-80 overflow-y-auto">
        {results.map((r) => {
          const cfg = TYPE_CONFIG[r.type];
          const Icon = cfg.icon;
          return (
            <li key={`${r.type}:${r.name}`}>
              <Link
                href={cfg.href(r.name)}
                onClick={onClose}
                className="flex items-center gap-3 px-4 py-3 hover:bg-slate-50 transition-colors"
              >
                <Icon className="w-4 h-4 text-slate-400 flex-shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-slate-800 truncate">{r.name}</p>
                  <p className="text-xs text-slate-500">{cfg.label}</p>
                </div>
                {r.score !== null && (
                  <span className={`text-xs font-semibold ${r.score >= 3 ? "text-emerald-600" : "text-amber-500"}`}>
                    {r.score.toFixed(2)}
                  </span>
                )}
              </Link>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
