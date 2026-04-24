"use client";

import Link from "next/link";
import {
  BookOpen, CheckCircle, AlertTriangle, Clock,
  ArrowUpRight, ArrowDownRight,
} from "lucide-react";
import type { CohortEntry, CohortSummary } from "@/types/hidden_gems";

interface CohortTableProps {
  summary: CohortSummary;
  repos: CohortEntry[];
  reportLabel: string;
}

function ClassificationBadge({ classification }: { classification: CohortEntry["classification"] }) {
  if (classification === "true_positive") {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-emerald-50 text-emerald-700 border border-emerald-200">
        <CheckCircle className="w-3 h-3" /> Sustained
      </span>
    );
  }
  if (classification === "false_positive") {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-rose-50 text-rose-700 border border-rose-200">
        <AlertTriangle className="w-3 h-3" /> Dropped Off
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium bg-slate-100 text-slate-600 border border-slate-200">
      <Clock className="w-3 h-3" /> Pending
    </span>
  );
}

export default function CohortTable({ summary, repos, reportLabel }: CohortTableProps) {
  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-white p-5 rounded-xl border border-slate-200 shadow-sm flex items-center justify-between">
          <div>
            <p className="text-sm font-medium text-slate-500">Total Flagged</p>
            <p className="text-2xl font-bold text-slate-900 mt-1">{summary.total}</p>
          </div>
          <div className="w-10 h-10 rounded-full bg-blue-50 flex items-center justify-center">
            <BookOpen className="w-5 h-5 text-blue-600" />
          </div>
        </div>
        <div className="bg-white p-5 rounded-xl border border-emerald-200 shadow-sm flex items-center justify-between ring-1 ring-emerald-50">
          <div>
            <p className="text-sm font-medium text-emerald-600">Sustained Gems</p>
            <p className="text-2xl font-bold text-emerald-700 mt-1">{summary.sustained}</p>
          </div>
          <div className="w-10 h-10 rounded-full bg-emerald-100 flex items-center justify-center">
            <CheckCircle className="w-5 h-5 text-emerald-600" />
          </div>
        </div>
        <div className="bg-white p-5 rounded-xl border border-rose-200 shadow-sm flex items-center justify-between ring-1 ring-rose-50">
          <div>
            <p className="text-sm font-medium text-rose-600">Dropped Off</p>
            <p className="text-2xl font-bold text-rose-700 mt-1">{summary.dropped}</p>
          </div>
          <div className="w-10 h-10 rounded-full bg-rose-100 flex items-center justify-center">
            <AlertTriangle className="w-5 h-5 text-rose-600" />
          </div>
        </div>
      </div>

      {/* Table */}
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
        <div className="px-6 py-4 border-b border-slate-200 bg-slate-50">
          <h3 className="font-semibold text-slate-800">Cohort Analysis: {reportLabel}</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="bg-white border-b border-slate-100 text-xs uppercase tracking-wider text-slate-500 font-semibold">
                <th className="p-4 pl-6">Repository</th>
                <th className="p-4">Classification</th>
                <th className="p-4">Score Trajectory</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {repos.map((repo) => (
                <tr key={repo.repo_id} className="hover:bg-slate-50 transition-colors">
                  <td className="p-4 pl-6">
                    <Link
                      href={`/hidden-gems/repos/${repo.full_name}`}
                      className="text-indigo-600 font-medium hover:underline flex items-center gap-1.5"
                    >
                      <BookOpen className="w-4 h-4 text-slate-400" />
                      {repo.full_name}
                    </Link>
                  </td>
                  <td className="p-4">
                    <ClassificationBadge classification={repo.classification} />
                  </td>
                  <td className="p-4">
                    <div className="flex items-center gap-3">
                      <span className="font-semibold text-slate-700">
                        {repo.prev_score.toFixed(1)}
                      </span>
                      {repo.current_score === null ? (
                        <Clock className="w-5 h-5 text-slate-400" />
                      ) : repo.current_score >= repo.prev_score ? (
                        <ArrowUpRight className="w-5 h-5 text-emerald-500" />
                      ) : (
                        <ArrowDownRight className="w-5 h-5 text-rose-500" />
                      )}
                      {repo.current_score !== null && (
                        <span className={`font-semibold ${repo.current_score >= 1.5 ? "text-emerald-600" : "text-rose-500"}`}>
                          {repo.current_score.toFixed(1)}
                        </span>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
