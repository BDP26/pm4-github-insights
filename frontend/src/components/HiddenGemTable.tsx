"use client";

import Link from "next/link";
import {
  Star, GitFork, BookOpen, ChevronLeft, ChevronRight,
} from "lucide-react";
import type { HiddenGemRepo, HiddenGemUser, HiddenGemOrg } from "@/types/hidden_gems";

type Scope = "repos" | "users" | "orgs";

interface HiddenGemTableProps {
  items: (HiddenGemRepo | HiddenGemUser | HiddenGemOrg)[];
  scope: Scope;
  page: number;
  onPageChange: (p: number) => void;
}

function ScoreBadge({ score }: { score: number }) {
  const isSignificant = score >= 3.0;
  return (
    <div className="flex flex-col items-start">
      <span className={`text-lg font-bold ${isSignificant ? "text-emerald-600" : "text-amber-500"}`}>
        {score.toFixed(2)}
      </span>
      <span className="text-xs text-slate-400 mt-0.5">
        {isSignificant ? "> 95% Conf." : "< 95% Conf."}
      </span>
    </div>
  );
}

function RepoRow({ item, rank }: { item: HiddenGemRepo; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors">
      <td className="p-4 text-center font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <div className="flex flex-col">
          <Link
            href={`/hidden-gems/repos/${item.full_name}`}
            className="text-indigo-600 font-semibold hover:underline flex items-center gap-1 text-base"
          >
            <BookOpen className="w-4 h-4 text-slate-400" />
            {item.full_name}
          </Link>
          <div className="flex items-center gap-3 mt-2 text-xs font-medium text-slate-500">
            {item.language && (
              <span className="flex items-center gap-1">
                <span className="w-2 h-2 rounded-full bg-blue-500" />
                {item.language}
              </span>
            )}
            {item.license_spdx && (
              <span className="px-1.5 py-0.5 rounded bg-slate-100 border border-slate-200">
                {item.license_spdx}
              </span>
            )}
          </div>
        </div>
      </td>
      <td className="p-4">
        <ScoreBadge score={item.sig_score} />
      </td>
      <td className="p-4">
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <Star className="w-4 h-4 text-amber-400" />
            <span className="text-sm font-semibold text-slate-700">
              {item.total_stars.toLocaleString()}
            </span>
            <span className="text-xs font-medium text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded">
              +{item.count_stars_interval}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <GitFork className="w-4 h-4 text-slate-400" />
            <span className="text-sm font-semibold text-slate-700">
              {item.total_forks.toLocaleString()}
            </span>
            <span className="text-xs font-medium text-emerald-600 bg-emerald-50 px-1.5 py-0.5 rounded">
              +{item.count_forks_interval}
            </span>
          </div>
        </div>
      </td>
    </tr>
  );
}

function UserRow({ item, rank }: { item: HiddenGemUser; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors">
      <td className="p-4 text-center font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/users/${item.username}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.username}
        </Link>
        {item.best_repo && (
          <p className="text-xs text-slate-500 mt-1">Best: {item.best_repo}</p>
        )}
      </td>
      <td className="p-4">
        <ScoreBadge score={item.total_score} />
      </td>
      <td className="p-4">
        <span className="text-sm text-slate-600">
          {item.hidden_gem_count} gems · {item.active_repos_in_window} active repos
        </span>
      </td>
    </tr>
  );
}

function OrgRow({ item, rank }: { item: HiddenGemOrg; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors">
      <td className="p-4 text-center font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/orgs/${item.org_login}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.org_login}
        </Link>
      </td>
      <td className="p-4">
        <ScoreBadge score={item.org_repos_total_score} />
      </td>
      <td className="p-4">
        <span className="text-sm text-slate-600">
          {item.org_hidden_gem_count} org gems · {item.member_hidden_gem_count} member gems
        </span>
      </td>
    </tr>
  );
}

export default function HiddenGemTable({
  items, scope, page, onPageChange,
}: HiddenGemTableProps) {
  const offset = (page - 1) * 25;

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-50 border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500 font-semibold">
              <th className="p-4 w-12 text-center">Rank</th>
              <th className="p-4">{scope === "repos" ? "Repository" : scope === "users" ? "User" : "Organisation"}</th>
              <th className="p-4">Sig. Score</th>
              <th className="p-4">Activity</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {items.map((item, idx) => {
              const rank = offset + idx + 1;
              if (scope === "repos") return <RepoRow key={(item as HiddenGemRepo).repo_id} item={item as HiddenGemRepo} rank={rank} />;
              if (scope === "users") return <UserRow key={(item as HiddenGemUser).username} item={item as HiddenGemUser} rank={rank} />;
              return <OrgRow key={(item as HiddenGemOrg).org_login} item={item as HiddenGemOrg} rank={rank} />;
            })}
          </tbody>
        </table>
      </div>
      <div className="bg-slate-50 border-t border-slate-200 p-4 flex items-center justify-between text-sm text-slate-600">
        <span>Page {page}</span>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={() => onPageChange(page - 1)}
            disabled={page === 1}
            className="px-3 py-1 bg-white border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-40 flex items-center gap-1"
          >
            <ChevronLeft className="w-4 h-4" /> Prev
          </button>
          <button
            type="button"
            onClick={() => onPageChange(page + 1)}
            disabled={items.length < 25}
            className="px-3 py-1 bg-white border border-slate-300 rounded hover:bg-slate-50 disabled:opacity-40 flex items-center gap-1"
          >
            Next <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
