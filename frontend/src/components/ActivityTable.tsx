"use client";

import Link from "next/link";
import { Star, GitFork, BookOpen } from "lucide-react";
import type {
  ActivityScope,
  ActivityItem,
  ActivityRepoItem,
  ActivityUserItem,
  ActivityOrgItem,
} from "@/types/activity";

interface ActivityTableProps {
  items: ActivityItem[];
  scope: ActivityScope;
}

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}k`;
  return n.toLocaleString();
}

function isRepoItem(item: ActivityItem): item is ActivityRepoItem {
  return "repo_id" in item;
}

function isUserItem(item: ActivityItem): item is ActivityUserItem {
  return "username" in item;
}

function isOrgItem(item: ActivityItem): item is ActivityOrgItem {
  return "org_login" in item;
}

function ScoreCell({ score }: { score: number }) {
  return (
    <span className="font-bold text-indigo-600 tabular-nums">
      {fmt(Math.round(score))}
    </span>
  );
}

function RepoRow({ item, rank }: { item: ActivityRepoItem; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors cursor-pointer">
      <td className="p-4 text-center text-sm font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/repos/${item.full_name}`}
          className="text-indigo-600 font-semibold hover:underline flex items-center gap-1"
        >
          <BookOpen className="w-4 h-4 text-slate-400 shrink-0" />
          {item.full_name}
        </Link>
        {item.language && (
          <span className="text-xs text-slate-500 mt-1 flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-blue-500 inline-block" />
            {item.language}
          </span>
        )}
      </td>
      <td className="p-4 text-sm text-slate-600">{item.owner_login}</td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <Star className="w-3.5 h-3.5 text-amber-400" />
          {fmt(item.total_stars)}
        </div>
      </td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <GitFork className="w-3.5 h-3.5 text-slate-400" />
          {fmt(item.total_forks)}
        </div>
      </td>
      <td className="p-4"><ScoreCell score={item.impact_score} /></td>
    </tr>
  );
}

function UserRow({ item, rank }: { item: ActivityUserItem; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors cursor-pointer">
      <td className="p-4 text-center text-sm font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/users/${item.username}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.username}
        </Link>
        {item.location && (
          <p className="text-xs text-slate-500 mt-0.5">{item.location}</p>
        )}
      </td>
      <td className="p-4 text-sm text-slate-600">{item.total_repos}</td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <Star className="w-3.5 h-3.5 text-amber-400" />
          {fmt(item.total_stars)}
        </div>
      </td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <GitFork className="w-3.5 h-3.5 text-slate-400" />
          {fmt(item.total_forks)}
        </div>
      </td>
      <td className="p-4"><ScoreCell score={item.impact_score} /></td>
    </tr>
  );
}

function OrgRow({ item, rank }: { item: ActivityOrgItem; rank: number }) {
  return (
    <tr className="hover:bg-slate-50 transition-colors cursor-pointer">
      <td className="p-4 text-center text-sm font-medium text-slate-400">#{rank}</td>
      <td className="p-4">
        <Link
          href={`/hidden-gems/orgs/${item.org_login}`}
          className="text-indigo-600 font-semibold hover:underline"
        >
          {item.org_login}
        </Link>
      </td>
      <td className="p-4 text-sm text-slate-600">{item.total_repos}</td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <Star className="w-3.5 h-3.5 text-amber-400" />
          {fmt(item.total_stars)}
        </div>
      </td>
      <td className="p-4">
        <div className="flex items-center gap-1 text-sm">
          <GitFork className="w-3.5 h-3.5 text-slate-400" />
          {fmt(item.total_forks)}
        </div>
      </td>
      <td className="p-4"><ScoreCell score={item.impact_score} /></td>
    </tr>
  );
}

const HEADERS: Record<ActivityScope, string[]> = {
  repos: ["#", "Repository", "Owner",  "Stars", "Forks", "Score"],
  users: ["#", "Username",   "Repos",  "Stars", "Forks", "Score"],
  orgs:  ["#", "Org",        "Repos",  "Stars", "Forks", "Score"],
};

export default function ActivityTable({ items, scope }: ActivityTableProps) {
  if (items.length === 0) {
    return (
      <div className="bg-white border border-slate-200 rounded-xl shadow-sm p-12 text-center text-slate-400">
        No data available.
      </div>
    );
  }

  return (
    <div className="bg-white border border-slate-200 rounded-xl shadow-sm overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full text-left border-collapse">
          <thead>
            <tr className="bg-slate-50 border-b border-slate-200 text-xs uppercase tracking-wider text-slate-500 font-semibold">
              {HEADERS[scope].map((h, i) => (
                <th key={i} className="p-4">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {items.map((item, idx) => {
              const rank = idx + 1;
              if (isRepoItem(item)) {
                return <RepoRow key={item.repo_id} item={item} rank={rank} />;
              }
              if (isUserItem(item)) {
                return <UserRow key={item.username} item={item} rank={rank} />;
              }
              if (isOrgItem(item)) {
                return <OrgRow key={item.org_login} item={item} rank={rank} />;
              }
              return null;
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
