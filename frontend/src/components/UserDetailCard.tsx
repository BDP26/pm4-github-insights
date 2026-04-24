import Link from "next/link";
import { BookOpen, Star } from "lucide-react";
import type { HiddenGemUser } from "@/types/hidden_gems";

interface UserDetailCardProps {
  user: HiddenGemUser;
  repos: { full_name: string; sig_score: number; language: string | null; total_stars: number }[];
}

export default function UserDetailCard({ user, repos }: UserDetailCardProps) {
  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">{user.username}</h1>
            <p className="text-slate-500 mt-1 text-sm">
              {user.hidden_gem_count} hidden gems · {user.active_repos_in_window} active repos in window
            </p>
            {user.best_repo && (
              <p className="text-sm text-slate-600 mt-2">
                Best repo:{" "}
                <Link href={`/hidden-gems/repos/${user.best_repo}`} className="text-indigo-600 hover:underline">
                  {user.best_repo}
                </Link>
              </p>
            )}
          </div>
          <div className="text-right px-4 py-3 rounded-xl bg-emerald-50 border border-emerald-200">
            <p className="text-3xl font-bold text-emerald-700">
              {user.total_score != null ? user.total_score.toFixed(2) : "—"}
            </p>
            <p className="text-xs mt-1 text-emerald-600">Total Score</p>
          </div>
        </div>
      </div>

      {repos.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-100 bg-slate-50">
            <h3 className="font-semibold text-slate-800">Scored Repositories</h3>
          </div>
          <ul className="divide-y divide-slate-100">
            {repos.map((r) => (
              <li key={r.full_name} className="flex items-center justify-between px-6 py-3 hover:bg-slate-50">
                <Link
                  href={`/hidden-gems/repos/${r.full_name}`}
                  className="flex items-center gap-2 text-indigo-600 font-medium hover:underline text-sm"
                >
                  <BookOpen className="w-4 h-4 text-slate-400" />
                  {r.full_name}
                </Link>
                <div className="flex items-center gap-4 text-sm text-slate-600">
                  <span className="flex items-center gap-1">
                    <Star className="w-3.5 h-3.5 text-amber-400" />
                    {r.total_stars.toLocaleString()}
                  </span>
                  <span className={`font-semibold ${r.sig_score >= 3 ? "text-emerald-600" : "text-amber-500"}`}>
                    {r.sig_score.toFixed(2)}
                  </span>
                </div>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
