import { Star, GitFork, Code, Shield } from "lucide-react";
import type { HiddenGemRepo } from "@/types/hidden_gems";

interface RepoDetailCardProps {
  repo: HiddenGemRepo;
}

export default function RepoDetailCard({ repo }: RepoDetailCardProps) {
  const isSignificant = repo.sig_score >= 3.0;

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">{repo.full_name}</h1>
          <div className="flex flex-wrap items-center gap-3 mt-3 text-sm">
            {repo.language && (
              <span className="flex items-center gap-1 text-slate-600">
                <Code className="w-4 h-4" /> {repo.language}
              </span>
            )}
            {repo.license_spdx && (
              <span className="flex items-center gap-1 text-slate-600">
                <Shield className="w-4 h-4" /> {repo.license_spdx}
              </span>
            )}
          </div>
          {repo.topics.length > 0 && (
            <div className="flex flex-wrap gap-2 mt-3">
              {repo.topics.map((t) => (
                <span key={t} className="px-2 py-0.5 text-xs bg-indigo-50 text-indigo-700 rounded-full border border-indigo-100">
                  {t}
                </span>
              ))}
            </div>
          )}
        </div>

        <div className={`text-right px-4 py-3 rounded-xl ${isSignificant ? "bg-emerald-50 border border-emerald-200" : "bg-amber-50 border border-amber-200"}`}>
          <p className={`text-3xl font-bold ${isSignificant ? "text-emerald-700" : "text-amber-600"}`}>
            {repo.sig_score.toFixed(2)}
          </p>
          <p className={`text-xs mt-1 ${isSignificant ? "text-emerald-600" : "text-amber-500"}`}>
            {isSignificant ? "≥ 95% Confidence" : "< 95% Confidence"}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-6">
        {[
          { icon: Star, label: "Total Stars", value: repo.total_stars.toLocaleString(), delta: `+${repo.count_stars_interval}` },
          { icon: GitFork, label: "Total Forks", value: repo.total_forks.toLocaleString(), delta: `+${repo.count_forks_interval}` },
        ].map(({ icon: Icon, label, value, delta }) => (
          <div key={label} className="bg-slate-50 rounded-lg p-3 border border-slate-200">
            <div className="flex items-center gap-2 text-slate-500 text-xs mb-1">
              <Icon className="w-3.5 h-3.5" /> {label}
            </div>
            <p className="font-semibold text-slate-900">{value}</p>
            <p className="text-xs text-emerald-600 font-medium">{delta} this window</p>
          </div>
        ))}
      </div>
    </div>
  );
}
