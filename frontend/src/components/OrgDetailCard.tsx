import type { HiddenGemOrg } from "@/types/hidden_gems";

interface OrgDetailCardProps {
  org: HiddenGemOrg;
}

function StatBox({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="bg-slate-50 rounded-lg p-3 border border-slate-200">
      <p className="text-xs text-slate-500 mb-1">{label}</p>
      <p className="font-semibold text-slate-900">{value}</p>
    </div>
  );
}

export default function OrgDetailCard({ org }: OrgDetailCardProps) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
      <div className="flex items-start justify-between gap-4">
        <h1 className="text-2xl font-bold text-slate-900">{org.org_login}</h1>
        <div className="text-right px-4 py-3 rounded-xl bg-emerald-50 border border-emerald-200">
          <p className="text-3xl font-bold text-emerald-700">
            {org.org_repos_total_score != null ? org.org_repos_total_score.toFixed(2) : "—"}
          </p>
          <p className="text-xs mt-1 text-emerald-600">Org Score</p>
        </div>
      </div>

      <div className="mt-6 space-y-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Org-Owned Repos</p>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatBox label="Total Score"    value={org.org_repos_total_score != null ? org.org_repos_total_score.toFixed(2) : "—"} />
            <StatBox label="Best Score"     value={org.org_repos_best_score != null ? org.org_repos_best_score.toFixed(2) : "—"} />
            <StatBox label="Active Repos"   value={org.org_active_repos} />
            <StatBox label="Gems (≥3.0)"    value={org.org_hidden_gem_count} />
          </div>
        </div>
        <div>
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-500 mb-2">Member Repos</p>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatBox label="Total Score"    value={org.member_repos_total_score != null ? org.member_repos_total_score.toFixed(2) : "—"} />
            <StatBox label="Best Score"     value={org.member_repos_best_score != null ? org.member_repos_best_score.toFixed(2) : "—"} />
            <StatBox label="Active Repos"   value={org.member_active_repos} />
            <StatBox label="Active Users"   value={org.member_active_users} />
          </div>
        </div>
      </div>
    </div>
  );
}
