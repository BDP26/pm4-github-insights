import Link from "next/link";
import { ChevronLeft } from "lucide-react";
import UserDetailCard from "@/components/UserDetailCard";
import ScoreHistoryChart from "@/components/ScoreHistoryChart";
import type { UserDetailResponse } from "@/types/hidden_gems";

export const dynamic = "force-dynamic";

const BASE_URL = process.env.API_URL ?? "http://localhost:8000";

interface Props {
  params: Promise<{ username: string }>;
  searchParams: Promise<{ interval_hours?: string }>;
}

export default async function UserDetailPage({ params, searchParams }: Props) {
  const { username } = await params;
  const { interval_hours = "168" } = await searchParams;

  let data: UserDetailResponse | null = null;
  try {
    const res = await fetch(
      `${BASE_URL}/api/hidden-gems/users/${encodeURIComponent(username)}?interval_hours=${interval_hours}`,
      { cache: "no-store" }
    );
    if (res.ok) data = await res.json();
  } catch {
    // data stays null
  }

  return (
    <div className="space-y-6">
      <Link href="/hidden-gems" className="inline-flex items-center gap-1 text-sm text-slate-500 hover:text-indigo-600 transition-colors">
        <ChevronLeft className="w-4 h-4" /> Back to Hidden Gems
      </Link>

      {data?.current ? (
        <>
          <UserDetailCard
            user={data.current}
            repos={data.repos as { full_name: string; sig_score: number; language: string | null; total_stars: number }[]}
          />
          <ScoreHistoryChart
            data={data.history}
            scoreKey="total_score"
            title="Total Score History"
          />
        </>
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-8 text-center text-slate-500">
          <p className="font-medium">No data found for <code>{username}</code></p>
          <p className="text-sm mt-2">This user may not have appeared in any scored window yet.</p>
        </div>
      )}
    </div>
  );
}
