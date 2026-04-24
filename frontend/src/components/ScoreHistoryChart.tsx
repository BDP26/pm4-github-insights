import { LineChart } from "@tremor/react";
import type { ScoreHistoryPoint, UserHistoryPoint, OrgHistoryPoint } from "@/types/hidden_gems";

type HistoryPoint = ScoreHistoryPoint | UserHistoryPoint | OrgHistoryPoint;

interface ScoreHistoryChartProps {
  data: HistoryPoint[];
  scoreKey: string;
  title?: string;
}

export default function ScoreHistoryChart({
  data,
  scoreKey,
  title = "Score History",
}: ScoreHistoryChartProps) {
  const chartData = data.map((p) => ({
    date: new Date(p.run_at).toLocaleDateString("en-CH", {
      month: "short",
      day: "numeric",
    }),
    Score: (p as unknown as Record<string, unknown>)[scoreKey] as number ?? 0,
  }));

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
      <h3 className="font-semibold text-slate-800 mb-4">{title}</h3>
      {chartData.length === 0 ? (
        <p className="text-sm text-slate-500 text-center py-8">
          No snapshot history yet. History builds up as the scheduler runs.
        </p>
      ) : (
        <LineChart
          data={chartData}
          index="date"
          categories={["Score"]}
          colors={["indigo"]}
          showLegend={false}
          yAxisWidth={48}
          className="h-48"
        />
      )}
    </div>
  );
}
