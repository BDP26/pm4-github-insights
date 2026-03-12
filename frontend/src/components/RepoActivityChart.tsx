"use client";

import { Card, Title, BarChart } from "@tremor/react";
import type { RepoActivity } from "@/types/dashboard";

interface RepoActivityChartProps {
  data: RepoActivity[];
}

export default function RepoActivityChart({ data }: RepoActivityChartProps) {
  return (
    <Card>
      <Title>Top Repositories by Activity</Title>
      <BarChart
        className="mt-4 h-48"
        data={data}
        index="repo"
        categories={["events"]}
        colors={["violet"]}
        showLegend={false}
        layout="vertical"
      />
    </Card>
  );
}
