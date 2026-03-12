"use client";

import { Card, Title, AreaChart } from "@tremor/react";
import type { CommitDataPoint } from "@/types/dashboard";

interface CommitsChartProps {
  data: CommitDataPoint[];
}

export default function CommitsChart({ data }: CommitsChartProps) {
  return (
    <Card>
      <Title>Commits Over Time (Last 30 Days)</Title>
      <AreaChart
        className="mt-4 h-48"
        data={data}
        index="date"
        categories={["commits"]}
        colors={["indigo"]}
        showLegend={false}
        showGridLines
        autoMinValue
      />
    </Card>
  );
}
