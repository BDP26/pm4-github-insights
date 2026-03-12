import { Card, Metric, Text, BadgeDelta } from "@tremor/react";
import type { KpiMetric } from "@/types/dashboard";

interface KpiCardProps {
  metric: KpiMetric;
}

export default function KpiCard({ metric }: KpiCardProps) {
  return (
    <Card>
      <Text>{metric.title}</Text>
      <Metric className="mt-1">{metric.value}</Metric>
      <div className="mt-2">
        <BadgeDelta deltaType={metric.deltaType} size="sm">
          {metric.delta}
        </BadgeDelta>
      </div>
    </Card>
  );
}
