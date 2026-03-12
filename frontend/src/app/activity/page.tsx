import { Title, Text, Card } from "@tremor/react";

export default function ActivityPage() {
  return (
    <div className="space-y-6">
      <div>
        <Title>Activity</Title>
        <Text className="mt-1">
          Live event stream and historical activity log.
        </Text>
      </div>
      <Card>
        <Text className="text-gray-400 text-sm">
          Coming soon — will be populated once the SSE endpoint is wired in.
        </Text>
      </Card>
    </div>
  );
}
