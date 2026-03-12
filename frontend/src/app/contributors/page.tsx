import { Title, Text, Card } from "@tremor/react";

export default function ContributorsPage() {
  return (
    <div className="space-y-6">
      <div>
        <Title>Contributors</Title>
        <Text className="mt-1">
          Top contributors ranked by commit frequency and review activity.
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
