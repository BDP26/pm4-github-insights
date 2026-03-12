import { Title, Text, Card } from "@tremor/react";

export default function RepositoriesPage() {
  return (
    <div className="space-y-6">
      <div>
        <Title>Repositories</Title>
        <Text className="mt-1">
          Per-repository breakdown of commits, PRs, and contributors.
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
