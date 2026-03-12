import {
  Card,
  Title,
  Table,
  TableHead,
  TableHeaderCell,
  TableBody,
  TableRow,
  TableCell,
  Badge,
} from "@tremor/react";
import type { RecentEvent } from "@/types/dashboard";

interface RecentEventsTableProps {
  events: RecentEvent[];
}

const EVENT_COLORS: Record<string, "blue" | "green" | "orange" | "purple" | "gray"> = {
  PushEvent: "blue",
  PullRequestEvent: "green",
  IssuesEvent: "orange",
  PullRequestReviewEvent: "purple",
  IssueCommentEvent: "gray",
};

function formatRelative(isoString: string): string {
  const diff = Math.floor((Date.now() - new Date(isoString).getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

export default function RecentEventsTable({ events }: RecentEventsTableProps) {
  return (
    <Card>
      <Title>Recent Events</Title>
      <Table className="mt-4">
        <TableHead>
          <TableRow>
            <TableHeaderCell>Repository</TableHeaderCell>
            <TableHeaderCell>Event Type</TableHeaderCell>
            <TableHeaderCell>Actor</TableHeaderCell>
            <TableHeaderCell>Time</TableHeaderCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {events.map((event) => (
            <TableRow key={event.id}>
              <TableCell className="font-mono text-xs">{event.repo}</TableCell>
              <TableCell>
                <Badge color={EVENT_COLORS[event.eventType] ?? "gray"} size="sm">
                  {event.eventType}
                </Badge>
              </TableCell>
              <TableCell>{event.actor}</TableCell>
              <TableCell className="text-gray-500 text-xs">
                {formatRelative(event.timestamp)}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Card>
  );
}
