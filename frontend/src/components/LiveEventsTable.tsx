"use client";

import { useEffect, useState } from "react";
import { useSSE } from "@/hooks/useSSE";
import RecentEventsTable from "@/components/RecentEventsTable";
import type { RecentEvent } from "@/types/dashboard";

interface Props {
  initialEvents: RecentEvent[];
}

const SSE_URL =
  (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000") + "/stream/events";

export default function LiveEventsTable({ initialEvents }: Props) {
  const [events, setEvents] = useState<RecentEvent[]>(initialEvents);
  const { data, isConnected } = useSSE<RecentEvent>(SSE_URL);

  useEffect(() => {
    if (!data) return;
    setEvents((prev) => [data, ...prev].slice(0, 20));
  }, [data]);

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <span
          className={[
            "w-2 h-2 rounded-full",
            isConnected ? "bg-green-400 animate-pulse" : "bg-gray-400",
          ].join(" ")}
        />
        <span className="text-xs text-gray-500">
          {isConnected ? "Live" : "Connecting to live stream…"}
        </span>
      </div>
      <RecentEventsTable events={events} />
    </div>
  );
}
