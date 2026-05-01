"use client";

import { useEffect, useRef, useState } from "react";
import { useSSE } from "@/hooks/useSSE";
import RecentEventsTable from "@/components/RecentEventsTable";
import type { RecentEvent } from "@/types/dashboard";

interface Props {
  initialEvents: RecentEvent[];
}

const API_BASE = "";
const SSE_URL  = `${API_BASE}/stream/events`;

// Poll interval when SSE is disconnected — keeps table fresh even without SSE
const POLL_INTERVAL_MS = 30_000;

export default function LiveEventsTable({ initialEvents }: Props) {
  const [events, setEvents] = useState<RecentEvent[]>(initialEvents);
  const { data, isConnected } = useSSE<RecentEvent>(SSE_URL);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Prepend incoming SSE events, deduplicating by id
  useEffect(() => {
    if (!data) return;
    setEvents((prev) => {
      if (prev.some((e) => e.id === data.id)) return prev;
      return [data, ...prev].slice(0, 20);
    });
  }, [data]);

  // Fallback: poll REST endpoint every 30 s when SSE is not connected
  useEffect(() => {
    if (isConnected) {
      if (pollRef.current !== null) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
      return;
    }

    async function poll() {
      try {
        const res = await fetch(`${API_BASE}/api/recent-events?limit=20`, {
          cache: "no-store",
        });
        if (!res.ok) return;
        const data: RecentEvent[] = await res.json();
        setEvents(data);
      } catch {
        // network error — silently ignore, will retry
      }
    }

    poll(); // immediate first poll when SSE drops
    pollRef.current = setInterval(poll, POLL_INTERVAL_MS);

    return () => {
      if (pollRef.current !== null) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [isConnected]);

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <span
          className={[
            "w-2 h-2 rounded-full",
            isConnected ? "bg-green-400 animate-pulse" : "bg-amber-400",
          ].join(" ")}
        />
        <span className="text-xs text-gray-500">
          {isConnected ? "Live" : "Polling for updates…"}
        </span>
      </div>
      <RecentEventsTable events={events} />
    </div>
  );
}
