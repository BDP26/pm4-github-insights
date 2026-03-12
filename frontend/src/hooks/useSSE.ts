"use client";

import { useEffect, useRef, useState } from "react";
import type { SSEState } from "@/types/dashboard";

/**
 * useSSE — subscribes to a Server-Sent Events endpoint.
 *
 * Usage:
 *   const { data, error, isConnected } = useSSE<MyEventPayload>(
 *     "/api/events/stream"
 *   );
 *
 * The hook automatically reconnects when the connection drops and cleans
 * up the EventSource on component unmount.
 *
 * Wire it into the UI once the FastAPI SSE endpoint is live.
 */
export function useSSE<T>(url: string): SSEState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Event | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  const sourceRef = useRef<EventSource | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    let active = true;

    function connect(): void {
      if (!active) return;

      const source = new EventSource(url);
      sourceRef.current = source;

      source.onopen = () => {
        if (!active) return;
        setIsConnected(true);
        setError(null);
      };

      source.onmessage = (event: MessageEvent) => {
        if (!active) return;
        try {
          const parsed = JSON.parse(event.data as string) as T;
          setData(parsed);
        } catch {
          // Non-JSON message — ignore
        }
      };

      source.onerror = (evt: Event) => {
        if (!active) return;
        setIsConnected(false);
        setError(evt);
        source.close();
        sourceRef.current = null;

        // Exponential-ish back-off: reconnect after 3 s
        reconnectTimerRef.current = setTimeout(() => {
          if (active) connect();
        }, 3000);
      };
    }

    connect();

    return () => {
      active = false;
      if (reconnectTimerRef.current !== null) {
        clearTimeout(reconnectTimerRef.current);
      }
      sourceRef.current?.close();
      sourceRef.current = null;
    };
  }, [url]);

  return { data, error, isConnected };
}
