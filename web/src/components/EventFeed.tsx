import { useEffect, useRef } from "react";
import type { UIEvent } from "../types";

interface EventFeedProps {
  events: UIEvent[];
  filter?: (event: UIEvent) => boolean;
  maxVisible?: number;
}

export function EventFeed({ events, filter, maxVisible = 100 }: EventFeedProps) {
  const bottomRef = useRef<HTMLDivElement>(null);

  const filtered = filter ? events.filter(filter) : events;
  const visible = filtered.slice(-maxVisible);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [visible.length]);

  if (visible.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-gray-600 text-sm">
        Waiting for events…
      </div>
    );
  }

  return (
    <div className="event-feed h-full">
      {visible.map((event, i) => (
        <div key={`${event.timestamp}-${i}`} className="event-entry flex items-start gap-2">
          {/* Timestamp */}
          <span className="text-gray-600 text-xs shrink-0 w-12 pt-0.5 font-medium">
            {formatTime(event.timestamp)}
          </span>

          {/* Icon */}
          <span
            className="shrink-0 text-sm pt-0.5"
            style={{ color: event.color }}
          >
            {event.icon || "•"}
          </span>

          {/* Specialist badge */}
          {event.specialist && (
            <span
              className="shrink-0 text-xs font-semibold px-1.5 py-0.5 rounded"
              style={{
                color: event.color,
                backgroundColor: `${event.color}15`,
              }}
            >
              {event.specialist}
            </span>
          )}

          {/* Action text */}
          <span className="text-gray-300 text-sm leading-5 truncate">
            {event.action}
          </span>
        </div>
      ))}
      <div ref={bottomRef} />
    </div>
  );
}

function formatTime(ts: number): string {
  const d = new Date(ts * 1000);
  return `${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}`;
}
