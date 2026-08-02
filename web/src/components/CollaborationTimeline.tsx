import { useMemo, useState, useEffect, useRef, useCallback } from "react";
import type { UIEvent } from "../types";

interface CollaborationTimelineProps {
  events: UIEvent[];
}

/** Event types that show in the collaboration timeline */
const COLLAB_EVENT_TYPES = new Set([
  "architect_decision",
  "blackboard_publication",
  "finding_consumed",
  "challenge_raised",
  "consensus_formed",
  "execution_started",
  "execution_completed",
  "report_generated",
  "task_board_transition",
  "graph_started",
  "graph_completed",
  "recovery_initiated",
  "recovery_completed",
  "recovery_failed",
  "node_transition",
  "task_created",
  "task_assigned",
  "task_completed",
  "task_failed",
  "verification_started",
  "verification_passed",
  "verification_failed",
  "verification_running",
  "system_online",
]);

/** Group label and color for specialist-related event clusters */
const SPECIALIST_GROUPS: Record<string, { label: string; color: string }> = {
  ARCHITECT: { label: "Architect", color: "#3b82f6" },
  ORACLE: { label: "Oracle", color: "#8c5cff" },
  FORGE: { label: "Forge", color: "#00e38c" },
  SENTINEL: { label: "Sentinel", color: "#ff5c7a" },
  TERMINUS: { label: "Terminus", color: "#f7b731" },
  HERALD: { label: "Herald", color: "#39c8ff" },
  CONSENSUS: { label: "Consensus", color: "#19f5a5" },
};

/** Helper: format timestamp to human-readable string */
function formatTimestamp(ts: number): string {
  const d = new Date(ts * 1000);
  const now = Date.now();
  const diff = now - d.getTime();

  // Today: show HH:MM
  if (d.toDateString() === new Date().toDateString()) {
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }
  // This week: show day + HH:MM
  if (diff < 7 * 86400000) {
    return d.toLocaleDateString([], { weekday: "short", hour: "2-digit", minute: "2-digit" });
  }
  // Older: show date
  return d.toLocaleDateString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

/** Helper: format relative time for tooltips */
function formatRelative(ts: number): string {
  const diff = Date.now() - ts * 1000;
  if (diff < 60000) return "just now";
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return `${Math.floor(diff / 86400000)}d ago`;
}

export function CollaborationTimeline({ events }: CollaborationTimelineProps) {
  const [search, setSearch] = useState("");
  const [specialistFilter, setSpecialistFilter] = useState<string | null>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [selectedEvent, setSelectedEvent] = useState<number | null>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const prevCountRef = useRef(events.length);

  // Filter and sort events
  const timelineEvents = useMemo(() => {
    let filtered = events.filter((e) => COLLAB_EVENT_TYPES.has(e.type));

    // Specialist filter
    if (specialistFilter) {
      filtered = filtered.filter(
        (e) => e.specialist.toUpperCase() === specialistFilter
      );
    }

    // Search filter
    if (search.trim()) {
      const q = search.toLowerCase();
      filtered = filtered.filter(
        (e) =>
          e.action.toLowerCase().includes(q) ||
          e.specialist.toLowerCase().includes(q) ||
          e.type.toLowerCase().includes(q)
      );
    }

    // Sort by timestamp ascending
    return filtered.sort((a, b) => a.timestamp - b.timestamp);
  }, [events, specialistFilter, search]);

  // Auto-scroll on new events
  useEffect(() => {
    if (autoScroll && events.length > prevCountRef.current) {
      bottomRef.current?.scrollIntoView({ behavior: "smooth" });
    }
    prevCountRef.current = events.length;
  }, [events.length, autoScroll]);

  // Detect scroll position for auto-scroll toggle
  const handleScroll = useCallback(() => {
    if (!containerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
    const isNearBottom = scrollHeight - scrollTop - clientHeight < 100;
    setAutoScroll(isNearBottom);
  }, []);

  // Get unique specialists from events
  const activeSpecialists = useMemo(() => {
    const specs = new Set<string>();
    for (const e of events) {
      if (e.specialist && COLLAB_EVENT_TYPES.has(e.type)) {
        specs.add(e.specialist.toUpperCase());
      }
    }
    return Array.from(specs).sort();
  }, [events]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* ── Controls Bar ─────────────────────────────────── */}
      <div className="border-b border-surface-border px-6 py-3 space-y-3 shrink-0">
        {/* Top row: title + count */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h2 className="text-lg font-bold text-ink">Collaboration Timeline</h2>
            <span className="text-xs text-ink-muted">{timelineEvents.length} events</span>
          </div>
          <div className="flex items-center gap-2 text-xs text-ink-muted">
            <span>Auto-scroll</span>
            <button
              onClick={() => setAutoScroll(!autoScroll)}
              className={`w-8 h-4 rounded-full transition-colors duration-200 ${
                autoScroll ? "bg-accent-blue" : "bg-surface-border"
              }`}
            >
              <span
                className={`block w-3 h-3 rounded-full bg-white transition-transform duration-200 ${
                  autoScroll ? "translate-x-[18px]" : "translate-x-[2px]"
                }`}
              />
            </button>
          </div>
        </div>

        {/* Search + filters */}
        <div className="flex items-center gap-3">
          <div className="relative flex-1 max-w-sm">
            <span className="absolute left-3 top-1/2 -translate-y-1/2 text-ink-muted text-sm">🔍</span>
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search events..."
              className="w-full bg-surface border border-surface-border rounded-lg pl-9 pr-3 py-1.5 text-sm text-ink placeholder-ink-muted focus:outline-none focus:border-accent-blue/50 transition-colors"
            />
          </div>

          {/* Specialist filter chips */}
          <div className="flex items-center gap-1.5 overflow-x-auto">
            <button
              onClick={() => setSpecialistFilter(null)}
              className={`text-xs px-2.5 py-1 rounded-full border transition-colors shrink-0 ${
                specialistFilter === null
                  ? "bg-accent-blue/10 text-accent-blue border-accent-blue/30"
                  : "text-ink-muted border-surface-border hover:border-surface-border"
              }`}
            >
              All
            </button>
            {activeSpecialists.map((spec) => {
              const group = SPECIALIST_GROUPS[spec];
              const color = group?.color || "#52627f";
              return (
                <button
                  key={spec}
                  onClick={() =>
                    setSpecialistFilter(specialistFilter === spec ? null : spec)
                  }
                  className={`text-xs px-2.5 py-1 rounded-full border transition-colors shrink-0 ${
                    specialistFilter === spec
                      ? "text-white border-transparent"
                      : "text-ink-soft border-surface-border hover:border-surface-border"
                  }`}
                  style={
                    specialistFilter === spec
                      ? { backgroundColor: color, borderColor: color }
                      : undefined
                  }
                >
                  {group?.label || spec}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* ── Timeline Content ─────────────────────────────── */}
      <div
        ref={containerRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto"
      >
        {timelineEvents.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <div className="text-3xl mb-2 text-ink-muted">◈</div>
              <p className="text-ink-muted text-sm">
                {search || specialistFilter
                  ? "No events match your filters"
                  : "Waiting for collaboration events…"}
              </p>
            </div>
          </div>
        ) : (
          <div className="relative px-6 py-6">
            {/* Vertical connector line */}
            <div className="absolute left-[60px] top-0 bottom-0 w-px bg-surface-border" />

            <div className="space-y-1">
              {timelineEvents.map((event, idx) => (
                <TimelineEntry
                  key={`${event.timestamp}-${idx}`}
                  event={event}
                  index={idx}
                  isSelected={selectedEvent === idx}
                  onToggle={() =>
                    setSelectedEvent(selectedEvent === idx ? null : idx)
                  }
                />
              ))}
            </div>
            <div ref={bottomRef} />
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Single Timeline Entry ────────────────────────────────────── */

interface TimelineEntryProps {
  event: UIEvent;
  index: number;
  isSelected: boolean;
  onToggle: () => void;
}

function TimelineEntry({ event, isSelected, onToggle }: TimelineEntryProps) {
  const specialist = event.specialist?.toUpperCase() || "";
  const group = SPECIALIST_GROUPS[specialist];
  const dotColor = group?.color || event.color;

  // Human-readable event category
  const category = (() => {
    const t = event.type;
    if (t === "architect_decision") return "Decision";
    if (t === "blackboard_publication") return "Finding";
    if (t === "finding_consumed") return "Consumed";
    if (t === "challenge_raised") return "Challenge";
    if (t === "consensus_formed") return "Consensus";
    if (t === "execution_started" || t === "execution_completed") return "Execution";
    if (t === "report_generated") return "Report";
    if (t === "task_board_transition") return "Task";
    if (t === "graph_started" || t === "graph_completed") return "Graph";
    if (t.startsWith("recovery")) return "Recovery";
    if (t.startsWith("task_")) return "Task";
    if (t.startsWith("verification")) return "Verification";
    if (t === "system_online") return "System";
    return "Event";
  })();

  return (
    <div className="relative flex gap-4 group">
      {/* Timeline dot + connector */}
      <div className="relative shrink-0 flex flex-col items-center w-[72px]">
        {/* Timestamp */}
        <span className="text-[10px] text-ink-muted font-mono whitespace-nowrap mb-2">
          {formatTimestamp(event.timestamp)}
        </span>
        {/* Dot */}
        <div
          className="w-3 h-3 rounded-full border-2 border-surface z-10 transition-transform duration-150 group-hover:scale-125"
          style={{ backgroundColor: dotColor, borderColor: dotColor }}
        />
      </div>

      {/* Event card */}
      <div
        className="flex-1 pb-3 min-w-0 cursor-pointer"
        onClick={onToggle}
      >
        <div
          className={`border rounded-lg px-4 py-2.5 transition-all duration-150 ${
            isSelected
              ? "border-accent-blue/30 bg-accent-blue/5"
              : "border-surface-border bg-surface-alt/50 hover:bg-surface-alt hover:border-surface-border/80"
          }`}
        >
          {/* Header row */}
          <div className="flex items-center gap-2 mb-1">
            {/* Category badge */}
            <span
              className="text-[10px] font-semibold uppercase px-1.5 py-0.5 rounded shrink-0"
              style={{
                color: dotColor,
                backgroundColor: `${dotColor}15`,
              }}
            >
              {category}
            </span>

            {/* Specialist */}
            {event.specialist && (
              <span
                className="text-xs font-bold shrink-0"
                style={{ color: dotColor }}
              >
                {group?.label || event.specialist}
              </span>
            )}

            {/* Event type */}
            <span className="text-[10px] text-ink-muted font-mono shrink-0">
              {event.type.replace(/_/g, " ")}
            </span>

            {/* Relative time */}
            <span className="text-[10px] text-ink-muted ml-auto shrink-0 opacity-0 group-hover:opacity-100 transition-opacity">
              {formatRelative(event.timestamp)}
            </span>
          </div>

          {/* Action text */}
          <p className="text-sm text-ink-soft leading-relaxed">
            {event.action}
          </p>

          {/* Expanded details */}
          {isSelected && event.data && Object.keys(event.data).length > 0 && (
            <div className="mt-2 pt-2 border-t border-surface-border">
              <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
                {Object.entries(event.data).map(([key, val]) => {
                  // Skip long or complex values
                  if (val === null || val === undefined) return null;
                  const display =
                    typeof val === "string"
                      ? val.length > 80
                        ? val.slice(0, 80) + "…"
                        : val
                      : typeof val === "object"
                        ? JSON.stringify(val).slice(0, 60)
                        : String(val);
                  return (
                    <div key={key} className="flex items-start gap-2">
                      <span className="text-ink-muted font-mono shrink-0">
                        {key}:
                      </span>
                      <span className="text-ink-soft truncate">{display}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
