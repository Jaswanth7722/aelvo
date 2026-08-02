import { useMemo, useState } from "react";
import type { UIEvent, TaskBoardItem, TaskStatus, KanbanColumn } from "../types";

interface TaskBoardProps {
  events: UIEvent[];
}

const COLUMNS: KanbanColumn[] = [
  { key: "pending",    label: "Pending",    icon: "○",  color: "#52627f" },
  { key: "assigned",   label: "Assigned",   icon: "→",  color: "#a565ff" },
  { key: "active",     label: "Active",     icon: "◉",  color: "#3b82f6" },
  { key: "review",     label: "Review",     icon: "◐",  color: "#f7b731" },
  { key: "blocked",    label: "Blocked",    icon: "⊘",  color: "#ff5c7a" },
  { key: "completed",  label: "Complete",   icon: "✓",  color: "#00e38c" },
];

/** Map incoming to_status values to kanban TaskStatus */
function mapToStatus(raw: string): TaskStatus {
  const s = raw.toLowerCase().trim();
  if (s === "pending" || s === "created")           return "pending";
  if (s === "assigned")                              return "assigned";
  if (s === "in_progress" || s === "running" || s === "active") return "active";
  if (s === "review" || s === "reviewing")           return "review";
  if (s === "blocked" || s === "waiting")            return "blocked";
  if (s === "completed" || s === "done")             return "completed";
  // Terminal failures shown alongside completed tasks
  if (s === "failed" || s === "error")               return "completed";
  // Treat unknown as pending
  return "pending";
}

export function TaskBoard({ events }: TaskBoardProps) {
  const [expandedTask, setExpandedTask] = useState<string | null>(null);

  const byColumn = useMemo(() => {
    // Collect all task_board_transition events
    const transitions = events.filter((e) => e.type === "task_board_transition");

    // Track latest status per task_id
    const taskMap = new Map<string, TaskBoardItem>();

    for (const ev of transitions) {
      const d = ev.data || {};
      const taskId = String(d.task_id || "");
      if (!taskId) continue;

      const rawStatus = String(d.to_status || "pending");
      const status = mapToStatus(rawStatus);

      const existing = taskMap.get(taskId);
      taskMap.set(taskId, {
        id: taskId,
        taskId,
        taskType: String(d.task_type || ""),
        status,
        specialist: String(d.specialist || existing?.specialist || ""),
        reason: String(d.reason || ev.action || ""),
        sessionId: String(d.session_id || ""),
        timestamp: existing?.timestamp || ev.timestamp,
        lastUpdated: ev.timestamp,
      });
    }
    const tasks = Array.from(taskMap.values()).sort(
      (a, b) => b.lastUpdated - a.lastUpdated
    );

    // Group by column
    const byColumn = new Map<TaskStatus, TaskBoardItem[]>();
    for (const col of COLUMNS) {
      byColumn.set(col.key, []);
    }
    for (const task of tasks) {
      const col = byColumn.get(task.status);
      if (col) col.push(task);
      else byColumn.get("pending")!.push(task);
    }

    return byColumn;
  }, [events]);

  const allCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const [key, items] of byColumn) {
      counts[key] = items.length;
    }
    return counts;
  }, [byColumn]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Column headers */}
      <div className="flex gap-3 px-6 pt-4 pb-2 shrink-0 overflow-x-auto">
        {COLUMNS.map((col) => (
          <div
            key={col.key}
            className="flex items-center gap-2 min-w-[220px]"
          >
            <span style={{ color: col.color }}>{col.icon}</span>
            <span className="text-xs font-semibold uppercase tracking-wider text-ink-muted">
              {col.label}
            </span>
            <span
              className="text-[10px] font-bold px-1.5 py-0.5 rounded-full"
              style={{
                color: col.color,
                backgroundColor: `${col.color}18`,
              }}
            >
              {allCounts[col.key] || 0}
            </span>
          </div>
        ))}
      </div>

      {/* Board */}
      <div className="flex-1 flex gap-3 px-6 pb-6 overflow-x-auto overflow-y-hidden">
        {COLUMNS.map((col) => (
          <div
            key={col.key}
            className="min-w-[220px] w-[220px] shrink-0 flex flex-col overflow-hidden"
          >
            {/* Column body */}
            <div className="flex-1 overflow-y-auto space-y-2 pr-1">
              {(byColumn.get(col.key) || []).length === 0 && (
                <div className="text-center text-ink-muted text-xs py-8">
                  No tasks
                </div>
              )}
              {(byColumn.get(col.key) || []).map((task) => (
                <TaskCard
                  key={task.taskId}
                  task={task}
                  expanded={expandedTask === task.taskId}
                  onToggle={() =>
                    setExpandedTask(
                      expandedTask === task.taskId ? null : task.taskId
                    )
                  }
                />
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ── Single Task Card ──────────────────────────────────────────── */

interface TaskCardProps {
  task: TaskBoardItem;
  expanded: boolean;
  onToggle: () => void;
}

function TaskCard({ task, expanded, onToggle }: TaskCardProps) {
  const col = COLUMNS.find((c) => c.key === task.status) || COLUMNS[0];

  return (
    <div
      className="border border-surface-border rounded-lg bg-surface-alt hover:bg-surface-border/20 transition-colors duration-150 cursor-pointer select-none"
      onClick={onToggle}
    >
      {/* Header */}
      <div className="px-3 py-2">
        <div className="flex items-center justify-between gap-2 mb-1">
          <span className="text-xs font-mono text-ink-muted truncate">
            {task.taskId.length > 10
              ? task.taskId.slice(0, 10) + "…"
              : task.taskId}
          </span>
          {task.taskType && (
            <span
              className="text-[10px] font-medium px-1.5 py-0.5 rounded shrink-0"
              style={{
                color: col.color,
                backgroundColor: `${col.color}15`,
              }}
            >
              {task.taskType}
            </span>
          )}
        </div>

        {task.reason && (
          <p className="text-xs text-ink-soft leading-tight line-clamp-2">
            {task.reason}
          </p>
        )}
      </div>

      {/* Expanded details */}
      {expanded && (
        <div className="px-3 pb-3 pt-1 border-t border-surface-border space-y-1.5">
          {task.specialist && (
            <div className="flex items-center gap-2 text-[11px]">
              <span className="text-ink-muted">Agent:</span>
              <span className="text-ink-soft font-medium">{task.specialist}</span>
            </div>
          )}
          <div className="flex items-center gap-2 text-[11px]">
            <span className="text-ink-muted">Updated:</span>
            <span className="text-ink-soft">{formatTime(task.lastUpdated)}</span>
          </div>
          {task.sessionId && (
            <div className="flex items-center gap-2 text-[11px]">
              <span className="text-ink-muted">Session:</span>
              <span className="text-ink-soft font-mono">
                {task.sessionId.slice(0, 8)}
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function formatTime(ts: number): string {
  const d = new Date(ts * 1000);
  const now = Date.now();
  const diff = now - d.getTime();

  if (diff < 60000) return "just now";
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;

  return `${d.getHours().toString().padStart(2, "0")}:${d
    .getMinutes()
    .toString()
    .padStart(2, "0")}`;
}
