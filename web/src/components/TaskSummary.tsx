import { useMemo } from "react";
import type { UIEvent, TaskSummary } from "../types";

interface TaskSummaryCardProps {
  events: UIEvent[];
}

export function TaskSummaryCard({ events }: TaskSummaryCardProps) {
  const summary = useMemo<TaskSummary>(() => {
    // Collect graph lifecycle events
    const graphStarted = events.filter((e) => e.type === "graph_started").length;
    const graphCompleted = events.filter((e) => e.type === "graph_completed").length;

    // Count completions with failure data
    let failedGraphs = 0;
    let completedNodes = 0;
    let failedNodes = 0;

    for (const e of events) {
      if (e.type === "graph_completed") {
        const failedCount = Number(e.data?.failed_count || 0);
        if (failedCount > 0) failedGraphs++;
        completedNodes += Number(e.data?.completed_count || 0);
        failedNodes += failedCount;
      }
    }

    // Count task_board_transition events for status tracking
    const taskTransitions = events.filter((e) => e.type === "task_board_transition");
    let active = 0;
    let pending = 0;
    let completed = 0;
    let failed = 0;
    let blocked = 0;

    // Track latest status per task_id
    const taskStatuses = new Map<string, string>();
    for (const t of taskTransitions) {
      const toStatus = String(t.data?.to_status || "");
      const taskId = String(t.data?.task_id || "");
      if (taskId) {
        taskStatuses.set(taskId, toStatus);
      }
    }

    for (const status of taskStatuses.values()) {
      const s = status.toLowerCase();
      if (s === "completed" || s === "done") completed++;
      else if (s === "failed" || s === "error") failed++;
      else if (s === "blocked") blocked++;
      else if (s === "running" || s === "in_progress" || s === "active") active++;
      else if (s === "pending" || s === "assigned") pending++;
    }

    // Use graph events as fallback when task transitions are sparse
    if (taskTransitions.length === 0) {
      const totalGraphs = Math.max(graphStarted, graphCompleted);
      const runningGraphs = Math.max(0, graphStarted - graphCompleted);
      return {
        total: totalGraphs,
        pending: 0,  // indistinguishable from active without task transitions
        active: runningGraphs,
        completed: graphCompleted - failedGraphs,
        failed: failedGraphs,
        blocked: 0,
      };
    }

    const total = taskStatuses.size;
    return { total, pending, active, completed, failed, blocked };
  }, [events]);

  const maxVal = Math.max(summary.total, 1);

  return (
    <div className="panel">
      <h3 className="text-xs text-ink-muted uppercase tracking-wider mb-4">Task Summary</h3>

      {/* Total */}
      <div className="text-center mb-4">
        <div className="text-3xl font-bold text-ink">{summary.total}</div>
        <div className="text-xs text-ink-muted">Total Tasks</div>
      </div>

      {/* Horizontal stacked bar */}
      <div className="w-full h-3 bg-surface-border rounded-full overflow-hidden flex mb-3">
        {summary.completed > 0 && (
          <div
            className="h-full bg-accent-green transition-all duration-500"
            style={{ width: `${(summary.completed / maxVal) * 100}%` }}
            title={`${summary.completed} completed`}
          />
        )}
        {summary.active > 0 && (
          <div
            className="h-full bg-accent-blue transition-all duration-500"
            style={{ width: `${(summary.active / maxVal) * 100}%` }}
            title={`${summary.active} active`}
          />
        )}
        {summary.pending > 0 && (
          <div
            className="h-full bg-gray-500 transition-all duration-500"
            style={{ width: `${(summary.pending / maxVal) * 100}%` }}
            title={`${summary.pending} pending`}
          />
        )}
        {summary.failed > 0 && (
          <div
            className="h-full bg-accent-red transition-all duration-500"
            style={{ width: `${(summary.failed / maxVal) * 100}%` }}
            title={`${summary.failed} failed`}
          />
        )}
        {summary.blocked > 0 && (
          <div
            className="h-full bg-accent-amber transition-all duration-500"
            style={{ width: `${(summary.blocked / maxVal) * 100}%` }}
            title={`${summary.blocked} blocked`}
          />
        )}
      </div>

      {/* Legend */}
      <div className="grid grid-cols-5 gap-1 text-center text-[10px]">
        <div>
          <div className="w-2 h-2 rounded-full bg-accent-green mx-auto mb-0.5" />
          <div className="text-ink-muted">{summary.completed}</div>
          <div className="text-ink-muted">Done</div>
        </div>
        <div>
          <div className="w-2 h-2 rounded-full bg-accent-blue mx-auto mb-0.5" />
          <div className="text-ink-muted">{summary.active}</div>
          <div className="text-ink-muted">Active</div>
        </div>
        <div>
          <div className="w-2 h-2 rounded-full bg-gray-500 mx-auto mb-0.5" />
          <div className="text-ink-muted">{summary.pending}</div>
          <div className="text-ink-muted">Pending</div>
        </div>
        <div>
          <div className="w-2 h-2 rounded-full bg-accent-red mx-auto mb-0.5" />
          <div className="text-ink-muted">{summary.failed}</div>
          <div className="text-ink-muted">Failed</div>
        </div>
        <div>
          <div className="w-2 h-2 rounded-full bg-accent-amber mx-auto mb-0.5" />
          <div className="text-ink-muted">{summary.blocked}</div>
          <div className="text-ink-muted">Blocked</div>
        </div>
      </div>
    </div>
  );
}
