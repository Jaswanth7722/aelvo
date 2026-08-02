import { useMemo } from "react";
import type { UIEvent, RecoveryState } from "../types";
import { PALETTE, STATUS } from "../theme";

interface RecoveryStateCardProps {
  events: UIEvent[];
}

export function RecoveryStateCard({ events }: RecoveryStateCardProps) {
  const state = useMemo<RecoveryState>(() => {
    const recoveryEvents = events.filter(
      (e) =>
        e.type === "recovery_initiated" ||
        e.type === "recovery_completed" ||
        e.type === "recovery_failed"
    );

    if (recoveryEvents.length === 0) {
      return { totalEvents: 0, succeeded: 0, failed: 0, successRate: 0, recentActions: [] };
    }

    let succeeded = 0;
    let failed = 0;
    const recentActions: string[] = [];

    for (const e of recoveryEvents) {
      if (e.type === "recovery_completed") succeeded++;
      else if (e.type === "recovery_failed") failed++;
      // recovery_initiated is neutral — we count success/fail from the terminal events

      if (e.action && recentActions.length < 5) {
        recentActions.push(e.action);
      }
    }

    // Also count graph_completed events with failed_count > 0
    const graphCompletions = events.filter(
      (e) => e.type === "graph_completed" && Number(e.data?.failed_count || 0) > 0
    );
    failed += graphCompletions.length;

    const total = succeeded + failed;
    const successRate = total > 0 ? Math.round((succeeded / total) * 100) : 100;

    return { totalEvents: total, succeeded, failed, successRate, recentActions };
  }, [events]);

  const barColor =
    state.totalEvents === 0
      ? "bg-gray-600"
      : state.successRate >= 90
        ? "bg-accent-green"
        : state.successRate >= 70
          ? "bg-accent-amber"
          : "bg-accent-red";

  return (
    <div className="panel">
      <h3 className="text-xs text-ink-muted uppercase tracking-wider mb-4">Recovery State</h3>

      {/* Rate circle */}
      <div className="flex items-center justify-center mb-4">
        <div className="relative w-16 h-16">
          <svg className="w-16 h-16 -rotate-90" viewBox="0 0 64 64">
            <circle
              cx="32" cy="32" r="28"
              fill="none"
              stroke={PALETTE.neutral}
              strokeWidth="4"
            />
            <circle
              cx="32" cy="32" r="28"
              fill="none"
              stroke={state.totalEvents === 0 ? PALETTE.neutral : state.successRate >= 90 ? STATUS.ok : state.successRate >= 70 ? STATUS.warn : STATUS.err}
              strokeWidth="4"
              strokeDasharray={`${(state.successRate / 100) * 176} 176`}
              strokeLinecap="round"
              className="transition-all duration-700"
            />
          </svg>
          <div className="absolute inset-0 flex items-center justify-center">
            <span              className={`text-sm font-bold ${state.totalEvents === 0 ? "text-ink-muted" : barColor.replace("bg-", "text-")}`}>
              {state.successRate}%
            </span>
          </div>
        </div>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-3 gap-3 text-center mb-3">
        <div>
          <div className="text-lg font-bold text-ink">{state.totalEvents}</div>
          <div className="text-[10px] text-ink-muted">Total</div>
        </div>
        <div>
          <div className="text-lg font-bold text-accent-green">{state.succeeded}</div>
          <div className="text-[10px] text-ink-muted">Success</div>
        </div>
        <div>
          <div className="text-lg font-bold text-accent-red">{state.failed}</div>
          <div className="text-[10px] text-ink-muted">Failed</div>
        </div>
      </div>

      {/* Recent actions */}
      {state.recentActions.length > 0 && (
        <div>
          <div className="text-[10px] text-ink-muted uppercase tracking-wider mb-1">Recent Actions</div>
          <div className="space-y-0.5">
            {state.recentActions.map((action, i) => (
              <div key={i} className="text-xs text-ink-soft truncate">
                {action}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
