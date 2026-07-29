import { useMemo } from "react";
import type { UIEvent } from "../types";

interface SystemOverviewProps {
  events: UIEvent[];
}

export function SystemOverview({ events }: SystemOverviewProps) {
  const overview = useMemo(() => {
    // Extract latest overview data from OVERVIEW_UPDATED events
    const overviewEvents = events.filter((e) => e.type === "overview_updated");
    const latest = overviewEvents[overviewEvents.length - 1];

    if (!latest?.data) {
      return {
        provider: "—",
        model: "—",
        agents: 0,
        uptime: 0,
        goal: "—",
        task: "—",
        progress: 0,
      };
    }

    const ov = latest.data.overview as Record<string, unknown> | undefined;
    return {
      provider: (ov?.provider as string) || "—",
      model: (ov?.model as string) || "—",
      agents: (ov?.agents_active as number) || 0,
      uptime: (ov?.uptime_seconds as number) || 0,
      goal: (ov?.current_goal as string) || (latest.data.current_goal as string) || "—",
      task: (ov?.current_task as string) || (latest.data.current_task as string) || "—",
      progress: (ov?.progress as number) || 0,
    };
  }, [events]);

  const progressPct = Math.round(overview.progress * 100);

  return (
    <div className="panel">
      <div className="grid grid-cols-2 gap-4 text-sm">
        {/* Provider + Model */}
        <div>
          <div className="text-gray-500 text-xs uppercase tracking-wider mb-1">Provider</div>
          <div className="text-gray-200 font-semibold">{overview.provider}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs uppercase tracking-wider mb-1">Model</div>
          <div className="text-gray-200 font-semibold">{overview.model}</div>
        </div>

        {/* Active Agents */}
        <div>
          <div className="text-gray-500 text-xs uppercase tracking-wider mb-1">Active Agents</div>
          <div className="text-accent-blue font-semibold">{overview.agents}</div>
        </div>

        {/* Uptime */}
        <div>
          <div className="text-gray-500 text-xs uppercase tracking-wider mb-1">Uptime</div>
          <div className="text-gray-200 font-semibold">{formatDuration(overview.uptime)}</div>
        </div>
      </div>

      {/* Goal */}
      <div className="mt-4">
        <div className="text-gray-500 text-xs uppercase tracking-wider mb-1">Goal</div>
        <div className="text-gray-200 text-sm truncate">{overview.goal}</div>
      </div>

      {/* Task */}
      <div className="mt-3">
        <div className="text-gray-500 text-xs uppercase tracking-wider mb-1">Task</div>
        <div className="text-accent-cyan text-sm truncate">{overview.task}</div>
      </div>

      {/* Progress bar */}
      <div className="mt-3">
        <div className="flex items-center justify-between text-xs mb-1">
          <span className="text-gray-500">Progress</span>
          <span className="text-gray-400">{progressPct}%</span>
        </div>
        <div className="w-full h-1.5 bg-surface-border rounded-full overflow-hidden">
          <div
            className="h-full rounded-full bg-gradient-to-r from-accent-blue to-accent-green transition-all duration-500 ease-out"
            style={{ width: `${Math.min(progressPct, 100)}%` }}
          />
        </div>
      </div>
    </div>
  );
}

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}
