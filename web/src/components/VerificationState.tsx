import { useMemo } from "react";
import type { UIEvent, VerificationState } from "../types";

interface VerificationStateCardProps {
  events: UIEvent[];
}

export function VerificationStateCard({ events }: VerificationStateCardProps) {
  const state = useMemo<VerificationState>(() => {
    const verifEvents = events.filter(
      (e) =>
        e.type === "verification_started" ||
        e.type === "verification_completed" ||
        e.type === "verification_failed" ||
        e.type === "verification_passed" ||
        e.type === "verification_running"
    );

    // Also check blackboard_publication for verification_status data
    const bbEvents = events.filter(
      (e) => e.type === "blackboard_publication" && e.data?.verification_status
    );

    let passed = 0;
    let failed = 0;
    let running = 0;

    for (const e of verifEvents) {
      if (e.type === "verification_passed" || e.type === "verification_completed") passed++;
      else if (e.type === "verification_failed") failed++;
      else if (e.type === "verification_running" || e.type === "verification_started") running++;
    }

    // Also count blackboard verification_status
    for (const e of bbEvents) {
      const vStatus = String(e.data?.verification_status || "");
      if (vStatus === "verified") passed++;
      else if (vStatus === "failed") failed++;
    }

    const total = passed + failed + running;
    const passRate = total > 0 ? Math.round((passed / total) * 100) : 100;

    return { totalChecks: total, passed, failed, running, passRate };
  }, [events]);

  return (
    <div className="panel">
      <h3 className="text-xs text-gray-500 uppercase tracking-wider mb-4">Verification State</h3>

      {/* Main rate display */}
      <div className="flex items-baseline gap-1 mb-4">
        <span
          className={`text-3xl font-bold ${
            state.passRate >= 90
              ? "text-accent-green"
              : state.passRate >= 70
                ? "text-accent-amber"
                : "text-accent-red"
          }`}
        >
          {state.passRate}
        </span>
        <span className="text-sm text-gray-500">% pass rate</span>
      </div>

      {/* Stacked bar */}
      <div className="w-full h-2 bg-surface-border rounded-full overflow-hidden flex mb-4">
        {state.passed > 0 && (
          <div
            className="h-full bg-accent-green transition-all duration-500"
            style={{ width: `${(state.passed / Math.max(state.totalChecks, 1)) * 100}%` }}
          />
        )}
        {state.failed > 0 && (
          <div
            className="h-full bg-accent-red transition-all duration-500"
            style={{ width: `${(state.failed / Math.max(state.totalChecks, 1)) * 100}%` }}
          />
        )}
        {state.running > 0 && (
          <div
            className="h-full bg-accent-blue animate-pulse transition-all duration-500"
            style={{ width: `${(state.running / Math.max(state.totalChecks, 1)) * 100}%` }}
          />
        )}
      </div>

      {/* Counts grid */}
      <div className="grid grid-cols-4 gap-2 text-center">
        <div>
          <div className="text-lg font-bold text-gray-200">{state.totalChecks}</div>
          <div className="text-[10px] text-gray-500">Total</div>
        </div>
        <div>
          <div className="text-lg font-bold text-accent-green">{state.passed}</div>
          <div className="text-[10px] text-gray-500">Passed</div>
        </div>
        <div>
          <div className="text-lg font-bold text-accent-red">{state.failed}</div>
          <div className="text-[10px] text-gray-500">Failed</div>
        </div>
        <div>
          <div className="text-lg font-bold text-accent-amber">{state.running}</div>
          <div className="text-[10px] text-gray-500">Running</div>
        </div>
      </div>
    </div>
  );
}
