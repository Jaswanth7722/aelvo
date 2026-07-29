import { useWebSocket } from "../hooks/useWebSocket";
import { SystemOverview } from "../components/SystemOverview";
import { TrustScore } from "../components/TrustScore";
import { ConsensusStateCard } from "../components/ConsensusState";
import { RecoveryStateCard } from "../components/RecoveryState";
import { VerificationStateCard } from "../components/VerificationState";
import { TaskSummaryCard } from "../components/TaskSummary";
import { EventFeed } from "../components/EventFeed";

export default function DashboardPage() {
  const { status, events, clearEvents } = useWebSocket();

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* ── Top bar ───────────────────────────────────────────────── */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-bold text-gray-200">System Dashboard</h2>
          <span className="text-xs text-gray-600">{events.length} events captured</span>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={clearEvents}
            className="text-xs text-gray-500 hover:text-gray-300 transition-colors px-3 py-1.5 rounded border border-surface-border hover:border-gray-600"
          >
            Clear Events
          </button>
          <span
            className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-full border ${
              status === "connected"
                ? "text-accent-green border-accent-green/30 bg-accent-green/5"
                : status === "connecting"
                  ? "text-accent-amber border-accent-amber/30 bg-accent-amber/5"
                  : "text-accent-red border-accent-red/30 bg-accent-red/5"
            }`}
          >
            <span
              className={`w-1.5 h-1.5 rounded-full ${
                status === "connected"
                  ? "bg-accent-green"
                  : status === "connecting"
                    ? "bg-accent-amber animate-pulse"
                    : "bg-accent-red"
              }`}
            />
            {status}
          </span>
        </div>
      </header>

      {/* ── Scrollable Content ─────────────────────────────────────── */}
      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-7xl mx-auto space-y-6">
          {/* Row 1: System Overview (full width) */}
          <SystemOverview events={events} />

          {/* Row 2: 3-column grid — Trust, Consensus, Task Summary */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <TrustScore events={events} />
            <ConsensusStateCard events={events} />
            <TaskSummaryCard events={events} />
          </div>

          {/* Row 3: 2-column grid — Verification, Recovery */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <VerificationStateCard events={events} />
            <RecoveryStateCard events={events} />
          </div>

          {/* Row 4: Live collaboration feed */}
          <div className="panel">
            <div className="text-xs text-gray-500 uppercase tracking-wider mb-3 flex items-center justify-between">
              <span>Live Collaboration Feed</span>
              <span className="text-gray-600 text-[10px]">
                {events.filter(
                  (e) =>
                    e.type !== "overview_updated" &&
                    e.type !== "work_queue_updated" &&
                    e.type !== "consensus_updated" &&
                    e.type !== "recovery_updated" &&
                    e.type !== "agent_metrics_updated"
                ).length}{" "}
                visible
              </span>
            </div>
            <div className="h-64 overflow-y-auto">
              <EventFeed
                events={events}
                filter={(e) =>
                  e.type !== "overview_updated" &&
                  e.type !== "work_queue_updated" &&
                  e.type !== "consensus_updated" &&
                  e.type !== "recovery_updated" &&
                  e.type !== "agent_metrics_updated"
                }
                maxVisible={100}
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
