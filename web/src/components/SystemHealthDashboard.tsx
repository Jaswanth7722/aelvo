import { useMemo, useState } from "react";
import type { UIEvent, HealthStatus, AgentLiveness, RecoveryEvent, NodeState, EventTypeBreakdown } from "../types";

interface SystemHealthDashboardProps {
  events: UIEvent[];
}

const SPECIALIST_AGENTS = [
  { name: "ARCHITECT", label: "Architect", color: "#3b82f6", icon: "◉" },
  { name: "ORACLE", label: "Oracle", color: "#8c5cff", icon: "◆" },
  { name: "FORGE", label: "Forge", color: "#00d889", icon: "△" },
  { name: "SENTINEL", label: "Sentinel", color: "#ff5c7a", icon: "✓" },
  { name: "TERMINUS", label: "Terminus", color: "#f7b731", icon: "▶" },
  { name: "HERALD", label: "Herald", color: "#39c8ff", icon: "★" },
  { name: "CONSENSUS", label: "Consensus", color: "#19f5a5", icon: "↻" },
];

function fmtRelative(ts: number): string {
  const diff = Date.now() / 1000 - ts;
  if (diff < 10) return "just now";
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function fmtTimestamp(ts: number): string {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function SystemHealthDashboard({ events }: SystemHealthDashboardProps) {
  const [activeTab, setActiveTab] = useState<"liveness" | "recovery" | "nodes" | "breakdown">("liveness");

  const {
    overallStatus,
    uptime,
    eventThroughput,
    agents,
    recoveryEvents,
    recoveryStats,
    nodeStates,
    breakdown,
    errorCount,
    errorRate,
  } = useMemo(() => {
    // ── Uptime: from system_online events ──
    const onlineEvents = events.filter((e) => e.type === "system_online");
    const firstOnline = onlineEvents[0];
    const uptime = firstOnline ? Date.now() / 1000 - firstOnline.timestamp : 0;

    // ── Event throughput: events in last 60s ──
    const now = Date.now() / 1000;
    const recentEvents = events.filter((e) => now - e.timestamp < 60);
    const eventThroughput = recentEvents.length;

    // ── Agent liveness ──
    const agents: AgentLiveness[] = SPECIALIST_AGENTS.map((spec) => {
      const specEvents = events.filter((e) => e.specialist?.toUpperCase() === spec.name);
      const last = specEvents[specEvents.length - 1];
      const lastSeen = last ? last.timestamp : 0;
      const isRecent = now - lastSeen < 300; // within 5 minutes
      return {
        name: spec.name,
        label: spec.label,
        color: spec.color,
        icon: spec.icon,
        lastSeen,
        status: lastSeen === 0 ? "unknown" : isRecent ? "active" : "idle",
        eventCount: specEvents.length,
        recentActions: specEvents.slice(-3).map((e) => e.action),
      };
    });

    // ── Recovery events ──
    const rawRecovery = events.filter(
      (e) => e.type.startsWith("recovery_")
    );
    const recoveryEvents: RecoveryEvent[] = rawRecovery.map((e) => {
      let outcome: "initiated" | "completed" | "failed";
      if (e.type === "recovery_completed") outcome = "completed";
      else if (e.type === "recovery_failed") outcome = "failed";
      else outcome = "initiated";
      return {
        id: String(e.data?.recovery_id || e.data?.id || ""),
        type: e.type,
        specialist: e.specialist,
        action: e.action,
        outcome,
        timestamp: e.timestamp,
      };
    }).sort((a, b) => b.timestamp - a.timestamp);

    const recoveryStats = {
      total: recoveryEvents.length,
      succeeded: recoveryEvents.filter((r) => r.outcome === "completed").length,
      failed: recoveryEvents.filter((r) => r.outcome === "failed").length,
      initiated: recoveryEvents.filter((r) => r.outcome === "initiated").length,
      successRate: recoveryEvents.filter((r) => r.outcome !== "initiated").length > 0
        ? Math.round(
            (recoveryEvents.filter((r) => r.outcome === "completed").length /
              recoveryEvents.filter((r) => r.outcome !== "initiated").length) * 100
          )
        : 100,
    };

    // ── Node transitions ──
    const rawNodes = events.filter((e) => e.type === "node_transition");
    const nodeStates: NodeState[] = rawNodes.map((e) => ({
      nodeId: String(e.data?.node_id || e.specialist || "unknown"),
      state: String(e.data?.new_state || e.data?.state || "unknown"),
      previousState: String(e.data?.previous_state || e.data?.old_state || ""),
      timestamp: e.timestamp,
    })).sort((a, b) => b.timestamp - a.timestamp);

    // ── Event type breakdown ──
    const typeCounts = new Map<string, number>();
    for (const e of events) {
      typeCounts.set(e.type, (typeCounts.get(e.type) || 0) + 1);
    }

    const iconMap: Record<string, string> = {
      blackboard_publication: "◆", finding_consumed: "▷", challenge_raised: "⚠",
      consensus_formed: "↻", architect_decision: "◉", execution_started: "▶",
      execution_completed: "✓", report_generated: "★", recovery_initiated: "🔄",
      recovery_completed: "✅", recovery_failed: "❌", node_transition: "◈",
      graph_completed: "✓", graph_started: "▶", task_created: "○", task_assigned: "→",
      task_completed: "✓", task_failed: "✗", system_online: "●",
      verification_started: "◐", verification_passed: "✓", verification_failed: "✗",
      verification_running: "◌", task_board_transition: "⊘",
    };
    const colorMap: Record<string, string> = {
      blackboard_publication: "#8c5cff", finding_consumed: "#00d889", challenge_raised: "#ff5c7a",
      consensus_formed: "#19f5a5", architect_decision: "#3b82f6", execution_started: "#f7b731",
      execution_completed: "#00e38c", report_generated: "#39c8ff", recovery_initiated: "#3b82f6",
      recovery_completed: "#00e38c", recovery_failed: "#ff5c7a", node_transition: "#a565ff",
      graph_completed: "#00e38c", graph_started: "#f7b731", task_created: "#52627f",
      task_assigned: "#a565ff", task_completed: "#00e38c", task_failed: "#ff5c7a",
      system_online: "#00e38c", verification_started: "#f7b731", verification_passed: "#00e38c",
      verification_failed: "#ff5c7a", verification_running: "#52627f", task_board_transition: "#52627f",
    };

    const breakdown: EventTypeBreakdown[] = [...typeCounts.entries()]
      .map(([type, count]) => ({
        type,
        count,
        icon: iconMap[type] || "•",
        color: colorMap[type] || "#52627f",
      }))
      .sort((a, b) => b.count - a.count);

    // ── Error rate ──
    const errorTypes = ["recovery_failed", "task_failed", "verification_failed"];
    const errorCount = events.filter((e) =>
      errorTypes.includes(e.type) ||
      (e.type === "execution_completed" && e.data?.exit_code !== undefined && Number(e.data?.exit_code) !== 0)
    ).length;
    const errorRate = events.length > 0 ? Math.round((errorCount / events.length) * 100) : 0;

    // ── Overall status ──
    const hasActiveAgents = agents.some((a) => a.status === "active");
    const hasFailedRecovery = recoveryEvents.some((r) => r.outcome === "failed");
    const hasRecentErrors = events.filter((e) =>
      (errorTypes.includes(e.type) ||
        (e.type === "execution_completed" && e.data?.exit_code !== undefined && Number(e.data?.exit_code) !== 0)) &&
      now - e.timestamp < 300
    ).length > 0;

    const overallStatus: HealthStatus =
      !hasActiveAgents && events.length > 0 ? "unhealthy"
      : hasFailedRecovery || hasRecentErrors ? "degraded"
      : events.length === 0 ? "unknown"
      : "healthy";

    return {
      overallStatus,
      uptime,
      eventThroughput,
      agents,
      recoveryEvents,
      recoveryStats,
      nodeStates,
      breakdown,
      errorCount,
      errorRate,
    };
  }, [events]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-bold text-ink">System Health</h2>
          <span className={`inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded-full border ${
            overallStatus === "healthy"
              ? "text-accent-green border-accent-green/40 bg-accent-green/8"
              : overallStatus === "degraded"
                ? "text-accent-amber border-accent-amber/40 bg-accent-amber/8"
                : overallStatus === "unhealthy"
                  ? "text-accent-red border-accent-red/40 bg-accent-red/8"
                  : "text-ink-muted border-surface-border/40 bg-gray-300/20"
          }`}>
            <span className={`w-1.5 h-1.5 rounded-full ${
              overallStatus === "healthy" ? "bg-accent-green"
              : overallStatus === "degraded" ? "bg-accent-amber"
              : overallStatus === "unhealthy" ? "bg-accent-red animate-pulse"
              : "bg-gray-500"
            }`} />
            {overallStatus.toUpperCase()}
          </span>
        </div>
        <div className="text-xs text-ink-muted">
          {events.length} events · {eventThroughput}/min throughput
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-7xl mx-auto space-y-6">
          {/* Row 1: Summary cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <SummaryCard
              label="Event Throughput"
              value={eventThroughput}
              unit="/min"
              color="#3b82f6"
              icon="◈"
            />
            <SummaryCard
              label="Active Agents"
              value={agents.filter((a) => a.status === "active").length}
              sub={`${agents.filter((a) => a.status === "idle").length} idle · ${agents.filter((a) => a.status === "unknown").length} unknown`}
              color="#00e38c"
              icon="●"
            />
            <SummaryCard
              label="Recovery Rate"
              value={recoveryStats.total > 0 ? recoveryStats.successRate : 100}
              isPercent
              color={recoveryStats.successRate >= 90 ? "#00e38c" : recoveryStats.successRate >= 70 ? "#f7b731" : "#ff5c7a"}
              icon="🔄"
            />
            <SummaryCard
              label="Error Rate"
              value={errorRate}
              isPercent
              color={errorRate < 5 ? "#00e38c" : errorRate < 15 ? "#f7b731" : "#ff5c7a"}
              icon="⚠"
            />
          </div>

          {/* Row 2: Uptime + system info */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <div className="panel lg:col-span-2">
              <div className="text-xs text-ink-muted uppercase tracking-wider mb-3">System Uptime</div>
              <div className="flex items-end gap-3">
                <div className="text-3xl font-bold text-ink font-mono">
                  {formatUptime(uptime)}
                </div>
              </div>
              <div className="mt-2 w-full h-1.5 bg-surface-border rounded-full overflow-hidden">
                <div
                  className="h-full rounded-full bg-gradient-to-r from-accent-blue via-accent-green to-accent-cyan transition-all duration-1000"
                  style={{ width: `${Math.min((uptime / 86400) * 100, 100)}%` }}
                />
              </div>
              <div className="flex justify-between text-[10px] text-ink-muted mt-1">
                <span>startup</span>
                <span>24h</span>
              </div>
            </div>

            <div className="panel">
              <div className="text-xs text-ink-muted uppercase tracking-wider mb-3">Event Summary</div>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-ink-soft">Total events</span>
                  <span className="text-ink font-semibold">{events.length}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-ink-soft">Unique types</span>
                  <span className="text-ink font-semibold">{breakdown.length}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-ink-soft">Errors</span>
                  <span className="text-accent-red font-semibold">{errorCount}</span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-ink-soft">Recoveries</span>
                  <span className="text-ink font-semibold">{recoveryStats.total}</span>
                </div>
              </div>
            </div>
          </div>

          {/* Row 3: Tabbed detail view */}
          <div className="panel">
            <div className="flex gap-4 border-b border-surface-border pb-3 mb-3">
              {[
                { key: "liveness" as const, label: "Agent Liveness", count: agents.length, color: "#00e38c" },
                { key: "recovery" as const, label: "Recovery History", count: recoveryEvents.length, color: "#3b82f6" },
                { key: "nodes" as const, label: "Node Transitions", count: nodeStates.length, color: "#a565ff" },
                { key: "breakdown" as const, label: "Event Breakdown", count: breakdown.length, color: "#52627f" },
              ].map((tab) => (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`text-xs font-medium pb-3 -mb-3 border-b-2 transition-colors ${
                    activeTab === tab.key ? "text-ink" : "text-ink-muted hover:text-ink-soft"
                  }`}
                  style={{ borderColor: activeTab === tab.key ? tab.color : "transparent" }}
                >
                  {tab.label}
                  <span className="ml-1.5 text-ink-muted">({tab.count})</span>
                </button>
              ))}
            </div>

            <div className="max-h-[400px] overflow-y-auto space-y-2">
              {/* ── Agent Liveness Tab ── */}
              {activeTab === "liveness" && (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-3">
                  {agents.map((agent) => (
                    <div
                      key={agent.name}
                      className="border border-surface-border rounded-lg p-3 hover:bg-surface-alt/50 transition-colors"
                    >
                      <div className="flex items-center gap-2 mb-2">
                        <div
                          className={`w-2 h-2 rounded-full ${
                            agent.status === "active" ? "bg-accent-green shadow-[0_0_6px_rgba(0,227,140,0.5)]"
                            : agent.status === "idle" ? "bg-accent-amber"
                            : "bg-gray-600"
                          }`}
                        />
                        <span className="text-xs font-semibold" style={{ color: agent.color }}>
                          {agent.icon} {agent.name}
                        </span>
                        <span className={`text-[10px] ml-auto ${
                          agent.status === "active" ? "text-accent-green"
                          : agent.status === "idle" ? "text-accent-amber"
                          : "text-ink-muted"
                        }`}>
                          {agent.status.toUpperCase()}
                        </span>
                      </div>
                      <div className="text-xs text-ink-muted space-y-0.5">
                        <div>Events: <span className="text-ink-soft">{agent.eventCount}</span></div>
                        <div>
                          Last seen:{" "}
                          <span className={agent.lastSeen > 0 ? "text-ink-soft" : "text-ink-muted"}>
                            {agent.lastSeen > 0 ? fmtRelative(agent.lastSeen) : "never"}
                          </span>
                        </div>
                      </div>
                      {agent.recentActions.length > 0 && (
                        <div className="mt-2 space-y-0.5">
                          {agent.recentActions.map((action, i) => (
                            <div key={i} className="text-[10px] text-ink-muted truncate">
                              {action}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}

              {/* ── Recovery History Tab ── */}
              {activeTab === "recovery" && (
                <>
                  {recoveryEvents.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No recovery events recorded</div>
                  )}
                  {recoveryEvents.length > 0 && (
                    <div className="space-y-1.5">
                      {/* Mini stats */}
                      <div className="flex gap-4 mb-3 px-1">
                        <span className="text-xs text-ink-muted">
                          Total: <span className="text-ink-soft font-semibold">{recoveryStats.total}</span>
                        </span>
                        <span className="text-xs text-accent-green">
                          ✓ {recoveryStats.succeeded}
                        </span>
                        <span className="text-xs text-accent-red">
                          ✗ {recoveryStats.failed}
                        </span>
                        <span className="text-xs text-ink-muted">
                          ↻ {recoveryStats.initiated}
                        </span>
                        <span className="text-xs text-ink-muted">
                          Rate: <span className={recoveryStats.successRate >= 90 ? "text-accent-green" : "text-accent-amber"}>
                            {recoveryStats.successRate}%
                          </span>
                        </span>
                      </div>
                      {recoveryEvents.slice(0, 50).map((r, i) => (
                        <div
                          key={r.id || i}
                          className="border border-surface-border rounded-lg px-3 py-2 hover:bg-surface-alt/50 transition-colors flex items-center gap-3"
                        >
                          <span className={`text-sm ${
                            r.outcome === "completed" ? "text-accent-green"
                            : r.outcome === "failed" ? "text-accent-red"
                            : "text-accent-blue"
                          }`}>
                            {r.outcome === "completed" ? "✅" : r.outcome === "failed" ? "❌" : "🔄"}
                          </span>
                          <div className="flex-1 min-w-0">
                            <div className="text-xs text-ink-soft truncate">{r.action}</div>
                            {r.specialist && (
                              <div className="text-[10px] text-ink-muted">{r.specialist}</div>
                            )}
                          </div>
                          <span className="text-[10px] text-ink-muted shrink-0">
                            {fmtRelative(r.timestamp)}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}

              {/* ── Node Transitions Tab ── */}
              {activeTab === "nodes" && (
                <>
                  {nodeStates.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No node transitions recorded</div>
                  )}
                  {nodeStates.length > 0 && (
                    <div className="space-y-1.5">
                      {nodeStates.slice(0, 50).map((n, i) => (
                        <div
                          key={`${n.nodeId}-${n.timestamp}` || i}
                          className="border border-surface-border rounded-lg px-3 py-2 hover:bg-surface-alt/50 transition-colors flex items-center gap-3"
                        >
                          <span className="text-sm text-accent-purple">◈</span>
                          <div className="flex-1 min-w-0">
                            <div className="text-xs text-ink-soft font-mono truncate">{n.nodeId}</div>
                            <div className="flex items-center gap-1.5 mt-0.5">
                              {n.previousState && (
                                <>
                                  <span className="text-[10px] text-ink-muted">{n.previousState}</span>
                                  <span className="text-[10px] text-ink-muted">→</span>
                                </>
                              )}
                              <span className="text-[10px] text-accent-blue font-medium">{n.state}</span>
                            </div>
                          </div>
                          <span className="text-[10px] text-ink-muted shrink-0">{fmtRelative(n.timestamp)}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}

              {/* ── Event Breakdown Tab ── */}
              {activeTab === "breakdown" && (
                <>
                  {breakdown.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No events recorded</div>
                  )}
                  {breakdown.length > 0 && (
                    <div className="space-y-1">
                      {breakdown.map((b) => {
                        const pct = Math.round((b.count / events.length) * 100);
                        return (
                          <div key={b.type} className="flex items-center gap-3 px-1 py-1.5">
                            <span className="text-xs" style={{ color: b.color }}>{b.icon}</span>
                            <span className="text-xs text-ink-soft w-40 truncate" title={b.type}>{b.type}</span>
                            <div className="flex-1 h-3 bg-surface-border rounded-full overflow-hidden">
                              <div
                                className="h-full rounded-full transition-all duration-500"
                                style={{ width: `${pct}%`, backgroundColor: b.color }}
                              />
                            </div>
                            <span className="text-xs text-ink-muted w-12 text-right font-mono">{b.count}</span>
                            <span className="text-[10px] text-ink-muted w-10 text-right">{pct}%</span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Sub-components ────────────────────────────────────────────── */

function SummaryCard({ label, value, unit, sub, isPercent, color, icon }: {
  label: string; value: number; unit?: string; sub?: string; isPercent?: boolean; color: string; icon: string;
}) {
  return (
    <div className="panel">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs" style={{ color }}>{icon}</span>
        <div className="text-[10px] text-ink-muted uppercase tracking-wider">{label}</div>
      </div>
      <div className="text-2xl font-bold" style={{ color }}>
        {value}{unit || ""}{isPercent ? "%" : ""}
      </div>
      {sub && <div className="text-[10px] text-ink-muted mt-0.5 truncate">{sub}</div>}
    </div>
  );
}

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h < 24) return `${h}h ${m}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h ${m}m`;
}
