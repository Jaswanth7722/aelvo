import { useMemo, useState } from "react";
import type { UIEvent, MonitorAlert, MonitorRule, MetricSnapshot, EventRateBucket, SubsystemHealthSummary } from "../types";

interface MonitoringDashboardProps {
  events: UIEvent[];
}

const DEFAULT_RULES: MonitorRule[] = [
  { id: "rule_recovery_failure", name: "High recovery failure rate", description: "Alert when recovery failures exceed normal rate", subsystem: "recovery", severity: "error", metricName: "recovery.failure", thresholdMin: null, thresholdMax: 3, enabled: true, cooldownSeconds: 300 },
  { id: "rule_verification_failure", name: "Verification failures", description: "Alert on consecutive verification failures", subsystem: "recovery", severity: "warning", metricName: "verification.failed", thresholdMin: null, thresholdMax: 2, enabled: true, cooldownSeconds: 300 },
  { id: "rule_task_failure", name: "Task failure rate", description: "Alert when tasks fail above threshold", subsystem: "scaling", severity: "warning", metricName: "task.failed", thresholdMin: null, thresholdMax: 5, enabled: true, cooldownSeconds: 600 },
  { id: "rule_system_health", name: "System health degradation", description: "Alert when overall health drops to degraded", subsystem: "system", severity: "critical", metricName: "health.overall", thresholdMin: null, thresholdMax: 0, enabled: true, cooldownSeconds: 120 },
  { id: "rule_execution_error", name: "Execution errors", description: "Alert on execution failures with non-zero exit code", subsystem: "scaling", severity: "error", metricName: "execution.exit_code", thresholdMin: null, thresholdMax: 0, enabled: true, cooldownSeconds: 300 },
];

function fmtRelative(ts: number): string {
  const diff = Date.now() / 1000 - ts;
  if (diff < 10) return "just now";
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

export function MonitoringDashboard({ events }: MonitoringDashboardProps) {
  const [activeTab, setActiveTab] = useState<"rates" | "alerts" | "metrics" | "health">("rates");

  const { rateBuckets, alerts, metricSeries, subsystemHealth, stats } = useMemo(() => {
    const now = Date.now() / 1000;

    // ── Event rate buckets (5 buckets of events/time) ──
    const buckets: EventRateBucket[] = [];
    const bucketSize = 60; // 60-second buckets
    for (let i = 4; i >= 0; i--) {
      const start = now - (i + 1) * bucketSize;
      const end = now - i * bucketSize;
      const count = events.filter((e) => e.timestamp >= start && e.timestamp < end).length;
      const label = i === 4 ? "-5m" : i === 3 ? "-4m" : i === 2 ? "-3m" : i === 1 ? "-2m" : "-1m";
      buckets.push({ label, count, periodStart: start });
    }

    // ── Alerts derived from error/failure events ──
    // Collect alerts: one per failure event, plus execution_completed with non-zero exit_code
    const rawAlerts: MonitorAlert[] = [];
    for (const e of events) {
      if (e.type === "recovery_failed") {
        rawAlerts.push({ id: String(e.data?.recovery_id || e.data?.id || ""), severity: "error", title: "Recovery operation failed", message: e.action, subsystem: "recovery", source: e.specialist, timestamp: e.timestamp, acknowledged: false });
      } else if (e.type === "verification_failed") {
        rawAlerts.push({ id: String(e.data?.check_id || e.data?.id || ""), severity: "warning", title: "Verification check failed", message: e.action, subsystem: "recovery", source: e.specialist, timestamp: e.timestamp, acknowledged: false });
      } else if (e.type === "task_failed") {
        rawAlerts.push({ id: String(e.data?.task_id || e.data?.id || ""), severity: "error", title: "Task execution failed", message: e.action, subsystem: "scaling", source: e.specialist, timestamp: e.timestamp, acknowledged: false });
      } else if (e.type === "execution_completed" && e.data?.exit_code !== undefined && Number(e.data?.exit_code) !== 0) {
        rawAlerts.push({ id: String(e.data?.task_id || e.data?.id || ""), severity: "warning", title: "Execution completed with errors", message: `exit code ${e.data?.exit_code}`, subsystem: "scaling", source: e.specialist, timestamp: e.timestamp, acknowledged: false });
      }
    }
    const alerts = rawAlerts.sort((a, b) => b.timestamp - a.timestamp).slice(0, 100);

    // ── Metric series (aggregated event counts) ──
    const metricSeries: MetricSnapshot[] = [
      { name: "recovery.attempt", count: events.filter((e) => e.type.startsWith("recovery_")).length, avg: null, min: null, max: null, latest: null },
      { name: "verification.total", count: events.filter((e) => e.type.startsWith("verification_")).length, avg: null, min: null, max: null, latest: null },
      { name: "architect.decisions", count: events.filter((e) => e.type === "architect_decision").length, avg: null, min: null, max: null, latest: null },
      { name: "consensus.sessions", count: events.filter((e) => e.type === "consensus_formed").length, avg: null, min: null, max: null, latest: null },
      { name: "blackboard.publications", count: events.filter((e) => e.type === "blackboard_publication").length, avg: null, min: null, max: null, latest: null },
      { name: "execution.completed", count: events.filter((e) => e.type === "execution_completed").length, avg: null, min: null, max: null, latest: null },
      { name: "challenges.raised", count: events.filter((e) => e.type === "challenge_raised").length, avg: null, min: null, max: null, latest: null },
      { name: "reports.generated", count: events.filter((e) => e.type === "report_generated").length, avg: null, min: null, max: null, latest: null },
    ];

    // Compute per-minute rates for metric series
    const elapsedMin = events.length > 0 ? Math.max((now - events[0].timestamp) / 60, 1) : 1;
    for (const m of metricSeries) {
      m.avg = Math.round((m.count / elapsedMin) * 10) / 10;
      m.latest = m.count;
    }

    // ── Subsystem health summaries ──
    const subsystemHealth: SubsystemHealthSummary[] = [
      {
        name: "recovery",
        status: events.some((e) => e.type === "recovery_failed" && now - e.timestamp < 300) ? "degraded"
          : events.some((e) => e.type.startsWith("recovery_")) ? "healthy"
          : "unknown",
        checksPassing: events.filter((e) => e.type === "recovery_completed").length,
        checksFailing: events.filter((e) => e.type === "recovery_failed").length,
        totalChecks: events.filter((e) => e.type.startsWith("recovery_")).length,
        activeAlerts: alerts.filter((a) => a.subsystem === "recovery" && !a.acknowledged).length,
        description: "Consensus, specialist, and task-level failure recovery",
      },
      {
        name: "governance",
        status: events.some((e) => e.type === "verification_failed" && now - e.timestamp < 300) ? "degraded"
          : events.some((e) => e.type === "architect_decision") ? "healthy"
          : "unknown",
        checksPassing: events.filter((e) => e.type === "verification_passed").length,
        checksFailing: events.filter((e) => e.type === "verification_failed").length,
        totalChecks: events.filter((e) => e.type.startsWith("verification_")).length,
        activeAlerts: alerts.filter((a) => a.subsystem === "governance" && !a.acknowledged).length,
        description: "Policy enforcement and approval management",
      },
      {
        name: "scaling",
        status: events.some((e) => e.type === "task_failed" && now - e.timestamp < 300) ? "degraded"
          : events.some((e) => e.type === "execution_started") ? "healthy"
          : "unknown",
        checksPassing: events.filter((e) => e.type === "execution_completed" && Number(e.data?.exit_code) === 0).length,
        checksFailing: events.filter((e) => e.type === "task_failed" || (e.type === "execution_completed" && Number(e.data?.exit_code) !== 0)).length,
        totalChecks: events.filter((e) => e.type.startsWith("execution_") || e.type.startsWith("task_")).length,
        activeAlerts: alerts.filter((a) => a.subsystem === "scaling" && !a.acknowledged).length,
        description: "Resource pooling, async pipelines, and batch processing",
      },
    ];

    // ── Stats ──
    const totalAlerts = alerts.length;
    const unacknowledged = alerts.filter((a) => !a.acknowledged).length;
    const criticalCount = alerts.filter((a) => a.severity === "critical").length;
    const errorCount = alerts.filter((a) => a.severity === "error").length;
    const warningCount = alerts.filter((a) => a.severity === "warning").length;

    return {
      rateBuckets: buckets,
      alerts,
      metricSeries,
      subsystemHealth,
      stats: {
        totalAlerts,
        unacknowledged,
        criticalCount,
        errorCount,
        warningCount,
        eventRate1m: buckets[buckets.length - 1]?.count || 0,
        eventRate5m: buckets.reduce((s, b) => s + b.count, 0),
        healthySubsystems: subsystemHealth.filter((s) => s.status === "healthy").length,
        totalSubsystems: subsystemHealth.length,
      },
    };
  }, [events]);

  const maxBucketCount = Math.max(...rateBuckets.map((b) => b.count), 1);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-bold text-gray-200">Monitoring Dashboard</h2>
          <span className="text-xs text-gray-500">{events.length} total events</span>
        </div>
        <div className="flex items-center gap-3 text-xs">
          <span className="text-accent-cyan">{stats.eventRate1m}/min current</span>
          <span className="text-gray-500">
            {stats.healthySubsystems}/{stats.totalSubsystems} subsystems healthy
          </span>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-7xl mx-auto space-y-6">
          {/* Row 1: Summary cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <SummaryCard
              label="Event Rate"
              value={stats.eventRate1m}
              unit="/min"
              sub={`${stats.eventRate5m} in last 5 min`}
              color="#3b82f6"
              icon="◈"
            />
            <SummaryCard
              label="Active Alerts"
              value={stats.unacknowledged}
              sub={`${stats.criticalCount} critical · ${stats.errorCount} error · ${stats.warningCount} warning`}
              color={stats.criticalCount > 0 ? "#ff5c7a" : stats.errorCount > 0 ? "#f7b731" : "#00e38c"}
              icon="⚠"
            />
            <SummaryCard
              label="Subsystem Health"
              value={stats.healthySubsystems}
              unit={`/${stats.totalSubsystems}`}
              sub="subsystems active"
              color={stats.healthySubsystems === stats.totalSubsystems ? "#00e38c" : stats.healthySubsystems > 0 ? "#f7b731" : "#ff5c7a"}
              icon="◉"
            />
            <SummaryCard
              label="Metric Series"
              value={metricSeries.length}
              sub={`${metricSeries.filter((m) => m.count > 0).length} active`}
              color="#8c5cff"
              icon="◆"
            />
          </div>

          {/* Row 2: Event rate bar chart */}
          <div className="panel">
            <div className="text-xs text-gray-500 uppercase tracking-wider mb-3">Event Rate (events/min)</div>
            <div className="flex items-end gap-3 h-32 px-1">
              {rateBuckets.map((bucket) => {
                const pct = (bucket.count / maxBucketCount) * 100;
                return (
                  <div key={bucket.label} className="flex-1 flex flex-col items-center gap-1 h-full justify-end">
                    <span className="text-[10px] text-gray-500 font-mono">{bucket.count}</span>
                    <div
                      className="w-full rounded-t transition-all duration-500"
                      style={{
                        height: `${Math.max(pct, 2)}%`,
                        backgroundColor: bucket.count === 0 ? "#21262d" : bucket.count >= maxBucketCount * 0.8 ? "#ff5c7a" : bucket.count >= maxBucketCount * 0.5 ? "#f7b731" : "#3b82f6",
                      }}
                    />
                    <span className="text-[10px] text-gray-600">{bucket.label}</span>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Row 3: Tabbed detail view */}
          <div className="panel">
            <div className="flex gap-4 border-b border-surface-border pb-3 mb-3 overflow-x-auto">
              {[
                { key: "rates" as const, label: "Event Rates", count: rateBuckets.length, color: "#3b82f6" },
                { key: "alerts" as const, label: "Alert Feed", count: stats.totalAlerts, color: "#ff5c7a" },
                { key: "metrics" as const, label: "Metric Series", count: metricSeries.length, color: "#8c5cff" },
                { key: "health" as const, label: "Subsystem Health", count: subsystemHealth.length, color: "#00e38c" },
              ].map((tab) => (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`text-xs font-medium pb-3 -mb-3 border-b-2 transition-colors whitespace-nowrap ${
                    activeTab === tab.key ? "text-gray-200" : "text-gray-600 hover:text-gray-400"
                  }`}
                  style={{ borderColor: activeTab === tab.key ? tab.color : "transparent" }}
                >
                  {tab.label}
                  <span className="ml-1.5 text-gray-600">({tab.count})</span>
                </button>
              ))}
            </div>

            <div className="max-h-[420px] overflow-y-auto space-y-2">
              {/* ── Event Rates Tab (additional detail) ── */}
              {activeTab === "rates" && (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                  {metricSeries.filter((m) => m.count > 0).map((m) => (
                    <div key={m.name} className="border border-surface-border rounded-lg p-3 hover:bg-surface-alt/50 transition-colors">
                      <div className="text-xs text-gray-300 font-semibold mb-1">{m.name}</div>
                      <div className="text-lg font-bold text-gray-200">{m.count}</div>
                      <div className="text-[10px] text-gray-500">{m.avg} /min average</div>
                    </div>
                  ))}
                  {metricSeries.filter((m) => m.count > 0).length === 0 && (
                    <div className="text-center text-gray-600 text-sm py-8 col-span-full">No active metric series</div>
                  )}
                </div>
              )}

              {/* ── Alert Feed Tab ── */}
              {activeTab === "alerts" && (
                <>
                  {/* Configured Rules Section */}
                  <details className="mb-3 group">
                    <summary className="text-xs text-gray-500 uppercase tracking-wider cursor-pointer hover:text-gray-400 select-none">
                      Alert Rules ({DEFAULT_RULES.length})
                    </summary>
                    <div className="mt-2 space-y-1.5">
                      {DEFAULT_RULES.map((rule) => (
                        <RuleRow key={rule.id} rule={rule} />
                      ))}
                    </div>
                  </details>

                  <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Alert History</div>
                  {alerts.length === 0 && (
                    <div className="text-center text-gray-600 text-sm py-4">No alerts recorded</div>
                  )}
                  <div className="space-y-1">
                    <div className="flex gap-4 mb-3 px-1">
                      <span className="text-xs text-gray-500">Total: <span className="text-gray-300 font-semibold">{stats.totalAlerts}</span></span>
                      <span className="text-xs text-accent-red">CRITICAL {stats.criticalCount}</span>
                      <span className="text-xs text-accent-red/70">ERROR {stats.errorCount}</span>
                      <span className="text-xs text-accent-amber">WARNING {stats.warningCount}</span>
                    </div>
                    {alerts.slice(0, 50).map((a, i) => (
                      <AlertRow key={`${a.id}-${i}`} alert={a} />
                    ))}
                  </div>
                </>
              )}

              {/* ── Metric Series Tab ── */}
              {activeTab === "metrics" && (
                <div className="space-y-1">
                  {metricSeries.map((m) => {
                    const maxCount = Math.max(...metricSeries.map((x) => x.count), 1);
                    const pct = (m.count / maxCount) * 100;
                    return (
                      <div key={m.name} className="flex items-center gap-3 px-1 py-1.5">
                        <span className="text-xs text-gray-400 w-40 truncate" title={m.name}>{m.name}</span>
                        <div className="flex-1 h-3 bg-surface-border rounded-full overflow-hidden">
                          <div className="h-full rounded-full bg-accent-purple transition-all duration-500" style={{ width: `${pct}%` }} />
                        </div>
                        <span className="text-xs text-gray-500 w-12 text-right font-mono">{m.count}</span>
                        <span className="text-[10px] text-gray-600 w-14 text-right">{m.avg}/min</span>
                      </div>
                    );
                  })}
                  <div className="text-[10px] text-gray-600 italic pt-2 px-1">
                    Per-minute averages computed over {Math.max(Math.round((events.length > 0 ? (Date.now() / 1000 - events[0].timestamp) / 60 : 1)), 1)} min window
                  </div>
                </div>
              )}

              {/* ── Subsystem Health Tab ── */}
              {activeTab === "health" && (
                <div className="space-y-3">
                  {subsystemHealth.map((sh) => (
                    <SubsystemRow key={sh.name} summary={sh} />
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Sub-components ────────────────────────────────────────────── */

function SummaryCard({ label, value, unit, sub, color, icon }: {
  label: string; value: number; unit?: string; sub?: string; color: string; icon: string;
}) {
  return (
    <div className="panel">
      <div className="flex items-center gap-1.5 mb-1">
        <span className="text-xs" style={{ color }}>{icon}</span>
        <div className="text-[10px] text-gray-500 uppercase tracking-wider">{label}</div>
      </div>
      <div className="text-xl font-bold" style={{ color }}>
        {value}{unit || ""}
      </div>
      {sub && <div className="text-[10px] text-gray-600 mt-0.5 truncate">{sub}</div>}
    </div>
  );
}

function RuleRow({ rule }: { rule: MonitorRule }) {
  const sevColor = rule.severity === "critical" ? "#ff5c7a"
    : rule.severity === "error" ? "#f7b731"
    : rule.severity === "warning" ? "#3b82f6"
    : "#52627f";

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-1">
        <span className={`w-1.5 h-1.5 rounded-full ${rule.enabled ? "bg-accent-green" : "bg-gray-600"}`} />
        <span className="text-xs font-semibold text-gray-300">{rule.name}</span>
        <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded" style={{ color: sevColor, backgroundColor: `${sevColor}15` }}>
          {rule.severity.toUpperCase()}
        </span>
        <span className="text-[10px] text-gray-600">{rule.subsystem}</span>
        {!rule.enabled && <span className="text-[10px] text-gray-600">DISABLED</span>}
      </div>
      <p className="text-[11px] text-gray-500 mb-1">{rule.description}</p>
      <div className="flex items-center gap-3 text-[10px] text-gray-600">
        <span>Metric: <span className="text-gray-500">{rule.metricName}</span></span>
        {rule.thresholdMax !== null && <span>Max: <span className="text-gray-500">{rule.thresholdMax}</span></span>}
        {rule.thresholdMin !== null && <span>Min: <span className="text-gray-500">{rule.thresholdMin}</span></span>}
        <span>Cooldown: <span className="text-gray-500">{rule.cooldownSeconds}s</span></span>
      </div>
    </div>
  );
}

function AlertRow({ alert }: { alert: MonitorAlert }) {
  const sevColor = alert.severity === "critical" ? "#ff5c7a"
    : alert.severity === "error" ? "#f7b731"
    : alert.severity === "warning" ? "#3b82f6"
    : "#52627f";

  const sevIcon = alert.severity === "critical" ? "🔴"
    : alert.severity === "error" ? "🟠"
    : alert.severity === "warning" ? "🟡"
    : "🔵";

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2 hover:bg-surface-alt/50 transition-colors flex items-center gap-3">
      <span className="text-sm">{sevIcon}</span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs font-semibold" style={{ color: sevColor }}>{alert.severity.toUpperCase()}</span>
          <span className="text-xs text-gray-300 truncate">{alert.title}</span>
        </div>
        <div className="flex items-center gap-2 mt-0.5">
          <span className="text-[10px] text-gray-600">{alert.subsystem}</span>
          {alert.source && <span className="text-[10px] text-gray-600">· {alert.source}</span>}
          {alert.message && <span className="text-[10px] text-gray-600">· {alert.message.slice(0, 60)}</span>}
        </div>
      </div>
      <span className="text-[10px] text-gray-600 shrink-0">{fmtRelative(alert.timestamp)}</span>
    </div>
  );
}

function SubsystemRow({ summary }: { summary: SubsystemHealthSummary }) {
  const statusColor = summary.status === "healthy" ? "#00e38c"
    : summary.status === "degraded" ? "#f7b731"
    : summary.status === "unhealthy" ? "#ff5c7a"
    : "#52627f";

  const statusIcon = summary.status === "healthy" ? "✓"
    : summary.status === "degraded" ? "⚠"
    : summary.status === "unhealthy" ? "✗"
    : "?";

  const totalChecks = summary.totalChecks || 1;
  const passPct = Math.round((summary.checksPassing / totalChecks) * 100);

  return (
    <div className="border border-surface-border rounded-lg px-4 py-3 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-2">
        <span className="text-sm" style={{ color: statusColor }}>{statusIcon}</span>
        <span className="text-xs font-semibold text-gray-300 uppercase">{summary.name}</span>
        <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded" style={{ color: statusColor, backgroundColor: `${statusColor}15` }}>
          {summary.status.toUpperCase()}
        </span>
        {summary.activeAlerts > 0 && (
          <span className="text-[10px] text-accent-red ml-auto">{summary.activeAlerts} active alert{summary.activeAlerts > 1 ? "s" : ""}</span>
        )}
      </div>
      <p className="text-[11px] text-gray-500 mb-2">{summary.description}</p>
      <div className="flex items-center gap-3 text-[10px] text-gray-600">
        <span>Checks: <span className="text-accent-green">{summary.checksPassing}</span> / <span className={summary.checksFailing > 0 ? "text-accent-red" : "text-gray-500"}>{summary.totalChecks}</span></span>
        <span>Pass rate: <span className={passPct >= 90 ? "text-accent-green" : passPct >= 70 ? "text-accent-amber" : "text-accent-red"}>{passPct}%</span></span>
      </div>
      {totalChecks > 0 && (
        <div className="mt-2 w-full h-1.5 bg-surface-border rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{ width: `${passPct}%`, backgroundColor: statusColor }}
          />
        </div>
      )}
    </div>
  );
}
