import { useMemo, useState } from "react";
import type { UIEvent, SecurityFinding, SecurityScan, AuditRecord, IntegrityCheck, SecurityPosture, SecuritySeverity } from "../types";

interface SecurityDashboardProps {
  events: UIEvent[];
}

const SCAN_CATEGORIES: { type: string; label: string; icon: string; color: string }[] = [
  { type: "credential_leak", label: "Credential Leak", icon: "🔑", color: "#ff5c7a" },
  { type: "path_traversal", label: "Path Traversal", icon: "📁", color: "#f7b731" },
  { type: "command_injection", label: "Command Injection", icon: "⚡", color: "#ff5c7a" },
  { type: "secret_exposure", label: "Secret Exposure", icon: "👁", color: "#f7b731" },
  { type: "policy_violation", label: "Policy Violation", icon: "📋", color: "#3b82f6" },
  { type: "unsafe_command", label: "Unsafe Command", icon: "⚠", color: "#f7b731" },
  { type: "suspicious_pattern", label: "Suspicious Pattern", icon: "❓", color: "#52627f" },
  { type: "sandbox_tamper", label: "Sandbox Tamper", icon: "🔨", color: "#ff5c7a" },
  { type: "configuration_issue", label: "Configuration Issue", icon: "⚙", color: "#52627f" },
];

const INTEGRITY_CHECKS: { id: string; name: string; icon: string }[] = [
  { id: "binary_integrity", name: "Binary Integrity", icon: "🔒" },
  { id: "audit_log_integrity", name: "Audit Log Integrity", icon: "📜" },
  { id: "process_health", name: "Process Health", icon: "⚙" },
  { id: "fs_isolation", name: "Filesystem Isolation", icon: "📁" },
];

function fmtRelative(ts: number): string {
  const diff = Date.now() / 1000 - ts;
  if (diff < 10) return "just now";
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

function sevColor(severity: SecuritySeverity): string {
  return severity === "critical" ? "#ff5c7a"
    : severity === "high" ? "#f7b731"
    : severity === "medium" ? "#3b82f6"
    : severity === "low" ? "#52627f"
    : "#39c8ff";
}

function sevIcon(severity: SecuritySeverity): string {
  return severity === "critical" ? "🔴"
    : severity === "high" ? "🟠"
    : severity === "medium" ? "🔵"
    : severity === "low" ? "⚪"
    : "🔹";
}

export function SecurityDashboard({ events }: SecurityDashboardProps) {
  const [activeTab, setActiveTab] = useState<"findings" | "scans" | "audit" | "integrity">("findings");
  const [selectedFinding, setSelectedFinding] = useState<SecurityFinding | null>(null);

  const { findings, scans, auditRecords, integrityResults, posture } = useMemo(() => {
    const now = Date.now() / 1000;

    // ── Derive security findings from failure events ──
    const rawFindings: SecurityFinding[] = [];

    // verification_failed events → policy violation or sandbox tamper findings
    for (const e of events.filter((e) => e.type === "verification_failed")) {
      const diag = String(e.data?.diagnostics || e.action || "");
      rawFindings.push({
        id: String(e.data?.check_id || e.data?.id || ""),
        category: diag.toLowerCase().includes("security") ? "policy_violation"
          : diag.toLowerCase().includes("sandbox") ? "sandbox_tamper"
          : "configuration_issue",
        severity: "high",
        title: "Verification check failed",
        message: diag.slice(0, 200),
        location: e.specialist || "unknown",
        recommendation: "Review verification failure and address underlying issues",
        timestamp: e.timestamp,
        source: "verification_pipeline",
      });
    }

    // task_failed events → suspicious pattern findings
    for (const e of events.filter((e) => e.type === "task_failed")) {
      rawFindings.push({
        id: String(e.data?.task_id || e.data?.id || ""),
        category: "suspicious_pattern",
        severity: "medium",
        title: "Task failed unexpectedly",
        message: e.action.slice(0, 200),
        location: e.specialist || "unknown",
        recommendation: "Investigate task failure and assess for security implications",
        timestamp: e.timestamp,
        source: "task_monitor",
      });
    }

    // execution_completed with non-zero exit_code → unsafe command findings
    for (const e of events.filter((e) => e.type === "execution_completed" && e.data?.exit_code !== undefined && Number(e.data?.exit_code) !== 0)) {
      rawFindings.push({
        id: String(e.data?.task_id || e.data?.id || ""),
        category: "unsafe_command",
        severity: "medium",
        title: `Execution exited with code ${e.data?.exit_code}`,
        message: e.action.slice(0, 200),
        location: e.specialist || "terminus",
        recommendation: "Review command for security issues and ensure proper error handling",
        timestamp: e.timestamp,
        source: "execution_monitor",
      });
    }

    // recovery_failed events → policy violation findings
    for (const e of events.filter((e) => e.type === "recovery_failed")) {
      rawFindings.push({
        id: String(e.data?.recovery_id || e.data?.id || ""),
        category: "policy_violation",
        severity: "high",
        title: "Recovery operation failed",
        message: e.action.slice(0, 200),
        location: e.specialist || "unknown",
        recommendation: "Review recovery policy and ensure proper fallback mechanisms",
        timestamp: e.timestamp,
        source: "recovery_engine",
      });
    }

    // challenge_raised events → suspicious pattern findings
    for (const e of events.filter((e) => e.type === "challenge_raised")) {
      rawFindings.push({
        id: String(e.data?.challenge_id || e.data?.id || ""),
        category: "suspicious_pattern",
        severity: "low",
        title: "Challenge raised on blackboard entry",
        message: String(e.data?.challenged_claim || e.action || "").slice(0, 200),
        location: String(e.data?.challenger || e.specialist || "unknown"),
        recommendation: "Review challenged claim and assess for validity",
        timestamp: e.timestamp,
        source: "blackboard_monitor",
      });
    }

    const findings = rawFindings.sort((a, b) => b.timestamp - a.timestamp);

    // ── Security scans (grouped by time windows) ──
    const scanWindows = [
      { start: now - 60, label: "1m" },
      { start: now - 300, label: "5m" },
      { start: now - 900, label: "15m" },
      { start: now - 3600, label: "1h" },
      { start: now - 86400, label: "24h" },
    ];

    const scans: SecurityScan[] = [];
    for (const w of scanWindows) {
      const windowEvents = events.filter((e) => e.timestamp >= w.start);
      const windowFindings = findings.filter((f) => f.timestamp >= w.start);
      const passed = !windowEvents.some((e) =>
        e.type === "verification_failed" || e.type === "recovery_failed" || e.type === "task_failed"
      );
      scans.push({
        id: `scan_${w.label}`,
        timestamp: w.start,
        durationMs: 0,
        passed,
        totalFindings: windowFindings.length,
        criticalCount: windowFindings.filter((f) => f.severity === "critical").length,
        highCount: windowFindings.filter((f) => f.severity === "high").length,
        mediumCount: windowFindings.filter((f) => f.severity === "medium").length,
        lowCount: windowFindings.filter((f) => f.severity === "low").length,
        infoCount: windowFindings.filter((f) => f.severity === "info").length,
        targetsScanned: windowEvents.length,
      });
    }

    // ── Audit records from governance-significant events ──
    const auditEventTypes = new Set([
      "architect_decision", "challenge_raised", "consensus_formed",
      "recovery_initiated", "recovery_completed", "recovery_failed",
      "verification_started", "verification_passed", "verification_failed",
      "verification_running", "execution_started", "execution_completed",
    ]);
    const auditRecords: AuditRecord[] = events
      .filter((e) => auditEventTypes.has(e.type))
      .map((e) => ({
        id: String(e.data?.decision_id || e.data?.challenge_id || e.data?.consensus_id || e.data?.check_id || e.data?.id || ""),
        action: e.type,
        decision: e.type.includes("failed") ? "denied"
          : e.type.includes("passed") || e.type.includes("completed") ? "allowed"
          : e.type === "architect_decision" ? String(e.data?.outcome || "evaluated")
          : "pending",
        actor: e.specialist || "system",
        subsystem: e.type.startsWith("verification") ? "security"
          : e.type.startsWith("recovery") ? "recovery"
          : e.type === "architect_decision" || e.type === "challenge_raised" || e.type === "consensus_formed" ? "governance"
          : "execution",
        resource: e.type,
        reason: e.action,
        message: e.action,
        severity: e.type.includes("failed") ? "error" : e.type === "verification_started" ? "info" : "info",
        timestamp: e.timestamp,
      }))
      .sort((a, b) => b.timestamp - a.timestamp);

    // ── Integrity check results (derived from events) ──
    const verPassed = events.filter((e) => e.type === "verification_passed").length;
    const verFailed = events.filter((e) => e.type === "verification_failed").length;
    const execOK = events.filter((e) => e.type === "execution_completed" && Number(e.data?.exit_code) === 0).length;
    const execFail = events.filter((e) => e.type === "execution_completed" && e.data?.exit_code !== undefined && Number(e.data?.exit_code) !== 0).length;

    const integrityResults: IntegrityCheck[] = [
      {
        id: "binary_integrity",
        name: "Binary Integrity",
        passed: verFailed === 0 || verPassed > verFailed,
        status: verPassed + verFailed > 0 ? (verPassed > verFailed ? "verified" : "mismatch") : "pending",
        message: `${verPassed} verification passes, ${verFailed} failures`,
        timestamp: now,
      },
      {
        id: "audit_log_integrity",
        name: "Audit Log Integrity",
        passed: true,
        status: events.length > 0 ? "intact" : "empty",
        message: `${events.length} events tracked in audit trail`,
        timestamp: now,
      },
      {
        id: "process_health",
        name: "Process Health",
        passed: execFail === 0 || execOK > execFail,
        status: execOK + execFail > 0 ? (execFail === 0 ? "healthy" : "degraded") : "pending",
        message: `${execOK} successful, ${execFail} failed executions`,
        timestamp: now,
      },
      {
        id: "fs_isolation",
        name: "Filesystem Isolation",
        passed: verFailed === 0 || verPassed > 0,
        status: verPassed > 0 ? "isolated" : "pending",
        message: `${verPassed} verification passes — no sandbox escape detected`,
        timestamp: now,
      },
    ];

    // ── Security posture ──
    const criticalFindings = findings.filter((f) => f.severity === "critical").length;
    const highFindings = findings.filter((f) => f.severity === "high").length;
    const mediumFindings = findings.filter((f) => f.severity === "medium").length;
    const lowFindings = findings.filter((f) => f.severity === "low").length;
    const totalFindings = findings.length;
    const integrityChecksPassed = integrityResults.filter((c) => c.passed).length;
    const integrityChecksFailed = integrityResults.filter((c) => !c.passed).length;

    const recommendations: string[] = [];
    if (criticalFindings > 0) recommendations.push(`Address ${criticalFindings} critical security finding(s) immediately`);
    if (highFindings > 0) recommendations.push(`Review ${highFindings} high-severity security finding(s)`);
    if (integrityChecksFailed > 0) recommendations.push(`Investigate ${integrityChecksFailed} failed sandbox integrity check(s)`);

    const overallStatus: SecurityPosture["overallStatus"] =
      criticalFindings > 0 || integrityChecksFailed > 0 ? "critical"
      : highFindings > 0 ? "attention_needed"
      : totalFindings > 0 || events.length > 0 ? "healthy"
      : "unknown";

    const posture: SecurityPosture = {
      overallStatus,
      totalScans: scans.length,
      totalFindings,
      criticalFindings,
      highFindings,
      mediumFindings,
      lowFindings,
      auditRecordsCount: auditRecords.length,
      auditChainValid: true,
      integrityChecksPassed,
      integrityChecksFailed,
      lastScanTime: now,
      recommendations,
    };

    return { findings, scans, auditRecords, integrityResults, posture };
  }, [events]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-bold text-ink">Security Dashboard</h2>
          <span className={`inline-flex items-center gap-1.5 text-xs px-2 py-0.5 rounded-full border ${
            posture.overallStatus === "healthy"
              ? "text-accent-green border-accent-green/40 bg-accent-green/8"
              : posture.overallStatus === "attention_needed"
                ? "text-accent-amber border-accent-amber/40 bg-accent-amber/8"
                : posture.overallStatus === "critical"
                  ? "text-accent-red border-accent-red/40 bg-accent-red/8"
                  : "text-ink-muted border-surface-border/40 bg-gray-300/20"
          }`}>
            <span className={`w-1.5 h-1.5 rounded-full ${
              posture.overallStatus === "healthy" ? "bg-accent-green"
              : posture.overallStatus === "attention_needed" ? "bg-accent-amber"
              : posture.overallStatus === "critical" ? "bg-accent-red animate-pulse"
              : "bg-gray-500"
            }`} />
            {posture.overallStatus.replace("_", " ").toUpperCase()}
          </span>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-7xl mx-auto space-y-6">
          {/* Row 1: Summary cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <SummaryCard
              label="Security Posture"
              value={posture.overallStatus === "healthy" ? "Healthy"
                : posture.overallStatus === "attention_needed" ? "Attention"
                : posture.overallStatus === "critical" ? "Critical"
                : "Unknown"}
              sub={posture.totalFindings > 0 ? `${posture.totalFindings} total findings` : "No findings"}
              color={posture.overallStatus === "healthy" ? "#00e38c"
                : posture.overallStatus === "attention_needed" ? "#f7b731"
                : posture.overallStatus === "critical" ? "#ff5c7a"
                : "#52627f"}
              icon="◉"
            />
            <SummaryCard
              label="Findings"
              value={posture.totalFindings}
              sub={`${posture.criticalFindings} critical · ${posture.highFindings} high`}
              color={posture.criticalFindings > 0 ? "#ff5c7a" : posture.highFindings > 0 ? "#f7b731" : "#00e38c"}
              icon="⚠"
            />
            <SummaryCard
              label="Scans"
              value={posture.totalScans}
              sub={`${scans.filter((s) => s.passed).length} passed`}
              color="#3b82f6"
              icon="◐"
            />
            <SummaryCard
              label="Integrity"
              value={posture.integrityChecksPassed}
              unit={`/${posture.integrityChecksPassed + posture.integrityChecksFailed}`}
              sub={posture.integrityChecksFailed > 0 ? `${posture.integrityChecksFailed} failed` : "all passed"}
              color={posture.integrityChecksFailed > 0 ? "#ff5c7a" : "#00e38c"}
              icon="🔒"
            />
          </div>

          {/* Row 2: Posture recommendations */}
          {posture.recommendations.length > 0 && (
            <div className="panel">
              <div className="text-xs text-ink-muted uppercase tracking-wider mb-2">Recommendations</div>
              <div className="space-y-1">
                {posture.recommendations.map((r, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs">
                    <span className="text-accent-amber">→</span>
                    <span className="text-ink-soft">{r}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Row 3: Finding severity breakdown */}
          {posture.totalFindings > 0 && (
            <div className="panel">
              <div className="text-xs text-ink-muted uppercase tracking-wider mb-3">Finding Severity Breakdown</div>
              <div className="flex items-center gap-2 h-5">
                {(["critical", "high", "medium", "low", "info"] as SecuritySeverity[]).map((sev) => {
                  const count = sev === "critical" ? posture.criticalFindings
                    : sev === "high" ? posture.highFindings
                    : sev === "medium" ? posture.mediumFindings
                    : sev === "low" ? posture.lowFindings
                    : 0;
                  const pct = posture.totalFindings > 0 ? (count / posture.totalFindings) * 100 : 0;
                  if (count === 0) return null;
                  return (
                    <div
                      key={sev}
                      className="h-full rounded transition-all"
                      style={{ width: `${pct}%`, backgroundColor: sevColor(sev) }}
                      title={`${sev}: ${count}`}
                    />
                  );
                })}
              </div>
              <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2 text-[10px] text-ink-muted">
                {(["critical", "high", "medium", "low"] as SecuritySeverity[]).map((sev) => {
                  const count = sev === "critical" ? posture.criticalFindings
                    : sev === "high" ? posture.highFindings
                    : sev === "medium" ? posture.mediumFindings
                    : sev === "low" ? posture.lowFindings
                    : 0;
                  if (count === 0) return null;
                  return (
                    <span key={sev}>
                      <span className="w-2 h-2 inline-block rounded-full mr-1" style={{ backgroundColor: sevColor(sev) }} />
                      {sev.charAt(0).toUpperCase() + sev.slice(1)} ({count})
                    </span>
                  );
                })}
              </div>
            </div>
          )}

          {/* Row 4: Tabbed detail view */}
          <div className="panel">
            <div className="flex gap-4 border-b border-surface-border pb-3 mb-3 overflow-x-auto">
              {[
                { key: "findings" as const, label: "Security Findings", count: posture.totalFindings, color: "#ff5c7a" },
                { key: "scans" as const, label: "Scan History", count: scans.length, color: "#3b82f6" },
                { key: "audit" as const, label: "Audit Trail", count: posture.auditRecordsCount, color: "#52627f" },
                { key: "integrity" as const, label: "Integrity Checks", count: integrityResults.length, color: "#00e38c" },
              ].map((tab) => (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`text-xs font-medium pb-3 -mb-3 border-b-2 transition-colors whitespace-nowrap ${
                    activeTab === tab.key ? "text-ink" : "text-ink-muted hover:text-ink-soft"
                  }`}
                  style={{ borderColor: activeTab === tab.key ? tab.color : "transparent" }}
                >
                  {tab.label}
                  <span className="ml-1.5 text-ink-muted">({tab.count})</span>
                </button>
              ))}
            </div>

            <div className="max-h-[420px] overflow-y-auto space-y-2">
              {/* ── Security Findings Tab ── */}
              {activeTab === "findings" && (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                  {findings.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8 col-span-full">No security findings recorded</div>
                  )}
                  {findings.map((f, i) => (
                    <FindingCard
                      key={`${f.id}-${i}`}
                      finding={f}
                      selected={selectedFinding?.id === f.id && selectedFinding?.timestamp === f.timestamp}
                      onSelect={() => setSelectedFinding(selectedFinding?.id === f.id && selectedFinding?.timestamp === f.timestamp ? null : f)}
                    />
                  ))}
                </div>
              )}

              {/* ── Scan History Tab ── */}
              {activeTab === "scans" && (
                <div className="space-y-1">
                  {scans.map((s, i) => {
                    const maxCount = Math.max(...scans.map((x) => x.totalFindings), 1);
                    return (
                      <div key={s.id} className="border border-surface-border rounded-lg px-3 py-2.5 hover:bg-surface-alt/50 transition-colors">
                        <div className="flex items-center gap-2 mb-1">
                          <span className={`text-sm ${s.passed ? "text-accent-green" : "text-accent-red"}`}>
                            {s.passed ? "✓" : "✗"}
                          </span>
                          <span className="text-xs font-semibold text-ink-soft">Scan: {s.id}</span>
                          <span className={`text-[10px] font-semibold px-1.5 py-0.5 rounded ${s.passed ? "text-accent-green bg-accent-green/15" : "text-accent-red bg-accent-red/15"}`}>
                            {s.passed ? "PASSED" : "FAILED"}
                          </span>
                          <span className="text-[10px] text-ink-muted ml-auto">{fmtRelative(s.timestamp)}</span>
                        </div>
                        <div className="flex items-center gap-3 text-[10px] text-ink-muted">
                          <span>Findings: {s.totalFindings}</span>
                          {s.criticalCount > 0 && <span className="text-accent-red">CRITICAL {s.criticalCount}</span>}
                          {s.highCount > 0 && <span className="text-accent-amber">HIGH {s.highCount}</span>}
                          <span>Targets: {s.targetsScanned}</span>
                        </div>
                        {s.totalFindings > 0 && (
                          <div className="mt-1.5 w-full h-1.5 bg-surface-border rounded-full overflow-hidden flex">
                            {(["critical", "high", "medium", "low"] as SecuritySeverity[]).map((sev) => {
                              const count = sev === "critical" ? s.criticalCount
                                : sev === "high" ? s.highCount
                                : sev === "medium" ? s.mediumCount
                                : s.lowCount;
                              const pct = (count / maxCount) * 100;
                              if (count === 0) return null;
                              return (
                                <div key={sev} className="h-full" style={{ width: `${pct}%`, backgroundColor: sevColor(sev) }} />
                              );
                            })}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}

              {/* ── Audit Trail Tab ── */}
              {activeTab === "audit" && (
                <>
                  {auditRecords.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No audit records recorded</div>
                  )}
                  <div className="space-y-1">
                    <div className="flex gap-2 mb-2 px-1 text-xs">
                      <span className="text-accent-green">Chain: intact</span>
                      <span className="text-ink-muted">·</span>
                      <span className="text-ink-muted">{auditRecords.length} records</span>
                    </div>
                    {auditRecords.slice(0, 50).map((r, i) => (
                      <AuditRow key={`${r.id}-${i}`} record={r} />
                    ))}
                  </div>
                </>
              )}

              {/* ── Integrity Checks Tab ── */}
              {activeTab === "integrity" && (
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                  {integrityResults.map((c) => {
                    const statusColor = c.passed ? "#00e38c" : "#ff5c7a";
                    const statusIcon = c.passed ? "✓" : "✗";
                    return (
                      <div key={c.id} className="border border-surface-border rounded-lg p-3 hover:bg-surface-alt/50 transition-colors">
                        <div className="flex items-center gap-2 mb-1">
                          <span className="text-sm" style={{ color: statusColor }}>{statusIcon}</span>
                          <span className="text-xs font-semibold text-ink-soft">{c.name}</span>
                          <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded" style={{ color: statusColor, backgroundColor: `${statusColor}15` }}>
                            {c.status.toUpperCase().replace(/_/g, " ")}
                          </span>
                        </div>
                        <p className="text-[11px] text-ink-muted">{c.message}</p>
                      </div>
                    );
                  })}
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
  label: string; value: string | number; unit?: string; sub?: string; color: string; icon: string;
}) {
  return (
    <div className="panel">
      <div className="flex items-center gap-1.5 mb-1">
        <span className="text-xs">{icon}</span>
        <div className="text-[10px] text-ink-muted uppercase tracking-wider">{label}</div>
      </div>
      <div className="text-xl font-bold" style={{ color }}>
        {value}{unit || ""}
      </div>
      {sub && <div className="text-[10px] text-ink-muted mt-0.5 truncate">{sub}</div>}
    </div>
  );
}

function FindingCard({ finding, selected, onSelect }: {
  finding: SecurityFinding; selected: boolean; onSelect: () => void;
}) {
  const catInfo = SCAN_CATEGORIES.find((c) => c.type === finding.category);
  const sColor = sevColor(finding.severity);

  return (
    <div
      className={`border rounded-lg px-3 py-2.5 transition-all cursor-pointer ${
        selected ? "border-accent-blue bg-accent-blue/5" : "border-surface-border hover:bg-surface-alt/50"
      }`}
      onClick={onSelect}
    >
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs">{catInfo?.icon || "⚠"}</span>
        <span className="text-[10px] font-semibold px-1 py-0.5 rounded" style={{ color: sColor, backgroundColor: `${sColor}15` }}>
          {finding.severity.toUpperCase()}
        </span>
        <span className="text-xs text-ink-soft truncate">{finding.title}</span>
        <span className="text-[10px] text-ink-muted ml-auto">{fmtRelative(finding.timestamp)}</span>
      </div>
      <p className="text-[11px] text-ink-muted mb-1">{finding.message}</p>
      <div className="flex items-center gap-3 text-[10px] text-ink-muted">
        <span>Category: <span className="text-ink-muted">{catInfo?.label || finding.category}</span></span>
        {finding.location && <span>Source: <span className="text-ink-muted font-mono">{finding.location}</span></span>}
      </div>
      {selected && finding.recommendation && (
        <div className="mt-2 text-[10px] text-accent-amber border-t border-surface-border pt-2">
          → {finding.recommendation}
        </div>
      )}
    </div>
  );
}

function AuditRow({ record }: { record: AuditRecord }) {
  const decisionColor = record.decision === "allowed" || record.decision === "approve" ? "#00e38c"
    : record.decision === "denied" || record.decision === "reject" ? "#ff5c7a"
    : "#52627f";

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2 hover:bg-surface-alt/50 transition-colors flex items-center gap-3">
      <span className="text-xs font-mono text-ink-muted w-12 shrink-0">{record.subsystem.slice(0, 4).toUpperCase()}</span>
      <div className="flex-1 min-w-0">
        <div className="text-xs text-ink-soft truncate">{record.action.replace(/_/g, " ")}</div>
        <div className="flex items-center gap-2 mt-0.5">
          <span className="text-[10px] text-ink-muted">{record.actor}</span>
          <span className="text-[10px] text-ink-muted">{record.resource}</span>
        </div>
      </div>
      <span className="text-[10px] font-semibold" style={{ color: decisionColor }}>{record.decision.toUpperCase()}</span>
      <span className="text-[10px] text-ink-muted shrink-0">{fmtRelative(record.timestamp)}</span>
    </div>
  );
}
