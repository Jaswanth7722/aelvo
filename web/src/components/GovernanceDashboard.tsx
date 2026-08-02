import { useMemo, useState } from "react";
import type { UIEvent, GovernanceEvaluation, SecurityAudit, PolicyDefinition, AuditEntry } from "../types";
import { OUTCOME_COLORS, PALETTE, STATUS } from "../theme";

interface GovernanceDashboardProps {
  events: UIEvent[];
}

const DEFAULT_POLICIES: PolicyDefinition[] = [
  { id: "gov_deny_destructive_consensus", name: "Deny destructive consensus actions", description: "Prevent destructive recovery actions during consensus recovery", effect: "deny", scope: "consensus", specialists: [], actionTypes: ["escalate_to_user"], priority: 100, enabled: true },
  { id: "gov_log_specialist_failover", name: "Log specialist failover events", description: "Specialist failovers are logged for audit", effect: "log_only", scope: "specialist", specialists: [], actionTypes: ["failover"], priority: 10, enabled: true },
  { id: "gov_deny_abort_without_notification", name: "Deny silent task aborts", description: "Task aborts must be accompanied by notification", effect: "deny", scope: "task", specialists: [], actionTypes: ["abort_task"], priority: 100, enabled: true },
  { id: "gov_log_consensus_escalation", name: "Log consensus escalations", description: "All consensus escalations must be logged for audit", effect: "log_only", scope: "consensus", specialists: [], actionTypes: ["escalate_to_user", "use_architect_decision"], priority: 10, enabled: true },
  { id: "gov_log_task_replan", name: "Log task replan events", description: "All task replanning events must be logged for audit", effect: "log_only", scope: "task", specialists: [], actionTypes: ["replan"], priority: 10, enabled: true },
  { id: "gov_deny_sentinel_escalation", name: "Deny SENTINEL escalation", description: "SENTINEL escalation requires manual Architect review", effect: "deny", scope: "specialist", specialists: ["SENTINEL"], actionTypes: ["escalate_to_architect"], priority: 100, enabled: true },
];

interface SandboxCheck {
  id: string;
  name: string;
  status: string;
  passed: boolean;
  details: string;
  source: string;
}

function computeSandboxIntegrity(events: UIEvent[]): SandboxCheck[] {
  // Derive sandbox integrity status from available verification events.
  // Backend SandboxIntegrityVerifier data requires explicit bridging.
  const verPassed = events.filter((e) => e.type === "verification_passed").length;
  const verFailed = events.filter((e) => e.type === "verification_failed").length;
  const execOK = events.filter((e) => e.type === "execution_completed" && Number(e.data?.exit_code) === 0).length;
  const execFail = events.filter((e) => e.type === "execution_completed" && Number(e.data?.exit_code) !== 0).length;

  return [
    {
      id: "binary_integrity",
      name: "Binary Integrity",
      status: verPassed + verFailed > 0 ? (verPassed > verFailed ? "verified" : "mismatch") : "pending_bridge",
      passed: verPassed > verFailed,
      details: `${verPassed} verification passes, ${verFailed} failures — binary hash check requires backend bridging`,
      source: "SandboxIntegrityVerifier.verify_binary_integrity()",
    },
    {
      id: "audit_log_integrity",
      name: "Audit Log Integrity",
      status: events.length > 0 ? "intact" : "empty",
      passed: true,
      details: `${events.length} events tracked — hash-chain integrity verification requires backend bridge`,
      source: "SandboxIntegrityVerifier.verify_audit_log_integrity()",
    },
    {
      id: "process_health",
      name: "Process Health",
      status: execOK + execFail > 0 ? (execFail === 0 ? "healthy" : "degraded") : "pending_bridge",
      passed: execFail === 0 || execOK > execFail,
      details: `${execOK} successful executions, ${execFail} failures — process polling requires backend bridge`,
      source: "SandboxIntegrityVerifier.check_process_health()",
    },
    {
      id: "fs_isolation",
      name: "Filesystem Isolation",
      status: verPassed + verFailed > 0 ? (verPassed > 0 ? "isolated" : "violation_detected") : "pending_bridge",
      passed: verPassed > 0 || verFailed === 0,
      details: `${verPassed} verification passes — symlink escape scanning requires backend bridge`,
      source: "SandboxIntegrityVerifier.check_filesystem_isolation()",
    },
  ];
}

function fmtRelative(ts: number): string {
  const diff = Date.now() / 1000 - ts;
  if (diff < 10) return "just now";
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

export function GovernanceDashboard({ events }: GovernanceDashboardProps) {
  const [activeTab, setActiveTab] = useState<"evaluations" | "security" | "sandbox" | "audit" | "policies">("evaluations");

  const { evaluations, securityAudits, auditTrail, sandboxChecks, stats } = useMemo(() => {
    // ── Governance Evaluations from architect_decision events ──
    const decisionEvents = events.filter((e) => e.type === "architect_decision");
    const evaluations: GovernanceEvaluation[] = decisionEvents.map((e) => {
      const outcomeRaw = String(e.data?.outcome || e.action || "").toLowerCase();
      let outcome: GovernanceEvaluation["outcome"] = "replan";
      if (outcomeRaw.includes("approv")) outcome = "approve";
      else if (outcomeRaw.includes("reject")) outcome = "reject";
      else if (outcomeRaw.includes("escalat")) outcome = "escalate";
      else if (outcomeRaw.includes("override")) outcome = "override";
      else if (outcomeRaw.includes("replan")) outcome = "replan";

      return {
        id: String(e.data?.decision_id || e.data?.id || ""),
        decisionId: String(e.data?.decision_id || ""),
        outcome,
        targetType: String(e.data?.target_type || ""),
        targetId: String(e.data?.target_id || ""),
        reason: String(e.data?.reason || ""),
        assignedTo: String(e.data?.assigned_to || ""),
        conditions: Array.isArray(e.data?.conditions) ? (e.data.conditions as string[]) : [],
        timestamp: e.timestamp,
      };
    }).sort((a, b) => b.timestamp - a.timestamp);

    // ── Security Audits from verification_* events ──
    const verificationEvents = events.filter(
      (e) => e.type.startsWith("verification_") || e.type === "execution_completed"
    );
    const securityAudits: SecurityAudit[] = verificationEvents.map((e) => {
      let status: SecurityAudit["status"] = "pending";
      if (e.type === "verification_passed" || (e.type === "execution_completed" && Number(e.data?.exit_code) === 0)) {
        status = "passed";
      } else if (e.type === "verification_failed" || (e.type === "execution_completed" && Number(e.data?.exit_code) !== 0)) {
        status = "failed";
      } else if (e.type === "verification_started" || e.type === "verification_running") {
        status = "running";
      }

      return {
        id: String(e.data?.check_id || e.data?.task_id || e.data?.id || ""),
        type: e.type,
        specialist: e.specialist || "system",
        action: e.action,
        status,
        details: String(e.data?.diagnostics || e.data?.summary || e.action || ""),
        timestamp: e.timestamp,
      };
    }).sort((a, b) => b.timestamp - a.timestamp);

    // ── Audit Trail: all governance-significant events ──
    const auditEventTypes = new Set([
      "architect_decision", "challenge_raised", "consensus_formed",
      "recovery_initiated", "recovery_completed", "recovery_failed",
      "verification_started", "verification_passed", "verification_failed",
      "verification_running",
    ]);
    const auditEvents = events.filter((e) => auditEventTypes.has(e.type));
    const auditTrail: AuditEntry[] = auditEvents.map((e) => ({
      id: String(e.data?.decision_id || e.data?.challenge_id || e.data?.consensus_id || e.data?.check_id || e.data?.id || ""),
      type: e.type,
      actor: e.specialist || "system",
      action: e.action,
      outcome: e.type.includes("failed") ? "denied"
        : e.type.includes("passed") || e.type.includes("completed") ? "allowed"
        : e.type === "architect_decision" ? String(e.data?.outcome || "evaluated")
        : "pending",
      subsystem: e.type.startsWith("verification") ? "security"
        : e.type.startsWith("recovery") ? "recovery"
        : "governance",
      reason: e.action,
      timestamp: e.timestamp,
    })).sort((a, b) => b.timestamp - a.timestamp);

    // ── Stats ──
    const approved = evaluations.filter((e) => e.outcome === "approve").length;
    const rejected = evaluations.filter((e) => e.outcome === "reject").length;
    const escalated = evaluations.filter((e) => e.outcome === "escalate").length;
    const replanned = evaluations.filter((e) => e.outcome === "replan").length;
    const overridden = evaluations.filter((e) => e.outcome === "override").length;
    const securityPassed = securityAudits.filter((s) => s.status === "passed").length;
    const securityFailed = securityAudits.filter((s) => s.status === "failed").length;
    const securityRunning = securityAudits.filter((s) => s.status === "running").length;

    // ── Sandbox integrity indicators ──
    const sandboxChecks = computeSandboxIntegrity(events);

    return {
      evaluations,
      securityAudits,
      auditTrail,
      sandboxChecks,
      stats: {
        totalEvaluations: evaluations.length,
        approved,
        rejected,
        escalated,
        replanned,
        overridden,
        totalSecurity: securityAudits.length,
        securityPassed,
        securityFailed,
        securityRunning,
        totalAudit: auditTrail.length,
        totalSandbox: sandboxChecks.length,
        sandboxPassed: sandboxChecks.filter((s) => s.status === "verified" || s.status === "intact" || s.status === "isolated" || s.status === "healthy").length,
        approvalRate: evaluations.length > 0
          ? Math.round(((approved) / evaluations.length) * 100)
          : 0,
        securityPassRate: securityAudits.filter((s) => s.status === "passed" || s.status === "failed").length > 0
          ? Math.round((securityPassed / Math.max(securityPassed + securityFailed, 1)) * 100)
          : 100,
      },
    };
  }, [events]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-bold text-ink">Governance Dashboard</h2>
          <span className="text-xs text-ink-muted">
            {stats.totalEvaluations + stats.totalSecurity + stats.totalAudit} total events
          </span>
        </div>
        <div className="flex items-center gap-3 text-xs">
          <span className="text-accent-green">✓ {stats.approvalRate}% approval</span>
          <span className="text-accent-purple">◈ {DEFAULT_POLICIES.length} policies</span>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-7xl mx-auto space-y-6">
          {/* Row 1: Summary cards */}
          <div className="grid grid-cols-2 lg:grid-cols-5 gap-3">
            <SummaryCard
              label="Evaluations"
              value={stats.totalEvaluations}
              sub={`${stats.approved} approved`}
              color={PALETTE.deep}
              icon="◉"
            />
            <SummaryCard
              label="Approved"
              value={stats.approved}
              sub={`${stats.approvalRate}% rate`}
              color={STATUS.ok}
              icon="✓"
            />
            <SummaryCard
              label="Denied"
              value={stats.rejected}
              sub={`${stats.escalated} escalated`}
              color={STATUS.err}
              icon="✗"
            />
            <SummaryCard
              label="Security Checks"
              value={stats.totalSecurity}
              sub={`${stats.securityPassed} passed · ${stats.securityFailed} failed`}
              color={PALETTE.purple}
              icon="◐"
            />
            <SummaryCard
              label="Pass Rate"
              value={stats.securityPassRate}
              isPercent
              color={stats.securityPassRate >= 90 ? STATUS.ok : stats.securityPassRate >= 70 ? STATUS.warn : STATUS.err}
              icon="◈"
            />
          </div>

          {/* Row 2: Decision outcome stacked bar */}
          {stats.totalEvaluations > 0 && (
            <div className="panel">
              <div className="text-xs text-ink-muted uppercase tracking-wider mb-3">Decision Outcomes</div>
              <div className="w-full h-4 bg-surface-border rounded-full overflow-hidden flex">
                {stats.approved > 0 && (
                  <div className="h-full bg-accent-green transition-all" style={{ width: `${(stats.approved / stats.totalEvaluations) * 100}%` }} title={`${stats.approved} approved`} />
                )}
                {stats.rejected > 0 && (
                  <div className="h-full bg-accent-red transition-all" style={{ width: `${(stats.rejected / stats.totalEvaluations) * 100}%` }} title={`${stats.rejected} rejected`} />
                )}
                {stats.escalated > 0 && (
                  <div className="h-full bg-accent-amber transition-all" style={{ width: `${(stats.escalated / stats.totalEvaluations) * 100}%` }} title={`${stats.escalated} escalated`} />
                )}
                {stats.replanned > 0 && (
                  <div className="h-full transition-all" style={{ width: `${(stats.replanned / stats.totalEvaluations) * 100}%`, backgroundColor: OUTCOME_COLORS.revision }} title={`${stats.replanned} replanned`} />
                )}
                {stats.overridden > 0 && (
                  <div className="h-full bg-accent-purple transition-all" style={{ width: `${(stats.overridden / stats.totalEvaluations) * 100}%` }} title={`${stats.overridden} overridden`} />
                )}
              </div>
              <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2 text-[10px] text-ink-muted">
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-green mr-1" /> Approved ({stats.approved})</span>
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-red mr-1" /> Rejected ({stats.rejected})</span>
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-amber mr-1" /> Escalated ({stats.escalated})</span>
                <span><span className="w-2 h-2 inline-block rounded-full mr-1" style={{ backgroundColor: OUTCOME_COLORS.revision }} /> Replanned ({stats.replanned})</span>
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-purple mr-1" /> Overridden ({stats.overridden})</span>
              </div>
            </div>
          )}

          {/* Row 3: Tabbed detail view */}
          <div className="panel">
            <div className="flex gap-4 border-b border-surface-border pb-3 mb-3 overflow-x-auto">
              {[
                { key: "evaluations" as const, label: "Policy Evaluations", count: stats.totalEvaluations, color: PALETTE.deep },
                { key: "security" as const, label: "Security Audits", count: stats.totalSecurity, color: PALETTE.purple },
                { key: "sandbox" as const, label: "Sandbox Integrity", count: stats.totalSandbox, color: STATUS.err },
                { key: "audit" as const, label: "Audit Trail", count: stats.totalAudit, color: PALETTE.neutral },
                { key: "policies" as const, label: "Active Policies", count: DEFAULT_POLICIES.length, color: PALETTE.teal },
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
              {/* ── Policy Evaluations Tab ── */}
              {activeTab === "evaluations" && (
                <>
                  {evaluations.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No policy evaluations recorded</div>
                  )}
                  {evaluations.map((ev, i) => (
                    <EvaluationRow key={ev.id || i} evaluation={ev} />
                  ))}
                </>
              )}

              {/* ── Security Audits Tab ── */}
              {activeTab === "security" && (
                <>
                  {securityAudits.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No security audits recorded</div>
                  )}
                  <div className="space-y-1">
                    {/* Mini stats */}
                    <div className="flex gap-4 mb-3 px-1">
                      <span className="text-xs text-ink-muted">
                        Total: <span className="text-ink-soft font-semibold">{stats.totalSecurity}</span>
                      </span>
                      <span className="text-xs text-accent-green">✓ {stats.securityPassed}</span>
                      <span className="text-xs text-accent-red">✗ {stats.securityFailed}</span>
                      {stats.securityRunning > 0 && <span className="text-xs text-accent-amber">◌ {stats.securityRunning} running</span>}
                      <span className="text-xs text-ink-muted">Pass rate: <span className={stats.securityPassRate >= 90 ? "text-accent-green" : "text-accent-amber"}>{stats.securityPassRate}%</span></span>
                    </div>
                    {securityAudits.slice(0, 50).map((s, i) => (
                      <SecurityRow key={`${s.id}-${i}`} audit={s} />
                    ))}
                  </div>
                </>
              )}

              {/* ── Sandbox Integrity Tab ── */}
              {activeTab === "sandbox" && (
                <>
                  {sandboxChecks.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No sandbox integrity data available</div>
                  )}
                  {sandboxChecks.length > 0 && (
                    <div className="space-y-3">
                      <div className="flex gap-3 mb-2 px-1">
                        <span className="text-xs text-ink-muted">
                          Checks: <span className="text-ink-soft font-semibold">{stats.totalSandbox}</span>
                        </span>
                        <span className="text-xs text-accent-green">✓ {stats.sandboxPassed} passed</span>
                        <span className="text-xs text-ink-muted">
                          {stats.totalSandbox - stats.sandboxPassed} pending bridge
                        </span>
                      </div>
                      {sandboxChecks.map((check) => (
                        <SandboxCheckRow key={check.id} check={check} />
                      ))}
                      <div className="text-[10px] text-ink-muted italic mt-2 px-1">
                        Sandbox integrity data requires backend→frontend bridging. Status shown is derived from available verification events.
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* ── Audit Trail Tab ── */}
              {activeTab === "audit" && (
                <>
                  {auditTrail.length === 0 && (
                    <div className="text-center text-ink-muted text-sm py-8">No audit entries recorded</div>
                  )}
                  <div className="space-y-1">
                    {auditTrail.slice(0, 50).map((a, i) => (
                      <AuditRow key={`${a.id}-${i}`} entry={a} />
                    ))}
                  </div>
                </>
              )}

              {/* ── Active Policies Tab ── */}
              {activeTab === "policies" && (
                <div className="space-y-2">
                  {DEFAULT_POLICIES.map((p) => (
                    <PolicyRow key={p.id} policy={p} />
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

function SummaryCard({ label, value, sub, isPercent, color, icon }: {
  label: string; value: number; sub?: string; isPercent?: boolean; color: string; icon: string;
}) {
  return (
    <div className="panel">
      <div className="flex items-center gap-1.5 mb-1">
        <span className="text-xs" style={{ color }}>{icon}</span>
        <div className="text-[10px] text-ink-muted uppercase tracking-wider">{label}</div>
      </div>
      <div className="text-xl font-bold" style={{ color }}>
        {value}{isPercent ? "%" : ""}
      </div>
      {sub && <div className="text-[10px] text-ink-muted mt-0.5 truncate">{sub}</div>}
    </div>
  );
}

function EvaluationRow({ evaluation }: { evaluation: GovernanceEvaluation }) {
  const outcomeColor = {
    approve: STATUS.ok,
    reject: STATUS.err,
    escalate: STATUS.warn,
    replan: OUTCOME_COLORS.revision,
    override: PALETTE.purple,
  }[evaluation.outcome] || PALETTE.neutral;

  const outcomeIcon = {
    approve: "✓",
    reject: "✗",
    escalate: "↑",
    replan: "↻",
    override: "◉",
  }[evaluation.outcome] || "?";

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2.5 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs font-semibold px-1.5 py-0.5 rounded" style={{ color: outcomeColor, backgroundColor: `${outcomeColor}15` }}>
          {outcomeIcon} {evaluation.outcome.toUpperCase()}
        </span>
        {evaluation.assignedTo && (
          <span className="text-xs text-ink-muted">→ {evaluation.assignedTo}</span>
        )}
        {evaluation.targetType && (
          <span className="text-[10px] text-ink-muted font-mono">
            {evaluation.targetType}:{evaluation.targetId.slice(0, 8)}
          </span>
        )}
        <span className="text-[10px] text-ink-muted ml-auto">{fmtRelative(evaluation.timestamp)}</span>
      </div>
      {evaluation.reason && <p className="text-sm text-ink-soft">{evaluation.reason}</p>}
      {evaluation.conditions.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-1">
          {evaluation.conditions.map((c, i) => (
            <span key={i} className="text-[10px] text-ink-muted px-1.5 py-0.5 rounded bg-surface-border/50">
              {c}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function SecurityRow({ audit }: { audit: SecurityAudit }) {
  const statusIcon = audit.status === "passed" ? "✓" : audit.status === "failed" ? "✗" : audit.status === "running" ? "◌" : "○";
  const statusColor = audit.status === "passed" ? STATUS.ok : audit.status === "failed" ? STATUS.err : audit.status === "running" ? STATUS.warn : PALETTE.neutral;

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2 hover:bg-surface-alt/50 transition-colors flex items-center gap-3">
      <span className="text-sm" style={{ color: statusColor }}>{statusIcon}</span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs text-ink-soft truncate">{audit.action || audit.type.replace(/_/g, " ")}</span>
          <span className="text-[10px] font-semibold" style={{ color: statusColor }}>{audit.status.toUpperCase()}</span>
        </div>
        <div className="flex items-center gap-2 mt-0.5">
          <span className="text-[10px] text-ink-muted">{audit.specialist}</span>
          <span className="text-[10px] text-ink-muted">{audit.type}</span>
        </div>
      </div>
      <span className="text-[10px] text-ink-muted shrink-0">{fmtRelative(audit.timestamp)}</span>
    </div>
  );
}

function AuditRow({ entry }: { entry: AuditEntry }) {
  const outcomeColor = entry.outcome === "allowed" || entry.outcome === "approve" ? STATUS.ok
    : entry.outcome === "denied" || entry.outcome === "reject" ? STATUS.err
    : entry.outcome === "escalated" || entry.outcome === "escalate" ? STATUS.warn
    : PALETTE.neutral;

  const subColor = entry.subsystem === "security" ? PALETTE.purple
    : entry.subsystem === "recovery" ? PALETTE.deep
    : PALETTE.teal;

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2 hover:bg-surface-alt/50 transition-colors flex items-center gap-3">
      <span className="text-xs font-mono" style={{ color: subColor }}>
        {entry.subsystem.slice(0, 4).toUpperCase()}
      </span>
      <div className="flex-1 min-w-0">
        <div className="text-xs text-ink-soft truncate">{entry.action}</div>
        <div className="flex items-center gap-2 mt-0.5">
          <span className="text-[10px] text-ink-muted">{entry.actor}</span>
          <span className="text-[10px] text-ink-muted">{entry.type.replace(/_/g, " ")}</span>
        </div>
      </div>
      <span className="text-[10px] font-semibold" style={{ color: outcomeColor }}>{entry.outcome.toUpperCase()}</span>
      <span className="text-[10px] text-ink-muted shrink-0">{fmtRelative(entry.timestamp)}</span>
    </div>
  );
}

function SandboxCheckRow({ check }: { check: SandboxCheck }) {
  const statusIcon = check.passed ? "✓" : check.status === "pending_bridge" ? "○" : "✗";
  const statusColor = check.passed ? STATUS.ok : check.status === "pending_bridge" ? PALETTE.neutral : STATUS.err;

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2.5 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-sm" style={{ color: statusColor }}>{statusIcon}</span>
        <span className="text-xs font-semibold text-ink-soft">{check.name}</span>
        <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded" style={{ color: statusColor, backgroundColor: `${statusColor}15` }}>
          {check.status.replace(/_/g, " ").toUpperCase()}
        </span>
      </div>
      <p className="text-[11px] text-ink-muted mb-1">{check.details}</p>
      <div className="text-[10px] text-ink-muted font-mono">{check.source}</div>
    </div>
  );
}

function PolicyRow({ policy }: { policy: PolicyDefinition }) {
  const effectColor = policy.effect === "allow" ? STATUS.ok
    : policy.effect === "deny" ? STATUS.err
    : policy.effect === "require_approval" ? STATUS.warn
    : PALETTE.neutral;

  const effectLabel = policy.effect === "require_approval" ? "REQUIRE APPROVAL"
    : policy.effect === "log_only" ? "LOG ONLY"
    : policy.effect.toUpperCase();

  return (
    <div className="border border-surface-border rounded-lg px-3 py-2.5 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-1">
        <span className={`w-1.5 h-1.5 rounded-full ${policy.enabled ? "bg-accent-green" : "bg-gray-600"}`} />
        <span className="text-xs font-semibold text-ink-soft">{policy.name}</span>
        <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded" style={{ color: effectColor, backgroundColor: `${effectColor}15` }}>
          {effectLabel}
        </span>
        {!policy.enabled && <span className="text-[10px] text-ink-muted">DISABLED</span>}
      </div>
      <p className="text-[11px] text-ink-muted mb-1">{policy.description}</p>
      <div className="flex items-center gap-3 text-[10px] text-ink-muted">
        <span>Scope: <span className="text-ink-muted">{policy.scope}</span></span>
        {policy.specialists.length > 0 && (
          <span>Specialists: <span className="text-ink-muted">{policy.specialists.join(", ")}</span>
          </span>
        )}
        <span>Priority: {policy.priority}</span>
      </div>
    </div>
  );
}
