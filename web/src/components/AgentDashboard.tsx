import { useMemo } from "react";
import type { UIEvent, AgentState, AgentAction } from "../types";

interface AgentDashboardProps {
  events: UIEvent[];
}

/** Specialist configuration */
const AGENTS: Record<string, { label: string; color: string; icon: string }> = {
  ARCHITECT: { label: "Architect", color: "#3b82f6", icon: "◉" },
  ORACLE:    { label: "Oracle",    color: "#8c5cff", icon: "◆" },
  FORGE:     { label: "Forge",     color: "#00e38c", icon: "⚙" },
  SENTINEL:  { label: "Sentinel",  color: "#ff5c7a", icon: "🛡" },
  TERMINUS:  { label: "Terminus",  color: "#f7b731", icon: "▶" },
  HERALD:    { label: "Herald",    color: "#39c8ff", icon: "★" },
  CONSENSUS: { label: "Consensus", color: "#19f5a5", icon: "↻" },
};

const AGENT_KEYS = Object.keys(AGENTS);

/** Determine agent status based on recent event activity */
function deriveStatus(
  specEvents: UIEvent[],
  latestEvents: UIEvent[]
): "active" | "idle" | "thinking" | "acting" {
  if (specEvents.length === 0) return "idle";

  // Check the most recent event type
  const last = latestEvents[0];
  if (!last) return "idle";

  const type = last.type;
  const isRecent = Date.now() / 1000 - last.timestamp < 300; // within 5 min

  if (!isRecent) return "idle";
  if (type.includes("thinking")) return "thinking";
  if (type.includes("started") || type.includes("activated")) return "acting";
  return "active";
}

export function AgentDashboard({ events }: AgentDashboardProps) {
  const agents = useMemo(() => {
    const now = Date.now() / 1000;

    return AGENT_KEYS.map((key): AgentState => {
      const cfg = AGENTS[key];
      const specEvents = events.filter(
        (e) => e.specialist?.toUpperCase() === key
      );
      const sorted = [...specEvents].sort(
        (a, b) => b.timestamp - a.timestamp
      );

      const status = deriveStatus(specEvents, sorted);

      // Current task = latest action text
      const currentTask = sorted[0]?.action || "—";

      // Confidence = average confidence from blackboard publications
      const pubs = specEvents.filter(
        (e) => e.type === "blackboard_publication"
      );
      const confidences = pubs
        .map((e) => Number(e.data?.confidence || 0))
        .filter((c) => c > 0);
      const avgConfidence =
        confidences.length > 0
          ? confidences.reduce((a, b) => a + b, 0) / confidences.length
          : 0;

      // Success rate: varies by specialist type
      let successes = 0;
      let totals = 0;

      if (key === "TERMINUS") {
        // Execution exit codes
        const execs = specEvents.filter((e) => e.type === "execution_completed");
        totals = execs.length;
        successes = execs.filter(
          (e) => Number(e.data?.exit_code || 0) === 0
        ).length;
      } else if (key === "SENTINEL") {
        // Verification pass/fail
        const verifs = specEvents.filter((e) => e.type.startsWith("verification"));
        totals = verifs.length;
        successes = verifs.filter(
          (e) => e.type === "verification_passed" || e.type === "verification_completed"
        ).length;
        // Also count blackboard verification status
        const pubsWithStatus = pubs.filter(
          (e) => e.data?.verification_status === "verified"
        );
        successes += pubsWithStatus.length;
        totals += pubsWithStatus.length;
        const pubsFailed = pubs.filter(
          (e) => e.data?.verification_status === "failed" || e.data?.verification_status === "challenged"
        );
        totals += pubsFailed.length;
      } else if (key === "FORGE") {
        // Publications that were consumed (success) vs total
        totals = pubs.length;
        successes = pubs.filter(
          (e) => e.data?.verification_status === "verified" || e.data?.consumedBy
        ).length;
      } else if (key === "ORACLE") {
        // Count publications whose entry_id appears in consumption events
        const consumedEntryIds = new Set(
          specEvents
            .filter((ce) => ce.type === "finding_consumed" && String(ce.data?.entry_owner || "").toUpperCase() === "ORACLE")
            .map((ce) => String(ce.data?.entry_id || ""))
            .filter(Boolean)
        );
        totals = pubs.length;
        successes = pubs.filter((e) => {
          const entryId = String(e.data?.entry_id || e.data?.id || "");
          return consumedEntryIds.has(entryId) || e.data?.verification_status === "verified";
        }).length;
      } else if (key === "ARCHITECT") {
        // Decision events
        const decisions = specEvents.filter((e) => e.type === "architect_decision");
        totals = decisions.length;
        successes = decisions.filter(
          (e) =>
            !e.action.toLowerCase().includes("reject") &&
            !e.action.toLowerCase().includes("override")
        ).length;
      } else if (key === "HERALD") {
        // Reports generated
        totals = specEvents.length;
        successes = specEvents.filter(
          (e) => e.type === "report_generated"
        ).length;
      } else if (key === "CONSENSUS") {
        // Consensus outcomes
        const cons = specEvents.filter((e) => e.type === "consensus_formed");
        totals = cons.length;
        successes = cons.filter(
          (e) => Number(e.data?.confidence || 0) >= 0.7
        ).length;
      }

      const successRate = totals > 0 ? successes / totals : 0;

      // Contribution score: weighted combination of metrics
      const pubScore = pubs.length * 3;
      const consumedCount = specEvents.filter(
        (e) => e.type === "finding_consumed"
      ).length;
      const execScore = specEvents.filter(
        (e) => e.type === "execution_completed"
      ).length * 2;
      const decisionScore = specEvents.filter(
        (e) => e.type === "architect_decision"
      ).length * 4;
      const verifScore = specEvents.filter(
        (e) => e.type.startsWith("verification")
      ).length * 2;
      const reportScore = specEvents.filter(
        (e) => e.type === "report_generated"
      ).length * 3;
      const rawScore = pubScore + consumedCount + execScore + decisionScore + verifScore + reportScore;
      const contributionScore = Math.min(100, rawScore);

      // Recent actions (last 5)
      const recentActions: AgentAction[] = sorted.slice(0, 5).map((e) => ({
        type: e.type,
        summary: e.action,
        timestamp: e.timestamp,
        color: e.color,
        icon: e.icon || "•",
      }));

      // Metrics breakdown
      const metrics = {
        totalEvents: specEvents.length,
        publications: pubs.length,
        consumptions: specEvents.filter((e) => e.type === "finding_consumed").length,
        verifications: specEvents.filter((e) => e.type.startsWith("verification")).length,
        decisions: specEvents.filter((e) => e.type === "architect_decision").length,
        executions: specEvents.filter((e) => e.type === "execution_started" || e.type === "execution_completed").length,
        reports: specEvents.filter((e) => e.type === "report_generated").length,
      };

      return {
        name: key,
        label: cfg.label,
        color: cfg.color,
        icon: cfg.icon,
        status,
        currentTask,
        confidence: Math.round(avgConfidence * 100),
        successRate: Math.round(successRate * 100),
        contributionScore,
        metrics,
        recentActions,
      };
    });
  }, [events]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Top bar */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <h2 className="text-lg font-bold text-ink">Agent Dashboard</h2>
        <span className="text-xs text-ink-muted">
          {agents.filter((a) => a.status !== "idle").length} active
        </span>
      </header>

      {/* Agent cards grid */}
      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-7xl mx-auto grid grid-cols-1 lg:grid-cols-2 gap-4">
          {agents.map((agent) => (
            <AgentCard key={agent.name} agent={agent} />
          ))}
        </div>
      </div>
    </div>
  );
}

/* ── Single Agent Card ──────────────────────────────────────────── */

function AgentCard({ agent }: { agent: AgentState }) {
  const statusColor = {
    active: "bg-accent-green",
    idle: "bg-gray-600",
    thinking: "bg-accent-amber",
    acting: "bg-accent-blue",
  }[agent.status];

  const statusLabel = {
    active: "Active",
    idle: "Idle",
    thinking: "Thinking",
    acting: "Acting",
  }[agent.status];

  return (
    <div className="panel">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <span className="text-xl" style={{ color: agent.color }}>{agent.icon}</span>
          <div>
            <h3 className="text-base font-bold" style={{ color: agent.color }}>
              {agent.label}
            </h3>
            <span className="text-[10px] font-mono text-ink-muted">{agent.name}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className={`w-2 h-2 rounded-full ${statusColor}`} />
          <span className="text-xs text-ink-muted">{statusLabel}</span>
        </div>
      </div>

      {/* Current task */}
      <div className="mb-4">
        <div className="text-[10px] text-ink-muted uppercase tracking-wider mb-1">Current Task</div>
        <p className="text-sm text-ink-soft leading-relaxed line-clamp-2">{agent.currentTask}</p>
      </div>

      {/* Metrics grid */}
      <div className="grid grid-cols-3 gap-3 mb-4">
        {/* Confidence */}
        <div className="text-center">
          <div className="text-lg font-bold text-ink">{agent.confidence}%</div>
          <div className="text-[10px] text-ink-muted">Confidence</div>
        </div>
        {/* Success Rate */}
        <div className="text-center">
          <div
            className={`text-lg font-bold ${
              agent.successRate >= 80
                ? "text-accent-green"
                : agent.successRate >= 50
                  ? "text-accent-amber"
                  : "text-accent-red"
            }`}
          >
            {agent.successRate}%
          </div>
          <div className="text-[10px] text-ink-muted">Success</div>
        </div>
        {/* Contribution Score */}
        <div className="text-center">
          <div className="text-lg font-bold text-accent-purple">{agent.contributionScore}</div>
          <div className="text-[10px] text-ink-muted">Contrib</div>
        </div>
      </div>

      {/* Mini contribution bar */}
      <div className="w-full h-1 bg-surface-border rounded-full overflow-hidden mb-3">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{
            width: `${Math.min(agent.contributionScore, 100)}%`,
            backgroundColor: agent.color,
          }}
        />
      </div>

      {/* Activity metrics */}
      <div className="grid grid-cols-4 gap-2 mb-3 text-center text-[10px]">
        <div>
          <div className="text-ink-muted">{agent.metrics.publications}</div>
          <div className="text-ink-muted">Pubs</div>
        </div>
        <div>
          <div className="text-ink-muted">{agent.metrics.consumptions}</div>
          <div className="text-ink-muted">Consumed</div>
        </div>
        <div>
          <div className="text-ink-muted">{agent.metrics.verifications}</div>
          <div className="text-ink-muted">Verifs</div>
        </div>
        <div>
          <div className="text-ink-muted">{agent.metrics.totalEvents}</div>
          <div className="text-ink-muted">Events</div>
        </div>
      </div>

      {/* Recent actions */}
      {agent.recentActions.length > 0 && (
        <div>
          <div className="text-[10px] text-ink-muted uppercase tracking-wider mb-1.5">Recent Actions</div>
          <div className="space-y-1">
            {agent.recentActions.map((action, i) => (
              <div key={i} className="flex items-start gap-2 text-xs">
                <span style={{ color: action.color || agent.color }} className="shrink-0 mt-0.5">
                  {action.icon || "•"}
                </span>
                <span className="text-ink-soft truncate flex-1">{action.summary}</span>
                <span className="text-ink-muted shrink-0">
                  {formatRelative(action.timestamp)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function formatRelative(ts: number): string {
  const diff = Date.now() - ts * 1000;
  if (diff < 60000) return "now";
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h`;
  return `${Math.floor(diff / 86400000)}d`;
}
