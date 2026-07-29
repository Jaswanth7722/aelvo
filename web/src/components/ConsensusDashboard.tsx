import { useMemo, useState } from "react";
import type { UIEvent, ConsensusRecord, DecisionRecord, ChallengeRecord } from "../types";

interface ConsensusDashboardProps {
  events: UIEvent[];
}

function fmt(ts: number): string {
  const d = new Date(ts * 1000);
  const now = Date.now();
  const diff = now - d.getTime();
  if (diff < 60000) return "just now";
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return d.toLocaleDateString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

export function ConsensusDashboard({ events }: ConsensusDashboardProps) {
  const [activeTab, setActiveTab] = useState<"decisions" | "consensus" | "challenges">("decisions");

  const { decisions, consensusItems, challenges, stats } = useMemo(() => {
    const decEvents = events.filter((e) => e.type === "architect_decision");
    const consEvents = events.filter((e) => e.type === "consensus_formed");
    const chalEvents = events.filter((e) => e.type === "challenge_raised");

    const decisions: DecisionRecord[] = decEvents.map((e) => ({
      id: String(e.data?.decision_id || e.data?.id || ""),
      outcome: String(e.data?.outcome || e.action || ""),
      targetType: String(e.data?.target_type || ""),
      targetId: String(e.data?.target_id || ""),
      reason: String(e.data?.reason || ""),
      conditions: Array.isArray(e.data?.conditions) ? e.data?.conditions as string[] : [],
      assignedTo: String(e.data?.assigned_to || ""),
      overriddenRec: String(e.data?.overridden_recommendation || ""),
      timestamp: e.timestamp,
    })).sort((a, b) => b.timestamp - a.timestamp);

    const consensusItems: ConsensusRecord[] = consEvents.map((e) => ({
      id: String(e.data?.consensus_id || e.data?.id || ""),
      targetId: String(e.data?.target_id || ""),
      recommendation: String(e.data?.recommendation || e.action || ""),
      confidence: Number(e.data?.confidence || 0),
      method: String(e.data?.method || e.data?.resolution_strategy || "majority"),
      positions: (e.data?.positions as Record<string, string>) || {},
      timestamp: e.timestamp,
    })).sort((a, b) => b.timestamp - a.timestamp);

    const challenges: ChallengeRecord[] = chalEvents.map((e) => ({
      id: String(e.data?.challenge_id || ""),
      entryId: String(e.data?.entry_id || ""),
      challenger: String(e.data?.challenger || e.specialist || ""),
      claim: String(e.data?.challenged_claim || e.action || ""),
      evidence: String(e.data?.evidence || ""),
      timestamp: e.timestamp,
    })).sort((a, b) => b.timestamp - a.timestamp);

    const approved = decisions.filter((d) => d.outcome.toLowerCase().includes("approv")).length;
    const rejected = decisions.filter((d) => d.outcome.toLowerCase().includes("reject")).length;
    const escalated = decisions.filter((d) => d.outcome.toLowerCase().includes("escalat")).length;
    const revision = decisions.filter((d) => d.outcome.toLowerCase().includes("revision")).length;
    const replanned = decisions.filter((d) => d.outcome.toLowerCase().includes("replan") || d.outcome.toLowerCase().includes("override")).length;

    const highConf = consensusItems.filter((c) => c.confidence >= 0.7).length;
    const totalPositionSlots = consensusItems.reduce((sum, c) => sum + Object.keys(c.positions).length, 0);
    const forCount = consensusItems.reduce((sum, c) =>
      sum + Object.values(c.positions).filter((p) => p.toLowerCase() === "for" || p.toLowerCase() === "approve" || p.toLowerCase() === "yes").length, 0
    );

    return {
      decisions,
      consensusItems,
      challenges,
      stats: {
        totalDecisions: decisions.length,
        approved,
        rejected,
        escalated,
        revision,
        replanned,
        totalConsensus: consensusItems.length,
        highConf,
        totalChallenges: challenges.length,
        totalPositionSlots,
        forCount,
      },
    };
  }, [events]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-bold text-gray-200">Consensus Dashboard</h2>
          <span className="text-xs text-gray-500">{stats.totalDecisions + stats.totalConsensus + stats.totalChallenges} total events</span>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-7xl mx-auto space-y-6">
          {/* Row 1: Summary cards */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <SummaryCard
              label="Decisions"
              value={stats.totalDecisions}
              sub={`${stats.approved} approved · ${stats.rejected} rejected · ${stats.escalated} escalated`}
              color="#3b82f6"
            />
            <SummaryCard
              label="Consensus"
              value={stats.totalConsensus}
              sub={`${stats.highConf} high confidence · ${stats.forCount} for`}
              color="#19f5a5"
            />
            <SummaryCard
              label="Challenges"
              value={stats.totalChallenges}
              sub={`across ${stats.totalPositionSlots} positions`}
              color="#ff5c7a"
            />
            <SummaryCard
              label="Approval Rate"
              value={stats.totalDecisions > 0 ? Math.round((stats.approved / stats.totalDecisions) * 100) : 0}
              sub={`${stats.replanned} replanned/override`}
              color="#00e38c"
              isPercent
            />
          </div>

          {/* Row 2: Stacked outcome bar */}
          {stats.totalDecisions > 0 && (
            <div className="panel">
              <div className="text-xs text-gray-500 uppercase tracking-wider mb-3">Decision Outcomes</div>
              <div className="w-full h-4 bg-surface-border rounded-full overflow-hidden flex">
                {stats.approved > 0 && <div className="h-full bg-accent-green transition-all" style={{ width: `${(stats.approved / stats.totalDecisions) * 100}%` }} title={`${stats.approved} approved`} />}
                {stats.rejected > 0 && <div className="h-full bg-accent-red transition-all" style={{ width: `${(stats.rejected / stats.totalDecisions) * 100}%` }} title={`${stats.rejected} rejected`} />}
                {stats.escalated > 0 && <div className="h-full bg-accent-amber transition-all" style={{ width: `${(stats.escalated / stats.totalDecisions) * 100}%` }} title={`${stats.escalated} escalated`} />}
                {stats.revision > 0 && <div className="h-full transition-all" style={{ width: `${(stats.revision / stats.totalDecisions) * 100}%`, backgroundColor: "#39c8ff" }} title={`${stats.revision} revision`} />}
                {stats.replanned > 0 && <div className="h-full bg-accent-purple transition-all" style={{ width: `${(stats.replanned / stats.totalDecisions) * 100}%` }} title={`${stats.replanned} replanned`} />}
              </div>
              <div className="flex gap-4 mt-2 text-[10px] text-gray-500">
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-green mr-1" /> Approved ({stats.approved})</span>
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-red mr-1" /> Rejected ({stats.rejected})</span>
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-amber mr-1" /> Escalated ({stats.escalated})</span>
                <span><span className="w-2 h-2 inline-block rounded-full mr-1" style={{ backgroundColor: "#39c8ff" }} /> Revision ({stats.revision})</span>
                <span><span className="w-2 h-2 inline-block rounded-full bg-accent-purple mr-1" /> Replanned ({stats.replanned})</span>
              </div>
            </div>
          )}

          {/* Row 3: Tabbed detail view */}
          <div className="panel">
            {/* Tabs */}
            <div className="flex gap-4 border-b border-surface-border pb-3 mb-3">
              {[ 
                { key: "decisions" as const, label: "Architect Decisions", count: stats.totalDecisions, color: "#3b82f6" },
                { key: "consensus" as const, label: "Consensus Sessions", count: stats.totalConsensus, color: "#19f5a5" },
                { key: "challenges" as const, label: "Challenges", count: stats.totalChallenges, color: "#ff5c7a" },
              ].map((tab) => (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`text-xs font-medium pb-3 -mb-3 border-b-2 transition-colors ${
                    activeTab === tab.key
                      ? "text-gray-200"
                      : "text-gray-600 hover:text-gray-400"
                  }`}
                  style={{ borderColor: activeTab === tab.key ? tab.color : "transparent" }}
                >
                  {tab.label}
                  <span className="ml-1.5 text-gray-600">({tab.count})</span>
                </button>
              ))}
            </div>

            {/* Tab content */}
            <div className="max-h-[400px] overflow-y-auto space-y-2">
              {activeTab === "decisions" && decisions.length === 0 && (
                <div className="text-center text-gray-600 text-sm py-8">No architect decisions yet</div>
              )}
              {activeTab === "decisions" && decisions.map((d, i) => (
                <DecisionRow key={d.id || i} decision={d} />
              ))}

              {activeTab === "consensus" && consensusItems.length === 0 && (
                <div className="text-center text-gray-600 text-sm py-8">No consensus sessions yet</div>
              )}
              {activeTab === "consensus" && consensusItems.map((c, i) => (
                <ConsensusRow key={c.id || i} item={c} />
              ))}

              {activeTab === "challenges" && challenges.length === 0 && (
                <div className="text-center text-gray-600 text-sm py-8">No challenges raised yet</div>
              )}
              {activeTab === "challenges" && challenges.map((c, i) => (
                <ChallengeRow key={c.id || i} item={c} />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Sub-components ──────────────────────────────────────────────── */

function SummaryCard({ label, value, sub, color, isPercent }: {
  label: string; value: number; sub: string; color: string; isPercent?: boolean;
}) {
  return (
    <div className="panel">
      <div className="text-xs text-gray-500 uppercase tracking-wider mb-1">{label}</div>
      <div className="text-2xl font-bold" style={{ color }}>{value}{isPercent ? "%" : ""}</div>
      <div className="text-[10px] text-gray-600 mt-1 truncate">{sub}</div>
    </div>
  );
}

function DecisionRow({ decision }: { decision: DecisionRecord }) {
  const outcomeColor = decision.outcome.toLowerCase().includes("approv") ? "#00e38c"
    : decision.outcome.toLowerCase().includes("reject") ? "#ff5c7a"
    : decision.outcome.toLowerCase().includes("escalat") ? "#f7b731"
    : decision.outcome.toLowerCase().includes("override") ? "#8c5cff"
    : "#52627f";

  return (
    <div className="border border-surface-border rounded-lg px-4 py-2.5 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs font-semibold px-1.5 py-0.5 rounded" style={{ color: outcomeColor, backgroundColor: `${outcomeColor}15` }}>
          {decision.outcome.toUpperCase()}
        </span>
        {decision.assignedTo && <span className="text-xs text-gray-500">→ {decision.assignedTo}</span>}
        <span className="text-[10px] text-gray-600 ml-auto">{fmt(decision.timestamp)}</span>
      </div>
      {decision.reason && <p className="text-sm text-gray-400">{decision.reason}</p>}
      {decision.overriddenRec && (
        <div className="mt-1 text-[10px] text-accent-purple">Override: {decision.overriddenRec}</div>
      )}
    </div>
  );
}

function ConsensusRow({ item }: { item: ConsensusRecord }) {
  const positions = Object.entries(item.positions);
  const forP = positions.filter(([_, p]) => p.toLowerCase() === "for" || p.toLowerCase() === "approve" || p.toLowerCase() === "yes").length;
  const againstP = positions.filter(([_, p]) => p.toLowerCase() === "against" || p.toLowerCase() === "reject" || p.toLowerCase() === "no").length;

  return (
    <div className="border border-surface-border rounded-lg px-4 py-2.5 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs font-semibold" style={{ color: "#19f5a5" }}>CONSENSUS</span>
        <span className="text-xs text-gray-500">{item.method}</span>
        <span className="text-xs text-gray-500">
          {item.confidence >= 0.7 ? "✅" : item.confidence >= 0.4 ? "◌" : "⚠"} {Math.round(item.confidence * 100)}%
        </span>
        <span className="text-[10px] text-gray-600 ml-auto">{fmt(item.timestamp)}</span>
      </div>
      <p className="text-sm text-gray-300">{item.recommendation}</p>
      {positions.length > 0 && (
        <div className="flex items-center gap-2 mt-1.5">
          <span className="text-xs text-accent-green">{forP} for</span>
          <span className="text-xs text-gray-600">·</span>
          <span className="text-xs text-accent-red">{againstP} against</span>
          <span className="text-xs text-gray-600">·</span>
          <span className="text-xs text-gray-500">{positions.length - forP - againstP} neutral</span>
        </div>
      )}
      {positions.length > 0 && (
        <div className="flex flex-wrap gap-1 mt-1.5">
          {positions.slice(0, 6).map(([spec, pos]) => (
            <span key={spec} className="text-[10px] px-1.5 py-0.5 rounded" style={{
              color: pos.toLowerCase() === "for" || pos.toLowerCase() === "approve" ? "#00e38c" : pos.toLowerCase() === "against" || pos.toLowerCase() === "reject" ? "#ff5c7a" : "#52627f",
              backgroundColor: pos.toLowerCase() === "for" || pos.toLowerCase() === "approve" ? "#00e38c15" : pos.toLowerCase() === "against" || pos.toLowerCase() === "reject" ? "#ff5c7a15" : "#52627f15",
            }}>
              {spec}: {pos.toUpperCase()}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

function ChallengeRow({ item }: { item: ChallengeRecord }) {
  return (
    <div className="border border-surface-border rounded-lg px-4 py-2.5 hover:bg-surface-alt/50 transition-colors">
      <div className="flex items-center gap-2 mb-1">
        <span className="text-xs font-semibold text-accent-red">⚠ CHALLENGE</span>
        <span className="text-xs text-gray-400">by {item.challenger}</span>
        {item.entryId && <span className="text-[10px] font-mono text-gray-600">entry: {item.entryId.slice(0, 10)}</span>}
        <span className="text-[10px] text-gray-600 ml-auto">{fmt(item.timestamp)}</span>
      </div>
      <p className="text-sm text-gray-400">{item.claim}</p>
      {item.evidence && <p className="text-xs text-gray-600 mt-1 italic">{item.evidence}</p>}
    </div>
  );
}
