import type { AgentLiveStatus } from "../types";

interface AgentActivityPanelProps {
  agents: AgentLiveStatus[];
  eventCount: number;
  onNavigate?: (path: string) => void;
}

const AGENT_ORDER = ["HERMES", "ARCHITECT", "ORACLE", "FORGE", "SENTINEL", "TERMINUS", "HERALD"];

const AGENT_COLORS: Record<string, string> = {
  HERMES: "#0891B2",
  ARCHITECT: "#7C3AED",
  ORACLE: "#8B5CF6",
  FORGE: "#16A34A",
  SENTINEL: "#E11D48",
  TERMINUS: "#F59E0B",
  HERALD: "#FF9F45",
};

export function AgentActivityPanel({ agents, eventCount, onNavigate }: AgentActivityPanelProps) {
  const sorted = AGENT_ORDER
    .map((name) => agents.find((a) => a.name.toUpperCase() === name))
    .filter(Boolean) as AgentLiveStatus[];

  return (
    <div className="w-72 border-l border-surface-border bg-white/70 backdrop-blur-md flex flex-col shrink-0 overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-surface-border">
        <h3 className="text-xs font-bold text-ink-soft uppercase tracking-wider">Agent Activity</h3>
        <p className="text-[10px] text-ink-muted mt-0.5">{eventCount} events in this session</p>
      </div>

      {/* Agent cards */}
      <div className="flex-1 overflow-y-auto p-3 space-y-2">
        {sorted.length === 0 ? (
          <>
            {AGENT_ORDER.map((name) => (
              <AgentCardSkeleton key={name} name={name} />
            ))}
          </>
        ) : (
          sorted.map((agent) => (
            <AgentCard key={agent.name} agent={agent} onNavigate={onNavigate} />
          ))
        )}
      </div>

      {/* Legend footer */}
      <div className="px-4 py-2 border-t border-surface-border">
        <div className="flex items-center gap-3 text-[9px] text-ink-muted flex-wrap">
          <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-gray-300" /> Idle</span>
          <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-accent-amber animate-pulse" /> Acting</span>
          <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-brand-purple" /> Thinking</span>
          <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-accent-green" /> Done</span>
        </div>
      </div>
    </div>
  );
}

/* ── Agent Card ─────────────────────────────────────────── */

interface AgentCardProps {
  agent: AgentLiveStatus;
  onNavigate?: (path: string) => void;
}

function AgentCard({ agent, onNavigate }: AgentCardProps) {
  const statusColor =
    agent.status === "idle" ? "#9CA3AF" :
    agent.status === "thinking" ? "#7C3AED" :
    agent.status === "acting" ? "#F59E0B" :
    "#16A34A";

  const statusPulse = agent.status === "acting" || agent.status === "thinking" ? "animate-pulse-glow" : "";

  return (
    <div className="bg-white border border-surface-border rounded-xl p-3 shadow-soft transition-all duration-200 hover:shadow-card hover:border-brand-orange/40 hover:-translate-y-0.5">
      {/* Header row */}
      <div className="flex items-center gap-2 mb-2">
        <span className="text-base w-7 h-7 rounded-lg bg-brand-purple/10 flex items-center justify-center" style={{ color: agent.color }}>
          {agent.icon}
        </span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5">
            <span className="text-xs font-bold text-ink">{agent.label}</span>
            <span className="text-[9px] font-mono text-ink-muted">{agent.name}</span>
          </div>
        </div>
        <span className={`w-2 h-2 rounded-full ${statusPulse}`} style={{ backgroundColor: statusColor }} />
      </div>

      {/* Status bar */}
      <div className="mb-1.5">
        <div className="flex items-center justify-between text-[10px] mb-0.5">
          <span className="text-ink-soft truncate">{agent.currentTask || "Waiting..."}</span>
          <span className="text-ink-muted ml-2">{Math.round(agent.progress * 100)}%</span>
        </div>
        <div className="w-full h-1.5 bg-surface-border/60 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500 ease-out"
            style={{
              width: `${agent.progress * 100}%`,
              backgroundColor: statusColor,
            }}
          />
        </div>
      </div>

      {/* Last action */}
      {agent.lastAction && (
        <p className="text-[10px] text-ink-muted truncate">{agent.lastAction}</p>
      )}
    </div>
  );
}

/* ── Skeleton (placeholder when no data) ─────────────────── */

function AgentCardSkeleton({ name }: { name: string }) {
  const config: Record<string, { label: string; color: string; icon: string }> = {
    HERMES:    { label: "Hermes",    color: "#0891B2", icon: "◉" },
    ARCHITECT: { label: "Architect", color: "#7C3AED", icon: "◈" },
    ORACLE:    { label: "Oracle",    color: "#8B5CF6", icon: "◆" },
    FORGE:     { label: "Forge",     color: "#16A34A", icon: "⚙" },
    SENTINEL:  { label: "Sentinel",  color: "#E11D48", icon: "🛡" },
    TERMINUS:  { label: "Terminus",  color: "#F59E0B", icon: "▶" },
    HERALD:    { label: "Herald",    color: "#FF9F45", icon: "★" },
  };
  const cfg = config[name] || { label: name, color: "#7C3AED", icon: "●" };

  return (
    <div className="bg-white border border-surface-border rounded-xl p-3 opacity-70 shadow-soft">
      <div className="flex items-center gap-2 mb-2">
        <span className="text-base w-7 h-7 rounded-lg bg-brand-purple/10 flex items-center justify-center" style={{ color: cfg.color }}>
          {cfg.icon}
        </span>
        <span className="text-xs font-bold text-ink-soft">{cfg.label}</span>
        <span className="w-2 h-2 rounded-full bg-gray-300 ml-auto" />
      </div>
      <div className="w-full h-1.5 bg-surface-border/60 rounded-full overflow-hidden">
        <div className="h-full w-0 rounded-full shimmer" />
      </div>
      <p className="text-[10px] text-ink-muted mt-1">Waiting for connection...</p>
    </div>
  );
}
