import type { AgentLiveStatus } from "../types";

interface AgentActivityPanelProps {
  agents: AgentLiveStatus[];
  eventCount: number;
  onNavigate?: (path: string) => void;
}

const AGENT_ORDER = ["HERMES", "ARCHITECT", "ORACLE", "FORGE", "SENTINEL", "TERMINUS", "HERALD"];

export function AgentActivityPanel({ agents, eventCount, onNavigate }: AgentActivityPanelProps) {
  const sorted = AGENT_ORDER
    .map((name) => agents.find((a) => a.name.toUpperCase() === name))
    .filter(Boolean) as AgentLiveStatus[];

  return (
    <div className="w-72 border-l border-surface-border bg-surface-alt flex flex-col shrink-0 overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-surface-border">
        <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider">Agent Activity</h3>
        <p className="text-[10px] text-gray-600 mt-0.5">{eventCount} events in this session</p>
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
        <div className="flex items-center gap-3 text-[9px] text-gray-600">
          <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-accent-green" /> Idle</span>
          <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-accent-amber animate-pulse" /> Acting</span>
          <span className="flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-accent-blue" /> Thinking</span>
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
    agent.status === "idle" ? "#52627f" :
    agent.status === "thinking" ? "#3b82f6" :
    agent.status === "acting" ? "#f7b731" :
    "#00e38c";

  const statusPulse = agent.status === "acting" || agent.status === "thinking" ? "animate-pulse" : "";

  return (
    <div className="bg-surface border border-surface-border rounded-lg p-3 transition-colors hover:border-gray-600">
      {/* Header row */}
      <div className="flex items-center gap-2 mb-2">
        <span className="text-base" style={{ color: agent.color }}>{agent.icon}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5">
            <span className="text-xs font-bold text-gray-300">{agent.label}</span>
            <span className="text-[9px] font-mono text-gray-600">{agent.name}</span>
          </div>
        </div>
        <span className={`w-2 h-2 rounded-full ${statusPulse}`} style={{ backgroundColor: statusColor }} />
      </div>

      {/* Status bar */}
      <div className="mb-1.5">
        <div className="flex items-center justify-between text-[10px] mb-0.5">
          <span className="text-gray-500">{agent.currentTask || "Waiting..."}</span>
          <span className="text-gray-600">{Math.round(agent.progress * 100)}%</span>
        </div>
        <div className="w-full h-1 bg-surface-border rounded-full overflow-hidden">
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
        <p className="text-[10px] text-gray-600 truncate">{agent.lastAction}</p>
      )}
    </div>
  );
}

/* ── Skeleton (placeholder when no data) ─────────────────── */

function AgentCardSkeleton({ name }: { name: string }) {
  const config: Record<string, { label: string; color: string; icon: string }> = {
    HERMES:    { label: "Hermes",    color: "#39c8ff", icon: "◉" },
    ARCHITECT: { label: "Architect", color: "#3b82f6", icon: "◈" },
    ORACLE:    { label: "Oracle",    color: "#8c5cff", icon: "◆" },
    FORGE:     { label: "Forge",     color: "#00e38c", icon: "⚙" },
    SENTINEL:  { label: "Sentinel",  color: "#ff5c7a", icon: "🛡" },
    TERMINUS:  { label: "Terminus",  color: "#f7b731", icon: "▶" },
    HERALD:    { label: "Herald",    color: "#19f5a5", icon: "★" },
  };
  const cfg = config[name] || { label: name, color: "#52627f", icon: "●" };

  return (
    <div className="bg-surface border border-surface-border rounded-lg p-3 opacity-60">
      <div className="flex items-center gap-2 mb-2">
        <span className="text-base" style={{ color: cfg.color }}>{cfg.icon}</span>
        <span className="text-xs font-bold text-gray-500">{cfg.label}</span>
        <span className="w-2 h-2 rounded-full bg-gray-700 ml-auto" />
      </div>
      <div className="w-full h-1 bg-surface-border rounded-full overflow-hidden">
        <div className="h-full w-0 rounded-full transition-all" />
      </div>
      <p className="text-[10px] text-gray-700 mt-1">Waiting for connection...</p>
    </div>
  );
}
