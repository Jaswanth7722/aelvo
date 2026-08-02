import { useState } from "react";
import type { ChatMessage, AgentStep, VerificationStepStatus } from "../types";

interface ChatMessageProps {
  message: ChatMessage;
}

/** Format a timestamp relative to now */
function fmtRel(ts: number): string {
  const diff = Date.now() / 1000 - ts;
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

const AGENT_CONFIG: Record<string, { label: string; color: string; icon: string }> = {
  HERMES:    { label: "Hermes",    color: "#0891B2", icon: "◉" },
  ARCHITECT: { label: "Architect", color: "#7C3AED", icon: "◈" },
  ORACLE:    { label: "Oracle",    color: "#8B5CF6", icon: "◆" },
  FORGE:     { label: "Forge",     color: "#16A34A", icon: "⚙" },
  SENTINEL:  { label: "Sentinel",  color: "#E11D48", icon: "🛡" },
  TERMINUS:  { label: "Terminus",  color: "#F59E0B", icon: "▶" },
  HERALD:    { label: "Herald",    color: "#FF9F45", icon: "★" },
};

function getAgentCfg(name: string) {
  const key = name.toUpperCase();
  return AGENT_CONFIG[key] || { label: name, color: "#7C3AED", icon: "●" };
}

function AgentStepRow({ step }: { step: AgentStep }) {
  const cfg = getAgentCfg(step.agent);
  const statusIcon =
    step.status === "completed" ? "✓" :
    step.status === "failed" ? "✗" :
    step.status === "active" ? "◌" : "○";
  const statusColor =
    step.status === "completed" ? cfg.color :
    step.status === "failed" ? "#E11D48" :
    step.status === "active" ? "#F59E0B" : "#9CA3AF";

  return (
    <div className="flex items-center gap-2 py-1.5 fade-up">
      <span className="text-sm shrink-0" style={{ color: cfg.color }}>{cfg.icon}</span>
      <span className="text-xs font-semibold shrink-0" style={{ color: cfg.color }}>
        {cfg.label}
      </span>
      <span className="text-xs" style={{ color: statusColor }}>
        {statusIcon} {step.action}
      </span>
      <span className="text-[10px] text-ink-muted ml-auto">
        {fmtRel(step.timestamp)}
      </span>
    </div>
  );
}

function VerificationBadge({ check }: { check: VerificationStepStatus }) {
  const color =
    check.status === "passed" ? "#16A34A" :
    check.status === "failed" ? "#E11D48" :
    check.status === "running" ? "#F59E0B" : "#9CA3AF";
  const icon =
    check.status === "passed" ? "✓" :
    check.status === "failed" ? "✗" :
    check.status === "running" ? "◌" : "○";

  return (
    <span
      className="inline-flex items-center gap-1 text-[10px] px-2 py-0.5 rounded-full border"
      style={{ color, backgroundColor: `${color}12`, borderColor: `${color}30` }}
    >
      <span>{icon}</span>
      <span>{check.check}</span>
      {check.details && <span className="text-ink-muted">— {check.details}</span>}
    </span>
  );
}

function PhaseTimeline({ phases }: { phases: ChatMessage["phases"] }) {
  if (!phases || phases.length === 0) return null;

  return (
    <div className="flex items-center gap-1 my-2 flex-wrap">
      {phases.map((phase, i) => {
        const color =
          phase.status === "completed" ? "#16A34A" :
          phase.status === "failed" ? "#E11D48" :
          phase.status === "active" ? "#FF9F45" : "#9CA3AF";
        return (
          <span
            key={i}
            className="text-[9px] px-1.5 py-0.5 rounded border font-medium"
            style={{ color, backgroundColor: `${color}12`, borderColor: `${color}25` }}
          >
            {phase.status === "completed" ? "✓ " : phase.status === "active" ? "◌ " : ""}
            {phase.name}
          </span>
        );
      })}
    </div>
  );
}

export function ChatMessageBubble({ message }: ChatMessageProps) {
  const [expanded, setExpanded] = useState(false);

  const isUser = message.role === "user";
  const isSystem = message.role === "system";

  return (
    <div className={`flex ${isUser ? "justify-end" : "justify-start"} mb-4 fade-up`}>
      <div
        className={`max-w-[75%] min-w-0 rounded-2xl px-4 py-3 shadow-soft ${
          isUser
            ? "bg-gradient-to-br from-brand-orange to-brand-deep text-white"
            : isSystem
              ? "bg-surface-alt border border-surface-border text-ink"
              : "bg-white border border-surface-border text-ink"
        }`}
      >
        {/* Header for assistant messages */}
        {!isUser && (
          <div className="flex items-center gap-2 mb-2">
            {message.agentSteps && message.agentSteps.length > 0 && (
              <>
                {/* Show first agent icon */}
                <span
                  className="text-sm w-6 h-6 rounded-lg bg-brand-purple/10 flex items-center justify-center"
                  style={{ color: getAgentCfg(message.agentSteps[0].agent).color }}
                >
                  {getAgentCfg(message.agentSteps[0].agent).icon}
                </span>
                <span className="text-xs font-bold text-gradient">AELVO</span>
                {message.streaming && (
                  <span className="flex gap-0.5">
                    <span className="w-1.5 h-1.5 rounded-full bg-brand-orange animate-bounce" style={{ animationDelay: "0ms" }} />
                    <span className="w-1.5 h-1.5 rounded-full bg-brand-orange animate-bounce" style={{ animationDelay: "150ms" }} />
                    <span className="w-1.5 h-1.5 rounded-full bg-brand-orange animate-bounce" style={{ animationDelay: "300ms" }} />
                  </span>
                )}
              </>
            )}
            <span className="text-[10px] text-ink-muted ml-auto">{fmtRel(message.timestamp)}</span>
          </div>
        )}

        {/* System label */}
        {isSystem && (
          <div className="text-[10px] text-ink-muted uppercase tracking-wider mb-1 font-semibold">
            System
          </div>
        )}

        {/* User label */}
        {isUser && (
          <div className="flex items-center justify-between mb-1">
            <span className="text-[10px] uppercase tracking-wider opacity-80 font-semibold">You</span>
            <span className="text-[10px] opacity-70">{fmtRel(message.timestamp)}</span>
          </div>
        )}

        {/* Content */}
        <div className={`text-sm leading-relaxed whitespace-pre-wrap break-words ${isUser ? "text-white" : "text-ink"}`}>
          {message.streamedContent || message.content}
        </div>

        {/* Phase timeline */}
        {!isUser && message.phases && message.phases.length > 0 && (
          <PhaseTimeline phases={message.phases} />
        )}

        {/* Agent steps (collapsible) */}
        {!isUser && message.agentSteps && message.agentSteps.length > 0 && (
          <div className="mt-2 pt-2 border-t border-surface-border/60">
            <button
              onClick={() => setExpanded(!expanded)}
              className="text-[10px] text-ink-muted hover:text-brand-deep transition-colors flex items-center gap-1 font-medium"
            >
              <span>{expanded ? "▼" : "▶"}</span>
              <span>{message.agentSteps.length} specialist steps</span>
            </button>
            {expanded && (
              <div className="mt-1.5 space-y-0.5">
                {message.agentSteps.map((step, i) => (
                  <AgentStepRow key={i} step={step} />
                ))}
              </div>
            )}
          </div>
        )}

        {/* Verification summary */}
        {!isUser && message.verificationSummary && message.verificationSummary.length > 0 && (
          <div className="mt-2 flex flex-wrap gap-1">
            {message.verificationSummary.map((check, i) => (
              <VerificationBadge key={i} check={check} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
