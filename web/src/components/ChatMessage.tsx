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
  HERMES:    { label: "Hermes",    color: "#39c8ff", icon: "◉" },
  ARCHITECT: { label: "Architect", color: "#3b82f6", icon: "◈" },
  ORACLE:    { label: "Oracle",    color: "#8c5cff", icon: "◆" },
  FORGE:     { label: "Forge",     color: "#00e38c", icon: "⚙" },
  SENTINEL:  { label: "Sentinel",  color: "#ff5c7a", icon: "🛡" },
  TERMINUS:  { label: "Terminus",  color: "#f7b731", icon: "▶" },
  HERALD:    { label: "Herald",    color: "#19f5a5", icon: "★" },
};

function getAgentCfg(name: string) {
  const key = name.toUpperCase();
  return AGENT_CONFIG[key] || { label: name, color: "#52627f", icon: "●" };
}

function AgentStepRow({ step }: { step: AgentStep }) {
  const cfg = getAgentCfg(step.agent);
  const statusIcon =
    step.status === "completed" ? "✓" :
    step.status === "failed" ? "✗" :
    step.status === "active" ? "◌" : "○";
  const statusColor =
    step.status === "completed" ? cfg.color :
    step.status === "failed" ? "#ff5c7a" :
    step.status === "active" ? "#f7b731" : "#52627f";

  return (
    <div className="flex items-center gap-2 py-1.5">
      <span className="text-sm shrink-0" style={{ color: cfg.color }}>{cfg.icon}</span>
      <span className="text-xs font-semibold shrink-0" style={{ color: cfg.color }}>
        {cfg.label}
      </span>
      <span className="text-xs" style={{ color: statusColor }}>
        {statusIcon} {step.action}
      </span>
      <span className="text-[10px] text-gray-600 ml-auto">
        {fmtRel(step.timestamp)}
      </span>
    </div>
  );
}

function VerificationBadge({ check }: { check: VerificationStepStatus }) {
  const color =
    check.status === "passed" ? "#00e38c" :
    check.status === "failed" ? "#ff5c7a" :
    check.status === "running" ? "#f7b731" : "#52627f";
  const icon =
    check.status === "passed" ? "✓" :
    check.status === "failed" ? "✗" :
    check.status === "running" ? "◌" : "○";

  return (
    <span
      className="inline-flex items-center gap-1 text-[10px] px-2 py-0.5 rounded-full"
      style={{ color, backgroundColor: `${color}15`, borderColor: `${color}30` }}
    >
      <span>{icon}</span>
      <span>{check.check}</span>
      {check.details && <span className="text-gray-600">— {check.details}</span>}
    </span>
  );
}

function PhaseTimeline({ phases }: { phases: ChatMessage["phases"] }) {
  if (!phases || phases.length === 0) return null;

  return (
    <div className="flex items-center gap-1 my-2">
      {phases.map((phase, i) => {
        const color =
          phase.status === "completed" ? "#00e38c" :
          phase.status === "failed" ? "#ff5c7a" :
          phase.status === "active" ? "#f7b731" : "#52627f";
        return (
          <span
            key={i}
            className="text-[9px] px-1.5 py-0.5 rounded font-medium"
            style={{ color, backgroundColor: `${color}15` }}
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
    <div className={`flex ${isUser ? "justify-end" : "justify-start"} mb-4`}>
      <div
        className={`max-w-[75%] min-w-0 rounded-2xl px-4 py-3 ${
          isUser
            ? "bg-accent-blue/15 border border-accent-blue/20"
            : isSystem
              ? "bg-surface-border/30 border border-surface-border"
              : "bg-surface-alt border border-surface-border"
        }`}
      >
        {/* Header for assistant messages */}
        {!isUser && (
          <div className="flex items-center gap-2 mb-2">
            {message.agentSteps && message.agentSteps.length > 0 && (
              <>
                {/* Show first agent icon */}
                <span className="text-sm" style={{ color: getAgentCfg(message.agentSteps[0].agent).color }}>
                  {getAgentCfg(message.agentSteps[0].agent).icon}
                </span>
                <span className="text-xs font-bold text-gray-300">
                  AELVO
                </span>
                {message.streaming && (
                  <span className="flex gap-0.5">
                    <span className="w-1.5 h-1.5 rounded-full bg-accent-blue animate-bounce" style={{ animationDelay: "0ms" }} />
                    <span className="w-1.5 h-1.5 rounded-full bg-accent-blue animate-bounce" style={{ animationDelay: "150ms" }} />
                    <span className="w-1.5 h-1.5 rounded-full bg-accent-blue animate-bounce" style={{ animationDelay: "300ms" }} />
                  </span>
                )}
              </>
            )}
            <span className="text-[10px] text-gray-600 ml-auto">{fmtRel(message.timestamp)}</span>
          </div>
        )}

        {/* System label */}
        {isSystem && (
          <div className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">
            System
          </div>
        )}

        {/* User label */}
        {isUser && (
          <div className="flex items-center justify-between mb-1">
            <span className="text-[10px] text-gray-500 uppercase tracking-wider">You</span>
            <span className="text-[10px] text-gray-600">{fmtRel(message.timestamp)}</span>
          </div>
        )}

        {/* Content */}
        <div className="text-sm text-gray-200 leading-relaxed whitespace-pre-wrap break-words">
          {message.streamedContent || message.content}
        </div>

        {/* Phase timeline */}
        {!isUser && message.phases && message.phases.length > 0 && (
          <PhaseTimeline phases={message.phases} />
        )}

        {/* Agent steps (collapsible) */}
        {!isUser && message.agentSteps && message.agentSteps.length > 0 && (
          <div className="mt-2 pt-2 border-t border-surface-border/50">
            <button
              onClick={() => setExpanded(!expanded)}
              className="text-[10px] text-gray-500 hover:text-gray-300 transition-colors flex items-center gap-1"
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
