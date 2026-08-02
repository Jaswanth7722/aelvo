import { useState, useRef, useEffect, useCallback } from "react";
import type { UIEvent, ChatMessage, AgentStep, VerificationStepStatus, ChatPhase } from "../types";
import { ChatMessageBubble } from "./ChatMessage";

interface ChatWorkspaceProps {
  events: UIEvent[];
  connectionStatus: string;
  sendMessage?: (message: string) => boolean;
}

const AGENT_NAMES = ["HERMES", "ARCHITECT", "ORACLE", "FORGE", "SENTINEL", "TERMINUS", "HERALD"];

const AGENT_DISPLAY: Record<string, { label: string; color: string; icon: string }> = {
  HERMES:    { label: "Hermes",    color: "#0891B2", icon: "◉" },
  ARCHITECT: { label: "Architect", color: "#7C3AED", icon: "◈" },
  ORACLE:    { label: "Oracle",    color: "#8B5CF6", icon: "◆" },
  FORGE:     { label: "Forge",     color: "#16A34A", icon: "⚙" },
  SENTINEL:  { label: "Sentinel",  color: "#E11D48", icon: "🛡" },
  TERMINUS:  { label: "Terminus",  color: "#F59E0B", icon: "▶" },
  HERALD:    { label: "Herald",    color: "#FF9F45", icon: "★" },
};

let msgCounter = 0;

function genId(prefix: string): string {
  return `${prefix}_${Date.now()}_${++msgCounter}`;
}

export function ChatWorkspace({ events, connectionStatus, sendMessage }: ChatWorkspaceProps) {
  // Single continuous conversation
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [isProcessing, setIsProcessing] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Track the assistant message currently waiting on a real agent_response
  const pendingAssistantRef = useRef<{ messageId: string } | null>(null);
  // Track the last event count we processed for the real-response watcher
  const lastEventCountRef = useRef(0);
  // Timeout guard so a missing agent_response can never wedge the input
  const pendingTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages.length]);

  const addMessage = useCallback((msg: ChatMessage) => {
    setMessages((prev) => [...prev, msg]);
  }, []);

  const patchMessage = useCallback((messageId: string, patch: Partial<ChatMessage>) => {
    setMessages((prev) =>
      prev.map((m) => (m.id === messageId ? { ...m, ...patch } : m))
    );
  }, []);

  // ── Watch for real agent_response events and render them ──────────────
  useEffect(() => {
    const pending = pendingAssistantRef.current;
    if (!pending) return;
    if (events.length <= lastEventCountRef.current) return;

    const fresh = events.slice(lastEventCountRef.current);
    lastEventCountRef.current = events.length;

    for (const ev of fresh) {
      if (ev.type === "agent_response" || ev.type === "agent_error") {
        if (ev.type === "agent_response") {
          patchMessage(pending.messageId, {
            content: ev.action || "(no output)",
            streamedContent: undefined,
            streaming: false,
          });
        } else {
          patchMessage(pending.messageId, {
            content: `⚠️ ${ev.action || "Agent error"}`,
            streamedContent: undefined,
            streaming: false,
          });
        }
        pendingAssistantRef.current = null;
        if (pendingTimeoutRef.current) {
          clearTimeout(pendingTimeoutRef.current);
          pendingTimeoutRef.current = null;
        }
        setIsProcessing(false);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [events, patchMessage]);

  // ── Simulate processing a message through the AELVO pipeline ───────────
  const simulatePipeline = useCallback(async (userMsg: string, assistantId: string) => {
    const now = Date.now() / 1000;

    const pipeline: Array<{ agent: string; name: string; action: string }> = [
      { agent: "HERMES",    name: "Calibration",     action: "Calibrating request..." },
      { agent: "ARCHITECT", name: "Planning",        action: "Creating execution plan..." },
      { agent: "ORACLE",    name: "Research",        action: "Researching context..." },
      { agent: "FORGE",     name: "Implementation",  action: "Implementing solution..." },
      { agent: "SENTINEL",  name: "Security Review", action: "Reviewing for vulnerabilities..." },
      { agent: "TERMINUS",  name: "Execution",       action: "Executing commands..." },
      { agent: "HERALD",    name: "Reporting",       action: "Generating response..." },
    ];

    const phases: ChatPhase[] = pipeline.map((p, i) => ({
      name: p.name,
      specialist: p.agent,
      status: "pending" as const,
      timestamp: now + i * 1,
    }));

    const steps: AgentStep[] = [];
    const verifications: VerificationStepStatus[] = [
      { check: "Lint", status: "pending" },
      { check: "Typecheck", status: "pending" },
      { check: "Security Scan", status: "pending" },
    ];

    const responseTexts = [
      `I've analyzed your request. Let me work through this systematically using the AELVO multi-agent pipeline.`,
      `\n\n**Hermes** has calibrated the request and identified the key requirements.`,
      `\n\n**Architect** has created a strategic plan with the optimal execution phases.`,
      `\n\n**Oracle** has gathered relevant context and cross-referenced available information.`,
      `\n\n**Forge** is implementing the solution with proper patterns and conventions.`,
      `\n\n**Sentinel** has completed security review — all checks passed with no vulnerabilities detected.`,
      `\n\n**Terminus** executed all commands successfully.`,
      `\n\n**Herald** synthesized the results. Here's the summary:\n\n✅ Task complete. All specialists contributed collaboratively through the shared blackboard, with consensus-driven decision making and verification-driven execution.`,
    ];

    const totalSteps = pipeline.length;

    for (let i = 0; i < totalSteps; i++) {
      const p = pipeline[i];
      const stepStart = now + 1 + i * 1.5;

      phases[i] = { ...phases[i], status: "active" };
      steps.push({
        agent: p.agent,
        action: p.action,
        status: "active",
        timestamp: stepStart,
      });

      patchMessage(assistantId, {
        agentSteps: [...steps],
        phases: [...phases],
        streamedContent: responseTexts.slice(0, i + 2).join(""),
        verificationSummary: i >= 4
          ? verifications.map((v, vi) =>
              vi <= i - 4
                ? { ...v, status: "passed" as const, details: vi === 2 ? "No vulnerabilities" : "Clean" }
                : { ...v, status: vi === i - 4 ? "running" as const : "pending" as const }
            )
          : verifications,
      });

      await new Promise((r) => setTimeout(r, 600));

      steps[steps.length - 1] = { ...steps[steps.length - 1], status: "completed" };
      phases[i] = { ...phases[i], status: "completed" };

      patchMessage(assistantId, {
        agentSteps: [...steps],
        phases: [...phases],
        streamedContent: responseTexts.slice(0, i + 2).join(""),
      });
    }

    const finalVerifications: VerificationStepStatus[] = [
      { check: "Lint", status: "passed", details: "Clean" },
      { check: "Typecheck", status: "passed", details: "Clean" },
      { check: "Security Scan", status: "passed", details: "No vulnerabilities" },
    ];

    patchMessage(assistantId, {
      content: responseTexts.join(""),
      streamedContent: undefined,
      streaming: false,
      verificationSummary: finalVerifications,
    });
  }, [patchMessage]);

  const handleSend = useCallback(async () => {
    const text = input.trim();
    if (!text || isProcessing) return;
    setInput("");

    const now = Date.now() / 1000;

    // Add user message
    const userChatMsg: ChatMessage = {
      id: genId("user"),
      role: "user",
      content: text,
      timestamp: now,
    };
    addMessage(userChatMsg);

    const assistantId = genId("assistant");
    const assistantMsg: ChatMessage = {
      id: assistantId,
      role: "assistant",
      content: "",
      timestamp: now + 0.5,
      agentSteps: [],
      verificationSummary: [],
      phases: [],
      streaming: true,
      streamedContent: "Thinking…",
    };
    addMessage(assistantMsg);
    setIsProcessing(true);

    // Real agent path — send over the WebSocket bridge
    if (sendMessage && sendMessage(text)) {
      lastEventCountRef.current = events.length;
      pendingAssistantRef.current = { messageId: assistantId };
      // Safety net: if the backend never replies, drop the pending state
      // so the user can send again (message stays as "Thinking…").
      if (pendingTimeoutRef.current) clearTimeout(pendingTimeoutRef.current);
      pendingTimeoutRef.current = setTimeout(() => {
        if (pendingAssistantRef.current) {
          pendingAssistantRef.current = null;
          setIsProcessing(false);
        }
      }, 120000);
      return; // the events watcher completes the message
    }

    // Offline fallback — simulate the pipeline
    await simulatePipeline(text, assistantId);
    setIsProcessing(false);
  }, [input, isProcessing, addMessage, sendMessage, simulatePipeline, events.length]);

  // Clear any pending real-response timeout on unmount
  useEffect(() => {
    return () => {
      if (pendingTimeoutRef.current) {
        clearTimeout(pendingTimeoutRef.current);
        pendingTimeoutRef.current = null;
      }
    };
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    },
    [handleSend]
  );

  return (
    <div className="flex-1 flex overflow-hidden">
      {/* Full-width chat (no sidebars) */}
      <div className="flex-1 flex flex-col overflow-hidden bg-[#FFFBF4]">
        {/* Top bar */}
        <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0 bg-white/70 backdrop-blur-md">
          <h2 className="text-lg font-extrabold text-ink">Chat</h2>
          <div className="flex items-center gap-2 text-xs">
            <span
              className={`w-2 h-2 rounded-full ${
                connectionStatus === "connected"
                  ? "bg-accent-green animate-pulse-glow"
                  : connectionStatus === "connecting"
                    ? "bg-accent-amber animate-pulse"
                    : "bg-accent-red"
              }`}
            />
            <span className="text-ink-soft font-medium capitalize">{connectionStatus}</span>
            {connectionStatus !== "connected" && (
              <span className="text-[10px] text-ink-muted">(demo mode)</span>
            )}
          </div>
        </header>

        {/* Message area */}
        <div className="flex-1 overflow-y-auto px-6 py-4">
          {messages.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              <div className="text-center max-w-lg">
                <div className="text-5xl mb-4 animate-float">◈</div>
                <h3 className="text-xl font-extrabold text-gradient mb-2">AELVO Chat Workspace</h3>
                <p className="text-sm text-ink-soft leading-relaxed mb-6">
                  The primary interface for the AELVO multi-agent engineering system.
                  Type a task below — the 7 specialists collaborate to complete it.
                </p>
                <div className="flex flex-wrap justify-center gap-2">
                  {AGENT_NAMES.map((name) => {
                    const cfg = AGENT_DISPLAY[name];
                    return (
                      <span
                        key={name}
                        className="chip"
                        style={{ borderColor: `${cfg.color}30` }}
                      >
                        <span style={{ color: cfg.color }}>{cfg.icon}</span>
                        <span style={{ color: cfg.color }}>{cfg.label}</span>
                      </span>
                    );
                  })}
                </div>
                <div className="mt-6 grid grid-cols-2 gap-2 max-w-sm mx-auto">
                  {[
                    "Refactor the auth module to use async sessions",
                    "Add rate limiting to the API endpoints",
                    "Create a new React component for user profiles",
                    "Fix the race condition in worker_pool.py",
                  ].map((suggestion) => (
                    <button
                      key={suggestion}
                      onClick={() => { setInput(suggestion); }}
                      className="text-[10px] text-ink-soft hover:text-brand-deep hover:border-brand-orange/50 bg-white border border-surface-border rounded-lg px-2.5 py-2 text-left transition-all duration-150 shadow-soft hover:shadow-card hover:-translate-y-0.5 leading-relaxed"
                    >
                      {suggestion}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          ) : (
            <div className="max-w-4xl mx-auto">
              {messages.map((msg) => (
                <ChatMessageBubble key={msg.id} message={msg} />
              ))}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        {/* Input area */}
        <div className="border-t border-surface-border px-6 py-4 shrink-0 bg-white/70 backdrop-blur-md">
          <div className="max-w-4xl mx-auto">
            <div className="flex items-end gap-3">
              <div className="flex-1 relative">
                <textarea
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  onKeyDown={handleKeyDown}
                  placeholder={
                    isProcessing
                      ? "AELVO is processing your request..."
                      : "Ask AELVO to build, refactor, research, or analyze..."
                  }
                  disabled={isProcessing}
                  rows={2}
                  className="input-field"
                />
                {/* Attachment button */}
                <button
                  className="absolute right-3 bottom-3 text-ink-muted hover:text-brand-orange transition-colors text-sm"
                  title="Attach context"
                >
                  📎
                </button>
              </div>
              <button
                onClick={handleSend}
                disabled={isProcessing || !input.trim()}
                className="btn-primary"
              >
                {isProcessing ? (
                  <>
                    <span className="w-3 h-3 border-2 border-white/40 border-t-white rounded-full animate-spin" />
                    Working...
                  </>
                ) : (
                  <>
                    Send
                    <span className="text-xs opacity-70">↵</span>
                  </>
                )}
              </button>
            </div>
            <p className="text-[10px] text-ink-muted mt-2">
              {connectionStatus === "connected"
                ? "Live agent mode — messages execute through the AELVO orchestrator."
                : "Demo mode — connect the AELVO backend to run real agent turns."}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
