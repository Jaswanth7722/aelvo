import { useState, useRef, useEffect, useMemo, useCallback } from "react";
import type { UIEvent, ChatMessage, ChatSession, AgentLiveStatus, AgentStep, VerificationStepStatus, ChatPhase } from "../types";
import { ChatMessageBubble } from "./ChatMessage";
import { SessionSidebar } from "./SessionSidebar";
import { AgentActivityPanel } from "./AgentActivityPanel";

interface ChatWorkspaceProps {
  events: UIEvent[];
  connectionStatus: string;
}

const AGENT_NAMES = ["HERMES", "ARCHITECT", "ORACLE", "FORGE", "SENTINEL", "TERMINUS", "HERALD"];

const AGENT_DISPLAY: Record<string, { label: string; color: string; icon: string }> = {
  HERMES:    { label: "Hermes",    color: "#39c8ff", icon: "◉" },
  ARCHITECT: { label: "Architect", color: "#3b82f6", icon: "◈" },
  ORACLE:    { label: "Oracle",    color: "#8c5cff", icon: "◆" },
  FORGE:     { label: "Forge",     color: "#00e38c", icon: "⚙" },
  SENTINEL:  { label: "Sentinel",  color: "#ff5c7a", icon: "🛡" },
  TERMINUS:  { label: "Terminus",  color: "#f7b731", icon: "▶" },
  HERALD:    { label: "Herald",    color: "#19f5a5", icon: "★" },
};

let sessionCounter = 0;
let msgCounter = 0;

function genId(prefix: string): string {
  return `${prefix}_${Date.now()}_${++msgCounter}`;
}

export function ChatWorkspace({ events, connectionStatus }: ChatWorkspaceProps) {
  const [sessions, setSessions] = useState<ChatSession[]>(() => {
    const id = `session_${Date.now()}`;
    sessionCounter = 1;
    return [{
      id,
      title: "New Conversation",
      createdAt: Date.now() / 1000,
      updatedAt: Date.now() / 1000,
      messageCount: 0,
      pinned: false,
    }];
  });
  const [activeSessionId, setActiveSessionId] = useState(sessions[0]?.id ?? null);

  // Messages per session
  const [messagesBySession, setMessagesBySession] = useState<Record<string, ChatMessage[]>>(() => {
    const init: Record<string, ChatMessage[]> = {};
    for (const s of sessions) init[s.id] = [];
    return init;
  });

  // Input state
  const [input, setInput] = useState("");
  const [isProcessing, setIsProcessing] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Active session's messages
  const activeMessages = activeSessionId ? messagesBySession[activeSessionId] ?? [] : [];

  // Derive agent live status from events
  const agentStatuses = useMemo((): AgentLiveStatus[] => {
    return AGENT_NAMES.map((name) => {
      const cfg = AGENT_DISPLAY[name];
      const agentEvents = events.filter(
        (e) => e.specialist?.toUpperCase() === name
      );
      const lastEvent = agentEvents[agentEvents.length - 1];
      const recentCount = agentEvents.length;

      let status: AgentLiveStatus["status"] = "idle";
      let currentTask = "Waiting...";
      let progress = 0;

      if (lastEvent) {
        const type = lastEvent.type || "";
        if (type.includes("thinking") || type.includes("running") || type.includes("started")) {
          status = "thinking";
          progress = 0.3;
        } else if (type.includes("action") || type.includes("executing") || type.includes("working")) {
          status = "acting";
          progress = 0.6;
        } else if (type.includes("completed") || type.includes("passed") || type.includes("succeeded")) {
          status = "done";
          progress = 1;
        } else if (type.includes("failed") || type.includes("error")) {
          status = "done";
          progress = 0;
        }

        currentTask = lastEvent.action || "Working...";
        if (lastEvent.action && recentCount > 1) {
          progress = Math.min(1, progress + recentCount * 0.05);
        }
      }

      return {
        name,
        label: cfg.label,
        color: cfg.color,
        icon: cfg.icon,
        status,
        currentTask,
        progress: Math.min(1, Math.max(0, progress)),
        lastAction: lastEvent?.action || "",
      };
    });
  }, [events]);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [activeMessages.length]);

  // Update session message counts
  useEffect(() => {
    setSessions((prev) =>
      prev.map((s) => {
        const count = messagesBySession[s.id]?.length ?? 0;
        return s.id === activeSessionId
          ? { ...s, messageCount: count, updatedAt: Date.now() / 1000 }
          : { ...s, messageCount: count };
      })
    );
  }, [messagesBySession, activeSessionId]);

  // Update session title from first message
  const updateSessionTitle = useCallback((sessionId: string, firstMsg: string) => {
    setSessions((prev) =>
      prev.map((s) =>
        s.id === sessionId
          ? { ...s, title: firstMsg.length > 60 ? firstMsg.slice(0, 57) + "..." : firstMsg }
          : s
      )
    );
  }, []);

  const createNewSession = useCallback(() => {
    const id = `session_${Date.now()}_${++sessionCounter}`;
    const now = Date.now() / 1000;
    setSessions((prev) => [
      ...prev,
      { id, title: "New Conversation", createdAt: now, updatedAt: now, messageCount: 0, pinned: false },
    ]);
    setMessagesBySession((prev) => ({ ...prev, [id]: [] }));
    setActiveSessionId(id);
  }, []);

  const deleteSession = useCallback((id: string) => {
    setSessions((prev) => {
      const next = prev.filter((s) => s.id !== id);
      if (activeSessionId === id && next.length > 0) {
        setActiveSessionId(next[0].id);
      } else if (next.length === 0) {
        // Re-create a default session
        const now = Date.now() / 1000;
        const newId = `session_${Date.now()}_${++sessionCounter}`;
        setActiveSessionId(newId);
        setMessagesBySession((m) => ({ ...m, [newId]: [] }));
        return [{ id: newId, title: "New Conversation", createdAt: now, updatedAt: now, messageCount: 0, pinned: false }];
      }
      return next;
    });
    setMessagesBySession((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
  }, [activeSessionId]);

  const togglePin = useCallback((id: string) => {
    setSessions((prev) =>
      prev.map((s) => (s.id === id ? { ...s, pinned: !s.pinned } : s))
    );
  }, []);

  // Add a message to the active session
  const addMessage = useCallback((msg: ChatMessage) => {
    setMessagesBySession((prev) => {
      const sid = activeSessionId;
      if (!sid || !prev[sid]) return prev;
      return {
        ...prev,
        [sid]: [...prev[sid], msg],
      };
    });
  }, [activeSessionId]);

  // Simulate processing a message through the AELVO pipeline
  const processMessage = useCallback(async (userMsg: string) => {
    if (!activeSessionId || isProcessing) return;
    setIsProcessing(true);

    const now = Date.now() / 1000;

    // Add user message
    const userChatMsg: ChatMessage = {
      id: genId("user"),
      role: "user",
      content: userMsg,
      timestamp: now,
    };
    addMessage(userChatMsg);
    updateSessionTitle(activeSessionId, userMsg);

    // Build the pipeline phases
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

    // Create the streaming assistant message placeholder
    const assistantId = genId("assistant");
    const assistantMsg: ChatMessage = {
      id: assistantId,
      role: "assistant",
      content: "",
      timestamp: now + 0.5,
      agentSteps: steps,
      verificationSummary: verifications,
      phases,
      streaming: true,
      streamedContent: "",
    };
    addMessage(assistantMsg);

    // Simulate the pipeline with delays
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

      // Update phase status
      phases[i] = { ...phases[i], status: "active" };
      steps.push({
        agent: p.agent,
        action: p.action,
        status: "active",
        timestamp: stepStart,
      });

      // Update the assistant message with this step active
      setMessagesBySession((prev) => {
        const sid = activeSessionId;
        if (!sid || !prev[sid]) return prev;
        return {
          ...prev,
          [sid]: prev[sid].map((m) =>
            m.id === assistantId
              ? {
                  ...m,
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
                }
              : m
          ),
        };
      });

      await new Promise((r) => setTimeout(r, 800));

      // Mark step as completed
      steps[steps.length - 1] = { ...steps[steps.length - 1], status: "completed" };
      phases[i] = { ...phases[i], status: "completed" };

      setMessagesBySession((prev) => {
        const sid = activeSessionId;
        if (!sid || !prev[sid]) return prev;
        return {
          ...prev,
          [sid]: prev[sid].map((m) =>
            m.id === assistantId
              ? {
                  ...m,
                  agentSteps: [...steps],
                  phases: [...phases],
                  streamedContent: responseTexts.slice(0, i + 2).join(""),
                }
              : m
          ),
        };
      });
    }

    // Finalize message
    const finalVerifications: VerificationStepStatus[] = [
      { check: "Lint", status: "passed", details: "Clean" },
      { check: "Typecheck", status: "passed", details: "Clean" },
      { check: "Security Scan", status: "passed", details: "No vulnerabilities" },
    ];

    setMessagesBySession((prev) => {
      const sid = activeSessionId;
      if (!sid || !prev[sid]) return prev;
      return {
        ...prev,
        [sid]: prev[sid].map((m) =>
          m.id === assistantId
            ? {
                ...m,
                content: responseTexts.join(""),
                streamedContent: undefined,
                streaming: false,
                verificationSummary: finalVerifications,
              }
            : m
        ),
      };
    });

    setIsProcessing(false);
  }, [activeSessionId, isProcessing, addMessage, updateSessionTitle]);

  const handleSend = useCallback(async () => {
    const text = input.trim();
    if (!text || isProcessing) return;
    setInput("");
    await processMessage(text);
  }, [input, isProcessing, processMessage]);

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
      {/* Left sidebar — Sessions */}
      <SessionSidebar
        sessions={sessions}
        activeSessionId={activeSessionId}
        onSelectSession={setActiveSessionId}
        onNewSession={createNewSession}
        onTogglePin={togglePin}
        onDeleteSession={deleteSession}
      />

      {/* Center — Chat */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Top bar */}
        <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
          <div className="flex items-center gap-4">
            <h2 className="text-lg font-bold text-gray-200">Chat</h2>
            <span className="text-xs text-gray-600">
              {sessions.find((s) => s.id === activeSessionId)?.title || "Conversation"}
            </span>
          </div>
          <div className="flex items-center gap-2 text-xs">
            <span
              className={`w-2 h-2 rounded-full ${
                connectionStatus === "connected"
                  ? "bg-accent-green"
                  : connectionStatus === "connecting"
                    ? "bg-accent-amber animate-pulse"
                    : "bg-accent-red"
              }`}
            />
            <span className="text-gray-500">{connectionStatus}</span>
          </div>
        </header>

        {/* Message area */}
        <div className="flex-1 overflow-y-auto px-6 py-4">
          {activeMessages.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              <div className="text-center max-w-lg">
                <div className="text-4xl mb-4 text-gray-700">◈</div>
                <h3 className="text-lg font-bold text-gray-300 mb-2">AELVO Chat Workspace</h3>
                <p className="text-sm text-gray-500 leading-relaxed mb-6">
                  This is the primary interface for interacting with AELVO's multi-agent engineering system.
                  Type a task below to begin — the 7 specialists will collaborate to complete it.
                </p>
                <div className="flex flex-wrap justify-center gap-2">
                  {AGENT_NAMES.map((name) => {
                    const cfg = AGENT_DISPLAY[name];
                    return (
                      <span
                        key={name}
                        className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full border border-surface-border"
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
                      onClick={() => {
                        setInput(suggestion);
                      }}
                      className="text-[10px] text-gray-500 hover:text-gray-300 hover:border-gray-600 bg-surface border border-surface-border rounded-lg px-2.5 py-2 text-left transition-colors leading-relaxed"
                    >
                      {suggestion}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          ) : (
            <div className="max-w-4xl mx-auto">
              {activeMessages.map((msg) => (
                <ChatMessageBubble key={msg.id} message={msg} />
              ))}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        {/* Input area */}
        <div className="border-t border-surface-border px-6 py-4 shrink-0">
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
                  className="w-full bg-surface border border-surface-border rounded-xl px-4 py-3 text-sm text-gray-200 placeholder-gray-600 focus:outline-none focus:border-accent-blue/50 transition-colors resize-none disabled:opacity-50"
                />
                {/* Attachment button */}
                <button
                  className="absolute right-3 bottom-3 text-gray-600 hover:text-gray-400 transition-colors text-sm"
                  title="Attach context"
                >
                  📎
                </button>
              </div>
              <button
                onClick={handleSend}
                disabled={isProcessing || !input.trim()}
                className="bg-accent-blue hover:bg-accent-blue/80 disabled:bg-gray-700 disabled:text-gray-500 text-white rounded-xl px-5 py-3 text-sm font-medium transition-colors duration-150 flex items-center gap-2 shrink-0"
              >
                {isProcessing ? (
                  <>
                    <span className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin" />
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
            <p className="text-[10px] text-gray-700 mt-2">
              AELVO uses 7 specialists (Hermes → Architect → Oracle → Forge → Sentinel → Terminus → Herald) to complete your request collaboratively.
            </p>
          </div>
        </div>
      </div>

      {/* Right sidebar — Agent Activity */}
      <AgentActivityPanel agents={agentStatuses} eventCount={events.length} />
    </div>
  );
}
