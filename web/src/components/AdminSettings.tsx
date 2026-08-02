import { useState, useRef, useEffect, useMemo, useCallback } from "react";
import type { UIEvent, ConnectionStatus } from "../types";
import { useSettings } from "../context/SettingsContext";

interface AdminSettingsProps {
  events: UIEvent[];
  connectionStatus: ConnectionStatus;
  lastEvent: UIEvent | null;
  onClearEvents: () => void;
  onReconnect: () => void;
}

type AdminTab = "connection" | "events";

export function AdminSettings({
  events,
  connectionStatus,
  lastEvent,
  onClearEvents,
  onReconnect,
}: AdminSettingsProps) {
  const { config, updateConfig, resetConfig } = useSettings();
  const [activeTab, setActiveTab] = useState<AdminTab>("connection");

  // ── Connection Settings Tab ──────────────────────────────────

  const [editUrl, setEditUrl] = useState(config.url);
  const [editReconnectDelay, setEditReconnectDelay] = useState(
    String(config.reconnectDelay)
  );
  const [editMaxEvents, setEditMaxEvents] = useState(String(config.maxEvents));
  const [saveStatus, setSaveStatus] = useState<"idle" | "saved" | "error">(
    "idle"
  );

  // Sync local state when config changes externally (e.g. reset)
  useEffect(() => {
    setEditUrl(config.url);
    setEditReconnectDelay(String(config.reconnectDelay));
    setEditMaxEvents(String(config.maxEvents));
  }, [config]);

  const handleSaveConnection = () => {
    const delay = parseInt(editReconnectDelay, 10);
    const max = parseInt(editMaxEvents, 10);

    if (Number.isNaN(delay) || delay < 500) {
      setSaveStatus("error");
      return;
    }
    if (Number.isNaN(max) || max < 10) {
      setSaveStatus("error");
      return;
    }

    updateConfig({
      url: editUrl.trim() || "ws://127.0.0.1:8765",
      reconnectDelay: delay,
      maxEvents: max,
    });
    setSaveStatus("saved");
    setTimeout(() => setSaveStatus("idle"), 2500);
  };

  const handleReset = () => {
    resetConfig();
    setSaveStatus("saved");
    setTimeout(() => setSaveStatus("idle"), 2500);
  };

  const statusBadge = {
    connected: { label: "Connected", className: "bg-emerald-600/20 text-emerald-400 border-emerald-600/30" },
    connecting: { label: "Connecting…", className: "bg-amber-600/20 text-amber-400 border-amber-600/30" },
    disconnected: { label: "Disconnected", className: "bg-red-600/20 text-red-400 border-red-600/30" },
    error: { label: "Error", className: "bg-red-600/20 text-red-400 border-red-600/30" },
  }[connectionStatus] || { label: "Unknown", className: "bg-gray-600/20 text-ink-soft border-surface-border/30" };

  // ── Raw Event Log Tab ───────────────────────────────────────

  const [searchQuery, setSearchQuery] = useState("");
  const [autoScroll, setAutoScroll] = useState(true);
  const [showRaw, setShowRaw] = useState(true);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  const filteredEvents = useMemo(() => {
    if (!searchQuery.trim()) return [...events].reverse();
    const q = searchQuery.toLowerCase();
    return [...events]
      .reverse()
      .filter(
        (e) =>
          e.type.toLowerCase().includes(q) ||
          e.source.toLowerCase().includes(q) ||
          e.specialist.toLowerCase().includes(q) ||
          e.action.toLowerCase().includes(q) ||
          JSON.stringify(e.data).toLowerCase().includes(q)
      );
  }, [events, searchQuery]);

  // Auto-scroll to bottom on new events
  useEffect(() => {
    if (autoScroll && logEndRef.current) {
      logEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [events.length, autoScroll]);

  const handleCopyRaw = useCallback(async (e: UIEvent) => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(e, null, 2));
      setCopiedId(`${e.type}-${e.timestamp}`);
      setTimeout(() => setCopiedId(null), 2000);
    } catch {
      // Clipboard API may not be available
    }
  }, []);

  const handleClear = () => {
    onClearEvents();
  };

  const formatTimestamp = (ts: number) => {
    const d = new Date(ts * 1000);
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    const ms = String(d.getMilliseconds()).padStart(3, "0");
    return `${hh}:${mm}:${ss}.${ms}`;
  };

  const formatRelative = (ts: number) => {
    const diff = Date.now() / 1000 - ts;
    if (diff < 5) return "just now";
    if (diff < 60) return `${Math.floor(diff)}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    return `${Math.floor(diff / 3600)}h ago`;
  };

  const tabs: { key: AdminTab; label: string; count?: number }[] = [
    { key: "connection", label: "Connection" },
    { key: "events", label: "Raw Events", count: events.length },
  ];

  return (
    <div className="flex-1 flex flex-col overflow-hidden p-6">
      {/* Page header */}
      <div className="mb-6">
        <h2 className="text-xl font-extrabold text-ink">Admin Settings</h2>
        <p className="text-sm text-ink-muted mt-1">
          Configure WebSocket connection parameters and inspect raw event data
        </p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 mb-6 border-b border-surface-border">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-4 py-2.5 text-sm font-medium transition-colors duration-150 border-b-2 -mb-px ${
              activeTab === tab.key
                ? "border-accent-blue text-accent-blue"
                : "border-transparent text-ink-muted hover:text-ink-soft hover:border-surface-border"
            }`}
          >
            {tab.label}
            {tab.count !== undefined && (
              <span className="ml-2 text-xs bg-surface-border/60 px-1.5 py-0.5 rounded-full">
                {tab.count}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* ────────────── Connection Tab ────────────── */}
      {activeTab === "connection" && (
        <div className="flex-1 overflow-y-auto space-y-6 pr-2">
          {/* Status indicator */}
          <div className="bg-surface-alt border border-surface-border rounded-xl p-5">
            <h3 className="text-sm font-semibold text-ink-soft uppercase tracking-wider mb-4">
              Connection Status
            </h3>
            <div className="flex items-center gap-4">
              <span
                className={`px-3 py-1 rounded-full text-xs font-medium border ${statusBadge.className}`}
              >
                {statusBadge.label}
              </span>
              <span className="text-sm text-ink-soft">
                {config.url}
              </span>
              <span className="text-xs text-ink-muted">
                {connectionStatus === "connected"
                  ? "Receiving events"
                  : "Not connected"}
              </span>
            </div>
            <div className="mt-4 flex gap-2">
              <button
                onClick={onReconnect}
                className="btn-soft"
              >
                ↻ Reconnect
              </button>
            </div>
          </div>

          {/* Connection parameters form */}
          <div className="bg-surface-alt border border-surface-border rounded-xl p-5">
            <h3 className="text-sm font-semibold text-ink-soft uppercase tracking-wider mb-4">
              Connection Parameters
            </h3>

            <div className="space-y-4">
              {/* WebSocket URL */}
              <div>
                <label className="block text-sm text-ink-soft mb-1.5">
                  WebSocket URL
                </label>
                <input
                  type="text"
                  value={editUrl}
                  onChange={(e) => setEditUrl(e.target.value)}
                  placeholder="ws://127.0.0.1:8765"
                  className="w-full bg-surface border border-surface-border rounded-lg px-3 py-2 text-sm text-ink placeholder-ink-muted focus:outline-none focus:border-accent-blue/50 transition-colors"
                />
                <p className="text-xs text-ink-muted mt-1">
                  The WebSocket endpoint the backend bridge listens on. Change
                  requires a reconnection to take effect.
                </p>
              </div>

              {/* Reconnect delay */}
              <div>
                <label className="block text-sm text-ink-soft mb-1.5">
                  Reconnect Delay (ms)
                </label>
                <input
                  type="number"
                  value={editReconnectDelay}
                  onChange={(e) => setEditReconnectDelay(e.target.value)}
                  min={500}
                  step={100}
                  className="w-full bg-surface border border-surface-border rounded-lg px-3 py-2 text-sm text-ink placeholder-ink-muted focus:outline-none focus:border-accent-blue/50 transition-colors"
                />
                <p className="text-xs text-ink-muted mt-1">
                  Time to wait before attempting to reconnect after a
                  disconnection. Minimum 500ms.
                </p>
              </div>

              {/* Max events */}
              <div>
                <label className="block text-sm text-ink-soft mb-1.5">
                  Max Events in Memory
                </label>
                <input
                  type="number"
                  value={editMaxEvents}
                  onChange={(e) => setEditMaxEvents(e.target.value)}
                  min={10}
                  step={50}
                  className="w-full bg-surface border border-surface-border rounded-lg px-3 py-2 text-sm text-ink placeholder-ink-muted focus:outline-none focus:border-accent-blue/50 transition-colors"
                />
                <p className="text-xs text-ink-muted mt-1">
                  Maximum number of events kept in memory across all pages.
                  Older events are dropped when this limit is exceeded. Minimum
                  10.
                </p>
              </div>
            </div>

            {/* Action buttons */}
            <div className="mt-6 flex items-center gap-3">
              <button
                onClick={handleSaveConnection}
                className="btn-soft !px-4 !py-2 !text-sm"
              >
                Save & Reconnect
              </button>
              <button
                onClick={handleReset}
                className="px-4 py-2 text-sm text-ink-muted hover:text-ink-soft transition-colors"
              >
                Reset to Defaults
              </button>

              {saveStatus === "saved" && (
                <span className="text-sm text-emerald-400">✓ Saved</span>
              )}
              {saveStatus === "error" && (
                <span className="text-sm text-red-400">
                  Invalid values — reconnect delay must be ≥500, max events ≥10
                </span>
              )}
            </div>
          </div>

          {/* Last event preview */}
          {lastEvent && (
            <div className="bg-surface-alt border border-surface-border rounded-xl p-5">
              <h3 className="text-sm font-semibold text-ink-soft uppercase tracking-wider mb-3">
                Last Event Received
              </h3>
              <div className="bg-surface rounded-lg p-3 overflow-x-auto">
                <pre className="text-xs text-ink-soft leading-relaxed">
                  <span className="text-ink-muted">
                    {formatTimestamp(lastEvent.timestamp)}
                  </span>{" "}
                  <span style={{ color: lastEvent.color || "#6b7280" }}>
                    {lastEvent.icon || "●"}
                  </span>{" "}
                  <span className="text-ink">{lastEvent.type}</span>
                  {"\n"}
                  {JSON.stringify(lastEvent, null, 2)}
                </pre>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ────────────── Raw Events Tab ────────────── */}
      {activeTab === "events" && (
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Toolbar */}
          <div className="flex items-center gap-3 mb-3 shrink-0">
            <div className="relative flex-1 max-w-md">
              <input
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Search events by type, source, specialist, action, or data…"
                className="w-full bg-surface border border-surface-border rounded-lg pl-8 pr-3 py-1.5 text-xs text-ink placeholder-ink-muted focus:outline-none focus:border-accent-blue/50 transition-colors"
              />
              <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-ink-muted text-xs">
                ◐
              </span>
            </div>

            <div className="flex items-center gap-2 text-xs text-ink-muted">
              <label className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={autoScroll}
                  onChange={(e) => setAutoScroll(e.target.checked)}
                  className="accent-accent-blue"
                />
                Auto-scroll
              </label>
              <label className="flex items-center gap-1.5 cursor-pointer">
                <input
                  type="checkbox"
                  checked={showRaw}
                  onChange={(e) => setShowRaw(e.target.checked)}
                  className="accent-accent-blue"
                />
                Raw JSON
              </label>
            </div>

            <span className="text-xs text-ink-muted">
              {filteredEvents.length} / {events.length}
            </span>

            <button
              onClick={handleClear}
              className="btn-danger"
            >
              Clear All
            </button>
          </div>

          {/* Event log */}
          <div className="flex-1 overflow-y-auto bg-surface-alt border border-surface-border rounded-xl">
            {filteredEvents.length === 0 ? (
              <div className="flex items-center justify-center h-full text-ink-muted text-sm">
                {searchQuery
                  ? "No events match your search"
                  : "No events received yet. Connect to the backend bridge to see events."}
              </div>
            ) : (
              <div className="divide-y divide-surface-border">
                {filteredEvents.map((e, i) => {
                  const key = `${e.type}-${e.timestamp}-${i}`;
                  const isCopied = copiedId === `${e.type}-${e.timestamp}`;
                  return (
                    <div
                      key={key}
                      className="group hover:bg-surface/40 transition-colors"
                    >
                      {/* Event header */}
                      <div className="flex items-center gap-2 px-4 py-2 cursor-pointer">
                        <span
                          className="w-2 h-2 rounded-full shrink-0"
                          style={{
                            backgroundColor: e.color || "#6b7280",
                          }}
                        />
                        <span className="text-xs text-ink-muted tabular-nums w-24 shrink-0 font-mono">
                          {formatTimestamp(e.timestamp)}
                        </span>
                        <span
                          className="text-xs shrink-0"
                          style={{ color: e.color || "#6b7280" }}
                        >
                          {e.icon || "●"}
                        </span>
                        <span className="text-xs font-medium text-ink shrink-0">
                          {e.type}
                        </span>
                        <span className="text-xs text-ink-muted hidden sm:inline shrink-0">
                          {e.specialist}
                        </span>
                        <span className="text-xs text-ink-muted truncate flex-1 min-w-0">
                          {e.action}
                        </span>
                        <span className="text-xs text-ink-muted shrink-0 tabular-nums">
                          {formatRelative(e.timestamp)}
                        </span>
                        <button
                          onClick={(ev) => {
                            ev.stopPropagation();
                            handleCopyRaw(e);
                          }}
                          className="opacity-0 group-hover:opacity-100 text-xs px-1.5 py-0.5 rounded text-ink-muted hover:text-ink-soft transition-all shrink-0"
                          title="Copy raw JSON"
                        >
                          {isCopied ? "✓" : "⎘"}
                        </button>
                      </div>

                      {/* Expandable raw data */}
                      {showRaw && (
                        <div className="px-4 pb-2 pl-14">
                          <pre className="text-xs text-ink-muted leading-relaxed bg-surface/50 rounded-md p-2 overflow-x-auto max-h-48 overflow-y-auto">
                            {JSON.stringify(e.data, null, 2) || "{}"}
                          </pre>
                          <div className="text-[10px] text-ink-muted mt-1 font-mono">
                            source: {e.source} | specialist: {e.specialist} |
                            action: {e.action}
                          </div>
                        </div>
                      )}
                    </div>
                  );
                })}
                <div ref={logEndRef} />
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
