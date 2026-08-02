import { useState } from "react";
import type { ChatSession } from "../types";

interface SessionSidebarProps {
  sessions: ChatSession[];
  activeSessionId: string | null;
  onSelectSession: (id: string) => void;
  onNewSession: () => void;
  onTogglePin: (id: string) => void;
  onDeleteSession: (id: string) => void;
}

function fmtDate(ts: number): string {
  const d = new Date(ts * 1000);
  const now = new Date();
  const diff = now.getTime() - d.getTime();
  if (diff < 86400000) return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  if (diff < 604800000) return d.toLocaleDateString([], { weekday: "short" });
  return d.toLocaleDateString([], { month: "short", day: "numeric" });
}

export function SessionSidebar({
  sessions,
  activeSessionId,
  onSelectSession,
  onNewSession,
  onTogglePin,
  onDeleteSession,
}: SessionSidebarProps) {
  const [search, setSearch] = useState("");

  const filtered = search.trim()
    ? sessions.filter((s) => s.title.toLowerCase().includes(search.toLowerCase()))
    : sessions;

  const pinned = filtered.filter((s) => s.pinned);
  const recent = filtered.filter((s) => !s.pinned);

  return (
    <div className="w-64 border-r border-surface-border bg-white/70 backdrop-blur-md flex flex-col shrink-0 overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-surface-border">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-xs font-bold text-ink-soft uppercase tracking-wider">Sessions</h3>
          <button
            onClick={onNewSession}
            className="text-xs text-brand-deep hover:text-brand-orange transition-colors font-semibold"
          >
            + New
          </button>
        </div>
        <div className="relative">
          <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-ink-muted text-xs">🔍</span>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search sessions..."
            className="input-field !py-1.5 !pl-8 text-xs"
          />
        </div>
      </div>

      {/* Session list */}
      <div className="flex-1 overflow-y-auto">
        {pinned.length > 0 && (
          <div className="px-3 pt-3 pb-1">
            <div className="text-[9px] text-ink-muted uppercase tracking-wider mb-1 px-1 font-semibold">Pinned</div>
            {pinned.map((s) => (
              <SessionRow
                key={s.id}
                session={s}
                isActive={s.id === activeSessionId}
                onSelect={onSelectSession}
                onTogglePin={onTogglePin}
                onDelete={onDeleteSession}
              />
            ))}
          </div>
        )}

        <div className="px-3 pt-3 pb-1">
          <div className="text-[9px] text-ink-muted uppercase tracking-wider mb-1 px-1 font-semibold">
            {pinned.length > 0 ? "Recent" : "All Sessions"}
          </div>
          {recent.length === 0 ? (
            <div className="text-xs text-ink-muted text-center py-8">
              {search ? "No sessions match your search" : "No sessions yet. Start a conversation!"}
            </div>
          ) : (
            recent.map((s) => (
              <SessionRow
                key={s.id}
                session={s}
                isActive={s.id === activeSessionId}
                onSelect={onSelectSession}
                onTogglePin={onTogglePin}
                onDelete={onDeleteSession}
              />
            ))
          )}
        </div>
      </div>
    </div>
  );
}

interface SessionRowProps {
  session: ChatSession;
  isActive: boolean;
  onSelect: (id: string) => void;
  onTogglePin: (id: string) => void;
  onDelete: (id: string) => void;
}

function SessionRow({ session, isActive, onSelect, onTogglePin, onDelete }: SessionRowProps) {
  return (
    <div
      className={`group flex items-center gap-2 px-2.5 py-2 rounded-lg cursor-pointer transition-all duration-150 mb-0.5 ${
        isActive
          ? "bg-gradient-to-r from-brand-orange/20 to-brand-purple/10 border border-brand-orange/30 shadow-soft"
          : "hover:bg-brand-orange/10 border border-transparent hover:translate-x-0.5"
      }`}
      onClick={() => onSelect(session.id)}
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className={`text-xs font-medium truncate ${isActive ? "text-brand-deep" : "text-ink"}`}>
            {session.title}
          </span>
          {session.pinned && <span className="text-[9px] text-accent-amber shrink-0">📌</span>}
        </div>
        <div className="flex items-center gap-2 text-[10px] text-ink-muted">
          <span>{session.messageCount} messages</span>
          <span>•</span>
          <span>{fmtDate(session.updatedAt)}</span>
        </div>
      </div>

      {/* Hover actions */}
      <div className="hidden group-hover:flex items-center gap-1 shrink-0">
        <button
          onClick={(e) => { e.stopPropagation(); onTogglePin(session.id); }}
          className="text-[10px] text-ink-muted hover:text-accent-amber transition-colors px-1"
          title={session.pinned ? "Unpin" : "Pin"}
        >
          📌
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); onDelete(session.id); }}
          className="text-[10px] text-ink-muted hover:text-accent-red transition-colors px-1"
          title="Delete"
        >
          ✕
        </button>
      </div>
    </div>
  );
}
