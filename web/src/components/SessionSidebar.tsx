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
    <div className="w-64 border-r border-surface-border bg-surface-alt flex flex-col shrink-0 overflow-hidden">
      {/* Header */}
      <div className="px-4 py-3 border-b border-surface-border">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider">Sessions</h3>
          <button
            onClick={onNewSession}
            className="text-xs text-accent-blue hover:text-accent-blue/80 transition-colors font-medium"
          >
            + New
          </button>
        </div>
        <div className="relative">
          <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-600 text-xs">🔍</span>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search sessions..."
            className="w-full bg-surface border border-surface-border rounded-lg pl-8 pr-3 py-1.5 text-xs text-gray-200 placeholder-gray-600 focus:outline-none focus:border-accent-blue/50 transition-colors"
          />
        </div>
      </div>

      {/* Session list */}
      <div className="flex-1 overflow-y-auto">
        {pinned.length > 0 && (
          <div className="px-3 pt-3 pb-1">
            <div className="text-[9px] text-gray-600 uppercase tracking-wider mb-1 px-1">Pinned</div>
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
          <div className="text-[9px] text-gray-600 uppercase tracking-wider mb-1 px-1">
            {pinned.length > 0 ? "Recent" : "All Sessions"}
          </div>
          {recent.length === 0 ? (
            <div className="text-xs text-gray-600 text-center py-8">
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
      className={`group flex items-center gap-2 px-2.5 py-2 rounded-lg cursor-pointer transition-colors mb-0.5 ${
        isActive
          ? "bg-accent-blue/10 border border-accent-blue/20"
          : "hover:bg-surface-border/30 border border-transparent"
      }`}
      onClick={() => onSelect(session.id)}
    >
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-gray-300 truncate">{session.title}</span>
          {session.pinned && <span className="text-[9px] text-accent-amber shrink-0">📌</span>}
        </div>
        <div className="flex items-center gap-2 text-[10px] text-gray-600">
          <span>{session.messageCount} messages</span>
          <span>•</span>
          <span>{fmtDate(session.updatedAt)}</span>
        </div>
      </div>

      {/* Hover actions */}
      <div className="hidden group-hover:flex items-center gap-1 shrink-0">
        <button
          onClick={(e) => { e.stopPropagation(); onTogglePin(session.id); }}
          className="text-[10px] text-gray-500 hover:text-accent-amber transition-colors px-1"
          title={session.pinned ? "Unpin" : "Pin"}
        >
          📌
        </button>
        <button
          onClick={(e) => { e.stopPropagation(); onDelete(session.id); }}
          className="text-[10px] text-gray-500 hover:text-accent-red transition-colors px-1"
          title="Delete"
        >
          ✕
        </button>
      </div>
    </div>
  );
}
