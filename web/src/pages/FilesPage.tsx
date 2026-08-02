import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useWebSocket } from "../hooks/useWebSocket";
import type {
  UIEvent,
  FsListing,
  FsReadResult,
  FsWorkspaceResult,
} from "../types";

interface Line {
  kind: "cmd" | "out" | "err" | "info";
  text: string;
  ts: number;
}

function fmtSize(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)}M`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)}K`;
  return `${bytes}B`;
}

function fmtTime(ts: number): string {
  const d = new Date(ts * 1000);
  return d.toLocaleDateString([], { month: "short", day: "numeric" }) +
    " " + d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function iconFor(name: string, type: string): string {
  if (type === "dir") return "▸";
  const ext = name.split(".").pop()?.toLowerCase() || "";
  const map: Record<string, string> = {
    py: "🐍", ts: "🟦", tsx: "🟦", js: "🟨", jsx: "🟨",
    json: "🧾", md: "📝", rs: "🦀", html: "🌐", css: "🎨",
    yml: "⚙", yaml: "⚙", toml: "⚙", lock: "🔒",
  };
  return map[ext] || "📄";
}

export default function FilesPage() {
  const { status, events, sendCommand } = useWebSocket();

  const [cwd, setCwd] = useState<string>(".");
  const [workspaceRoot, setWorkspaceRoot] = useState<string>("");
  const [listing, setListing] = useState<FsListing | null>(null);
  const [preview, setPreview] = useState<FsReadResult | null>(null);
  const [lines, setLines] = useState<Line[]>([]);
  const [input, setInput] = useState("");
  const [openPath, setOpenPath] = useState("");
  const [busy, setBusy] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const outputRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const connected = status === "connected";

  const push = useCallback((kind: Line["kind"], text: string) => {
    setLines((prev) => [...prev, { kind, text, ts: Date.now() }]);
  }, []);

  // ── Parse fs_* events from the bridge ─────────────────────────
  const fsEvents = useMemo(
    () => events.filter((e) =>
      e.type.startsWith("fs_") || e.type === "agent_metrics"
    ),
    [events]
  );

  const lastEvent = fsEvents[fsEvents.length - 1];

  // React to the latest fs_* event
  useEffect(() => {
    if (!lastEvent) return;
    const d = lastEvent.data || {};

    if (lastEvent.type === "fs_list") {
      setListing(d as unknown as FsListing);
      if (d.cwd !== undefined) setCwd(String(d.cwd));
      if (d.root !== undefined) setWorkspaceRoot(String(d.root));
      setBusy(false);
    } else if (lastEvent.type === "fs_list_error") {
      push("err", `ls: ${String(d.error || lastEvent.action)}`);
      setBusy(false);
    } else if (lastEvent.type === "fs_read") {
      setPreview(d as unknown as FsReadResult);
      setBusy(false);
    } else if (lastEvent.type === "fs_read_error") {
      push("err", `cat: ${String(d.error || lastEvent.action)}`);
      setBusy(false);
    } else if (lastEvent.type === "fs_workspace") {
      if (d.root) {
        setWorkspaceRoot(String(d.root));
        push("info", `workspace: ${String(d.root)}`);
      }
    } else if (lastEvent.type === "fs_workspace_result") {
      const result = d as unknown as FsWorkspaceResult;
      setBusy(false);
      if (result.success && result.root) {
        setWorkspaceRoot(result.root);
        push("info", result.message || `workspace → ${result.root}`);
      } else {
        push("err", result.error || String(lastEvent.action));
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lastEvent]);

  // Initial load: fetch workspace + root listing on connect
  useEffect(() => {
    if (connected && !loaded) {
      sendCommand("fs_workspace");
      sendCommand("fs_list", { path: "." });
      setLoaded(true);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connected, loaded]);

  const loadDir = useCallback((path: string) => {
    setBusy(true);
    setPreview(null);
    sendCommand("fs_list", { path });
  }, [sendCommand]);

  const readFile = useCallback((path: string) => {
    setBusy(true);
    sendCommand("fs_read", { path });
  }, [sendCommand]);

  // Auto-scroll output
  useEffect(() => {
    if (outputRef.current) {
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
    }
  }, [lines]);

  // ── CLI command dispatch ───────────────────────────────────────
  const runCommand = useCallback((raw: string) => {
    const text = raw.trim();
    if (!text) return;
    push("cmd", text);

    const [name, ...args] = text.split(/\s+/);
    const arg = args.join(" ");

    const join = (base: string, child: string): string => {
      if (!child || child === ".") return base;
      if (child === "..") {
        if (base === "." || base === "") return ".";
        const parts = base.split("/").filter(Boolean);
        parts.pop();
        return parts.length ? parts.join("/") : ".";
      }
      if (child.startsWith("/")) return child.replace(/^\/+/, "");
      return base === "." ? child : `${base}/${child}`;
    };

    switch (name) {
      case "help":
        push("out", "AELVO file browser — commands:");
        push("out", "  ls [dir]          list directory entries");
        push("out", "  cd <dir>          change directory");
        push("out", "  cat <file>        view file contents");
        push("out", "  pwd               print current directory");
        push("out", "  open <folder>     open as the agent's workspace");
        push("out", "  clear             clear the terminal");
        push("out", "  help              show this help");
        break;
      case "pwd":
        push("out", cwd);
        break;
      case "ls":
        loadDir(join(cwd, arg || "."));
        break;
      case "cd": {
        if (!arg) {
          push("out", cwd);
          break;
        }
        loadDir(join(cwd, arg));
        break;
      }
      case "cat":
        if (!arg) {
          push("err", "cat: missing file path");
          break;
        }
        readFile(join(cwd, arg));
        break;
      case "open":
        if (!arg) {
          push("err", "open: missing folder path");
          break;
        }
        setBusy(true);
        sendCommand("fs_set_workspace", { path: join(cwd, arg) });
        break;
      case "clear":
        setLines([]);
        break;
      default:
        push("err", `${name}: command not found (try 'help')`);
    }
  }, [cwd, loadDir, readFile, push, sendCommand]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    runCommand(input);
    setInput("");
  };

  const setWorkspace = () => {
    setBusy(true);
    sendCommand("fs_set_workspace", { path: openPath.trim() || cwd });
  };

  const parentDir = cwd === "." ? null : (() => {
    const parts = cwd.split("/").filter(Boolean);
    parts.pop();
    return parts.length ? parts.join("/") : ".";
  })();

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Top bar */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0 bg-white/70 backdrop-blur-md">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-bold text-ink">Files</h2>
          <span className="chip">
            <span className={`w-2 h-2 rounded-full ${connected ? "bg-accent-green animate-pulse-glow" : "bg-accent-amber animate-pulse"}`} />
            {connected ? "Live" : "Offline"}
          </span>
        </div>
        <span className="text-xs text-ink-muted font-mono truncate max-w-[40%]" title={workspaceRoot}>
          {workspaceRoot || "loading workspace…"}
        </span>
      </header>

      <div className="flex-1 flex min-h-0">
        {/* ── Terminal ─────────────────────────────────────── */}
        <div className="flex-1 flex flex-col min-w-0 border-r border-surface-border">
          {/* Output */}
          <div
            ref={outputRef}
            onClick={() => inputRef.current?.focus()}
            className="flex-1 overflow-y-auto bg-[#2A2438]/95 p-4 font-mono text-[13px] leading-relaxed cursor-text"
          >
            {lines.map((l, i) => (
              <div
                key={i}
                className={
                  l.kind === "cmd"
                    ? "text-brand-gold"
                    : l.kind === "err"
                      ? "text-accent-red"
                      : l.kind === "info"
                        ? "text-brand-purple"
                        : "text-white/90"
                }
              >
                {l.kind === "cmd" ? (
                  <span>
                    <span className="text-brand-orange">aelvo@ws</span>
                    <span className="text-ink-muted">:{cwd}$ </span>
                    {l.text}
                  </span>
                ) : (
                  <span className="whitespace-pre-wrap break-words">{l.text}</span>
                )}
              </div>
            ))}
            {busy && (
              <div className="text-ink-muted animate-pulse">…</div>
            )}
          </div>

          {/* Listing */}
          <div className="max-h-56 overflow-y-auto border-t border-surface-border bg-white">
            {listing ? (
              <div className="divide-y divide-surface-border/60">
                {parentDir && (
                  <button
                    onClick={() => loadDir(parentDir)}
                    className="w-full text-left px-4 py-1.5 text-xs font-mono text-ink-muted hover:bg-brand-orange/10 transition-colors"
                  >
                    <span className="text-brand-orange mr-2">▴</span> ../
                  </button>
                )}
                {listing.entries.length === 0 && (
                  <div className="px-4 py-3 text-xs text-ink-muted">
                    (empty directory)
                  </div>
                )}
                {listing.entries.map((entry) => (
                  <button
                    key={entry.name}
                    onClick={() =>
                      entry.type === "dir"
                        ? loadDir(cwd === "." ? entry.name : `${cwd}/${entry.name}`)
                        : readFile(cwd === "." ? entry.name : `${cwd}/${entry.name}`)
                    }
                    className="w-full text-left px-4 py-1.5 text-xs font-mono flex items-center gap-2 hover:bg-brand-orange/10 transition-colors"
                    title={entry.type === "dir" ? "cd into folder" : "cat file"}
                  >
                    <span
                      className={
                        entry.type === "dir" ? "text-brand-orange" : "text-ink-muted"
                      }
                    >
                      {iconFor(entry.name, entry.type)}
                    </span>
                    <span
                      className={
                        entry.type === "dir"
                          ? "text-brand-deep font-semibold"
                          : "text-ink-soft"
                      }
                    >
                      {entry.name}
                    </span>
                    {entry.type === "dir" && <span className="text-ink-muted">/</span>}
                    <span className="ml-auto text-[10px] text-ink-muted">
                      {entry.type === "file" ? fmtSize(entry.size) : ""}
                      {entry.type === "file" ? ` · ${fmtTime(entry.modified)}` : ""}
                    </span>
                  </button>
                ))}
              </div>
            ) : (
              <div className="px-4 py-3 text-xs text-ink-muted">
                {connected ? "Run 'ls' to list the workspace…" : "Connecting to AELVO backend…"}
              </div>
            )}
          </div>

          {/* Prompt input */}
          <form
            onSubmit={handleSubmit}
            className="shrink-0 flex items-center gap-2 px-4 py-2.5 bg-[#2A2438]"
          >
            <span className="text-sm font-mono text-brand-orange shrink-0">aelvo@ws</span>
            <span className="text-sm font-mono text-ink-muted shrink-0">:{cwd}$</span>
            <input
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              autoFocus
              spellCheck={false}
              placeholder="type ls, cd &lt;dir&gt;, cat &lt;file&gt;, open &lt;folder&gt;, help…"
              className="flex-1 bg-transparent text-sm font-mono text-white placeholder-ink-muted/60 focus:outline-none"
            />
          </form>
        </div>

        {/* ── Preview / Workspace panel ─────────────────────── */}
        <div className="w-96 shrink-0 flex flex-col bg-white/60 backdrop-blur-md">
          {/* Open as workspace */}
          <div className="p-4 border-b border-surface-border">
            <div className="text-xs text-ink-muted uppercase tracking-wider mb-2">
              Agent Workspace
            </div>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                setWorkspace();
              }}
              className="flex gap-2"
            >
              <input
                value={openPath}
                onChange={(e) => setOpenPath(e.target.value)}
                placeholder="folder path (or blank = current)"
                className="input-field !py-2 !text-xs font-mono flex-1 min-w-0"
              />
              <button type="submit" disabled={busy} className="btn-primary !py-2 !px-3 !text-xs shrink-0">
                Open
              </button>
            </form>
            <p className="text-[11px] text-ink-muted mt-2 leading-relaxed">
              Point the agent at any folder — its tools (read, write, bash,
              tree) operate on it directly, like CLI/web/desktop coding agents.
            </p>
          </div>

          {/* File preview */}
          <div className="flex-1 flex flex-col min-h-0">
            <div className="px-4 py-2 border-b border-surface-border flex items-center justify-between">
              <span className="text-xs text-ink-muted uppercase tracking-wider">
                {preview ? `Preview — ${preview.path}` : "Preview"}
              </span>
              {preview?.truncated && (
                <span className="text-[10px] text-accent-amber">
                  truncated ({preview.size} bytes total)
                </span>
              )}
            </div>
            <div className="flex-1 overflow-auto p-3">
              {preview ? (
                <pre className="text-[11px] leading-relaxed text-ink-soft whitespace-pre-wrap break-words font-mono">
                  {preview.content}
                </pre>
              ) : (
                <div className="text-xs text-ink-muted">
                  Run <span className="font-mono">cat &lt;file&gt;</span> or click a
                  file in the listing to preview it here.
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
