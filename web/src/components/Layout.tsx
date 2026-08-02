import type { ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";

interface LayoutProps {
  children: ReactNode;
  connectionStatus: string;
  eventCount: number;
}

const NAV_ITEMS = [
  { path: "/", label: "Chat", icon: "◈" },
  { path: "/dashboard", label: "Dashboard", icon: "◉" },
  { path: "/events", label: "Timeline", icon: "◈" },
  { path: "/tasks", label: "Tasks", icon: "☰" },
  { path: "/knowledge", label: "Knowledge", icon: "◈" },
  { path: "/consensus", label: "Consensus", icon: "↻" },
  { path: "/agents", label: "Agents", icon: "●" },
  { path: "/health", label: "Health", icon: "◈" },
  { path: "/governance", label: "Governance", icon: "◉" },
  { path: "/monitoring", label: "Monitoring", icon: "◈" },
  { path: "/security", label: "Security", icon: "🔒" },
  { path: "/admin", label: "Admin", icon: "⚙" },
];

const STATUS_STYLES: Record<string, { color: string; label: string; dot: string }> = {
  connected: { color: "text-emerald-600", label: "Connected", dot: "bg-emerald-500" },
  connecting: { color: "text-amber-600", label: "Connecting…", dot: "bg-amber-500 animate-pulse" },
  disconnected: { color: "text-rose-600", label: "Disconnected", dot: "bg-rose-500" },
  error: { color: "text-rose-600", label: "Error", dot: "bg-rose-500" },
};

export function Layout({ children, connectionStatus, eventCount }: LayoutProps) {
  const location = useLocation();
  const status = STATUS_STYLES[connectionStatus] || STATUS_STYLES.disconnected;

  return (
    <div className="flex h-screen overflow-hidden bg-[#FFF7EC]">
      {/* Sidebar */}
      <aside className="w-56 border-r border-surface-border bg-white/80 backdrop-blur-md flex flex-col shrink-0">
        {/* Logo / Brand */}
        <div className="px-5 py-4 border-b border-surface-border">
          <h1 className="text-xl font-extrabold tracking-tight">
            <span className="text-gradient">AELVO</span>
          </h1>
          <p className="text-[11px] text-ink-muted mt-0.5 font-medium">Multi-Agent OS</p>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
          {NAV_ITEMS.map((item) => {
            const active = location.pathname === item.path;
            return (
              <Link
                key={item.path}
                to={item.path}
                className={`group flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-150 ${
                  active
                    ? "bg-gradient-to-r from-brand-orange/20 to-brand-purple/15 text-brand-deep border border-brand-orange/30 shadow-soft"
                    : "text-ink-soft hover:text-brand-deep hover:bg-brand-orange/10 hover:translate-x-0.5 border border-transparent"
                }`}
              >
                <span
                  className={`w-5 text-center transition-transform duration-200 ${
                    active ? "text-brand-orange group-hover:scale-110" : ""
                  }`}
                >
                  {item.icon}
                </span>
                <span>{item.label}</span>
                {active && (
                  <span className="ml-auto w-1.5 h-1.5 rounded-full bg-brand-orange animate-pulse-glow" />
                )}
              </Link>
            );
          })}
        </nav>

        {/* Connection status footer */}
        <div className="px-5 py-3 border-t border-surface-border">
          <div className="flex items-center gap-2 text-xs">
            <span className={`w-2 h-2 rounded-full ${status.dot}`} />
            <span className={`font-semibold ${status.color}`}>{status.label}</span>
          </div>
          <div className="text-[11px] text-ink-muted mt-1">
            {eventCount} events streamed
          </div>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 flex flex-col overflow-hidden">
        {children}
      </main>
    </div>
  );
}
