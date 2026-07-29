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

export function Layout({ children, connectionStatus, eventCount }: LayoutProps) {
  const location = useLocation();

  const statusColor = {
    connected: "text-accent-green",
    connecting: "text-accent-amber",
    disconnected: "text-accent-red",
    error: "text-accent-red",
  }[connectionStatus] || "text-gray-400";

  const statusLabel = {
    connected: "Connected",
    connecting: "Connecting…",
    disconnected: "Disconnected",
    error: "Error",
  }[connectionStatus] || "Unknown";

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside className="w-56 border-r border-surface-border bg-surface-alt flex flex-col shrink-0">
        {/* Logo / Brand */}
        <div className="px-5 py-4 border-b border-surface-border">
          <h1 className="text-lg font-bold tracking-wide">
            <span className="text-accent-purple">AELVO</span>
          </h1>
          <p className="text-xs text-gray-500 mt-0.5">Multi-Agent OS</p>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 py-4 space-y-1">
          {NAV_ITEMS.map((item) => {
            const active = location.pathname === item.path;
            return (
              <Link
                key={item.path}
                to={item.path}
                className={`flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors duration-150 ${
                  active
                    ? "bg-accent-blue/10 text-accent-blue border border-accent-blue/20"
                    : "text-gray-400 hover:text-gray-200 hover:bg-surface-border/40"
                }`}
              >
                <span>{item.icon}</span>
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>

        {/* Connection status footer */}
        <div className="px-5 py-3 border-t border-surface-border">
          <div className="flex items-center gap-2 text-xs">
            <span className={`w-2 h-2 rounded-full ${statusColor} ${
              connectionStatus === "connecting" ? "animate-pulse" : ""
            }`} />
            <span className="text-gray-500">{statusLabel}</span>
          </div>
          <div className="text-xs text-gray-600 mt-1">
            {eventCount} events
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
