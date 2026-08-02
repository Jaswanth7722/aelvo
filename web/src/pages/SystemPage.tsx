import { useState } from "react";
import { useWebSocket } from "../hooks/useWebSocket";
import { SystemHealthDashboard } from "../components/SystemHealthDashboard";
import { MonitoringDashboard } from "../components/MonitoringDashboard";

type SystemTab = "health" | "monitoring";

const TABS: { key: SystemTab; label: string; icon: string }[] = [
  { key: "health", label: "Health", icon: "◈" },
  { key: "monitoring", label: "Monitoring", icon: "◉" },
];

export default function SystemPage() {
  const { events } = useWebSocket();
  const [activeTab, setActiveTab] = useState<SystemTab>("health");

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Page-level tab bar (merged Health + Monitoring) */}
      <header className="border-b border-surface-border px-6 py-2.5 flex items-center gap-1 shrink-0 bg-white/70 backdrop-blur-md">
        <h2 className="text-sm font-bold text-ink mr-4">System</h2>
        <div className="flex gap-1">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`px-3.5 py-1.5 rounded-lg text-xs font-medium transition-all duration-150 ${
                activeTab === tab.key
                  ? "bg-gradient-to-r from-brand-orange/15 to-brand-purple/15 text-brand-deep border border-brand-orange/30 shadow-soft"
                  : "text-ink-muted hover:text-ink-soft hover:bg-brand-orange/10 border border-transparent"
              }`}
            >
              <span className="mr-1.5">{tab.icon}</span>
              {tab.label}
            </button>
          ))}
        </div>
      </header>

      <div className="flex-1 flex flex-col overflow-hidden">
        {activeTab === "health" ? (
          <SystemHealthDashboard events={events} />
        ) : (
          <MonitoringDashboard events={events} />
        )}
      </div>
    </div>
  );
}
