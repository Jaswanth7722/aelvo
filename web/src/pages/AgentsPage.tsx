import { useWebSocket } from "../hooks/useWebSocket";
import { AgentDashboard } from "../components/AgentDashboard";

export default function AgentsPage() {
  const { events } = useWebSocket();

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <AgentDashboard events={events} />
    </div>
  );
}
