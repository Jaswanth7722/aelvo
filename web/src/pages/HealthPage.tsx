import { useWebSocket } from "../hooks/useWebSocket";
import { SystemHealthDashboard } from "../components/SystemHealthDashboard";

export default function HealthPage() {
  const { events } = useWebSocket();

  return (
    <SystemHealthDashboard events={events} />
  );
}
