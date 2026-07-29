import { useWebSocket } from "../hooks/useWebSocket";
import { MonitoringDashboard } from "../components/MonitoringDashboard";

export default function MonitoringPage() {
  const { events } = useWebSocket();

  return (
    <MonitoringDashboard events={events} />
  );
}
