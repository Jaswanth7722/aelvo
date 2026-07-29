import { useWebSocket } from "../hooks/useWebSocket";
import { SecurityDashboard } from "../components/SecurityDashboard";

export default function SecurityPage() {
  const { events } = useWebSocket();

  return (
    <SecurityDashboard events={events} />
  );
}
