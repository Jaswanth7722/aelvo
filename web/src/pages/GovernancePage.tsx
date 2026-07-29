import { useWebSocket } from "../hooks/useWebSocket";
import { GovernanceDashboard } from "../components/GovernanceDashboard";

export default function GovernancePage() {
  const { events } = useWebSocket();

  return (
    <GovernanceDashboard events={events} />
  );
}
