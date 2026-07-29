import { useWebSocket } from "../hooks/useWebSocket";
import { ConsensusDashboard } from "../components/ConsensusDashboard";

export default function ConsensusPage() {
  const { events } = useWebSocket();

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <ConsensusDashboard events={events} />
    </div>
  );
}
