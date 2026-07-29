import { useWebSocket } from "../hooks/useWebSocket";
import { KnowledgeExplorer } from "../components/KnowledgeExplorer";

export default function KnowledgePage() {
  const { events } = useWebSocket();

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <KnowledgeExplorer events={events} />
    </div>
  );
}
