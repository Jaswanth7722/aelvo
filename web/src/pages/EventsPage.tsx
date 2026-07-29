import { useWebSocket } from "../hooks/useWebSocket";
import { CollaborationTimeline } from "../components/CollaborationTimeline";

export default function EventsPage() {
  const { events } = useWebSocket();

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <CollaborationTimeline events={events} />
    </div>
  );
}
