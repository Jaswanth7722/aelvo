import { useWebSocket } from "../hooks/useWebSocket";
import { AdminSettings } from "../components/AdminSettings";

export default function AdminPage() {
  const { events, status, lastEvent, clearEvents, reconnect } =
    useWebSocket();

  return (
    <AdminSettings
      events={events}
      connectionStatus={status}
      lastEvent={lastEvent}
      onClearEvents={clearEvents}
      onReconnect={reconnect}
    />
  );
}
