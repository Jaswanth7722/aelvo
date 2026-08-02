import { useWebSocket } from "../hooks/useWebSocket";
import { ChatWorkspace } from "../components/ChatWorkspace";

export default function ChatPage() {
  const { status, events, sendMessage } = useWebSocket();

  return (
    <ChatWorkspace
      events={events}
      connectionStatus={status}
      sendMessage={sendMessage}
    />
  );
}
