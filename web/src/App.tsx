import { Routes, Route } from "react-router-dom";
import { useWebSocket } from "./hooks/useWebSocket";
import { Layout } from "./components/Layout";
import DashboardPage from "./pages/DashboardPage";
import TasksPage from "./pages/TasksPage";
import KnowledgePage from "./pages/KnowledgePage";
import AgentsPage from "./pages/AgentsPage";
import ProvidersPage from "./pages/ProvidersPage";
import ChatPage from "./pages/ChatPage";
import FilesPage from "./pages/FilesPage";

export default function App() {
  const { status, events } = useWebSocket();

  return (
    <Layout
      connectionStatus={status}
      eventCount={events.length}
    >
      <Routes>
        <Route path="/" element={<ChatPage />} />
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/tasks" element={<TasksPage />} />
        <Route path="/knowledge" element={<KnowledgePage />} />
        <Route path="/agents" element={<AgentsPage />} />
        <Route path="/providers" element={<ProvidersPage />} />
        <Route path="/files" element={<FilesPage />} />
        <Route path="/chat" element={<ChatPage />} />
      </Routes>
    </Layout>
  );
}
