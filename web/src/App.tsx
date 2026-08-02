import { Routes, Route } from "react-router-dom";
import { useWebSocket } from "./hooks/useWebSocket";
import { Layout } from "./components/Layout";
import DashboardPage from "./pages/DashboardPage";
import EventsPage from "./pages/EventsPage";
import TasksPage from "./pages/TasksPage";
import KnowledgePage from "./pages/KnowledgePage";
import AgentsPage from "./pages/AgentsPage";
import ConsensusPage from "./pages/ConsensusPage";
import HealthPage from "./pages/HealthPage";
import GovernancePage from "./pages/GovernancePage";
import MonitoringPage from "./pages/MonitoringPage";
import ProvidersPage from "./pages/ProvidersPage";
import AdminPage from "./pages/AdminPage";
import ChatPage from "./pages/ChatPage";

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
        <Route path="/events" element={<EventsPage />} />
        <Route path="/tasks" element={<TasksPage />} />
        <Route path="/knowledge" element={<KnowledgePage />} />
        <Route path="/agents" element={<AgentsPage />} />
        <Route path="/consensus" element={<ConsensusPage />} />
        <Route path="/health" element={<HealthPage />} />
        <Route path="/governance" element={<GovernancePage />} />
        <Route path="/monitoring" element={<MonitoringPage />} />
        <Route path="/providers" element={<ProvidersPage />} />
        <Route path="/chat" element={<ChatPage />} />
        <Route path="/admin" element={<AdminPage />} />
      </Routes>
    </Layout>
  );
}
