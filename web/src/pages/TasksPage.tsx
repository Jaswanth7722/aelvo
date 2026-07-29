import { useWebSocket } from "../hooks/useWebSocket";
import { TaskBoard } from "../components/TaskBoard";

export default function TasksPage() {
  const { events } = useWebSocket();
  const transitionCount = events.filter((e) => e.type === "task_board_transition").length;

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Top bar */}
      <header className="border-b border-surface-border px-6 py-3 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-4">
          <h2 className="text-lg font-bold text-gray-200">Task Board</h2>
          <span className="text-xs text-gray-600">{transitionCount} transitions recorded</span>
        </div>
      </header>

      {/* Kanban board */}
      <TaskBoard events={events} />
    </div>
  );
}
