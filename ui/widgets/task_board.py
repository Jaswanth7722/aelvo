from textual.widgets import Static
from textual.reactive import reactive
from rich.text import Text


STATUS_DISPLAY = {
    "pending": "·",
    "running": "◐",
    "review": "◑",
    "blocked": "◍",
    "failed": "×",
    "completed": "✓",
    "cancelled": "○",
}

STATUS_ORDER = {"running": 0, "review": 1, "blocked": 2, "pending": 3, "failed": 4, "completed": 5, "cancelled": 6}


class TaskBoard(Static):
    tasks: reactive[list] = reactive([], always_update=True)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#4682b4")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"

    def watch_tasks(self, tasks: list) -> None:
        self.render_content(tasks)

    def add_or_update_task(self, task_id: str, name: str, status: str, specialist: str = "", priority: str = "med", progress: float = 0.0) -> None:
        current = [t for t in self.tasks if t["id"] != task_id]
        current.append({
            "id": task_id, "name": name, "status": status,
            "specialist": specialist, "priority": priority, "progress": progress,
        })
        if len(current) > 50:
            current = current[-50:]
        self.tasks = current

    def remove_task(self, task_id: str) -> None:
        self.tasks = [t for t in self.tasks if t["id"] != task_id]

    def render_content(self, tasks: list) -> None:
        if not tasks:
            self.update("")
            return

        sorted_tasks = sorted(tasks, key=lambda t: (STATUS_ORDER.get(t.get("status", "pending"), 99), t.get("name", "")))
        lines = [" tasks"]

        for t in sorted_tasks:
            status = t.get("status", "pending")
            sname = t.get("name", "")[:30]
            status_sym = STATUS_DISPLAY.get(status, "·")
            color = "#2e8b57" if status == "completed" else "#cd5c5c" if status in ("failed", "blocked") else "#6495ed" if status == "running" else "white"
            lines.append(f"  [{color}]{status_sym}[/] {sname}")

        self.update("\n".join(lines))
