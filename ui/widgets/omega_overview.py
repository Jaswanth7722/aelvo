"""omega_overview.py — Lightweight command center panel

Displays only:
- Provider / Model
- Active Goal
- Current Task
- Progress
- Brief agent status summary (active/inactive count)

Removed: agent status tables, analytics, verification, consensus,
recovery, event trace, uptime, health indicators.
"""

import time
from typing import Any, Dict, List, Optional

from textual.containers import VerticalScroll
from textual.reactive import reactive
from textual.widgets import Static

from ui.models.system_overview import SystemOverview
from ui.core.ui_event import UIEvent, UIEventType


class OmegaOverview(VerticalScroll):
    """Minimal command center — provider, model, goal, task, progress."""

    overview: reactive[Optional[SystemOverview]] = reactive(None, always_update=True)
    current_goal: reactive[str] = reactive("", always_update=True)
    current_task: reactive[str] = reactive("", always_update=True)
    progress: reactive[int] = reactive(0, always_update=True)
    task_summary: reactive[dict] = reactive({}, always_update=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.started_at = time.time()

    def compose(self):
        yield Static(id="overview-content")

    @property
    def _content(self) -> Static:
        return self.query_one("#overview-content", Static)

    def on_mount(self) -> None:
        self.styles.border = ("none", "#030712")
        self.styles.padding = (0, 1)
        self.styles.background = "#030712"
        self._refresh_content()

    def watch_overview(self, val: Optional[SystemOverview]) -> None:
        self._refresh_content()

    def watch_current_goal(self, val: str) -> None:
        self._refresh_content()

    def watch_current_task(self, val: str) -> None:
        self._refresh_content()

    def watch_progress(self, val: int) -> None:
        self._refresh_content()

    def watch_task_summary(self, val: dict) -> None:
        self._refresh_content()

    # ── Phase 11: UIEvent handler ─────────────────────────────────

    def handle_ui_event(self, event: UIEvent) -> None:
        etype: UIEventType = event.type
        data: Dict[str, Any] = event.data

        if etype == UIEventType.OVERVIEW_UPDATED:
            ov = data.get("overview")
            if ov:
                self.overview = ov
                self.current_goal = getattr(ov, "current_goal", "") or ""
                self.progress = getattr(ov, "progress_pct", self.progress)

        # Track current task from lifecycle events
        elif etype in (UIEventType.TASK_STARTED, UIEventType.TASK_ASSIGNED):
            self.current_task = data.get("task_name", data.get("task_name", event.action))
        elif etype == UIEventType.TASK_COMPLETED:
            self.current_task = ""
            # Track completed count
            ts = dict(self.task_summary)
            ts["completed"] = ts.get("completed", 0) + 1
            self.task_summary = ts
        elif etype == UIEventType.TASK_FAILED:
            ts = dict(self.task_summary)
            ts["failed"] = ts.get("failed", 0) + 1
            self.task_summary = ts

    # ── Content Builder ─────────────────────────────────────────

    def _bar(self, percent: int, width: int = 18) -> str:
        filled = max(0, min(width, int(width * percent / 100)))
        return "#" * filled + "." * (width - filled)

    def _uptime_label(self, seconds: float = 0.0) -> str:
        seconds = max(0, int(seconds))
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h {minutes}m"
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def _refresh_content(self) -> None:
        ov = self.overview
        lines: list[str] = []

        lines.append("[bold #c6d4ff]COMMAND CENTER[/]")
        lines.append("")

        if ov is not None:
            # Provider / Model
            lines.append(
                f"[#52627f]provider[/]  [#8fa0c5]{ov.active_provider:<14}[/]"
            )
            lines.append(
                f"[#52627f]model[/]     [#8fa0c5]{ov.active_model:<14}[/]"
            )

            # Active agents (simple count)
            agents_str = f"[#1f8fff]{ov.agents_active}[/]/[#52627f]{ov.agents_idle}[/] active"
            lines.append(f"[#52627f]agents[/]    {agents_str}")

            # Uptime
            uptime_str = self._uptime_label(ov.uptime_seconds)
            lines.append(f"[#52627f]uptime[/]   [#8fa0c5]{uptime_str}[/]")
            lines.append("")

            # Goal
            goal = self.current_goal or getattr(ov, "current_goal", "") or ""
            if goal:
                lines.append(f"[#52627f]goal[/]  [#8fa0c5]{goal[:48]}[/]")

            # Task
            task = self.current_task
            if task:
                lines.append(f"[#52627f]task[/]  [#1f8fff]{task[:48]}[/]")
            lines.append("")

            # Progress bar
            pct = max(0, min(100, self.progress))
            bar = self._bar(pct)
            lines.append(
                f"[#1f8fff]{bar}[/] [bold #1f8fff]{pct:>3}%[/]"
            )
            lines.append("")

            # Mini task summary
            ts = self.task_summary
            if ts:
                parts = []
                if ts.get("completed", 0) > 0:
                    parts.append(f"[#00e38c]{ts['completed']} done[/]")
                if ts.get("failed", 0) > 0:
                    parts.append(f"[#ff5c7a]{ts['failed']} failed[/]")
                if parts:
                    lines.append(f"[#52627f]{'  '.join(parts)}[/]")

        else:
            # Fallback when no overview data
            lines.append("[#52627f]connecting...[/]")
            lines.append("")

        self._content.update("\n".join(lines))
        self.call_after_refresh(self.scroll_home, animate=False)
