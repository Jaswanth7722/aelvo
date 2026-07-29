"""
work_queue.py — Live Work Queue Widget

Displays the live work queue backed by the Task Board: tasks sorted
by priority and status, showing owner, progress, dependencies,
confidence, and lifecycle stage per the Phase 5 specification.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from rich.text import Text
from rich.style import Style
from textual.reactive import reactive
from textual.widgets import Static

from ui.core.ui_event import UIEvent, UIEventType
from ui.models.work_queue import (
    WorkQueueEntry,
    STATUS_COLORS,
    STATUS_LABELS,
    PRIORITY_COLORS,
    PRIORITY_LABELS,
)


# ── Helpers ─────────────────────────────────────────────────────

def _progress_bar(pct: float, width: int = 18) -> Text:
    """Render a colored progress bar."""
    filled = max(0, min(width, int(pct / 100 * width)))
    empty = width - filled

    if pct >= 100:
        bar_color = "green"
    elif pct >= 70:
        bar_color = "yellow"
    elif pct >= 40:
        bar_color = "blue"
    else:
        bar_color = "dim"

    segments = Text()
    segments.append("█" * filled, style=bar_color)
    segments.append("░" * empty, style="dim")
    segments.append(f" {int(pct)}%", style="bold green" if pct >= 100 else "bold")
    return segments


def _status_badge(status: str) -> Text:
    """Render a colored status badge."""
    color = STATUS_COLORS.get(status, "#64748b")
    label = STATUS_LABELS.get(status, status).upper()
    return Text(f" {label} ", style=Style(bgcolor=color, color="white", bold=True))


def _priority_badge(priority: str) -> Text:
    """Render a priority badge."""
    color = PRIORITY_COLORS.get(priority, "#64748b")
    label = PRIORITY_LABELS.get(priority, "MED")
    return Text(f" {label} ", style=Style(bgcolor=color, color="white", bold=True))



SPECIALIST_COLORS: Dict[str, str] = {
    "HERMES": "#8b5cf6",
    "ARCHITECT": "#3b82f6",
    "ORACLE": "#10b981",
    "FORGE": "#f59e0b",
    "SENTINEL": "#ef4444",
    "TERMINUS": "#06b6d4",
    "HERALD": "#ec4899",
}

DEFAULT_SPECIALIST_COLOR = "#64748b"


def _specialist_tag(specialist: str) -> Text:
    """Render a colored specialist tag."""
    if not specialist:
        return Text("")
    color = SPECIALIST_COLORS.get(specialist.upper(), DEFAULT_SPECIALIST_COLOR)
    return Text(f" @{specialist} ", style=Style(bgcolor=color, color="white", bold=True))


def _task_card(entry: Dict[str, Any], now: float) -> Text:
    """Render a single task as a rich card."""
    t = Text()
    
    # Line 1: Status badge + Priority badge + Specialist tag
    t.append(_status_badge(entry.get("status", "pending")))
    t.append(" ")
    t.append(_priority_badge(entry.get("priority", "medium")))
    
    owner = entry.get("owner", "")
    if owner:
        t.append(" ")
        t.append(_specialist_tag(owner))
    
    # Confidence if available
    conf = entry.get("confidence", 0.0)
    if conf > 0:
        t.append(f" conf:", style="dim")
        pct = int(conf * 100)
        conf_color = "green" if pct >= 80 else "yellow" if pct >= 50 else "red"
        t.append(f" {pct}%", style=f"bold {conf_color}")
    
    # Age
    age = entry.get("age", "")
    if age:
        t.append(f"  {age}", style="dim")
    
    t.append("\n")
    
    # Line 2: Title (truncated)
    title = entry.get("title", "")[:45]
    t.append(f"  {title}", style="bold white")
    
    # Task type if not general
    task_type = entry.get("task_type", "")
    if task_type and task_type != "general":
        t.append(f" [{task_type}]", style="dim italic")
    
    t.append("\n")
    
    # Line 3: Progress bar
    lifecycle = entry.get("lifecycle_progress", 0.0)
    t.append("   ")
    t.append(_progress_bar(lifecycle))
    
    # Dependencies
    deps = entry.get("depends_on", [])
    blocked = entry.get("blocked_by", [])
    dep_parts = []
    if deps:
        dep_parts.append(f"dep:{len(deps)}")
    if blocked:
        dep_parts.append(f"blkd:{len(blocked)}")
    if dep_parts:
        t.append("  ", style="dim")
        t.append(" | ".join(dep_parts), style="dim")
    
    # Error if failed
    error = entry.get("error", "")
    if error:
        t.append("\n")
        t.append(f"  ⚠ {error[:50]}", style="red italic")
    
    return t


class WorkQueueDisplay(Static):
    """Live Work Queue widget displaying tasks backed by the Task Board.

    Shows tasks sorted by priority (critical first) then status
    (active work first), with full detail per entry.
    """

    entries: reactive[list] = reactive([], always_update=True)
    show_completed: bool = False

    def __init__(
        self,
        *args,
        max_visible_active: int = 50,
        max_visible_completed: int = 10,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._max_visible_active = max_visible_active
        self._max_visible_completed = max_visible_completed

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#3b82f6")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"

    def watch_entries(self, entries: list) -> None:
        self.refresh_content(entries)

    def update_from_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Update from a WorkQueueTracker snapshot dict."""
        self.entries = snapshot.get("active", [])[:self._max_visible_active]

    # ── Phase 11: UIEvent handler ─────────────────────────────────

    def handle_ui_event(self, event: UIEvent) -> None:
        """Handle a standardized UIEvent to update the work queue display.

        Responds to WORK_QUEUE_UPDATED events by extracting the
        snapshot data and updating the widget's reactive entries.

        Args:
            event: The UIEvent; only WORK_QUEUE_UPDATED is processed.
        """
        if event.type == UIEventType.WORK_QUEUE_UPDATED:
            snapshot: Dict[str, Any] = event.data
            self.entries = snapshot.get("active", [])[:self._max_visible_active]

    def refresh_content(self, entries: list) -> None:
        """Render the work queue content."""
        if not entries:
            lines = Text()
            lines.append(" work queue", style="bold #3b82f6")
            lines.append("  (0 active)", style="dim")
            lines.append("\n")
            lines.append("\n  ⏳ awaiting tasks", style="dim italic")
            self.update(lines)
            return

        now = time.time()
        lines = Text()

        # Header
        lines.append(" work queue", style="bold #3b82f6")
        lines.append(f"  ({len(entries)} active", style="dim")
        lines.append(")", style="dim")
        lines.append("\n")

        # Render each task card
        for i, entry in enumerate(entries):
            if i > 0:
                lines.append("\n")
            lines.append(_task_card(entry, now))

        # Footer summary
        lines.append("\n")
        active_count = len(entries)
        lines.append(
            f"  {active_count} active · sorted by priority > status",
            style="dim",
        )

        self.update(lines)
