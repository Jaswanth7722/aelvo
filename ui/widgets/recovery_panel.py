"""ui/widgets/recovery_panel.py — Recovery Visibility Panel

Phase 8: Expose recovery events as a first-class UI concept.
Shows provider failures, fallback activations, retry attempts,
specialist reassignments, and recovery outcomes.

Users see resilience in real time.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from rich.text import Text
from rich.style import Style
from textual.reactive import reactive
from textual.widgets import Static

from ui.core.ui_event import UIEvent, UIEventType


# Recovery event type colors matching the tracker
RECOVERY_EVENT_COLORS: Dict[str, str] = {
    "provider_failure": "#ff5c7a",
    "fallback_activated": "#f7b731",
    "retry_started": "#3b82f6",
    "recovery_successful": "#00e38c",
    "recovery_failed": "#ff5c7a",
    "specialist_reassigned": "#a565ff",
}

RECOVERY_EVENT_ICONS: Dict[str, str] = {
    "provider_failure": "⛔",
    "fallback_activated": "🔀",
    "retry_started": "🔄",
    "recovery_successful": "✅",
    "recovery_failed": "❌",
    "specialist_reassigned": "🔄",
}

RECOVERY_EVENT_LABELS: Dict[str, str] = {
    "provider_failure": "PROVIDER FAILURE",
    "fallback_activated": "FALLBACK",
    "retry_started": "RETRY",
    "recovery_successful": "RECOVERED",
    "recovery_failed": "RECOVERY FAILED",
    "specialist_reassigned": "REASSIGNED",
}


def _render_entry(entry: Dict[str, Any], now: float) -> Text:
    """Render a single recovery event entry."""
    t = Text()

    event_type = entry.get("event_type", "")
    event_color = RECOVERY_EVENT_COLORS.get(event_type, "#f7b731")
    icon = RECOVERY_EVENT_ICONS.get(event_type, "⚡")
    label = RECOVERY_EVENT_LABELS.get(event_type, event_type.upper())

    # Icon + status label badge
    t.append(f" {icon}", style=event_color)
    t.append(f" [{label}] ", style=Style(bgcolor=event_color, color="white", bold=True))
    t.append(" ")

    # Specialist if present
    specialist = entry.get("specialist", "")
    if specialist:
        t.append(f"{specialist}", style=f"bold {event_color}")
        t.append(" ")

    # Summary
    summary = entry.get("summary", "")[:45]
    if summary:
        t.append(f"{summary}", style="white")

    # Age
    display_age = entry.get("display_age", "")
    if display_age:
        t.append(f"  {display_age}", style="dim")

    t.append("\n")

    # Detail line (if available)
    detail = entry.get("detail", "")
    if detail:
        t.append(f"  {detail[:55]}", style="dim italic")
        # Classification
        classification = entry.get("classification", "")
        if classification:
            t.append(f"  [{classification}]", style="dim")
        t.append("\n")

    # Retry count & duration
    retry_count = entry.get("retry_count", 0)
    duration = entry.get("duration", 0.0)
    extra_parts = []
    if retry_count > 0:
        extra_parts.append(f"attempt #{retry_count}")
    if duration > 0:
        extra_parts.append(f"took {duration}s")
    if extra_parts:
        t.append(f"  {' · '.join(extra_parts)}", style="dim")

    # Node ID
    node_id = entry.get("node_id", "")
    if node_id:
        t.append(f"  node:{node_id[:8]}", style="dim")

    return t


class RecoveryPanel(Static):
    """Recovery Visibility Panel.

    Displays recovery events in real time: provider failures,
    fallback activations, retry attempts, specialist reassignments,
    and recovery outcomes.
    """

    recovery_data: reactive[dict] = reactive({}, always_update=True)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#f7b731")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"

    def watch_recovery_data(self, data: dict) -> None:
        self.refresh_content(data)

    def update_from_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Update from a RecoveryTracker snapshot."""
        self.recovery_data = snapshot

    # ── Phase 11: UIEvent handler ─────────────────────────────────

    def handle_ui_event(self, event: UIEvent) -> None:
        """Handle a standardized UIEvent to update recovery display.

        Responds to RECOVERY_UPDATED events by refreshing the
        recovery panel with the latest snapshot data.

        Args:
            event: The UIEvent; only RECOVERY_UPDATED is processed.
        """
        if event.type == UIEventType.RECOVERY_UPDATED:
            self.recovery_data = event.data

    def refresh_content(self, data: dict) -> None:
        """Render the recovery panel content."""
        recent = data.get("recent", [])
        summary = data.get("summary", {})

        if not recent:
            lines = Text()
            lines.append(" recovery", style="bold #f7b731")
            lines.append("  (0 events)", style="dim")
            lines.append("\n")
            lines.append("\n  ⏳ awaiting recovery events", style="dim italic")
            self.update(lines)
            return

        now = time.time()
        lines = Text()

        # ── Header ──
        total = summary.get("total", 0)
        succeeded = summary.get("succeeded", 0)
        failed = summary.get("failed", 0)
        success_rate = summary.get("success_rate", 0.0)

        lines.append(" recovery", style="bold #f7b731")
        lines.append(f"  ({total} events", style="dim")
        if succeeded:
            lines.append(f"  [green]{succeeded} ok[/]", style="dim")
        if failed:
            lines.append(f"  [red]{failed} fail[/]", style="dim")
        if success_rate > 0:
            pct = int(success_rate * 100)
            rate_color = "green" if pct >= 80 else "yellow" if pct >= 50 else "red"
            lines.append(f"  [{rate_color}]{pct}% success[/]", style="dim")
        lines.append(")", style="dim")
        lines.append("\n")

        # By-type breakdown
        by_type = summary.get("by_type", {})
        if by_type:
            type_parts = []
            for etype, count in sorted(by_type.items(), key=lambda x: -x[1]):
                color = RECOVERY_EVENT_COLORS.get(etype, "#f7b731")
                type_parts.append(f"[{color}]{etype.replace('_', ' ')}: {count}[/]")
            if type_parts:
                lines.append("  ", style="dim")
                lines.append(" · ".join(type_parts), style="dim")
                lines.append("\n")

        # ── Recent events ──
        for entry in recent[-10:]:
            lines.append("\n")
            lines.append(_render_entry(entry, now))

        self.update(lines)
