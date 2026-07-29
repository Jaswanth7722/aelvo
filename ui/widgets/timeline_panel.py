"""
TimelinePanel — chronological event timeline for all system activity.

Displays a scrollable, timestamped feed of:
- Task events (created, started, completed, failed)
- Specialist events (activated, thinking, action)
- Collaboration events (findings, challenges, decisions, consensus, executions)
- Verification events (started, passed, failed)
- Tool events (started, completed)
"""

from textual.widgets import Static
from textual.reactive import reactive
import time

MAX_TIMELINE = 100

# Event type colors
TIMELINE_COLORS = {
    "task": "#6495ed",
    "specialist": "#9370db",
    "finding": "#9370db",
    "consumed": "#2e8b57",
    "challenge": "#cd5c5c",
    "consensus": "#ffd700",
    "decision": "#dda0dd",
    "execution": "#f5deb3",
    "verification_pass": "#2e8b57",
    "verification_fail": "#cd5c5c",
    "verification_start": "#6495ed",
    "verification_retry": "#ff8c00",
    "tool": "#6495ed",
    "system": "#666666",
    "report": "#dda0dd",
}

TIMELINE_ICONS = {
    "task_created": "○",
    "task_started": "◐",
    "task_completed": "✓",
    "task_failed": "✗",
    "task_blocked": "◍",
    "task_cancelled": "○",
    "specialist_activated": "●",
    "specialist_thinking": "◐",
    "specialist_action": "►",
    "collaboration_finding": "◈",
    "collaboration_consumed": "◈",
    "collaboration_challenge": "⚡",
    "collaboration_consensus": "◎",
    "collaboration_decision": "◆",
    "collaboration_execution_start": "▸",
    "collaboration_execution_end": "✓",
    "collaboration_report": "■",
    "verification_started": "◐",
    "verification_passed": "✓",
    "verification_failed": "✗",
    "verification_retry": "↻",
    "tool_started": "▸",
    "tool_completed": "✓",
    "tool_failed": "✗",
}


class TimelinePanel(Static):
    """A chronological, scrollable timeline showing all events with timestamps."""

    entries: reactive[list] = reactive([], always_update=True)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#6495ed")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"
        self.styles.overflow_y = "auto"

    def watch_entries(self, entries: list) -> None:
        self._render()

    def add_entry(self, category: str, summary: str, event_type_str: str = "") -> None:
        """Add a timed entry to the timeline.

        Args:
            category: Event category for color coding (task, specialist, finding, etc.)
            summary: Short description
            event_type_str: Specific event type for icon selection
        """
        current = list(self.entries)
        current.append({
            "category": category,
            "summary": summary[:70],
            "event_type": event_type_str,
            "timestamp": time.time(),
        })
        if len(current) > MAX_TIMELINE:
            current = current[-MAX_TIMELINE:]
        self.entries = current

    def clear(self) -> None:
        """Clear all timeline entries."""
        self.entries = []

    def _is_at_bottom(self) -> bool:
        """Check if the scroll position is at the bottom."""
        return self.max_scroll_y <= 0 or self.scroll_y >= self.max_scroll_y

    def _render(self) -> None:
        was_at_bottom = self._is_at_bottom()

        if not self.entries:
            self.update(" timeline\n  [#666666]awaiting events...[/]")
            return

        lines = [" timeline"]
        for entry in self.entries[-25:]:
            category = entry.get("category", "system")
            summary = entry.get("summary", "")
            event_type = entry.get("event_type", "")
            ts = entry.get("timestamp", 0)

            # Format time
            local_t = time.localtime(ts)
            time_str = f"{local_t.tm_hour:02d}:{local_t.tm_min:02d}:{local_t.tm_sec:02d}"

            # Pick icon
            icon = TIMELINE_ICONS.get(event_type, "·")

            # Pick color
            color = TIMELINE_COLORS.get(category, "#6495ed")
            # Override for verification pass/fail
            if category == "verification" and "pass" in summary.lower():
                color = "#2e8b57"
            elif category == "verification" and "fail" in summary.lower():
                color = "#cd5c5c"

            lines.append(f"  [{color}]{icon}[/] [#555555]{time_str}[/] {summary}")

        self.update("\n".join(lines))

        # Auto-scroll to bottom only if user was already at the bottom
        if was_at_bottom:
            self.call_after_refresh(self.scroll_end, animate=False)
