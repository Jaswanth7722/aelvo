"""
AuditLogPanel — filterable, color-coded chronological audit log.

Displays a rich, filterable stream of all collaboration events with:
- Specialist color-coded entries
- Full timestamps (HH:MM:SS) on every entry
- Filtering by event category (collaboration, execution, verification, etc.)
- Filtering by specialist name
- 200+ entry ring buffer
- Clear / filter mode indicator
"""

from textual.widgets import Static
from textual.reactive import reactive
import time

# ── Event Categories for filtering ───────────────────────────────

CATEGORY_COLLABORATION = "collaboration"
CATEGORY_EXECUTION = "execution"
CATEGORY_VERIFICATION = "verification"
CATEGORY_SPECIALIST = "specialist"
CATEGORY_TOOL = "tool"
CATEGORY_SYSTEM = "system"

ALL_CATEGORIES = [
    CATEGORY_COLLABORATION,
    CATEGORY_EXECUTION,
    CATEGORY_VERIFICATION,
    CATEGORY_SPECIALIST,
    CATEGORY_TOOL,
    CATEGORY_SYSTEM,
]

# ── Category → color mapping (matching dark TUI theme) ──────────

CATEGORY_COLORS = {
    CATEGORY_COLLABORATION: "#9370db",
    CATEGORY_EXECUTION: "#f5deb3",
    CATEGORY_VERIFICATION: "#2e8b57",
    CATEGORY_SPECIALIST: "#6495ed",
    CATEGORY_TOOL: "#4682b4",
    CATEGORY_SYSTEM: "#666666",
}

CATEGORY_DISPLAY = {
    CATEGORY_COLLABORATION: "collab",
    CATEGORY_EXECUTION: "exec",
    CATEGORY_VERIFICATION: "verify",
    CATEGORY_SPECIALIST: "agents",
    CATEGORY_TOOL: "tool",
    CATEGORY_SYSTEM: "system",
}

# ── Specialist color palette ─────────────────────────────────────

SPECIALIST_COLORS = {
    "ORACLE": "#9370db",
    "FORGE": "#2e8b57",
    "SENTINEL": "#cd5c5c",
    "ARCHITECT": "#dda0dd",
    "TERMINUS": "#f5deb3",
    "HERALD": "#6495ed",
    "HERMES": "#4682b4",
    "CONSENSUS": "#ffd700",
}

# ── Event type → icon mapping ───────────────────────────────────

EVENT_ICONS = {
    "finding": "◈",
    "consumed": "◈",
    "challenge": "⚡",
    "consensus": "◎",
    "decision": "◆",
    "execution_start": "▸",
    "execution_end": "✓",
    "execution_fail": "✗",
    "report": "■",
    "task_created": "○",
    "task_started": "◐",
    "task_completed": "✓",
    "task_failed": "✗",
    "task_blocked": "◍",
    "specialist_active": "●",
    "specialist_thinking": "◐",
    "specialist_action": "►",
    "tool_start": "▸",
    "tool_complete": "✓",
    "tool_fail": "✗",
    "verification_pass": "✓",
    "verification_fail": "✗",
    "verification_run": "◐",
    "verification_retry": "↻",
    "system": "·",
}

MAX_ENTRIES = 250

# ── Mapping from event_type_str to category ──────────────────────

EVENT_TYPE_TO_CATEGORY = {
    # Collaboration events
    "collaboration_finding": CATEGORY_COLLABORATION,
    "collaboration_consumed": CATEGORY_COLLABORATION,
    "collaboration_challenge": CATEGORY_COLLABORATION,
    "collaboration_consensus": CATEGORY_COLLABORATION,
    "collaboration_decision": CATEGORY_COLLABORATION,
    "collaboration_execution_start": CATEGORY_COLLABORATION,
    "collaboration_execution_end": CATEGORY_COLLABORATION,
    "collaboration_report": CATEGORY_COLLABORATION,
    # Specialist events
    "specialist_activated": CATEGORY_SPECIALIST,
    "specialist_thinking": CATEGORY_SPECIALIST,
    "specialist_action": CATEGORY_SPECIALIST,
    "specialist_deactivated": CATEGORY_SPECIALIST,
    # Task / execution events
    "task_created": CATEGORY_EXECUTION,
    "task_started": CATEGORY_EXECUTION,
    "task_completed": CATEGORY_EXECUTION,
    "task_failed": CATEGORY_EXECUTION,
    "task_blocked": CATEGORY_EXECUTION,
    "task_cancelled": CATEGORY_EXECUTION,
    "task_progress": CATEGORY_EXECUTION,
    # Tool events
    "tool_started": CATEGORY_TOOL,
    "tool_completed": CATEGORY_TOOL,
    "tool_failed": CATEGORY_TOOL,
    # Verification events
    "verification_started": CATEGORY_VERIFICATION,
    "verification_passed": CATEGORY_VERIFICATION,
    "verification_failed": CATEGORY_VERIFICATION,
    "verification_retry": CATEGORY_VERIFICATION,
    # System
    "system_startup": CATEGORY_SYSTEM,
    "system_shutdown": CATEGORY_SYSTEM,
    "system": CATEGORY_SYSTEM,
}


def _resolve_icon(event_type_str: str) -> str:
    """Pick the best icon for an event type string."""
    if not event_type_str:
        return "·"
    # Try exact match
    if event_type_str in EVENT_ICONS:
        return EVENT_ICONS[event_type_str]
    # Try suffix-based
    for suffix, icon in [
        ("finding", "◈"), ("consumed", "◈"), ("challenge", "⚡"),
        ("consensus", "◎"), ("decision", "◆"), ("report", "■"),
        ("execution_end", "✓"), ("execution_start", "▸"), ("execution", "▸"),
        ("started", "▸"), ("completed", "✓"), ("failed", "✗"),
        ("passed", "✓"), ("retry", "↻"), ("start", "◐"),
        ("thinking", "◐"), ("activated", "●"), ("action", "►"),
    ]:
        if suffix in event_type_str:
            return icon
    return "·"


class AuditLogPanel(Static):
    """Filterable, color-coded chronological audit log of all collaboration events.

    Features:
    - Specialist color-coded entries (ORACLE=purple, SENTINEL=red, etc.)
    - Timestamps on every entry
    - Category-based filtering (collaboration, execution, verification, etc.)
    - Specialist-based filtering
    - 250-entry ring buffer
    - Clear filter indicator in header
    """

    entries: reactive[list] = reactive([], always_update=True)
    filter_category: reactive[str] = reactive("")
    filter_specialist: reactive[str] = reactive("")
    entry_count: reactive[int] = reactive(0)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#9370db")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"
        self.styles.overflow_y = "auto"

    def watch_entries(self, val: list) -> None:
        self._render()

    def watch_filter_category(self, val: str) -> None:
        self._render()

    def watch_filter_specialist(self, val: str) -> None:
        self._render()

    # ── Public API ──────────────────────────────────────────────

    def add_entry(
        self,
        category: str,
        summary: str,
        event_type_str: str = "",
        specialist: str = "",
    ) -> None:
        """Add a timestamped entry to the audit log.

        Args:
            category: Event category for color coding (collaboration, execution, etc.)
            summary: Short description (70 chars max)
            event_type_str: Specific event type string for icon selection + category inference
            specialist: Specialist name for color-coding (optional)
        """
        # Infer category from event type if not explicitly set
        if not category or category == "specialist":
            inferred = EVENT_TYPE_TO_CATEGORY.get(event_type_str, "")
            if inferred:
                category = inferred

        current = list(self.entries)
        current.append({
            "category": category,
            "summary": summary[:80],
            "event_type": event_type_str,
            "specialist": specialist,
            "timestamp": time.time(),
        })
        if len(current) > MAX_ENTRIES:
            current = current[-MAX_ENTRIES:]
        self.entries = current
        self.entry_count = len(current)

    def set_filter_category(self, category: str) -> None:
        """Set the active category filter. Empty string = show all."""
        self.filter_category = category

    def set_filter_specialist(self, specialist: str) -> None:
        """Set the active specialist filter. Empty string = show all."""
        self.filter_specialist = specialist

    def clear_filters(self) -> None:
        """Clear all filters and show all entries."""
        self.filter_category = ""
        self.filter_specialist = ""

    def clear(self) -> None:
        """Clear all entries from the audit log."""
        self.entries = []
        self.entry_count = 0

    # ── Rendering ───────────────────────────────────────────────

    def _get_filtered_entries(self) -> list:
        """Return entries matching the active filters."""
        if not self.filter_category and not self.filter_specialist:
            return self.entries

        result = []
        for entry in self.entries:
            cat = entry.get("category", "")
            specialist = entry.get("specialist", "")

            if self.filter_category and cat != self.filter_category:
                continue
            if self.filter_specialist and specialist.upper() != self.filter_specialist.upper():
                continue
            result.append(entry)

        return result

    def _is_at_bottom(self) -> bool:
        """Check if the scroll position is at the bottom."""
        return self.max_scroll_y <= 0 or self.scroll_y >= self.max_scroll_y

    def _render(self) -> None:
        was_at_bottom = self._is_at_bottom()

        filtered = self._get_filtered_entries()
        lines = []

        # ── Header with filter indicator ──
        parts = ["audit log"]
        if self.filter_category:
            parts.append(f"[ffd700]@{self.filter_category[:8]}[/]")
        if self.filter_specialist:
            parts.append(f"[ffd700]@{self.filter_specialist[:8]}[/]")
        total = len(self.entries)
        shown = len(filtered)
        if self.filter_category or self.filter_specialist:
            parts.append(f"[#555555]{shown}/{total}[/]")
        else:
            parts.append(f"[#555555]{total}[/]")
        lines.append(f"  {' '.join(parts)}")

        if not filtered:
            lines.append("  [#666666]awaiting events...[/]")
            self.update("\n".join(lines))
            return

        # ── Render filtered entries (latest at bottom) ──
        for entry in filtered[-30:]:
            category = entry.get("category", "system")
            summary = entry.get("summary", "")
            event_type = entry.get("event_type", "")
            specialist = entry.get("specialist", "")
            ts = entry.get("timestamp", 0)

            # Timestamp
            local_t = time.localtime(ts)
            time_str = f"{local_t.tm_hour:02d}:{local_t.tm_min:02d}:{local_t.tm_sec:02d}"

            # Icon
            icon = _resolve_icon(event_type)

            # Specialist color → entry color
            if specialist:
                color = SPECIALIST_COLORS.get(specialist.upper(), CATEGORY_COLORS.get(category, "#6495ed"))
            else:
                color = CATEGORY_COLORS.get(category, "#6495ed")

            # Override for verification pass/fail
            if category == CATEGORY_VERIFICATION:
                if "pass" in summary.lower():
                    color = "#2e8b57"
                elif "fail" in summary.lower():
                    color = "#cd5c5c"
                elif "retry" in summary.lower():
                    color = "#ff8c00"

            # Build the line
            if specialist:
                spec_color = SPECIALIST_COLORS.get(specialist.upper(), "#6495ed")
                line = (
                    f"  [{color}]{icon}[/] "
                    f"[#555555]{time_str}[/] "
                    f"[{spec_color}]{specialist[:8]}[/] "
                    f"{summary}"
                )
            else:
                line = (
                    f"  [{color}]{icon}[/] "
                    f"[#555555]{time_str}[/] "
                    f"{summary}"
                )

            lines.append(line)

        self.update("\n".join(lines))

        # Auto-scroll to bottom only if user was already at the bottom
        if was_at_bottom:
            self.call_after_refresh(self.scroll_end, animate=False)
