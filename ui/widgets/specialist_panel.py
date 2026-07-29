"""
SpecialistPanel — detailed specialist state dashboard.

Shows:
- All specialists with current state (active/inactive/thinking/acting)
- Current activity per specialist
- Recent actions per specialist (last 3)
- Activation scores
"""

from textual.widgets import Static
from textual.reactive import reactive
from collections import defaultdict


SPECIALIST_ORDER = ["HERMES", "ARCHITECT", "ORACLE", "FORGE", "SENTINEL", "TERMINUS", "HERALD"]

SPECIALIST_COLORS = {
    "HERMES": "#4682b4",
    "ORACLE": "#9370db",
    "FORGE": "#2e8b57",
    "SENTINEL": "#cd5c5c",
    "ARCHITECT": "#dda0dd",
    "TERMINUS": "#f5deb3",
    "HERALD": "#6495ed",
}

STATE_COLORS = {
    "active": "#2e8b57",
    "thinking": "#6495ed",
    "acting": "#cd5c5c",
    "inactive": "#666666",
}

STATE_SYMBOLS = {
    "active": "●",
    "thinking": "◐",
    "acting": "►",
    "inactive": "○",
}

MAX_HISTORY = 3


class SpecialistPanel(Static):
    """Widget displaying detailed specialist states, activities, and action history."""

    specialists: reactive[dict] = reactive({}, always_update=True)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#4682b4")
        self.styles.padding = (0, 1)
        self.styles.background = "#1a1a2e"
        self.styles.overflow_y = "auto"

    def watch_specialists(self, val: dict) -> None:
        self._update_content()

    def update_specialist(self, name: str, state: str, activity: str = "", score: float = 0.0) -> None:
        """Update a specialist's state and activity history."""
        data = dict(self.specialists)
        current = data.get(name, {"state": "inactive", "activity": "", "score": 0.0, "history": []})

        # Add to action history if something interesting happened
        history = list(current.get("history", []))
        if activity and state in ("acting", "thinking"):
            history.append({"state": state, "activity": activity[:40], "score": score})
            if len(history) > MAX_HISTORY:
                history = history[-MAX_HISTORY:]

        data[name] = {
            "state": state,
            "activity": activity[:50],
            "score": score,
            "history": history,
        }
        self.specialists = data

    def remove_specialist(self, name: str) -> None:
        data = dict(self.specialists)
        data.pop(name, None)
        self.specialists = data

    def _is_at_bottom(self) -> bool:
        """Check if the scroll position is at the bottom."""
        return self.max_scroll_y <= 0 or self.scroll_y >= self.max_scroll_y

    def _update_content(self) -> None:
        was_at_bottom = self._is_at_bottom()

        lines = [" specialists"]

        for name in SPECIALIST_ORDER:
            info = self.specialists.get(name, {})
            state = info.get("state", "inactive")
            activity = info.get("activity", "")
            score = info.get("score", 0.0)
            history = info.get("history", [])

            state_color = STATE_COLORS.get(state, "#666666")
            sym = STATE_SYMBOLS.get(state, "○")
            spec_color = SPECIALIST_COLORS.get(name, "#ffffff")

            # Specialist name with color and state symbol
            lines.append(f"  [{state_color}]{sym}[/] [{spec_color}]{name.lower()}[/]")

            # Activity line if active
            if state != "inactive" and activity:
                lines.append(f"    [#666666]{activity}[/]")
                if score > 0:
                    lines.append(f"    [#666666]score: {score:.2f}[/]")

            # Show last action from history
            if history:
                last_action = history[-1].get("activity", "")
                if last_action != activity:
                    lines.append(f"    [#444444]last: {last_action[:35]}[/]")

        # Summary stats at bottom
        total = len(SPECIALIST_ORDER)
        active_count = sum(
            1 for name in SPECIALIST_ORDER
            if self.specialists.get(name, {}).get("state") not in ("inactive", None)
        )
        lines.append("")
        lines.append(f"  [#444444]{active_count}/{total} active[/]")

        self.update("\n".join(lines))

        # Auto-scroll to bottom only if user was already at the bottom
        if was_at_bottom:
            self.call_after_refresh(self.scroll_end, animate=False)
