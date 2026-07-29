from textual.widgets import Static
from textual.reactive import reactive
from rich.text import Text


class ExecutionFeed(Static):
    entries: reactive[list] = reactive([], always_update=True)

    def on_mount(self) -> None:
        self.styles.border = ("solid", "#4682b4")
        self.styles.padding = (0, 1)
        self.styles.overflow_y = "auto"
        self.styles.background = "#1a1a2e"

    def watch_entries(self, entries: list) -> None:
        self.render_content(entries)

    def add_entry(self, category: str, message: str) -> None:
        current = list(self.entries)
        current.append({"category": category, "message": message})
        if len(current) > 200:
            current = current[-200:]
        self.entries = current

    def _is_at_bottom(self) -> bool:
        """Check if the scroll position is at the bottom."""
        return self.max_scroll_y <= 0 or self.scroll_y >= self.max_scroll_y

    def render_content(self, entries: list) -> None:
        was_at_bottom = self._is_at_bottom()

        if not entries:
            self.update("")
            return

        lines = [" activity"]
        for e in entries[-25:]:
            cat = e.get("category", "")
            msg = e.get("message", "")[:70]
            
            # Skip auth-related messages
            if "auth" in msg.lower():
                continue
                
            color = "#2e8b57" if cat == "success" else "#cd5c5c" if cat == "error" else "#f5deb3" if cat == "warning" else "#6495ed"
            prefix = "✓" if cat == "success" else "!" if cat == "error" else "·"
            lines.append(f"  [{color}]{prefix}[/] {msg}")

        self.update("\n".join(lines))

        # Auto-scroll to bottom only if user was already at the bottom
        if was_at_bottom:
            self.call_after_refresh(self.scroll_end, animate=False)
