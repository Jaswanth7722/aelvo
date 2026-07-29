"""chat_view.py — Clean chat view for user/AI messages

OpenCode-style chat layout:
- User messages on the right with blue accent
- AI responses on the left with green accent
- System messages centered and dimmed
- Tool call results inline
- Markdown-like formatting
"""

import time
from typing import Optional

from rich.markup import escape
from textual.containers import VerticalScroll
from textual.widgets import Static


class ChatMessage:
    """A single chat message."""

    def __init__(self, role: str, content: str, timestamp: float = 0.0):
        self.role = role  # "user", "assistant", "system", "error"
        self.content = content
        self.timestamp = timestamp or time.time()


class ChatView(VerticalScroll):
    """Clean chat view — user messages, AI responses, system info."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._messages: list[ChatMessage] = []
        self._rendered_count = 0

    def compose(self):
        yield Static(id="chat-content")

    @property
    def _content(self) -> Static:
        return self.query_one("#chat-content", Static)

    def on_mount(self) -> None:
        self.styles.background = "#0d1117"
        self.styles.padding = (0, 1)
        self._render()

    # ── Public API ──────────────────────────────────────────

    def add_user(self, text: str) -> None:
        self._messages.append(ChatMessage("user", text))
        self._render()

    def add_assistant(self, text: str) -> None:
        self._messages.append(ChatMessage("assistant", text))
        self._render()

    def add_system(self, text: str) -> None:
        self._messages.append(ChatMessage("system", text))
        self._render()

    def add_error(self, text: str) -> None:
        self._messages.append(ChatMessage("error", text))
        self._render()

    def clear(self) -> None:
        self._messages.clear()
        self._render()

    # ── Rendering ───────────────────────────────────────────

    def _render(self) -> None:
        lines: list[str] = []

        if not self._messages:
            lines.append("")
            lines.append("  [#58a6ff]AELVO[/]  [#8b949e]ready[/]")
            lines.append("")
            lines.append("  [#8b949e]type a message to start[/]")
            self._content.update("\n".join(lines))
            return

        for msg in self._messages:
            lines.extend(self._render_message(msg))
            lines.append("")

        self._content.update("\n".join(lines))

        # Auto-scroll to bottom
        if self._messages and self._messages[-1].role in ("user", "assistant"):
            self.call_after_refresh(self.scroll_end, animate=False)

    def _render_message(self, msg: ChatMessage) -> list[str]:
        ts = time.strftime("%H:%M", time.localtime(msg.timestamp))
        lines: list[str] = []

        if msg.role == "user":
            lines.append(f"  [#58a6ff bold]you[/] [#8b949e]{ts}[/]")
            lines.append("")
            for line in msg.content.splitlines()[:20]:
                lines.append(f"  {escape(line)}")
            if msg.content.count("\n") > 20:
                lines.append("  [#8b949e]...[/]")

        elif msg.role == "assistant":
            lines.append(f"  [#3fb950 bold]aelvo[/] [#8b949e]{ts}[/]")
            lines.append("")
            for line in msg.content.splitlines()[:50]:
                lines.append(f"  {escape(line)}")
            if msg.content.count("\n") > 50:
                lines.append("  [#8b949e]...[/]")

        elif msg.role == "system":
            lines.append(f"  [#8b949e italic]{escape(msg.content)}[/]")

        elif msg.role == "error":
            lines.append(f"  [#f85149]{escape(msg.content)}[/]")

        return lines
