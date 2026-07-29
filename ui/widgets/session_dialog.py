"""session_dialog.py — Session switcher overlay (Ctrl+A)

Lists sessions, allows switching between them.
"""

from textual.screen import ModalScreen
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import Static


class SessionDialog(ModalScreen):
    """Session switcher dialog."""

    CSS = """
    SessionDialog {
        align: center middle;
    }

    #session-dialog-box {
        width: 40;
        height: auto;
        max-height: 30;
        background: #161b22;
        border: solid #30363d;
        padding: 1 2;
    }

    #session-dialog-title {
        height: 1;
        color: #f0f6fc;
        text-style: bold;
        margin-bottom: 1;
    }

    #session-dialog-list {
        height: auto;
        max-height: 24;
        color: #c9d1d9;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("up", "prev", "Previous"),
        Binding("down", "next", "Next"),
        Binding("enter", "select", "Select"),
        Binding("k", "prev", "Previous", show=False),
        Binding("j", "next", "Next", show=False),
    ]

    def __init__(self, sessions: list[str], current: str, callback=None):
        super().__init__()
        self.sessions = sessions
        self.current = current
        self._selected = sessions.index(current) if current in sessions else 0
        self._callback = callback

    def compose(self):
        with Vertical(id="session-dialog-box"):
            yield Static(" SESSIONS", id="session-dialog-title")
            yield Static(id="session-dialog-list")

    def on_mount(self) -> None:
        self._render_list()

    def _render_list(self) -> None:
        lines = []
        for i, s in enumerate(self.sessions):
            marker = ">" if i == self._selected else " "
            color = "#f0f6fc" if i == self._selected else "#8b949e"
            lines.append(f" [{color}]{marker} {s}[/]")
        self.query_one("#session-dialog-list", Static).update("\n".join(lines))

    def action_prev(self) -> None:
        self._selected = max(0, self._selected - 1)
        self._render_list()

    def action_next(self) -> None:
        self._selected = min(len(self.sessions) - 1, self._selected + 1)
        self._render_list()

    def action_select(self) -> None:
        if self._callback:
            self._callback(self.sessions[self._selected])
        self.dismiss()

    def action_close(self) -> None:
        self.dismiss()
