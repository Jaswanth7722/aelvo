"""permission_dialog.py — Permission dialog for tool calls

Shows when AELVO wants to execute a tool that requires approval.
"""

from textual.screen import ModalScreen
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.widgets import Static


class PermissionDialog(ModalScreen):
    """Permission dialog for tool execution."""

    CSS = """
    PermissionDialog {
        align: center middle;
    }

    #perm-dialog-box {
        width: 60;
        height: auto;
        background: #161b22;
        border: solid #30363d;
        padding: 1 2;
    }

    #perm-dialog-title {
        height: 1;
        color: #f0f6fc;
        text-style: bold;
        margin-bottom: 1;
    }

    #perm-dialog-detail {
        height: auto;
        color: #c9d1d9;
        margin-bottom: 1;
    }

    #perm-dialog-actions {
        height: 1;
        color: #8b949e;
    }
    """

    BINDINGS = [
        Binding("escape", "deny", "Deny"),
        Binding("a", "allow", "Allow"),
        Binding("A", "allow_session", "Allow Session"),
        Binding("d", "deny", "Deny"),
        Binding("enter", "allow", "Allow"),
        Binding("left", "move_left", "Left", show=False),
        Binding("right", "move_right", "Right", show=False),
    ]

    def __init__(self, tool_name: str, command: str, callback=None):
        super().__init__()
        self.tool_name = tool_name
        self.command = command
        self._callback = callback
        self._selected = 0  # 0=allow, 1=allow_session, 2=deny

    def compose(self):
        with Vertical(id="perm-dialog-box"):
            yield Static(" PERMISSION REQUIRED", id="perm-dialog-title")
            yield Static(id="perm-dialog-detail")
            yield Static(id="perm-dialog-actions")

    def on_mount(self) -> None:
        self.query_one("#perm-dialog-detail", Static).update(
            f" tool: [#58a6ff]{self.tool_name}[/]\n"
            f" cmd:  [#c9d1d9]{self.command[:50]}[/]"
        )
        self._render_actions()

    def _render_actions(self) -> None:
        options = ["allow", "allow session", "deny"]
        parts = []
        for i, opt in enumerate(options):
            if i == self._selected:
                parts.append(f"[#f0f6fc bold] {opt} [/]")
            else:
                parts.append(f"[#8b949e] {opt} [/]")
        self.query_one("#perm-dialog-actions", Static).update("  ".join(parts))

    def action_move_left(self) -> None:
        self._selected = max(0, self._selected - 1)
        self._render_actions()

    def action_move_right(self) -> None:
        self._selected = min(2, self._selected + 1)
        self._render_actions()

    def action_allow(self) -> None:
        if self._callback:
            self._callback("allow")
        self.dismiss(True)

    def action_allow_session(self) -> None:
        if self._callback:
            self._callback("allow_session")
        self.dismiss(True)

    def action_deny(self) -> None:
        if self._callback:
            self._callback("deny")
        self.dismiss(False)
