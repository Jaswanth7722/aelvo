"""file_changes.py — File changes panel (Ctrl+L)

Shows files modified during the current session.
"""

from textual.widgets import Static


class FileChangesPanel(Static):
    """File changes tracking panel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._files: list[dict] = []

    def on_mount(self) -> None:
        self.styles.background = "#161b22"
        self.styles.color = "#8b949e"
        self.styles.padding = (0, 1)
        self.update(" FILES")

    def add_change(self, path: str, change_type: str = "modified") -> None:
        self._files.append({"path": path, "type": change_type})
        self._render()

    def clear_changes(self) -> None:
        self._files.clear()
        self._render()

    def _render(self) -> None:
        if not self._files:
            self.update(" FILES\n\n [#8b949e]no changes yet[/]")
            return

        lines = [" FILES", ""]
        icons = {"modified": "~", "created": "+", "deleted": "-", "renamed": ">"}
        for f in self._files[-10:]:
            icon = icons.get(f["type"], "?")
            lines.append(f" [#58a6ff]{icon}[/] {f['path']}")
        if len(self._files) > 10:
            lines.append(f" [#8b949e]...{len(self._files) - 10} more[/]")
        self.update("\n".join(lines))
