"""header_bar.py — Minimal header bar (OpenCode style)

Single line: AELVO  provider/model  session:name
"""

from textual.widgets import Static


class HeaderBar(Static):
    """Minimal one-line header bar."""

    def on_mount(self) -> None:
        self.styles.dock = "top"
        self.styles.height = 1
        self.styles.background = "#161b22"
        self.styles.color = "#8b949e"
        self.styles.padding = (0, 1)
