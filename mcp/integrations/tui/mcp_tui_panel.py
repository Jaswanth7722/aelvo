"""MCP TUI panel for displaying MCP platform status in the AELVO terminal interface."""

from __future__ import annotations

from typing import List
from rich.table import Table
from rich.text import Text
from textual.reactive import reactive
from textual.widgets import Static


class MCPTUIPanel(Static):
    """Textual widget to display MCP Platform status in real-time."""

    servers: reactive[list] = reactive([], always_update=True)
    active_calls: reactive[list] = reactive([], always_update=True)
    recent_events: reactive[list] = reactive([], always_update=True)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.border_title = "MCP Platform"

    def watch_servers(self, servers: list) -> None:
        self.render_content()

    def watch_active_calls(self, active_calls: list) -> None:
        self.render_content()

    def watch_recent_events(self, recent_events: list) -> None:
        self.render_content()

    def render_content(self) -> None:
        table = Table.grid(padding=(0, 1), expand=True)
        table.add_column(ratio=1)

        # 1. Connected Servers
        conn_count = sum(1 for s in self.servers if s.get("connected", False))
        total_count = len(self.servers)
        table.add_row(
            Text(f"  CONNECTED SERVERS ({conn_count}/{total_count})", style="bold white")
        )
        table.add_row(
            Text("  " + "─" * 58, style="grey37")
        )

        if not self.servers:
            table.add_row(Text("    No registered servers", style="grey50 italic"))
        else:
            for s in self.servers:
                enabled = s.get("enabled", True)
                connected = s.get("connected", False)
                blocked = s.get("trust_level") == "blocked"

                if blocked:
                    bullet = "[bold red]✕[/]"
                    health_str = "—"
                elif not enabled:
                    bullet = "[grey50]○[/]"
                    health_str = "(disabled)"
                elif connected:
                    bullet = "[bold green]●[/]"
                    health_str = s.get("health", "HEALTHY").upper()
                else:
                    bullet = "[yellow]●[/]"
                    health_str = s.get("health", "UNKNOWN").upper()

                health_style = "green" if health_str == "HEALTHY" else (
                    "yellow" if health_str == "DEGRADED" else "red"
                )

                name = f"{s.get('id', ''):<18}"
                trust = f"{s.get('trust_level', 'SANDBOXED').upper():<10}"
                health = f"[{health_style}]{health_str:<10}[/]"
                tools = f"{s.get('tools_count', 0)} tools available" if enabled and not blocked else ""

                table.add_row(
                    f"    {bullet} {name}  {trust}  {health}  {tools}"
                )

        table.add_row("")

        # 2. Active Calls
        table.add_row(Text("  ACTIVE CALLS", style="bold white"))
        table.add_row(Text("  " + "─" * 58, style="grey37"))
        if not self.active_calls:
            table.add_row(Text("    No active calls", style="grey50 italic"))
        else:
            for call in self.active_calls:
                spec = f"[{call.get('specialist_color', 'cyan')}][{call.get('specialist', 'ORACLE')}][/]"
                server = call.get("server_id", "")
                tool = call.get("tool_name", "")
                dur = call.get("duration", 0.0)
                table.add_row(f"    {spec} → {server}:{tool}  ({dur:.1f}s)")

        table.add_row("")

        # 3. Recent Events
        table.add_row(Text("  RECENT EVENTS", style="bold white"))
        table.add_row(Text("  " + "─" * 58, style="grey37"))
        if not self.recent_events:
            table.add_row(Text("    No recent events", style="grey50 italic"))
        else:
            for ev in self.recent_events[-5:]:
                time_str = ev.get("time", "")
                event_type = ev.get("type", "")
                detail = ev.get("detail", "")
                table.add_row(f"    [grey50]{time_str}[/]  [white]{event_type}[/]  {detail}")

        self.update(table)
