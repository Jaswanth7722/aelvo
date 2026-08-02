"""
session.py — Terminal session objects for the AELVO CLI.

Two classes live here:

* ``TerminalSession`` — the object handed to ``orchestrator.execute_turn()``
  as ``tui_session``. The orchestrator's tool loop calls ``emit_tool`` /
  ``emit_memory`` / ``emit_system`` on it as the agent works, so the CLI can
  render live tool activity (Claude Code / CodeBuff style) without touching
  the orchestrator. The same ``stream_callback`` hook delivers the final
  answer.

* ``SessionRecorder`` — a condensed per-turn interaction record persisted to
  SQLite, mirroring the recorder the web bridge uses.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, List, Optional

from rich.console import Console, Group
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text

from runtime_next.models.events import EventType as RuntimeEventType

# Tool → emoji icon shown in the live activity feed.
TOOL_ICONS = {
    "read_file": "📖",
    "read_file_range": "📖",
    "write_file": "✏️",
    "edit_file": "✏️",
    "bash_exec": "⚙️",
    "python_exec": "🐍",
    "light_scrape": "🌐",
    "heavy_crawl": "🌐",
    "search_memory": "🧠",
    "save_constraint": "📌",
    "list_files": "📂",
    "find_files": "🔍",
    "grep_file": "🔍",
    "search_code": "🔍",
    "project_tree": "🌳",
    "scaffold_website": "🛠️",
    "hash_file": "#️⃣",
    "respond": "💬",
    "kernel": "⚡",
}

_DEFAULT_ICON = "🔧"
_THINKING = "AELVO is thinking…"


class TerminalSession:
    """Renders the agent's live activity into a ``rich.Live`` region.

    Implements exactly the ``tui_session`` interface the orchestrator's tool
    loop expects (``emit_system`` / ``emit_tool`` / ``emit_memory``), so the
    canonical ``execute_turn`` path renders into the terminal unchanged.
    """

    def __init__(self, console: Console):
        self.console = console
        self._lines: List[Any] = []
        self._live: Optional[Live] = None
        self._status_text = _THINKING
        self.final_answer = ""

    # ── lifecycle ──────────────────────────────────────────────────────────
    def start(self) -> None:
        self._live = Live(
            self._renderable(),
            console=self.console,
            refresh_per_second=12,
            transient=False,
        )
        self._live.start()

    def finish(self) -> None:
        if self._live is not None:
            self._live.stop()
            self._live = None

    # ── rendering helpers ──────────────────────────────────────────────────
    def _renderable(self) -> Any:
        if self._status_text:
            return Group(
                *self._lines,
                Spinner("dots", text=self._status_text, style="aelvo.brand"),
            )
        return Group(*self._lines)

    def _refresh(self) -> None:
        if self._live is not None:
            self._live.update(self._renderable())

    # ── stream_callback hook (final answer) ────────────────────────────────
    def on_final_answer(self, message: str) -> None:
        """Store the agent's final message; the REPL prints it as Markdown."""
        self.final_answer = message or ""
        self._status_text = ""
        self._refresh()

    # ── orchestrator tui_session hooks ─────────────────────────────────────
    async def emit_system(self, message: str) -> None:
        """System/kernel lines. The orchestrator also echoes the final answer
        here (``AELVO: ...``); the stream_callback hook (``on_final_answer``)
        is the canonical capture, so only clear the spinner here — no line."""
        if message.startswith("AELVO:"):
            self._status_text = ""
            self._refresh()
            return
        self._lines.append(Text(f"⚙ {message}", style="aelvo.dim"))
        self._refresh()

    async def emit_tool(
        self,
        event_type,
        tool_name: str,
        args_preview: str = "",
        status: str = "running",
        exit_code: int = 0,
    ) -> None:
        icon = TOOL_ICONS.get(tool_name, _DEFAULT_ICON)
        preview = str(args_preview or "").strip()

        if event_type == RuntimeEventType.TOOL_STARTED:
            self._status_text = f"{icon} {tool_name} {preview}"
        elif event_type == RuntimeEventType.TOOL_FAILED:
            self._lines.append(
                Text(f"✗ {icon} {tool_name} {preview}", style="aelvo.err")
            )
            self._status_text = _THINKING
        else:  # TOOL_COMPLETED
            marker = "✓" if int(exit_code or 0) == 0 else "✗"
            style = "aelvo.ok" if marker == "✓" else "aelvo.err"
            self._lines.append(Text(f"{marker} {icon} {tool_name} {preview}", style=style))
            self._status_text = _THINKING
        self._refresh()

    async def emit_memory(
        self,
        event_type,
        mem_type: str,
        query: str,
        count: int = 0,
        score: float = 0.0,
    ) -> None:
        if event_type == RuntimeEventType.MEMORY_STORED:
            self._lines.append(
                Text(f"🧠 stored {mem_type}: {query}", style="aelvo.dim")
            )
        else:
            self._lines.append(
                Text(f"🧠 retrieved {count} hit(s) for '{query}'", style="aelvo.dim")
            )
        self._refresh()


class SessionRecorder:
    """Condensed interaction record (query → tools → answer) saved to SQLite.

    Mirrors the interface the orchestrator expects from ``session_tracker``
    (``record_tool`` / ``record_answer`` / ``save``).
    """

    def __init__(self):
        self.user_query = ""
        self.tools_used: list = []
        self.files_touched: list = []
        self.final_answer = ""
        self.status = "success"

    def record_tool(self, tool_name: str, args: dict, outcome_status: str) -> None:
        self.tools_used.append(tool_name)
        if args.get("path"):
            self.files_touched.append(args["path"])
        if args.get("url"):
            self.files_touched.append(args["url"][:80])
        if outcome_status == "error":
            self.status = "partial"

    def record_answer(self, answer: str) -> None:
        self.final_answer = answer[:500]

    def save(self, db_path: str) -> None:
        if not self.user_query:
            return
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with sqlite3.connect(db_path) as db:
                db.execute(
                    """CREATE TABLE IF NOT EXISTS sessions (
                        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        user_query TEXT,
                        tools_used TEXT,
                        files_touched TEXT,
                        final_answer TEXT,
                        status TEXT DEFAULT 'success'
                    )"""
                )
                db.execute(
                    "INSERT INTO sessions (timestamp, user_query, tools_used, files_touched, final_answer, status)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        timestamp,
                        self.user_query[:200],
                        ", ".join(self.tools_used) if self.tools_used else "respond",
                        ", ".join(self.files_touched) if self.files_touched else "",
                        self.final_answer,
                        self.status,
                    ),
                )
        except Exception as exc:
            import logging

            logging.getLogger("aelvo.cli").debug("Session save failed: %s", exc)
