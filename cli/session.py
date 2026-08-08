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
from typing import Any, List

from rich.console import Console
from rich.text import Text

from runtime_next.models.events import EventType as RuntimeEventType

# Tool → emoji icon shown in the activity transcript.
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


class TerminalSession:
    """Prints the agent's activity as a plain, scrollable transcript.

    Deliberately avoids in-place redraws (no ``rich.Live``, no spinner): every
    event is appended as a real line to the terminal, so the terminal's native
    scrollback (mouse wheel / PgUp) keeps working during and after a turn.

    Implements exactly the ``tui_session`` interface the orchestrator's tool
    loop expects (``emit_system`` / ``emit_tool`` / ``emit_memory``).
    """

    def __init__(self, console: Console):
        self.console = console
        self._lines: List[Any] = []
        self._status_text = ""  # kept for interface/tests; always empty now
        self.final_answer = ""
        # True once any generation token was rendered live this turn; the
        # REPL then skips the final Markdown re-print (already on screen).
        self.streamed = False
        self._streamed_text = ""

    # ── lifecycle ──────────────────────────────────────────────────────────
    def start(self) -> None:
        pass

    def finish(self) -> None:
        pass

    def thinking(self, note: str = "") -> None:
        """Show a dim 'working…' line so a long, output-suppressed generation
        (the model streams tool-call JSON, which the TokenStreamFilter hides)
        doesn't look like a hang. Fast turns just get one harmless line.
        """
        suffix = f" {note}" if note else ""
        self._append(Text(f"⏳ thinking…{suffix}", style="aelvo.dim"))

    # ── rendering helpers ──────────────────────────────────────────────────
    def _append(self, text: Text) -> None:
        """Record the line and print it to the terminal (real scrollback)."""
        self._lines.append(text)
        self.console.print(text)

    # ── streaming hooks (final answer + live tokens) ───────────────────────
    def on_token(self, text: str) -> None:
        """Render one generated token live (no newline).

        Called from the LLM worker thread as chunks arrive, so the user
        watches the answer being written instead of waiting for the whole
        response. Marks the turn as ``streamed`` so the REPL does not
        re-print the answer as Markdown afterwards.
        """
        if not text:
            return
        self.streamed = True
        self._streamed_text += text
        self.console.print(text, end="")

    def on_final_answer(self, message: str) -> None:
        """Store the agent's final message; the REPL prints it as Markdown.

        Only used for answers that were NOT streamed token-by-token (e.g.
        ``respond`` tool messages or plain pipeline output) — streamed
        turns already rendered the text live and skip the Markdown pass.
        """
        self.final_answer = message or ""
        self._status_text = ""

    # ── orchestrator tui_session hooks ─────────────────────────────────────
    async def emit_system(self, message: str) -> None:
        """System/kernel lines. The orchestrator also echoes the final answer
        here (``AELVO: ...``); the stream_callback hook (``on_final_answer``)
        is the canonical capture, so skip that echo entirely."""
        if message.startswith("AELVO:"):
            return
        self._append(Text(f"⚙ {message}", style="aelvo.dim"))

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

        # Only start lines on completion/failure — keeps the transcript tidy
        # and each line lands in the scrollback exactly once.
        if event_type == RuntimeEventType.TOOL_STARTED:
            return
        if event_type == RuntimeEventType.TOOL_FAILED:
            self._append(
                Text(f"✗ {icon} {tool_name} {preview}".rstrip(), style="aelvo.err")
            )
            return
        # TOOL_COMPLETED
        marker = "✓" if int(exit_code or 0) == 0 else "✗"
        style = "aelvo.ok" if marker == "✓" else "aelvo.err"
        self._append(Text(f"{marker} {icon} {tool_name} {preview}".rstrip(), style=style))

    async def emit_memory(
        self,
        event_type,
        mem_type: str,
        query: str,
        count: int = 0,
        score: float = 0.0,
    ) -> None:
        if event_type == RuntimeEventType.MEMORY_STORED:
            self._append(Text(f"🧠 stored {mem_type}: {query}", style="aelvo.dim"))
        else:
            self._append(
                Text(f"🧠 retrieved {count} hit(s) for '{query}'", style="aelvo.dim")
            )


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
