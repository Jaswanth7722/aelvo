"""app.py — AELVO TUI (OpenCode-inspired design)

Clean chat-first layout with:
- Full-width conversation view
- Minimal header bar (model, provider, session)
- Collapsible sidebar (sessions, file changes)
- Permission dialog for tool calls
- Model selector overlay
- Keyboard-driven navigation
"""

import asyncio
import logging
from typing import Callable, Optional

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import Input, Static

from ui.core.bridge import UIBridge
from ui.widgets.chat_view import ChatView
from ui.widgets.header_bar import HeaderBar
from ui.widgets.model_dialog import ModelDialog

log = logging.getLogger("aelvo.ui.app")


class AelvoTUI(App):
    TITLE = "AELVO"
    CSS = """
    Screen {
        background: #0d1117;
        color: #c9d1d9;
    }

    #header-bar {
        dock: top;
        height: 1;
        background: #161b22;
        color: #8b949e;
        padding: 0 1;
    }

    #main-layout {
        layout: horizontal;
        height: 1fr;
    }

    #sidebar {
        width: 0;
        background: #161b22;
        border-right: solid #30363d;
        display: none;
    }

    #sidebar.open {
        width: 30;
        display: block;
    }

    #sidebar-header {
        height: 1;
        background: #161b22;
        color: #8b949e;
        border-bottom: solid #30363d;
        padding: 0 1;
    }

    #session-list {
        height: 1fr;
        background: #161b22;
        padding: 0 0;
    }

    #chat-area {
        width: 1fr;
        height: 1fr;
        background: #0d1117;
    }

    #chat-view {
        width: 1fr;
        height: 1fr;
        background: #0d1117;
        padding: 0 2;
        overflow-y: auto;
        scrollbar-size: 0 0;
    }

    #file-panel {
        height: 0;
        background: #161b22;
        border-top: solid #30363d;
        display: none;
    }

    #file-panel.open {
        height: 8;
        display: block;
    }

    #input-area {
        dock: bottom;
        height: 3;
        background: #0d1117;
        border-top: solid #21262d;
        padding: 0 2;
    }

    #input-prefix {
        width: 2;
        height: 1fr;
        content-align: center middle;
        color: #58a6ff;
    }

    #user-input {
        width: 1fr;
        height: 1fr;
        border: none;
        background: #0d1117;
        color: #f0f6fc;
    }

    #user-input:focus {
        border: none;
    }

    #status-bar {
        dock: bottom;
        height: 1;
        background: #161b22;
        color: #8b949e;
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("ctrl+c", "quit", "Quit", priority=True),
        Binding("ctrl+n", "new_session", "New Session"),
        Binding("ctrl+a", "toggle_sidebar", "Sessions"),
        Binding("ctrl+o", "toggle_model", "Model"),
        Binding("ctrl+l", "toggle_files", "Files"),
        Binding("ctrl+x", "cancel_generation", "Cancel"),
        Binding("escape", "escape_action", "Back"),
        Binding("i", "focus_input", "Type", show=False),
    ]

    def __init__(
        self,
        bridge: Optional[UIBridge] = None,
        user_callback: Optional[Callable] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bridge = bridge or UIBridge()
        self._user_callback = user_callback
        self.dark = True
        self._bridge_task: Optional[asyncio.Task] = None
        self._request_queue: asyncio.Queue = asyncio.Queue()
        self._queue_worker_task: Optional[asyncio.Task] = None
        self._processing = False
        self._sidebar_open = False
        self._files_open = False
        self._current_session = "default"
        self._sessions = ["default"]
        self._model = "nemotron-3-super-120b-a12b"
        self._provider = "nvidia"
        self._file_changes: list[dict] = []

    def compose(self) -> ComposeResult:
        yield HeaderBar(id="header-bar")

        with Container(id="main-layout"):
            with Vertical(id="sidebar"):
                yield Static(" SESSIONS", id="sidebar-header")
                yield Static(id="session-list")
            with Vertical(id="chat-area"):
                yield ChatView(id="chat-view")
                yield Static(id="file-panel")

        yield Static(id="status-bar")
        with Horizontal(id="input-area"):
            yield Static(">", id="input-prefix")
            yield Input(placeholder="", id="user-input")

    def on_mount(self) -> None:
        header = self.query_one("#header-bar", HeaderBar)
        header.update(f" AELVO  {self._provider}/{self._model}  session:{self._current_session}")

        status = self.query_one("#status-bar", Static)
        status.update(" ctrl+a sessions  ctrl+o model  ctrl+l files  ctrl+c quit")

        self._update_session_list()
        self._start_bridge()

        input_widget = self.query_one("#user-input", Input)
        input_widget.focus()

    @work
    async def _start_bridge(self) -> None:
        self._bridge_task = asyncio.current_task()
        await self.bridge.start()
        chat = self.query_one("#chat-view", ChatView)
        chat.add_system("Ready.")

    async def _queue_worker(self) -> None:
        while True:
            text = await self._request_queue.get()
            self._processing = True
            self._update_status("processing...")

            chat = self.query_one("#chat-view", ChatView)
            chat.add_user(text)

            async def stream_token(token: str) -> None:
                pass

            def sync_stream_token(token: str) -> None:
                asyncio.create_task(stream_token(token))

            try:
                if self._user_callback:
                    result = await self._user_callback(
                        text, stream_callback=sync_stream_token
                    )
                    if result and result.answer:
                        chat.add_assistant(result.answer)
                    if result and result.tools_used:
                        chat.add_system(f"Tools: {', '.join(result.tools_used[:5])}")
                else:
                    chat.add_system("No handler registered.")
            except asyncio.CancelledError:
                chat.add_system("Cancelled.")
            except Exception as e:
                chat.add_error(f"Error: {e}")
                log.exception("Request failed")
            finally:
                self._processing = False
                self._update_status("ready")
                self._request_queue.task_done()

    def _update_status(self, text: str) -> None:
        try:
            status = self.query_one("#status-bar", Static)
            provider = self._provider.upper()
            model = self._model
            left = f" {provider}/{model}"
            right = f"{text} "
            padding = max(0, self.size.width - len(left) - len(right))
            status.update(left + " " * padding + right)
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        if not text:
            return

        input_widget = self.query_one("#user-input", Input)
        input_widget.clear()

        if self._processing:
            chat = self.query_one("#chat-view", ChatView)
            chat.add_system("Queued...")
            self._request_queue.put_nowait(text)
            return

        self._request_queue.put_nowait(text)

    # ── Actions ──────────────────────────────────────────────

    def action_focus_input(self) -> None:
        input_widget = self.query_one("#user-input", Input)
        input_widget.focus()

    def action_toggle_sidebar(self) -> None:
        self._sidebar_open = not self._sidebar_open
        sidebar = self.query_one("#sidebar")
        if self._sidebar_open:
            sidebar.add_class("open")
            self._update_session_list()
        else:
            sidebar.remove_class("open")

    def action_toggle_model(self) -> None:
        self.push_screen(ModelDialog(self._provider, self._model, self._on_model_selected))

    def _on_model_selected(self, provider: str, model: str) -> None:
        self._provider = provider
        self._model = model
        header = self.query_one("#header-bar", HeaderBar)
        header.update(f" AELVO  {self._provider}/{self._model}  session:{self._current_session}")

    def action_toggle_files(self) -> None:
        self._files_open = not self._files_open
        panel = self.query_one("#file-panel")
        if self._files_open:
            panel.add_class("open")
        else:
            panel.remove_class("open")

    def action_new_session(self) -> None:
        import time
        name = f"session_{int(time.time()) % 10000}"
        self._sessions.append(name)
        self._current_session = name
        chat = self.query_one("#chat-view", ChatView)
        chat.clear()
        chat.add_system(f"New session: {name}")
        header = self.query_one("#header-bar", HeaderBar)
        header.update(f" AELVO  {self._provider}/{self._model}  session:{self._current_session}")
        if self._sidebar_open:
            self._update_session_list()

    def action_cancel_generation(self) -> None:
        if self._processing:
            self._processing = False
            chat = self.query_one("#chat-view", ChatView)
            chat.add_system("Generation cancelled.")

    def action_escape_action(self) -> None:
        if self._sidebar_open:
            self.action_toggle_sidebar()
        elif self._files_open:
            self.action_toggle_files()
        else:
            self.action_focus_input()

    def _update_session_list(self) -> None:
        try:
            session_list = self.query_one("#session-list", Static)
            lines = []
            for s in self._sessions:
                marker = ">" if s == self._current_session else " "
                lines.append(f" {marker} {s}")
            session_list.update("\n".join(lines) if lines else " (no sessions)")
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)

    async def shutdown(self) -> None:
        if self._queue_worker_task and not self._queue_worker_task.done():
            self._queue_worker_task.cancel()
            try:
                await self._queue_worker_task
            except asyncio.CancelledError as _ex:
                log.warning("Silenced exception: %s", _ex)
        if self._bridge_task and not self._bridge_task.done():
            self._bridge_task.cancel()
            try:
                await self._bridge_task
            except asyncio.CancelledError as _ex:
                log.warning("Silenced exception: %s", _ex)
        await self.bridge.stop()
        await super().shutdown()
