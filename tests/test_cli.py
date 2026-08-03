"""Tests for the AELVO terminal CLI (cli/ package + main.py --cli wiring)."""

import asyncio
import os
import sqlite3
import sys

import pytest

from cli.commands import CliContext, handle_command, is_slash_command, parse_command
from cli.session import SessionRecorder, TerminalSession
from cli.theme import build_console
from runtime_next.models.events import EventType as RuntimeEventType


@pytest.fixture(autouse=True)
def _clean_aelvo_env():
    """Isolate AELVO_* env vars between tests (main() mutates them)."""
    saved = {k: v for k, v in os.environ.items() if k.startswith("AELVO_")}
    yield
    for k in list(os.environ):
        if k.startswith("AELVO_"):
            os.environ.pop(k, None)
    os.environ.update(saved)


# ── command parsing ─────────────────────────────────────────────────────────

def test_is_slash_command():
    assert is_slash_command("/help")
    assert is_slash_command("  /exit ")
    assert not is_slash_command("hello world")
    assert not is_slash_command("")


def test_parse_command():
    assert parse_command("/help") == ("help", "")
    assert parse_command("/workspace /foo/bar") == ("workspace", "/foo/bar")
    assert parse_command("/ask  write a test ") == ("ask", "write a test")


def test_parse_command_aliases():
    assert parse_command("/q")[0] == "exit"
    assert parse_command("/quit")[0] == "exit"
    assert parse_command("/cd")[0] == "workspace"
    assert parse_command("/open")[0] == "workspace"
    assert parse_command("/h")[0] == "help"


def test_unknown_command_returns_none():
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
    )
    assert asyncio.run(handle_command(ctx, "nope", "")) is None


# ── TerminalSession live rendering ──────────────────────────────────────────

def _session():
    return TerminalSession(build_console())


def test_terminal_session_completed_tool_line():
    ts = _session()
    asyncio.run(ts.emit_tool(RuntimeEventType.TOOL_STARTED, "read_file", "foo.py", "running"))
    asyncio.run(ts.emit_tool(RuntimeEventType.TOOL_COMPLETED, "read_file", "foo.py", "completed", 0))
    rendered = " ".join(str(l) for l in ts._lines)
    assert "✓" in rendered
    assert "read_file" in rendered
    assert "foo.py" in rendered


def test_terminal_session_failed_tool_line():
    ts = _session()
    asyncio.run(ts.emit_tool(RuntimeEventType.TOOL_STARTED, "bash_exec", "rm -rf /", "running"))
    asyncio.run(ts.emit_tool(RuntimeEventType.TOOL_FAILED, "bash_exec", "rm -rf /", "failed"))
    rendered = " ".join(str(l) for l in ts._lines)
    assert "✗" in rendered


def test_terminal_session_final_answer_capture():
    ts = _session()
    ts.on_final_answer("Done fixing the bug.")
    assert ts.final_answer == "Done fixing the bug."
    assert ts._status_text == ""

    # The orchestrator also echoes the final answer via emit_system("AELVO: …")
    # — it must NOT append a line or clobber the canonical final_answer.
    asyncio.run(ts.emit_system("AELVO: Echoed answer"))
    assert ts.final_answer == "Done fixing the bug."
    assert len(ts._lines) == 0  # no duplicate line


def test_terminal_session_memory_event():
    ts = _session()
    asyncio.run(ts.emit_memory(RuntimeEventType.MEMORY_RETRIEVED, "semantic", "race condition", 3, 0.8))
    rendered = " ".join(str(l) for l in ts._lines)
    assert "retrieved 3 hit(s)" in rendered


# ── SessionRecorder ─────────────────────────────────────────────────────────

def test_session_recorder_save(tmp_path):
    db = str(tmp_path / "memory.db")
    rec = SessionRecorder()
    rec.user_query = "refactor auth"
    rec.record_tool("read_file", {"path": "auth.py"}, "success")
    rec.record_tool("write_file", {"path": "auth.py"}, "error")
    rec.record_answer("migrated the store")
    rec.save(db)

    with sqlite3.connect(db) as conn:
        rows = conn.execute("SELECT user_query, tools_used, final_answer, status FROM sessions").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "refactor auth"
    assert "read_file" in rows[0][1] and "write_file" in rows[0][1]
    assert rows[0][2] == "migrated the store"
    assert rows[0][3] == "partial"


def test_session_recorder_empty_query_skips(tmp_path):
    db = str(tmp_path / "memory.db")
    SessionRecorder().save(db)
    assert not os.path.exists(db) or True  # must not crash
    if os.path.exists(db):
        with sqlite3.connect(db) as conn:
            n = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        assert n == 0


# ── workspace slash command ─────────────────────────────────────────────────

def test_workspace_command_switches(tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    calls = []

    def switcher(path):
        calls.append(path)
        return str(path)

    class FakeOrchestrator:
        def __init__(self):
            self.root = None

        def set_workspace_root(self, path):
            self.root = path
            return path

    class FakeFS:
        def __init__(self):
            self.base = None

        def set_base_path(self, path):
            self.base = path
            return path

    orch = FakeOrchestrator()
    ffs = FakeFS()
    ctx = CliContext(
        agent=None, orchestrator=orch, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        fs=ffs, workspace_switcher=switcher, provider_name=None, model=None,
    )
    assert asyncio.run(handle_command(ctx, "workspace", str(root))) is None
    assert orch.root == str(root)
    assert ffs.base == str(root)
    assert ctx.workspace_path == str(root)
    assert calls == [str(root)]


def test_toolbar_builder_returns_prompt_toolkit_items():
    """Regression: the REPL passes ``lambda: _toolbar(ctx)`` — the callable
    must be invokable with zero args and return (style, text) pairs."""
    from cli.app import _toolbar

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path="/ws", project="p",
        provider_name="nvidia", model="m",
    )
    items = _toolbar(ctx)
    assert isinstance(items, list) and items
    assert all(isinstance(t, tuple) and len(t) == 2 for t in items)
    joined = " ".join(str(text) for _, text in items)
    assert "nvidia" in joined and "/ws" in joined


def test_run_turn_renders_answer(tmp_path):
    from cli.app import _run_turn

    class FakeOrchestrator:
        async def execute_turn(self, agent, user_input, **kwargs):
            kwargs["stream_callback"]("the answer")
            return {"output": "the answer", "specialists_active": ["HERMES"], "status": "success"}

    ctx = CliContext(
        agent=object(), orchestrator=FakeOrchestrator(), memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path=str(tmp_path / "memory.db"),
        workspace_path=str(tmp_path), project="t",
    )
    asyncio.run(_run_turn(ctx, "hi"))
    assert ctx.state["last_prompt"] == "hi"


def test_retry_returns_run_action():
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
    )
    ctx.state["last_prompt"] = "hello agent"
    assert asyncio.run(handle_command(ctx, "retry", "")) == ("run", "hello agent")


# ── main.py --cli / --ask wiring ────────────────────────────────────────────

def test_main_cli_args_map_to_env(monkeypatch):
    import main

    captured = {}

    def fake_run(coro):
        coro.close()  # avoid 'coroutine never awaited' warning
        captured["env"] = {
            k: v for k, v in os.environ.items() if k.startswith("AELVO_")
        }
        return None

    monkeypatch.setattr(main.asyncio, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["main.py", "--cli", "--project", "tcli"])
    main.main()
    assert captured["env"].get("AELVO_CLI") == "1"


def test_main_defaults_to_cli(monkeypatch):
    """No args must launch the terminal CLI, not the web dashboard."""
    import main

    captured = {}

    def fake_run(coro):
        coro.close()
        captured["env"] = {
            k: v for k, v in os.environ.items() if k.startswith("AELVO_")
        }
        return None

    monkeypatch.setattr(main.asyncio, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["main.py"])
    main.main()
    assert captured["env"].get("AELVO_CLI") == "1"
    assert "AELVO_WEB" not in captured["env"]


def test_main_web_flag_disables_cli(monkeypatch):
    """--web must opt out of the CLI and enable the dashboard."""
    import main

    captured = {}

    def fake_run(coro):
        coro.close()
        captured["env"] = {
            k: v for k, v in os.environ.items() if k.startswith("AELVO_")
        }
        return None

    monkeypatch.setattr(main.asyncio, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["main.py", "--web"])
    main.main()
    assert captured["env"].get("AELVO_WEB") == "1"
    assert captured["env"].get("AELVO_CLI", "") != "1"


def test_main_ask_implies_cli(monkeypatch):
    import main

    captured = {}

    def fake_run(coro):
        coro.close()  # avoid 'coroutine never awaited' warning
        captured["env"] = {
            k: v for k, v in os.environ.items() if k.startswith("AELVO_")
        }
        return None

    monkeypatch.setattr(main.asyncio, "run", fake_run)
    monkeypatch.setattr(sys, "argv", ["main.py", "--ask", "hello"])
    main.main()
    assert captured["env"].get("AELVO_CLI") == "1"
    assert captured["env"].get("AELVO_ASK") == "hello"
