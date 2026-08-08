"""
tests/test_modes.py — Agent effort-mode system.

Locks in the ``/mode`` dial the user asked for (low / medium / high / max):

* ``cli.modes`` — registry, normalization, per-folder persistence
* ``/mode`` command — set directly, unknown modes rejected
* Orchestrator fast paths — ``low`` = one LLM call (no pipeline, no tools);
  ``medium`` = one LLM call + tool loop; ``max`` forces collaborative Mode B
"""

from __future__ import annotations

import asyncio
import json
from io import StringIO
from unittest.mock import AsyncMock, MagicMock

from cli.commands import CliContext, handle_command

import cli.modes as M


# ── cli.modes helpers ───────────────────────────────────────────────────────


def _ctx(tmp_path) -> CliContext:
    from cli.theme import build_console

    console = build_console()
    console.file = StringIO()  # capture output instead of the terminal
    return CliContext(
        agent=None,
        orchestrator=None,
        memory_engine=None,
        aelvo_kernel=None,
        console=console,
        db_path="",
        workspace_path=str(tmp_path),
        project="t",
    )


def test_normalize_mode_valid_and_unknown():
    assert M.normalize_mode("low") == "low"
    assert M.normalize_mode("MEDIUM") == "medium"
    assert M.normalize_mode(" max ") == "max"
    assert M.normalize_mode("high") == "high"
    assert M.normalize_mode("ultra") == M.DEFAULT_MODE
    assert M.normalize_mode("") == M.DEFAULT_MODE
    assert M.normalize_mode(None) == M.DEFAULT_MODE


def test_default_mode_is_high():
    assert M.DEFAULT_MODE == "high"
    assert set(M.AGENT_MODES) == {"low", "medium", "high", "max"}


def test_write_then_read_mode(tmp_path, monkeypatch):
    ctx = _ctx(tmp_path)
    monkeypatch.delenv("AELVO_MODE", raising=False)
    assert M.write_mode(ctx, "medium") is True
    assert M.read_mode(ctx) == "medium"
    # Persisted to the hidden .aelvo state dir next to history.
    mode_file = tmp_path / ".aelvo" / "mode"
    assert mode_file.read_text(encoding="utf-8").strip() == "medium"


def test_env_overrides_file(tmp_path, monkeypatch):
    ctx = _ctx(tmp_path)
    M.write_mode(ctx, "low")
    monkeypatch.setenv("AELVO_MODE", "max")
    assert M.read_mode(ctx) == "max"
    monkeypatch.delenv("AELVO_MODE")
    assert M.read_mode(ctx) == "low"


def test_invalid_mode_file_falls_back(tmp_path, monkeypatch):
    ctx = _ctx(tmp_path)
    monkeypatch.delenv("AELVO_MODE", raising=False)
    state = tmp_path / ".aelvo"
    state.mkdir(exist_ok=True)
    (state / "mode").write_text("ultra\n", encoding="utf-8")
    assert M.read_mode(ctx) == "high"


# ── /mode command ───────────────────────────────────────────────────────────


async def _run_cmd(ctx, name: str, arg: str = ""):
    return await handle_command(ctx, name, arg)


def test_cmd_mode_sets_directly(tmp_path, monkeypatch):
    monkeypatch.delenv("AELVO_MODE", raising=False)
    ctx = _ctx(tmp_path)
    asyncio.run(_run_cmd(ctx, "mode", "low"))
    assert M.read_mode(ctx) == "low"


def test_cmd_mode_rejects_unknown(tmp_path, monkeypatch):
    monkeypatch.delenv("AELVO_MODE", raising=False)
    ctx = _ctx(tmp_path)
    asyncio.run(_run_cmd(ctx, "mode", "ultra"))
    # Nothing written — still the default.
    assert M.read_mode(ctx) == "high"


def test_cmd_mode_no_arg_prints_table(tmp_path, monkeypatch):
    """Non-interactive terminals get the table (picker returns None)."""
    monkeypatch.delenv("AELVO_MODE", raising=False)
    ctx = _ctx(tmp_path)
    asyncio.run(_run_cmd(ctx, "mode"))
    out = ctx.console.file.getvalue()  # type: ignore[union-attr]
    assert "Agent effort modes" in out
    assert "low" in out and "max" in out


def test_parse_command_aliases_mode():
    from cli.commands import parse_command

    assert parse_command("/mode") == ("mode", "")
    assert parse_command("/mode low") == ("mode", "low")
    assert parse_command("/effort max") == ("mode", "max")
    assert parse_command("/modes") == ("mode", "")


# ── Orchestrator fast paths ─────────────────────────────────────────────────


class _FakeAgent:
    """Minimal agent: records calls, returns canned output."""

    def __init__(self, reply: str = "plain hello answer"):
        self.reply = reply
        self.calls: list = []
        self.conversation_history: list = []
        self.session_id = "s1"

    def feed_result(self, outcome):
        pass

    async def send_user_message_async(self, msg, on_token=None):
        self.calls.append(msg)
        return self.reply


def _make_direct_orch():
    """A bare Orchestrator exposing just the fast-path surface."""
    from core.orchestration.orchestrator import Orchestrator

    orch = object.__new__(Orchestrator)
    orch._turn_counter = 0
    orch.specialist_failures = {}
    orch._notify_ui_task_completed = MagicMock()

    async def _tool_loop(*args, **kwargs):
        return "tooled answer"

    orch._execute_tool_loop = _tool_loop
    return orch


def test_direct_turn_low_returns_plain_answer():
    orch = _make_direct_orch()
    agent = _FakeAgent("hi there!")
    result = asyncio.run(
        orch._execute_direct_turn(
            agent, "hi", "t1", allow_tools=False,
            session_tracker=None, tui_session=None,
            stream_callback=None, token_callback=None,
            mcp_cli=None, db_path=":memory:",
        )
    )
    assert result["status"] == "success"
    assert result["output"] == "hi there!"
    assert result["specialists_active"] == []
    # Dict-shape parity with the pipeline path (web bridge indexes this).
    assert result["pipeline_result"] is None
    assert result["architect_plan_used"] is False
    # Exactly one direct LLM call — no follow-ups, no tool loop.
    assert agent.calls == ["hi"]


def test_direct_turn_low_extracts_respond_message():
    """If the model emits tool JSON anyway, low mode surfaces the respond msg."""
    orch = _make_direct_orch()
    agent = _FakeAgent(json.dumps([{"tool": "respond", "args": {"message": "hello"}}]))
    result = asyncio.run(
        orch._execute_direct_turn(
            agent, "hi", "t1", allow_tools=False,
            session_tracker=None, tui_session=None,
            stream_callback=None, token_callback=None,
            mcp_cli=None, db_path=":memory:",
        )
    )
    assert result["output"] == "hello"


def test_direct_turn_low_no_respond_hides_tool_json():
    """Tool JSON without a respond message is never surfaced raw in low mode."""
    orch = _make_direct_orch()
    agent = _FakeAgent(
        json.dumps([{"tool": "bash_exec", "args": {"command": "ls"}}])
    )
    result = asyncio.run(
        orch._execute_direct_turn(
            agent, "list files", "t1", allow_tools=False,
            session_tracker=None, tui_session=None,
            stream_callback=None, token_callback=None,
            mcp_cli=None, db_path=":memory:",
        )
    )
    # Not the raw JSON — a graceful note about tools being off in low mode.
    assert "bash_exec" not in result["output"]
    assert "low mode" in result["output"]
    assert "disabled in low mode" in result["output"]


def test_direct_turn_medium_runs_tool_loop():
    orch = _make_direct_orch()
    agent = _FakeAgent('[{"tool": "bash_exec", "args": {"command": "ls"}}]')
    result = asyncio.run(
        orch._execute_direct_turn(
            agent, "list files", "t1", allow_tools=True,
            session_tracker=None, tui_session=None,
            stream_callback=None, token_callback=None,
            mcp_cli=None, db_path=":memory:",
        )
    )
    # The tool loop consumed the raw output and produced the final answer.
    assert result["output"] == "tooled answer"


def test_execute_turn_low_mode_skips_pipeline():
    """execute_turn(mode='low') must NOT build a HermesContext via LLM
    nor run the pipeline — the fast path returns first."""
    from core.orchestration.orchestrator import Orchestrator

    orch = object.__new__(Orchestrator)
    orch._turn_counter = 0
    orch.session_manager = MagicMock()
    orch.session_manager.increment_turn = MagicMock()
    orch.runtime_runner = MagicMock()
    orch._notify_ui_task_created = MagicMock()
    orch._notify_ui_specialist_activated = MagicMock()
    orch.router = MagicMock()
    orch.router.parse_force_route = MagicMock(return_value=([], "hi"))
    orch._create_hermes_context = MagicMock()
    orch.pipeline = MagicMock()

    async def _direct(*args, **kwargs):
        return {"status": "success", "output": "ok"}

    orch._execute_direct_turn = _direct

    agent = _FakeAgent()
    result = asyncio.run(
        orch.execute_turn(
            agent, "hi", mode="low",
            session_tracker=None, tui_session=None,
            stream_callback=None, token_callback=None,
        )
    )
    assert result["output"] == "ok"
    # The HermesContext LLM analysis and the pipeline never ran.
    orch._create_hermes_context.assert_not_called()
    orch.pipeline.run.assert_not_called()


def test_execute_turn_max_forces_mode_b():
    """mode='max' must force the collaborative task board (Mode B)."""
    from core.orchestration.orchestrator import Orchestrator

    orch = object.__new__(Orchestrator)
    orch._turn_counter = 0
    orch.session_manager = MagicMock()
    orch.session_manager.increment_turn = MagicMock()
    orch.runtime_runner = MagicMock()
    orch._notify_ui_task_created = MagicMock()
    orch._notify_ui_specialist_activated = MagicMock()
    orch._notify_ui_specialist_thinking = MagicMock()
    orch._notify_ui_task_completed = MagicMock()
    orch._build_pipeline_display = MagicMock(return_value="")
    orch.router = MagicMock()
    orch.router.parse_force_route = MagicMock(return_value=([], "hi"))

    async def _hermes_impl(*args, **kwargs):
        ctx = MagicMock()
        ctx.task = "hi"
        ctx.risk_profile = "low"
        ctx.complexity = 1
        ctx.goals = []
        ctx.constraints = {}
        return ctx

    orch._create_hermes_context = AsyncMock(side_effect=_hermes_impl)
    orch._evaluate_mode_with_architect = MagicMock(
        return_value="consolidated"
    )  # would pick Mode A — must be overridden by max

    class _PR:
        success = True
        phases_executed = []
        failures = []
        recovery_actions = []
        verification_summary = ""
        final_output = "board output"
        total_duration_ms = 1.0
        memory_consolidated = False

    task_board = MagicMock()
    task_board.run = AsyncMock(return_value=_PR())
    orch.task_board_pipeline = task_board

    orch.memory_engine = MagicMock()
    orch.cognitive_engine = None
    orch._maybe_summarize_session = MagicMock(return_value=None)
    orch._execute_tool_loop = AsyncMock(return_value="final")

    agent = _FakeAgent()
    asyncio.run(
        orch.execute_turn(
            agent, "hi", mode="max",
            session_tracker=None, tui_session=None,
            stream_callback=None, token_callback=None,
        )
    )
    task_board.run.assert_called_once()
    # Architect would have said consolidated, but max forces the board.
    orch._evaluate_mode_with_architect.assert_not_called()
    # The pipeline result still feeds the tool loop for the final answer.
    orch._execute_tool_loop.assert_called_once()


def test_execute_turn_unknown_mode_falls_back_to_pipeline():
    """A garbage mode degrades to the full pipeline (current behavior)."""
    from core.orchestration.orchestrator import Orchestrator

    orch = object.__new__(Orchestrator)
    orch._turn_counter = 0
    orch.session_manager = MagicMock()
    orch.session_manager.increment_turn = MagicMock()
    orch.runtime_runner = MagicMock()
    orch._notify_ui_task_created = MagicMock()
    orch._notify_ui_specialist_activated = MagicMock()
    orch._notify_ui_specialist_thinking = MagicMock()
    orch._notify_ui_task_completed = MagicMock()
    orch._build_pipeline_display = MagicMock(return_value="")
    orch.router = MagicMock()
    orch.router.parse_force_route = MagicMock(return_value=([], "hi"))

    async def _hermes_impl(*args, **kwargs):
        ctx = MagicMock()
        ctx.task = "hi"
        ctx.risk_profile = "low"
        ctx.complexity = 1
        ctx.goals = []
        ctx.constraints = {}
        return ctx

    orch._create_hermes_context = AsyncMock(side_effect=_hermes_impl)
    orch._evaluate_mode_with_architect = MagicMock(return_value="consolidated")

    async def _direct(*args, **kwargs):
        return {"status": "success", "output": "should-not-happen"}

    orch._execute_direct_turn = _direct

    class _PR:
        success = True
        phases_executed = []
        failures = []
        recovery_actions = []
        verification_summary = ""
        final_output = "pipeline out"
        total_duration_ms = 1.0
        memory_consolidated = False

    pipeline = MagicMock()
    pipeline.run = AsyncMock(return_value=_PR())
    orch.pipeline = pipeline
    orch.memory_engine = MagicMock()
    orch.cognitive_engine = None
    orch._maybe_summarize_session = MagicMock(return_value=None)

    async def _tool_loop(*args, **kwargs):
        return "final"

    orch._execute_tool_loop = _tool_loop

    agent = _FakeAgent()
    asyncio.run(
        orch.execute_turn(
            agent, "hi", mode="ultra",
            session_tracker=None, tui_session=None,
            stream_callback=None, token_callback=None,
        )
    )
    orch._create_hermes_context.assert_called_once()
    pipeline.run.assert_called_once()
