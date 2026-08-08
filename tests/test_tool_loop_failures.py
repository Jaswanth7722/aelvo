"""Tests for the tool-loop failure circuit breaker and the tiny-model hint.

A weak (often small local) model keeps emitting broken tool calls; without a
breaker the orchestrator's tool loop would burn all 30 steps asking it again
and again. These tests lock in:

* the breaker stops the loop after N consecutive failures with a graceful answer
* a success between failures resets the counter
* the failure reason reaches the terminal transcript
* the model picker flags sub-2B local models as ``⚠ tiny``
"""

from __future__ import annotations

import asyncio
import json
from io import StringIO


# ── circuit breaker ──────────────────────────────────────────────────────────

def _make_orchestrator(tool_outcome_fn, extra_tools=None):
    """A bare Orchestrator wired with an always-failing / always-ok tool.

    ``extra_tools`` maps extra tool names to outcome functions (e.g.
    ``{"list_files": ...}``) for tests that drive other tools through the loop.
    """
    from core.orchestration.orchestrator import Orchestrator

    orch = object.__new__(Orchestrator)

    tool_map = {"bash_exec": {"fn": tool_outcome_fn}}
    for name, fn in (extra_tools or {}).items():
        tool_map[name] = {"fn": fn}

    class _Tools:
        tools = tool_map

    class _Runner:
        def __init__(self):
            self._tool_executor = None

        async def run_node(self, node, ctx):
            return await self._tool_executor(node.tool_name, node.args)

    class _Graph:
        def add_node(self, node):
            pass

        async def transition_node(self, nid, state, reason=""):
            pass

    orch.memory_engine = _Tools()
    orch.runtime_runner = _Runner()
    orch.runtime_graph = _Graph()
    orch.base_path = "/tmp"
    orch.runtime_runner._tool_executor = orch._graph_tool_executor
    return orch


def _fail(**kw):
    return {"status": "error", "logs": "command failed: not found", "executed": {}}


def _ok(**kw):
    return {"status": "success", "logs": "ok", "executed": {}}


class _Agent:
    """Fake agent: always replies with the same failing tool call."""

    def __init__(self, reply: str):
        self.reply = reply
        self.follow_ups = 0

    def feed_result(self, outcome):
        pass

    async def send_user_message_async(self, msg, on_token=None):
        self.follow_ups += 1
        return self.reply


_TOOL_CALL = json.dumps([{"tool": "bash_exec", "args": {"command": "nope"}}])


def test_tool_loop_breaker_stops_after_three_consecutive_failures():
    orch = _make_orchestrator(_fail)
    agent = _Agent(_TOOL_CALL)
    streamed: list = []

    answer = asyncio.run(
        orch._execute_tool_loop(
            agent,
            _TOOL_CALL,
            session_tracker=None,
            tui_session=None,
            stream_callback=streamed.append,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )

    # Initial batch fails (1) → 2 follow-ups fail (2, 3) → breaker trips.
    assert agent.follow_ups == 2
    assert "consecutive tool failures" in answer
    assert streamed and "consecutive tool failures" in streamed[-1]


def test_tool_loop_success_resets_failure_counter():
    # fail → success → fail → fail → fail: the success must reset the
    # consecutive-failure counter, so the breaker trips only on the 3rd
    # failure *after* the success. Without the reset it would trip at the
    # 3rd overall failure (one follow-up earlier).
    seq = [_fail, _ok, _fail, _fail, _fail]
    state = {"n": 0}

    def _flaky(**kw):
        fn = seq[min(state["n"], len(seq) - 1)]
        state["n"] += 1
        return fn(**kw)

    orch = _make_orchestrator(_flaky)
    agent = _Agent(_TOOL_CALL)

    answer = asyncio.run(
        orch._execute_tool_loop(
            agent,
            _TOOL_CALL,
            session_tracker=None,
            tui_session=None,
            stream_callback=None,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )

    # Batch1 fail(1) → f-up1 → batch2 ok(reset) → f-up2 → batch3 fail(1) →
    # f-up3 → batch4 fail(2) → f-up4 → batch5 fail(3) → trips.
    assert agent.follow_ups == 4
    assert "consecutive tool failures" in answer


def test_tool_loop_breaker_emits_failure_reason_to_terminal():
    """The ✗ line carries the real error, not just truncated args."""
    from cli.session import TerminalSession
    from rich.console import Console

    emitted: dict = {}
    term = TerminalSession(Console(file=StringIO()))

    async def _emit_tool(event_type, tool_name, args_preview="", status="running", exit_code=0):
        emitted["event_type"] = event_type
        emitted["preview"] = args_preview

    term.emit_tool = _emit_tool  # type: ignore[assignment]

    orch = _make_orchestrator(_fail)
    agent = _Agent(_TOOL_CALL)
    asyncio.run(
        orch._execute_tool_loop(
            agent,
            _TOOL_CALL,
            session_tracker=None,
            tui_session=term,
            stream_callback=None,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )
    assert "command failed: not found" in emitted["preview"]


# ── empty-respond fallback ───────────────────────────────────────────────────

def test_tool_loop_empty_respond_renders_tool_results():
    """A respond with an EMPTY message after successful tool work must not
    produce a blank screen — the loop renders what the tools returned."""
    from core.orchestration.orchestrator import _render_tool_results

    outcomes = [
        ("list_files", {
            "status": "success",
            "logs": "Listed 2 items",
            "data": [
                {"name": "src", "type": "dir"},
                {"name": "README.md", "type": "file"},
            ],
        }),
    ]
    rendered = _render_tool_results(outcomes)
    assert "list_files" in rendered
    assert "src" in rendered and "README.md" in rendered
    assert "✓" in rendered


def test_tool_loop_empty_respond_no_outcomes_has_placeholder():
    """Even with zero tool outcomes an empty respond gets a visible line."""
    from core.orchestration.orchestrator import _render_tool_results

    assert _render_tool_results([]) == ""


def test_tool_loop_empty_respond_after_failed_tool_shows_error():
    """A failed tool in the fallback renders with an ✗ marker and its log."""
    from core.orchestration.orchestrator import _render_tool_results

    rendered = _render_tool_results([
        ("bash_exec", {"status": "error", "logs": "command not found", "executed": {}}),
    ])
    assert "✗" in rendered
    assert "command not found" in rendered


def test_tool_loop_empty_respond_after_list_files_returns_visible_answer():
    """End-to-end: agent runs list_files, then closes with an empty respond —
    the final answer must contain the folder listing, not a blank line."""

    def _list_ok(**kw):
        return {
            "status": "success",
            "logs": "Listed 1 items",
            "data": [{"name": "app.py", "type": "file"}],
        }

    orch = _make_orchestrator(_fail, extra_tools={"list_files": _list_ok})

    class _SeqAgent:
        """First reply: list_files call. Second (follow-up): empty respond."""

        def __init__(self):
            self.follow_ups = 0

        def feed_result(self, outcome):
            pass

        async def send_user_message_async(self, msg, on_token=None):
            self.follow_ups += 1
            # The initial batch already carries the list_files call; the first
            # follow-up closes with an EMPTY respond — the exact transcript bug.
            return json.dumps([{"tool": "respond", "args": {"message": ""}}])

    agent = _SeqAgent()
    streamed: list = []
    answer = asyncio.run(
        orch._execute_tool_loop(
            agent,
            json.dumps([{"tool": "list_files", "args": {"path": "."}}]),
            session_tracker=None,
            tui_session=None,
            stream_callback=streamed.append,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )
    assert agent.follow_ups == 1
    assert "app.py" in answer
    assert answer.strip()  # never blank
    assert streamed and "app.py" in streamed[-1]


def test_tool_loop_empty_respond_no_prior_tools_gets_placeholder():
    """Loop-level: an empty respond with ZERO prior tool outcomes still gets
    a visible placeholder line (never a blank answer)."""
    orch = _make_orchestrator(_fail)

    class _EmptyAgent:
        def __init__(self):
            self.follow_ups = 0

        def feed_result(self, outcome):
            pass

        async def send_user_message_async(self, msg, on_token=None):
            self.follow_ups += 1
            return json.dumps([{"tool": "respond", "args": {"message": ""}}])

    agent = _EmptyAgent()
    streamed: list = []
    answer = asyncio.run(
        orch._execute_tool_loop(
            agent,
            json.dumps([{"tool": "respond", "args": {"message": ""}}]),
            session_tracker=None,
            tui_session=None,
            stream_callback=streamed.append,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )
    assert agent.follow_ups == 0  # respond in the first batch → never follow up
    assert "empty response" in answer
    assert answer.strip()


def test_tool_loop_failed_tool_does_not_skip_respond_in_batch():
    """A failed tool in a batch must NOT skip a respond that follows it in
    the SAME batch: the turn still closes with the respond's message. The old
    `break` on failure abandoned the rest of the batch, swallowed the final
    answer, and forced another (often garbage) model round-trip."""
    orch = _make_orchestrator(_fail)  # bash_exec always fails

    class _BatchAgent:
        def __init__(self):
            self.follow_ups = 0

        def feed_result(self, outcome):
            pass

        async def send_user_message_async(self, msg, on_token=None):
            self.follow_ups += 1
            return json.dumps([{"tool": "respond", "args": {"message": ""}}])

    agent = _BatchAgent()
    streamed: list = []
    answer = asyncio.run(
        orch._execute_tool_loop(
            agent,
            json.dumps([
                {"tool": "bash_exec", "args": {"command": "nope"}},
                {"tool": "respond", "args": {"message": "Here is the final answer."}},
            ]),
            session_tracker=None,
            tui_session=None,
            stream_callback=streamed.append,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )
    # The respond closed the turn inside the first batch — never re-asked.
    assert agent.follow_ups == 0
    assert answer == "Here is the final answer."
    assert streamed == ["Here is the final answer."]


def test_tool_loop_breaker_does_not_clobber_respond_in_same_batch():
    """A batch of >=3 failed tools followed by a respond must end with the
    respond's message — the circuit breaker fires only AFTER batch_complete is
    honored, so it never overwrites a completed turn's answer."""
    orch = _make_orchestrator(_fail)  # bash_exec always fails

    class _BatchAgent:
        def __init__(self):
            self.follow_ups = 0

        def feed_result(self, outcome):
            pass

        async def send_user_message_async(self, msg, on_token=None):
            self.follow_ups += 1
            return json.dumps([{"tool": "respond", "args": {"message": ""}}])

    agent = _BatchAgent()
    answer = asyncio.run(
        orch._execute_tool_loop(
            agent,
            json.dumps([
                {"tool": "bash_exec", "args": {"command": "x"}},
                {"tool": "bash_exec", "args": {"command": "x"}},
                {"tool": "bash_exec", "args": {"command": "x"}},
                {"tool": "respond", "args": {"message": "Done despite failures."}},
            ]),
            session_tracker=None,
            tui_session=None,
            stream_callback=None,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )
    assert agent.follow_ups == 0  # respond closed the turn in the first batch
    assert answer == "Done despite failures."  # never replaced by the breaker


def test_tool_loop_same_batch_tools_then_empty_respond_renders():
    """The classic tiny-model pattern: tools + an empty respond in ONE batch.
    Tools must execute first, then the respond renders their results."""

    def _list_ok(**kw):
        return {
            "status": "success",
            "logs": "Listed 1 items",
            "data": [{"name": "src", "type": "dir"}],
        }

    orch = _make_orchestrator(_fail, extra_tools={"list_files": _list_ok})

    class _BatchAgent:
        def __init__(self):
            self.follow_ups = 0

        def feed_result(self, outcome):
            pass

        async def send_user_message_async(self, msg, on_token=None):
            self.follow_ups += 1
            return json.dumps([{"tool": "respond", "args": {"message": ""}}])

    agent = _BatchAgent()
    answer = asyncio.run(
        orch._execute_tool_loop(
            agent,
            json.dumps([
                {"tool": "list_files", "args": {"path": "."}},
                {"tool": "respond", "args": {"message": ""}},
            ]),
            session_tracker=None,
            tui_session=None,
            stream_callback=None,
            token_callback=None,
            mcp_cli=None,
            db_path=":memory:",
        )
    )
    assert agent.follow_ups == 0  # respond in the first batch closes the turn
    assert "src" in answer
    assert "list_files" in answer


# ── tiny-model hint ─────────────────────────────────────────────────────────

def test_model_size_gb():
    from cli.providers import _model_size_gb

    assert _model_size_gb("qwen2.5-coder:0.5b") == 0.5
    assert _model_size_gb("llama3.2:3b") == 3.0
    assert _model_size_gb("deepseek-r1:14b") == 14.0
    assert _model_size_gb("qwen2.5:7b") == 7.0
    # Cloud ids (no ':Nb' tag) are unknown, never flagged.
    assert _model_size_gb("gpt-4o") is None
    assert _model_size_gb("claude-3-5-sonnet-20241022") is None
    assert _model_size_gb("llama3.2") is None
    assert _model_size_gb("") is None


def test_picker_hints_tiny_local_model(monkeypatch):
    from cli import providers as P
    from cli.commands import CliContext
    from rich.console import Console

    captured: dict = {}

    async def fake_available(ctx, provider_key):
        return (["qwen2.5-coder:0.5b", "llama3.2:3b"], "live")

    async def fake_pick_categorized(title, sections, **kwargs):
        captured["sections"] = sections
        return ""

    monkeypatch.setattr(P, "available_models", fake_available)
    monkeypatch.setattr("cli.picker.pick_categorized", fake_pick_categorized)

    ctx = CliContext(
        agent=None,
        orchestrator=None,
        memory_engine=None,
        aelvo_kernel=None,
        console=Console(file=StringIO()),
        db_path="",
        workspace_path=".",
        project="t",
    )
    asyncio.run(P.pick_model(ctx, "ollama"))

    labels = [label for _h, rows in captured["sections"] if rows for _m, label in rows]
    assert any("tiny" in label and "0.5B" in label for label in labels)
    assert not any("tiny" in label for label in labels if "3b" in label.lower())
