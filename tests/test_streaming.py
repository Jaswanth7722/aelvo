"""
Tests for true token streaming in the CLI chat loop.

Covers the full pipeline:

    provider SDK streaming (openai/anthropic/google)
        → AelvoAgent._call_llm(on_token=...) / send_user_message(...)
        → TokenStreamFilter (sniff prose vs tool JSON)
        → orchestrator _execute_tool_loop / RuntimePipeline
        → TerminalSession.on_token live rendering

The end-user contract: prose answers appear token-by-token as they are
generated, while tool-call JSON and kernel commands are never shown.
"""

import asyncio
import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from core.orchestration.stream_filter import TokenStreamFilter
from cli.session import TerminalSession
from cli.theme import build_console
from runtime_next.models.events import EventType as RuntimeEventType


# ── helpers ──────────────────────────────────────────────────────────────────

class _MockProviderConfig:
    """Minimal provider config for AelvoAgent construction."""
    sdk = "openai"
    base_url = None


@pytest.fixture
def openai_agent(monkeypatch):
    """AelvoAgent with a mocked OpenAI-compatible streaming client."""
    monkeypatch.delenv("AELVO_MAX_TOKENS", raising=False)
    monkeypatch.delenv("AELVO_PROMPT_CACHE_TTL", raising=False)
    from main import AelvoAgent

    ag = AelvoAgent(
        api_key="sk-test-placeholder",
        model="test-model",
        provider_name="test-provider",
        provider_config=_MockProviderConfig(),
    )
    ag.client = MagicMock()
    ag._llm_cache = MagicMock()
    ag._llm_cache.get.return_value = None  # hermetic: always a fresh call
    return ag


def _openai_chunk(text: str):
    """Build a fake OpenAI streaming chunk carrying ``text``."""
    chunk = MagicMock()
    chunk.choices = [MagicMock()]
    chunk.choices[0].delta.content = text
    return chunk


# ── TokenStreamFilter ───────────────────────────────────────────────────────

class TestTokenStreamFilter:
    def test_prose_streams_live_after_sniff_window(self):
        sink = []
        f = TokenStreamFilter(sink.append, max_sniff_chars=16)
        f("Hello ")   # buffered
        f("world, ")  # buffered
        assert f.streamed is False
        f("this is a longer sentence that crosses the threshold")
        assert f.streamed is True
        assert "".join(sink) == "Hello world, this is a longer sentence that crosses the threshold"

    def test_short_prose_flushed_on_end_of_stream(self):
        sink = []
        f = TokenStreamFilter(sink.append)
        f("All done.")
        assert f.streamed is False  # still sniffing
        f.flush()
        assert f.streamed is True
        assert "".join(sink) == "All done."

    def test_json_object_suppressed(self):
        sink = []
        f = TokenStreamFilter(sink.append)
        f('{"tool": "respond"')
        f(', "args": {"message": "hi"}}')
        f.flush()
        assert sink == []
        assert f.suppressed is True
        assert f.streamed is False

    def test_json_array_suppressed(self):
        sink = []
        f = TokenStreamFilter(sink.append)
        f('[{"tool": "read_file", "args": {"path": "x.py"}}]')
        f.flush()
        assert sink == []
        assert f.suppressed is True

    def test_kernel_command_suppressed(self):
        sink = []
        f = TokenStreamFilter(sink.append)
        f("#status")
        f.flush()
        assert sink == []
        assert f.suppressed is True

    def test_whitespace_prefixed_json_suppressed(self):
        sink = []
        f = TokenStreamFilter(sink.append)
        f("\n\n  ")
        f('{"tool": "bash_exec", "args": {"command": "ls"}}')
        f.flush()
        assert sink == []
        assert f.suppressed is True

    def test_empty_tokens_ignored(self):
        sink = []
        f = TokenStreamFilter(sink.append, max_sniff_chars=4)
        f("")
        f(None)
        assert f.streamed is False
        assert sink == []

    def test_none_sink_does_not_crash(self):
        f = TokenStreamFilter(None, max_sniff_chars=4)
        f("hello world tokens")
        f.flush()
        assert f.streamed is True

    def test_suppressed_stream_never_flushes_prose(self):
        sink = []
        f = TokenStreamFilter(sink.append)
        f('{"tool": "respond", "args": {"message": "hi"}}')
        f.flush()
        assert sink == []  # flush must not leak suppressed content

    def test_fenced_json_suppressed(self):
        """A weak model that wraps its tool JSON in a ```json code fence must
        be suppressed too — the fence started with a backtick, so the old
        marker check ({/[) missed it and streamed raw JSON to the terminal."""
        sink = []
        f = TokenStreamFilter(sink.append)
        f("```json\n")
        f('[{"tool": "list_files", "args": {"path": "."}}]')
        f("\n```")
        f.flush()
        assert sink == []
        assert f.suppressed is True
        assert f.streamed is False

    def test_fenced_json_with_leading_whitespace_suppressed(self):
        """Fences preceded by blank lines (common LLM formatting) still hide."""
        sink = []
        f = TokenStreamFilter(sink.append)
        f("\n\n```json\n")
        f('[{"tool": "read_file", "args": {"path": "x.py"}}]')
        f.flush()
        assert sink == []
        assert f.suppressed is True


# ── AelvoAgent._call_llm streaming ──────────────────────────────────────────

class TestAgentStreaming:
    def test_openai_streams_tokens_to_callback(self, openai_agent):
        stream = iter([
            _openai_chunk("Hel"),
            _openai_chunk("lo"),
            _openai_chunk(" "),
            _openai_chunk("world"),
        ])
        openai_agent.client.chat.completions.create.return_value = stream

        tokens = []
        result = openai_agent._call_llm(
            [{"role": "user", "content": "hi"}], on_token=tokens.append
        )

        assert result == "Hello world"
        assert "".join(tokens) == "Hello world"
        _, kwargs = openai_agent.client.chat.completions.create.call_args
        assert kwargs.get("stream") is True

    def test_openai_non_streaming_path_unchanged(self, openai_agent):
        resp = MagicMock()
        resp.choices = [MagicMock()]
        resp.choices[0].message.content = "plain"
        openai_agent.client.chat.completions.create.return_value = resp

        result = openai_agent._call_llm([{"role": "user", "content": "hi"}])

        assert result == "plain"
        _, kwargs = openai_agent.client.chat.completions.create.call_args
        assert "stream" not in kwargs or kwargs.get("stream") is False

    def test_openai_stream_skips_usage_chunks(self, openai_agent):
        usage_chunk = MagicMock()
        usage_chunk.choices = []  # final usage/metadata chunk
        stream = iter([_openai_chunk("ok"), usage_chunk, _openai_chunk("!")])
        openai_agent.client.chat.completions.create.return_value = stream

        tokens = []
        result = openai_agent._call_llm(
            [{"role": "user", "content": "hi"}], on_token=tokens.append
        )

        assert result == "ok!"
        assert "".join(tokens) == "ok!"

    def test_anthropic_streams_tokens(self, openai_agent):
        openai_agent.sdk_type = "anthropic"
        openai_agent.client = MagicMock()
        stream_cm = MagicMock()
        stream_cm.__enter__.return_value.text_stream = iter(["Hello", " world"])
        openai_agent.client.messages.stream.return_value = stream_cm

        tokens = []
        result = openai_agent._call_llm(
            [{"role": "user", "content": "hi"}], on_token=tokens.append
        )

        assert result == "Hello world"
        assert "".join(tokens) == "Hello world"

    def test_google_streams_tokens(self, openai_agent, monkeypatch):
        openai_agent.sdk_type = "google"

        class _Chunk:
            def __init__(self, text):
                self.text = text

        fake_genai = SimpleNamespace(
            configure=lambda **kwargs: None,
            types=SimpleNamespace(GenerationConfig=lambda **kwargs: kwargs),
            GenerativeModel=lambda *a, **k: SimpleNamespace(
                generate_content=lambda *a, **k: iter([_Chunk("Gem"), _Chunk("ini")])
            ),
        )
        monkeypatch.setitem(sys.modules, "google.generativeai", fake_genai)
        # ``import google.generativeai as genai`` resolves through the parent
        # ``google`` package attribute once the real SDK has been imported by
        # an earlier test in the suite — patch that too so the fake always wins
        # regardless of test order.
        try:
            import google as _google_pkg
        except ImportError:
            _google_pkg = None
        if _google_pkg is not None:
            # ``raising=False``: the attribute may not exist yet on the
            # namespace package; set it either way so ``import
            # google.generativeai as genai`` resolves to the fake regardless
            # of whether the real SDK was imported earlier in the suite.
            monkeypatch.setattr(_google_pkg, "generativeai", fake_genai, raising=False)

        tokens = []
        result = openai_agent._call_llm(
            [{"role": "user", "content": "hi"}], on_token=tokens.append
        )

        assert result == "Gemini"
        assert "".join(tokens) == "Gemini"

    def test_send_user_message_forwards_on_token(self, openai_agent):
        stream = iter([_openai_chunk("Ans"), _openai_chunk("wer")])
        openai_agent.client.chat.completions.create.return_value = stream

        tokens = []
        result = openai_agent.send_user_message("hello", on_token=tokens.append)

        assert result == "Answer"
        assert "".join(tokens) == "Answer"
        # The user message + assistant reply are both in history.
        roles = [m["role"] for m in openai_agent.conversation_history]
        assert roles == ["user", "assistant"]

    def test_cache_hit_returns_without_streaming(self, openai_agent):
        openai_agent._llm_cache.get.return_value = "cached answer"
        tokens = []
        result = openai_agent._call_llm(
            [{"role": "user", "content": "hi"}], on_token=tokens.append
        )
        assert result == "cached answer"
        assert tokens == []
        openai_agent.client.chat.completions.create.assert_not_called()


# ── TerminalSession live rendering ──────────────────────────────────────────

class TestTerminalSessionStreaming:
    def _session(self):
        return TerminalSession(build_console())

    def test_on_token_renders_live_and_marks_streamed(self):
        ts = self._session()
        with ts.console.capture() as cap:
            ts.on_token("Hel")
            ts.on_token("lo")
        assert ts.streamed is True
        assert ts._streamed_text == "Hello"
        assert "Hello" in cap.get()

    def test_on_token_empty_ignored(self):
        ts = self._session()
        ts.on_token("")
        assert ts.streamed is False

    def test_on_final_answer_stores_when_not_streamed(self):
        ts = self._session()
        ts.on_final_answer("Done fixing the bug.")
        assert ts.final_answer == "Done fixing the bug."
        assert ts.streamed is False

    def test_streamed_turn_still_records_final_answer(self):
        ts = self._session()
        ts.on_token("Wr")
        ts.on_token("itten")
        ts.on_final_answer("Written")  # trailing whole-answer callback
        assert ts.streamed is True
        assert ts.final_answer == "Written"

    def test_tool_lines_unaffected_by_streaming(self):
        ts = self._session()
        asyncio.run(ts.emit_tool(
            RuntimeEventType.TOOL_COMPLETED, "read_file", "foo.py", "completed", 0
        ))
        rendered = " ".join(str(l) for l in ts._lines)
        assert "✓" in rendered and "read_file" in rendered


# ── Orchestrator wiring ─────────────────────────────────────────────────────

class _RecordingAgent:
    """Agent stub that records on_token and answers the follow-up call.

    Mirrors AelvoAgent's interface: the orchestrator and pipeline now call
    the bounded async wrapper ``send_user_message_async`` (which the real
    agent implements via ``asyncio.to_thread(send_user_message, ...)``).
    """

    def __init__(self, follow_up: str):
        self.conversation_history = []
        self.last_context = None
        self.follow_up = follow_up

    def send_user_message(self, msg: str, on_token=None):
        return self._emit(msg, on_token)

    async def send_user_message_async(self, msg: str, on_token=None, **kwargs):
        return self._emit(msg, on_token)

    def _emit(self, msg: str, on_token=None):
        if on_token is not None:
            # Emit tokens as if the SDK streamed them.
            for piece in self.follow_up.split(" "):
                on_token(piece + " ")
            on_token("")
        return self.follow_up

    def feed_result(self, result: dict):
        self.conversation_history.append(result)


def _real_orchestrator():
    """Build a minimal Orchestrator with mock memory/kernel for tool loops."""
    import tempfile
    from core.governance.kernel import MemoryEngine
    from core.orchestration import Orchestrator

    tmp = tempfile.mkdtemp(prefix="stream_test_")
    anchor = os.path.join(tmp, "a.md")
    with open(anchor, "w", encoding="utf-8") as f:
        f.write("---\nconstraints:\n  DEV_NAME: {value: AELVO User, locked: true}\n---\n")

    engine = MemoryEngine(
        db_path=os.path.join(tmp, "m.db"),
        anchor_path=anchor,
        tool_registry={},
        project_name="stream_test",
    )

    def _mock_read(path="", **kwargs):
        return {"status": "success", "logs": f"Read {path}", "executed": {"path": path}}

    def _mock_respond(message="", **kwargs):
        return {"status": "success", "logs": message, "executed": {"message": message}}

    engine.tools = {"read_file": {"fn": _mock_read}, "respond": {"fn": _mock_respond}}

    orch = Orchestrator(
        memory_engine=engine,
        kernel=None,
        base_path=tmp,
    )
    orch.runtime_runner._tool_executor = orch._graph_tool_executor
    return orch


class TestOrchestratorStreaming:
    @pytest.mark.asyncio
    async def test_tool_loop_streams_final_prose(self):
        """The follow-up LLM call streams prose tokens to token_callback and
        the final answer is returned."""
        orch = _real_orchestrator()
        agent = _RecordingAgent(follow_up="The fix is complete now")
        tokens = []
        raw_output = json.dumps([{"tool": "read_file", "args": {"path": "x.py"}}])

        final = await orch._execute_tool_loop(
            agent, raw_output, token_callback=tokens.append
        )
        assert final == "The fix is complete now"
        # Short prose is buffered during sniffing, then flushed once.
        assert "".join(tokens) == "The fix is complete now "

    @pytest.mark.asyncio
    async def test_tool_loop_streams_long_prose_live(self):
        """Long prose crosses the sniff window and streams token-by-token."""
        orch = _real_orchestrator()
        long_answer = ("word " * 120).strip()  # ~600 chars, way over the window
        agent = _RecordingAgent(follow_up=long_answer)
        tokens = []
        raw_output = json.dumps([{"tool": "read_file", "args": {"path": "x.py"}}])

        final = await orch._execute_tool_loop(
            agent, raw_output, token_callback=tokens.append
        )
        assert final == long_answer
        assert "".join(tokens) == long_answer + " "
        assert len(tokens) > 1  # streamed in pieces, not a single flush

    @pytest.mark.asyncio
    async def test_tool_loop_suppresses_tool_json_follow_up(self):
        """If the follow-up returns more tool JSON, nothing is streamed."""
        orch = _real_orchestrator()
        agent = _RecordingAgent(
            follow_up=json.dumps([{"tool": "respond", "args": {"message": "Done."}}])
        )
        tokens = []
        raw_output = json.dumps([{"tool": "read_file", "args": {"path": "x.py"}}])

        final = await orch._execute_tool_loop(
            agent, raw_output, token_callback=tokens.append
        )
        assert tokens == []  # tool JSON never shown
        assert "Done" in final

    @pytest.mark.asyncio
    async def test_tool_loop_without_token_callback_is_noop(self):
        """token_callback=None keeps the legacy path byte-identical."""
        orch = _real_orchestrator()
        agent = _RecordingAgent(follow_up="plain answer")
        raw_output = json.dumps([{"tool": "read_file", "args": {"path": "x.py"}}])

        final = await orch._execute_tool_loop(agent, raw_output)
        assert final == "plain answer"


class TestPipelineStreaming:
    @pytest.mark.asyncio
    async def test_consolidated_turn_forwards_on_token(self):
        from core.orchestration.pipeline import RuntimePipeline

        class Agent:
            def __init__(self):
                self.calls = []

            async def send_user_message_async(self, msg, on_token=None, **kwargs):
                self.calls.append((msg, on_token))
                if on_token is not None:
                    on_token("The plan is ready.")
                return "The plan is ready."

        agent = Agent()
        pipeline = RuntimePipeline.__new__(RuntimePipeline)
        sink = []
        out = await pipeline._execute_consolidated_turn(
            agent, "prompt", on_token=TokenStreamFilter(sink.append, max_sniff_chars=8)
        )
        assert out == "The plan is ready."
        # TokenStreamFilter with a short sniff window went live immediately.
        assert "".join(sink) == "The plan is ready."
        # send_user_message received the on_token callback.
        assert agent.calls[0][1] is not None

    @pytest.mark.asyncio
    async def test_pipeline_run_threads_token_callback(self, monkeypatch):
        """pipeline.run passes stream/token callbacks to the consolidated turn."""
        from core.orchestration.pipeline import (
            PipelineContext,
            PipelinePhase,
            PipelineResult,
            RuntimePipeline,
        )

        async def fake_execute(self, agent, prompt, on_token=None):
            if on_token is not None:
                on_token("x" * 300)  # exceed sniff window -> live
            return "consolidated output"

        monkeypatch.setattr(RuntimePipeline, "_execute_consolidated_turn", fake_execute)

        class FakeOrch:
            specialist_failures = {}
            verification_pipeline = None
            cognitive_engine = None
            _plan_calibration = None

        pipe = RuntimePipeline(FakeOrch())
        pipe.specialists = {}

        # A real PipelineContext so the phase loop's record_phase_result,
        # handoff extraction and verification steps all work.
        ctx = PipelineContext(user_input="task", conversation_history=[])
        ctx.project = "p"
        ctx.workspace_path = "/w"
        monkeypatch.setattr(pipe, "_build_initial_context", lambda *a, **k: ctx)
        monkeypatch.setattr(
            pipe, "_determine_phases", lambda c: [PipelinePhase.CALIBRATION]
        )
        monkeypatch.setattr(pipe, "_build_consolidated_context", lambda c: {})
        monkeypatch.setattr(pipe, "_build_consolidated_prompt", lambda c, x: "prompt")
        monkeypatch.setattr(pipe, "_notify_pipeline_start", lambda c: None)
        monkeypatch.setattr(pipe, "_notify_pipeline_complete", lambda r: None)
        monkeypatch.setattr(pipe, "_consolidate_memory", _async_false)

        sink = []
        result = await pipe.run(
            "task", object(), [],
            stream_callback=lambda m: None,
            token_callback=sink.append,
        )
        assert isinstance(result, PipelineResult)
        # The consolidated turn ran with a live-streaming filter.
        assert "".join(sink) == "x" * 300


async def _async_false(*args, **kwargs):
    return False
