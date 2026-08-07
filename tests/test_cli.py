"""Tests for the AELVO terminal CLI (cli/ package + main.py --cli wiring)."""

import asyncio
import os
import sqlite3
import subprocess
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


@pytest.fixture(autouse=True)
def _no_live_model_network(monkeypatch):
    """Live model fetches are unit-tested in test_live_models.py — the CLI
    tests must never hit the network (a stray real API key on the dev machine
    would otherwise trigger an actual request)."""
    import cli.live_models as lm

    async def _none_async(*_a, **_k):
        return None

    monkeypatch.setattr(lm, "fetch_live_models_async", _none_async)
    monkeypatch.setattr(lm, "fetch_live_models", lambda *_a, **_k: None)


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
    assert parse_command("/switch nvidia")[0] == "provider"
    assert parse_command("/providers")[0] == "provider"
    assert parse_command("/sysinfo")[0] == "version"
    assert parse_command("/logs 20")[0] == "log"
    assert parse_command("/model gpt-4o")[0] == "model"


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


# ── provider / model / apikey / log / version commands ──────────────────────

from unittest.mock import MagicMock  # noqa: E402


def test_provider_table_lists_registry():
    from cli.providers import provider_table

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="nvidia", model="m",
    )
    themed = build_console()
    with themed.capture() as cap:
        themed.print(provider_table(ctx))
    rendered = cap.get()
    assert "openai" in rendered
    assert "nvidia" in rendered
    assert "active" in rendered


def test_provider_switch_unknown_provider():
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
    )
    result = asyncio.run(handle_command(ctx, "provider", "no_such_provider_xyz"))
    assert result is None  # prints an error, no crash


def test_provider_switch_builds_agent(monkeypatch):
    from cli import providers

    built = {}

    def fake_build(provider_key, cfg, api_key, model, pr):
        built["key"] = provider_key
        built["api_key"] = api_key
        agent = MagicMock()
        agent.model = model
        return agent

    monkeypatch.setattr(providers, "build_agent", fake_build)
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    monkeypatch.setattr(providers, "store_api_key", lambda *a, **k: True)
    monkeypatch.setattr(providers, "_vault_key", lambda k: "")

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    asyncio.run(handle_command(ctx, "provider", "nvidia sk-test-123"))
    assert ctx.provider_name == "nvidia"
    assert ctx.agent is not None
    assert built["key"] == "nvidia"
    assert built["api_key"] == "sk-test-123"
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


def test_provider_switch_reuses_vault_key(monkeypatch):
    from cli import providers

    agent = MagicMock()
    monkeypatch.setattr(providers, "build_agent", lambda *a, **k: agent)
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    monkeypatch.setattr(providers, "store_api_key", lambda *a, **k: True)
    monkeypatch.setattr(providers, "_vault_key", lambda k: "sk-from-vault")
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
    )
    asyncio.run(handle_command(ctx, "provider", "nvidia"))
    assert ctx.provider_name == "nvidia"
    assert ctx.agent is agent
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


def test_model_switch_updates_agent(monkeypatch):
    from cli import providers

    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    agent = MagicMock()
    agent.model = "old-model"
    ctx = CliContext(
        agent=agent, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="nvidia", model="old-model",
    )
    asyncio.run(handle_command(ctx, "model", "gpt-4o"))
    assert ctx.model == "gpt-4o"
    assert agent.model == "gpt-4o"
    monkeypatch.delenv("LLM_MODEL", raising=False)


def test_model_requires_provider():
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
    )
    result = asyncio.run(handle_command(ctx, "model", "gpt-4o"))
    assert result is None  # prints an error, no crash


def test_apikey_command_removed():
    """/apikey no longer exists — the key lives inside provider selection."""
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
    )
    result = asyncio.run(handle_command(ctx, "apikey", "sk-123"))
    assert result is None  # unknown command → error print, no crash


def test_switch_provider_prompts_and_stores_missing_key(monkeypatch):
    """A provider without a key prompts for it inline and stores it exactly once."""
    from cli import providers

    stored = {}
    monkeypatch.setattr(providers, "prompt_api_key", _async_val("sk-fresh"))
    monkeypatch.setattr(
        providers, "store_api_key", lambda k, n, v: stored.update(key=k, value=v) or True
    )
    monkeypatch.setattr(providers, "build_agent", lambda *a, **k: MagicMock())
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    monkeypatch.setattr(providers, "_vault_key", lambda k: "")
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    ok = asyncio.run(providers.switch_provider(ctx, "nvidia"))
    assert ok
    assert stored == {"key": "nvidia", "value": "sk-fresh"}  # prompted, not inline
    assert ctx.provider_name == "nvidia"
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


def test_switch_provider_does_not_restore_existing_key(monkeypatch):
    """An already-stored key must not be persisted again on every switch
    (the vault uses INSERT OR REPLACE keyed on a fresh UUID per store)."""
    from cli import providers

    stored = []
    agent = MagicMock()
    monkeypatch.setattr(providers, "build_agent", lambda *a, **k: agent)
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    monkeypatch.setattr(providers, "store_api_key", lambda k, n, v: stored.append(v) or True)
    monkeypatch.setattr(providers, "_vault_key", lambda k: "sk-from-vault")
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    assert asyncio.run(providers.switch_provider(ctx, "nvidia"))
    assert stored == []  # never re-stored
    assert ctx.provider_name == "nvidia"
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


def test_switch_provider_cancelled_key_aborts(monkeypatch):
    """Esc on the inline key prompt cancels the switch (no key, no store)."""
    from cli import providers

    stored = []
    monkeypatch.setattr(providers, "prompt_api_key", _async_val(""))  # cancelled
    monkeypatch.setattr(providers, "store_api_key", lambda *a, **k: stored.append(1) or True)
    monkeypatch.setattr(providers, "build_agent", lambda *a, **k: MagicMock())
    monkeypatch.setattr(providers, "_vault_key", lambda k: "")
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    assert not asyncio.run(providers.switch_provider(ctx, "nvidia"))
    assert stored == []
    assert ctx.provider_name is None


def test_pick_provider_prompts_and_stores_key_before_model(monkeypatch):
    """The key is part of provider selection: provider → (prompt + store key) →
    model, and the stored key powers the model step's live list."""
    from cli import picker, providers

    calls = []
    stored = {}

    async def fake_pick_item(title, items, **kwargs):
        calls.append(title)
        # Step 1 = provider, step 2 = its models (the key step has no picker).
        return "google" if len(calls) == 1 else "gemini-2.5-pro"

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "")  # no key anywhere
    monkeypatch.setattr(providers, "prompt_api_key", _async_val("AIza-fresh-key"))
    monkeypatch.setattr(
        providers, "store_api_key", lambda k, n, v: stored.update(key=k, value=v) or True
    )
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    result = asyncio.run(providers.pick_provider(ctx))
    assert result == ("google", "gemini-2.5-pro")
    assert stored == {"key": "google", "value": "AIza-fresh-key"}


def test_pick_provider_cancelled_key_aborts(monkeypatch):
    """Cancelling the inline key prompt stops the whole two-step flow."""
    from cli import picker, providers

    calls = []
    stored = []

    async def fake_pick_item(title, items, **kwargs):
        calls.append(title)
        return "google"

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "")
    monkeypatch.setattr(providers, "prompt_api_key", _async_val(""))  # cancelled
    monkeypatch.setattr(providers, "store_api_key", lambda *a, **k: stored.append(1) or True)
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    assert asyncio.run(providers.pick_provider(ctx)) is None
    assert stored == []  # nothing stored on cancel
    assert len(calls) == 1  # never reached the model step


def test_provider_flow_stores_key_exactly_once(monkeypatch):
    """End-to-end /provider two-step flow with a fresh key: the key is prompted
    and stored exactly once — pick_provider stores it, switch_provider must
    NOT re-store an existing key (vault rows are keyed on fresh UUIDs)."""
    from cli import picker, providers

    stores = []

    async def fake_pick_item(title, items, **kwargs):
        # Step 1 = provider picker, step 2 = model picker.
        return "openai" if title.startswith("Select a provider") else "gpt-4o"

    def fake_store(key, name, value):
        stores.append((key, value))
        return True

    def fake_resolve(key, env_key):
        # Once pick_provider stores the key, it is visible to switch_provider
        # (mirrors store_api_key setting os.environ[env_key]).
        return "sk-e2e" if stores else ""

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    monkeypatch.setattr(providers, "resolve_api_key", fake_resolve)
    monkeypatch.setattr(providers, "prompt_api_key", _async_val("sk-e2e"))
    monkeypatch.setattr(providers, "store_api_key", fake_store)
    monkeypatch.setattr(providers, "build_agent", lambda *a, **k: MagicMock())
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    asyncio.run(handle_command(ctx, "provider", ""))
    assert ctx.provider_name == "openai"
    assert ctx.model == "gpt-4o"
    assert stores == [("openai", "sk-e2e")]  # exactly once across the whole flow
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


_KEY_PROMPT_DRIVER = r'''
import asyncio, sys
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from cli import picker, providers

picker.is_interactive = lambda: True  # prompt_api_key checks cli.picker.is_interactive

async def _main():
    with create_pipe_input() as inp:
        inp.send_text(sys.argv[1])
        return await providers.prompt_api_key("Test Provider", _input=inp, _output=DummyOutput())

print(repr(asyncio.run(_main())))
'''


def _drive_key_prompt(keys: str) -> str:
    """Run the hidden key prompt in a fresh interpreter (same isolation trick
    as the picker tests — prompt_toolkit leaves global loop state behind)."""
    out = subprocess.run(
        [sys.executable, "-c", _KEY_PROMPT_DRIVER, keys],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def test_prompt_api_key_enter_returns_value():
    """Enter submits the pasted key (password-masked field)."""
    assert _drive_key_prompt("sk-test-123\r") == "'sk-test-123'"


def test_prompt_api_key_escape_cancels():
    """Esc cancels the key prompt and returns '' (callers abort the switch)."""
    assert _drive_key_prompt("\x1b") == "''"


def test_log_command_without_file():
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
    )
    result = asyncio.run(handle_command(ctx, "log", ""))
    assert result is None  # no crash


def test_version_command_prints():
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="nvidia", model="m",
    )
    result = asyncio.run(handle_command(ctx, "version", ""))
    assert result is None  # no crash


def test_api_key_source_detects_env_vs_vault(monkeypatch):
    """api_key_source: env var wins over the vault; empty when neither — and
    the env var name itself is never revealed."""
    from cli import providers

    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    monkeypatch.setattr(providers, "has_api_key", lambda k, e: True)
    assert providers.api_key_source("nvidia", "NVIDIA_API_KEY") == "vault"

    monkeypatch.setattr(providers, "has_api_key", lambda k, e: False)
    assert providers.api_key_source("nvidia", "NVIDIA_API_KEY") == ""

    os.environ["NVIDIA_API_KEY"] = "nvapi-test-123"
    try:
        assert providers.api_key_source("nvidia", "NVIDIA_API_KEY") == "env"
    finally:
        monkeypatch.delenv("NVIDIA_API_KEY", raising=False)


def test_status_shows_api_key_source(monkeypatch):
    """/status reports where the active provider's key came from (env vs vault)
    without leaking the key itself."""
    from cli import providers

    console = build_console()
    monkeypatch.setattr(providers, "api_key_source", lambda k, e: "vault")
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=console, db_path="", workspace_path=".", project="t",
        provider_name="nvidia", model="m",
    )
    with console.capture() as cap:
        asyncio.run(handle_command(ctx, "status", ""))
    rendered = cap.get()
    assert "API key" in rendered
    assert "encrypted vault" in rendered


def test_status_no_provider_shows_not_configured():
    """With no active provider the API key row says not configured (the
    source lookup is skipped entirely — nothing to resolve)."""
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    with ctx.console.capture() as cap:
        asyncio.run(handle_command(ctx, "status", ""))
    rendered = cap.get()
    assert "API key" in rendered
    assert "not configured" in rendered


# ── interactive pickers (/provider and /model with no args) ────────────────

def _async_val(value):
    """Return an async function that yields ``value``."""
    async def _fn(*_a, **_k):
        return value
    return _fn


def test_pick_item_returns_none_when_not_interactive(monkeypatch):
    """On non-interactive terminals (pipes, CI, tests) the picker must not
    launch a full-screen app — it returns None so callers fall back."""
    from cli import picker

    monkeypatch.setattr(picker, "is_interactive", lambda: False)
    result = asyncio.run(picker.pick_item("t", [("a", "A"), ("b", "B")]))
    assert result is None


_PICKER_DRIVER = r'''
import asyncio, sys
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from cli import picker

picker.is_interactive = lambda: True
KEYS = sys.argv[1]

async def _main():
    with create_pipe_input() as inp:
        inp.send_text(KEYS)
        return await picker.pick_item(
            "t", [("a", "A"), ("b", "B"), ("c", "C")],
            _input=inp,
            _output=DummyOutput(),
        )

print(repr(asyncio.run(_main())))
'''


def _drive_picker(keys: str) -> str:
    """Run one pick_item in a fresh interpreter.

    prompt_toolkit leaves global loop/input state behind after an
    ``asyncio.run`` exits, so a second full-screen Application in the same
    process can hang on Windows. Each run gets its own process instead.
    """
    out = subprocess.run(
        [sys.executable, "-c", _PICKER_DRIVER, keys],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def test_pick_item_enter_selects_default():
    """Pressing Enter (CR, the terminal's enter byte) selects the first item."""
    assert _drive_picker("\r") == "'a'"


def test_pick_item_arrow_down_then_enter():
    """Down arrow moves the cursor; Enter selects the second item."""
    assert _drive_picker("\x1b[B\r") == "'b'"


def test_pick_item_escape_cancels():
    """Esc cancels the picker and returns None."""
    assert _drive_picker("\x1b") == "None"


def test_pick_item_jk_navigation():
    """Vi-style j/k navigation works too."""
    assert _drive_picker("j\r") == "'b'"


def test_pick_provider_builds_items(monkeypatch):
    """Two-step pick_provider: offers every registered provider (creds/active
    markers), then its models, and returns (key, model)."""
    from cli import picker, providers

    captured = {"calls": [], "items": []}

    async def fake_pick_item(title, items, **kwargs):
        captured["calls"].append(title)
        captured["items"].append(list(items))
        # Step 1 = provider, step 2 = its models.
        return "openai" if len(captured["calls"]) == 1 else "gpt-4o"

    # pick_provider does `from cli.picker import pick_item` at call time.
    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    # Key already stored → the inline key step is skipped (2 picker calls).
    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "sk-test")
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="nvidia", model=None,
    )
    assert asyncio.run(providers.pick_provider(ctx)) == ("openai", "gpt-4o")
    provider_items = captured["items"][0]
    model_items = captured["items"][1]
    keys = [v for v, _ in provider_items]
    assert "openai" in keys and "nvidia" in keys and "google" in keys
    labels = " ".join(lbl for _, lbl in provider_items)
    assert "active" in labels  # nvidia marked as the current provider
    model_ids = [v for v, _ in model_items]
    assert model_ids[0] == "gpt-5"  # curated default first
    assert "gpt-4o" in model_ids and "o3" in model_ids
    assert len(captured["calls"]) == 2  # provider step, then model step
    assert captured["calls"][1] == "Select a model · openai"


def test_pick_provider_cancelled_at_model_step_keeps_default(monkeypatch):
    """Cancelling the model step yields (key, its default model)."""
    from cli import picker, providers

    calls = []

    async def fake_pick_item(title, items, **kwargs):
        calls.append(title)
        return "openai" if len(calls) == 1 else None  # cancel the model step

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "sk-test")
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    assert asyncio.run(providers.pick_provider(ctx)) == ("openai", "gpt-5")


NVIDIA_DEFAULT = "nvidia/nemotron-3-super-120b-a12b"


def test_provider_noarg_picker_switches_provider(monkeypatch):
    """Two-step selection runs the switch with the picked model (key from vault)."""
    from cli import providers

    agent = MagicMock()
    monkeypatch.setattr(providers, "pick_provider", _async_val(("nvidia", NVIDIA_DEFAULT)))
    monkeypatch.setattr(providers, "build_agent", lambda *a, **k: agent)
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    monkeypatch.setattr(providers, "store_api_key", lambda *a, **k: True)
    monkeypatch.setattr(providers, "_vault_key", lambda k: "sk-from-vault")
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    assert asyncio.run(handle_command(ctx, "provider", "")) is None
    assert ctx.provider_name == "nvidia"
    assert ctx.model == NVIDIA_DEFAULT  # the model picked in step 2
    assert ctx.agent is agent
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


def test_provider_noarg_picker_cancelled_model_keeps_default(monkeypatch):
    """Esc on the model step still switches, with the provider's default model."""
    from cli import providers

    agent = MagicMock()
    agent.model = "gpt-4o"
    monkeypatch.setattr(providers, "pick_provider", _async_val(("openai", "")))
    monkeypatch.setattr(providers, "build_agent", lambda *a, **k: agent)
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    monkeypatch.setattr(providers, "store_api_key", lambda *a, **k: True)
    monkeypatch.setattr(providers, "_vault_key", lambda k: "sk-openai")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    assert asyncio.run(handle_command(ctx, "provider", "")) is None
    assert ctx.provider_name == "openai"
    assert ctx.model == "gpt-5"  # openai's default model
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


def test_provider_noarg_cancelled_falls_back_to_table(monkeypatch):
    """Cancelling the provider step (or non-tty) prints the plain provider table."""
    from cli import providers

    console = build_console()
    monkeypatch.setattr(providers, "pick_provider", _async_val(None))
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=console, db_path="", workspace_path=".", project="t",
    )
    with console.capture() as cap:
        result = asyncio.run(handle_command(ctx, "provider", ""))
    assert result is None
    assert "Available providers" in cap.get()


def test_model_noarg_picker_switches(monkeypatch):
    """Selecting a model in the picker applies it to the live agent + env."""
    from cli import providers

    monkeypatch.setattr(providers, "pick_model", _async_val("gpt-4o"))
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    agent = MagicMock()
    agent.model = "old-model"
    ctx = CliContext(
        agent=agent, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model="old-model",
    )
    assert asyncio.run(handle_command(ctx, "model", "")) is None
    assert ctx.model == "gpt-4o"
    assert agent.model == "gpt-4o"
    monkeypatch.delenv("LLM_MODEL", raising=False)


def test_pick_model_offers_only_provider_models(monkeypatch):
    """The model picker must offer only the provider's curated models (no
    runtime embedding/random ids), styled like the provider picker."""
    from cli import picker, providers

    captured = {}

    async def fake_pick_item(title, items, **kwargs):
        captured["title"] = title
        captured["items"] = list(items)
        return "gpt-4o"

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    ctx = CliContext(
        agent=MagicMock(), orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model="gpt-4o",
    )
    assert asyncio.run(providers.pick_model(ctx)) == "gpt-4o"
    models = [v for v, _ in captured["items"]]
    assert models[0] == "gpt-5"  # curated default first
    assert "gpt-4o" in models and "o3" in models and "gpt-5-mini" in models
    assert len(models) >= 8  # top ~10, not just 3
    assert not any("embedding" in m for m in models)  # no runtime junk
    labels = " ".join(lbl for _, lbl in captured["items"])
    assert "current" in labels  # current model marked like the provider picker


def test_pick_model_for_new_provider_marks_default(monkeypatch):
    """Two-step flow: picking models for a provider that is NOT active marks
    its default model (not the old provider's current model)."""
    from cli import picker, providers

    captured = {}

    async def fake_pick_item(title, items, **kwargs):
        captured["items"] = list(items)
        return None  # cancel the model step

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    ctx = CliContext(
        agent=MagicMock(), orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="nvidia", model="some-old-model",
    )
    assert asyncio.run(providers.pick_model(ctx, "openai")) == ""
    labels = " ".join(lbl for _, lbl in captured["items"])
    assert "● default" in labels  # openai's default (gpt-5) preselected
    assert "some-old-model" not in labels


def test_list_models_for_is_provider_scoped():
    """list_models_for must prefer the curated per-provider list even when a
    provider_runtime exists (which would offer uncurated ids)."""
    from cli import providers

    class FakeRuntime:
        def list_models(self, provider_key):
            return ["gpt-4o", "text-embedding-3-large", "random-model-xyz"]

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_runtime=FakeRuntime(), provider_name="openai", model=None,
    )
    models = providers.list_models_for(ctx, "openai")
    assert models[0] == "gpt-5" and "gpt-4o" in models and "o3" in models
    assert "text-embedding-3-large" not in models
    assert "random-model-xyz" not in models


# ── live model lists (fetch → merge → picker) ───────────────────────────────

def test_available_models_live_merges_with_curated(monkeypatch):
    """A successful live fetch merges with the curated catalog — curated first
    (default on top), fresh live-only ids appended — and reports 'live'."""
    from cli import live_models, providers

    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "sk-live")
    monkeypatch.setattr(
        live_models, "fetch_live_models_async", _async_val(["gpt-5", "brand-new-live-model"])
    )
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model=None,
    )
    models, source = asyncio.run(providers.available_models(ctx, "openai"))
    assert source == "live"
    assert models[0] == "gpt-5"  # curated default first
    assert "brand-new-live-model" in models
    assert models.index("brand-new-live-model") > models.index("gpt-4o")


def test_available_models_no_key_uses_catalog(monkeypatch):
    """Without a key there is no fetch — the curated catalog is the source."""
    from cli import providers

    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "")
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model=None,
    )
    models, source = asyncio.run(providers.available_models(ctx, "openai"))
    assert source == "catalog"
    assert models[0] == "gpt-5"


def test_available_models_live_failure_uses_catalog(monkeypatch):
    """A failed live fetch (None) silently falls back to the curated catalog —
    the picker can never break on a network error."""
    from cli import live_models, providers

    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "sk-live")
    monkeypatch.setattr(
        live_models, "fetch_live_models_async", _async_val(None)
    )
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model=None,
    )
    models, source = asyncio.run(providers.available_models(ctx, "openai"))
    assert source == "catalog"
    assert models[0] == "gpt-5"


def test_openrouter_live_list_is_capped_to_vendor_prefixes(monkeypatch):
    """OpenRouter's live list is huge — only known vendor-prefixed routing
    families are merged in, so the picker stays sane."""
    from cli import live_models, providers

    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "sk-or")
    monkeypatch.setattr(
        live_models,
        "fetch_live_models_async",
        _async_val(
            [
                "anthropic/claude-sonnet-4-20250514",
                "some/random-vendor-model",
                "x-ai/grok-4",
                "totally/unrelated",
            ]
        ),
    )
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openrouter", model=None,
    )
    models, source = asyncio.run(providers.available_models(ctx, "openrouter"))
    assert source == "live"
    assert "anthropic/claude-sonnet-4-20250514" in models
    assert "x-ai/grok-4" in models
    assert "some/random-vendor-model" not in models
    assert "totally/unrelated" not in models


def test_list_models_for_live_first(monkeypatch):
    """The sync list_models_for also goes live-first when a key is present."""
    from cli import live_models, providers

    monkeypatch.setattr(providers, "resolve_api_key", lambda k, e: "sk-live")
    monkeypatch.setattr(
        live_models, "fetch_live_models", lambda *a, **k: ["gpt-5", "gpt-5-new"]
    )
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model=None,
    )
    models = providers.list_models_for(ctx, "openai")
    assert models[0] == "gpt-5"
    assert "gpt-5-new" in models


def test_pick_model_live_title_and_items(monkeypatch):
    """A live list is flagged '(live)' in the picker title and the fresh ids
    are offered alongside the curated ones."""
    from cli import picker, providers

    captured = {}

    async def fake_pick_item(title, items, **kwargs):
        captured["title"] = title
        captured["items"] = list(items)
        return "gpt-5-live-beta"

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    monkeypatch.setattr(
        providers, "available_models", _async_val((["gpt-5", "gpt-5-live-beta"], "live"))
    )
    ctx = CliContext(
        agent=MagicMock(), orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model="gpt-5",
    )
    assert asyncio.run(providers.pick_model(ctx)) == "gpt-5-live-beta"
    assert captured["title"].endswith("(live)")
    assert [v for v, _ in captured["items"]] == ["gpt-5", "gpt-5-live-beta"]


def test_cmd_models_live_title(monkeypatch):
    """/models reflects the live source in the table title."""
    from cli import providers

    console = build_console()
    monkeypatch.setattr(
        providers, "available_models", _async_val((["gpt-5", "gpt-5-live"], "live"))
    )
    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=console, db_path="", workspace_path=".", project="t",
        provider_name="openai", model=None,
    )
    with console.capture() as cap:
        asyncio.run(handle_command(ctx, "models", ""))
    rendered = cap.get()
    assert "Available models (live)" in rendered
    assert "gpt-5-live" in rendered


def test_switch_provider_keeps_explicit_live_model(monkeypatch):
    """A model explicitly picked from the live list must survive the switch —
    the curated-only validation list must not clamp it to the default."""
    from cli import providers

    built = {}

    def fake_build(provider_key, cfg, api_key, model, pr):
        built["model"] = model
        agent = MagicMock()
        agent.model = model
        return agent

    monkeypatch.setattr(providers, "build_agent", fake_build)
    monkeypatch.setattr(providers, "write_env", lambda k, v: None)
    monkeypatch.setattr(providers, "store_api_key", lambda *a, **k: True)
    monkeypatch.setattr(providers, "_vault_key", lambda k: "sk-openai")
    monkeypatch.setattr(
        providers, "available_models", _async_val((["gpt-5", "gpt-4o"], "catalog"))
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    ctx = CliContext(
        agent=None, orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name=None, model=None,
    )
    ok = asyncio.run(
        providers.switch_provider(ctx, "openai", model_override="gpt-5-live-beta")
    )
    assert ok
    assert built["model"] == "gpt-5-live-beta"  # not clamped to gpt-5
    assert ctx.model == "gpt-5-live-beta"
    monkeypatch.delenv("LLM_PROVIDER", raising=False)


# ── context + cost-tier hints ───────────────────────────────────────────────

def test_format_context_window():
    from core.registry.models import format_context_window

    assert format_context_window(131072) == "128k"
    assert format_context_window(400000) == "400k"
    assert format_context_window(200000) == "200k"
    assert format_context_window(2000000) == "2M"
    assert format_context_window(1047576) == "1M"
    assert format_context_window(1048576) == "1M"
    assert format_context_window(65536) == "64k"
    assert format_context_window(8192) == "8k"
    # Small windows render as raw digits; zero/negative → '?' (never a crash
    # or a bogus '-1k' in the picker rows).
    assert format_context_window(512) == "512"
    assert format_context_window(0) == "?"
    assert format_context_window(-1000) == "?"


def test_registry_package_exports():
    """CostTier and the tier/format helpers are reachable from the package
    root (``core.registry``), not just the models submodule."""
    from core.registry import (  # noqa: F401
        MODEL_REGISTRY,
        CostTier,
        format_context_window,
        get_cost_tier,
        get_model_manifest,
    )

    assert CostTier.PREMIUM.value == "premium"
    assert format_context_window(400000) == "400k"
    assert get_cost_tier("openai", "gpt-5") == CostTier.STANDARD


def test_get_cost_tier_runtime_fallback():
    """Live-only models get a price-derived tier + real context from the
    runtime registry; curated models keep their curated tier; unknown → standard."""
    from core.registry.models import CostTier, get_cost_tier, get_model_manifest

    # gpt-4-turbo is NOT in the curated openai catalog but has real pricing
    # ($10 in / $30 out per 1M) in auth.config → premium tier + real context.
    assert get_cost_tier("openai", "gpt-4-turbo") == CostTier.PREMIUM
    assert get_model_manifest("openai", "gpt-4-turbo").context_window == 128000
    # Curated models keep their curated tiers.
    assert get_cost_tier("openai", "gpt-5") == CostTier.STANDARD
    assert get_cost_tier("openai", "gpt-4o-mini") == CostTier.BUDGET
    assert get_cost_tier("anthropic", "claude-opus-4-1-20250805") == CostTier.PREMIUM
    # Unknown models default to standard.
    assert get_cost_tier("openai", "gpt-9-future-model") == CostTier.STANDARD


def test_pick_model_rows_show_context_and_tier(monkeypatch):
    """Each picker row shows the model's context window and cost tier, and
    the [● current] marker is preserved."""
    from cli import picker, providers

    captured = {}

    async def fake_pick_item(title, items, **kwargs):
        captured["items"] = list(items)
        return "gpt-4o-mini"

    monkeypatch.setattr(picker, "pick_item", fake_pick_item)
    ctx = CliContext(
        agent=MagicMock(), orchestrator=None, memory_engine=None, aelvo_kernel=None,
        console=build_console(), db_path="", workspace_path=".", project="t",
        provider_name="openai", model="gpt-4o-mini",
    )
    assert asyncio.run(providers.pick_model(ctx)) == "gpt-4o-mini"
    rows = {v: lbl for v, lbl in captured["items"]}
    assert "400k" in rows["gpt-5"] and "$$" in rows["gpt-5"]  # standard tier
    assert "128k" in rows["gpt-4o"] and "$$" in rows["gpt-4o"]
    assert "128k" in rows["gpt-4o-mini"] and "$" in rows["gpt-4o-mini"]  # budget
    assert "[● current]" in rows["gpt-4o-mini"]  # marker preserved


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
