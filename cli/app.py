"""
app.py — AELVO interactive terminal agent (CodeBuff / Claude Code style).

``run_cli`` is invoked from ``main.py`` when ``--cli`` (or ``--ask``) is used.
It reuses the exact same backend components the web dashboard uses — the
kernel, jailed filesystem, memory engine, orchestrator, and MCP subsystem —
so the CLI and web are always in sync.

Features:
    * Streaming REPL: Enter submits, Esc+Enter inserts a newline.
    * Live tool activity rendered as it executes (reads, writes, bash, …).
    * Slash commands: /help /exit /clear /workspace /pwd /status /projects
      /models /retry /ask.
    * Per-workspace command history + auto-suggest.
    * One-shot mode: ``python main.py --cli --ask "prompt"``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import sys
from typing import Optional

from rich.markdown import Markdown
from rich.rule import Rule
from rich.text import Text

from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import NestedCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.styles import Style

from cli.commands import (
    CliContext,
    handle_command,
    is_slash_command,
    parse_command,
)
from cli.session import SessionRecorder, TerminalSession
from cli.theme import build_console

log = logging.getLogger("aelvo.cli")


def _print_banner(console, ctx: CliContext) -> None:
    console.print()
    console.print(Text("AELVO", style="aelvo.brand"))
    console.print(
        Text("the automated engineering & logic-verification agent", style="aelvo.gold")
    )
    console.print(Rule(style="aelvo.brand"))
    console.print(
        Text(
            f"  project: {ctx.project}   provider: {ctx.provider_name or '—'}   "
            f"model: {ctx.model or '—'}",
            style="aelvo.dim",
        )
    )
    console.print(
        Text(f"  workspace: {ctx.workspace_path}", style="aelvo.dim")
    )
    if not ctx.provider_name:
        console.print(
            Text("  ⚠ no LLM provider — type /provider to configure one", style="aelvo.err")
        )
    console.print()


def _make_key_bindings() -> KeyBindings:
    kb = KeyBindings()

    @kb.add("enter")
    def _submit(event):
        event.current_buffer.validate_and_handle()

    @kb.add("escape", "enter")
    def _newline(event):
        event.current_buffer.insert_text("\n")

    return kb


def _make_completer() -> NestedCompleter:
    return NestedCompleter.from_nested_dict(
        {
            "/help": None,
            "/h": None,
            "/exit": None,
            "/quit": None,
            "/clear": None,
            "/workspace": None,
            "/open": None,
            "/cd": None,
            "/pwd": None,
            "/status": None,
            "/projects": None,
            "/models": None,
            "/provider": None,
            "/switch": None,
            "/model": None,
            "/apikey": None,
            "/log": None,
            "/version": None,
            "/retry": None,
            "/ask": None,
        }
    )


def _prompt_style() -> Style:
    return Style.from_dict(
        {
            "prompt": "bold #FF9A3C",
            "aelvo.toolbar": "bold #B79CFF",
        }
    )


def _make_prompt_session(ctx: CliContext) -> PromptSession:
    hist_dir = ctx.workspace_path or "."
    os.makedirs(hist_dir, exist_ok=True)
    history = FileHistory(os.path.join(hist_dir, ".aelvo_history"))
    # NOTE: no ``mouse_support=True`` here — enabling VT mouse tracking makes
    # the terminal capture the scroll wheel instead of scrolling native
    # scrollback, so the user couldn't scroll up through past output. With it
    # off, the terminal's native wheel/PgUp scrolling and click-to-select work.
    return PromptSession(
        history=history,
        completer=_make_completer(),
        key_bindings=_make_key_bindings(),
        style=_prompt_style(),
        auto_suggest=AutoSuggestFromHistory(),
        complete_while_typing=True,
        multiline=True,
        enable_history_search=True,  # up-arrow searches through past prompts
    )


def _toolbar(ctx: CliContext):
    return [
        ("class:aelvo.toolbar", f"  AELVO {ctx.project} | "),
        ("class:aelvo.toolbar", ctx.workspace_path or "?"),
        ("class:aelvo.toolbar", f" | {ctx.provider_name or 'no-provider'}"),
        ("class:aelvo.toolbar", f"/{ctx.model or '-'}"),
    ]


async def _execute_turn_task(ctx, user_input: str, recorder, terminal):
    return await ctx.orchestrator.execute_turn(
        ctx.agent,
        user_input,
        session_tracker=recorder,
        tui_session=terminal,
        stream_callback=terminal.on_final_answer,
        mcp_cli=ctx.mcp_cli,
        db_path=ctx.db_path,
    )


def _is_interactive() -> bool:
    """True when both stdin and stdout are real terminals."""
    try:
        return bool(sys.stdin.isatty() and sys.stdout.isatty())
    except Exception:
        return False


async def _run_turn(ctx: CliContext, user_input: str) -> None:
    """Execute one user prompt through the orchestrator with live rendering."""
    if ctx.agent is None:
        ctx.console.print(
            Text("No LLM provider configured — set one in .env or run the web UI.", style="aelvo.err")
        )
        return
    ctx.state["last_prompt"] = user_input
    recorder = SessionRecorder()
    recorder.user_query = user_input
    terminal = TerminalSession(ctx.console)

    task = asyncio.create_task(
        _execute_turn_task(ctx, user_input, recorder, terminal)
    )
    turn = None
    terminal.start()
    try:
        turn = await task
    except asyncio.CancelledError:
        # Ctrl+C mid-turn: cancel the child task, keep the REPL alive.
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        ctx.console.print(Text("\n⏹ Turn interrupted.", style="aelvo.err"))
        turn = None
    except Exception as exc:
        log.exception("Turn failed")
        ctx.console.print(Text(f"\n✗ Turn failed: {exc}", style="aelvo.err"))
        turn = None
    finally:
        terminal.finish()
        recorder.save(ctx.db_path)

    if turn is None:
        # Leave a blank line so the next prompt isn't glued to the activity feed.
        ctx.console.print()
        return

    answer = (turn.get("output") or "").strip()
    if answer:
        ctx.console.print()
        ctx.console.print(Markdown(answer))
        ctx.console.print()

    specialists = turn.get("specialists_active") or []
    if specialists:
        ctx.console.print(
            Text(f"  specialists: {' → '.join(specialists)}", style="aelvo.dim")
        )


async def _simple_repl(ctx: CliContext) -> None:
    """Line-based REPL for non-interactive stdin (pipes, files, CI, git-bash
    without winpty). Reads commands until EOF or /exit."""
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        if is_slash_command(line):
            name, arg = parse_command(line)
            action = await handle_command(ctx, name, arg)
            if ctx.exit_requested or action == ("exit", None):
                break
            if action and action[0] == "run":
                await _run_turn(ctx, action[1])
            continue
        await _run_turn(ctx, line)


async def _repl(ctx: CliContext) -> None:
    try:
        session = _make_prompt_session(ctx)
    except Exception as exc:
        # prompt_toolkit needs a real console (e.g. fails on git-bash pipes).
        log.debug("prompt_toolkit unavailable (%s) — falling back to line REPL", exc)
        ctx.console.print(Text("(non-interactive stdin — line mode)", style="aelvo.dim"))
        await _simple_repl(ctx)
        return
    while not ctx.exit_requested:
        try:
            text = await session.prompt_async(
                "❯ ", bottom_toolbar=lambda: _toolbar(ctx)
            )
        except (EOFError, KeyboardInterrupt):
            ctx.console.print()
            break

        line = text.strip()
        if not line:
            continue

        if is_slash_command(line):
            name, arg = parse_command(line)
            action = await handle_command(ctx, name, arg)
            if ctx.exit_requested or action == ("exit", None):
                break
            if action and action[0] == "run":
                await _run_turn(ctx, action[1])
            continue

        await _run_turn(ctx, line)


async def run_cli(
    agent,
    orchestrator,
    memory_engine,
    aelvo_kernel,
    db_path: str,
    workspace_path: str,
    project: str,
    mcp_cli=None,
    runtime_cli=None,
    provider_runtime=None,
    fs=None,
    workspace_switcher=None,
    provider_name: Optional[str] = None,
    model: Optional[str] = None,
    one_shot: str = "",
    **_ignored,
) -> None:
    """Launch the terminal CLI. Mirrors the web dashboard's backend wiring."""
    console = build_console()
    ctx = CliContext(
        agent=agent,
        orchestrator=orchestrator,
        memory_engine=memory_engine,
        aelvo_kernel=aelvo_kernel,
        console=console,
        db_path=db_path,
        workspace_path=workspace_path,
        project=project,
        mcp_cli=mcp_cli,
        runtime_cli=runtime_cli,
        provider_runtime=provider_runtime,
        fs=fs,
        workspace_switcher=workspace_switcher,
        provider_name=provider_name,
        model=model,
    )

    _print_banner(console, ctx)

    if agent is None and one_shot:
        console.print(Text("No LLM provider is configured.", style="aelvo.err"))
        console.print(Text("  Set your provider + API key in .env (e.g. LLM_PROVIDER=openai,", style="aelvo.snow"))
        console.print(Text("  OPENAI_API_KEY=sk-...), or run the web UI once to configure a provider", style="aelvo.snow"))
        console.print(Text("  from the browser.", style="aelvo.snow"))
        console.print()
        raise SystemExit(1)

    if agent is None:
        console.print(Text("No LLM provider configured — slash commands only (set .env or run the web UI once).", style="aelvo.err"))

    if one_shot:
        console.print(Rule("ONE-SHOT", style="aelvo.purple"))
        await _run_turn(ctx, one_shot)
        console.print(Rule("DONE", style="aelvo.purple"))
        return

    console.print(Text("type /help for commands · Esc+Enter for a newline · Ctrl+C to exit", style="aelvo.dim"))
    if _is_interactive():
        await _repl(ctx)
    else:
        console.print(Text("(non-interactive stdin — line mode)", style="aelvo.dim"))
        await _simple_repl(ctx)
