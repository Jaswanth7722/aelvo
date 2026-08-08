"""
commands.py — Slash-command registry for the AELVO CLI.

Slash commands are the terminal-native way to drive the agent without burning
a token on a chat turn: pick providers and models, inspect status, clear
history, and exit. AELVO opens any folder directly — there is no workspace
registry or switching command.

Model selection lives in ``/provider`` (provider → its model picker) and
``/model`` — there is no separate ``/models`` listing command.

A handler returns an optional ``(action, payload)`` tuple the REPL acts on:
    * ``("exit", None)``      — leave the REPL
    * ``("run", prompt)``     — execute a chat turn (used by /ask and /retry)
    * ``None``                — the command already printed its output
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from typing import Any, Optional, Tuple

from prompt_toolkit.output import create_output
from rich.table import Table
from rich.text import Text

from cli.theme import is_interactive

log = logging.getLogger("aelvo.cli")

# alias → canonical command
_ALIASES = {
    "help": "help", "h": "help", "?": "help",
    "exit": "exit", "quit": "exit", "q": "exit",
    "clear": "clear",
    "pwd": "pwd",
    "status": "status", "info": "status",
    "provider": "provider", "providers": "provider", "switch": "provider",
    "model": "model",
    "mode": "mode", "modes": "mode", "effort": "mode",
    "log": "log", "logs": "log",
    "version": "version", "sysinfo": "version", "ver": "version",
    "retry": "retry",
    "ask": "ask",
}

_COMMANDS = {
    "help": ("Show this help", "/help"),
    "exit": ("Exit the CLI", "/exit"),
    "clear": ("Clear the screen ('/clear history' resets the conversation)", "/clear [history]"),
    "pwd": ("Print the active folder", "/pwd"),
    "status": ("Provider, model, folder + agent metrics", "/status"),
    "provider": ("Pick / switch the LLM provider; keys are asked inline and existing ones can be rotated", "/provider [name] [key]"),
    "model": ("Pick or switch the active model", "/model [name]"),
    "mode": ("Set agent effort: low | medium | high | max", "/mode [low|medium|high|max]"),
    "log": ("Tail the AELVO log file", "/log [lines]"),
    "version": ("Show version and environment info", "/version"),
    "retry": ("Re-run the previous prompt", "/retry"),
    "ask": ("Run a prompt without the agent loop", "/ask <prompt>"),
}


class CliContext:
    """Everything a slash-command handler needs from the running app."""

    def __init__(
        self,
        *,
        agent,
        orchestrator,
        memory_engine,
        aelvo_kernel,
        console,
        db_path: str,
        workspace_path: str,
        project: str,
        mcp_cli=None,
        runtime_cli=None,
        provider_runtime=None,
        fs=None,
        provider_name=None,
        model=None,
    ):
        self.agent = agent
        self.orchestrator = orchestrator
        self.memory_engine = memory_engine
        self.aelvo_kernel = aelvo_kernel
        self.console = console
        self.db_path = db_path
        self.workspace_path = workspace_path
        self.project = project
        self.mcp_cli = mcp_cli
        self.runtime_cli = runtime_cli
        self.provider_runtime = provider_runtime
        self.fs = fs
        self.provider_name = provider_name
        self.model = model
        self.state: dict = {"last_prompt": ""}
        self.exit_requested = False


def is_slash_command(line: str) -> bool:
    """True when a prompt line starts a slash command."""
    return line.strip().startswith("/")


def parse_command(line: str) -> Tuple[str, str]:
    """Split ``/name arg`` into (canonical-name, arg)."""
    parts = line.strip().split(maxsplit=1)
    raw = parts[0][1:].lower() if parts else ""
    arg = parts[1].strip() if len(parts) > 1 else ""
    return _ALIASES.get(raw, raw), arg


def help_table() -> Table:
    table = Table(title="AELVO CLI commands", title_style="aelvo.gold")
    table.add_column("Command", style="aelvo.brand")
    table.add_column("Usage", style="aelvo.purple")
    table.add_column("Description", style="aelvo.snow")
    for name, (desc, usage) in _COMMANDS.items():
        table.add_row(f"/{name}", usage, desc)
    return table


async def handle_command(
    ctx: CliContext, name: str, arg: str
) -> Optional[Tuple[str, Any]]:
    """Run a slash command; returns an optional (action, payload) for the REPL."""
    if name not in _COMMANDS:
        ctx.console.print(
            Text(f"Unknown command: /{name} — type /help", style="aelvo.err")
        )
        return None

    if name == "help":
        ctx.console.print(help_table())
    elif name == "exit":
        ctx.exit_requested = True
        return ("exit", None)
    elif name == "clear":
        _cmd_clear(ctx, arg)
    elif name == "pwd":
        ctx.console.print(Text(ctx.workspace_path or "-", style="aelvo.snow"))
    elif name == "status":
        _cmd_status(ctx)
    elif name == "provider":
        from cli.providers import pick_provider, provider_table, switch_provider
        parts = arg.split(maxsplit=1)
        pname = parts[0].strip().lower() if parts else ""
        pkey = parts[1].strip() if len(parts) > 1 else ""
        if pname:
            await switch_provider(ctx, pname, pkey)
        else:
            # Interactive: two-step picker (provider → its model); cancel or
            # non-tty falls back to the table.
            picked = await pick_provider(ctx)
            if picked:
                pkey, pmodel = picked
                await switch_provider(ctx, pkey, model_override=pmodel)
            else:
                ctx.console.print(provider_table(ctx))
                ctx.console.print(
                    Text("Switch with: /provider <name> [api-key]  ·  the key is asked when you pick a provider", style="aelvo.dim")
                )
    elif name == "model":
        await _cmd_model(ctx, arg)
    elif name == "mode":
        await _cmd_mode(ctx, arg)
    elif name == "log":
        _cmd_log(ctx, arg)
    elif name == "version":
        await _cmd_version(ctx)
    elif name == "retry":
        last = ctx.state.get("last_prompt", "")
        if not last:
            ctx.console.print(Text("No previous prompt to retry.", style="aelvo.err"))
        else:
            return ("run", last)
    elif name == "ask":
        if arg:
            return ("run", arg)
        ctx.console.print(Text("Usage: /ask <prompt>", style="aelvo.err"))
    return None


# ── individual commands ─────────────────────────────────────────────────────

def _clear_screen(ctx: CliContext) -> None:
    """Clear the terminal without fighting the prompt_toolkit renderer.

    rich's ``Console.clear()`` writes a raw ANSI escape that clashes with
    prompt_toolkit's own screen buffer — the next REPL render redraws the old
    content, so ``/clear`` looked like a jump instead of a wipe. Clearing
    through prompt_toolkit's output API erases the screen and homes the cursor
    for real; the next ``prompt_async`` paints a fresh prompt on a clean
    screen.
    """
    if not is_interactive():
        # Piped / non-interactive context (line REPL, tests, CI): never emit
        # ANSI escapes into non-terminal output.
        ctx.console.clear()
        return
    try:
        out = create_output()
        out.erase_screen()
        out.cursor_goto(0, 0)
        out.flush()
    except Exception:
        # Win32/VT output can fail on odd consoles — fall back to rich's clear
        # (which no-ops when not interactive).
        ctx.console.clear()


def _cmd_clear(ctx: CliContext, arg: str) -> None:
    _clear_screen(ctx)
    if arg.lower() in ("history", "all", "reset", "memory"):
        if ctx.agent is not None:
            ctx.agent.conversation_history = []
        ctx.state["last_prompt"] = ""
        ctx.console.print(Text("Conversation history cleared.", style="aelvo.purple"))
    else:
        ctx.console.print(
            Text("Screen cleared. Use '/clear history' to reset the conversation.", style="aelvo.purple")
        )


def _cmd_status(ctx: CliContext) -> None:
    table = Table(title="AELVO status", title_style="aelvo.gold")
    table.add_column("Key", style="aelvo.purple")
    table.add_column("Value", style="aelvo.snow")
    table.add_row("Project", ctx.project or "-")
    table.add_row("Folder", ctx.workspace_path or "-")
    table.add_row("Provider", ctx.provider_name or "not configured")
    table.add_row("Model", ctx.model or "-")
    # Agent effort mode (low/medium/high/max) — /mode to change it.
    from cli.modes import read_mode

    table.add_row("Mode", read_mode(ctx))
    # Where the active provider's API key lives (env var vs encrypted vault).
    from cli.providers import api_key_source, get_registry

    if ctx.provider_name:
        cfg = get_registry().get(ctx.provider_name.lower())
        if cfg is not None:
            source = api_key_source(ctx.provider_name.lower(), cfg.env_key)
            key_label = {"env": "env var", "vault": "encrypted vault", "local": "none needed"}.get(
                source, "not configured"
            )
        else:
            key_label = "not configured"
    else:
        key_label = "not configured"
    table.add_row("API key", key_label)
    table.add_row("Turns", str(getattr(ctx.orchestrator, "_turn_counter", 0)))

    # System-prompt cache metrics (hits vs regenerations)
    if ctx.agent is not None and hasattr(ctx.agent, "prompt_cache_stats"):
        stats = ctx.agent.prompt_cache_stats()
        table.add_row(
            "Prompt cache",
            f"{stats['hits']} hits / {stats['regenerations']} regens "
            f"({stats['hit_rate'] * 100:.1f}% hit rate)",
        )

    ctx.console.print(table)

    metrics = getattr(ctx.orchestrator, "agent_metrics", None)
    if metrics is not None:
        try:
            report = metrics.generate_report()
            compact = {k: v for k, v in report.items() if k != "generated_at"}
            ctx.console.print(
                Text("Agent metrics:", style="aelvo.gold")
            )
            ctx.console.print(json.dumps(compact, indent=2, default=str))
        except Exception as exc:
            ctx.console.print(Text(f"metrics unavailable: {exc}", style="aelvo.dim"))

    if ctx.runtime_cli is not None:
        try:
            # RuntimeCLI.execute prints the dashboard itself; just confirm.
            result = ctx.runtime_cli.execute("#status dashboard")
            if isinstance(result, dict) and result.get("msg"):
                ctx.console.print(Text(result["msg"], style="aelvo.dim"))
        except Exception as exc:
            ctx.console.print(Text(f"runtime dashboard unavailable: {exc}", style="aelvo.dim"))


async def _cmd_model(ctx: CliContext, arg: str) -> None:
    """Show the active model, or open the picker / switch it on the live agent."""
    from cli.providers import available_models, pick_model

    name = arg.strip()
    if not name:
        # Interactive: open the picker; cancel or non-tty falls back to a list.
        if ctx.agent is not None and ctx.provider_name:
            picked = await pick_model(ctx)
            if picked:
                _apply_model(ctx, picked)
                return
        table = Table(title="Model", title_style="aelvo.gold")
        table.add_column("Key", style="aelvo.purple")
        table.add_column("Value", style="aelvo.snow")
        table.add_row("Provider", ctx.provider_name or "not configured")
        table.add_row("Current model", ctx.model or "-")
        ctx.console.print(table)
        if ctx.provider_name:
            available, _src = await available_models(ctx, ctx.provider_name)
            if available:
                ctx.console.print(Text("Available:", style="aelvo.gold"))
                for m in available:
                    ctx.console.print(Text(f"  • {m}", style="aelvo.snow"))
        ctx.console.print(Text("Switch with: /model <name>", style="aelvo.dim"))
        return

    if ctx.agent is None:
        ctx.console.print(
            Text("No active provider — set one first with /provider.", style="aelvo.err")
        )
        return
    _apply_model(ctx, name)


def _apply_model(ctx: CliContext, name: str) -> None:
    """Apply a model switch: live agent + ctx + .env + process env."""
    from cli.providers import write_env

    if ctx.agent is not None:
        ctx.agent.model = name
    ctx.model = name
    write_env("LLM_MODEL", name)
    os.environ["LLM_MODEL"] = name
    ctx.console.print(Text(f"✓ Model set to {name}", style="aelvo.ok"))


async def _cmd_mode(ctx: CliContext, arg: str) -> None:
    """Show the active effort mode, or open the picker / set it directly."""
    from cli.modes import (
        AGENT_MODES,
        mode_table,
        pick_mode,
        write_mode,
    )

    name = arg.strip().lower()
    if name:
        if name not in AGENT_MODES:
            ctx.console.print(
                Text(
                    f"Unknown mode: {name} — use low, medium, high or max",
                    style="aelvo.err",
                )
            )
            return
        write_mode(ctx, name)
        ctx.console.print(
            Text(f"✓ Mode set to {name} ({AGENT_MODES[name][0]})", style="aelvo.ok")
        )
        return

    # No argument: open the picker when interactive; fall back to the table.
    picked = await pick_mode(ctx)
    if picked:
        write_mode(ctx, picked)
        ctx.console.print(
            Text(
                f"✓ Mode set to {picked} ({AGENT_MODES[picked][0]})",
                style="aelvo.ok",
            )
        )
        return

    ctx.console.print(mode_table(ctx))
    ctx.console.print(
        Text(
            "Switch with: /mode <low|medium|high|max>  ·  low=chat, medium=chat+tools, high=full agent, max=collaborative",
            style="aelvo.dim",
        )
    )


def _cmd_log(ctx: CliContext, arg: str) -> None:
    """Tail the AELVO log file (default 40 lines)."""
    try:
        n = max(1, min(200, int(arg.strip() or "40")))
    except ValueError:
        n = 40
    from config.settings import get_data_dir

    log_path = os.path.join(str(get_data_dir()), ".aelvo_runtime", "aelvo.log")
    if not os.path.exists(log_path):
        ctx.console.print(
            Text("No log file yet — boot AELVO once to generate .aelvo_runtime/aelvo.log", style="aelvo.dim")
        )
        return
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception as exc:
        ctx.console.print(Text(f"Could not read log: {exc}", style="aelvo.err"))
        return
    ctx.console.print(Text(f"── aelvo.log · last {min(n, len(lines))} of {len(lines)} lines ──", style="aelvo.gold"))
    for line in lines[-n:]:
        ctx.console.print(Text(line.rstrip("\n"), style="aelvo.dim"))


async def _cmd_version(ctx: CliContext) -> None:
    """Show AELVO + environment version info (live update check in the REPL)."""
    import platform

    from cli.version import __version__

    table = Table(title="AELVO environment", title_style="aelvo.gold")
    table.add_column("Key", style="aelvo.purple")
    table.add_column("Value", style="aelvo.snow")
    table.add_row("AELVO CLI", __version__)
    table.add_row("Python", platform.python_version())
    table.add_row("Platform", platform.platform())
    table.add_row("Provider", ctx.provider_name or "not configured")
    table.add_row("Model", ctx.model or "-")
    table.add_row("Project", ctx.project or "-")
    table.add_row("Folder", ctx.workspace_path or "-")
    for mod_name in ("openai", "anthropic", "rich", "prompt_toolkit", "chromadb"):
        try:
            mod = __import__(mod_name)
            table.add_row(mod_name, getattr(mod, "__version__", "?"))
        except Exception:
            table.add_row(mod_name, "not installed")
    ctx.console.print(table)

    # Live npm update check — interactive sessions only, so piped runs
    # (tests, CI) stay hermetic and never touch the network. The registry
    # fetch runs in a worker thread so the async REPL loop never blocks.
    try:
        if sys.stdin.isatty() and sys.stdout.isatty():
            from cli.update_check import reminder

            hint = await asyncio.to_thread(reminder, refresh=True)
            if hint:
                ctx.console.print(Text(hint, style="aelvo.gold"))
    except Exception:
        pass
