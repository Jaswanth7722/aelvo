"""
commands.py — Slash-command registry for the AELVO CLI.

Slash commands are the terminal-native way to drive the agent without burning
a token on a chat turn: switch workspaces, inspect status, list projects and
models, clear history, and exit.

A handler returns an optional ``(action, payload)`` tuple the REPL acts on:
    * ``("exit", None)``      — leave the REPL
    * ``("run", prompt)``     — execute a chat turn (used by /ask and /retry)
    * ``None``                — the command already printed its output
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any, Optional, Tuple

from rich.table import Table
from rich.text import Text

log = logging.getLogger("aelvo.cli")

# alias → canonical command
_ALIASES = {
    "help": "help", "h": "help", "?": "help",
    "exit": "exit", "quit": "exit", "q": "exit",
    "clear": "clear",
    "workspace": "workspace", "open": "workspace", "cd": "workspace",
    "pwd": "pwd",
    "status": "status", "info": "status",
    "projects": "projects", "list": "projects",
    "models": "models",
    "retry": "retry",
    "ask": "ask",
}

_COMMANDS = {
    "help": ("Show this help", "/help"),
    "exit": ("Exit the CLI", "/exit"),
    "clear": ("Clear the screen ('/clear history' resets the conversation)", "/clear [history]"),
    "workspace": ("Point the agent at a folder (re-jails file tools)", "/workspace <dir>"),
    "pwd": ("Print the active workspace", "/pwd"),
    "status": ("Provider, model, workspace + agent metrics", "/status"),
    "projects": ("List known workspaces", "/projects"),
    "models": ("List available models", "/models"),
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
        workspace_switcher=None,
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
        self.workspace_switcher = workspace_switcher
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
    elif name == "workspace":
        _cmd_workspace(ctx, arg)
    elif name == "pwd":
        ctx.console.print(Text(ctx.workspace_path or "-", style="aelvo.snow"))
    elif name == "status":
        _cmd_status(ctx)
    elif name == "projects":
        _cmd_projects(ctx)
    elif name == "models":
        _cmd_models(ctx)
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

def _cmd_clear(ctx: CliContext, arg: str) -> None:
    ctx.console.clear()
    if arg.lower() in ("history", "all", "reset", "memory"):
        if ctx.agent is not None:
            ctx.agent.conversation_history = []
        ctx.state["last_prompt"] = ""
        ctx.console.print(Text("Conversation history cleared.", style="aelvo.purple"))
    else:
        ctx.console.print(
            Text("Screen cleared. Use '/clear history' to reset the conversation.", style="aelvo.purple")
        )


def _cmd_workspace(ctx: CliContext, arg: str) -> None:
    if not arg.strip():
        ctx.console.print(Text(ctx.workspace_path or "-", style="aelvo.snow"))
        return
    target = os.path.expanduser(arg.strip())
    if not os.path.isabs(target):
        target = os.path.abspath(target)
    created = False
    if not os.path.isdir(target):
        os.makedirs(target, exist_ok=True)
        created = True

    if ctx.workspace_switcher is None:
        ctx.console.print(Text("Workspace switching is unavailable in this mode.", style="aelvo.err"))
        return

    resolved = ctx.workspace_switcher(target)  # validates dir + invalidates prompt cache
    try:
        ctx.orchestrator.set_workspace_root(resolved)
    except Exception as exc:
        log.debug("Orchestrator workspace switch failed: %s", exc)
    if ctx.fs is not None:
        try:
            ctx.fs.set_base_path(resolved)
        except Exception as exc:
            log.debug("fs.set_base_path failed: %s", exc)
    ctx.workspace_path = resolved
    note = " (created)" if created else ""
    ctx.console.print(Text(f"✓ Workspace{note}: {resolved}", style="aelvo.ok"))


def _cmd_status(ctx: CliContext) -> None:
    table = Table(title="AELVO status", title_style="aelvo.gold")
    table.add_column("Key", style="aelvo.purple")
    table.add_column("Value", style="aelvo.snow")
    table.add_row("Project", ctx.project or "-")
    table.add_row("Workspace", ctx.workspace_path or "-")
    table.add_row("Provider", ctx.provider_name or "not configured")
    table.add_row("Model", ctx.model or "-")
    table.add_row("Turns", str(getattr(ctx.orchestrator, "_turn_counter", 0)))
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


def _cmd_projects(ctx: CliContext) -> None:
    from config.settings import BASE_DIR

    db = os.path.join(BASE_DIR, "global_memory.db")
    table = Table(title="Known workspaces", title_style="aelvo.gold")
    table.add_column("Name", style="aelvo.brand")
    table.add_column("Path", style="aelvo.snow")
    table.add_column("Last opened", style="aelvo.dim")
    try:
        with sqlite3.connect(db) as conn:
            rows = conn.execute(
                "SELECT name, path, last_opened FROM projects ORDER BY last_opened DESC"
            ).fetchall()
    except Exception as exc:
        ctx.console.print(Text(f"Could not list projects: {exc}", style="aelvo.err"))
        return
    if not rows:
        ctx.console.print(Text("No workspaces yet.", style="aelvo.dim"))
        return
    for name, path, last_opened in rows:
        table.add_row(name or "", path or "", str(last_opened or ""))
    ctx.console.print(table)


def _cmd_models(ctx: CliContext) -> None:
    models: list = []
    if ctx.provider_runtime is not None:
        try:
            models = ctx.provider_runtime.list_models() or []
        except Exception as exc:
            log.debug("provider_runtime.list_models failed: %s", exc)
    if not models:
        try:
            from core.registry import MODEL_REGISTRY

            models = list(MODEL_REGISTRY.keys())
        except Exception as exc:
            log.debug("MODEL_REGISTRY unavailable: %s", exc)
    if not models:
        ctx.console.print(Text("No model registry available.", style="aelvo.dim"))
        return
    table = Table(title="Available models", title_style="aelvo.gold")
    table.add_column("Model", style="aelvo.snow")
    for m in models:
        table.add_row(str(m))
    ctx.console.print(table)
