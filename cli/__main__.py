"""
__main__.py — Dedicated AELVO terminal CLI entry point.

Lets you run the terminal agent directly, without booting the whole web
platform (no MCP discovery, no long-horizon planning, no repo scans — the
lean boot in ``cli/boot.py`` spins up only what the REPL needs):

    python -m cli                          # interactive REPL
    python -m cli "fix the auth bug"       # one-shot prompt (positional)
    python -m cli --ask "fix the auth bug" # one-shot prompt (explicit)
    python -m cli -w path/to/folder        # open a folder as the workspace
    python -m cli --provider openai --model gpt-4o
    python -m cli --list-providers         # show providers + credential status
    python -m cli --version

On Windows, ``aelvo.bat`` at the repo root is an alias for ``python -m cli``.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform

__version__ = "2.0.0"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="aelvo",
        description="AELVO terminal agent — plan, code, verify, report (CodeBuff style).",
        epilog="examples:\n"
               "  python -m cli\n"
               "  python -m cli \"refactor the auth module\"\n"
               "  python -m cli -w ./my-project --provider openai",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "prompt",
        nargs="*",
        help="prompt → one-shot mode (run once and exit)",
    )
    parser.add_argument(
        "--ask",
        default="",
        help="one-shot prompt (same as a positional prompt)",
    )
    parser.add_argument(
        "--provider",
        default="",
        help="LLM provider key (e.g. openai, anthropic, groq, nvidia)",
    )
    parser.add_argument(
        "--model",
        default="",
        help="model override (e.g. gpt-4o, claude-3-5-sonnet-20241022)",
    )
    parser.add_argument(
        "--project",
        default="",
        help="workspace/project name (default: most recently used)",
    )
    parser.add_argument(
        "-w",
        "--workspace",
        default="",
        help="open a specific folder as the workspace (re-jails file tools)",
    )
    parser.add_argument(
        "--log-level",
        default="",
        help="console log verbosity: debug|info|warning|error (default: error)",
    )
    parser.add_argument(
        "--list-providers",
        action="store_true",
        help="list providers + credential status, then exit",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="show version and environment info, then exit",
    )
    return parser.parse_args(argv)


def _configure_logging() -> None:
    """Same quiet-console / full-file logging setup the main boot uses."""
    from main import _configure_logging as _setup

    _setup()


def _print_version() -> None:
    """Lightweight --version (no backend boot)."""
    from rich.table import Table

    from cli.theme import build_console

    console = build_console()
    table = Table(title="AELVO environment", title_style="aelvo.gold")
    table.add_column("Key", style="aelvo.purple")
    table.add_column("Value", style="aelvo.snow")
    table.add_row("AELVO CLI", __version__)
    table.add_row("Python", platform.python_version())
    table.add_row("Platform", platform.platform())
    table.add_row("Provider", os.environ.get("LLM_PROVIDER", "not configured"))
    for mod_name in ("rich", "prompt_toolkit"):
        try:
            mod = __import__(mod_name)
            table.add_row(mod_name, getattr(mod, "__version__", "?"))
        except Exception:
            table.add_row(mod_name, "not installed")
    console.print(table)


def _print_providers() -> None:
    """Lightweight --list-providers (no backend boot)."""
    from cli.commands import CliContext
    from cli.providers import provider_table
    from cli.theme import build_console
    from rich.text import Text

    console = build_console()
    ctx = CliContext(
        agent=None,
        orchestrator=None,
        memory_engine=None,
        aelvo_kernel=None,
        console=console,
        db_path="",
        workspace_path=os.getcwd(),
        project=os.environ.get("AELVO_PROJECT", ""),
        provider_name=os.environ.get("LLM_PROVIDER", ""),
    )
    console.print(provider_table(ctx))
    console.print(
        Text(
            "Switch with: python -m cli --provider <name>  ·  "
            "set a key in the REPL with /provider",
            style="aelvo.dim",
        )
    )


def main(argv=None) -> int:
    args = parse_args(argv)

    if args.log_level:
        os.environ["AELVO_LOG_LEVEL"] = args.log_level

    if args.version:
        _print_version()
        return 0

    if args.list_providers:
        _print_providers()
        return 0

    # One-shot prompt: --ask wins over positional.
    one_shot = (args.ask or " ".join(args.prompt)).strip()

    # Map flags → env (same contract main.py's argument parser uses).
    if args.provider:
        os.environ["LLM_PROVIDER"] = args.provider
        os.environ["AELVO_PROVIDER"] = args.provider
    if args.model:
        os.environ["LLM_MODEL"] = args.model
        os.environ["AELVO_MODEL"] = args.model
    if args.project:
        os.environ["AELVO_PROJECT"] = args.project

    _configure_logging()

    try:
        asyncio.run(_run(project=args.project, workspace_dir=args.workspace, one_shot=one_shot))
    except KeyboardInterrupt:
        return 130
    return 0


async def _run(project: str, workspace_dir: str, one_shot: str) -> None:
    from cli.app import run_cli
    from cli.boot import boot_backend

    backend = await boot_backend(project=project, workspace_dir=workspace_dir)
    try:
        await run_cli(**backend, one_shot=one_shot)
    finally:
        # Parity with main.main_async's CLI cleanup: close the workspace DB
        # connections (process exit would flush them anyway, but this avoids
        # unclosed-connection warnings when the CLI is embedded in a runner).
        for _obj in (backend.get("aelvo_kernel"), backend.get("memory_engine")):
            try:
                _obj.conn.close()
                _obj.db.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
