"""
modes.py — Agent effort modes for the AELVO CLI.

Backs the ``/mode`` slash command. The effort mode dials how much of the
agentic machinery runs per turn:

    * ``low``    — plain chat. One direct LLM call, no pipeline, no
                   specialist ceremony, no tool loop. Fastest; best for
                   small talk and quick questions.
    * ``medium`` — chat + tools. One direct LLM call but the tool loop is
                   active, so the model can use tools when it genuinely
                   needs them (Claude Code style). No plan / specialists.
    * ``high``   — full agent (default). The canonical consolidated
                   pipeline: HERMES context → ARCHITECT plan → ORACLE →
                   FORGE → SENTINEL → TERMINUS → HERALD, with the tool loop.
    * ``max``    — collaborative. Forces the Mode B task-board pipeline:
                   decomposition, consensus, verification, recovery.

The choice is persisted per-folder in ``.aelvo/mode`` (same hidden state dir
as the command history) and overridable with the ``AELVO_MODE`` environment
variable. ``/status`` shows the active mode.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from rich.table import Table

log = logging.getLogger("aelvo.cli")

#: Canonical effort-mode keys in display order.
MODE_LOW = "low"
MODE_MEDIUM = "medium"
MODE_HIGH = "high"
MODE_MAX = "max"

DEFAULT_MODE = MODE_HIGH

#: mode key → (short label, description shown in the picker).
AGENT_MODES: dict = {
    MODE_LOW: (
        "Low — chat",
        "One direct answer, no tools or specialists. Fastest.",
    ),
    MODE_MEDIUM: (
        "Medium — chat + tools",
        "Direct answer, but tools are available when needed.",
    ),
    MODE_HIGH: (
        "High — full agent",
        "Consolidated pipeline: HERMES → ARCHITECT → FORGE → … → HERALD.",
    ),
    MODE_MAX: (
        "Max — collaborative",
        "Task-board Mode B: decomposition, consensus, verification, recovery.",
    ),
}

_VALID_MODES = frozenset(AGENT_MODES)


def normalize_mode(mode: str) -> str:
    """Coerce an arbitrary string to a valid mode; unknown → default."""
    if not mode:
        return DEFAULT_MODE
    key = str(mode).strip().lower()
    return key if key in _VALID_MODES else DEFAULT_MODE


def _mode_file(ctx) -> str:
    base = getattr(ctx, "workspace_path", None) or "."
    return os.path.join(base, ".aelvo", "mode")


def read_mode(ctx) -> str:
    """Active mode: ``AELVO_MODE`` env → ``.aelvo/mode`` file → default."""
    env = os.environ.get("AELVO_MODE", "").strip().lower()
    if env in _VALID_MODES:
        return env
    try:
        with open(_mode_file(ctx), "r", encoding="utf-8") as f:
            stored = f.read().strip().lower()
            if stored in _VALID_MODES:
                return stored
    except Exception as exc:
        log.debug("Could not read mode file: %s", exc)
    return DEFAULT_MODE


def write_mode(ctx, mode: str) -> bool:
    """Persist the mode to ``.aelvo/mode`` and the process env; True on success."""
    key = normalize_mode(mode)
    os.environ["AELVO_MODE"] = key
    try:
        base = getattr(ctx, "workspace_path", None) or "."
        state_dir = os.path.join(base, ".aelvo")
        os.makedirs(state_dir, exist_ok=True)
        with open(os.path.join(state_dir, "mode"), "w", encoding="utf-8") as f:
            f.write(key)
        return True
    except Exception as exc:
        log.warning("Could not persist mode to .aelvo/mode: %s", exc)
        return False


async def pick_mode(ctx) -> Optional[str]:
    """Open the small full-screen mode picker; returns the mode or None.

    Mirrors the provider picker (alternate screen buffer, arrow keys).
    Returns ``None`` on non-interactive terminals so callers can fall back
    to the plain table.
    """
    from cli.picker import pick_item

    current = read_mode(ctx)
    items = [
        (key, f"{label:<22} {desc}")
        for key, (label, desc) in AGENT_MODES.items()
    ]
    picked = await pick_item(
        "Agent effort mode",
        items,
        subtitle="↑/↓ or j/k move · Enter selects · Esc cancels",
        default=current,
    )
    return picked


def mode_table(ctx) -> Table:
    """Table of all effort modes with the active one marked."""
    table = Table(title="Agent effort modes", title_style="aelvo.gold")
    table.add_column("Mode", style="aelvo.brand")
    table.add_column("What runs per turn", style="aelvo.snow")
    table.add_column("", style="aelvo.ok")
    current = read_mode(ctx)
    for key, (label, desc) in AGENT_MODES.items():
        active = "● active" if key == current else ""
        table.add_row(key, f"{label} — {desc}", active)
    return table
