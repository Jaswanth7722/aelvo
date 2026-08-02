"""
theme.py — AELVO CLI color theme.

The brand palette the user asked for: light orange, golden white, purple, and
snow white. Exposed as a ``rich`` Theme so every CLI surface (banner, tools,
status, prompts) uses the same warm/cool combination.
"""

from __future__ import annotations

import sys

from rich.console import Console
from rich.theme import Theme

# ── Brand palette ────────────────────────────────────────────────────────────
ORANGE = "#FF9A3C"    # light orange — primary brand / prompts / accents
GOLD = "#FFD98E"      # golden white — headlines & highlights
PURPLE = "#B79CFF"    # purple — tools, status, secondary accents
SNOW = "#F6F1EA"      # snow white — body text on dark terminals
RED = "#FF7B72"       # errors / failures
GREEN = "#8CE99A"     # success
DIM = "#9B938A"       # muted / system lines


def build_console() -> Console:
    """Build a ``rich`` console with the AELVO brand theme."""
    # Windows consoles default stdout to cp1252, which cannot encode the emoji
    # and box-drawing characters the CLI uses. Reconfigure the streams to UTF-8
    # and let rich render via ANSI/VT instead of the legacy cp1252 renderer.
    if sys.platform == "win32":
        for _stream in (sys.stdout, sys.stderr):
            try:
                _stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, ValueError, OSError):
                pass

    # Bold is baked into the accent tokens so single-token styles like
    # ``aelvo.brand`` work everywhere (Table columns and ``print(style=)`` do
    # full-string theme lookups and cannot combine a theme name with a space
    # attribute, e.g. ``aelvo.brand bold``).
    theme = Theme(
        {
            "aelvo.brand": f"bold {ORANGE}",
            "aelvo.gold": f"bold {GOLD}",
            "aelvo.purple": f"bold {PURPLE}",
            "aelvo.snow": SNOW,
            "aelvo.ok": GREEN,
            "aelvo.err": RED,
            "aelvo.dim": DIM,
            "aelvo.tool": PURPLE,
            "aelvo.sys": DIM,
        }
    )
    return Console(theme=theme, highlight=False, legacy_windows=False)
