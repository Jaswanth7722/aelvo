"""
picker.py — Small full-screen selection windows for the AELVO CLI.

``pick_item`` opens a temporary terminal window (the alternate screen
buffer — it takes over the terminal, lets you arrow through a list, and
closes cleanly after you pick, restoring the REPL underneath). This is the
"small shell" behind the interactive ``/provider`` and ``/model`` pickers.

Backed by prompt_toolkit's ``RadioList`` widget, which brings navigation
for free: ↑/↓ or j/k to move, PageUp/PageDown, type-ahead search (just type
a letter to jump), Enter/Space to select, Esc/Ctrl+C to cancel. Zero new
dependencies — everything is already in ``requirements.txt``.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, Optional, Sequence, Tuple

from prompt_toolkit.application import Application
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Label, RadioList

log = logging.getLogger("aelvo.cli")

# Brand palette mirrored from cli/theme.py so the picker matches the REPL.
_PICKER_STYLE = Style.from_dict(
    {
        "aelvo-picker.title": "bold #FFD98E",  # golden white
        "aelvo-picker.hint": "#9B938A",        # dim
        "radio-list": "#F6F1EA",               # snow body
        "radio": "#F6F1EA",
        "radio-selected": "bold #FF9A3C",      # light orange cursor
        "radio-checked": "#8CE99A",            # green check
    }
)


def is_interactive() -> bool:
    """True when both stdin and stdout are real terminals (a picker needs one)."""
    try:
        return bool(sys.stdin.isatty() and sys.stdout.isatty())
    except Exception:
        return False


async def pick_item(
    title: str,
    items: Sequence[Tuple[Any, Any]],
    *,
    subtitle: str = "",
    default: Any = None,
    _input: Any = None,
    _output: Any = None,
) -> Optional[Any]:
    """Full-screen single-select picker; returns the chosen value or None.

    Args:
        title: Window title shown at the top.
        items: Sequence of ``(value, label)`` pairs to choose from.
        subtitle: One-line hint under the title.
        default: Value (a key in ``items``) to preselect, if any.

    Returns the chosen value, or ``None`` when cancelled. On non-interactive
    terminals (pipes, CI, tests) it returns ``None`` immediately so callers
    can fall back to a plain table/list.

    ``_input``/``_output`` inject prompt_toolkit streams for tests (e.g.
    ``create_pipe_input`` + ``DummyOutput``); ``None`` uses the terminal.
    """
    if not is_interactive():
        return None
    items = list(items)
    if not items:
        return None

    try:
        radio = RadioList(values=items, default=default)
    except AssertionError:  # pragma: no cover - only when values is empty
        return None

    kb = KeyBindings()

    # ``eager=True`` is required: prompt_toolkit's key processor only calls the
    # LAST matching binding (``matches[-1]``). RadioList registers its own
    # ``enter``/``space`` and a catch-all ``Keys.Any`` binding, so without
    # eager our submit/cancel handlers would never run.
    @kb.add("enter", eager=True)
    @kb.add(" ", eager=True)
    def _submit(event) -> None:
        # Private _DialogList API (stable in the pinned prompt_toolkit 3.0.52):
        # with eager=True, RadioList's own enter binding is excluded, so this
        # is the only way to sync current_value with the cursor before exiting.
        radio._handle_enter()
        event.app.exit(result=radio.current_value)

    @kb.add("escape", eager=True)
    @kb.add("c-c", eager=True)
    def _cancel(event) -> None:
        event.app.exit(result=None)

    layout = Layout(
        HSplit(
            [
                Label(title, style="class:aelvo-picker.title"),
                Label(subtitle, style="class:aelvo-picker.hint"),
                radio,
                Label(
                    "↑/↓ or j/k move · Enter select · Esc cancel",
                    style="class:aelvo-picker.hint",
                ),
            ]
        )
    )

    app = Application(
        layout=layout,
        key_bindings=kb,
        full_screen=True,  # alternate screen buffer: opens, then restores the REPL
        mouse_support=False,
        style=_PICKER_STYLE,
        input=_input,
        output=_output,
    )
    try:
        return await app.run_async()
    except Exception as exc:  # pragma: no cover - defensive fallback
        log.debug("Picker failed: %s", exc)
        return None
