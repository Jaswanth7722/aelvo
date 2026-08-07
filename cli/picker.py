"""
picker.py — Small full-screen selection windows for the AELVO CLI.

``pick_item`` opens a temporary terminal window (the alternate screen
buffer — it takes over the terminal, lets you arrow through a list, and
closes cleanly after you pick, restoring the REPL underneath). This is the
"small shell" behind the interactive ``/provider`` picker.

``pick_categorized`` is the same idea, upgraded for the ``/model`` picker:
it groups rows under styled category headers and is fully mouse-friendly —
hovering moves the cursor, a left-click selects, and the wheel scrolls
(with a scrollbar appearing on overflow). Built on a custom
``prompt_toolkit`` control, so zero new dependencies.

Both return ``None`` on non-interactive terminals (pipes, CI, tests) so
callers can fall back to a plain table/list.
"""

from __future__ import annotations

import logging
import sys
from typing import Any, Optional, Sequence, Tuple

from prompt_toolkit.application import Application, get_app
from prompt_toolkit.data_structures import Point
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, ScrollOffsets, ScrollbarMargin, Window
from prompt_toolkit.layout.controls import UIControl, UIContent
from prompt_toolkit.mouse_events import MouseButton, MouseEventType
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Frame, Label, RadioList

log = logging.getLogger("aelvo.cli")

# Brand palette mirrored from cli/theme.py so the picker matches the REPL.
_PICKER_STYLE = Style.from_dict(
    {
        "aelvo-picker.title": "bold #FFD98E",  # golden white
        "aelvo-picker.hint": "#9B938A",        # dim
        "aelvo-picker.cat": "bold #B79CFF",    # purple category headers
        "aelvo-picker.item": "#F6F1EA",        # snow body
        # Dark text on an orange bar — reads as a cursor row, not inverted text.
        "aelvo-picker.selected": "bold #1A1B26 bg:#FF9A3C",
        "frame.border": "#5A4A3A",             # dim frame border
        "scrollbar": "#FF9A3C",                # orange scrollbar thumb
        "scrollbar.arrow": "#FF9A3C",
        "scrollbar.start": "#9B938A",
        "scrollbar.end": "#9B938A",
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


# ── sectioned picker (categories · mouse · scroll) ──────────────────────────

class _SectionListControl(UIControl):
    """List control that renders sectioned rows and understands the mouse.

    Rows are ``(kind, value, text)`` tuples where ``kind`` is ``"header"``
    (a category title — not selectable) or ``"item"`` (selectable). The host
    ``Window`` maps screen coordinates to *content* rows (scroll-adjusted)
    and handles wheel scrolling natively; this control only deals with hover
    (move the cursor) and left-click (select).

    ``on_activate`` is set by the picker and called with the clicked value.
    """

    def __init__(self, rows, item_rows, cursor: int = 0):
        self.rows = rows
        self.item_rows = item_rows
        self.on_activate = None
        n = len(item_rows)
        self.cursor = max(0, min(cursor, n - 1)) if n else 0

    # -- UIControl interface -------------------------------------------------
    def reset(self) -> None:
        pass

    def is_focusable(self) -> bool:
        return True

    def create_content(self, width: int, height: int) -> UIContent:
        return UIContent(
            get_line=lambda i: self._render_row(i),
            line_count=len(self.rows),
            # The Window uses this to auto-scroll (scroll_offsets) and to park
            # the terminal cursor on the selected row.
            cursor_position=Point(
                x=0, y=self.item_rows[self.cursor] if self.item_rows else 0
            ),
            show_cursor=True,
        )

    def _render_row(self, i: int):
        kind, _value, text = self.rows[i]
        if kind == "header":
            return [("class:aelvo-picker.cat", text)]
        selected = i == self.item_rows[self.cursor]
        prefix = "› " if selected else "  "
        style = (
            "class:aelvo-picker.selected" if selected else "class:aelvo-picker.item"
        )
        return [(style, prefix + text)]

    # -- cursor movement -----------------------------------------------------
    @property
    def current_value(self):
        """Value of the row under the cursor (None when the list is empty)."""
        if not self.item_rows:
            return None
        return self.rows[self.item_rows[self.cursor]][1]

    def move_cursor(self, delta: int) -> None:
        self._set_cursor(self.cursor + delta)

    def move_cursor_home(self) -> None:
        self._set_cursor(0)

    def move_cursor_end(self) -> None:
        self._set_cursor(len(self.item_rows) - 1)

    def move_cursor_page(self, delta: int, page_height: int) -> None:
        self._set_cursor(self.cursor + max(1, page_height) * delta)

    # These two are called by the host Window's native wheel handler when the
    # cursor sits at the scroll edge — keep the selection moving with the wheel.
    def move_cursor_up(self) -> None:
        self.move_cursor(-1)

    def move_cursor_down(self) -> None:
        self.move_cursor(1)

    def _set_cursor(self, index: int) -> None:
        n = len(self.item_rows)
        if not n:
            return
        index = max(0, min(n - 1, index))
        if index == self.cursor:
            return  # no change — skip the repaint (hover spam)
        self.cursor = index
        self._invalidate()

    def _invalidate(self) -> None:
        try:
            get_app().invalidate()
        except Exception:
            pass  # no running Application (e.g. direct unit tests)

    # -- mouse ----------------------------------------------------------------
    def mouse_handler(self, mouse_event):
        """Hover moves the cursor, left-click selects. Scroll events are left
        to the host Window (``NotImplemented``) so the wheel scrolls natively."""
        et = mouse_event.event_type
        if et in (MouseEventType.MOUSE_DOWN, MouseEventType.MOUSE_MOVE):
            y = mouse_event.position.y  # content row (scroll-adjusted by Window)
            if 0 <= y < len(self.rows) and self.rows[y][0] == "item":
                self._set_cursor(self.item_rows.index(y))
                # Only a left click selects — right/middle clicks just move the
                # cursor, like hovering.
                if (
                    et == MouseEventType.MOUSE_DOWN
                    and mouse_event.button == MouseButton.LEFT
                    and self.on_activate is not None
                ):
                    self.on_activate(self.current_value)
                return None
        return NotImplemented  # wheel scroll → the host Window scrolls natively


async def pick_categorized(
    title: str,
    sections: Sequence[Tuple[Optional[str], Sequence[Tuple[Any, Any]]]],
    *,
    subtitle: str = "",
    footer: str = "",
    default: Any = None,
    _input: Any = None,
    _output: Any = None,
) -> Optional[Any]:
    """Full-screen, mouse-friendly picker with category sections.

    ``sections`` is a sequence of ``(header, items)`` pairs; a ``None`` header
    renders its items without a category line (used for the custom-id entry).
    Items are ``(value, label)`` pairs. The picker opens the alternate screen
    buffer (a "small shell" that restores the REPL underneath):

    * rows are grouped under styled category headers,
    * the mouse is fully supported: hovering moves the cursor, a left-click
      selects the row, the wheel scrolls (a scrollbar appears on overflow),
    * keys: ↑/↓ or j/k move, PageUp/PageDown page, Home/End jump,
      Enter/Space select, Esc/Ctrl+C cancel.

    Returns the chosen value, or ``None`` when cancelled / non-interactive.

    ``_input``/``_output`` inject prompt_toolkit streams for tests.
    """
    if not is_interactive():
        return None
    rows = []
    item_rows = []
    cursor = 0
    for header, items in sections:
        if header:
            rows.append(("header", None, header))
        for value, label in items:
            rows.append(("item", value, label))
            item_rows.append(len(rows) - 1)
            if default is not None and value == default:
                cursor = len(item_rows) - 1
    if not item_rows:
        return None

    control = _SectionListControl(rows, item_rows, cursor)

    def _page_height() -> int:
        """Rows visible below title/subtitle/border/footer (for PgUp/PgDn)."""
        try:
            return max(1, get_app().output.get_size().rows - 6)
        except Exception:  # pragma: no cover - defensive
            return 8

    kb = KeyBindings()

    # ``eager=True``: the KeyProcessor only calls the LAST matching binding
    # (matches[-1]), so without eager our handlers would be shadowed.
    @kb.add("up", eager=True)
    @kb.add("k", eager=True)
    def _up(event) -> None:
        control.move_cursor(-1)

    @kb.add("down", eager=True)
    @kb.add("j", eager=True)
    def _down(event) -> None:
        control.move_cursor(1)

    @kb.add("pageup", eager=True)
    def _page_up(event) -> None:
        control.move_cursor_page(-1, _page_height())

    @kb.add("pagedown", eager=True)
    def _page_down(event) -> None:
        control.move_cursor_page(1, _page_height())

    @kb.add("home", eager=True)
    def _home(event) -> None:
        control.move_cursor_home()

    @kb.add("end", eager=True)
    def _end(event) -> None:
        control.move_cursor_end()

    @kb.add("enter", eager=True)
    @kb.add(" ", eager=True)
    def _submit(event) -> None:
        event.app.exit(result=control.current_value)

    @kb.add("escape", eager=True)
    @kb.add("c-c", eager=True)
    def _cancel(event) -> None:
        event.app.exit(result=None)

    list_window = Window(
        control,
        wrap_lines=False,
        scroll_offsets=ScrollOffsets(top=2, bottom=2),
        right_margins=[ScrollbarMargin(display_arrows=True)],
    )
    layout = Layout(
        HSplit(
            [
                Label(title, style="class:aelvo-picker.title"),
                Label(subtitle, style="class:aelvo-picker.hint"),
                Frame(list_window),
                Label(footer, style="class:aelvo-picker.hint"),
            ]
        )
    )
    app = Application(
        layout=layout,
        key_bindings=kb,
        full_screen=True,  # alternate screen buffer: opens, then restores the REPL
        mouse_support=True,  # hover, click and wheel
        style=_PICKER_STYLE,
        input=_input,
        output=_output,
    )
    control.on_activate = lambda value: app.exit(result=value)
    try:
        return await app.run_async()
    except Exception as exc:  # pragma: no cover - defensive fallback
        log.debug("Sectioned picker failed: %s", exc)
        return None
