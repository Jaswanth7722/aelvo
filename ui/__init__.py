"""
AELVO OMEGA Terminal UI
=======================
Provides both a legacy styled CLI and a modern Textual-based TUI.
"""

from ui.style import (
    C_PRIMARY, C_ACCENT, C_SUCCESS, C_WARNING, C_DANGER, C_MUTED, C_WHITE, C_RESET,
    BOLD, DIM, SYM_OK, SYM_INFO, SYM_WARN, SYM_FAIL, SYM_BULLET,
    print_styled, draw_header, draw_separator
)

from ui.menu import (
    select_project_interactive,
    detect_provider,
    show_boot_logo
)

from ui.app import AelvoTUI
from ui.core.bridge import UIBridge
from ui.events import EventBus, Event, EventType, get_event_bus

__all__ = [
    "C_PRIMARY", "C_ACCENT", "C_SUCCESS", "C_WARNING", "C_DANGER", "C_MUTED", "C_WHITE", "C_RESET",
    "BOLD", "DIM", "SYM_OK", "SYM_INFO", "SYM_WARN", "SYM_FAIL", "SYM_BULLET",
    "print_styled", "draw_header", "draw_separator",
    "select_project_interactive", "detect_provider", "show_boot_logo",
    "AelvoTUI", "UIBridge", "EventBus", "Event", "EventType", "get_event_bus",
]
