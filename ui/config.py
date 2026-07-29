"""
AELVO UI Configuration
========================
Defines visual style, panel layout, and behavioral settings for the terminal interface.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


class ColorScheme(Enum):
    """Predefined color schemes for different visual themes."""
    PASTEL = "pastel"
    MUTED = "muted"
    SOFT = "soft"
    HIGH_CONTRAST = "high_contrast"


@dataclass
class PanelConfig:
    """Configuration for individual panels."""
    name: str
    title: str
    enabled: bool = True
    collapsible: bool = True
    min_height: int = 3
    default_height: int = 10
    max_height: Optional[int] = None
    position: str = "right"  # left, right, top, bottom
    priority: int = 50


@dataclass
class UIConfig:
    """Main UI configuration class."""
    
    # Color scheme
    color_scheme: ColorScheme = ColorScheme.PASTEL
    
    # Panel layout configuration
    panels: Dict[str, PanelConfig] = field(default_factory=lambda: {
        "cognitive": PanelConfig(
            name="cognitive",
            title="COGNITIVE",
            enabled=True,
            default_height=12,
            position="top",
            priority=100
        ),
        "specialist_stream": PanelConfig(
            name="specialist_stream",
            title="SPECIALIST ACTIVITY",
            enabled=True,
            default_height=8,
            position="right",
            priority=90
        ),
        "execution_graph": PanelConfig(
            name="execution_graph",
            title="EXECUTION GRAPH",
            enabled=True,
            default_height=10,
            position="left",
            priority=80
        ),
        "tool_execution": PanelConfig(
            name="tool_execution",
            title="TOOL EXECUTION",
            enabled=True,
            default_height=8,
            position="bottom",
            priority=70
        ),
        "memory_awareness": PanelConfig(
            name="memory_awareness",
            title="MEMORY AWARENESS",
            enabled=True,
            default_height=6,
            position="right",
            priority=60
        ),
        "verification": PanelConfig(
            name="verification",
            title="VERIFICATION",
            enabled=True,
            default_height=5,
            position="right",
            priority=50
        ),
        "safety_governance": PanelConfig(
            name="safety_governance",
            title="SAFETY & GOVERNANCE",
            enabled=True,
            default_height=6,
            position="bottom",
            priority=95
        ),
        "timeline": PanelConfig(
            name="timeline",
            title="TIMELINE",
            enabled=True,
            default_height=8,
            position="bottom",
            priority=40
        )
    })
    
    # Status bar configuration
    status_bar_enabled: bool = True
    status_bar_height: int = 1
    
    # Intervention controls configuration
    intervention_enabled: bool = True
    intervention_position: str = "bottom"
    
    # Update rates
    ui_refresh_rate: float = 0.1  # seconds
    event_processing_rate: float = 0.05  # seconds
    
    # Display settings
    max_log_lines: int = 1000
    max_timeline_entries: int = 100
    compact_mode: bool = False
    show_timestamps: bool = True
    
    # Safety settings
    dangerous_command_threshold: List[str] = field(default_factory=lambda: [
        "rm -rf", "del", "format", "shutdown", "reboot", "DROP", "DELETE"
    ])
    require_confirmation: bool = True
    simulation_mode_available: bool = True
    
    # Performance settings
    async_event_buffer_size: int = 1000
    max_concurrent_updates: int = 10


# Color definitions for different schemes
COLOR_SCHEMES = {
    ColorScheme.PASTEL: {
        "primary": "deep_sky_blue1",
        "secondary": "sky_blue1",
        "accent": "plum2",
        "success": "dark_sea_green1",
        "warning": "wheat1",
        "error": "light_salmon1",
        "info": "azure1",
        "muted": "gray70",
        "border": "gray60",
        "active": "dark_sky_blue",
        "pending": "gray70",
        "blocked": "light_salmon1",
        "completed": "dark_sea_green1"
    },
    ColorScheme.MUTED: {
        "primary": "steel_blue",
        "secondary": "light_slate_blue",
        "accent": "medium_purple",
        "success": "olive_drab",
        "warning": "tan",
        "error": "indian_red",
        "info": "light_steel_blue",
        "muted": "slate_gray",
        "border": "slate_gray",
        "active": "steel_blue",
        "pending": "slate_gray",
        "blocked": "indian_red",
        "completed": "olive_drab"
    },
    ColorScheme.SOFT: {
        "primary": "cornflower_blue",
        "secondary": "light_sky_blue",
        "accent": "thistle1",
        "success": "pale_green1",
        "warning": "moccasin",
        "error": "pink1",
        "info": "light_cyan",
        "muted": "gray80",
        "border": "gray70",
        "active": "cornflower_blue",
        "pending": "gray80",
        "blocked": "pink1",
        "completed": "pale_green1"
    },
    ColorScheme.HIGH_CONTRAST: {
        "primary": "bold white",
        "secondary": "bold gray",
        "accent": "bold cyan",
        "success": "bold green",
        "warning": "bold yellow",
        "error": "bold red",
        "info": "bold white",
        "muted": "white",
        "border": "bold white",
        "active": "bold green",
        "pending": "bold yellow",
        "blocked": "bold red",
        "completed": "bold green"
    }
}


def get_colors(scheme: ColorScheme = ColorScheme.PASTEL) -> Dict[str, str]:
    """Get color definitions for a specific scheme."""
    return COLOR_SCHEMES.get(scheme, COLOR_SCHEMES[ColorScheme.PASTEL])


# Specialist color mappings
SPECIALIST_COLORS = {
    "HERMES": "cornflower_blue",
    "ORACLE": "medium_purple",
    "SENTINEL": "light_salmon1",
    "ARCHITECT": "plum2",
    "FORGE": "dark_sea_green1",
    "TERMINUS": "wheat1",
    "HERALD": "azure1"
}


# Task state colors
TASK_STATE_COLORS = {
    "pending": "gray70",
    "running": "cornflower_blue",
    "blocked": "light_salmon1",
    "failed": "light_salmon1",
    "completed": "dark_sea_green1",
    "cancelled": "gray70"
}


# Verification status colors
VERIFICATION_COLORS = {
    "pass": "dark_sea_green1",
    "fail": "light_salmon1",
    "retry": "wheat1",
    "blocked": "light_salmon1",
    "skipped": "gray70",
    "running": "cornflower_blue"
}