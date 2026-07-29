"""
AELVO UI Events Module (Integrated)
====================================
Event system directly integrated into AELVO.
"""

from .event_bus import EventBus, Event, EventType, get_event_bus
from .event_factory import (
    create_task_event,
    create_specialist_event,
    create_tool_event,
    create_memory_event,
    create_verification_event,
    create_safety_event,
    create_system_event
)

__all__ = [
    "EventBus",
    "Event",
    "EventType",
    "get_event_bus",
    "create_task_event",
    "create_specialist_event",
    "create_tool_event",
    "create_memory_event",
    "create_verification_event",
    "create_safety_event",
    "create_system_event"
]