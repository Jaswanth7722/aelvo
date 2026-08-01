"""
AELVO Event Bus
===============
Async event bus for real-time UI updates and inter-component communication.
"""

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import logging

log = logging.getLogger(__name__)



class EventType(Enum):
    """Event types for the AELVO system."""
    
    # System events
    SYSTEM_STARTUP = "system_startup"
    SYSTEM_SHUTDOWN = "system_shutdown"
    
    # Task events
    TASK_CREATED = "task_created"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_BLOCKED = "task_blocked"
    TASK_CANCELLED = "task_cancelled"
    TASK_PROGRESS = "task_progress"
    
    # Specialist events
    SPECIALIST_ACTIVATED = "specialist_activated"
    SPECIALIST_DEACTIVATED = "specialist_deactivated"
    SPECIALIST_THINKING = "specialist_thinking"
    SPECIALIST_ACTION = "specialist_action"
    
    # Tool events
    TOOL_STARTED = "tool_started"
    TOOL_OUTPUT = "tool_output"
    TOOL_COMPLETED = "tool_completed"
    TOOL_FAILED = "tool_failed"
    
    # Memory events
    MEMORY_RETRIEVED = "memory_retrieved"
    MEMORY_STORED = "memory_stored"
    MEMORY_INJECTED = "memory_injected"
    
    # Verification events
    VERIFICATION_STARTED = "verification_started"
    VERIFICATION_PASSED = "verification_passed"
    VERIFICATION_FAILED = "verification_failed"
    VERIFICATION_RETRY = "verification_retry"
    
    # Safety events
    SAFETY_CHECK = "safety_check"
    DANGEROUS_ACTION_DETECTED = "dangerous_action_detected"
    APPROVAL_REQUIRED = "approval_required"
    APPROVAL_GRANTED = "approval_granted"
    APPROVAL_DENIED = "approval_denied"
    
    # UI events
    UI_REFRESH = "ui_refresh"
    PANEL_RESIZED = "panel_resized"
    USER_INPUT = "user_input"
    
    # Session events
    SESSION_STARTED = "session_started"
    SESSION_PAUSED = "session_paused"
    SESSION_RESUMED = "session_resumed"
    SESSION_ENDED = "session_ended"
    
    # Recovery events
    RECOVERY_INITIATED = "recovery_initiated"
    RECOVERY_COMPLETED = "recovery_completed"
    RECOVERY_FAILED = "recovery_failed"

    # Collaboration / Mode B events
    COLLABORATION_FINDING = "collaboration_finding"
    COLLABORATION_CONSUMED = "collaboration_consumed"
    COLLABORATION_CHALLENGE = "collaboration_challenge"
    COLLABORATION_CONSENSUS = "collaboration_consensus"
    COLLABORATION_DECISION = "collaboration_decision"
    COLLABORATION_EXECUTION_START = "collaboration_execution_start"
    COLLABORATION_EXECUTION_END = "collaboration_execution_end"
    COLLABORATION_REPORT = "collaboration_report"


@dataclass
class Event:
    """Base event class."""
    event_type: EventType
    timestamp: float = field(default_factory=time.time)
    data: Dict[str, Any] = field(default_factory=dict)
    source: str = "system"
    correlation_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "data": self.data,
            "source": self.source,
            "correlation_id": self.correlation_id
        }


class EventBus:
    """
    Async event bus for publishing and subscribing to events.
    Supports event filtering, prioritization, and async handlers.
    """
    
    def __init__(self, max_buffer_size: int = 1000):
        """
        Initialize the event bus.
        
        Args:
            max_buffer_size: Maximum number of events to buffer
        """
        self._subscribers: Dict[EventType, List[Callable]] = {}
        self._event_buffer: deque = deque(maxlen=max_buffer_size)
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=max_buffer_size)
        self._running: bool = False
        self._processor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("aelvo.ui.event_bus")
        
        # Statistics
        self._events_published: int = 0
        self._events_processed: int = 0
        self._events_dropped: int = 0
    
    def subscribe(self, event_type: EventType, handler: Callable[[Event], Any]) -> None:
        """
        Subscribe to an event type.
        
        Args:
            event_type: The event type to subscribe to
            handler: Async or sync callback function that receives the event
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)
        self._logger.debug(f"Subscribed to {event_type.value}")
    
    def unsubscribe(self, event_type: EventType, handler: Callable[[Event], Any]) -> None:
        """
        Unsubscribe from an event type.
        
        Args:
            event_type: The event type to unsubscribe from
            handler: The callback function to remove
        """
        if event_type in self._subscribers:
            try:
                self._subscribers[event_type].remove(handler)
                self._logger.debug(f"Unsubscribed from {event_type.value}")
            except ValueError as _ex:
                log.warning("Silenced exception: %s", _ex)
    
    async def publish(self, event: Event) -> None:
        """
        Publish an event to the bus.
        
        Args:
            event: The event to publish
        """
        self._events_published += 1
        
        async with self._lock:
            self._event_buffer.append(event)
        
        # Add to processing queue
        try:
            await asyncio.wait_for(
                self._event_queue.put(event),
                timeout=0.1
            )
        except asyncio.TimeoutError:
            self._events_dropped += 1
            self._logger.warning(f"Event queue full, dropped event: {event.event_type.value}")
    
    async def start(self) -> None:
        """Start the event processor."""
        if self._running:
            return
        
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        self._logger.info("Event bus started")
    
    async def stop(self) -> None:
        """Stop the event processor."""
        if not self._running:
            return
        
        self._running = False
        
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError as _ex:
                log.warning("Silenced exception: %s", _ex)
        
        self._logger.info("Event bus stopped")
    
    async def _process_events(self) -> None:
        """Process events from the queue and notify subscribers."""
        while self._running:
            try:
                # Wait for event with timeout to allow checking _running flag
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=0.5
                )
                
                await self._notify_subscribers(event)
                self._events_processed += 1
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._logger.error(f"Error processing event: {e}")
    
    async def _notify_subscribers(self, event: Event) -> None:
        """
        Notify all subscribers of an event.
        
        Args:
            event: The event to broadcast
        """
        subscribers = self._subscribers.get(event.event_type, [])
        
        for handler in subscribers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                self._logger.error(f"Error in event handler: {e}")
    
    def get_recent_events(self, count: int = 100) -> List[Event]:
        """
        Get recent events from the buffer.
        
        Args:
            count: Maximum number of events to return
            
        Returns:
            List of recent events
        """
        return list(self._event_buffer)[-count:]
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Get event bus statistics.
        
        Returns:
            Dictionary with statistics
        """
        return {
            "events_published": self._events_published,
            "events_processed": self._events_processed,
            "events_dropped": self._events_dropped,
            "queue_size": self._event_queue.qsize(),
            "buffer_size": len(self._event_buffer),
            "subscriber_count": sum(len(handlers) for handlers in self._subscribers.values())
        }
    
    async def clear(self) -> None:
        """Clear the event buffer and queue."""
        async with self._lock:
            self._event_buffer.clear()
            
            # Clear queue
            while not self._event_queue.empty():
                try:
                    self._event_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            
            self._logger.info("Event bus cleared")


# Global event bus instance
_global_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """
    Get the global event bus instance.
    
    Returns:
        The global event bus
    """
    global _global_event_bus
    if _global_event_bus is None:
        _global_event_bus = EventBus()
    return _global_event_bus