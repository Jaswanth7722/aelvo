"""Replayable provider event system — event sourcing for provider interactions."""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ProviderEventType(Enum):
    """Types of provider events that can be replayed."""
    REQUEST_SENT = auto()
    RESPONSE_RECEIVED = auto()
    STREAM_CHUNK = auto()
    STREAM_COMPLETE = auto()
    TOOL_CALL = auto()
    TOOL_RESULT = auto()
    ERROR = auto()
    RETRY = auto()
    FALLBACK = auto()
    AUTH_REFRESH = auto()
    RATE_LIMITED = auto()
    TIMEOUT = auto()
    CANCELLED = auto()
    HEALTH_CHECK = auto()
    PROVIDER_REGISTERED = auto()
    PROVIDER_UNREGISTERED = auto()


@dataclass
class ProviderEvent:
    """A single replayable provider event."""

    event_id: str
    event_type: ProviderEventType
    provider_id: str
    model_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    sequence: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.name,
            "provider_id": self.provider_id,
            "model_id": self.model_id,
            "data": self.data,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "sequence": self.sequence,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProviderEvent:
        return cls(
            event_id=data["event_id"],
            event_type=ProviderEventType[data["event_type"]],
            provider_id=data["provider_id"],
            model_id=data.get("model_id", ""),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
            timestamp=data.get("timestamp", time.time()),
            sequence=data.get("sequence", 0),
        )


class EventStore:
    """Stores and replays provider events with sequencing."""

    def __init__(self) -> None:
        self._events: list[ProviderEvent] = []
        self._max_events: int = 10000
        self._sequence: int = 0
        self._listeners: list[Callable[[ProviderEvent], None]] = []

    def add_listener(
        self, listener: Callable[[ProviderEvent], None]
    ) -> None:
        self._listeners.append(listener)

    def record(
        self,
        event_type: ProviderEventType,
        provider_id: str,
        model_id: str = "",
        data: Optional[dict[str, Any]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> ProviderEvent:
        """Record a new event."""
        self._sequence += 1
        event = ProviderEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            provider_id=provider_id,
            model_id=model_id,
            data=data or {},
            metadata=metadata or {},
            sequence=self._sequence,
        )

        self._events.append(event)
        if len(self._events) > self._max_events:
            self._events = self._events[-self._max_events:]

        # Notify listeners
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error("Event listener error: %s", e)

        return event

    def get_events(
        self,
        provider_id: Optional[str] = None,
        event_type: Optional[ProviderEventType] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> list[ProviderEvent]:
        """Get events with optional filtering."""
        events = self._events

        if provider_id:
            events = [
                e for e in events if e.provider_id == provider_id
            ]
        if event_type:
            events = [
                e for e in events if e.event_type == event_type
            ]
        if since:
            events = [
                e for e in events if e.timestamp >= since
            ]

        return events[-limit:]

    def replay(
        self,
        events: list[ProviderEvent],
        handler: Callable[[ProviderEvent], None],
    ) -> None:
        """Replay a list of events through a handler."""
        for event in sorted(events, key=lambda e: e.sequence):
            handler(event)

    def export_json(self, path: str) -> None:
        """Export events to a JSON file."""
        with open(path, "w") as f:
            json.dump(
                [e.to_dict() for e in self._events],
                f,
                indent=2,
            )

    def import_json(self, path: str) -> int:
        """Import events from a JSON file. Returns count."""
        with open(path) as f:
            events_data = json.load(f)

        count = 0
        for data in events_data:
            event = ProviderEvent.from_dict(data)
            self._events.append(event)
            count += 1

        return count

    def clear(self) -> None:
        self._events.clear()
        self._sequence = 0

    @property
    def count(self) -> int:
        return len(self._events)

    def summary(
        self,
        provider_id: Optional[str] = None,
    ) -> dict[str, Any]:
        events = (
            self._events
            if provider_id is None
            else [
                e
                for e in self._events
                if e.provider_id == provider_id
            ]
        )

        type_counts: dict[str, int] = {}
        for e in events:
            type_counts[e.event_type.name] = (
                type_counts.get(e.event_type.name, 0) + 1
            )

        return {
            "total_events": len(events),
            "by_type": type_counts,
            "time_span": (
                {
                    "earliest": events[0].timestamp,
                    "latest": events[-1].timestamp,
                }
                if events
                else None
            ),
        }
