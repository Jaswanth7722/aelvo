"""ui/models/recovery_tracker.py — Recovery Event Model & Tracker

Phase 8: Recovery Visibility — expose recovery events as a first-class
UI concept so users see resilience: provider failures, fallback activations,
retry attempts, specialist reassignments, and recovery outcomes.

Every field is populated from actual runtime events.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RecoveryEntry:
    """A single recovery event record."""

    event_id: str = ""
    event_type: str = ""           # provider_failure | fallback_activated | retry_started |
                                   # recovery_successful | recovery_failed | specialist_reassigned
    specialist: str = ""           # The affected specialist or system
    summary: str = ""              # Short human-readable description
    detail: str = ""               # Additional context (strategy, error, fallback name)
    node_id: str = ""              # The affected graph node (if applicable)
    classification: str = ""       # Failure classification
    action: str = ""               # Recovery action taken
    retry_count: int = 0           # Retry attempt number
    duration: float = 0.0          # How long recovery took (seconds)
    success: bool = False          # Whether recovery succeeded
    timestamp: float = field(default_factory=time.time)

    @property
    def display_age(self) -> str:
        age = time.time() - self.timestamp
        if age < 5:
            return "just now"
        if age < 60:
            return f"{int(age)}s ago"
        if age < 3600:
            return f"{int(age / 60)}m ago"
        return f"{int(age / 3600)}h ago"

    @property
    def event_icon(self) -> str:
        icons = {
            "provider_failure": "⛔",
            "fallback_activated": "🔀",
            "retry_started": "🔄",
            "recovery_successful": "✅",
            "recovery_failed": "❌",
            "specialist_reassigned": "🔄",
        }
        return icons.get(self.event_type, "⚡")

    @property
    def event_color(self) -> str:
        colors = {
            "provider_failure": "#ff5c7a",
            "fallback_activated": "#f7b731",
            "retry_started": "#3b82f6",
            "recovery_successful": "#00e38c",
            "recovery_failed": "#ff5c7a",
            "specialist_reassigned": "#a565ff",
        }
        return colors.get(self.event_type, "#f7b731")


class RecoveryTracker:
    """Tracks recovery events in real time from EventBus data.

    Usage:
        tracker = RecoveryTracker()
        tracker.on_recovery_event("provider_failure", "OPENAI", "Connection timeout")
        tracker.on_recovery_event("recovery_successful", "OPENAI", "Fallback to NVIDIA")
        events = tracker.get_recent()  # List[RecoveryEntry]
        snapshot = tracker.snapshot()   # Dict for the widget
    """

    def __init__(self, max_entries: int = 50):
        self._entries: List[RecoveryEntry] = []
        self._max_entries = max_entries
        self._event_counter = 0
        self._recovery_counts: Dict[str, int] = {}  # event_type -> count
        self._failed = 0
        self._succeeded = 0

    def on_recovery_event(
        self,
        event_type: str,
        specialist: str = "",
        summary: str = "",
        detail: str = "",
        node_id: str = "",
        classification: str = "",
        action: str = "",
        retry_count: int = 0,
        duration: float = 0.0,
        success: bool = False,
    ) -> RecoveryEntry:
        """Record a recovery event."""
        self._event_counter += 1
        event_id = f"rec_{self._event_counter}"

        entry = RecoveryEntry(
            event_id=event_id,
            event_type=event_type,
            specialist=specialist,
            summary=summary,
            detail=detail,
            node_id=node_id,
            classification=classification,
            action=action,
            retry_count=retry_count,
            duration=duration,
            success=success,
            timestamp=time.time(),
        )

        self._entries.append(entry)
        if len(self._entries) > self._max_entries:
            self._entries = self._entries[-self._max_entries:]

        # Update counts
        self._recovery_counts[event_type] = self._recovery_counts.get(event_type, 0) + 1
        if success:
            self._succeeded += 1
        elif event_type in ("recovery_failed", "provider_failure"):
            self._failed += 1

        return entry

    def get_recent(self, limit: int = 20) -> List[RecoveryEntry]:
        """Get the most recent recovery events."""
        return self._entries[-limit:]

    def get_by_type(self, event_type: str) -> List[RecoveryEntry]:
        """Get recovery events filtered by type."""
        return [e for e in self._entries if e.event_type == event_type]

    @property
    def total_count(self) -> int:
        return len(self._entries)

    @property
    def success_count(self) -> int:
        return self._succeeded

    @property
    def failure_count(self) -> int:
        return self._failed

    @property
    def success_rate(self) -> float:
        total = self._succeeded + self._failed
        return round(self._succeeded / total, 4) if total > 0 else 0.0

    def snapshot(self) -> Dict[str, Any]:
        """Produce a snapshot dict for the recovery widget."""
        recent = self.get_recent(15)
        return {
            "recent": [
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "icon": e.event_icon,
                    "color": e.event_color,
                    "specialist": e.specialist,
                    "summary": e.summary,
                    "detail": e.detail,
                    "node_id": e.node_id,
                    "classification": e.classification,
                    "action": e.action,
                    "retry_count": e.retry_count,
                    "duration": round(e.duration, 1),
                    "success": e.success,
                    "display_age": e.display_age,
                    "timestamp": e.timestamp,
                }
                for e in recent
            ],
            "summary": {
                "total": self.total_count,
                "succeeded": self._succeeded,
                "failed": self._failed,
                "success_rate": self.success_rate,
                "by_type": dict(self._recovery_counts),
            },
        }
