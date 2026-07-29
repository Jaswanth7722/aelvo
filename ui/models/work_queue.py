"""
work_queue.py — Live Work Queue model

Tracks tasks from runtime events and maintains a real-time
queue with full detail: priority, owner, status, dependencies,
confidence, and lifecycle stage.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Status display helpers ──────────────────────────────────────

STATUS_LABELS: Dict[str, str] = {
    "pending": "pending",
    "assigned": "assigned",
    "running": "in progress",
    "processing": "in progress",
    "in_progress": "in progress",
    "reviewing": "reviewing",
    "review": "reviewing",
    "completed": "completed",
    "failed": "failed",
    "blocked": "blocked",
    "cancelled": "cancelled",
}

STATUS_ORDER: Dict[str, int] = {
    "running": 0,
    "processing": 0,
    "in_progress": 0,
    "reviewing": 1,
    "review": 1,
    "blocked": 2,
    "assigned": 3,
    "pending": 4,
    "failed": 5,
    "cancelled": 6,
    "completed": 7,
}

LIFECYCLE_PCT: Dict[str, float] = {
    "pending": 0.0,
    "assigned": 15.0,
    "running": 40.0,
    "processing": 40.0,
    "in_progress": 40.0,
    "reviewing": 70.0,
    "review": 70.0,
    "completed": 100.0,
    "failed": 0.0,
    "blocked": 40.0,
    "cancelled": 0.0,
}

STATUS_COLORS: Dict[str, str] = {
    "pending": "#64748b",
    "assigned": "#60a5fa",
    "running": "#22c55e",
    "processing": "#22c55e",
    "in_progress": "#22c55e",
    "reviewing": "#f59e0b",
    "review": "#f59e0b",
    "completed": "#64748b",
    "failed": "#ef4444",
    "blocked": "#f97316",
    "cancelled": "#64748b",
}

PRIORITY_ORDER: Dict[str, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
    "lowest": 4,
}

PRIORITY_LABELS: Dict[str, str] = {
    "critical": "CRIT",
    "high": "HIGH",
    "medium": "MED",
    "low": "LOW",
    "lowest": "LOWEST",
}

PRIORITY_COLORS: Dict[str, str] = {
    "critical": "#ef4444",
    "high": "#f97316",
    "medium": "#64748b",
    "low": "#64748b",
    "lowest": "#64748b",
}


@dataclass
class WorkQueueEntry:
    """A task entry in the live work queue with full detail."""

    task_id: str
    title: str
    task_type: str = "general"
    status: str = "pending"
    priority: str = "medium"
    owner: str = ""
    assigned_by: str = ""
    confidence: float = 0.0
    progress: float = 0.0
    depends_on: List[str] = field(default_factory=list)
    blocked_by: List[str] = field(default_factory=list)
    error: str = ""
    age_seconds: float = 0.0
    created_at: float = 0.0
    updated_at: float = 0.0
    tags: List[str] = field(default_factory=list)

    @property
    def status_label(self) -> str:
        return STATUS_LABELS.get(self.status, self.status)

    @property
    def priority_label(self) -> str:
        return PRIORITY_LABELS.get(self.priority, "MED")

    @property
    def priority_color(self) -> str:
        return PRIORITY_COLORS.get(self.priority, "#64748b")

    @property
    def status_color(self) -> str:
        return STATUS_COLORS.get(self.status, "#64748b")

    @property
    def lifecycle_progress(self) -> float:
        return LIFECYCLE_PCT.get(self.status, 0.0)

    @property
    def display_age(self) -> str:
        seconds = self.age_seconds
        if seconds < 60:
            return f"{int(seconds)}s"
        minutes = seconds / 60
        if minutes < 60:
            return f"{int(minutes)}m"
        hours = minutes / 60
        return f"{int(hours)}h"


class WorkQueueTracker:
    """Tracks the live work queue from runtime events.

    Maintains a dict of task_id -> WorkQueueEntry and provides
    snapshots sorted by priority and status for the TUI.
    """

    def __init__(self) -> None:
        self._entries: Dict[str, WorkQueueEntry] = {}
        self._max_entries = 200

    # ── Event Handlers ───────────────────────────────────────────

    def on_task_event(
        self,
        task_id: str,
        task_name: str,
        status: str,
        progress: float = 0.0,
        specialist: str = "",
        priority: str = "medium",
        task_type: str = "general",
        depends_on: Optional[List[str]] = None,
        error: str = "",
    ) -> None:
        """Update the work queue from a task event."""
        now = time.time()
        existing = self._entries.get(task_id)

        if existing:
            existing.title = task_name or existing.title
            existing.status = status
            existing.progress = progress
            if specialist:
                existing.owner = specialist
            if priority:
                existing.priority = priority
            if task_type:
                existing.task_type = task_type
            if depends_on is not None:
                existing.depends_on = depends_on
            if error:
                existing.error = error
            existing.updated_at = now
            existing.age_seconds = now - existing.created_at
        else:
            self._entries[task_id] = WorkQueueEntry(
                task_id=task_id,
                title=task_name,
                status=status,
                priority=priority,
                owner=specialist,
                task_type=task_type,
                progress=progress,
                depends_on=depends_on or [],
                error=error,
                created_at=now,
                updated_at=now,
                age_seconds=0.0,
            )

        # Trim to max entries
        if len(self._entries) > self._max_entries:
            self._prune()

    def on_specialist_event(
        self,
        specialist: str,
        task_id: str = "",
        confidence: float = 0.0,
    ) -> None:
        """Update confidence for a specialist's task."""
        if not task_id:
            return
        entry = self._entries.get(task_id)
        if entry and entry.owner.upper() == specialist.upper():
            entry.confidence = max(entry.confidence, confidence)
            entry.updated_at = time.time()

    def remove_task(self, task_id: str) -> None:
        """Remove a task from the queue."""
        self._entries.pop(task_id, None)

    def clear(self) -> None:
        """Clear all entries."""
        self._entries.clear()

    def _prune(self) -> None:
        """Remove oldest completed/failed/cancelled tasks."""
        terminal = {
            tid: e for tid, e in self._entries.items()
            if e.status in ("completed", "failed", "cancelled")
        }
        non_terminal = {
            tid: e for tid, e in self._entries.items()
            if tid not in terminal
        }

        # Keep last 50 active entries plus most recent 50 terminal
        sorted_terminal = sorted(
            terminal.items(), key=lambda x: x[1].updated_at, reverse=True
        )[:50]
        self._entries = {**non_terminal, **dict(sorted_terminal)}

    # ── Accessors ────────────────────────────────────────────────

    def get_entry(self, task_id: str) -> Optional[WorkQueueEntry]:
        return self._entries.get(task_id)

    def get_active(self) -> List[WorkQueueEntry]:
        """Get all non-terminal entries sorted by priority then status order."""
        non_terminal = [
            e for e in self._entries.values()
            if e.status not in ("completed", "failed", "cancelled")
        ]
        return sorted(
            non_terminal,
            key=lambda e: (
                PRIORITY_ORDER.get(e.priority, 99),
                STATUS_ORDER.get(e.status, 99),
                e.created_at,
            ),
        )

    def get_completed(self, limit: int = 20) -> List[WorkQueueEntry]:
        """Get most recently completed/failed tasks."""
        terminal = [
            e for e in self._entries.values()
            if e.status in ("completed", "failed", "cancelled")
        ]
        terminal.sort(key=lambda e: e.updated_at, reverse=True)
        return terminal[:limit]

    def get_all(self) -> List[WorkQueueEntry]:
        """Get all entries sorted by priority then status."""
        return sorted(
            self._entries.values(),
            key=lambda e: (
                PRIORITY_ORDER.get(e.priority, 99),
                STATUS_ORDER.get(e.status, 99),
                e.created_at,
            ),
        )

    def snapshot(self) -> Dict[str, Any]:
        """Get a complete snapshot for TUI display."""
        active = self.get_active()
        completed = self.get_completed()
        return {
            "active": [self._entry_to_dict(e) for e in active],
            "completed": [self._entry_to_dict(e) for e in completed],
            "total": len(self._entries),
            "active_count": len(active),
            "completed_count": len(completed),
        }

    @staticmethod
    def _entry_to_dict(entry: WorkQueueEntry) -> Dict[str, Any]:
        return {
            "task_id": entry.task_id,
            "title": entry.title,
            "status": entry.status,
            "status_label": entry.status_label,
            "status_color": entry.status_color,
            "priority": entry.priority,
            "priority_label": entry.priority_label,
            "priority_color": entry.priority_color,
            "owner": entry.owner,
            "task_type": entry.task_type,
            "confidence": entry.confidence,
            "progress": entry.progress,
            "lifecycle_progress": entry.lifecycle_progress,
            "depends_on": entry.depends_on,
            "blocked_by": entry.blocked_by,
            "error": entry.error,
            "age": entry.display_age,
            "created_at": entry.created_at,
        }
