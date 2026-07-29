"""
task.py — Task Model for the Shared Task Board

Defines the Task Pydantic model that tracks work items through
their lifecycle in Mode B (Collaborative) execution.
"""

from __future__ import annotations

import hashlib
import time
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """Lifecycle states for a task on the Shared Task Board."""

    PENDING = "pending"            # Created, not yet assigned
    ASSIGNED = "assigned"           # Assigned to a specialist
    IN_PROGRESS = "in_progress"     # Specialist is actively working
    REVIEWING = "reviewing"         # Output submitted for review
    COMPLETED = "completed"         # Task finished successfully
    FAILED = "failed"               # Task failed unrecoverably
    BLOCKED = "blocked"             # Task blocked by a dependency
    CANCELLED = "cancelled"         # Task cancelled by Architect


class TaskPriority(str, Enum):
    """Priority levels for task scheduling."""

    LOWEST = "lowest"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TaskType(str, Enum):
    """Types of work items the Task Board can track."""

    RESEARCH = "research"               # ORACLE: investigate, gather facts
    IMPLEMENT = "implement"             # FORGE: write code, refactor
    SECURITY_REVIEW = "security_review" # SENTINEL: review for risk
    EXECUTE = "execute"                 # TERMINUS: run commands
    CONSENSUS = "consensus"            # Consensus: vote on positions
    REPORT = "report"                   # HERALD: summarize, communicate
    GENERAL = "general"                 # Fallback for untyped tasks


class Task(BaseModel):
    """A work item tracked by the Shared Task Board.

    Each Task represents a unit of work assigned to a specialist.
    The task lifecycle is managed by TaskStateMachine, which enforces
    valid transitions and publishes events on every change.
    """

    id: str = Field(
        description="Unique task identifier (hash-derived)",
    )
    type: TaskType = TaskType.GENERAL
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.MEDIUM

    # ── Assignment ────────────────────────────────────────────────
    specialist: str = Field(
        default="",
        description="Specialist assigned to this task (e.g., ORACLE, FORGE)",
    )
    assigned_by: str = Field(
        default="",
        description="ARCHITECT ID or 'system' that assigned this task",
    )

    # ── Content ───────────────────────────────────────────────────
    title: str = Field(
        default="",
        description="Short human-readable title",
    )
    description: str = Field(
        default="",
        description="Full task description / instructions",
    )
    context: Dict[str, Any] = Field(
        default_factory=dict,
        description="Task-type-specific context payload",
    )
    result: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Task result payload (set on COMPLETED)",
    )

    # ── Dependencies ──────────────────────────────────────────────
    depends_on: List[str] = Field(
        default_factory=list,
        description="IDs of tasks this task depends on",
    )
    blocked_by: List[str] = Field(
        default_factory=list,
        description="IDs of tasks currently blocking this task",
    )

    # ── Error / Failure ───────────────────────────────────────────
    error: Optional[str] = Field(
        default=None,
        description="Error message if task FAILED",
    )
    failure_reason: Optional[str] = Field(
        default=None,
        description="Human-readable failure reason",
    )

    # ── Metadata ──────────────────────────────────────────────────
    session_id: str = Field(
        default="",
        description="Session that created this task",
    )
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    history: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Chronological state transition log",
    )

    # ── Timing ────────────────────────────────────────────────────
    created_at: float = Field(
        default_factory=time.time,
        description="Unix timestamp of task creation",
    )
    updated_at: float = Field(
        default_factory=time.time,
        description="Unix timestamp of last modification",
    )
    assigned_at: Optional[float] = Field(
        default=None,
        description="Unix timestamp when task was assigned",
    )
    completed_at: Optional[float] = Field(
        default=None,
        description="Unix timestamp when task was completed/failed",
    )

    # ── Public Helpers ────────────────────────────────────────────

    def record_transition(
        self, from_status: TaskStatus, to_status: TaskStatus,
        reason: str = "",
    ) -> None:
        """Record a state transition in the task history."""
        self.history.append({
            "from": from_status.value,
            "to": to_status.value,
            "reason": reason,
            "timestamp": time.time(),
        })
        self.updated_at = time.time()

    @property
    def age_seconds(self) -> float:
        """Seconds since task creation."""
        return time.time() - self.created_at

    @property
    def duration_seconds(self) -> Optional[float]:
        """Seconds between assignment and completion, if both exist."""
        if self.assigned_at and self.completed_at:
            return self.completed_at - self.assigned_at
        return None

    def summary(self) -> Dict[str, Any]:
        """Compact summary for logging / UI."""
        return {
            "id": self.id[:12],
            "type": self.type.value,
            "status": self.status.value,
            "specialist": self.specialist,
            "title": self.title[:60],
            "priority": self.priority.value,
            "age_s": round(self.age_seconds, 1),
            "depends_on": len(self.depends_on),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        status_icon = {
            TaskStatus.PENDING: "○",
            TaskStatus.ASSIGNED: "◎",
            TaskStatus.IN_PROGRESS: "◉",
            TaskStatus.REVIEWING: "◐",
            TaskStatus.COMPLETED: "✓",
            TaskStatus.FAILED: "✗",
            TaskStatus.BLOCKED: "⊘",
            TaskStatus.CANCELLED: "−",
        }.get(self.status, "?")

        lines = [
            f"  {status_icon} [{self.id[:10]}] {self.type.value.upper():16s}"
            f"  {self.specialist or '(unassigned)':12s}"
            f"  {self.title[:50]}",
        ]
        if self.depends_on:
            lines.append(f"       Depends: {', '.join(d[:10] for d in self.depends_on)}")
        if self.error:
            lines.append(f"       Error: {self.error[:80]}")
        return "\n".join(lines)

    # ── Factory ───────────────────────────────────────────────────

    @classmethod
    def create(
        cls,
        task_type: TaskType = TaskType.GENERAL,
        specialist: str = "",
        title: str = "",
        description: str = "",
        priority: TaskPriority = TaskPriority.MEDIUM,
        context: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
        assigned_by: str = "",
        session_id: str = "",
        tags: Optional[List[str]] = None,
    ) -> Task:
        """Create a new Task with auto-generated ID."""
        raw_id = f"{task_type.value}_{specialist}_{title}_{time.time()}"
        task_id = hashlib.sha256(raw_id.encode()).hexdigest()[:16]

        return cls(
            id=task_id,
            type=task_type,
            status=TaskStatus.PENDING,
            priority=priority,
            specialist=specialist,
            assigned_by=assigned_by,
            title=title,
            description=description,
            context=context or {},
            depends_on=depends_on or [],
            session_id=session_id,
            tags=tags or [],
            history=[{
                "from": "none",
                "to": TaskStatus.PENDING.value,
                "reason": "Task created",
                "timestamp": time.time(),
            }],
        )
