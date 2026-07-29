# shared_task_board/models.py — Data Models for the Collaborative Task Board
#
# The task board is the central coordination primitive for the collaborative architecture.
# Every specialist interaction flows through it.

from __future__ import annotations

import uuid
import hashlib
from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from pydantic import BaseModel, Field


# ============================================================================
# Specialists
# ============================================================================

class SpecialistName(str, Enum):
    """The seven AELVO Omega specialists."""
    HERMES = "HERMES"
    ARCHITECT = "ARCHITECT"
    ORACLE = "ORACLE"
    FORGE = "FORGE"
    SENTINEL = "SENTINEL"
    TERMINUS = "TERMINUS"
    HERALD = "HERALD"


# ============================================================================
# Task Types
# ============================================================================

class TaskType(str, Enum):
    """Types of tasks that can appear on the board. Enum — not string — to prevent typo bugs."""
    RESEARCH = "RESEARCH"
    IMPLEMENTATION = "IMPLEMENTATION"
    SECURITY_REVIEW = "SECURITY_REVIEW"
    EXECUTION = "EXECUTION"
    SYNTHESIS = "SYNTHESIS"
    REVIEW = "REVIEW"
    CONSENSUS = "CONSENSUS"


# ============================================================================
# Task Status — State Machine
# ============================================================================

class TaskStatus(str, Enum):
    """Valid task states. Only explicit transitions are allowed."""
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    UNDER_REVIEW = "UNDER_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    COMPLETED = "COMPLETED"


# ============================================================================
# Valid State Transitions
# ============================================================================

_VALID_TRANSITIONS: Dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.ASSIGNED},
    TaskStatus.ASSIGNED: {TaskStatus.ACTIVE, TaskStatus.PENDING},
    TaskStatus.ACTIVE: {TaskStatus.UNDER_REVIEW, TaskStatus.BLOCKED, TaskStatus.ASSIGNED},
    TaskStatus.BLOCKED: {TaskStatus.ACTIVE, TaskStatus.PENDING},
    TaskStatus.UNDER_REVIEW: {TaskStatus.APPROVED, TaskStatus.REJECTED, TaskStatus.ACTIVE},
    TaskStatus.APPROVED: {TaskStatus.COMPLETED, TaskStatus.UNDER_REVIEW},
    TaskStatus.REJECTED: {TaskStatus.PENDING},
    TaskStatus.COMPLETED: set(),
}


# ============================================================================
# Typed Exceptions
# ============================================================================

class InvalidTaskTransition(Exception):
    """Raised when an invalid state transition is attempted."""
    def __init__(self, task_id: str, from_status: TaskStatus, to_status: TaskStatus, message: str = ""):
        self.task_id = task_id
        self.from_status = from_status
        self.to_status = to_status
        detail = message or f"Cannot transition task {task_id} from {from_status.value} to {to_status.value}"
        super().__init__(detail)


class TaskNotFoundError(Exception):
    """Raised when a task ID doesn't exist on the board."""


class DependencyNotSatisfiedError(Exception):
    """Raised when a task has unsatisfied dependencies."""


# ============================================================================
# Priority
# ============================================================================

class Priority(int, Enum):
    """Task priority where 10 is highest."""
    LOWEST = 0
    LOW = 2
    MEDIUM = 5
    HIGH = 8
    CRITICAL = 10


# ============================================================================
# Event Entry
# ============================================================================

class TaskEventEntry(BaseModel):
    """An immutable event recorded for every task state transition."""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    actor: str
    from_status: Optional[TaskStatus] = None
    to_status: Optional[TaskStatus] = None
    detail: str = ""


# ============================================================================
# Review Request
# ============================================================================

class ReviewRequest(BaseModel):
    """A request for review from one specialist to another."""
    review_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    requesting_specialist: SpecialistName
    reviewing_specialist: SpecialistName
    question: str
    response: Optional[str] = None
    approved: Optional[bool] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    responded_at: Optional[datetime] = None


# ============================================================================
# Consensus Request
# ============================================================================

class ConsensusRequest(BaseModel):
    """A request for consensus on a task's results."""
    consensus_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    topic: str
    options: List[str] = Field(default_factory=list)
    criteria: List[str] = Field(default_factory=list)
    participants: List[SpecialistName] = Field(default_factory=list)
    deadline: Optional[datetime] = None


class ConsensusOutcome(str, Enum):
    """The outcome of a consensus process."""
    APPROVED = "APPROVED"
    APPROVED_WITH_RISK = "APPROVED_WITH_RISK"
    REQUIRES_REVISION = "REQUIRES_REVISION"
    REJECTED = "REJECTED"
    ESCALATED = "ESCALATED"


# ============================================================================
# Task Spec — for creating new tasks
# ============================================================================

class TaskSpec(BaseModel):
    """Specification for creating a new task."""
    title: str
    task_type: TaskType
    priority: int = Priority.MEDIUM
    created_by: str = "system"
    context: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# Task — the core data model
# ============================================================================

class Task(BaseModel):
    """A task on the shared task board."""
    task_id: str
    title: str
    task_type: TaskType
    status: TaskStatus = TaskStatus.PENDING
    owner: Optional[SpecialistName] = None
    assignees: List[SpecialistName] = Field(default_factory=list)
    dependencies: List[str] = Field(default_factory=list)
    created_by: str = "system"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    priority: int = Priority.MEDIUM
    context: Dict[str, Any] = Field(default_factory=dict)
    results: Dict[str, Any] = Field(default_factory=dict)
    review_requests: List[ReviewRequest] = Field(default_factory=list)
    consensus_request: Optional[ConsensusRequest] = None
    consensus_outcome: Optional[str] = None
    events: List[TaskEventEntry] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_spec(cls, spec: TaskSpec) -> Task:
        """Create a Task from a TaskSpec with deterministic task_id."""
        raw = f"{spec.task_type.value}_{spec.title}_{spec.created_by}_{datetime.now(timezone.utc).isoformat()}"
        task_id = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return cls(
            task_id=task_id,
            title=spec.title,
            task_type=spec.task_type,
            priority=spec.priority,
            created_by=spec.created_by,
            context=spec.context,
            dependencies=spec.dependencies,
            metadata=spec.metadata,
            events=[TaskEventEntry(
                actor=spec.created_by,
                detail=f"Task created: {spec.title}",
            )],
        )

    def can_transition_to(self, new_status: TaskStatus) -> bool:
        """Check if the transition to new_status is valid."""
        allowed = _VALID_TRANSITIONS.get(self.status, set())
        return new_status in allowed

    def transition_to(self, new_status: TaskStatus, actor: str, detail: str = "") -> None:
        """Transition to a new status, raising InvalidTaskTransition if invalid."""
        if not self.can_transition_to(new_status):
            raise InvalidTaskTransition(self.task_id, self.status, new_status)
        self.events.append(TaskEventEntry(
            actor=actor,
            from_status=self.status,
            to_status=new_status,
            detail=detail or f"Transitioned from {self.status.value} to {new_status.value}",
        ))
        self.status = new_status


# ============================================================================
# Board State — snapshot for TUI
# ============================================================================

class BoardState(BaseModel):
    """A complete snapshot of the current board state for TUI rendering."""
    total_tasks: int = 0
    by_status: Dict[str, int] = Field(default_factory=dict)
    by_type: Dict[str, int] = Field(default_factory=dict)
    pending_reviews: int = 0
    active_tasks: List[Task] = Field(default_factory=list)
    blocked_tasks: List[Task] = Field(default_factory=list)
    under_review_tasks: List[Task] = Field(default_factory=list)
