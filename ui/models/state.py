"""
AELVO State Models
===================
Pydantic models for representing system state (tasks, specialists, tools, memory, etc.).
"""

import time
from typing import Any, Dict, List, Optional
from enum import Enum
from dataclasses import dataclass, field


class TaskState(str, Enum):
    """Task execution states."""
    PENDING = "pending"
    RUNNING = "running"
    BLOCKED = "blocked"
    FAILED = "failed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class SpecialistState(str, Enum):
    """Specialist activation states."""
    INACTIVE = "inactive"
    ACTIVATING = "activating"
    ACTIVE = "active"
    THINKING = "thinking"
    ACTING = "acting"
    DEACTIVATING = "deactivating"


class ToolState(str, Enum):
    """Tool execution states."""
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class VerificationState(str, Enum):
    """Verification states."""
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    RETRY = "retry"
    BLOCKED = "blocked"
    SKIPPED = "skipped"


class RiskLevel(str, Enum):
    """Risk levels for safety assessment."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Task:
    """Represents a task in the execution graph."""
    id: str
    name: str
    description: str = ""
    state: TaskState = TaskState.PENDING
    specialist: str = "HERMES"
    progress: float = 0.0
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    parent_id: Optional[str] = None
    child_ids: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    
    @property
    def duration(self) -> Optional[float]:
        """Get task duration if completed."""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "state": self.state.value,
            "specialist": self.specialist,
            "progress": self.progress,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "parent_id": self.parent_id,
            "child_ids": self.child_ids,
            "dependencies": self.dependencies,
            "metadata": self.metadata,
            "error_message": self.error_message,
            "duration": self.duration
        }


@dataclass
class Specialist:
    """Represents a specialist agent."""
    name: str
    state: SpecialistState = SpecialistState.INACTIVE
    activation_score: float = 0.0
    current_task_id: Optional[str] = None
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    last_activity: Optional[float] = None
    capabilities: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_active(self) -> bool:
        """Check if specialist is active."""
        return self.state in [SpecialistState.ACTIVE, SpecialistState.THINKING, SpecialistState.ACTING]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert specialist to dictionary."""
        return {
            "name": self.name,
            "state": self.state.value,
            "activation_score": self.activation_score,
            "current_task_id": self.current_task_id,
            "total_tasks_completed": self.total_tasks_completed,
            "total_tasks_failed": self.total_tasks_failed,
            "last_activity": self.last_activity,
            "capabilities": self.capabilities,
            "metadata": self.metadata,
            "is_active": self.is_active
        }


@dataclass
class ToolExecution:
    """Represents a tool execution."""
    id: str
    tool_name: str
    command: str
    state: ToolState = ToolState.IDLE
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    exit_code: Optional[int] = None
    output: str = ""
    error_output: str = ""
    task_id: Optional[str] = None
    duration: Optional[float] = None
    
    @property
    def is_running(self) -> bool:
        """Check if tool is currently running."""
        return self.state == ToolState.RUNNING
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert tool execution to dictionary."""
        return {
            "id": self.id,
            "tool_name": self.tool_name,
            "command": self.command,
            "state": self.state.value,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "exit_code": self.exit_code,
            "output": self.output,
            "error_output": self.error_output,
            "task_id": self.task_id,
            "duration": self.duration,
            "is_running": self.is_running
        }


@dataclass
class MemoryItem:
    """Represents a memory item retrieved or stored."""
    id: str
    content: str
    memory_type: str  # episodic, semantic, procedural
    relevance_score: float = 0.0
    source: str = ""
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert memory item to dictionary."""
        return {
            "id": self.id,
            "content": self.content,
            "memory_type": self.memory_type,
            "relevance_score": self.relevance_score,
            "source": self.source,
            "timestamp": self.timestamp,
            "metadata": self.metadata
        }


@dataclass
class VerificationResult:
    """Represents a verification result."""
    id: str
    verification_type: str  # lint, typecheck, test, security
    target: str
    state: VerificationState = VerificationState.PENDING
    confidence: float = 0.0
    details: str = ""
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    task_id: Optional[str] = None
    
    @property
    def is_running(self) -> bool:
        """Check if verification is running."""
        return self.state == VerificationState.RUNNING
    
    @property
    def passed(self) -> bool:
        """Check if verification passed."""
        return self.state == VerificationState.PASSED
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert verification result to dictionary."""
        return {
            "id": self.id,
            "verification_type": self.verification_type,
            "target": self.target,
            "state": self.state.value,
            "confidence": self.confidence,
            "details": self.details,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "task_id": self.task_id,
            "is_running": self.is_running,
            "passed": self.passed
        }


@dataclass
class SafetyEvent:
    """Represents a safety event."""
    id: str
    action: str
    risk_level: RiskLevel
    reason: str
    requires_approval: bool = False
    impact_assessment: str = ""
    timestamp: float = field(default_factory=time.time)
    approved: Optional[bool] = None
    approved_by: Optional[str] = None
    
    @property
    def is_pending(self) -> bool:
        """Check if approval is pending."""
        return self.requires_approval and self.approved is None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert safety event to dictionary."""
        return {
            "id": self.id,
            "action": self.action,
            "risk_level": self.risk_level.value,
            "reason": self.reason,
            "requires_approval": self.requires_approval,
            "impact_assessment": self.impact_assessment,
            "timestamp": self.timestamp,
            "approved": self.approved,
            "approved_by": self.approved_by,
            "is_pending": self.is_pending
        }


@dataclass
class SystemState:
    """Overall system state."""
    tasks: Dict[str, Task] = field(default_factory=dict)
    specialists: Dict[str, Specialist] = field(default_factory=dict)
    tool_executions: Dict[str, ToolExecution] = field(default_factory=dict)
    memory_items: List[MemoryItem] = field(default_factory=list)
    verification_results: Dict[str, VerificationResult] = field(default_factory=dict)
    safety_events: List[SafetyEvent] = field(default_factory=list)
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID."""
        return self.tasks.get(task_id)
    
    def get_specialist(self, name: str) -> Optional[Specialist]:
        """Get specialist by name."""
        return self.specialists.get(name)
    
    def get_active_specialists(self) -> List[Specialist]:
        """Get all active specialists."""
        return [s for s in self.specialists.values() if s.is_active]
    
    def get_running_tasks(self) -> List[Task]:
        """Get all running tasks."""
        return [t for t in self.tasks.values() if t.state == TaskState.RUNNING]
    
    def get_pending_approvals(self) -> List[SafetyEvent]:
        """Get all pending safety approvals."""
        return [e for e in self.safety_events if e.is_pending]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert system state to dictionary."""
        return {
            "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
            "specialists": {k: v.to_dict() for k, v in self.specialists.items()},
            "tool_executions": {k: v.to_dict() for k, v in self.tool_executions.items()},
            "memory_items": [m.to_dict() for m in self.memory_items],
            "verification_results": {k: v.to_dict() for k, v in self.verification_results.items()},
            "safety_events": [e.to_dict() for e in self.safety_events]
        }