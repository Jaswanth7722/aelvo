"""
shared_task_board — Task Board for the Collaborative Architecture

The Task Board organizes work in Mode B (Collaborative) execution.
It tracks task state machine, persists to SQLite, and emits events
on every transition via the EventBus.

Components:
  - task.py: Task Pydantic model with state definitions
  - state_machine.py: Valid transitions, guards, validation
  - board.py: SharedTaskBoard with SQLite persistence + EventBus
  - context_schemas.py: Typed context payloads per task type
  - result_schemas.py: Typed result payloads per task type
  - collaboration_orchestrator.py: Multi-Agent Collaboration & Task Board Routing
"""

from shared_task_board.task import (
    Task,
    TaskStatus,
    TaskPriority,
    TaskType,
)
from shared_task_board.state_machine import (
    TaskStateMachine,
    InvalidTransitionError,
    TRANSITION_RULES,
)
from shared_task_board.board import (
    SharedTaskBoard,
    TaskNotFoundError,
    TaskBoardConfig,
)
from shared_task_board.context_schemas import (
    TaskContext,
    ResearchContext,
    ImplementContext,
    SecurityReviewContext,
    ExecuteContext,
    ConsensusContext,
    ReportContext,
)
from shared_task_board.result_schemas import (
    TaskResult,
    ResearchResult,
    ImplementResult,
    SecurityReviewResult,
    ExecuteResult,
    ConsensusResult,
    ReportResult,
)
from shared_task_board.collaboration_orchestrator import (
    CollaborationSession,
    CollaborationOrchestrator,
    IntelligentRouter,
    RoutingDecision,
    RoutingStrategy,
    SessionStatus,
    CollaborationPhase,
    SessionTaskRecord,
)

__all__ = [
    # task
    "Task",
    "TaskStatus",
    "TaskPriority",
    "TaskType",
    # state_machine
    "TaskStateMachine",
    "InvalidTransitionError",
    "TRANSITION_RULES",
    # board
    "SharedTaskBoard",
    "TaskNotFoundError",
    "TaskBoardConfig",
    # context_schemas
    "TaskContext",
    "ResearchContext",
    "ImplementContext",
    "SecurityReviewContext",
    "ExecuteContext",
    "ConsensusContext",
    "ReportContext",
    # result_schemas
    "TaskResult",
    "ResearchResult",
    "ImplementResult",
    "SecurityReviewResult",
    "ExecuteResult",
    "ConsensusResult",
    "ReportResult",
    # collaboration_orchestrator
    "CollaborationSession",
    "CollaborationOrchestrator",
    "IntelligentRouter",
    "RoutingDecision",
    "RoutingStrategy",
    "SessionStatus",
    "CollaborationPhase",
    "SessionTaskRecord",
]
