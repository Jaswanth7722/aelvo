"""
state_machine.py — Task State Machine

Defines valid state transitions, guard conditions, and the
TaskStateMachine that validates every transition before it
is applied to a Task.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from shared_task_board.task import TaskStatus


class InvalidTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""

    def __init__(
        self,
        task_id: str,
        from_status: TaskStatus,
        to_status: TaskStatus,
        reason: str = "",
    ):
        self.task_id = task_id
        self.from_status = from_status
        self.to_status = to_status
        msg = (
            f"Invalid transition for task {task_id[:12]}: "
            f"{from_status.value} -> {to_status.value}"
        )
        if reason:
            msg += f" ({reason})"
        super().__init__(msg)


# ── Transition Rules ─────────────────────────────────────────────
# Map of from_status -> set of allowed to_status values

TRANSITION_RULES: Dict[TaskStatus, Set[TaskStatus]] = {
    # PENDING can be assigned or cancelled
    TaskStatus.PENDING: {
        TaskStatus.ASSIGNED,
        TaskStatus.CANCELLED,
    },
    # ASSIGNED can start work or be blocked
    TaskStatus.ASSIGNED: {
        TaskStatus.IN_PROGRESS,
        TaskStatus.BLOCKED,
        TaskStatus.CANCELLED,
    },
    # IN_PROGRESS can move to review, get blocked, or fail
    TaskStatus.IN_PROGRESS: {
        TaskStatus.REVIEWING,
        TaskStatus.BLOCKED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
    },
    # REVIEWING can complete, fail, or go back for more work
    TaskStatus.REVIEWING: {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.IN_PROGRESS,  # architect asked for revisions
        TaskStatus.BLOCKED,
    },
    # COMPLETED is terminal
    TaskStatus.COMPLETED: set(),
    # FAILED can be retried (goes back to ASSIGNED or IN_PROGRESS)
    TaskStatus.FAILED: {
        TaskStatus.ASSIGNED,      # reassign to a (possibly different) specialist
        TaskStatus.IN_PROGRESS,   # retry with the same specialist
        TaskStatus.CANCELLED,
    },
    # BLOCKED can unblock into any active state
    TaskStatus.BLOCKED: {
        TaskStatus.ASSIGNED,
        TaskStatus.IN_PROGRESS,
        TaskStatus.CANCELLED,
    },
    # CANCELLED is terminal
    TaskStatus.CANCELLED: set(),
}


class TaskStateMachine:
    """Enforces valid state transitions for Tasks.

    The state machine is conservative — it only allows transitions
    explicitly listed in TRANSITION_RULES. Any other transition
    raises InvalidTransitionError.
    """

    @classmethod
    def transition(
        cls,
        task_id: str,
        from_status: TaskStatus,
        to_status: TaskStatus,
    ) -> bool:
        """Validate and apply a state transition.

        Returns True if the transition is valid, False otherwise.
        Raises InvalidTransitionError on invalid transitions.
        """
        allowed = TRANSITION_RULES.get(from_status, set())
        if to_status not in allowed:
            raise InvalidTransitionError(task_id, from_status, to_status)
        return True

    @classmethod
    def can_transition(
        cls,
        from_status: TaskStatus,
        to_status: TaskStatus,
    ) -> bool:
        """Check if a transition is valid without raising."""
        allowed = TRANSITION_RULES.get(from_status, set())
        return to_status in allowed

    @classmethod
    def allowed_transitions(
        cls, status: TaskStatus,
    ) -> List[TaskStatus]:
        """Return all valid target states from a given status."""
        return sorted(
            TRANSITION_RULES.get(status, set()),
            key=lambda s: s.value,
        )

    @classmethod
    def is_terminal(cls, status: TaskStatus) -> bool:
        """Check if a status is terminal (no outgoing transitions)."""
        return len(TRANSITION_RULES.get(status, set())) == 0

    @classmethod
    def apply_task(
        cls,
        task,
        to_status: TaskStatus,
        reason: str = "",
    ) -> bool:
        """Validate and apply a transition on a Task object.

        Returns True if the transition was applied.
        Raises InvalidTransitionError on invalid transitions.
        """
        from shared_task_board.task import Task as _  # noqa: F401 — type check only

        from_status = task.status
        cls.transition(task.id, from_status, to_status)

        # Record history
        task.record_transition(from_status, to_status, reason)

        # Update status
        task.status = to_status

        # Update timing metadata
        import time
        if to_status == TaskStatus.ASSIGNED and task.assigned_at is None:
            task.assigned_at = time.time()
        if to_status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
            task.completed_at = time.time()

        return True
