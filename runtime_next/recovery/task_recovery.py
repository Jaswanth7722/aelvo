# runtime_next/recovery/task_recovery.py
# Phase 11: Enhanced task-level recovery
# Handles: task abort, rollback, plan-level recovery coordination,
#          multi-step recovery chains, and recovery from plan execution failures

from __future__ import annotations

import hashlib
import logging
import time
import threading
from typing import Any, Dict, List, Optional, Set, Callable
from datetime import datetime, timezone
from enum import Enum

log = logging.getLogger("aelvo.runtime.recovery.task")


class TaskRecoveryTrigger(str, Enum):
    """What triggered the task-level recovery."""
    PLAN_FAILURE = "plan_failure"
    """The entire execution plan failed."""
    PHASE_FAILURE = "phase_failure"
    """A specific phase within the plan failed."""
    DEPENDENCY_FAILURE = "dependency_failure"
    """A dependency of the current task failed."""
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    """Task exceeded its resource budget (time, steps, retries)."""
    CONSENSUS_FAILURE = "consensus_failure"
    """Consensus required for the task failed."""
    GOVERNANCE_BLOCK = "governance_block"
    """Governance kernel blocked the task."""


class TaskRecoveryAction(str, Enum):
    """Recovery actions for task-level failures."""
    RETRY_PHASE = "retry_phase"
    """Retry the failed phase of the plan."""
    ROLLBACK_PHASE = "rollback_phase"
    """Rollback the failed phase to its starting state."""
    SKIP_PHASE = "skip_phase"
    """Skip the failed phase and continue with remaining phases."""
    REPLAN = "replan"
    """Trigger a full replan via Architect."""
    BREAKDOWN_TASK = "breakdown_task"
    """Break the task into smaller sub-tasks."""
    ESCALATE_TO_ARCHITECT = "escalate_to_architect"
    """Escalate to Architect for task-level decisions."""
    ABORT_TASK = "abort_task"
    """Abort the entire task."""
    SAVEPOINT_ROLLBACK = "savepoint_rollback"
    """Rollback to the last known good savepoint."""


class TaskRecoveryStrategy:
    """A single recovery strategy for a task failure trigger."""

    def __init__(
        self,
        trigger: TaskRecoveryTrigger,
        action: TaskRecoveryAction,
        description: str,
        max_attempts: int = 2,
        requires_architect: bool = False,
    ):
        self.trigger = trigger
        self.action = action
        self.description = description
        self.max_attempts = max_attempts
        self.requires_architect = requires_architect


class TaskRecoveryEngine:
    """Manages task-level recovery from execution failures.

    Coordinates recovery across the three levels:
    1. Consensus recovery (below)
    2. Specialist recovery (below)
    3. Task-level recovery (this engine)

    Supports:
    - Phase-level retry, rollback, and skip
    - Plan-level replanning
    - Savepoint-based rollback
    - Architect escalation
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._history: List[Dict[str, Any]] = []
        self._attempt_counts: Dict[str, int] = {}
        self._savepoints: Dict[str, Dict[str, Any]] = {}
        self._strategies: Dict[TaskRecoveryTrigger, List[TaskRecoveryStrategy]] = {}
        self._architect_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
        self._governance_hooks: Any = None
        self._register_defaults()

    def set_architect_callback(
        self, callback: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        """Register a callback for Architect escalation."""
        self._architect_callback = callback

    def set_governance_hooks(self, hooks: Any) -> None:
        """Link governance hooks for policy enforcement on recovery actions."""
        self._governance_hooks = hooks

    def _register_defaults(self) -> None:
        """Register default recovery strategies for each task failure trigger."""
        self._strategies[TaskRecoveryTrigger.PLAN_FAILURE] = [
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.PLAN_FAILURE,
                action=TaskRecoveryAction.REPLAN,
                description="Trigger full replan via Architect with failure context",
                max_attempts=2,
                requires_architect=True,
            ),
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.PLAN_FAILURE,
                action=TaskRecoveryAction.ABORT_TASK,
                description="Cannot recover from plan failure — aborting task",
                max_attempts=1,
            ),
        ]

        self._strategies[TaskRecoveryTrigger.PHASE_FAILURE] = [
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.PHASE_FAILURE,
                action=TaskRecoveryAction.RETRY_PHASE,
                description="Retry the failed phase with error context",
                max_attempts=2,
            ),
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.PHASE_FAILURE,
                action=TaskRecoveryAction.SKIP_PHASE,
                description="Skip the failed phase and continue",
                max_attempts=1,
                requires_architect=True,
            ),
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.PHASE_FAILURE,
                action=TaskRecoveryAction.ROLLBACK_PHASE,
                description="Rollback the failed phase to its starting state",
                max_attempts=1,
            ),
        ]

        self._strategies[TaskRecoveryTrigger.DEPENDENCY_FAILURE] = [
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.DEPENDENCY_FAILURE,
                action=TaskRecoveryAction.RETRY_PHASE,
                description="Retry after dependency is resolved",
                max_attempts=3,
            ),
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.DEPENDENCY_FAILURE,
                action=TaskRecoveryAction.BREAKDOWN_TASK,
                description="Break task to isolate from broken dependency",
                max_attempts=2,
                requires_architect=True,
            ),
        ]

        self._strategies[TaskRecoveryTrigger.RESOURCE_EXHAUSTION] = [
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.RESOURCE_EXHAUSTION,
                action=TaskRecoveryAction.BREAKDOWN_TASK,
                description="Break task into smaller pieces to fit budget",
                max_attempts=2,
                requires_architect=True,
            ),
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.RESOURCE_EXHAUSTION,
                action=TaskRecoveryAction.ABORT_TASK,
                description="Task exceeded budget — aborting",
                max_attempts=1,
            ),
        ]

        self._strategies[TaskRecoveryTrigger.CONSENSUS_FAILURE] = [
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.CONSENSUS_FAILURE,
                action=TaskRecoveryAction.ESCALATE_TO_ARCHITECT,
                description="Consensus failure — escalate to Architect for decision",
                max_attempts=1,
                requires_architect=True,
            ),
        ]

        self._strategies[TaskRecoveryTrigger.GOVERNANCE_BLOCK] = [
            TaskRecoveryStrategy(
                trigger=TaskRecoveryTrigger.GOVERNANCE_BLOCK,
                action=TaskRecoveryAction.ESCALATE_TO_ARCHITECT,
                description="Governance block — escalate to Architect for override",
                max_attempts=1,
                requires_architect=True,
            ),
        ]

    # ── Savepoint Management ─────────────────────────────────────────────────

    def create_savepoint(self, task_id: str, state: Dict[str, Any]) -> None:
        """Create a savepoint that can be rolled back to.

        Args:
            task_id: The task to create a savepoint for.
            state: The execution state to save.
        """
        with self._lock:
            self._savepoints[task_id] = {
                "state": state,
                "created_at": time.time(),
            }
            log.info("Savepoint created for task %s", task_id[:8])

    def rollback_to_savepoint(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Rollback a task to its last savepoint.

        Args:
            task_id: The task to rollback.

        Returns:
            The saved state if a savepoint exists, None otherwise.
        """
        with self._lock:
            savepoint = self._savepoints.get(task_id)
            if savepoint is None:
                log.warning("No savepoint found for task %s", task_id[:8])
                return None
            log.info("Rolled back task %s to savepoint", task_id[:8])
            return savepoint["state"]

    def clear_savepoint(self, task_id: str) -> None:
        """Clear a savepoint (e.g., after successful completion)."""
        with self._lock:
            self._savepoints.pop(task_id, None)

    # ── Task Recovery ────────────────────────────────────────────────────────

    def handle_task_failure(
        self,
        task_id: str,
        trigger: TaskRecoveryTrigger,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Handle a task-level failure with staged recovery.

        Args:
            task_id: The ID of the failed task.
            trigger: The trigger that caused the failure.
            context: Additional context (phase info, error details, etc.).

        Returns:
            Dict with keys:
            - recovered: bool — whether recovery was found
            - action: str — the recovery action
            - details: str — description of what was done
            - next_steps: list — recommended next actions
        """
        ctx = context or {}
        with self._lock:
            key = f"{task_id}:{trigger.value}"
            self._attempt_counts[key] = self._attempt_counts.get(key, 0) + 1
            attempt = self._attempt_counts[key]

            strategies = self._strategies.get(trigger, [])
            if not strategies:
                return {
                    "recovered": False,
                    "action": "no_strategy",
                    "details": f"No recovery strategy for {trigger.value}",
                    "next_steps": ["abort_task"],
                }

            for strategy in strategies:
                if attempt > strategy.max_attempts:
                    continue

                # Governance pre-hook check
                if self._governance_hooks is not None:
                    outcome = self._governance_hooks.pre_task_recovery(
                        task_id=task_id,
                        action_type=strategy.action.value,
                        task_trigger=trigger.value,
                        context=ctx,
                    )
                    if outcome.result.value in ("denied", "approval_pending"):
                        log.warning(
                            "Governance blocked task action '%s' for %s: %s",
                            strategy.action.value, task_id[:8], outcome.reason,
                        )
                        self._history.append({
                            "task_id": task_id,
                            "trigger": trigger.value,
                            "attempt": attempt,
                            "strategy": strategy.action.value,
                            "result": {
                                "recovered": False,
                                "action": "governance_blocked",
                                "details": outcome.reason,
                            },
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        })
                        continue

                if strategy.requires_architect and self._architect_callback:
                    try:
                        self._architect_callback(task_id, {
                            "trigger": trigger.value,
                            "strategy": strategy.action.value,
                            "context": ctx,
                        })
                    except Exception as e:
                        log.warning("Architect callback failed: %s", e)

                result = self._execute_strategy(strategy, task_id, ctx)
                self._history.append({
                    "task_id": task_id,
                    "trigger": trigger.value,
                    "attempt": attempt,
                    "strategy": strategy.action.value,
                    "result": result,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

                if result.get("recovered", False):
                    return result

            return {
                "recovered": False,
                "action": TaskRecoveryAction.ABORT_TASK.value,
                "details": f"All recovery strategies exhausted for {task_id[:8]}",
                "next_steps": ["abort_task"],
            }

    def get_history(
        self,
        task_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get recovery history, optionally filtered by task_id."""
        with self._lock:
            results = list(self._history)
            if task_id:
                results = [h for h in results if h["task_id"] == task_id]
            return results[-limit:]

    def reset(self) -> None:
        """Reset all state — for testing and session boundaries."""
        with self._lock:
            self._history.clear()
            self._attempt_counts.clear()
            self._savepoints.clear()

    @property
    def recovery_count(self) -> int:
        return len(self._history)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _execute_strategy(
        self,
        strategy: TaskRecoveryStrategy,
        task_id: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a single task recovery strategy."""
        action = strategy.action

        if action == TaskRecoveryAction.RETRY_PHASE:
            phase_id = context.get("phase_id", "")
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Retrying phase {phase_id}" if phase_id else "Retrying failed phase",
                "phase_id": phase_id,
                "next_steps": ["retry_phase_execution"],
            }

        if action == TaskRecoveryAction.ROLLBACK_PHASE:
            phase_id = context.get("phase_id", "")
            saved = self.rollback_to_savepoint(task_id)
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Rolled back phase {phase_id}" if phase_id else "Rolled back failed phase",
                "phase_id": phase_id,
                "saved_state": saved,
                "next_steps": ["retry_phase_execution"],
            }

        if action == TaskRecoveryAction.SKIP_PHASE:
            phase_id = context.get("phase_id", "")
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Skipping phase {phase_id}" if phase_id else "Skipping failed phase",
                "phase_id": phase_id,
                "next_steps": ["continue_to_next_phase"],
            }

        if action == TaskRecoveryAction.SAVEPOINT_ROLLBACK:
            saved = self.rollback_to_savepoint(task_id)
            if saved is None:
                return {
                    "recovered": False,
                    "action": action.value,
                    "details": "No savepoint to rollback to",
                    "next_steps": ["abort_task"],
                }
            return {
                "recovered": True,
                "action": action.value,
                "details": "Rolled back to last savepoint",
                "saved_state": saved,
                "next_steps": ["retry_task"],
            }

        if action == TaskRecoveryAction.REPLAN:
            return {
                "recovered": True,
                "action": action.value,
                "details": "Triggering full replan via Architect",
                "failure_context": {
                    "task_id": task_id,
                    "trigger": strategy.trigger.value,
                    "context": context,
                },
                "next_steps": ["architect_replan"],
            }

        if action == TaskRecoveryAction.BREAKDOWN_TASK:
            return {
                "recovered": True,
                "action": action.value,
                "details": "Task will be broken into smaller sub-tasks",
                "next_steps": ["architect_breakdown"],
            }

        if action == TaskRecoveryAction.ABORT_TASK:
            return {
                "recovered": False,
                "action": action.value,
                "details": f"Task {task_id[:8]} aborted",
                "next_steps": ["user_notification"],
            }

        if action == TaskRecoveryAction.ESCALATE_TO_ARCHITECT:
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Escalating {strategy.trigger.value} to Architect",
                "next_steps": ["architect_decides"],
            }

        return {
            "recovered": False,
            "action": "unknown",
            "details": f"Unknown action: {action.value}",
            "next_steps": ["abort_task"],
        }

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
