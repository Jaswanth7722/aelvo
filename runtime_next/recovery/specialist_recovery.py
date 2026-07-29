# runtime_next/recovery/specialist_recovery.py
# Phase 11: Specialist-level recovery
# Handles: deadline-based reassignment, specialist failover, timeout recovery,
#          context preservation for retries, specialist health monitoring

from __future__ import annotations

import hashlib
import logging
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timezone
from enum import Enum

log = logging.getLogger("aelvo.runtime.recovery.specialist")


class SpecialistState(str, Enum):
    """Health state of a specialist."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNRESPONSIVE = "unresponsive"
    FAILED = "failed"
    RECOVERING = "recovering"


class SpecialistRecoveryAction(str, Enum):
    """Recovery actions specific to specialist failures."""
    RETRY_SAME = "retry_same"
    """Retry the same specialist with context preservation."""
    FAILOVER = "failover"
    """Reassign to an alternate specialist."""
    ESCALATE_TO_ARCHITECT = "escalate_to_architect"
    """Escalate reassignment decision to Architect."""
    REDUCE_SCOPE = "reduce_scope"
    """Retry same specialist with reduced task scope."""
    BREAKDOWN_TASK = "breakdown_task"
    """Break the task into smaller pieces for easier handling."""
    WAIT_AND_RETRY = "wait_and_retry"
    """Wait for specialist to become healthy and retry."""
    PRESERVE_CONTEXT = "preserve_context"
    """Persist the specialist's execution context for later use."""


class SpecialistFailoverStrategy:
    """A failover strategy for when a specialist fails."""

    def __init__(
        self,
        primary: str,
        failovers: List[str],
        max_retries: int = 3,
        timeout_seconds: float = 60.0,
    ):
        self.primary = primary
        self.failovers = failovers
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds

    @property
    def all_specialists(self) -> List[str]:
        """Get all specialists in priority order (primary first, then failovers)."""
        return [self.primary] + self.failovers


class SpecialistRecoveryEngine:
    """Manages specialist-level recovery with failover, context preservation,
    and health monitoring.

    Tracks:
    - Each specialist's health state
    - Active deadlines for in-flight tasks
    - Failover chains (primary → failover(s))
    - Execution context for retries

    Default failover chains:
    - FORGE → TERMINUS (tool execution fallback)
    - SENTINEL → ARCHITECT (security review escalation)
    - ORACLE → HERMES (research fallback)
    - TERMINUS → FORGE (execution fallback)
    """

    DEFAULT_FAILOVERS: Dict[str, List[str]] = {
        "FORGE": ["TERMINUS"],
        "SENTINEL": ["ARCHITECT"],
        "ORACLE": ["HERMES"],
        "TERMINUS": ["FORGE"],
        "ARCHITECT": [],
        "HERMES": ["ORACLE"],
        "HERALD": [],
    }

    DEFAULT_TIMEOUTS: Dict[str, float] = {
        "FORGE": 120.0,
        "SENTINEL": 60.0,
        "ORACLE": 90.0,
        "TERMINUS": 60.0,
        "ARCHITECT": 30.0,
        "HERMES": 30.0,
        "HERALD": 30.0,
    }

    def __init__(self):
        self._lock = threading.RLock()
        self._states: Dict[str, SpecialistState] = {}
        self._deadlines: Dict[str, Dict[str, Any]] = {}
        self._contexts: Dict[str, List[Dict[str, Any]]] = {}
        self._failover_strategies: Dict[str, SpecialistFailoverStrategy] = {}
        self._history: List[Dict[str, Any]] = []
        self._reassign_callback: Optional[Callable[[str, str, Dict[str, Any]], None]] = None
        self._governance_hooks: Any = None

        self._register_defaults()

    def set_reassign_callback(
        self, callback: Callable[[str, str, Dict[str, Any]], None]
    ) -> None:
        """Register a callback invoked when a specialist is reassigned.
        
        Args:
            callback: (original_specialist, new_specialist, context) -> None
        """
        self._reassign_callback = callback

    def set_governance_hooks(self, hooks: Any) -> None:
        """Link governance hooks for policy enforcement on recovery actions."""
        self._governance_hooks = hooks

    def _register_defaults(self) -> None:
        """Register default failover strategies for all known specialists."""
        for specialist, failovers in self.DEFAULT_FAILOVERS.items():
            timeout = self.DEFAULT_TIMEOUTS.get(specialist, 60.0)
            self._failover_strategies[specialist] = SpecialistFailoverStrategy(
                primary=specialist,
                failovers=failovers,
                max_retries=3,
                timeout_seconds=timeout,
            )
            self._states[specialist] = SpecialistState.HEALTHY

    # ── Health Monitoring ─────────────────────────────────────────────────────

    def get_state(self, specialist: str) -> SpecialistState:
        """Get the current health state of a specialist."""
        with self._lock:
            return self._states.get(specialist.upper(), SpecialistState.HEALTHY)

    def set_state(self, specialist: str, state: SpecialistState) -> None:
        """Set the health state of a specialist."""
        with self._lock:
            self._states[specialist.upper()] = state

    def mark_failure(self, specialist: str) -> None:
        """Mark a specialist as degraded/failed based on failure count."""
        with self._lock:
            upper = specialist.upper()
            current = self._states.get(upper, SpecialistState.HEALTHY)
            if current == SpecialistState.HEALTHY:
                self._states[upper] = SpecialistState.DEGRADED
            elif current == SpecialistState.DEGRADED:
                self._states[upper] = SpecialistState.UNRESPONSIVE
            elif current == SpecialistState.UNRESPONSIVE:
                self._states[upper] = SpecialistState.FAILED

    def recover(self, specialist: str) -> None:
        """Mark a specialist as recovering (health restored)."""
        with self._lock:
            self._states[specialist.upper()] = SpecialistState.RECOVERING

    def mark_healthy(self, specialist: str) -> None:
        """Mark a specialist as fully healthy."""
        with self._lock:
            self._states[specialist.upper()] = SpecialistState.HEALTHY

    def get_all_states(self) -> Dict[str, SpecialistState]:
        """Get health states for all known specialists."""
        with self._lock:
            return dict(self._states)

    # ── Deadline Management ───────────────────────────────────────────────────

    def start_task(
        self,
        task_id: str,
        specialist: str,
        description: str = "",
        timeout_seconds: Optional[float] = None,
    ) -> None:
        """Start tracking a task with a deadline for a specialist.

        Args:
            task_id: Unique task identifier.
            specialist: The specialist responsible.
            description: Task description (stored in context).
            timeout_seconds: Custom timeout; uses default if None.
        """
        with self._lock:
            if task_id in self._deadlines:
                log.warning(
                    "Task %s already has a deadline — overwriting existing entry",
                    task_id[:8],
                )
            strategy = self._failover_strategies.get(specialist.upper())
            timeout = timeout_seconds or (strategy.timeout_seconds if strategy else 60.0)
            deadline = time.time() + timeout
            self._deadlines[task_id] = {
                "specialist": specialist.upper(),
                "deadline": deadline,
                "timeout_seconds": timeout,
                "description": description,
                "retry_count": 0,
                "started_at": time.time(),
            }

    def check_deadline(self, task_id: str) -> bool:
        """Check if a task has exceeded its deadline.

        Returns:
            True if deadline is exceeded (task timed out), False otherwise.
        """
        with self._lock:
            info = self._deadlines.get(task_id)
            if info is None:
                return True  # Unknown task = should recover
            if time.time() > info["deadline"]:
                return True  # Deadline exceeded
            return False

    def complete_task(self, task_id: str) -> None:
        """Mark a task as completed successfully."""
        with self._lock:
            self._deadlines.pop(task_id, None)

    def get_active_tasks(self) -> List[Dict[str, Any]]:
        """Get all currently tracked tasks with their status."""
        with self._lock:
            now = time.time()
            tasks = []
            for task_id, info in self._deadlines.items():
                remaining = max(0.0, info["deadline"] - now)
                tasks.append({
                    "task_id": task_id,
                    "specialist": info["specialist"],
                    "description": info["description"],
                    "remaining_seconds": round(remaining, 1),
                    "is_overdue": remaining <= 0,
                    "retry_count": info["retry_count"],
                })
            return tasks

    # ── Context Preservation ──────────────────────────────────────────────────

    def preserve_context(self, specialist: str, context: Dict[str, Any]) -> None:
        """Save execution context for a specialist for later retry.

        Args:
            specialist: The specialist whose context is being saved.
            context: The execution context to preserve.
        """
        with self._lock:
            upper = specialist.upper()
            if upper not in self._contexts:
                self._contexts[upper] = []
            self._contexts[upper].append({
                "context": context,
                "saved_at": time.time(),
            })
            # Keep only the last 5 contexts per specialist
            if len(self._contexts[upper]) > 5:
                self._contexts[upper] = self._contexts[upper][-5:]

    def get_preserved_context(self, specialist: str) -> Optional[Dict[str, Any]]:
        """Get the most recent preserved context for a specialist.

        Returns:
            The most recent context dict, or None if none preserved.
        """
        with self._lock:
            upper = specialist.upper()
            contexts = self._contexts.get(upper, [])
            if not contexts:
                return None
            return contexts[-1]["context"]

    def clear_preserved_context(self, specialist: str) -> None:
        """Clear all preserved contexts for a specialist."""
        with self._lock:
            self._contexts.pop(specialist.upper(), None)

    # ── Failover / Reassignment ──────────────────────────────────────────────

    def handle_failure(
        self,
        task_id: str,
        specialist: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Handle a specialist failure with staged recovery.

        Stages:
        1. Preserve context for potential retry
        2. Check deadline
        3. Attempt retry (same specialist) if within budget
        4. Attempt failover to alternate specialist
        5. Escalate to Architect for reassignment

        Args:
            task_id: The ID of the failed task.
            specialist: The specialist that failed.
            context: Optional execution context to preserve.

        Returns:
            Dict with keys:
            - recovered: bool — whether recovery was found
            - action: str — the recovery action taken
            - specialist: str — which specialist should handle next
            - details: str — description of the recovery
            - preserved_context: dict or None — context for retry
        """
        with self._lock:
            upper = specialist.upper()

            # Preserve context if provided
            if context:
                self.preserve_context(upper, context)

            # Update health state
            self.mark_failure(upper)

            # Get task deadline info
            deadline_info = self._deadlines.get(task_id)
            retry_count = deadline_info["retry_count"] if deadline_info else 0

            # Get failover strategy
            strategy = self._failover_strategies.get(upper)
            if not strategy:
                result = {
                    "recovered": False,
                    "action": "no_strategy",
                    "specialist": upper,
                    "details": f"No failover strategy for {upper}",
                }
                self._history.append({
                    "task_id": task_id,
                    "specialist": upper,
                    "result": result,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
                return result

            # Stage 1: Retry same specialist if within budget
            if retry_count < strategy.max_retries:
                governance_blocked = False
                if self._governance_hooks:
                    outcome = self._governance_hooks.pre_specialist_recovery(
                        task_id=task_id,
                        action_type=SpecialistRecoveryAction.RETRY_SAME.value,
                        specialist=upper,
                        context=context,
                    )
                    if outcome.result.value in ("denied", "approval_pending"):
                        log.warning(
                            "Governance blocked retry for %s on %s: %s",
                            upper, task_id[:8], outcome.reason,
                        )
                        governance_blocked = True

                if not governance_blocked:
                    if deadline_info:
                        deadline_info["retry_count"] += 1
                    result = {
                        "recovered": True,
                        "action": SpecialistRecoveryAction.RETRY_SAME.value,
                        "specialist": upper,
                        "details": f"Retry {retry_count + 1}/{strategy.max_retries} for {upper}",
                        "preserved_context": self.get_preserved_context(upper),
                        "next_steps": ["retry_execution"],
                    }
                    self._history.append({
                        "task_id": task_id,
                        "specialist": upper,
                        "result": result,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    return result
                else:
                    # Governance blocked — consume retry budget to avoid infinite loop
                    if deadline_info:
                        deadline_info["retry_count"] += 1
                    log.info(
                        "Retry budget consumed due to governance block for %s on %s",
                        upper, task_id[:8],
                    )
                    # Fall through to failover stage

            # Stage 2: Try failovers
            for failover in strategy.failovers:
                failover_state = self._states.get(failover, SpecialistState.HEALTHY)
                if failover_state in (SpecialistState.HEALTHY, SpecialistState.DEGRADED):
                    # Governance check for failover
                    if self._governance_hooks:
                        outcome = self._governance_hooks.pre_specialist_recovery(
                            task_id=task_id,
                            action_type=SpecialistRecoveryAction.FAILOVER.value,
                            specialist=upper,
                            context={"target": failover, **(context or {})},
                        )
                        if outcome.result.value in ("denied", "approval_pending"):
                            log.warning(
                                "Governance blocked failover from %s to %s: %s",
                                upper, failover, outcome.reason,
                            )
                            continue  # Try next failover

                    if self._reassign_callback:
                        try:
                            self._reassign_callback(
                                upper, failover,
                                context or {},
                            )
                        except Exception as e:
                            log.warning("Reassign callback failed: %s", e)

                    result = {
                        "recovered": True,
                        "action": SpecialistRecoveryAction.FAILOVER.value,
                        "specialist": failover,
                        "details": f"Failover from {upper} to {failover}",
                        "preserved_context": self.get_preserved_context(upper),
                        "next_steps": ["reassign_execution"],
                    }
                    self._history.append({
                        "task_id": task_id,
                        "specialist": upper,
                        "result": result,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    return result

            # Stage 3: Escalate to Architect
            result = {
                "recovered": False,
                "action": SpecialistRecoveryAction.ESCALATE_TO_ARCHITECT.value,
                "specialist": None,
                "details": (
                    f"{upper} failed and no healthy failover available — "
                    f"escalating to Architect"
                ),
                "preserved_context": self.get_preserved_context(upper),
                "next_steps": ["architect_reassign"],
            }
            self._history.append({
                "task_id": task_id,
                "specialist": upper,
                "result": result,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            return result

    def get_history(
        self,
        specialist: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get recovery history, optionally filtered by specialist."""
        with self._lock:
            results = list(self._history)
            if specialist:
                results = [h for h in results if h.get("specialist") == specialist.upper()]
            return results[-limit:]

    def reset(self) -> None:
        """Reset all state — for testing and session boundaries."""
        with self._lock:
            self._states.clear()
            self._deadlines.clear()
            self._contexts.clear()
            self._history.clear()
            self._register_defaults()

    @property
    def failure_count(self) -> int:
        return sum(1 for s in self._states.values() if s in (
            SpecialistState.FAILED, SpecialistState.UNRESPONSIVE
        ))

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
