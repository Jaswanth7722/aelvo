# runtime_next/governance/recovery_hooks.py
# Phase 13: Pre/post governance hooks for recovery actions.
#
# Integrates governance policy enforcement into each recovery level:
# - Consensus recovery: pre-hook checks policies before action execution
# - Specialist recovery: pre-hook checks policies before failover/escalation
# - Task recovery: pre-hook checks policies before retry/rollback/abort
#
# Each hook provides:
# - Pre-hook: Evaluate policies and return approval/deny/require_approval
# - Post-hook: Record policy evaluation result for audit

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, field

from .policy_engine import (
    GovernancePolicyEngine,
    PolicyScope,
    PolicyEffect,
    PolicyEvaluation,
    PolicyRule,
)

log = logging.getLogger("aelvo.runtime.governance.hooks")


class HookResult(str, Enum):
    """Result of a governance hook evaluation."""
    ALLOWED = "allowed"
    """Action is permitted to proceed."""
    DENIED = "denied"
    """Action is blocked by policy."""
    APPROVAL_PENDING = "approval_pending"
    """Action requires approval before proceeding."""



@dataclass
class HookOutcome:
    """Outcome of a governance hook evaluation."""

    result: HookResult
    reason: str
    policy_id: Optional[str] = None
    approval_token: Optional[str] = None
    evaluation: Optional[PolicyEvaluation] = None
    duration_ms: float = 0.0


class RecoveryGovernanceHooks:
    """Pre/post governance hooks for all three recovery levels.

    Each recovery level has:
    - A pre-hook that evaluates policies before the action executes
    - A post-hook that records the outcome for audit

    The hooks integrate with the GovernancePolicyEngine to enforce
    organizational policies on recovery actions.
    """

    def __init__(self, policy_engine: Optional[GovernancePolicyEngine] = None):
        self._policy_engine = policy_engine or GovernancePolicyEngine()
        self._hook_history: List[Dict[str, Any]] = []
        self._metrics_collector: Any = None

    def set_metrics_collector(self, collector: Any) -> None:
        """Link a metrics collector for automatic instrumentation."""
        self._metrics_collector = collector

    @property
    def policy_engine(self) -> GovernancePolicyEngine:
        return self._policy_engine

    def _record_governance_metrics(
        self, level: str, action_type: str, outcome: HookOutcome
    ) -> None:
        """Record governance evaluation metrics if a collector is linked."""
        if self._metrics_collector is None:
            return
        try:
            effect = outcome.result.value
            self._metrics_collector.record_governance_evaluation(
                scope=level,
                effect=effect,
                policy_id=outcome.policy_id,
            )
            self._metrics_collector.record_hook_execution(
                level=level,
                result=effect,
                duration_ms=outcome.duration_ms,
            )
        except Exception:
            pass  # Don't let metrics recording interfere with governance

    # ── Consensus Recovery Hooks ─────────────────────────────────────────

    def pre_consensus_recovery(
        self,
        consensus_id: str,
        action_type: str,
        consensus_type: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> HookOutcome:
        """Pre-hook for consensus recovery actions.

        Evaluates policies before a consensus recovery action executes.
        Returns HookOutcome indicating whether the action is allowed,
        denied, or requires approval.
        """
        start = time.time()
        specialist = (context or {}).get("participants", [None])[0] if (context or {}).get("participants") else None

        evaluation = self._policy_engine.evaluate_consensus_action(
            action_type=action_type,
            consensus_type=consensus_type,
            specialist=specialist,
        )

        outcome = self._evaluation_to_outcome(evaluation, consensus_id, start)
        self._record_hook("consensus", consensus_id, action_type, outcome)
        self._record_governance_metrics("consensus", action_type, outcome)
        return outcome

    def post_consensus_recovery(
        self,
        consensus_id: str,
        action_type: str,
        outcome_data: Dict[str, Any],
    ) -> None:
        """Post-hook for consensus recovery actions.

        Records the outcome of a consensus recovery for audit.
        """
        log.info(
            "Consensus recovery post-hook: %s — action=%s, recovered=%s",
            consensus_id[:8], action_type,
            outcome_data.get("recovered", "unknown"),
        )

    # ── Specialist Recovery Hooks ────────────────────────────────────────

    def pre_specialist_recovery(
        self,
        task_id: str,
        action_type: str,
        specialist: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> HookOutcome:
        """Pre-hook for specialist recovery actions.

        Evaluates policies before a specialist recovery action executes.
        """
        start = time.time()
        failure_type = (context or {}).get("error", None) if context else None

        evaluation = self._policy_engine.evaluate_specialist_action(
            action_type=action_type,
            specialist=specialist,
            failure_type=failure_type,
        )

        outcome = self._evaluation_to_outcome(evaluation, task_id, start)
        self._record_hook("specialist", task_id, action_type, outcome)
        self._record_governance_metrics("specialist", action_type, outcome)
        return outcome

    def post_specialist_recovery(
        self,
        task_id: str,
        action_type: str,
        outcome_data: Dict[str, Any],
    ) -> None:
        """Post-hook for specialist recovery actions.

        Records the outcome of a specialist recovery for audit.
        """
        log.info(
            "Specialist recovery post-hook: %s — action=%s, recovered=%s",
            task_id[:8], action_type,
            outcome_data.get("recovered", "unknown"),
        )

    # ── Task Recovery Hooks ──────────────────────────────────────────────

    def pre_task_recovery(
        self,
        task_id: str,
        action_type: str,
        task_trigger: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> HookOutcome:
        """Pre-hook for task recovery actions.

        Evaluates policies before a task recovery action executes.
        """
        start = time.time()
        specialist = (context or {}).get("specialist", None) if context else None
        failure_type = (context or {}).get("error_type", None) if context else None

        evaluation = self._policy_engine.evaluate_task_action(
            action_type=action_type,
            task_trigger=task_trigger,
            specialist=specialist,
            failure_type=failure_type,
        )

        outcome = self._evaluation_to_outcome(evaluation, task_id, start)
        self._record_hook("task", task_id, action_type, outcome)
        self._record_governance_metrics("task", action_type, outcome)
        return outcome

    def post_task_recovery(
        self,
        task_id: str,
        action_type: str,
        outcome_data: Dict[str, Any],
    ) -> None:
        """Post-hook for task recovery actions.

        Records the outcome of a task recovery for audit.
        """
        log.info(
            "Task recovery post-hook: %s — action=%s, recovered=%s",
            task_id[:8], action_type,
            outcome_data.get("recovered", "unknown"),
        )

    # ── Convenience: Evaluate all three levels ───────────────────────────

    def evaluate_recovery_action(
        self,
        scope: PolicyScope,
        action_type: str,
        entity_id: str,
        specialist: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> HookOutcome:
        """Evaluate a recovery action at any level using the appropriate hook.

        Args:
            scope: Which recovery level to evaluate at.
            action_type: The recovery action type.
            entity_id: The ID of the entity (consensus_id, task_id, etc.).
            specialist: Optional specialist name.
            context: Optional context dict.

        Returns:
            HookOutcome indicating whether the action is allowed.
        """
        if scope == PolicyScope.CONSENSUS:
            return self.pre_consensus_recovery(
                entity_id, action_type,
                context.get("consensus_type", "unknown") if context else "unknown",
                context,
            )
        elif scope == PolicyScope.SPECIALIST:
            return self.pre_specialist_recovery(
                entity_id, action_type,
                specialist or "unknown", context,
            )
        elif scope == PolicyScope.TASK:
            return self.pre_task_recovery(
                entity_id, action_type,
                context.get("task_trigger", "unknown") if context else "unknown",
                context,
            )
        else:
            # ALL scope — evaluate at all levels, return most restrictive
            results = []
            if context:
                results.append(self.pre_consensus_recovery(
                    entity_id, action_type,
                    context.get("consensus_type", "unknown"), context,
                ))
            results.append(self.pre_specialist_recovery(
                entity_id, action_type, specialist or "unknown", context,
            ))
            results.append(self.pre_task_recovery(
                entity_id, action_type,
                context.get("task_trigger", "unknown"), context,
            ))

            # Return the most restrictive outcome
            for r in results:
                if r.result == HookResult.DENIED:
                    return r
            for r in results:
                if r.result == HookResult.APPROVAL_PENDING:
                    return r
            return results[0] if results else HookOutcome(
                result=HookResult.ALLOWED,
                reason="No applicable policies at any level",
            )

    # ── Helpers ──────────────────────────────────────────────────────────

    def _evaluation_to_outcome(
        self,
        evaluation: PolicyEvaluation,
        entity_id: str,
        start_time: float,
    ) -> HookOutcome:
        """Convert a PolicyEvaluation to a HookOutcome."""
        duration = (time.time() - start_time) * 1000

        if evaluation.is_denied:
            return HookOutcome(
                result=HookResult.DENIED,
                reason=evaluation.reason,
                policy_id=evaluation.matching_rules[0].policy_id if evaluation.matching_rules else None,
                evaluation=evaluation,
                duration_ms=round(duration, 2),
            )
        elif evaluation.requires_approval:
            token = self._policy_engine.request_approval(evaluation, {
                "entity_id": entity_id,
            })
            return HookOutcome(
                result=HookResult.APPROVAL_PENDING,
                reason=evaluation.reason,
                policy_id=evaluation.matching_rules[0].policy_id if evaluation.matching_rules else None,
                approval_token=token,
                evaluation=evaluation,
                duration_ms=round(duration, 2),
            )
        else:
            return HookOutcome(
                result=HookResult.ALLOWED,
                reason=evaluation.reason,
                evaluation=evaluation,
                duration_ms=round(duration, 2),
            )

    def _record_hook(
        self,
        level: str,
        entity_id: str,
        action_type: str,
        outcome: HookOutcome,
    ) -> None:
        """Record a hook evaluation for audit."""
        self._hook_history.append({
            "level": level,
            "entity_id": entity_id,
            "action_type": action_type,
            "result": outcome.result.value,
            "reason": outcome.reason,
            "policy_id": outcome.policy_id,
            "duration_ms": outcome.duration_ms,
            "timestamp": time.time(),
        })

    def get_hook_history(
        self,
        level: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get hook evaluation history, optionally filtered by level."""
        results = list(self._hook_history)
        if level:
            results = [h for h in results if h["level"] == level]
        return results[-limit:]

    def get_hook_stats(self) -> Dict[str, Any]:
        """Get hook statistics."""
        total = len(self._hook_history)
        denied = sum(1 for h in self._hook_history if h["result"] == "denied")
        approved = sum(1 for h in self._hook_history if h["result"] == "approval_pending")
        allowed = sum(1 for h in self._hook_history if h["result"] == "allowed")
        return {
            "total_hooks": total,
            "denied": denied,
            "approval_pending": approved,
            "allowed": allowed,
            "by_level": {
                level: sum(1 for h in self._hook_history if h["level"] == level)
                for level in {"consensus", "specialist", "task"}
            },
        }
