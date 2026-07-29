"""Layer 10 — Autonomous Recovery Governance.

AELVO must know when autonomy should stop.

Not every failure should trigger autonomous recovery. The governance layer
decides:
  - safe autonomous recovery
  - approval-required recovery
  - abort execution
  - request user intervention

Dangerous recovery actions require confirmation.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from .types import (
    FailureClassification,
    GovernanceDecision,
    RecoveryStrategy,
    Confidence,
)

log = logging.getLogger("aelvo.runtime.verification.governance")


class RecoveryGovernance:
    """Governs autonomous recovery decisions.

    Determines whether AELVO can proceed autonomously or needs
    human intervention.
    """

    def __init__(self):
        self._decisions: List[GovernanceDecision] = []
        self._user_approval_pending: Set[str] = set()
        self._danger_levels: Dict[str, str] = {
            # action_type -> danger level
            "retry": "safe",
            "inject_node": "reversible",
            "escalate": "safe",
            "rollback": "destructive",
            "skip": "safe",
        }

    async def decide(
        self,
        failure_type: FailureClassification,
        strategy: RecoveryStrategy,
        action_type: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> GovernanceDecision:
        """Decide whether to proceed with autonomous recovery.

        Args:
            failure_type: The classified failure
            strategy: The recovery strategy to use
            action_type: Type of recovery action
            context: Additional context for decision

        Returns:
            GovernanceDecision with verdict and reasoning
        """
        ctx = context or {}

        # 1. Check if action type is known
        danger_level = self._danger_levels.get(
            action_type, "destructive"
        )

        # 2. Check strategy-level danger
        if strategy.danger_level == "destructive":
            danger_level = "destructive"
        elif strategy.danger_level == "approval_required":
            danger_level = "approval_required"

        # 3. Check retry budget
        retry_count = ctx.get("retry_count", 0)
        max_retries = strategy.max_retries
        budget_exhausted = retry_count >= max_retries

        # 4. Check for unknown failures
        if failure_type == FailureClassification.UNKNOWN_FAILURE:
            return self._build_decision(
                verdict="abort",
                reason=(
                    "Unknown failure classification. "
                    "Autonomous recovery is not safe."
                ),
                danger_assessment="destructive",
                requires_user_intervention=True,
                suggested_message=(
                    "I encountered an unknown failure and cannot "
                    "safely recover. Here's what happened:\n"
                    f"Failure: {ctx.get('error_message', 'Unknown')}\n"
                    "Please review and decide how to proceed."
                ),
            )

        # 5. Check for destructive actions
        if danger_level == "destructive":
            return self._build_decision(
                verdict="require_approval",
                reason=(
                    f"Recovery action '{action_type}' is destructive "
                    f"(strategy: {strategy.name}). "
                    f"Requires user confirmation."
                ),
                danger_assessment="destructive",
                requires_user_intervention=True,
                suggested_message=(
                    f"I need to perform a destructive recovery action:\n"
                    f"  Strategy: {strategy.name}\n"
                    f"  Action: {action_type}\n"
                    f"  Reason: {strategy.description}\n\n"
                    f"Should I proceed with this recovery?"
                ),
            )

        # 6. Check for approval-required strategies
        if strategy.requires_user_approval:
            return self._build_decision(
                verdict="require_approval",
                reason=(
                    f"Strategy '{strategy.name}' requires user approval "
                    f"before execution."
                ),
                danger_assessment=danger_level,
                requires_user_intervention=True,
                suggested_message=(
                    f"I need approval to execute recovery:\n"
                    f"  Strategy: {strategy.name}\n"
                    f"  Reason: {strategy.description}\n\n"
                    f"May I proceed?"
                ),
            )

        # 7. Check budget exhaustion
        if budget_exhausted:
            return self._build_decision(
                verdict="notify_user",
                reason=(
                    f"Retry budget exhausted "
                    f"({retry_count}/{max_retries}). "
                    f"Cannot continue autonomous recovery."
                ),
                danger_assessment="reversible",
                requires_user_intervention=True,
                suggested_message=(
                    f"I've exhausted retry attempts "
                    f"({retry_count}/{max_retries}) for: "
                    f"'{ctx.get('node_description', 'unknown task')}'. "
                    f"The failure persists. Would you like me to try "
                    f"a different approach?"
                ),
            )

        # 8. Check for permission denied (always escalate)
        if failure_type == FailureClassification.PERMISSION_DENIED:
            return self._build_decision(
                verdict="notify_user",
                reason="Permission denied — requires user intervention.",
                danger_assessment="safe",
                requires_user_intervention=True,
                suggested_message=(
                    f"Permission was denied while trying to: "
                    f"'{ctx.get('node_description', 'unknown task')}'. "
                    f"I cannot proceed without the necessary permissions."
                ),
            )

        # 9. Check for architecture violations (always escalate)
        if (
            failure_type
            == FailureClassification.ARCHITECTURE_VIOLATION
        ):
            return self._build_decision(
                verdict="notify_user",
                reason=(
                    "Architecture violation detected — "
                    "requires human review."
                ),
                danger_assessment="reversible",
                requires_user_intervention=True,
                suggested_message=(
                    "An architecture violation was detected. "
                    "This requires your review to determine "
                    "the best course of action."
                ),
            )

        # 10. Safe autonomous recovery
        return self._build_decision(
            verdict="auto_recover",
            reason=(
                f"Safe autonomous recovery using "
                f"'{strategy.name}' "
                f"({danger_level}, attempt {retry_count + 1}/{max_retries})"
            ),
            danger_assessment=danger_level,
            requires_user_intervention=False,
        )

    def _build_decision(
        self,
        verdict: str,
        reason: str,
        danger_assessment: str = "safe",
        requires_user_intervention: bool = False,
        suggested_message: Optional[str] = None,
    ) -> GovernanceDecision:
        decision = GovernanceDecision(
            verdict=verdict,
            reason=reason,
            confidence=(
                Confidence.HIGH
                if verdict == "auto_recover"
                else Confidence.CERTAIN
            ),
            danger_assessment=danger_assessment,
            requires_user_intervention=requires_user_intervention,
            suggested_message=suggested_message,
        )
        self._decisions.append(decision)
        return decision

    # ------------------------------------------------------------------
    # Approval management
    # ------------------------------------------------------------------

    def mark_approval_pending(self, decision_id: str):
        """Mark a decision as awaiting user approval."""
        self._user_approval_pending.add(decision_id)

    def approve(self, decision_id: str) -> bool:
        """Approve a pending decision."""
        if decision_id in self._user_approval_pending:
            self._user_approval_pending.discard(decision_id)
            return True
        return False

    def reject(self, decision_id: str) -> bool:
        """Reject a pending decision."""
        if decision_id in self._user_approval_pending:
            self._user_approval_pending.discard(decision_id)
            return True
        return False

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def pending_approvals(self) -> List[GovernanceDecision]:
        """Get all decisions awaiting approval."""
        return [
            d
            for d in self._decisions
            if d.requires_user_intervention
        ]

    @property
    def decisions(self) -> List[GovernanceDecision]:
        return list(self._decisions)

    @property
    def auto_recovery_count(self) -> int:
        return sum(
            1 for d in self._decisions if d.verdict == "auto_recover"
        )

    @property
    def intervention_count(self) -> int:
        return sum(
            1
            for d in self._decisions
            if d.requires_user_intervention
        )
