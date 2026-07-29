# runtime_next/recovery/consensus_recovery.py
# Phase 11: Consensus-level recovery strategies
# Handles: deadlocked consensus, vetoed proposals, escalated decisions, architect overrides

from __future__ import annotations

import hashlib
import logging
import time
import threading
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING
from datetime import datetime, timezone
from enum import Enum

if TYPE_CHECKING:
    from runtime_next.recovery.engine import RecoveryEngine

log = logging.getLogger("aelvo.runtime.recovery.consensus")


class ConsensusFailureType(str, Enum):
    """Types of consensus failures that can trigger recovery."""
    DEADLOCKED = "deadlocked"
    """All specialists voted but no consensus reached (tied or all disagreed)."""
    VETOED = "vetoed"
    """A specialist (typically SENTINEL) vetoed the proposal."""
    ESCALATED = "escalated"
    """Architect escalated to user for resolution."""
    PARTICIPANT_TIMEOUT = "participant_timeout"
    """A specialist failed to respond within the deadline."""
    GOVERNANCE_BLOCKED = "governance_blocked"
    """Governance kernel blocked the consensus."""
    ARCHITECT_REJECTED = "architect_rejected"
    """Architect rejected the consensus recommendation."""


class ConsensusRecoveryAction(str, Enum):
    """Recovery actions specific to consensus failures."""
    REDUCE_PARTICIPANTS = "reduce_participants"
    """Retry with a smaller set of specialists."""
    ADD_ARCHITECT = "add_architect"
    """Bring in Architect to break the tie."""
    MODIFY_PROPOSAL = "modify_proposal"
    """Retry with a modified proposal (compromise)."""
    ESCALATE_TO_USER = "escalate_to_user"
    """Escalate to user for final decision."""
    FORCE_VOTE = "force_vote"
    """Force all remaining specialists to vote with urgency."""
    USE_ARCHITECT_DECISION = "use_architect_decision"
    """Skip consensus and use Architect's authority directly."""
    BACKOFF_RETRY = "backoff_retry"
    """Wait with exponential backoff and retry the consensus."""


class ConsensusRecoveryStrategy:
    """A single recovery strategy for a consensus failure type."""

    def __init__(
        self,
        failure_type: ConsensusFailureType,
        action: ConsensusRecoveryAction,
        description: str,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
    ):
        self.failure_type = failure_type
        self.action = action
        self.description = description
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds


class ConsensusRecoveryEngine:
    """Manages recovery from consensus failures.

    Three-stage recovery for each consensus failure:
    1. **Automatic** — retry with modified parameters (fewer participants, Architect tiebreak)
    2. **Managed** — escalate to Architect for decision
    3. **Manual** — escalate to user for final resolution

    Integrates with the main RecoveryEngine via callback registration.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._history: List[Dict[str, Any]] = []
        self._attempt_counts: Dict[str, int] = {}
        self._strategies: Dict[ConsensusFailureType, List[ConsensusRecoveryStrategy]] = {}
        self._recovery_engine: Optional[RecoveryEngine] = None
        self._escalate_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
        self._governance_hooks: Any = None
        self._register_defaults()

    def link_recovery_engine(self, engine: RecoveryEngine) -> None:
        """Link to the main RecoveryEngine for coordinated recovery."""
        self._recovery_engine = engine

    def set_escalate_callback(self, callback: Callable[[str, Dict[str, Any]], None]) -> None:
        """Register a callback for user escalation when consensus cannot be recovered."""
        self._escalate_callback = callback

    def set_governance_hooks(self, hooks: Any) -> None:
        """Link governance hooks for policy enforcement on recovery actions."""
        self._governance_hooks = hooks

    def _register_defaults(self) -> None:
        """Register default recovery strategies for each consensus failure type."""
        self._strategies[ConsensusFailureType.DEADLOCKED] = [
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.DEADLOCKED,
                action=ConsensusRecoveryAction.ADD_ARCHITECT,
                description="Add Architect to break the tie with weighted vote",
                max_attempts=2,
            ),
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.DEADLOCKED,
                action=ConsensusRecoveryAction.MODIFY_PROPOSAL,
                description="Re-present the proposal with compromise wording",
                max_attempts=2,
            ),
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.DEADLOCKED,
                action=ConsensusRecoveryAction.USE_ARCHITECT_DECISION,
                description="Architect makes the final decision, bypassing consensus",
                max_attempts=1,
            ),
        ]

        self._strategies[ConsensusFailureType.VETOED] = [
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.VETOED,
                action=ConsensusRecoveryAction.REDUCE_PARTICIPANTS,
                description="Remove vetoing specialist and retry with remaining participants",
                max_attempts=2,
            ),
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.VETOED,
                action=ConsensusRecoveryAction.MODIFY_PROPOSAL,
                description="Modify proposal to address veto concerns",
                max_attempts=2,
            ),
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.VETOED,
                action=ConsensusRecoveryAction.ESCALATE_TO_USER,
                description="Escalate veto to user for resolution",
                max_attempts=1,
            ),
        ]

        self._strategies[ConsensusFailureType.ESCALATED] = [
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.ESCALATED,
                action=ConsensusRecoveryAction.USE_ARCHITECT_DECISION,
                description="Architect reviews escalation and makes binding decision",
                max_attempts=1,
            ),
        ]

        self._strategies[ConsensusFailureType.PARTICIPANT_TIMEOUT] = [
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.PARTICIPANT_TIMEOUT,
                action=ConsensusRecoveryAction.REDUCE_PARTICIPANTS,
                description="Remove unresponsive specialist and proceed with remaining",
                max_attempts=3,
                backoff_seconds=0.5,
            ),
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.PARTICIPANT_TIMEOUT,
                action=ConsensusRecoveryAction.FORCE_VOTE,
                description="Force vote with shortened deadline",
                max_attempts=2,
            ),
        ]

        self._strategies[ConsensusFailureType.GOVERNANCE_BLOCKED] = [
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.GOVERNANCE_BLOCKED,
                action=ConsensusRecoveryAction.USE_ARCHITECT_DECISION,
                description="Architect overrides governance block with documented rationale",
                max_attempts=1,
            ),
        ]

        self._strategies[ConsensusFailureType.ARCHITECT_REJECTED] = [
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.ARCHITECT_REJECTED,
                action=ConsensusRecoveryAction.MODIFY_PROPOSAL,
                description="Modify proposal based on Architect's rejection reasons",
                max_attempts=2,
            ),
            ConsensusRecoveryStrategy(
                failure_type=ConsensusFailureType.ARCHITECT_REJECTED,
                action=ConsensusRecoveryAction.ESCALATE_TO_USER,
                description="Escalate Architect rejection to user",
                max_attempts=1,
            ),
        ]

    # ── Public API ────────────────────────────────────────────────────────────

    def handle_consensus_failure(
        self,
        consensus_id: str,
        failure_type: ConsensusFailureType,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Handle a consensus failure with staged recovery.

        Args:
            consensus_id: The ID of the failed consensus event.
            failure_type: The type of consensus failure.
            context: Additional context (participants, topic, votes, etc.).

        Returns:
            Dict with keys:
            - recovered: bool — whether recovery was successful
            - action: str — the recovery action taken
            - details: str — description of what was done
            - next_steps: list — recommended next actions
        """
        ctx = context or {}
        with self._lock:
            # Increment attempt count
            key = f"{consensus_id}:{failure_type.value}"
            self._attempt_counts[key] = self._attempt_counts.get(key, 0) + 1
            attempt = self._attempt_counts[key]

            # Get strategies for this failure type
            strategies = self._strategies.get(failure_type, [])
            if not strategies:
                return {
                    "recovered": False,
                    "action": "no_strategy",
                    "details": f"No recovery strategy for {failure_type.value}",
                    "next_steps": ["escalate_to_user"],
                }

            # Try strategies in order
            for strategy in strategies:
                if attempt > strategy.max_attempts:
                    continue

                # Governance pre-hook check
                if self._governance_hooks is not None:
                    outcome = self._governance_hooks.pre_consensus_recovery(
                        consensus_id=consensus_id,
                        action_type=strategy.action.value,
                        consensus_type=failure_type.value,
                        context=ctx,
                    )
                    if outcome.result.value in ("denied", "approval_pending"):
                        log.warning(
                            "Governance blocked consensus action '%s' for %s: %s",
                            strategy.action.value, consensus_id[:8], outcome.reason,
                        )
                        # Record the skipped strategy and continue to next
                        self._history.append({
                            "consensus_id": consensus_id,
                            "failure_type": failure_type.value,
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

                result = self._execute_strategy(strategy, consensus_id, ctx)
                self._history.append({
                    "consensus_id": consensus_id,
                    "failure_type": failure_type.value,
                    "attempt": attempt,
                    "strategy": strategy.action.value,
                    "result": result,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })

                if result.get("recovered", False):
                    return result

            # All strategies exhausted — escalate
            return self._escalate(consensus_id, failure_type, ctx)

    def get_history(
        self,
        consensus_id: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Get recovery history, optionally filtered by consensus_id."""
        with self._lock:
            results = list(self._history)
            if consensus_id:
                results = [h for h in results if h["consensus_id"] == consensus_id]
            return results[-limit:]

    def reset(self) -> None:
        """Reset all state — used for testing and session boundaries."""
        with self._lock:
            self._history.clear()
            self._attempt_counts.clear()

    @property
    def recovery_count(self) -> int:
        return len(self._history)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _execute_strategy(
        self,
        strategy: ConsensusRecoveryStrategy,
        consensus_id: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a single recovery strategy."""
        action = strategy.action

        if action == ConsensusRecoveryAction.BACKOFF_RETRY:
            # Backoff is a valid recovery — the consensus will be retried after delay
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Backoff retry ({strategy.backoff_seconds}s delay)",
                "backoff_seconds": strategy.backoff_seconds,
                "next_steps": ["wait_backoff", "retry_consensus"],
            }

        if action == ConsensusRecoveryAction.REDUCE_PARTICIPANTS:
            participants = context.get("participants", [])
            unresponsive = context.get("unresponsive_participants", [])
            remaining = [p for p in participants if p not in unresponsive]
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Reduced from {len(participants)} to {len(remaining)} participants",
                "remaining_participants": remaining,
                "next_steps": ["retry_consensus"],
            }

        if action == ConsensusRecoveryAction.ADD_ARCHITECT:
            participants = context.get("participants", [])
            if "ARCHITECT" not in participants:
                participants = participants + ["ARCHITECT"]
            return {
                "recovered": True,
                "action": action.value,
                "details": "Added ARCHITECT to break consensus tie",
                "remaining_participants": participants,
                "next_steps": ["retry_consensus"],
            }

        if action == ConsensusRecoveryAction.MODIFY_PROPOSAL:
            topic = context.get("topic", "")
            veto_reason = context.get("veto_reason", "")
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Proposal modification needed: {veto_reason[:100]}"
                           if veto_reason else "Generic proposal modification",
                "original_topic": topic,
                "next_steps": ["modify_proposal", "retry_consensus"],
            }

        if action == ConsensusRecoveryAction.FORCE_VOTE:
            remaining = context.get("participants", [])
            return {
                "recovered": True,
                "action": action.value,
                "details": f"Forcing vote from {len(remaining)} remaining participants",
                "remaining_participants": remaining,
                "next_steps": ["force_vote", "retry_consensus"],
            }

        if action == ConsensusRecoveryAction.USE_ARCHITECT_DECISION:
            return {
                "recovered": True,
                "action": action.value,
                "details": "Architect makes binding decision, bypassing consensus",
                "next_steps": ["architect_decides"],
            }

        if action == ConsensusRecoveryAction.ESCALATE_TO_USER:
            return self._escalate(consensus_id, strategy.failure_type, context)

        return {
            "recovered": False,
            "action": "unknown",
            "details": f"Unknown recovery action: {action.value}",
            "next_steps": ["escalate_to_user"],
        }

    def _escalate(
        self,
        consensus_id: str,
        failure_type: ConsensusFailureType,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Escalate to user when all recovery strategies are exhausted."""
        result = {
            "recovered": False,
            "action": ConsensusRecoveryAction.ESCALATE_TO_USER.value,
            "details": (
                f"Consensus {consensus_id[:8]} failed with {failure_type.value} "
                f"after all recovery strategies exhausted"
            ),
            "next_steps": ["user_resolution"],
            "consensus_id": consensus_id,
            "failure_type": failure_type.value,
            "context": context,
        }

        if self._escalate_callback:
            try:
                self._escalate_callback(consensus_id, result)
            except Exception as e:
                log.warning("Escalation callback failed: %s", e)

        if self._recovery_engine:
            log.warning(
                "Consensus recovery escalation: %s (%s)",
                consensus_id[:8], failure_type.value,
            )

        return result

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
