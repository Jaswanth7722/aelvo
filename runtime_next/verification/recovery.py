"""Layer 4 â€” Recovery Strategy Engine.

Recovery is not retry. Recovery is directed graph mutation based on
classified runtime truth.

Recovery strategies are typed, observable, replayable, interruptible.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Awaitable
from datetime import datetime, timezone

from .types import (
    FailureClassification,
    RecoveryStrategy,
    RecoveryAction,
    Confidence,
    Severity,
    Retryability,
    DEFAULT_RECOVERY_MAP,
)

log = logging.getLogger("aelvo.runtime.verification.recovery")


class RecoveryStrategyEngine:
    """Manages and executes recovery strategies for classified failures.

    Features:
    - Strategy registry keyed by failure classification
    - Default strategies for all known failure types
    - Executor abstraction for running recovery actions
    - Observable recovery history
    """

    def __init__(self):
        self._strategies: Dict[FailureClassification, RecoveryStrategy] = {}
        self._executors: Dict[
            str,
            Callable[
                [RecoveryAction, Dict[str, Any]],
                Awaitable[Dict[str, Any]],
            ],
        ] = {}
        self._history: List[RecoveryAction] = []
        self._register_defaults()

    def _register_defaults(self):
        """Register default strategies for all known failure types."""
        defaults = {
            FailureClassification.SYNTAX_ERROR: RecoveryStrategy(
                id="strat_syntax_error",
                name="Reinvoke with diagnostics",
                failure_type=FailureClassification.SYNTAX_ERROR,
                description="Pass error diagnostics back to FORGE for correction",
                danger_level="safe",
                max_retries=3,
            ),
            FailureClassification.DEPENDENCY_MISSING: RecoveryStrategy(
                id="strat_dep_missing",
                name="Install missing dependency",
                failure_type=FailureClassification.DEPENDENCY_MISSING,
                description="Attempt safe installation of missing dependency",
                danger_level="reversible",
                max_retries=2,
            ),
            FailureClassification.PERMISSION_DENIED: RecoveryStrategy(
                id="strat_perm_denied",
                name="Block and notify user",
                failure_type=FailureClassification.PERMISSION_DENIED,
                description="Cannot recover autonomously â€” requires user intervention",
                danger_level="safe",
                max_retries=0,
                requires_user_approval=True,
            ),
            FailureClassification.ENVIRONMENT_FAILURE: RecoveryStrategy(
                id="strat_env_failure",
                name="Refresh capabilities",
                failure_type=FailureClassification.ENVIRONMENT_FAILURE,
                description="Refresh capability snapshot and retry",
                danger_level="safe",
                max_retries=2,
            ),
            FailureClassification.TIMEOUT: RecoveryStrategy(
                id="strat_timeout",
                name="Retry with adjusted limits",
                failure_type=FailureClassification.TIMEOUT,
                description="Retry with increased timeout or reduced scope",
                danger_level="safe",
                max_retries=2,
            ),
            FailureClassification.VERIFICATION_FAILURE: RecoveryStrategy(
                id="strat_ver_failure",
                name="Reverify with additional context",
                failure_type=FailureClassification.VERIFICATION_FAILURE,
                description="Re-run verification with expanded diagnostics",
                danger_level="safe",
                max_retries=2,
            ),
            FailureClassification.GRAPH_INCONSISTENCY: RecoveryStrategy(
                id="strat_graph_inconsistency",
                name="Rebuild graph segment",
                failure_type=FailureClassification.GRAPH_INCONSISTENCY,
                description="Rebuild the affected portion of the execution graph",
                danger_level="reversible",
                max_retries=1,
            ),
            FailureClassification.SERIALIZATION_FAILURE: RecoveryStrategy(
                id="strat_serialization",
                name="Rollback graph checkpoint",
                failure_type=FailureClassification.SERIALIZATION_FAILURE,
                description="Rollback to last known good graph checkpoint",
                danger_level="reversible",
                max_retries=1,
            ),
            FailureClassification.TOOL_FAILURE: RecoveryStrategy(
                id="strat_tool_failure",
                name="Retry with clean state",
                failure_type=FailureClassification.TOOL_FAILURE,
                description="Clear tool state and retry",
                danger_level="safe",
                max_retries=2,
            ),
            FailureClassification.STALE_RUNTIME_STATE: RecoveryStrategy(
                id="strat_stale_state",
                name="Refresh runtime state",
                failure_type=FailureClassification.STALE_RUNTIME_STATE,
                description="Refresh capability snapshot, graph state, and replay log",
                danger_level="safe",
                max_retries=1,
            ),
            FailureClassification.MUTEX_VIOLATION: RecoveryStrategy(
                id="strat_mutex",
                name="Reschedule execution",
                failure_type=FailureClassification.MUTEX_VIOLATION,
                description="Wait and reschedule with exponential backoff",
                danger_level="safe",
                max_retries=3,
            ),
            FailureClassification.REPLAY_DIVERGENCE: RecoveryStrategy(
                id="strat_replay_divergence",
                name="Abort and notify",
                failure_type=FailureClassification.REPLAY_DIVERGENCE,
                description="Replay divergence detected â€” abort execution",
                danger_level="safe",
                max_retries=0,
                requires_user_approval=True,
            ),
            FailureClassification.CAPABILITY_MISMATCH: RecoveryStrategy(
                id="strat_cap_mismatch",
                name="Refresh capabilities",
                failure_type=FailureClassification.CAPABILITY_MISMATCH,
                description="Refresh capability registry and re-evaluate",
                danger_level="safe",
                max_retries=2,
            ),
            FailureClassification.ARCHITECTURE_VIOLATION: RecoveryStrategy(
                id="strat_arch_violation",
                name="Block and notify",
                failure_type=FailureClassification.ARCHITECTURE_VIOLATION,
                description="Architecture violation detected â€” requires human review",
                danger_level="safe",
                max_retries=0,
                requires_user_approval=True,
            ),
            FailureClassification.UNKNOWN_FAILURE: RecoveryStrategy(
                id="strat_unknown",
                name="Abort and notify",
                failure_type=FailureClassification.UNKNOWN_FAILURE,
                description="Unknown failure â€” cannot recover autonomously",
                danger_level="safe",
                max_retries=0,
                requires_user_approval=True,
            ),
        }

        for cls, strategy in defaults.items():
            self._strategies[cls] = strategy

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_strategy(
        self, strategy: RecoveryStrategy
    ) -> None:
        """Register or override a recovery strategy for a failure type."""
        self._strategies[strategy.failure_type] = strategy
        log.info(
            f"Registered recovery strategy '{strategy.name}' "
            f"for {strategy.failure_type.value}"
        )

    def register_executor(
        self,
        strategy_id: str,
        executor: Callable[
            [RecoveryAction, Dict[str, Any]],
            Awaitable[Dict[str, Any]],
        ],
    ) -> None:
        """Register an executor function for a strategy."""
        self._executors[strategy_id] = executor

    # ------------------------------------------------------------------
    # Strategy execution
    # ------------------------------------------------------------------

    async def execute_recovery(
        self,
        node_id: str,
        failure_type: FailureClassification,
        classification_result: Any,
        context: Dict[str, Any],
    ) -> Optional[RecoveryAction]:
        """Execute the appropriate recovery strategy for a classified failure.

        Args:
            node_id: The failing node
            failure_type: The classified failure
            classification_result: Full classification result
            context: Runtime context for recovery execution

        Returns:
            RecoveryAction if a strategy was executed, None if none found
        """
        strategy = self._strategies.get(failure_type)
        if strategy is None:
            log.warning(
                f"No recovery strategy for {failure_type.value} "
                f"(node={node_id})"
            )
            return None

        # Check retry budget
        retry_count = context.get("retry_count", 0)
        if retry_count >= strategy.max_retries:
            log.warning(
                f"Strategy '{strategy.name}' exhausted retry budget "
                f"({retry_count}/{strategy.max_retries}) for {node_id}"
            )
            action = RecoveryAction(
                id=hashlib.sha256(
                    f"recovery_exhausted_{node_id}_{time.time()}".encode()
                ).hexdigest()[:16],
                strategy_id=strategy.id,
                node_id=node_id,
                failure_classification=failure_type,
                action_type="escalate",
                description=f"Retry budget exhausted ({retry_count}/{strategy.max_retries})",
                success=False,
                params={"retry_count": retry_count, "max_retries": strategy.max_retries},
                timestamp=datetime.now(timezone.utc),
            )
            self._history.append(action)
            return action

        # Build the action
        action = RecoveryAction(
            id=hashlib.sha256(
                f"recovery_{node_id}_{strategy.id}_{time.time()}".encode()
            ).hexdigest()[:16],
            strategy_id=strategy.id,
            node_id=node_id,
            failure_classification=failure_type,
            action_type=self._determine_action_type(strategy),
            description=f"Executing '{strategy.name}': {strategy.description}",
            params={
                "strategy_name": strategy.name,
                "failure_type": failure_type.value,
                "retry_count": retry_count,
                "max_retries": strategy.max_retries,
            },
            timestamp=datetime.now(timezone.utc),
        )

        # Execute via registered executor or default behavior
        executor = self._executors.get(strategy.id)
        if executor:
            try:
                result = await executor(action, context)
                action.success = result.get("success", False)
                action.result = result
                action.duration_ms = result.get("duration_ms", 0.0)
                if result.get("injected_node_id"):
                    action.injected_node_id = result["injected_node_id"]
            except Exception as e:
                action.success = False
                action.result = {"error": str(e)}
                log.error(
                    f"Recovery executor failed for {strategy.id}: {e}"
                )
        else:
            # No executor â€” mark as structural (injection-based)
            log.info(
                f"No executor for {strategy.id}, will be handled by injector"
            )

        self._history.append(action)
        return action

    def _determine_action_type(
        self, strategy: RecoveryStrategy
    ) -> str:
        """Determine the recovery action type from strategy properties."""
        if strategy.max_retries == 0:
            return "escalate" if strategy.requires_user_approval else "skip"
        if strategy.danger_level == "destructive":
            return "rollback"
        return "retry"

    # ------------------------------------------------------------------
    # Strategy lookup
    # ------------------------------------------------------------------

    def get_strategy(
        self, failure_type: FailureClassification
    ) -> Optional[RecoveryStrategy]:
        """Get the recovery strategy for a failure type."""
        return self._strategies.get(failure_type)

    def get_recovery_history(
        self, node_id: Optional[str] = None
    ) -> List[RecoveryAction]:
        """Get recovery history, optionally filtered by node."""
        if node_id:
            return [a for a in self._history if a.node_id == node_id]
        return list(self._history)

    @property
    def recovery_count(self) -> int:
        return len(self._history)

    @property
    def strategies(self) -> Dict[FailureClassification, RecoveryStrategy]:
        return dict(self._strategies)
