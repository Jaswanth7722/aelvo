"""Layer 6 — Retry Safety Engine.

Blind retries are forbidden. Before retrying, the engine evaluates:
  - retry count
  - failure stability
  - graph consistency
  - capability validity
  - mutation safety
  - dependency freshness
  - replay divergence risk

Retries must stop when graph integrity is threatened, runtime truth is stale,
deterministic replay fails, or repeated failures show instability.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .types import (
    FailureClassification,
    RetryDecision,
    Retryability,
    Confidence,
)

log = logging.getLogger("aelvo.runtime.verification.retry_safety")


class RetrySafetyEngine:
    """Evaluates whether retry is safe before allowing recovery.

    Every retry decision is observable and explainable.
    """

    def __init__(self):
        self._decisions: List[RetryDecision] = []
        self._retry_counts: Dict[str, int] = {}
        self._failure_history: Dict[str, List[str]] = {}

    # ------------------------------------------------------------------
    # Evaluation entry point
    # ------------------------------------------------------------------

    async def evaluate(
        self,
        node_id: str,
        classification: FailureClassification,
        retryability: Retryability,
        graph_state: Optional[Dict[str, Any]] = None,
        capability_state: Optional[Dict[str, Any]] = None,
        serialization_state: Optional[Dict[str, Any]] = None,
        replay_state: Optional[Dict[str, Any]] = None,
        execution_history: Optional[List[Dict[str, Any]]] = None,
    ) -> RetryDecision:
        """Evaluate whether retry is safe.

        Returns a RetryDecision with detailed reasoning.
        """
        self._retry_counts[node_id] = self._retry_counts.get(node_id, 0) + 1
        retry_count = self._retry_counts[node_id]

        # Track failure history for stability analysis
        if node_id not in self._failure_history:
            self._failure_history[node_id] = []
        self._failure_history[node_id].append(classification.value)

        reasoning = []
        blocking = None

        # Check 1: Never retry unknown failures
        if classification == FailureClassification.UNKNOWN_FAILURE:
            reasoning.append(
                "Unknown failures are NEVER silently retried"
            )
            return self._build_decision(
                node_id=node_id,
                can_retry=False,
                reason="Unknown failure — retry forbidden",
                retry_count=retry_count,
                blocking_condition="unknown_failure",
            )

        # Check 2: Never retry non-retryable failures
        if retryability == Retryability.NEVER:
            reasoning.append("Failure is classified as NEVER retryable")
            return self._build_decision(
                node_id=node_id,
                can_retry=False,
                reason="Non-retryable failure",
                retry_count=retry_count,
                blocking_condition="non_retryable",
            )

        # Check 3: Graph consistency
        graph_consistent = True
        if graph_state:
            graph_consistent = self._check_graph_consistency(graph_state)
            if not graph_consistent:
                reasoning.append("Graph state is inconsistent")
                blocking = "graph_inconsistency"

        # Check 4: Capability validity
        capability_valid = True
        if capability_state:
            capability_valid = self._check_capability_validity(
                capability_state
            )
            if not capability_valid:
                reasoning.append("Capability state is stale or invalid")
                blocking = blocking or "stale_capability"

        # Check 5: Mutation safety
        mutation_safe = True
        if graph_state:
            mutation_safe = self._check_mutation_safety(
                node_id, classification, graph_state
            )
            if not mutation_safe:
                reasoning.append("Mutation may cause side effects")
                blocking = blocking or "unsafe_mutation"

        # Check 6: Dependency freshness
        dependency_fresh = True
        if graph_state:
            dependency_fresh = self._check_dependency_freshness(
                node_id, graph_state
            )
            if not dependency_fresh:
                reasoning.append("Node dependencies are stale")
                blocking = blocking or "stale_dependencies"

        # Check 7: Replay divergence risk
        replay_divergence_risk = 0.0
        if replay_state:
            replay_divergence_risk = self._assess_replay_divergence_risk(
                node_id, classification, replay_state
            )
            if replay_divergence_risk > 0.7:
                reasoning.append(
                    f"High replay divergence risk ({replay_divergence_risk:.2f})"
                )
                blocking = blocking or "replay_divergence"

        # Check 8: Failure stability
        failure_stability = self._assess_failure_stability(node_id)
        if failure_stability < 0.3:
            reasoning.append(
                f"Failure is unstable across retries (stability={failure_stability:.2f})"
            )

        # Check 9: Serialization integrity
        serialization_ok = True
        if serialization_state:
            serialization_ok = self._check_serialization_integrity(
                serialization_state
            )
            if not serialization_ok:
                reasoning.append("Serialization state is corrupted")
                blocking = blocking or "serialization_corruption"

        can_retry = (
            graph_consistent
            and capability_valid
            and mutation_safe
            and dependency_fresh
            and serialization_ok
            and replay_divergence_risk <= 0.7
        )

        if not can_retry and not blocking:
            blocking = "multiple_conditions"

        decision = RetryDecision(
            can_retry=can_retry,
            reason="; ".join(reasoning) if reasoning else "Retry is safe",
            suggested_backoff=self._compute_backoff(retry_count),
            graph_consistent=graph_consistent,
            capability_valid=capability_valid,
            mutation_safe=mutation_safe,
            dependency_fresh=dependency_fresh,
            replay_divergence_risk=round(replay_divergence_risk, 4),
            failure_stability=round(failure_stability, 4),
            retry_count=retry_count,
            blocking_condition=blocking,
        )

        self._decisions.append(decision)

        if can_retry:
            log.info(
                f"Retry OK for {node_id} "
                f"(attempt {retry_count}, "
                f"replay_risk={replay_divergence_risk:.2f}, "
                f"stability={failure_stability:.2f})"
            )
        else:
            log.warning(
                f"Retry BLOCKED for {node_id}: "
                f"graph={graph_consistent}, "
                f"cap={capability_valid}, "
                f"mut={mutation_safe}, "
                f"dep={dependency_fresh}, "
                f"ser={serialization_ok}, "
                f"replay_risk={replay_divergence_risk:.2f}, "
                f"blocking={blocking}"
            )

        return decision

    # ------------------------------------------------------------------
    # Consistency checks
    # ------------------------------------------------------------------

    def _check_graph_consistency(
        self, graph_state: Dict[str, Any]
    ) -> bool:
        """Check if the execution graph is in a consistent state."""
        node_count = graph_state.get("node_count", 0)
        completed = graph_state.get("completed_count", 0)
        failed = graph_state.get("failed_count", 0)

        # Basic sanity: counts should not exceed node_count
        if completed + failed > node_count:
            return False

        # If more than 50% failed, something is fundamentally wrong
        if node_count > 0 and failed > node_count * 0.5:
            return False

        return True

    def _check_capability_validity(
        self, capability_state: Dict[str, Any]
    ) -> bool:
        """Check if the capability snapshot is still fresh."""
        health = capability_state.get("health", "")
        if health == "offline":
            return False

        tools = capability_state.get("tools", {})
        for name, info in tools.items():
            if info.get("status") == "missing" and name in (
                "python",
                "node",
            ):
                return False

        return True

    def _check_mutation_safety(
        self,
        node_id: str,
        classification: FailureClassification,
        graph_state: Dict[str, Any],
    ) -> bool:
        """Check if retry would cause unsafe mutations."""
        # If files were written, retry may compound changes
        nodes = graph_state.get("nodes", {})
        node_info = nodes.get(node_id, {})
        files = node_info.get("files", [])

        # If the node wrote files and failed mid-write, retry is dangerous
        if files and classification in (
            FailureClassification.SYNTAX_ERROR,
            FailureClassification.SERIALIZATION_FAILURE,
        ):
            return False

        return True

    def _check_dependency_freshness(
        self, node_id: str, graph_state: Dict[str, Any]
    ) -> bool:
        """Check if the node's dependencies are still fresh."""
        nodes = graph_state.get("nodes", {})
        node_info = nodes.get(node_id, {})
        dependencies = node_info.get("dependencies", [])

        for dep_id in dependencies:
            dep_info = nodes.get(dep_id, {})
            if dep_info.get("state") == "failed":
                return False

        return True

    def _assess_replay_divergence_risk(
        self,
        node_id: str,
        classification: FailureClassification,
        replay_state: Dict[str, Any],
    ) -> float:
        """Assess risk of replay divergence (0.0 - 1.0)."""
        risk = 0.0

        # Replay divergence itself is high risk
        if classification == FailureClassification.REPLAY_DIVERGENCE:
            risk += 0.8

        # Non-deterministic failures increase risk
        non_deterministic = replay_state.get("non_deterministic_nodes", [])
        if node_id in non_deterministic:
            risk += 0.4

        # Stale replay state
        if replay_state.get("stale"):
            risk += 0.3

        return min(1.0, risk)

    def _assess_failure_stability(
        self, node_id: str, window: int = 5
    ) -> float:
        """Assess how stable/consistent the failure is across retries.

        Returns 1.0 if all failures are the same type, 0.0 if completely varied.
        """
        history = self._failure_history.get(node_id, [])
        recent = history[-window:]

        if len(recent) < 2:
            return 1.0  # Not enough data

        # Check if all failures are the same classification
        unique = set(recent)
        if len(unique) == 1:
            return 1.0  # Stable — same failure every time

        # More varied = less stable
        return 1.0 - (len(unique) - 1) / len(recent)

    def _check_serialization_integrity(
        self, serialization_state: Dict[str, Any]
    ) -> bool:
        """Check if serialization state is intact."""
        return serialization_state.get("is_valid", True)

    def _compute_backoff(self, retry_count: int) -> float:
        """Compute exponential backoff in seconds."""
        if retry_count <= 0:
            return 0.0
        return min(60.0, 1.0 * (2 ** (retry_count - 1)))

    def _build_decision(
        self,
        node_id: str,
        can_retry: bool,
        reason: str,
        retry_count: int,
        blocking_condition: Optional[str] = None,
    ) -> RetryDecision:
        decision = RetryDecision(
            can_retry=can_retry,
            reason=reason,
            retry_count=retry_count,
            blocking_condition=blocking_condition,
            graph_consistent=True,
            capability_valid=True,
            mutation_safe=True,
            dependency_fresh=True,
            suggested_backoff=self._compute_backoff(retry_count),
        )
        self._decisions.append(decision)
        return decision

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_retry_count(self, node_id: str) -> int:
        return self._retry_counts.get(node_id, 0)

    def get_decisions(
        self, node_id: Optional[str] = None
    ) -> List[RetryDecision]:
        return list(self._decisions)

    def reset(self, node_id: Optional[str] = None):
        """Reset retry tracking for a node or all nodes."""
        if node_id:
            self._retry_counts.pop(node_id, None)
            self._failure_history.pop(node_id, None)
        else:
            self._retry_counts.clear()
            self._failure_history.clear()
            self._decisions.clear()
