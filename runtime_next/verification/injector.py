"""Layer 5 — Recovery Node Injection.

Recovery actions become execution graph nodes — not hidden runtime behavior.

This is CRITICAL. Every recovery:
  - appears in graph state
  - emits events
  - can fail
  - can retry
  - can be paused
  - can be replayed

Recovery is part of execution history.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from .types import (
    RecoveryAction,
    RecoveryStrategy,
)

log = logging.getLogger("aelvo.runtime.verification.injector")


class RecoveryNodeInjector:
    """Injects recovery operations as explicit graph nodes.

    Works with any compatible graph that supports:
    - add_node(node_id, properties)
    - add_edge(source, target)
    - transition_node(node_id, state, reason)
    """

    def __init__(self):
        self._injected_nodes: Dict[str, Dict[str, Any]] = {}

    async def inject_recovery_node(
        self,
        action: RecoveryAction,
        strategy: RecoveryStrategy,
        graph: Any,
        context: Dict[str, Any],
    ) -> Optional[str]:
        """Inject a recovery node into the execution graph.

        The injected node appears explicitly in graph state and history.

        Args:
            action: The recovery action to inject
            strategy: The strategy being executed
            graph: The execution graph (must support add_node/add_edge/transition_node)
            context: Runtime context

        Returns:
            The ID of the injected node, or None if injection failed
        """
        inject_id = f"recover_{action.node_id}_{int(time.time())}"

        # 1. Create the recovery node
        node_properties = {
            "id": inject_id,
            "description": f"Recovery: {strategy.description}",
            "node_type": "recovery",
            "specialist": "VERIFICATION",
            "danger": strategy.danger_level,
            "files": [],
            "history": [],
            "state": "pending",
            "retry_count": 0,
            "retry_budget": strategy.max_retries,
            "metadata": {
                "recovery_action_id": action.id,
                "original_node_id": action.node_id,
                "strategy_id": strategy.id,
                "failure_classification": action.failure_classification.value,
                "is_recovery_node": True,
            },
        }

        try:
            if hasattr(graph, "inject_node"):
                # Legacy ExecutionGraph API
                result = graph.inject_node(
                    node_properties,
                    dependencies=[action.node_id],
                )
                if asyncio.iscoroutine(result):
                    await result
                log.info(
                    f"Injected recovery node {inject_id} "
                    f"into graph (legacy API)"
                )
            elif hasattr(graph, "add_node") and hasattr(graph, "add_edge"):
                # Generic graph API
                result = graph.add_node(inject_id, node_properties)
                if asyncio.iscoroutine(result):
                    await result
                result = graph.add_edge(action.node_id, inject_id)
                if asyncio.iscoroutine(result):
                    await result
                log.info(
                    f"Injected recovery node {inject_id} into graph"
                )
            else:
                log.warning(
                    f"Graph does not support node injection "
                    f"for {inject_id}"
                )
                return None

            # 2. Transition to ready
            if hasattr(graph, "transition_node"):
                transition = graph.transition_node(
                    inject_id,
                    "pending",
                    reason=f"Injected for recovery: {strategy.description}",
                )
                # CA6 fix: use asyncio.iscoroutine() instead of fragile
                # hasattr(transition, '__await__') duck detection.
                if asyncio.iscoroutine(transition):
                    await transition

            self._injected_nodes[inject_id] = {
                "action_id": action.id,
                "original_node_id": action.node_id,
                "strategy_id": strategy.id,
                "failure_type": action.failure_classification.value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "node_properties": node_properties,
            }

            return inject_id

        except Exception as e:
            log.error(
                f"Failed to inject recovery node {inject_id}: {e}"
            )
            return None

    async def inject_rollback_node(
        self,
        plan_id: str,
        reason: str,
        checkpoint_path: str,
        nodes_affected: List[str],
        graph: Any,
    ) -> Optional[str]:
        """Inject a rollback node that restores graph to a checkpoint."""
        rollback_id = f"rollback_{plan_id}_{int(time.time())}"

        node_properties = {
            "id": rollback_id,
            "description": f"Graph rollback: {reason}",
            "node_type": "recovery",
            "specialist": "VERIFICATION",
            "danger": "reversible",
            "metadata": {
                "is_rollback_node": True,
                "checkpoint_path": checkpoint_path,
                "nodes_affected": nodes_affected,
                "plan_id": plan_id,
            },
        }

        try:
            if hasattr(graph, "add_node"):
                result = graph.add_node(rollback_id, node_properties)
                if asyncio.iscoroutine(result):
                    await result
                for nid in nodes_affected:
                    if hasattr(graph, "add_edge"):
                        result = graph.add_edge(rollback_id, nid)
                        if asyncio.iscoroutine(result):
                            await result

            log.info(
                f"Injected rollback node {rollback_id} "
                f"for plan {plan_id}"
            )
            return rollback_id

        except Exception as e:
            log.error(
                f"Failed to inject rollback node {rollback_id}: {e}"
            )
            return None

    @property
    def injected_nodes(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._injected_nodes)

    def get_injections_for_node(
        self, node_id: str
    ) -> List[Dict[str, Any]]:
        """Get all recovery injections for a specific node."""
        return [
            info
            for info in self._injected_nodes.values()
            if info["original_node_id"] == node_id
        ]

    def clear(self):
        self._injected_nodes.clear()
