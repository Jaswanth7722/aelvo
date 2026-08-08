from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field

from runtime_next.models.plan import (
    ExecutionPlan, ExecutionNode, ExecutionEdge,
    NodeState,
)


log = logging.getLogger("aelvo.cognition.replan")


class ReplanTrigger(str, Enum):
    NODE_FAILURE = "node_failure"
    BLOCKED_PATH = "blocked_path"
    GOAL_CHANGE = "goal_change"
    EVIDENCE_INVALIDATED = "evidence_invalidated"
    TIMEOUT = "timeout"
    MANUAL = "manual"
    STRATEGY_CHANGE = "strategy_change"


class ReplanAction(str, Enum):
    RETRY = "retry"
    SKIP = "skip"
    SUBSTITUTE = "substitute"
    RESTRUCTURE = "restructure"
    ABORT = "abort"
    DECOMPOSE = "decompose"


class ReplanResult(BaseModel):

    plan_id: str
    trigger: ReplanTrigger
    action: ReplanAction
    description: str = ""
    modified_node_ids: List[str] = Field(default_factory=list)
    added_node_ids: List[str] = Field(default_factory=list)
    removed_node_ids: List[str] = Field(default_factory=list)
    requires_consensus: bool = False
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))


class DynamicReplanningEngine:
    """Dynamic Replanning Engine.

    Monitors execution plan state, detects when plans become invalid (node
    failures, blocked paths, goal changes, evidence invalidation), and
    generates replan actions. Can mutate the execution graph directly.
    """

    def __init__(self):
        self._history: List[ReplanResult] = []
        self._plan_states: Dict[str, Dict[str, NodeState]] = {}

    def evaluate(
        self,
        plan: ExecutionPlan,
        trigger: ReplanTrigger,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[ReplanResult]:
        context = context or {}
        failed_node = context.get("failed_node_id")
        failure_reason = context.get("failure_reason", "")

        if trigger == ReplanTrigger.NODE_FAILURE:
            return self._handle_node_failure(plan, failed_node, failure_reason)
        elif trigger == ReplanTrigger.BLOCKED_PATH:
            return self._handle_blocked_path(plan, context)
        elif trigger == ReplanTrigger.GOAL_CHANGE:
            return self._handle_goal_change(plan, context)
        elif trigger == ReplanTrigger.EVIDENCE_INVALIDATED:
            return self._handle_evidence_invalidation(plan, context)
        elif trigger == ReplanTrigger.TIMEOUT:
            return self._handle_timeout(plan, context)
        elif trigger == ReplanTrigger.MANUAL:
            return self._handle_manual(plan, context)
        return None

    def try_retry(self, plan: ExecutionPlan, node_id: str) -> Optional[ReplanResult]:
        node = plan.get_node(node_id)
        if node is None:
            return None
        if node.retry_policy is None:
            return None
        if node.steps_consumed >= node.retry_policy.max_retries:
            return None
        result = ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.NODE_FAILURE,
            action=ReplanAction.RETRY,
            description=f"Retrying node {node_id} (attempt {node.steps_consumed + 1})",
            modified_node_ids=[node_id],
        )
        self._history.append(result)
        log.info("Replan: retry %s (attempt %d)", node_id, node.steps_consumed + 1)
        return result

    def try_substitute(
        self,
        plan: ExecutionPlan,
        node_id: str,
        substitute_node: ExecutionNode,
    ) -> Optional[ReplanResult]:
        if node_id not in plan.nodes:
            return None
        plan.nodes[node_id]

        outgoing = plan.get_outgoing_edges(node_id)
        incoming = plan.get_incoming_edges(node_id)

        plan.nodes[substitute_node.id] = substitute_node
        for edge in incoming:
            plan.add_edge(ExecutionEdge(
                id=f"e_{edge.source_node_id}->{substitute_node.id}",
                source_node_id=edge.source_node_id,
                target_node_id=substitute_node.id,
                condition=edge.condition,
                data_transformer=edge.data_transformer,
            ))
        for edge in outgoing:
            plan.add_edge(ExecutionEdge(
                id=f"e_{substitute_node.id}->{edge.target_node_id}",
                source_node_id=substitute_node.id,
                target_node_id=edge.target_node_id,
                condition=edge.condition,
                data_transformer=edge.data_transformer,
            ))

        old_plan_edges = plan.edges[:]
        plan.edges = [e for e in old_plan_edges if e.source_node_id != node_id and e.target_node_id != node_id]
        del plan.nodes[node_id]

        result = ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.NODE_FAILURE,
            action=ReplanAction.SUBSTITUTE,
            description=f"Substituted {node_id} with {substitute_node.id}",
            modified_node_ids=[substitute_node.id],
            removed_node_ids=[node_id],
        )
        self._history.append(result)
        return result

    def try_restructure(
        self,
        plan: ExecutionPlan,
        new_nodes: List[ExecutionNode],
        new_edges: List[ExecutionEdge],
        remove_node_ids: Optional[List[str]] = None,
    ) -> ReplanResult:
        remove_ids = remove_node_ids or []
        for nid in remove_ids:
            if nid in plan.nodes:
                del plan.nodes[nid]
        plan.edges = [e for e in plan.edges
                      if e.source_node_id not in remove_ids and e.target_node_id not in remove_ids]

        for node in new_nodes:
            plan.nodes[node.id] = node
        for edge in new_edges:
            existing = {(e.source_node_id, e.target_node_id) for e in plan.edges}
            if (edge.source_node_id, edge.target_node_id) not in existing:
                plan.edges.append(edge)

        plan.critical_path = plan.calculate_critical_path()
        plan.exit_node_ids = [nid for nid in plan.nodes if not plan.get_dependent_ids(nid)]
        topo = plan.topological_sort()
        if topo:
            plan.entry_node_id = topo[0]

        result = ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.STRATEGY_CHANGE,
            action=ReplanAction.RESTRUCTURE,
            description=f"Restructured plan: added {len(new_nodes)} nodes, removed {len(remove_ids)} nodes",
            added_node_ids=[n.id for n in new_nodes],
            removed_node_ids=remove_ids,
        )
        self._history.append(result)
        return result

    def try_abort(self, plan: ExecutionPlan, reason: str) -> ReplanResult:
        result = ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.NODE_FAILURE,
            action=ReplanAction.ABORT,
            description=f"Aborted: {reason}",
        )
        self._history.append(result)
        return result

    def get_history(self, plan_id: Optional[str] = None) -> List[ReplanResult]:
        if plan_id:
            return [h for h in self._history if h.plan_id == plan_id]
        return self._history

    def snapshot(self) -> Dict[str, Any]:
        return {
            "total_replans": len(self._history),
            "by_action": self._count_by_action(),
            "by_trigger": self._count_by_trigger(),
        }

    def _handle_node_failure(self, plan: ExecutionPlan, node_id: Optional[str], reason: str) -> Optional[ReplanResult]:
        if node_id is None or node_id not in plan.nodes:
            return None
        node = plan.nodes[node_id]
        if node.retry_policy and node.retry_policy.is_retryable(reason):
            if node.steps_consumed < node.retry_policy.max_retries:
                return self.try_retry(plan, node_id)
            return None
        dependents = plan.get_dependent_ids(node_id)
        if not dependents:
            result = ReplanResult(
                plan_id=plan.id,
                trigger=ReplanTrigger.NODE_FAILURE,
                action=ReplanAction.SKIP,
                description=f"Skipping failed leaf node {node_id}: {reason}",
                removed_node_ids=[node_id],
            )
            self._history.append(result)
            return result
        return None

    def _handle_blocked_path(self, plan: ExecutionPlan, context: Dict[str, Any]) -> Optional[ReplanResult]:
        blocked = context.get("blocked_path")
        if blocked is None:
            return None
        alternatives = context.get("alternatives", [])
        if alternatives:
            return self._route_via_alternative(plan, blocked, alternatives)
        return None

    def _handle_goal_change(self, plan: ExecutionPlan, context: Dict[str, Any]) -> Optional[ReplanResult]:
        new_goal = context.get("new_goal_description")
        if not new_goal:
            return None
        return ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.GOAL_CHANGE,
            action=ReplanAction.RESTRUCTURE,
            description=f"Goal changed: {new_goal[:100]}",
            modified_node_ids=list(plan.nodes.keys()) if plan.nodes else [],
        )

    def _handle_evidence_invalidation(self, plan: ExecutionPlan, context: Dict[str, Any]) -> Optional[ReplanResult]:
        invalidated_nodes = context.get("invalidated_node_ids", [])
        if invalidated_nodes:
            return ReplanResult(
                plan_id=plan.id,
                trigger=ReplanTrigger.EVIDENCE_INVALIDATED,
                action=ReplanAction.RESTRUCTURE,
                description=f"Evidence invalidated for {len(invalidated_nodes)} nodes",
                modified_node_ids=invalidated_nodes,
            )
        return None

    def _handle_timeout(self, plan: ExecutionPlan, context: Dict[str, Any]) -> Optional[ReplanResult]:
        return ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.TIMEOUT,
            action=ReplanAction.DECOMPOSE,
            description="Plan timed out — decomposing remaining work",
        )

    def _handle_manual(self, plan: ExecutionPlan, context: Dict[str, Any]) -> Optional[ReplanResult]:
        manual_action = context.get("manual_action", "retry")
        manual_description = context.get("manual_description", "User-initiated replan")
        action_map = {
            "retry": ReplanAction.RETRY,
            "skip": ReplanAction.SKIP,
            "substitute": ReplanAction.SUBSTITUTE,
            "restructure": ReplanAction.RESTRUCTURE,
            "abort": ReplanAction.ABORT,
        }
        return ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.MANUAL,
            action=action_map.get(manual_action, ReplanAction.RETRY),
            description=manual_description,
            modified_node_ids=list(plan.nodes.keys()) if plan.nodes else [],
        )

    def _route_via_alternative(
        self,
        plan: ExecutionPlan,
        blocked_step_id: str,
        alternatives: List[str],
    ) -> ReplanResult:
        result = ReplanResult(
            plan_id=plan.id,
            trigger=ReplanTrigger.BLOCKED_PATH,
            action=ReplanAction.SUBSTITUTE,
            description=f"Rerouting around {blocked_step_id} via alternatives: {alternatives}",
            modified_node_ids=[blocked_step_id],
        )
        self._history.append(result)
        return result

    def _count_by_action(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for h in self._history:
            counts[h.action.value] = counts.get(h.action.value, 0) + 1
        return counts

    def _count_by_trigger(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for h in self._history:
            counts[h.trigger.value] = counts.get(h.trigger.value, 0) + 1
        return counts
