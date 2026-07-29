# planning/plan_evolution.py - Plan Evolution Engine for AELVO OMEGA
"""
The Plan Evolution Engine manages conservative plan revisions.

Plans must evolve when reality diverges from assumptions. But they must
not thrash — plan instability is itself a planning failure. This engine
applies the four-trigger model: only four kinds of events justify plan
revision. All revisions are recorded in the node's revision_history so
the system learns from its own planning mistakes.

The Four Triggers:
1. VERIFICATION_FAILURE: A test, security scan, or type check contradicted
   a planning assumption about a milestone.
2. CAPABILITY_DISCOVERY: A new tool, library, or pattern makes something
   the plan assumed would be hard trivially easy.
3. RESOURCE_CONSTRAINT: A planned approach is now infeasible (rate limit,
   access blocked, dependency yanked).
4. USER_DIRECTIVE: The user explicitly changed strategic direction.

Only USER_DIRECTIVE can change what to build. The other three triggers
change how to build what was already committed to.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from planning.memory_types import (
    HierarchyLevel,
    PlanNodeState,
    RevisionRecord,
)
from planning.goal_hierarchy import GoalHierarchyEngine

log = logging.getLogger("aelvo.planning.evolution")


class EvolutionTrigger(str, Enum):
    """The four trigger types that justify plan revision."""
    VERIFICATION_FAILURE = "verification_failure"
    CAPABILITY_DISCOVERY = "capability_discovery"
    RESOURCE_CONSTRAINT = "resource_constraint"
    USER_DIRECTIVE = "user_directive"


class RevisionScope(str, Enum):
    """How much of the hierarchy a revision affects."""
    TASK_ONLY = "task_only"             # Single task, no structural change
    MILESTONE = "milestone"             # One milestone and its tasks
    INITIATIVE = "initiative"           # One initiative and all children
    OBJECTIVE = "objective"             # Whole objective subtree
    MISSION = "mission"                 # Mission statement revision


class PlanRevisionResult:
    """Return type from a revision operation."""

    def __init__(
        self,
        success: bool,
        scope: RevisionScope,
        revised_nodes: List[str],
        revision_records: List[RevisionRecord],
        rejection_reason: str = "",
    ):
        self.success = success
        self.scope = scope
        self.revised_nodes = revised_nodes
        self.revision_records = revision_records
        self.rejection_reason = rejection_reason

    def __repr__(self) -> str:
        return (
            f"PlanRevisionResult(success={self.success}, scope={self.scope.value}, "
            f"revised={len(self.revised_nodes)} nodes)"
        )


class PlanEvolutionEngine:
    """Conservative plan revision engine.

    Implements four-trigger evolution with scope-gating: the scope of a
    revision is never wider than the trigger justifies. A verification
    failure in a single task cannot revise the Strategic Objective — it can
    only revise the milestone and below.

    All revisions are recorded to the node's revision_history. This is how
    the system builds institutional memory of what kinds of plans succeed.
    """

    # Confidence thresholds
    FAILURE_CONFIDENCE_PENALTY = 0.10   # Applied to milestone on verification failure
    DISCOVERY_CONFIDENCE_BOOST = 0.08   # Applied when capability discovered
    CONSTRAINT_CONFIDENCE_PENALTY = 0.15  # Applied when resource constraint found

    # Scope gates: which triggers allow which revision scopes
    _SCOPE_GATES: Dict[EvolutionTrigger, List[RevisionScope]] = {
        EvolutionTrigger.VERIFICATION_FAILURE: [
            RevisionScope.TASK_ONLY,
            RevisionScope.MILESTONE,
        ],
        EvolutionTrigger.CAPABILITY_DISCOVERY: [
            RevisionScope.TASK_ONLY,
            RevisionScope.MILESTONE,
            RevisionScope.INITIATIVE,
        ],
        EvolutionTrigger.RESOURCE_CONSTRAINT: [
            RevisionScope.TASK_ONLY,
            RevisionScope.MILESTONE,
            RevisionScope.INITIATIVE,
        ],
        EvolutionTrigger.USER_DIRECTIVE: [
            RevisionScope.TASK_ONLY,
            RevisionScope.MILESTONE,
            RevisionScope.INITIATIVE,
            RevisionScope.OBJECTIVE,
            RevisionScope.MISSION,
        ],
    }

    def __init__(self, hierarchy: GoalHierarchyEngine):
        self.hierarchy = hierarchy
        self._revision_count = 0
        self._trigger_counts: Dict[str, int] = {t.value: 0 for t in EvolutionTrigger}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_verification_failure(
        self,
        milestone_id: str,
        check_name: str,
        failure_summary: str,
        failed_task_ids: Optional[List[str]] = None,
        event_memory_id: Optional[str] = None,
    ) -> PlanRevisionResult:
        """Process a verification failure event.

        A verification failure means a planned assumption was wrong. The
        milestone's confidence is penalized. Any tasks whose success
        criteria depended on the failed check are re-proposed for rework.
        """
        node = self.hierarchy.get_node(milestone_id)
        if not node:
            return PlanRevisionResult(
                success=False,
                scope=RevisionScope.MILESTONE,
                revised_nodes=[],
                revision_records=[],
                rejection_reason=f"Milestone {milestone_id} not found in hierarchy",
            )

        revised_nodes = []
        records = []

        # 1. Apply confidence penalty to milestone
        old_confidence = node.confidence
        new_confidence = max(0.0, old_confidence - self.FAILURE_CONFIDENCE_PENALTY)
        ok = self.hierarchy.update_confidence(
            node_id=milestone_id,
            new_confidence=new_confidence,
            rationale=f"Verification failure: {check_name} — {failure_summary[:200]}",
            trigger_type=EvolutionTrigger.VERIFICATION_FAILURE.value,
        )
        if ok:
            revised_nodes.append(milestone_id)
            if node.revision_history:
                records.append(node.revision_history[-1])

        # 2. Re-propose failed tasks for rework
        for task_id in (failed_task_ids or []):
            task = self.hierarchy.get_node(task_id)
            if task and task.state == PlanNodeState.COMPLETE:
                ok = self.hierarchy.update_node_state(
                    node_id=task_id,
                    new_state=PlanNodeState.PROPOSED,
                    trigger_summary=f"Verification failure requires rework: {check_name}",
                    trigger_type=EvolutionTrigger.VERIFICATION_FAILURE.value,
                )
                if ok:
                    revised_nodes.append(task_id)
                    if task.revision_history:
                        records.append(task.revision_history[-1])

        # 3. Check if verification strategy specifies a recovery path
        recovery_path = ""
        if node.verification_strategy and node.verification_strategy.on_failure_recovery_path:
            recovery_path = node.verification_strategy.on_failure_recovery_path

        self._revision_count += 1
        self._trigger_counts[EvolutionTrigger.VERIFICATION_FAILURE.value] += 1

        log.info(
            "Verification failure: milestone '%s', confidence %.2f → %.2f, "
            "revised %d nodes, recovery='%s'",
            node.title, old_confidence, new_confidence,
            len(revised_nodes), recovery_path[:60],
        )

        return PlanRevisionResult(
            success=True,
            scope=RevisionScope.MILESTONE,
            revised_nodes=revised_nodes,
            revision_records=records,
        )

    def process_capability_discovery(
        self,
        affected_milestone_ids: List[str],
        capability_description: str,
        simplification_summary: str,
        event_memory_id: Optional[str] = None,
    ) -> PlanRevisionResult:
        """Process a capability discovery event.

        A capability discovery means an approach can be simplified. The
        milestone's confidence is boosted. If any tasks in the milestone
        are no longer needed, they can be deferred.
        """
        if not self._validate_scope(EvolutionTrigger.CAPABILITY_DISCOVERY, RevisionScope.MILESTONE):
            return PlanRevisionResult(
                success=False,
                scope=RevisionScope.MILESTONE,
                revised_nodes=[],
                revision_records=[],
                rejection_reason="Scope gate violation — capability discovery cannot exceed milestone scope",
            )

        revised_nodes = []
        records = []

        for ms_id in affected_milestone_ids:
            node = self.hierarchy.get_node(ms_id)
            if not node:
                continue

            old_confidence = node.confidence
            new_confidence = min(1.0, old_confidence + self.DISCOVERY_CONFIDENCE_BOOST)
            ok = self.hierarchy.update_confidence(
                node_id=ms_id,
                new_confidence=new_confidence,
                rationale=f"Capability discovered: {capability_description[:200]}. {simplification_summary[:200]}",
                trigger_type=EvolutionTrigger.CAPABILITY_DISCOVERY.value,
            )
            if ok:
                revised_nodes.append(ms_id)
                if node.revision_history:
                    records.append(node.revision_history[-1])

        self._revision_count += 1
        self._trigger_counts[EvolutionTrigger.CAPABILITY_DISCOVERY.value] += 1

        log.info(
            "Capability discovery: %d milestones revised (boost=+%.2f): %s",
            len(revised_nodes), self.DISCOVERY_CONFIDENCE_BOOST,
            capability_description[:80],
        )

        return PlanRevisionResult(
            success=True,
            scope=RevisionScope.MILESTONE,
            revised_nodes=revised_nodes,
            revision_records=records,
        )

    def process_resource_constraint(
        self,
        affected_node_ids: List[str],
        constraint_description: str,
        alternative_approach: str = "",
        event_memory_id: Optional[str] = None,
    ) -> PlanRevisionResult:
        """Process a resource constraint event.

        A resource constraint means a planned approach is no longer feasible.
        Affected nodes are blocked. If an alternative approach is specified,
        we update the nodes' content to reflect it.
        """
        revised_nodes = []
        records = []

        for node_id in affected_node_ids:
            node = self.hierarchy.get_node(node_id)
            if not node:
                continue

            # 1. Block the node
            ok = self.hierarchy.update_node_state(
                node_id=node_id,
                new_state=PlanNodeState.BLOCKED,
                trigger_summary=f"Resource constraint: {constraint_description[:200]}",
                trigger_type=EvolutionTrigger.RESOURCE_CONSTRAINT.value,
            )
            if ok:
                revised_nodes.append(node_id)

            # 2. Apply confidence penalty
            old_confidence = node.confidence
            new_confidence = max(0.0, old_confidence - self.CONSTRAINT_CONFIDENCE_PENALTY)
            self.hierarchy.update_confidence(
                node_id=node_id,
                new_confidence=new_confidence,
                rationale=f"Resource constraint: {constraint_description[:200]}",
                trigger_type=EvolutionTrigger.RESOURCE_CONSTRAINT.value,
            )

            if node.revision_history:
                records.append(node.revision_history[-1])

        self._revision_count += 1
        self._trigger_counts[EvolutionTrigger.RESOURCE_CONSTRAINT.value] += 1

        log.info(
            "Resource constraint: %d nodes blocked: %s",
            len(revised_nodes), constraint_description[:80],
        )

        return PlanRevisionResult(
            success=True,
            scope=RevisionScope.MILESTONE,
            revised_nodes=revised_nodes,
            revision_records=records,
        )

    def process_user_directive(
        self,
        directive_text: str,
        target_level: HierarchyLevel,
        target_node_id: Optional[str] = None,
        new_priority_objective_id: Optional[str] = None,
        deactivate_objective_ids: Optional[List[str]] = None,
    ) -> PlanRevisionResult:
        """Process a user directive that changes strategic direction.

        USER_DIRECTIVE is the only trigger that can change what to build.
        It can affect any level of the hierarchy, up to and including the
        Mission statement itself.
        """
        if not self._validate_scope(EvolutionTrigger.USER_DIRECTIVE, RevisionScope.MISSION):
            return PlanRevisionResult(
                success=False,
                scope=RevisionScope.MISSION,
                revised_nodes=[],
                revision_records=[],
                rejection_reason="USER_DIRECTIVE scope gate check failed unexpectedly",
            )

        revised_nodes = []
        records = []

        # Activate priority objective
        if new_priority_objective_id:
            node = self.hierarchy.get_node(new_priority_objective_id)
            if node:
                ok = self.hierarchy.update_node_state(
                    node_id=new_priority_objective_id,
                    new_state=PlanNodeState.ACTIVE,
                    trigger_summary=f"User directive: {directive_text[:200]}",
                    trigger_type=EvolutionTrigger.USER_DIRECTIVE.value,
                )
                if ok:
                    revised_nodes.append(new_priority_objective_id)
                    if node.revision_history:
                        records.append(node.revision_history[-1])

        # Deactivate objectives explicitly superseded
        for obj_id in (deactivate_objective_ids or []):
            node = self.hierarchy.get_node(obj_id)
            if node:
                ok = self.hierarchy.update_node_state(
                    node_id=obj_id,
                    new_state=PlanNodeState.DEFERRED,
                    trigger_summary=f"User directive — deactivated: {directive_text[:200]}",
                    trigger_type=EvolutionTrigger.USER_DIRECTIVE.value,
                )
                if ok:
                    revised_nodes.append(obj_id)

        self._revision_count += 1
        self._trigger_counts[EvolutionTrigger.USER_DIRECTIVE.value] += 1

        log.info(
            "User directive processed: '%s', revised %d nodes",
            directive_text[:80], len(revised_nodes),
        )

        return PlanRevisionResult(
            success=True,
            scope=RevisionScope.MISSION if target_level == HierarchyLevel.MISSION else RevisionScope.OBJECTIVE,
            revised_nodes=revised_nodes,
            revision_records=records,
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Return revision statistics for inspection."""
        return {
            "total_revisions": self._revision_count,
            "by_trigger": dict(self._trigger_counts),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_scope(self, trigger: EvolutionTrigger, requested_scope: RevisionScope) -> bool:
        """Check that a requested revision scope is permitted for this trigger."""
        allowed_scopes = self._SCOPE_GATES.get(trigger, [])
        return requested_scope in allowed_scopes
