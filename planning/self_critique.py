# planning/self_critique.py - Self-Critique Engine for AELVO OMEGA
"""
The Self-Critique Engine audits the plan quality at every session boundary.

It enforces five planning defect rules â€” violations that turn a plan from a
strategic tool into a wishlist that cannot be executed. Every defect is
recorded as a SelfCritiqueDefect memory entry and, if it persists for three
consecutive runs without resolution, it is escalated.

The five rules:
1. FLOATING_TASK: Every task must trace to a Strategic Objective.
2. ASPIRATIONAL_OBJECTIVE: Every objective must have at least one milestone.
3. CIRCULAR_DEPENDENCY: No dependency cycle may exist in the graph.
4. CONFIDENCE_DRIFT: No node's confidence may decline for >2 consecutive scans
   without being addressed.
5. UNVERIFIED_COMPLETION: No node may be marked COMPLETE unless it has a
   VerificationStrategy and at least one RevisionRecord proving verification ran.

The self-critique result is surfaced to the user only for escalated defects.
All defects are written to the audit log regardless of escalation.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Dict, List, Set, Tuple

from planning.memory_types import (
    SelfCritiqueDefect,
    DefectType,
    HierarchyLevel,
    PlanNodeState,
    MEMORY_TYPE_CRITIQUE_AUDIT,
    IMPORTANCE_CRITIQUE_AUDIT,
)
from planning.goal_hierarchy import GoalHierarchyEngine

log = logging.getLogger("aelvo.planning.critique")


# Confidence drift detection window
_CONFIDENCE_DRIFT_SESSIONS = 2    # Declare drift after this many consecutive declining scans
_ESCALATION_THRESHOLD = 3         # Escalate after this many consecutive failing scans


class CritiqueRunResult:
    """Result of a single self-critique run."""

    def __init__(
        self,
        run_id: str,
        defects: List[SelfCritiqueDefect],
        plan_quality_score: float,
        escalated_defects: List[SelfCritiqueDefect],
    ):
        self.run_id = run_id
        self.defects = defects
        self.plan_quality_score = plan_quality_score
        self.escalated_defects = escalated_defects
        self.has_blocking_defects = any(
            d.defect_type in (DefectType.CIRCULAR_DEPENDENCY,) for d in defects
        )

    def to_summary(self) -> str:
        """Return a compact human-readable summary."""
        if not self.defects:
            return f"Plan quality: {self.plan_quality_score:.2f} â€” No defects detected."
        escalated = len(self.escalated_defects)
        lines = [
            f"Plan quality: {self.plan_quality_score:.2f} â€” {len(self.defects)} defect(s) detected."
        ]
        if escalated:
            lines.append(f"  âš  {escalated} ESCALATED defect(s) require attention:")
            for d in self.escalated_defects:
                lines.append(f"    â€¢ [{d.defect_type.value}] {d.affected_node_title}: {d.defect_description}")
        return "\n".join(lines)


class SelfCritiqueEngine:
    """Plan quality enforcement engine.

    Runs at session boundaries and whenever the plan evolution engine makes
    a significant revision. Never blocks execution â€” defects are advisory
    unless explicitly escalated.
    """

    def __init__(
        self,
        hierarchy: GoalHierarchyEngine,
        memory_engine,
        project: str,
    ):
        self.hierarchy = hierarchy
        self.memory_engine = memory_engine
        self.project = project
        self.collection = memory_engine.memory_collection
        self._run_count = 0
        # Track consecutive defect counts for escalation logic
        # key: (defect_type, node_id), value: consecutive_count
        self._consecutive_defect_counts: Dict[Tuple[str, str], int] = {}
        # Track confidence history for drift detection
        # key: node_id, value: [confidence at each scan]
        self._confidence_history: Dict[str, List[float]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> CritiqueRunResult:
        """Execute a full self-critique pass over the current plan.

        The five checks are independent and are all run even if earlier
        checks find defects. All defects are persisted to ChromaDB.
        """
        self._run_count += 1
        run_id = hashlib.sha256(
            f"critique_{self.project}_{time.time()}".encode("utf-8")
        ).hexdigest()[:12]

        log.info("Self-critique run #%d starting (run_id=%s)", self._run_count, run_id)

        all_defects: List[SelfCritiqueDefect] = []

        # Execute all five checks
        all_defects.extend(self._check_floating_tasks(run_id))
        all_defects.extend(self._check_aspirational_objectives(run_id))
        all_defects.extend(self._check_circular_dependencies(run_id))
        all_defects.extend(self._check_confidence_drift(run_id))
        all_defects.extend(self._check_unverified_completions(run_id))

        # Update consecutive counts and detect escalation
        escalated: List[SelfCritiqueDefect] = []
        current_keys: Set[Tuple[str, str]] = set()

        for defect in all_defects:
            key = (defect.defect_type.value, defect.affected_node_id)
            current_keys.add(key)
            prev_count = self._consecutive_defect_counts.get(key, 0)
            self._consecutive_defect_counts[key] = prev_count + 1
            defect.consecutive_run_count = self._consecutive_defect_counts[key]
            if defect.consecutive_run_count >= _ESCALATION_THRESHOLD:
                defect.escalated = True
                escalated.append(defect)

        # Reset consecutive counts for defects that were resolved
        for key in list(self._consecutive_defect_counts.keys()):
            if key not in current_keys:
                del self._consecutive_defect_counts[key]

        # Persist all defects
        for defect in all_defects:
            defect.critique_run_id = run_id
            self._persist_defect(defect)

        # Compute plan quality score
        quality_score = self._compute_quality_score(all_defects)

        log.info(
            "Self-critique run #%d complete: %d defect(s), %d escalated, quality=%.2f",
            self._run_count, len(all_defects), len(escalated), quality_score,
        )

        return CritiqueRunResult(
            run_id=run_id,
            defects=all_defects,
            plan_quality_score=quality_score,
            escalated_defects=escalated,
        )

    def mark_defect_resolved(self, defect_id: str) -> bool:
        """Mark a defect as resolved in ChromaDB."""
        try:
            results = self.collection.get(ids=[defect_id], include=["metadatas"])
            if not results.get("metadatas"):
                return False
            meta = dict(results["metadatas"][0])
            meta["resolved"] = True
            meta["resolved_unix"] = time.time()
            self.collection.update(ids=[defect_id], metadatas=[meta])
            return True
        except Exception as exc:
            log.warning("Failed to mark defect %s resolved: %s", defect_id, exc)
            return False

    # ------------------------------------------------------------------
    # Check 1: Floating Tasks
    # ------------------------------------------------------------------

    def _check_floating_tasks(self, run_id: str) -> List[SelfCritiqueDefect]:
        """Rule: Every task must trace to a Strategic Objective."""
        floating = self.hierarchy.find_floating_tasks()
        defects = []

        for task in floating:
            defect = SelfCritiqueDefect(
                type=MEMORY_TYPE_CRITIQUE_AUDIT,
                content=(
                    f"Floating task detected: '{task.title}' (node_id={task.node_id}) "
                    f"cannot be traced to any Strategic Objective."
                ),
                importance=IMPORTANCE_CRITIQUE_AUDIT,
                project=self.project,
                defect_type=DefectType.FLOATING_TASK,
                affected_node_id=task.node_id,
                affected_node_title=task.title,
                defect_description=(
                    f"Task '{task.title}' has no ancestor Strategic Objective. "
                    f"Work that cannot be traced to a strategic objective is "
                    f"either mis-scoped or signals a missing objective."
                ),
                recommended_correction=(
                    f"Attach task '{task.title}' to an appropriate Milestone and Initiative, "
                    f"or create a new Strategic Objective for this work area."
                ),
                critique_run_id=run_id,
            )
            defects.append(defect)

        return defects

    # ------------------------------------------------------------------
    # Check 2: Aspirational Objectives
    # ------------------------------------------------------------------

    def _check_aspirational_objectives(self, run_id: str) -> List[SelfCritiqueDefect]:
        """Rule: Every objective must have at least one milestone."""
        objectives = self.hierarchy.find_nodes_by_level(HierarchyLevel.STRATEGIC_OBJECTIVE)
        defects = []

        for obj in objectives:
            if obj.state in (PlanNodeState.CANCELLED, PlanNodeState.COMPLETE):
                continue
            children = self.hierarchy.get_children(obj.node_id)
            has_milestone = any(
                c.level == HierarchyLevel.MILESTONE for c in children
            )
            # Also check through programs/initiatives
            if not has_milestone:
                for child in children:
                    grandchildren = self.hierarchy.get_children(child.node_id)
                    if any(c.level == HierarchyLevel.MILESTONE for c in grandchildren):
                        has_milestone = True
                        break

            if not has_milestone:
                defect = SelfCritiqueDefect(
                    type=MEMORY_TYPE_CRITIQUE_AUDIT,
                    content=(
                        f"Aspirational objective: '{obj.title}' (node_id={obj.node_id}) "
                        f"has no milestones and cannot be executed."
                    ),
                    importance=IMPORTANCE_CRITIQUE_AUDIT,
                    project=self.project,
                    defect_type=DefectType.ASPIRATIONAL_OBJECTIVE,
                    affected_node_id=obj.node_id,
                    affected_node_title=obj.title,
                    defect_description=(
                        f"Strategic Objective '{obj.title}' has no milestones. "
                        f"Without milestones, there is no measurable path to completion."
                    ),
                    recommended_correction=(
                        f"Create at least one Milestone under '{obj.title}' with "
                        f"explicit success criteria and a verification strategy."
                    ),
                    critique_run_id=run_id,
                )
                defects.append(defect)

        return defects

    # ------------------------------------------------------------------
    # Check 3: Circular Dependencies
    # ------------------------------------------------------------------

    def _check_circular_dependencies(self, run_id: str) -> List[SelfCritiqueDefect]:
        """Rule: No circular dependency may exist in the blocking_dependencies graph."""
        cycles = self.hierarchy.detect_circular_dependencies()
        defects = []

        for node_id, dep_id in cycles:
            node = self.hierarchy.get_node(node_id)
            dep = self.hierarchy.get_node(dep_id)
            node_title = node.title if node else node_id
            dep_title = dep.title if dep else dep_id

            defect = SelfCritiqueDefect(
                type=MEMORY_TYPE_CRITIQUE_AUDIT,
                content=(
                    f"Circular dependency: '{node_title}' â†’ '{dep_title}' â†’ ... â†’ '{node_title}'"
                ),
                importance=IMPORTANCE_CRITIQUE_AUDIT,
                project=self.project,
                defect_type=DefectType.CIRCULAR_DEPENDENCY,
                affected_node_id=node_id,
                affected_node_title=node_title,
                defect_description=(
                    f"Node '{node_title}' lists '{dep_title}' as a blocking dependency, "
                    f"but '{dep_title}' is an ancestor of '{node_title}'. "
                    f"This cycle will deadlock execution."
                ),
                recommended_correction=(
                    f"Remove the blocking_dependency from '{node_title}' to '{dep_title}', "
                    f"or restructure the dependency relationship."
                ),
                critique_run_id=run_id,
            )
            defects.append(defect)

        return defects

    # ------------------------------------------------------------------
    # Check 4: Confidence Drift
    # ------------------------------------------------------------------

    def _check_confidence_drift(self, run_id: str) -> List[SelfCritiqueDefect]:
        """Rule: No node's confidence may decline for >2 consecutive scans without response."""
        defects = []
        active_nodes = [
            n for n in self.hierarchy._nodes.values()
            if n.state in (PlanNodeState.ACTIVE, PlanNodeState.PROPOSED)
        ]

        for node in active_nodes:
            history = self._confidence_history.setdefault(node.node_id, [])
            history.append(node.confidence)

            # Keep only the last N+1 readings
            if len(history) > _CONFIDENCE_DRIFT_SESSIONS + 1:
                history.pop(0)

            # Check for consistent decline
            if len(history) >= _CONFIDENCE_DRIFT_SESSIONS + 1:
                is_declining = all(
                    history[i] > history[i + 1]
                    for i in range(len(history) - 1)
                )
                if is_declining:
                    drift_amount = history[0] - history[-1]
                    defect = SelfCritiqueDefect(
                        type=MEMORY_TYPE_CRITIQUE_AUDIT,
                        content=(
                            f"Confidence drift detected: '{node.title}' (node_id={node.node_id}) "
                            f"confidence declining from {history[0]:.2f} to {history[-1]:.2f} "
                            f"over {len(history)} scans without intervention."
                        ),
                        importance=IMPORTANCE_CRITIQUE_AUDIT,
                        project=self.project,
                        defect_type=DefectType.CONFIDENCE_DRIFT,
                        affected_node_id=node.node_id,
                        affected_node_title=node.title,
                        defect_description=(
                            f"Confidence for '{node.title}' has declined by {drift_amount:.2f} "
                            f"over {len(history)} consecutive critique runs "
                            f"({history[0]:.2f} â†’ {history[-1]:.2f}) "
                            f"without any revision or state change."
                        ),
                        recommended_correction=(
                            f"Either address the underlying causes of declining confidence "
                            f"(verification failures, resource constraints), or mark "
                            f"'{node.title}' as BLOCKED to reflect reality."
                        ),
                        critique_run_id=run_id,
                    )
                    defects.append(defect)

        return defects

    # ------------------------------------------------------------------
    # Check 5: Unverified Completions
    # ------------------------------------------------------------------

    def _check_unverified_completions(self, run_id: str) -> List[SelfCritiqueDefect]:
        """Rule: No node may be COMPLETE unless it has a VerificationStrategy and revision history."""
        defects = []
        complete_nodes = [
            n for n in self.hierarchy._nodes.values()
            if n.state in (PlanNodeState.COMPLETE, PlanNodeState.TENTATIVE_COMPLETE)
            and n.level in (HierarchyLevel.MILESTONE, HierarchyLevel.TASK)
        ]

        for node in complete_nodes:
            # Check verification strategy exists
            if not node.verification_strategy:
                defect = SelfCritiqueDefect(
                    type=MEMORY_TYPE_CRITIQUE_AUDIT,
                    content=(
                        f"Unverified completion: '{node.title}' (node_id={node.node_id}) "
                        f"is marked COMPLETE but has no VerificationStrategy."
                    ),
                    importance=IMPORTANCE_CRITIQUE_AUDIT,
                    project=self.project,
                    defect_type=DefectType.UNVERIFIED_COMPLETION,
                    affected_node_id=node.node_id,
                    affected_node_title=node.title,
                    defect_description=(
                        f"'{node.title}' is marked {node.state.value} "
                        f"but has no VerificationStrategy attached. "
                        f"Completion without verification is a wishlist, not a plan."
                    ),
                    recommended_correction=(
                        f"Either add a VerificationStrategy to '{node.title}' and re-run "
                        f"verification, or revert its state to ACTIVE."
                    ),
                    critique_run_id=run_id,
                )
                defects.append(defect)
                continue

            # Check revision history contains at least one verification-related revision
            has_verification_record = any(
                "verification" in (r.trigger_type or "").lower()
                or "verified" in r.changes_made.lower()
                or "verification" in r.rationale.lower()
                for r in node.revision_history
            )
            if not has_verification_record:
                defect = SelfCritiqueDefect(
                    type=MEMORY_TYPE_CRITIQUE_AUDIT,
                    content=(
                        f"Unverified completion: '{node.title}' is COMPLETE "
                        f"with no verification record in revision history."
                    ),
                    importance=IMPORTANCE_CRITIQUE_AUDIT,
                    project=self.project,
                    defect_type=DefectType.UNVERIFIED_COMPLETION,
                    affected_node_id=node.node_id,
                    affected_node_title=node.title,
                    defect_description=(
                        f"'{node.title}' has a VerificationStrategy but its "
                        f"revision history ({len(node.revision_history)} records) "
                        f"contains no verification events."
                    ),
                    recommended_correction=(
                        f"Run the verification checks specified in '{node.title}'s "
                        f"VerificationStrategy and record the outcome."
                    ),
                    critique_run_id=run_id,
                )
                defects.append(defect)

        return defects

    # ------------------------------------------------------------------
    # Quality Score
    # ------------------------------------------------------------------

    def _compute_quality_score(self, defects: List[SelfCritiqueDefect]) -> float:
        """Compute a 0.0â€“1.0 plan quality score.

        Scoring is based on the severity and escalation status of detected defects.
        A plan with no defects scores 1.0. Each defect type has a base penalty.
        Escalated defects carry double the penalty.
        """
        if not self.hierarchy._nodes:
            return 1.0  # Empty plan is technically perfect

        base_score = 1.0
        _penalties = {
            DefectType.CIRCULAR_DEPENDENCY: 0.30,      # Blocking defect
            DefectType.FLOATING_TASK: 0.10,
            DefectType.ASPIRATIONAL_OBJECTIVE: 0.10,
            DefectType.CONFIDENCE_DRIFT: 0.08,
            DefectType.UNVERIFIED_COMPLETION: 0.12,
        }

        for defect in defects:
            penalty = _penalties.get(defect.defect_type, 0.05)
            if defect.escalated:
                penalty *= 2.0
            base_score -= penalty

        return max(0.0, round(base_score, 4))

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist_defect(self, defect: SelfCritiqueDefect) -> None:
        """Write a SelfCritiqueDefect to ChromaDB."""
        try:
            meta = {
                "type": MEMORY_TYPE_CRITIQUE_AUDIT,
                "importance": float(defect.importance),
                "timestamp_unix": float(defect.timestamp_unix),
                "usage_count": int(defect.usage_count),
                "project": self.project,
                "source_specialist": "planning",
                "defect_type": defect.defect_type.value,
                "affected_node_id": defect.affected_node_id,
                "affected_node_title": defect.affected_node_title[:100],
                "escalated": defect.escalated,
                "resolved": defect.resolved,
                "consecutive_run_count": defect.consecutive_run_count,
                "critique_run_id": defect.critique_run_id,
            }
            self.collection.add(
                ids=[defect.id],
                documents=[defect.content],
                metadatas=[meta],
            )
        except Exception as exc:
            log.debug("Failed to persist defect %s: %s", defect.id, exc)
