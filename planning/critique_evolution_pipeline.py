# planning/critique_evolution_pipeline.py — Self-Critique → Plan Evolution Pipeline
"""
The SelfCritiqueEvolutionPipeline connects plan quality auditing (self-critique)
with plan revision (evolution) in a closed feedback loop.

Flow:
  1. Run self-critique over the current goal hierarchy
  2. For each actionable defect, map it to an evolution trigger
  3. Cascade the evolution — produce a revised plan
  4. Re-run self-critique to verify the fix
  5. Repeat up to max_iterations until defects are resolved
  6. Report pipeline result with before/after quality scores

Design principles:
- Never blocks execution — the pipeline is advisory
- Each defect maps to a proportional evolution scope (scope-gating)
- The pipeline stops early if quality improves or all critical defects are fixed
- All actions are recorded via RevisionRecords for auditability
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from planning.memory_types import (
    SelfCritiqueDefect,
    DefectType,
    HierarchyLevel,
    PlanNodeState,
)
from planning.goal_hierarchy import GoalHierarchyEngine
from planning.plan_evolution import (
    PlanEvolutionEngine,
    PlanRevisionResult,
    EvolutionTrigger,
    RevisionScope,
)
from planning.self_critique import SelfCritiqueEngine, CritiqueRunResult

log = logging.getLogger("aelvo.planning.critique_pipeline")


# ---------------------------------------------------------------------------
# Pipeline types
# ---------------------------------------------------------------------------


class PipelineStatus(str, Enum):
    """Final status of a pipeline run."""
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETE_CLEAN = "complete_clean"           # All defects resolved
    COMPLETE_PARTIAL = "complete_partial"       # Some defects remain (non-critical)
    COMPLETE_BLOCKED = "complete_blocked"       # Critical defects still present after max iterations
    MAX_ITERATIONS_REACHED = "max_iterations_reached"  # Stopped before all defects were fixed
    ERROR = "error"                             # Pipeline encountered an unrecoverable error


class SeverityLevel(str, Enum):
    """Severity rating for defects and pipeline actions."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class EvolutionAction:
    """A single evolution action triggered by a pipeline defect.

    Records which defect triggered it, which evolution trigger was selected,
    what scope was allowed, and whether automatic remediation was possible.
    """
    action_id: str
    defect_type: DefectType
    defect_id: str
    affected_node_id: str
    evolution_trigger: EvolutionTrigger
    revision_scope: RevisionScope
    auto_remediated: bool = False
    revision_result: Optional[PlanRevisionResult] = None
    error_message: str = ""

    @property
    def success(self) -> bool:
        return self.revision_result is not None and self.revision_result.success


@dataclass
class PipelineIteration:
    """A single iteration of the critique→evolve→verify cycle."""
    iteration_number: int
    critique_before: CritiqueRunResult
    evolution_actions: List[EvolutionAction] = field(default_factory=list)
    critique_after: Optional[CritiqueRunResult] = None

    @property
    def quality_improvement(self) -> float:
        """How much the quality score improved in this iteration."""
        if self.critique_after is None:
            return 0.0
        return self.critique_after.plan_quality_score - self.critique_before.plan_quality_score

    @property
    def defects_resolved(self) -> int:
        """Number of defects that were present before but not after."""
        if self.critique_after is None:
            return 0
        before_ids = {d.id for d in self.critique_before.defects}
        after_ids = {d.id for d in self.critique_after.defects}
        return len(before_ids - after_ids)

    @property
    def new_defects_introduced(self) -> int:
        """Number of new defects that appeared after evolution."""
        if self.critique_after is None:
            return 0
        before_ids = {d.id for d in self.critique_before.defects}
        after_ids = {d.id for d in self.critique_after.defects}
        return len(after_ids - before_ids)


@dataclass
class PipelineResult:
    """Complete result of a pipeline run with full traceability."""
    pipeline_id: str
    status: PipelineStatus
    iterations: List[PipelineIteration] = field(default_factory=list)
    total_actions_taken: int = 0
    initial_quality_score: float = 0.0
    final_quality_score: float = 0.0
    errors: List[str] = field(default_factory=list)
    started_at: float = 0.0
    completed_at: float = 0.0

    @property
    def duration_ms(self) -> float:
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at) * 1000.0
        return 0.0

    @property
    def quality_improvement(self) -> float:
        return self.final_quality_score - self.initial_quality_score

    @property
    def all_evolution_actions(self) -> List[EvolutionAction]:
        return [a for it in self.iterations for a in it.evolution_actions]

    @property
    def total_defects_found(self) -> int:
        return sum(len(it.critique_before.defects) for it in self.iterations)

    def to_summary(self) -> str:
        """Compact human-readable summary."""
        lines = [
            f"Pipeline: {self.pipeline_id[:12]}",
            f"  Status: {self.status.value}",
            f"  Duration: {self.duration_ms:.0f}ms",
            f"  Iterations: {len(self.iterations)}",
            f"  Actions: {self.total_actions_taken}",
            f"  Quality: {self.initial_quality_score:.2f} → {self.final_quality_score:.2f} "
            f"(Δ={self.quality_improvement:+.2f})",
        ]
        if self.errors:
            lines.append(f"  Errors: {len(self.errors)}")
            for err in self.errors[:3]:
                lines.append(f"    • {err[:120]}")
        return "\n".join(lines)

    def format_report(self) -> Dict[str, Any]:
        """Structured report for integration points."""
        return {
            "pipeline_id": self.pipeline_id,
            "status": self.status.value,
            "duration_ms": self.duration_ms,
            "iterations": len(self.iterations),
            "total_actions": self.total_actions_taken,
            "initial_quality": self.initial_quality_score,
            "final_quality": self.final_quality_score,
            "quality_delta": round(self.quality_improvement, 4),
            "errors": list(self.errors),
            "defects_found": self.total_defects_found,
        }


# ---------------------------------------------------------------------------
# Defect → Evolution Trigger Mapping & Scope Calculation
# ---------------------------------------------------------------------------


# Canonical mapping: defect type → appropriate evolution trigger
DEFECT_TO_TRIGGER: Dict[DefectType, EvolutionTrigger] = {
    DefectType.FLOATING_TASK: EvolutionTrigger.USER_DIRECTIVE,
    DefectType.ASPIRATIONAL_OBJECTIVE: EvolutionTrigger.USER_DIRECTIVE,
    DefectType.CIRCULAR_DEPENDENCY: EvolutionTrigger.RESOURCE_CONSTRAINT,
    DefectType.CONFIDENCE_DRIFT: EvolutionTrigger.VERIFICATION_FAILURE,
    DefectType.UNVERIFIED_COMPLETION: EvolutionTrigger.VERIFICATION_FAILURE,
}

# Desired impact scope for each defect type (what level of revision is warranted)
DEFECT_TO_SCOPE: Dict[DefectType, RevisionScope] = {
    DefectType.FLOATING_TASK: RevisionScope.TASK_ONLY,
    DefectType.ASPIRATIONAL_OBJECTIVE: RevisionScope.OBJECTIVE,
    DefectType.CIRCULAR_DEPENDENCY: RevisionScope.MILESTONE,
    DefectType.CONFIDENCE_DRIFT: RevisionScope.MILESTONE,
    DefectType.UNVERIFIED_COMPLETION: RevisionScope.TASK_ONLY,
}

# Severity classification for each defect type
DEFECT_TO_SEVERITY: Dict[DefectType, SeverityLevel] = {
    DefectType.CIRCULAR_DEPENDENCY: SeverityLevel.CRITICAL,
    DefectType.UNVERIFIED_COMPLETION: SeverityLevel.HIGH,
    DefectType.FLOATING_TASK: SeverityLevel.MEDIUM,
    DefectType.ASPIRATIONAL_OBJECTIVE: SeverityLevel.MEDIUM,
    DefectType.CONFIDENCE_DRIFT: SeverityLevel.LOW,
}

# Quality plateau detection: if quality improves by less than this in an iteration, stop early
_QUALITY_PLATEAU_THRESHOLD = 0.02


# ---------------------------------------------------------------------------
# Automatic Remediation Strategies
# ---------------------------------------------------------------------------


def _auto_fix_floating_task(
    hierarchy: GoalHierarchyEngine,
    defect: SelfCritiqueDefect,
) -> Optional[PlanRevisionResult]:
    """Attempt to attach a floating task to the most relevant active milestone.

    This is a heuristic: if there's a single active milestone, attach the
    task to it. Otherwise, create a parent link suggestion via evolution.
    """
    active_milestones = hierarchy.get_active_milestones()
    if not active_milestones:
        return None

    # Find the best parent (most relevant milestone)
    task_lower = defect.affected_node_title.lower()
    best_parent = max(
        active_milestones,
        key=lambda m: len(set(task_lower.split()) & set((m.title + " " + m.content).lower().split())),
    )

    node = hierarchy.get_node(defect.affected_node_id)
    if node is None:
        return None

    # Check that attaching under this milestone respects hierarchy rules
    if hierarchy._validate_parent_child_level(best_parent.level, node.level):
        node.parent_id = best_parent.node_id
        best_parent.children_ids.append(node.node_id)
        hierarchy._children_index.setdefault(best_parent.node_id, [])
        if node.node_id not in hierarchy._children_index[best_parent.node_id]:
            hierarchy._children_index[best_parent.node_id].append(node.node_id)
        node.record_revision(
            trigger_type=EvolutionTrigger.USER_DIRECTIVE.value,
            trigger_summary=f"Auto-remediation: attached floating task to milestone '{best_parent.title}'",
            changes_made=f"parent_id: None → {best_parent.node_id}",
            rationale="Self-critique detected floating task; auto-attached to best-matching milestone",
        )
        hierarchy._persist_node_update(node)
        hierarchy._persist_node_update(best_parent)
        log.info("Auto-fix: attached floating task '%s' → milestone '%s'", node.title, best_parent.title)
        return PlanRevisionResult(
            success=True,
            scope=RevisionScope.TASK_ONLY,
            revised_nodes=[node.node_id, best_parent.node_id],
            revision_records=[node.revision_history[-1]] if node.revision_history else [],
        )

    return None


def _auto_fix_circular_dependency(
    hierarchy: GoalHierarchyEngine,
    defect: SelfCritiqueDefect,
) -> Optional[PlanRevisionResult]:
    """Attempt to break a circular dependency by removing the problematic edge.

    The defect description contains the cycle information. We parse the
    affected node and its dependency target, then remove the dependency edge.
    """
    node_id = defect.affected_node_id
    node = hierarchy.get_node(node_id)
    if node is None:
        return None

    # Find the circular dependency edge by checking each blocking_dependency
    for dep_id in list(node.blocking_dependencies):
        if hierarchy._is_ancestor_of(node_id, dep_id):
            node.blocking_dependencies.remove(dep_id)
            node.record_revision(
                trigger_type=EvolutionTrigger.RESOURCE_CONSTRAINT.value,
                trigger_summary=f"Auto-remediation: removed circular dependency on '{dep_id}'",
                changes_made=f"blocking_dependencies: removed '{dep_id}'",
                rationale="Self-critique detected circular dependency; removed the problematic edge",
            )
            hierarchy._persist_node_update(node)
            log.info("Auto-fix: broke circular dependency '%s' → '%s'", node.title, dep_id)
            return PlanRevisionResult(
                success=True,
                scope=RevisionScope.TASK_ONLY,
                revised_nodes=[node.node_id],
                revision_records=[node.revision_history[-1]] if node.revision_history else [],
            )

    return None


def _auto_fix_unverified_completion(
    hierarchy: GoalHierarchyEngine,
    defect: SelfCritiqueDefect,
) -> Optional[PlanRevisionResult]:
    """Revert an unverified completion back to ACTIVE state.

    A node marked COMPLETE without verification evidence is a planning
    defect. The safest auto-fix is to revert to ACTIVE and let the
    verification system run properly.
    """
    node = hierarchy.get_node(defect.affected_node_id)
    if node is None:
        return None

    if node.state not in (PlanNodeState.COMPLETE, PlanNodeState.TENTATIVE_COMPLETE):
        return None

    # Revert to ACTIVE (or PROPOSED if it was never started)
    new_state = PlanNodeState.PROPOSED if node.progress_pct < 5.0 else PlanNodeState.ACTIVE
    ok = hierarchy.update_node_state(
        node_id=defect.affected_node_id,
        new_state=new_state,
        trigger_summary="Auto-remediation: reverted unverified completion to ACTIVE",
        trigger_type=EvolutionTrigger.VERIFICATION_FAILURE.value,
    )
    if ok:
        log.info("Auto-fix: reverted unverified completion '%s' → %s", node.title, new_state.value)
        return PlanRevisionResult(
            success=True,
            scope=RevisionScope.TASK_ONLY,
            revised_nodes=[node.node_id],
            revision_records=[node.revision_history[-1]] if node.revision_history else [],
        )

    return None


# Registry of automatic remediation handlers keyed by defect type
_AUTO_REMEDIATION_HANDLERS = {
    DefectType.FLOATING_TASK: _auto_fix_floating_task,
    DefectType.CIRCULAR_DEPENDENCY: _auto_fix_circular_dependency,
    DefectType.UNVERIFIED_COMPLETION: _auto_fix_unverified_completion,
    # ASPIRATIONAL_OBJECTIVE and CONFIDENCE_DRIFT require user judgment
}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class SelfCritiqueEvolutionPipeline:
    """Connects self-critique defect detection to plan evolution triggers.

    The pipeline runs a closed loop:
      Critique → Evolve → Verify → (repeat if needed)

    It never blocks execution. All actions are recorded with full traceability
    so the system learns from its own planning corrections.

    Usage:
        pipeline = SelfCritiqueEvolutionPipeline(hierarchy, evolution, critique)
        result = pipeline.run(max_iterations=3)
        print(result.to_summary())
    """

    def __init__(
        self,
        hierarchy: GoalHierarchyEngine,
        evolution: PlanEvolutionEngine,
        critique: SelfCritiqueEngine,
    ):
        self.hierarchy = hierarchy
        self.evolution = evolution
        self.critique = critique
        self._run_count = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        max_iterations: int = 3,
        auto_remediate: bool = True,
        target_defect_types: Optional[List[DefectType]] = None,
    ) -> PipelineResult:
        """Execute the full critique → evolve → verify pipeline.

        Args:
            max_iterations: Maximum critique→evolve cycles (default 3).
            auto_remediate: Whether to attempt automatic fixes for known defect
                            types (floating task, circular dependency,
                            unverified completion).
            target_defect_types: If provided, only process these defect types.
                                 Processes all types by default.

        Returns:
            PipelineResult with full iteration traceability.
        """
        self._run_count += 1
        pipeline_id = hashlib.sha256(
            f"critique_pipeline_{time.time()}_{self._run_count}".encode("utf-8")
        ).hexdigest()[:16]

        log.info(
            "SelfCritiqueEvolutionPipeline run #%d starting (id=%s, max_iterations=%d)",
            self._run_count, pipeline_id, max_iterations,
        )

        result = PipelineResult(
            pipeline_id=pipeline_id,
            status=PipelineStatus.IN_PROGRESS,
            started_at=time.time(),
        )

        # Phase 1: Initial self-critique
        initial_critique = self.critique.run()
        result.initial_quality_score = initial_critique.plan_quality_score

        if not initial_critique.defects:
            log.info("Pipeline run #%d: no defects found — plan is clean", self._run_count)
            result.status = PipelineStatus.COMPLETE_CLEAN
            result.final_quality_score = initial_critique.plan_quality_score
            result.completed_at = time.time()
            return result

        # Filter defects if target types specified
        defects_to_process = [
            d for d in initial_critique.defects
            if target_defect_types is None or d.defect_type in target_defect_types
        ]

        if not defects_to_process:
            log.info(
                "Pipeline run #%d: no matching defect types to process",
                self._run_count,
            )
            result.status = PipelineStatus.COMPLETE_CLEAN
            result.final_quality_score = initial_critique.plan_quality_score
            result.completed_at = time.time()
            return result

        # Phase 2-4: Iterate critique → evolve → verify
        current_critique = initial_critique
        any(
            DEFECT_TO_SEVERITY.get(d.defect_type, SeverityLevel.LOW) == SeverityLevel.CRITICAL
            for d in defects_to_process
        )

        for iteration_num in range(1, max_iterations + 1):
            log.info(
                "Pipeline iteration %d/%d: %d defects, quality=%.2f",
                iteration_num, max_iterations,
                len(current_critique.defects),
                current_critique.plan_quality_score,
            )

            iteration = PipelineIteration(
                iteration_number=iteration_num,
                critique_before=current_critique,
            )

            # Phase 2: Cascade defects to evolution actions
            actions = self._cascade_defects_to_evolution(
                defects=current_critique.defects,
                auto_remediate=auto_remediate,
                target_types=target_defect_types,
            )
            iteration.evolution_actions = actions
            result.total_actions_taken += len(actions)

            # Phase 3: Verify — re-run critique after evolution
            critique_after = self.critique.run()
            iteration.critique_after = critique_after

            result.iterations.append(iteration)

            # Phase 4: Check if we should stop
            remaining_defects = critique_after.defects
            remaining_critical = [
                d for d in remaining_defects
                if DEFECT_TO_SEVERITY.get(d.defect_type, SeverityLevel.LOW) == SeverityLevel.CRITICAL
            ]

            quality_improved = critique_after.plan_quality_score - current_critique.plan_quality_score

            if not remaining_defects:
                log.info("Pipeline iteration %d: all defects resolved", iteration_num)
                current_critique = critique_after
                result.status = PipelineStatus.COMPLETE_CLEAN
                break

            if not remaining_critical:
                log.info(
                    "Pipeline iteration %d: all critical defects resolved (%d minor remain)",
                    iteration_num, len(remaining_defects),
                )
                current_critique = critique_after
                if iteration_num == max_iterations:
                    result.status = PipelineStatus.COMPLETE_PARTIAL
                else:
                    # Continue to try to fix remaining defects
                    current_critique = critique_after
                    continue

            if quality_improved < _QUALITY_PLATEAU_THRESHOLD and iteration_num > 1:
                log.info(
                    "Pipeline iteration %d: quality plateau (Δ=%.4f) — stopping early",
                    iteration_num, quality_improved,
                )
                current_critique = critique_after
                result.status = PipelineStatus.MAX_ITERATIONS_REACHED
                break

            # Update for next iteration
            current_critique = critique_after

        else:
            # Max iterations reached
            result.status = PipelineStatus.MAX_ITERATIONS_REACHED

        # Final state
        result.final_quality_score = current_critique.plan_quality_score

        # Final status classification
        if result.status == PipelineStatus.IN_PROGRESS:
            final_defects = current_critique.defects
            final_critical = [
                d for d in final_defects
                if DEFECT_TO_SEVERITY.get(d.defect_type, SeverityLevel.LOW) in (
                    SeverityLevel.CRITICAL, SeverityLevel.HIGH,
                )
            ]
            if not final_defects:
                result.status = PipelineStatus.COMPLETE_CLEAN
            elif not final_critical:
                result.status = PipelineStatus.COMPLETE_PARTIAL
            else:
                result.status = PipelineStatus.COMPLETE_BLOCKED

        result.completed_at = time.time()

        log.info(
            "Pipeline run #%d complete: status=%s, quality: %.2f → %.2f, "
            "iterations=%d, actions=%d",
            self._run_count,
            result.status.value,
            result.initial_quality_score,
            result.final_quality_score,
            len(result.iterations),
            result.total_actions_taken,
        )

        return result

    def run_targeted(
        self,
        defect_types: List[DefectType],
        max_iterations: int = 2,
    ) -> PipelineResult:
        """Run the pipeline targeting only specific defect types.

        Useful when a known defect type keeps recurring and you want
        focused remediation without touching other plan aspects.
        """
        return self.run(
            max_iterations=max_iterations,
            target_defect_types=defect_types,
        )

    # ------------------------------------------------------------------
    # Internal: Defect → Evolution cascading
    # ------------------------------------------------------------------

    def _cascade_defects_to_evolution(
        self,
        defects: List[SelfCritiqueDefect],
        auto_remediate: bool = True,
        target_types: Optional[List[DefectType]] = None,
    ) -> List[EvolutionAction]:
        """Map self-critique defects to evolution actions.

        For each defect:
        1. Try automatic remediation first (if enabled and handler exists)
        2. If auto-remediation fails or isn't available, map to evolution trigger
        3. Record the action with full traceability

        Returns list of EvolutionAction records (one per defect).
        """
        actions: List[EvolutionAction] = []

        for defect in defects:
            # Filter by target types if specified
            if target_types is not None and defect.defect_type not in target_types:
                continue

            d_type = defect.defect_type
            trigger = DEFECT_TO_TRIGGER.get(d_type)
            scope = DEFECT_TO_SCOPE.get(d_type, RevisionScope.TASK_ONLY)

            if trigger is None:
                log.debug("No evolution trigger mapped for defect type %s — skipping", d_type.value)
                continue

            action_id = hashlib.sha256(
                f"action_{defect.id}_{time.time()}".encode("utf-8")
            ).hexdigest()[:12]

            action = EvolutionAction(
                action_id=action_id,
                defect_type=d_type,
                defect_id=defect.id,
                affected_node_id=defect.affected_node_id,
                evolution_trigger=trigger,
                revision_scope=scope,
            )

            # Try automatic remediation
            if auto_remediate:
                handler = _AUTO_REMEDIATION_HANDLERS.get(d_type)
                if handler is not None:
                    try:
                        revision = handler(self.hierarchy, defect)
                        if revision is not None:
                            action.auto_remediated = True
                            action.revision_result = revision
                            log.info(
                                "Auto-remediation succeeded for %s on '%s'",
                                d_type.value, defect.affected_node_title,
                            )
                            actions.append(action)
                            continue
                    except Exception as exc:
                        action.error_message = f"Auto-remediation failed: {exc}"
                        log.warning(
                            "Auto-remediation failed for %s: %s",
                            d_type.value, exc,
                        )

            # Fall back to evolution engine
            revision = self._apply_evolution_trigger(trigger, defect, d_type)
            action.revision_result = revision
            if revision is not None:
                action.revision_result = revision
                if not revision.success:
                    action.error_message = revision.rejection_reason
            else:
                action.error_message = "No evolution handler available"

            actions.append(action)

        return actions

    def _apply_evolution_trigger(
        self,
        trigger: EvolutionTrigger,
        defect: SelfCritiqueDefect,
        defect_type: DefectType,
    ) -> Optional[PlanRevisionResult]:
        """Apply the appropriate evolution engine method for a given trigger.

        Dispatches to the PlanEvolutionEngine's trigger-specific methods
        with parameters derived from the defect context.
        """
        node_id = defect.affected_node_id
        node = self.hierarchy.get_node(node_id)
        node_title = node.title if node else defect.affected_node_title

        try:
            if trigger == EvolutionTrigger.VERIFICATION_FAILURE:
                return self.evolution.process_verification_failure(
                    milestone_id=node_id,
                    check_name=f"self_critique_{defect_type.value}",
                    failure_summary=defect.defect_description[:200],
                    failed_task_ids=[node_id],
                )

            elif trigger == EvolutionTrigger.RESOURCE_CONSTRAINT:
                return self.evolution.process_resource_constraint(
                    affected_node_ids=[node_id],
                    constraint_description=defect.defect_description[:200],
                    alternative_approach=defect.recommended_correction[:200],
                )

            elif trigger == EvolutionTrigger.USER_DIRECTIVE:
                # For FLOATING_TASK: the auto_remediate handler should have caught this
                # For ASPIRATIONAL_OBJECTIVE: flag for user attention
                return self.evolution.process_user_directive(
                    directive_text=f"Self-critique: {defect.defect_description[:200]}",
                    target_level=HierarchyLevel.STRATEGIC_OBJECTIVE
                    if defect_type == DefectType.ASPIRATIONAL_OBJECTIVE
                    else HierarchyLevel.MILESTONE,
                    new_priority_objective_id=node_id,
                )

            else:
                log.warning("Unknown evolution trigger: %s", trigger)
                return None

        except Exception as exc:
            log.error(
                "Evolution trigger %s failed for node '%s': %s",
                trigger.value, node_title, exc,
            )
            return None

    def get_statistics(self) -> Dict[str, Any]:
        """Return pipeline statistics for monitoring."""
        return {
            "run_count": self._run_count,
            "defect_types_mapped": {dt.value: {
                "trigger": DEFECT_TO_TRIGGER.get(dt).value if DEFECT_TO_TRIGGER.get(dt) else None,
                "scope": DEFECT_TO_SCOPE.get(dt).value if DEFECT_TO_SCOPE.get(dt) else None,
                "severity": DEFECT_TO_SEVERITY.get(dt).value if DEFECT_TO_SEVERITY.get(dt) else None,
                "has_auto_fix": dt in _AUTO_REMEDIATION_HANDLERS,
            } for dt in DefectType},
            "auto_remediation_handlers": list(_AUTO_REMEDIATION_HANDLERS.keys()),
        }

    def snapshot(self) -> Dict[str, Any]:
        """Quick state snapshot for monitoring/dashboard."""
        return {
            "pipeline": "SelfCritiqueEvolutionPipeline",
            "run_count": self._run_count,
        }
