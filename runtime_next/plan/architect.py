"""
architect.py â€” ARCHITECT Orchestrator: Master Planning Intelligence for AELVO OMEGA

The ArchitectOrchestrator is NOT a light-weight planner. It is the authoritative
strategic brain that produces structured, dependency-aware, risk-assessed,
verifiable, recoverable execution plans.

Responsibilities:
  1. Objective interpretation â€” understand the goal, infer constraints, detect ambiguity
  2. Repository-aware planning â€” inspect structure, identify affected modules, estimate blast radius
  3. Hierarchical decomposition â€” break work into phases, milestones, dependency-aware sub-tasks
  4. Specialist orchestration â€” decide which specialist does what, define handoff contracts
  5. Verification planning â€” define what must be validated and how
  6. Recovery planning â€” predict failure modes, define fallback strategies
  7. Risk analysis â€” evaluate security, architecture, implementation, runtime, maintenance, coordination risks
  8. Cost analysis â€” estimate complexity, surface area, expected effort, probable regressions
  9. Execution design â€” specify order, prerequisites, completion criteria for each stage
  10. Self-critique â€” review the plan before finalizing, detect missing steps and circular reasoning

Usage:
    orchestrator = ArchitectOrchestrator(repo_intelligence=engine)
    plan = orchestrator.create_plan(objective="Refactor auth module", context={...})
    issues = orchestrator.self_critique(plan)
    if not issues:
        orchestrator.finalize(plan)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..models.events import ArchitectPlanEvent, EventType
from .architect_types import (
    ArchitectPlan,
    SpecialistRole,
    PlanStatus,
)
from .brain import ArchitectIntelligenceBrain, _classify_task_type
from .calibration import PlanCalibrationSystem

log = logging.getLogger("aelvo.plan.architect")


class ArchitectOrchestrator:
    """Master planning intelligence for AELVO Omega.

    Produces structured, dependency-aware, risk-assessed, verifiable plans
    that specialists can execute without confusion.
    """

    def __init__(
        self,
        repo_intelligence: Any = None,
        forge_memory: Any = None,
        event_bus: Optional[Any] = None,
    ):
        self._repo_intel = repo_intelligence
        self._forge = forge_memory
        self._event_bus = event_bus
        self._plans: Dict[str, ArchitectPlan] = {}
        self._brain = ArchitectIntelligenceBrain(repo_intelligence)
        self._calibration = PlanCalibrationSystem()
        self._cognitive_strategic_memory: Optional[Any] = None

    # =========================================================================
    # Public API
    # =========================================================================

    def create_plan(
        self,
        objective: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> ArchitectPlan:
        """Create a complete ArchitectPlan from an objective statement.

        Uses the Architect Intelligence Brain which runs all 13 intelligence
        engines with real reasoning: objective analysis, repository intelligence
        bridging, architectural reasoning, strategic selection, risk analysis,
        execution design, specialist assignment, verification design, recovery
        design, dependency analysis, long-horizon impact, governance analysis,
        and iterative self-critique.

        Args:
            objective: The goal or task description
            context: Shared context from the orchestrator (workspace, constraints,
                    memory, repo intelligence, etc.)

        Returns:
            A fully populated ArchitectPlan
        """
        context = context or {}

        return self._create_plan_with_brain(objective, context)

    def _create_plan_with_brain(
        self,
        objective: str,
        context: Dict[str, Any],
    ) -> ArchitectPlan:
        """Create a plan using the Architect Intelligence Brain.

        The brain runs all 13 intelligence engines with real reasoning:
        objective analysis, repository intelligence bridging, architectural
        reasoning, strategic selection, risk analysis, execution design,
        specialist assignment, verification design, recovery design,
        dependency analysis, long-horizon impact, governance analysis,
        and iterative self-critique.

        Calibration adjustments from past outcomes are injected into the
        context so every engine can adapt its reasoning based on what has
        been learned from previous plan executions.
        """
        plan_id = self._generate_plan_id(objective)

        # Inject calibration adjustments from past outcomes
        task_types = _classify_task_type(objective)
        task_type_label = "refactor" if task_types.get("is_refactor") else (
            "fix" if task_types.get("is_fix") else (
                "feature" if task_types.get("is_feature") else "general"
            )
        )
        strategy_class = context.get("strategy_class", task_type_label)
        adjustments = self._calibration.get_adjustments_for_task(task_type_label, strategy_class)
        if adjustments:
            context["_calibration_adjustments"] = [
                a.model_dump() for a in adjustments
            ]
            log.info(
                "Injecting %d calibration adjustments for task_type=%s, strategy=%s",
                len(adjustments), task_type_label, strategy_class,
            )

        # Run the brain
        strategic_output = self._brain.reason(objective, context)

        # Assemble the plan
        plan = self._brain.assemble_plan(objective, strategic_output, plan_id, context)

        # Store plan
        self._plans[plan_id] = plan

        log.info(
            "Brain-created plan %s: %d phases, %d specialists, %d checks, score=%.2f, approved=%s",
            plan_id[:12],
            len(plan.execution_strategy.phases),
            len(plan.specialist_assignments.assignments),
            len(plan.verification_plan.checks),
            plan.self_review.score,
            plan.final_approved_plan.approved,
        )

        # Persist plan to memory for cross-session recall
        self.persist_plan_to_memory(plan)

        self._emit_plan_event(plan, EventType.PLAN_CREATED, failure_reason=None)
        return plan

    def get_plan(self, plan_id: str) -> Optional[ArchitectPlan]:
        """Retrieve a previously created plan."""
        return self._plans.get(plan_id)

    def list_plans(self) -> List[str]:
        """List all plan IDs."""
        return list(self._plans.keys())

    def finalize(self, plan_id: str) -> bool:
        """Mark a plan as validated and ready for execution."""
        plan = self._plans.get(plan_id)
        if plan is None:
            return False
        issues = plan.validate_complete()
        if issues:
            log.warning("Cannot finalize plan %s: %s", plan_id[:12], "; ".join(issues))
            self._emit_plan_event(plan, EventType.PLAN_FAILED, failure_reason="; ".join(issues))
            return False
        if not plan.self_review.passes_review():
            log.warning("Cannot finalize plan %s: self-review failed", plan_id[:12])
            self._emit_plan_event(plan, EventType.PLAN_FAILED, failure_reason="Self-review failed")
            return False
        if not plan.final_approved_plan.approved:
            reason = "; ".join(plan.final_approved_plan.blocking_reasons) or "Strategic approval required"
            log.warning("Cannot finalize plan %s: %s", plan_id[:12], reason)
            self._emit_plan_event(plan, EventType.PLAN_FAILED, failure_reason=reason)
            return False
        plan.status = PlanStatus.VALIDATED
        plan.updated_at = datetime.now(timezone.utc)
        self._emit_plan_event(plan, EventType.PLAN_VALIDATED, failure_reason=None)
        return True

    def self_critique(self, plan: ArchitectPlan) -> List[str]:
        """Critically review a plan and return issues found.

        This is a deterministic check that can be called at any point.
        It validates structural integrity, completeness, and consistency
        of the plan.
        """
        issues: List[str] = []

        # 1. Check all 10 sections exist
        section_issues = plan.validate_complete()
        issues.extend(section_issues)

        # 2. Check dependency consistency
        if plan.execution_strategy.phases:
            phase_ids = {p.id for p in plan.execution_strategy.phases}
            for edge in plan.execution_strategy.dependency_edges:
                if edge.source not in phase_ids:
                    issues.append(f"Dependency edge source '{edge.source}' not in any phase")
                if edge.target not in phase_ids:
                    issues.append(f"Dependency edge target '{edge.target}' not in any phase")

        # 3. Check phase ordering consistency
        for phase in plan.execution_strategy.phases:
            for prereq in phase.prerequisites:
                prereq_phase = next((p for p in plan.execution_strategy.phases if p.id == prereq), None)
                if prereq_phase and prereq_phase.order >= phase.order:
                    issues.append(
                        f"Phase '{phase.id}' (order {phase.order}) depends on "
                        f"'{prereq}' (order {prereq_phase.order}) which is not before it"
                    )

        # 4. Check specialist assignments reference valid phases
        phase_ids = {p.id for p in plan.execution_strategy.phases} if plan.execution_strategy.phases else set()
        for assignment in plan.specialist_assignments.assignments:
            if assignment.phase_id and assignment.phase_id not in phase_ids:
                issues.append(
                    f"Specialist assignment for {assignment.specialist.value} references "
                    f"unknown phase '{assignment.phase_id}'"
                )

        # 5. Check verification checks reference valid phases
        for check in plan.verification_plan.checks:
            if check.phase_id and check.phase_id not in phase_ids:
                issues.append(
                    f"Verification check '{check.description[:40]}' references "
                    f"unknown phase '{check.phase_id}'"
                )

        # 6. Check recovery strategies reference valid phases
        for fs in plan.recovery_plan.failure_strategies:
            if fs.phase_id and fs.phase_id not in phase_ids:
                issues.append(
                    f"Recovery strategy for '{fs.failure_mode[:40]}' references "
                    f"unknown phase '{fs.phase_id}'"
                )

        # 7. Check for minimum content
        if plan.objective.goal and len(plan.objective.goal) < 10:
            issues.append("Objective is too short (< 10 characters)")

        if len(plan.objective.success_criteria) < 1:
            issues.append("No success criteria defined")

        if len(plan.completion_criteria.criteria) < 1:
            issues.append("No completion criteria defined")

        if (
            plan.metadata.get("architect_intelligence_contract") == "omega-14"
            and not plan.final_approved_plan.approved
        ):
            issues.extend(
                f"Strategic approval blocked: {reason}"
                for reason in plan.final_approved_plan.blocking_reasons
            )

        return issues

    def estimate_cost(self, plan: ArchitectPlan) -> Dict[str, Any]:
        """Estimate the cost/complexity of a plan."""
        total_effort = sum(p.estimated_effort for p in plan.execution_strategy.phases)
        file_count = len(plan.impact_analysis.affected_files)
        module_count = len(plan.impact_analysis.affected_modules)
        assignment_count = len(plan.specialist_assignments.assignments)
        risk_score = sum(r.risk_score for r in plan.risks.risks) / max(1, len(plan.risks.risks))

        return {
            "total_estimated_effort": total_effort,
            "affected_files": file_count,
            "affected_modules": module_count,
            "specialist_assignments": assignment_count,
            "verification_checks": len(plan.verification_plan.checks),
            "average_risk_score": round(risk_score, 3),
            "estimated_regression_probability": round(
                min(1.0, (file_count * 0.05 + risk_score * 0.3)), 3
            ),
            "critical_path_length": len(plan.execution_strategy.critical_path),
        }

    # =========================================================================
    # Memory Persistence â€” persists strategic objectives, roadmap, and plan
    # outcomes to forge_memory for cross-session recall.
    # =========================================================================

    def persist_plan_to_memory(self, plan: ArchitectPlan) -> None:
        """Persist strategic objectives, roadmap history, and plan outcomes
        to forge_memory for cross-session recall.

        Stores:
        - Strategic objective as a STRATEGIC_PLAN memory entry
        - Roadmap milestones as a ROADMAP memory entry
        - Plan summary with verification and risk context
        """
        if self._forge is None:
            return

        try:
            objective = plan.objective.goal[:500]
            plan_id = plan.id[:16]

            # 1. Persist strategic objective
            strategic_content = (
                f"STRATEGIC PLAN {plan_id}: {objective}\n"
                f"Score: {plan.self_review.score:.2f}, "
                f"Approved: {plan.final_approved_plan.approved}\n"
                f"Phases: {len(plan.execution_strategy.phases)}, "
                f"Specialists: {len(plan.specialist_assignments.assignments)}, "
                f"Verification checks: {len(plan.verification_plan.checks)}\n"
                f"Risk level: {plan.risks.overall_level.value}\n"
            )
            self._forge.save_code_pattern(
                description=strategic_content,
                pattern_type="strategic_plan",
                context=f"plan_id={plan_id},objective={objective[:100]}",
            )
            log.info("Persisted strategic objective for plan %s to memory", plan_id)

            # 2. Persist roadmap if available
            roadmap = plan.long_term_impact.roadmap
            if roadmap and roadmap.milestones:
                roadmap_content = (
                    f"ROADMAP for {plan_id}: {len(roadmap.milestones)} milestones, "
                    f"multi_session={roadmap.multi_session}, "
                    f"completion_confidence={roadmap.completion_confidence}\n"
                )
                for ms in roadmap.milestones[:5]:
                    roadmap_content += (
                        f"  Milestone {ms.id}: {ms.description[:80]} "
                        f"(session {ms.target_session}, confidence={ms.confidence})\n"
                    )
                if roadmap.resource_budget:
                    budget_str = ", ".join(
                        f"{k}={v}" for k, v in roadmap.resource_budget.items()
                    )
                    roadmap_content += f"Resource budget: {budget_str}\n"
                if roadmap.plan_evolution_path:
                    roadmap_content += f"Evolution path: {roadmap.plan_evolution_path}\n"

                self._forge.save_code_pattern(
                    description=roadmap_content,
                    pattern_type="roadmap",
                    context=f"plan_id={plan_id},milestones={len(roadmap.milestones)}",
                )
                log.info(
                    "Persisted roadmap for plan %s (%d milestones)",
                    plan_id, len(roadmap.milestones),
                )

            # 3. Persist plan context and metadata summary
            context_content = (
                f"PLAN CONTEXT {plan_id}: {objective[:200]}\n"
                f"Governance escalation: {plan.governance_analysis.escalation_required}\n"
                f"Protected components: {len(plan.governance_analysis.protected_components)}\n"
                f"Repository status: {plan.repository_analysis.intelligence_status}\n"
                f"Architecture layers: {len(plan.repository_analysis.architecture_layers)}\n"
            )
            if plan.long_term_impact.maintenance_effects:
                context_content += "Maintenance effects:\n"
                for effect in plan.long_term_impact.maintenance_effects[:3]:
                    context_content += f"  - {effect[:80]}\n"
            if plan.long_term_impact.recommendations:
                context_content += "Recommendations:\n"
                for rec in plan.long_term_impact.recommendations[:3]:
                    context_content += f"  - {rec[:80]}\n"

            self._forge.save_code_pattern(
                description=context_content,
                pattern_type="plan_context",
                context=f"plan_id={plan_id},governance={plan.governance_analysis.escalation_required}",
            )

            # 4. Persist to StrategicMemory if cognitive engine is linked
            if self._cognitive_strategic_memory:
                try:
                    from cognition.types import MemoryType

                    self._cognitive_strategic_memory.store(
                        memory_type=MemoryType.STRATEGIC_PLAN,
                        content=strategic_content[:1000],
                        importance=0.8 if plan.final_approved_plan.approved else 0.5,
                        tags=["strategic_plan", plan.status.value],
                    )
                    if roadmap:
                        self._cognitive_strategic_memory.store(
                            memory_type=MemoryType.ROADMAP,
                            content=roadmap_content[:1000],
                            importance=0.7,
                            tags=["roadmap", f"milestones_{len(roadmap.milestones)}"],
                        )
                except Exception as e:
                    log.debug("Failed to persist to StrategicMemory: %s", e)

        except Exception as e:
            log.warning("Failed to persist plan to memory: %s", e)

    def link_strategic_memory(self, strategic_memory: Any) -> None:
        """Link a StrategicMemory instance for cognitive memory persistence."""
        self._cognitive_strategic_memory = strategic_memory

    # =========================================================================
    # Integration helpers
    # =========================================================================

    def enrich_context_with_plan(
        self,
        plan: ArchitectPlan,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Inject a completed plan into a shared context dict for downstream specialists.

        Specialists will be able to see their assignments, what phases exist,
        and what verification/recovery expectations are.
        """
        context["architect_plan"] = plan.model_dump(mode="json")
        context["architect_plan_id"] = plan.id
        context["architect_plan_display"] = plan.to_terminal_display()
        context["architect_plan_sections"] = {
            "objective": plan.objective.goal,
            "execution_phases": len(plan.execution_strategy.phases),
            "specialist_assignments": [
                a.specialist.value for a in plan.specialist_assignments.assignments
            ],
            "verification_checks": len(plan.verification_plan.checks),
            "risks": plan.risks.overall_level.value,
            "self_review_score": plan.self_review.score,
            "repository_intelligence_status": plan.repository_analysis.intelligence_status,
            "governance_escalation_required": plan.governance_analysis.escalation_required,
            "final_approval": plan.final_approved_plan.approval_status,
        }
        context["architect_dependency_analysis"] = plan.dependency_analysis.model_dump(mode="json")
        context["architect_governance"] = plan.governance_analysis.model_dump(mode="json")
        context["architect_long_term_impact"] = plan.long_term_impact.model_dump(mode="json")
        context["architect_final_approval"] = plan.final_approved_plan.model_dump(mode="json")
        context["architect_coordination_decisions"] = plan.metadata.get("coordination_decisions", [])
        context["architect_verification_strategy"] = [
            {
                "phase_id": check.phase_id,
                "method": check.method.value,
                "blocking": check.is_blocking,
                "success_threshold": check.success_threshold,
            }
            for check in plan.verification_plan.checks
        ]
        context["architect_recovery_strategy"] = [
            {
                "phase_id": strategy.phase_id,
                "failure_mode": strategy.failure_mode,
                "strategy": strategy.strategy.value,
                "fallback": strategy.fallback_description,
            }
            for strategy in plan.recovery_plan.failure_strategies
        ]

        # Inject per-specialist assignments into context
        for role in SpecialistRole:
            assignments = plan.specialist_assignments.get_by_specialist(role)
            if assignments:
                context[f"{role.value}_assignments"] = [
                    {"task": a.task, "phase_id": a.phase_id, "critical": a.critical}
                    for a in assignments
                ]

        return context

    def build_plan_from_conversation(
        self,
        objective: str,
        repo_intel_output: Optional[Dict[str, Any]] = None,
        memory_context: Optional[Dict[str, Any]] = None,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> ArchitectPlan:
        """Build a plan from conversation context.

        This is a convenience wrapper for the cognitive engine.
        """
        context = {
            "task": objective,
            "constraints": constraints or {},
            "project": "",
        }

        if repo_intel_output:
            context["affected_files"] = repo_intel_output.get("files", [])
            context["relevant_modules"] = repo_intel_output.get("modules", [])

        if memory_context:
            context.update(memory_context)

        return self.create_plan(objective, context)

    def get_calibration_summary(self) -> Dict[str, Any]:
        """Get calibration system summary."""
        return self._calibration.get_calibration_summary()

    # =========================================================================
    # Event Emission
    # =========================================================================

    def _emit_plan_event(
        self,
        plan: Optional[ArchitectPlan],
        event_type: EventType,
        failure_reason: Optional[str] = None,
    ) -> None:
        """Emit a plan lifecycle event through the event bus.

        Supports both sync and async publish methods. If publish returns a
        coroutine, it is scheduled via the event loop (fire-and-forget).
        If publish is synchronous, it is called directly for immediate effect.

        Args:
            plan: The plan the event relates to (None if unknown)
            event_type: The lifecycle event type (PLAN_CREATED, PLAN_VALIDATED, PLAN_FAILED)
            failure_reason: Reason for failure (only for PLAN_FAILED)
        """
        if self._event_bus is None:
            return

        # Build the event payload
        specialist_roles = []
        phase_count = 0
        if plan is not None:
            specialist_roles = sorted({a.specialist.value for a in plan.specialist_assignments.assignments})
            phase_count = len(plan.execution_strategy.phases)

        event = ArchitectPlanEvent(
            id=f"plan_{event_type.value}_{hash(plan.id if plan else failure_reason or 'unknown')}",
            type=event_type,
            payload={
                "plan_id": plan.id if plan else "unknown",
                "event_type": event_type.value,
                "failure_reason": failure_reason,
            },
            plan_id=plan.id if plan else "unknown",
            plan_title=plan.title if plan else "",
            objective=(plan.objective.goal[:200] if plan else ""),
            phase_count=phase_count,
            specialist_roles=specialist_roles,
            risk_level=(plan.risks.overall_level.value if plan else ""),
            verification_count=len(plan.verification_plan.checks) if plan else 0,
            self_review_score=plan.self_review.score if plan else 0.0,
            failure_reason=failure_reason,
        )

        # Handle both sync and async publish
        try:
            result = self._event_bus.publish(event)
            if asyncio.iscoroutine(result):
                try:
                    # Check if we're in a running event loop
                    loop = asyncio.get_running_loop()
                    loop.create_task(result)
                except RuntimeError:
                    # No running loop â€” run synchronously
                    asyncio.run(result)
            # If result is not a coroutine, publish was synchronous â€” event is already recorded
        except Exception as e:
            log.debug("Failed to emit plan lifecycle event: %s", e)

    # =========================================================================
    # Internals
    # =========================================================================

    def _generate_plan_id(self, objective: str) -> str:
        raw = f"arch_plan_{objective}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


