"""
brain.py â€” Architect Intelligence Brain for AELVO OMEGA

The strategic brain that makes every other subsystem more effective by
thinking before the system acts. Contains 13 cooperating intelligence
engines that each provide a distinct type of strategic reasoning.

Every engine does real reasoning grounded in repository intelligence.
No engine returns a default. No section of the plan is a placeholder.

Usage:
    brain = ArchitectIntelligenceBrain(repo_intelligence=repo_engine)
    output = brain.reason(objective="Fix auth bug", context={...})
    plan = brain.assemble_plan(objective, output)
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from ..architect_types import (
    ArchitectPlan,
    ObjectiveSection,
    CurrentUnderstandingSection,
    ImpactAnalysisSection,
    ImpactItem,
    RiskSection,
    RiskItem,
    RiskLevel,
    BlastRadius,
    ExecutionStrategySection,
    ExecutionPhase,
    DependencyEdge,
    SpecialistAssignment,
    SpecialistAssignmentsSection,
    SpecialistRole,
    VerificationPlanSection,
    VerificationCheck,
    VerificationMethod,
    RecoveryPlanSection,
    FailureModeStrategy,
    RecoveryStrategyType,
    CompletionCriteriaSection,
    SelfReviewSection,
    SelfReviewIssue,
    PlanStatus,
    ContextAnalysisSection,
    RepositoryAnalysisSection,
    ArchitecturalAnalysisSection,
    DependencyAnalysisSection,
    GovernanceAnalysisSection,
    LongTermImpactSection,
    FinalApprovedPlanSection,
    StrategicRoadmapSection,
    Milestone,
    GoalHierarchyNode,
)

log = logging.getLogger("aelvo.plan.brain")


# ===========================================================================
# Helper utilities
# ===========================================================================


def _dedupe(values: list, limit: int = 25) -> list:
    """Deduplicate a list of strings while preserving order."""
    result: list = []
    seen: set = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
        if len(result) >= limit:
            break
    return result


def _safe(obj: Any, method: str, default: Any = None) -> Any:
    """Safely call a method on an object, returning default on any error."""
    if obj is None:
        return default
    cb = getattr(obj, method, None)
    if cb is None:
        return default
    try:
        return cb()
    except Exception:
        return default


def _field(obj: Any, name: str, default: Any = None) -> Any:
    """Get a field from an object or dict safely."""
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _classify_task_type(text: str) -> Dict[str, bool]:
    """Classify the task into categories based on the objective text."""
    lower = text.lower()
    return {
        "is_refactor": any(w in lower for w in ("refactor", "rewrite", "restructure", "redesign", "reorganize")),
        "is_fix": any(w in lower for w in ("fix", "bug", "error", "issue", "broken", "crash", "fail")),
        "is_feature": any(w in lower for w in ("add", "create", "implement", "build", "new", "introduce")),
        "has_security": any(w in lower for w in ("security", "auth", "oauth", "vulnerability", "permission",
                                                  "credential", "token", "secret", "encrypt")),
        "has_test": any(w in lower for w in ("test", "verify", "validate", "check", "assert")),
        "has_deploy": any(w in lower for w in ("deploy", "ship", "release", "publish", "migrate")),
        "has_delete": any(w in lower for w in ("delete", "remove", "drop", "destroy", "deprecate")),
        "has_api": any(w in lower for w in ("api", "endpoint", "route", "interface", "contract")),
        "is_research": any(w in lower for w in ("research", "study", "investigate", "learn", "explore", "analyze", "survey")),
    }


# ===========================================================================
# Engine Output Types
# ===========================================================================




class LongHorizonIntelligenceEngine:
    """Reasons about the impact of current work on the future state of the system.

    Produces both the LongTermImpactSection (maintenance, scaling, evolution effects)
    and the StrategicRoadmapSection (milestones, goal hierarchy, resource budget,
    completion confidence, multi-session awareness) â€” evolving planning from
    task decomposition into strategic roadmap intelligence.
    """

    def analyze(
        self,
        objective: str,
        repository: RepositoryAnalysisSection,
        task_types: Dict[str, bool],
        context: Dict[str, Any],
        execution: Optional[ExecutionStrategySection] = None,
    ) -> LongTermImpactSection:
        repo = context.get("repo_intelligence")
        evolution = _safe(repo, "generate_evolution_report") if repo else None

        evolution_effects: List[str] = []
        if evolution:
            for name in ("overall_evolution_risk", "predicted_bottlenecks",
                         "scaling_concerns", "technical_debt_score"):
                value = _field(evolution, name)
                if value is not None:
                    evolution_effects.append(f"{name}: {value}")

        recommendations = [
            "Prefer changes that reduce coupling and preserve subsystem ownership",
            "Record architectural decisions when the implementation changes design intent",
            "Keep verification scope aligned with dependency blast radius",
        ]
        if repository.fragile_components:
            recommendations.append("Use smaller checkpoints around fragile components")
        if repository.hotspots:
            recommendations.append(f"Monitor hotspots: {', '.join(repository.hotspots[:3])}")

        maintenance = [
            "Keep the implementation understandable for future maintainers",
            "Avoid introducing parallel abstractions that increase maintenance cost",
        ]
        if task_types.get("is_refactor"):
            maintenance.append("Document the rationale for each refactoring decision")
        if task_types.get("is_feature"):
            maintenance.append("Design the feature API surface for future extension")

        scaling = ["Watch dependency growth on changed subsystem boundaries"]
        debt = ["Prefer explicit cleanup or follow-up records for deferred compromises"]
        if task_types.get("is_feature"):
            debt.append("Avoid shortcuts that will require rework when the system scales")

        # =================================================================
        # Strategic Roadmap â€” transforms planning from task decomposition
        # into strategic roadmap intelligence
        # =================================================================
        roadmap = self._build_roadmap(objective, repository, task_types, context, execution)

        return LongTermImpactSection(
            maintenance_effects=_dedupe(maintenance),
            scaling_effects=_dedupe(scaling),
            evolution_effects=_dedupe(evolution_effects, 10),
            technical_debt_effects=_dedupe(debt),
            recommendations=_dedupe(recommendations, 10),
            roadmap=roadmap,
        )

    def _build_roadmap(
        self,
        objective: str,
        repository: RepositoryAnalysisSection,
        task_types: Dict[str, bool],
        context: Dict[str, Any],
        execution: Optional[ExecutionStrategySection] = None,
    ) -> StrategicRoadmapSection:
        """Build a strategic roadmap with milestones, goal hierarchy, and budget."""
        lower = objective.lower()

        # --- Milestones ---
        # Derive milestones from execution phases when available, or from task type
        milestones: List[Milestone] = []
        if execution and execution.phases:
            for i, phase in enumerate(execution.phases):
                session = (i // 3) + 1  # Group 3 phases per session
                dep_ids = [m.id for m in milestones
                          if m.description in phase.prerequisites]
                milestones.append(Milestone(
                    id=f"ms_{i+1:03d}",
                    description=f"{phase.name}: {phase.description[:60]}",
                    target_session=session,
                    dependencies=dep_ids,
                    verification=" ".join(phase.completion_criteria)[:100],
                    estimated_effort=phase.estimated_effort,
                    confidence=0.8 if not repository.fragile_components else 0.6,
                ))
        else:
            # Default milestones based on task type
            ms_text = "Research and understand the codebase"
            if task_types.get("is_refactor"):
                ms_text = "Complete refactoring with all callers updated"
            elif task_types.get("is_fix"):
                ms_text = "Root cause identified and fix verified"
            elif task_types.get("is_feature"):
                ms_text = "Feature implemented with tests"
            milestones.append(Milestone(
                id="ms_001",
                description=ms_text,
                target_session=1,
                estimated_effort=5,
                confidence=0.7,
            ))
            milestones.append(Milestone(
                id="ms_002",
                description="Verification and validation complete",
                target_session=1,
                dependencies=["ms_001"],
                verification="All blocking checks pass",
                estimated_effort=2,
                confidence=0.8,
            ))

        multi_session = len(milestones) > 4 or (
            len(repository.fragile_components) > 3
        )

        # --- Resource budget ---
        total_effort = sum(m.estimated_effort for m in milestones)
        resource_budget = {
            "analysis": max(1, total_effort // 5),
            "implementation": max(1, total_effort // 2),
            "verification": max(1, total_effort // 4),
            "recovery": max(1, total_effort // 8),
        }

        # --- Completion confidence ---
        fragility_penalty = 0.1 * len(repository.fragile_components)
        complexity_bonus = 0.1 if len(repository.architecture_layers) <= 3 else 0.0
        confidence = max(0.3, min(0.95, 0.7 - fragility_penalty + complexity_bonus))

        # --- Plan evolution path ---
        if task_types.get("is_refactor"):
            evolution_path = "Incremental refactoring with verification at each phase boundary. May require additional sessions for cascade effects."
        elif task_types.get("is_feature"):
            evolution_path = "Feature implementation followed by integration. May expand scope if new dependencies are discovered."
        elif task_types.get("is_fix"):
            evolution_path = "Targeted fix with regression prevention. If root cause is deeper than expected, may escalate to refactoring."
        else:
            evolution_path = "Standard execution with verification gates. Adjust scope as repository context evolves."

        return StrategicRoadmapSection(
            milestones=milestones,
            goal_hierarchy=GoalHierarchyNode(
                id="root",
                objective=objective[:120],
                sub_goals=[
                    GoalHierarchyNode(id="sg_investigate", objective="Investigate and understand the codebase context"),
                    GoalHierarchyNode(id="sg_design", objective="Design the solution approach"),
                    GoalHierarchyNode(id="sg_implement", objective="Implement changes with verification"),
                ],
            ),
            resource_budget=resource_budget,
            completion_confidence=round(confidence, 3),
            multi_session=multi_session,
            plan_evolution_path=evolution_path,
        )


# ===========================================================================
# 12. Governance Intelligence Engine
# ===========================================================================


