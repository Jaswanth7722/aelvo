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




class SpecialistIntelligenceEngine:
    """Produces specialist assignments that are more specific and useful than
    keyword routing. Includes activation rationale and inter-specialist coordination."""

    def assign(
        self,
        execution: ExecutionStrategySection,
        task_types: Dict[str, bool],
        objective: str,
        context: Dict[str, Any],
    ) -> SpecialistAssignmentsSection:
        assignments: List[SpecialistAssignment] = []
        activated: Set[str] = set()

        for phase in execution.phases:
            pid = phase.id

            # Phase 1: Investigation â€” ARCHITECT + optionally ORACLE
            if "Investigate" in phase.name or "Understand" in phase.name:
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.ARCHITECT,
                    phase_id=pid,
                    task="Analyze the current codebase structure and understand the objective scope",
                    rationale="ARCHITECT has system-level understanding needed before any implementation",
                    estimated_effort=phase.estimated_effort,
                    critical=True,
                ))
                activated.add("ARCHITECT")
                if task_types.get("is_refactor") or task_types.get("is_fix") or task_types.get("is_research"):
                    task_text = "Research codebase patterns and prior context from memory"
                    rationale = "ORACLE provides repository knowledge and historical context"
                    if task_types.get("is_research"):
                        task_text = "Conduct research and gather relevant information from the codebase"
                        rationale = "ORACLE specializes in research and repository intelligence"
                    assignments.append(SpecialistAssignment(
                        specialist=SpecialistRole.ORACLE,
                        phase_id=pid,
                        task=task_text,
                        rationale=rationale,
                        estimated_effort=max(1, phase.estimated_effort // 2),
                        critical=False,
                    ))
                    activated.add("ORACLE")

            # Phase 2: Design â€” ARCHITECT
            elif "Design" in phase.name:
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.ARCHITECT,
                    phase_id=pid,
                    task="Design the implementation approach, select strategy, define execution plan",
                    rationale="ARCHITECT designs the solution architecture before implementation begins",
                    estimated_effort=phase.estimated_effort,
                    critical=True,
                ))
                activated.add("ARCHITECT")

            # Phase 3: Implementation â€” FORGE
            elif any(kw in phase.name for kw in ("Apply", "Implement", "Execute", "Update", "Changes")):
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.FORGE,
                    phase_id=pid,
                    task=phase.description,
                    rationale="FORGE handles all code generation, refactoring, and modification",
                    estimated_effort=phase.estimated_effort,
                    critical=True,
                ))
                activated.add("FORGE")

            # Caller identification â€” ORACLE
            elif "Caller" in phase.name or "Usages" in phase.name:
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.ORACLE,
                    phase_id=pid,
                    task="Search for all usages and callers of targeted code symbols",
                    rationale="ORACLE has code search and repository intelligence capabilities",
                    estimated_effort=phase.estimated_effort,
                    critical=True,
                ))
                activated.add("ORACLE")

            # Security review â€” SENTINEL
            elif "Security" in phase.name:
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.SENTINEL,
                    phase_id=pid,
                    task="Review all changes for security vulnerabilities, trust boundary violations, and attack surface",
                    rationale="SENTINEL specializes in security analysis and vulnerability detection",
                    estimated_effort=phase.estimated_effort,
                    critical=True,
                ))
                activated.add("SENTINEL")

            # Verification â€” FORGE + SENTINEL
            elif "Verify" in phase.name:
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.FORGE,
                    phase_id=pid,
                    task="Run typechecks and test suite to verify correctness",
                    rationale="FORGE handles code verification and test execution",
                    estimated_effort=phase.estimated_effort,
                    critical=True,
                ))
                activated.add("FORGE")
                if task_types.get("has_security"):
                    assignments.append(SpecialistAssignment(
                        specialist=SpecialistRole.SENTINEL,
                        phase_id=pid,
                        task="Verify no security regressions in the changes",
                        rationale="SENTINEL confirms security properties are preserved",
                        estimated_effort=1,
                        critical=False,
                    ))
                    activated.add("SENTINEL")

            # Synthesis â€” HERALD + HERMES
            elif "Synthesize" in phase.name or "Result" in phase.name:
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.HERALD,
                    phase_id=pid,
                    task="Synthesize findings and communicate clear results to the user",
                    rationale="HERALD handles reporting and communication",
                    estimated_effort=phase.estimated_effort,
                    critical=True,
                ))
                activated.add("HERALD")
                assignments.append(SpecialistAssignment(
                    specialist=SpecialistRole.HERMES,
                    phase_id=pid,
                    task="Calibrate response style and tone for the user",
                    rationale="HERMES personalizes communication based on user model",
                    estimated_effort=1,
                    critical=False,
                ))
                activated.add("HERMES")

        # Add TERMINUS if operational commands are needed
        if task_types.get("has_deploy"):
            last_phase = execution.phases[-1].id if execution.phases else "phase_01"
            assignments.append(SpecialistAssignment(
                specialist=SpecialistRole.TERMINUS,
                phase_id=last_phase,
                task="Execute operational commands (build, deploy, migrate) safely",
                rationale="TERMINUS handles DevOps and pipeline operations with safety checks",
                estimated_effort=2,
                critical=True,
            ))
            activated.add("TERMINUS")

        return SpecialistAssignmentsSection(assignments=assignments)


# ===========================================================================
# 9. Verification Design Intelligence Engine
# ===========================================================================


