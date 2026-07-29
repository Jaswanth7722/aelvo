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




class RecoveryDesignEngine:
    """Performs pre-execution failure analysis and produces recovery paths
    embedded in the execution specification before any work begins."""

    def design(
        self,
        execution: ExecutionStrategySection,
        risks: RiskSection,
        task_types: Dict[str, bool],
    ) -> RecoveryPlanSection:
        strategies: List[FailureModeStrategy] = []
        rollback_points: List[str] = []

        for phase in execution.phases:
            rollback_points.append(phase.id)

            # Implementation phases â†’ retry strategy
            if any(kw in phase.name for kw in ("Apply", "Implement", "Execute", "Update")):
                strategies.append(FailureModeStrategy(
                    failure_mode=f"Implementation errors in {phase.name}",
                    phase_id=phase.id,
                    strategy=RecoveryStrategyType.RETRY,
                    fallback_description="Re-attempt the phase with error context injected into specialist prompt",
                    max_retries=3,
                ))
                strategies.append(FailureModeStrategy(
                    failure_mode=f"Dependency or prerequisite missing for {phase.name}",
                    phase_id=phase.id,
                    strategy=RecoveryStrategyType.DECOMPOSE,
                    fallback_description="Break the phase into smaller steps and retry incrementally",
                    max_retries=2,
                ))

            # Verification phase â†’ decompose + escalate
            elif "Verify" in phase.name:
                strategies.append(FailureModeStrategy(
                    failure_mode="Verification failures in tests or type checks",
                    phase_id=phase.id,
                    strategy=RecoveryStrategyType.DECOMPOSE,
                    fallback_description="Isolate failing checks and address them individually",
                    max_retries=2,
                ))

            # Security phase â†’ escalate
            elif "Security" in phase.name:
                strategies.append(FailureModeStrategy(
                    failure_mode="Security vulnerabilities found during review",
                    phase_id=phase.id,
                    strategy=RecoveryStrategyType.ESCALATE,
                    fallback_description="Document vulnerabilities and escalate to user for decision",
                    triggers_human_review=True,
                    max_retries=0,
                ))

        # Risk-based recovery strategies
        for risk in risks.risks:
            strategy_type = {
                "architecture": RecoveryStrategyType.ROLLBACK,
                "security": RecoveryStrategyType.ESCALATE,
                "runtime": RecoveryStrategyType.RETRY,
                "integration": RecoveryStrategyType.DECOMPOSE,
                "coordination": RecoveryStrategyType.SUBSTITUTE,
                "maintenance": RecoveryStrategyType.RETRY,
                "implementation": RecoveryStrategyType.RETRY,
            }.get(risk.category, RecoveryStrategyType.RETRY)

            last_phase = execution.phases[-1].id if execution.phases else "phase_01"
            strategies.append(FailureModeStrategy(
                failure_mode=risk.description[:80],
                phase_id=last_phase,
                strategy=strategy_type,
                fallback_description=risk.contingency or risk.mitigation,
                triggers_human_review=risk.level in (RiskLevel.HIGH, RiskLevel.CRITICAL),
                max_retries=1 if risk.level == RiskLevel.CRITICAL else 2,
            ))

        return RecoveryPlanSection(
            failure_strategies=strategies,
            rollback_points=rollback_points,
            general_approach=(
                "Failures trigger the configured recovery strategy. "
                "If retries are exhausted, the phase is escalated for human review. "
                "Any phase can be rolled back to its starting state."
            ),
        )


# ===========================================================================
# 11. Long-Horizon Intelligence Engine
# ===========================================================================


