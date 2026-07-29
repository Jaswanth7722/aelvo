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




class GovernanceIntelligenceEngine:
    """Protects critical infrastructure and escalates dangerous changes."""

    _SECURITY_HINTS = ("security", "auth", "oauth", "permission", "credential", "secret", "token")
    _DANGEROUS_HINTS = ("delete", "drop", "destroy", "reset", "production", "irreversible")

    def analyze(
        self,
        objective: str,
        repository: RepositoryAnalysisSection,
        impact: ImpactAnalysisSection,
        risks: RiskSection,
        context: Dict[str, Any],
    ) -> GovernanceAnalysisSection:
        lower = objective.lower()
        affected = {p.replace("\\", "/").lower() for p in impact.affected_files}

        # Check if any protected components are affected
        targeted_protected = [
            c for c in repository.protected_components
            if any(c.replace("\\", "/").lower() in p or p in c.replace("\\", "/").lower()
                   for p in affected)
        ]

        critical_risks = [r.description for r in risks.risks if r.level == RiskLevel.CRITICAL]
        dangerous = [h for h in self._DANGEROUS_HINTS if h in lower]

        escalation_required = bool(
            context.get("approval_required")
            or targeted_protected
            or critical_risks
            or dangerous
        )

        rationale: List[str] = []
        if targeted_protected:
            rationale.append("The proposed scope intersects protected repository components")
        if critical_risks:
            rationale.append("At least one critical strategic risk requires stronger validation")
        if dangerous:
            rationale.append(f"Dangerous operation signals detected: {', '.join(dangerous)}")
        if context.get("approval_required"):
            rationale.append("Upstream governance explicitly requires approval")

        requirements: List[str] = []
        if escalation_required:
            requirements.extend([
                "Obtain explicit approval before dangerous execution",
                "Preserve a rollback point before modifying protected infrastructure",
                "Run blocking verification after the guarded change",
            ])

        security_sensitive = [h for h in self._SECURITY_HINTS if h in lower]

        return GovernanceAnalysisSection(
            protected_components=_dedupe(targeted_protected, 10),
            security_sensitive_systems=_dedupe(security_sensitive, 10),
            requirements=_dedupe(requirements, 10),
            escalation_required=escalation_required,
            rationale=_dedupe(rationale, 10),
        )


# ===========================================================================
# 13. Self-Critique Engine
# ===========================================================================


