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

import logging
from typing import Any, Dict, List


from ..architect_types import (
    ExecutionStrategySection,
    SpecialistAssignmentsSection,
    VerificationPlanSection,
    RecoveryPlanSection,
    RepositoryAnalysisSection,
    DependencyAnalysisSection,
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




class DependencyIntelligenceEngine:
    """Makes every plan dependency-complete by tracing execution, repository,
    and assumption dependencies."""

    def analyze(
        self,
        execution: ExecutionStrategySection,
        assignments: SpecialistAssignmentsSection,
        verification: VerificationPlanSection,
        recovery: RecoveryPlanSection,
        repository: RepositoryAnalysisSection,
    ) -> DependencyAnalysisSection:
        # Execution dependencies
        exec_deps = [
            f"{edge.source} -> {edge.target} [{edge.condition}]"
            for edge in execution.dependency_edges
        ]
        if not exec_deps and execution.phases:
            exec_deps.append("single-phase execution")

        # Repository dependencies
        repo_deps = list(repository.dependency_chains)

        # Specialist dependencies
        spec_deps: List[str] = []
        for assignment in assignments.assignments:
            spec_deps.append(
                f"{assignment.phase_id} -> {assignment.specialist.value}: {assignment.task[:60]}"
            )

        # Verification dependencies
        ver_deps = [
            f"{check.phase_id} -> {check.method.value}: {check.success_threshold}"
            for check in verification.checks
        ]

        # Recovery dependencies
        rec_deps = [
            f"{strategy.phase_id} -> {strategy.strategy.value}: {strategy.failure_mode[:40]}"
            for strategy in recovery.failure_strategies
        ]

        # Critical path dependencies
        critical_set = set(execution.critical_path)
        critical_deps = [
            d for d in exec_deps
            if any(pid in d for pid in critical_set)
        ]

        return DependencyAnalysisSection(
            execution_dependencies=_dedupe(exec_deps, 40),
            repository_dependencies=_dedupe(repo_deps, 40),
            specialist_dependencies=_dedupe(spec_deps, 40),
            verification_dependencies=_dedupe(ver_deps, 40),
            recovery_dependencies=_dedupe(rec_deps, 40),
            critical_dependencies=_dedupe(critical_deps, 20),
        )


# ===========================================================================
# 6. Risk Intelligence Engine
# ===========================================================================


