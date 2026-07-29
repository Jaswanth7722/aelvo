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
    VerificationPlanSection,
    VerificationCheck,
    VerificationMethod,
    RepositoryAnalysisSection,
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




class VerificationDesignEngine:
    """Designs the verification strategy before any execution begins.
    Layered: cheap verification first, expensive at phase boundaries.
    Includes negative verification (regression checks)."""

    def design(
        self,
        execution: ExecutionStrategySection,
        task_types: Dict[str, bool],
        repo_analysis: RepositoryAnalysisSection,
        objective: str,
    ) -> VerificationPlanSection:
        checks: List[VerificationCheck] = []
        verify_phase = None
        for phase in execution.phases:
            if "Verify" in phase.name:
                verify_phase = phase.id
                break
        if not verify_phase:
            verify_phase = execution.phases[-1].id if execution.phases else "phase_01"

        # --- Layer 1: Cheap verification (every phase) ---
        checks.append(VerificationCheck(
            description="Type check all modified files",
            method=VerificationMethod.TYPECHECK,
            phase_id=verify_phase,
            is_blocking=True,
            success_threshold="No type errors in modified files",
        ))

        checks.append(VerificationCheck(
            description="Lint all modified files for code quality and conventions",
            method=VerificationMethod.LINT,
            phase_id=verify_phase,
            is_blocking=False,
            success_threshold="No lint errors in modified files",
        ))

        # --- Layer 2: Medium verification ---
        checks.append(VerificationCheck(
            description="Verify architectural consistency (layer boundaries, dependency rules)",
            method=VerificationMethod.ARCHITECTURE_CHECK,
            phase_id=verify_phase,
            is_blocking=True,
            success_threshold="No architecture violations introduced",
        ))

        # --- Layer 3: Task-specific verification ---
        if task_types.get("has_test") or task_types.get("is_fix"):
            checks.append(VerificationCheck(
                description="Run unit tests for affected modules",
                method=VerificationMethod.UNIT_TEST,
                phase_id=verify_phase,
                is_blocking=True,
                success_threshold="All tests pass",
            ))

        if task_types.get("has_security"):
            checks.append(VerificationCheck(
                description="Security scan of all changes",
                method=VerificationMethod.SECURITY_SCAN,
                phase_id=verify_phase,
                is_blocking=True,
                success_threshold="No security vulnerabilities found",
            ))

        if task_types.get("is_refactor"):
            checks.append(VerificationCheck(
                description="Verify behavior preservation through comparison",
                method=VerificationMethod.COMPARISON,
                phase_id=verify_phase,
                is_blocking=True,
                success_threshold="Behavior matches before refactoring",
            ))

        # --- Negative verification (regression checks) ---
        if repo_analysis.fragile_components:
            checks.append(VerificationCheck(
                description=f"Regression check for fragile components: {', '.join(repo_analysis.fragile_components[:3])}",
                method=VerificationMethod.UNIT_TEST,
                phase_id=verify_phase,
                is_blocking=True,
                success_threshold="Fragile component tests still pass",
            ))

        return VerificationPlanSection(checks=checks)


# ===========================================================================
# 10. Recovery Design Intelligence Engine
# ===========================================================================


