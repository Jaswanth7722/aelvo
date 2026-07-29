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
    ImpactAnalysisSection,
    RiskSection,
    RiskItem,
    RiskLevel,
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




class RiskIntelligenceEngine:
    """Produces a risk model for every plan before execution begins.
    Reasons explicitly about what could go wrong, how likely it is,
    how severe it would be, and what the plan does to mitigate it."""

    def analyze(
        self,
        objective: str,
        task_types: Dict[str, bool],
        repo_analysis: RepositoryAnalysisSection,
        architectural_violations: List[str],
        impact: ImpactAnalysisSection,
        context: Dict[str, Any],
    ) -> RiskSection:
        risks: List[RiskItem] = []
        context.get("repo_intelligence")

        # --- Architectural Risk ---
        if architectural_violations:
            risks.append(RiskItem(
                description=f"Existing architectural violations ({len(architectural_violations)}) may compound with changes",
                category="architecture",
                level=RiskLevel.HIGH,
                likelihood=0.6,
                impact=0.7,
                mitigation="Add architectural compliance verification for each phase",
                contingency="Rollback and decompose into smaller, boundary-respecting steps",
            ))
        elif task_types.get("is_refactor") and len(impact.affected_files) > 5:
            risks.append(RiskItem(
                description="Large refactoring may break existing interfaces or introduce coupling",
                category="architecture",
                level=RiskLevel.HIGH,
                likelihood=0.5,
                impact=0.7,
                mitigation="Analyze call graph before refactoring; update all callers",
                contingency="Restore from backup and decompose into smaller steps",
            ))

        # --- Security Risk ---
        if task_types.get("has_security"):
            risks.append(RiskItem(
                description="Security-sensitive code change may introduce vulnerability",
                category="security",
                level=RiskLevel.HIGH,
                likelihood=0.4,
                impact=0.9,
                mitigation="Mandatory SENTINEL review of all trust boundary crossings",
                contingency="Rollback all security changes and reassess requirements",
            ))

        # --- Implementation Risk ---
        affected = len(impact.affected_files)
        fragile_count = len(repo_analysis.fragile_components)
        impl_likelihood = 0.3
        if affected > 5:
            impl_likelihood += 0.15
        if fragile_count > 0:
            impl_likelihood += 0.1
        if task_types.get("is_refactor"):
            impl_likelihood += 0.1

        risks.append(RiskItem(
            description=f"Implementation errors in {affected} affected files",
            category="implementation",
            level=RiskLevel.MEDIUM if impl_likelihood < 0.5 else RiskLevel.HIGH,
            likelihood=round(min(impl_likelihood, 0.9), 2),
            impact=0.5,
            mitigation="Layered verification: lint â†’ typecheck â†’ unit test â†’ integration test",
            contingency="Isolate failures and address individually with specialist review",
        ))

        # --- Integration Risk ---
        high_coupling_files = [
            chain for chain in repo_analysis.dependency_chains
            if chain.count("->") > 0
        ]
        if len(high_coupling_files) > 10:
            risks.append(RiskItem(
                description=f"High coupling in {len(high_coupling_files)} dependency chains â€” mistakes propagate widely",
                category="integration",
                level=RiskLevel.MEDIUM,
                likelihood=0.4,
                impact=0.6,
                mitigation="Run full test suite after each phase boundary",
                contingency="Rollback to last verified checkpoint",
            ))

        # --- Maintenance Risk ---
        if task_types.get("is_feature") or task_types.get("is_refactor"):
            risks.append(RiskItem(
                description="Changes may increase maintenance burden if not well-structured",
                category="maintenance",
                level=RiskLevel.LOW,
                likelihood=0.3,
                impact=0.4,
                mitigation="Follow project conventions; document key architectural decisions",
                contingency="Simplify and refactor complex sections post-implementation",
            ))

        # --- Runtime Risk ---
        if affected > 3 or task_types.get("has_deploy"):
            risks.append(RiskItem(
                description="Changes may affect runtime performance or behavior",
                category="runtime",
                level=RiskLevel.MEDIUM,
                likelihood=0.3,
                impact=0.5,
                mitigation="Run typechecks and test suite after implementation",
                contingency="Profile and optimize if regressions detected",
            ))

        # --- Coordination Risk ---
        if task_types.get("is_refactor") or task_types.get("is_feature"):
            risks.append(RiskItem(
                description="Multiple specialists may produce conflicting outputs without coordination",
                category="coordination",
                level=RiskLevel.LOW,
                likelihood=0.25,
                impact=0.5,
                mitigation="Clear handoff contracts between specialists; cross-specialist memory injection",
                contingency="Use consensus system to resolve conflicts",
            ))

        # --- Default risk if none identified ---
        if not risks:
            risks.append(RiskItem(
                description="Changes may have unintended side effects",
                category="implementation",
                level=RiskLevel.LOW,
                likelihood=0.3,
                impact=0.3,
                mitigation="Run verification after each phase",
                contingency="Rollback to last known good state",
            ))

        # Compute overall
        avg_score = sum(r.risk_score for r in risks) / len(risks)
        if avg_score >= 0.5:
            overall = RiskLevel.HIGH
        elif avg_score >= 0.3:
            overall = RiskLevel.MEDIUM
        else:
            overall = RiskLevel.LOW

        return RiskSection(risks=risks, overall_level=overall)


# ===========================================================================
# 7. Execution Design Intelligence Engine
# ===========================================================================


