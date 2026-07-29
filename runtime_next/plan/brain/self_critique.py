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
from typing import Any, Dict, List, Tuple


from ..architect_types import (
    ObjectiveSection,
    RiskSection,
    RiskLevel,
    ExecutionStrategySection,
    SpecialistAssignmentsSection,
    VerificationPlanSection,
    RecoveryPlanSection,
    CompletionCriteriaSection,
    SelfReviewSection,
    SelfReviewIssue,
    DependencyAnalysisSection,
    GovernanceAnalysisSection,
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




class SelfCritiqueEngine:
    """The final gate before a plan is approved.
    Reviews completeness, consistency, and correctness iteratively."""

    def critique(
        self,
        objective: ObjectiveSection,
        execution: ExecutionStrategySection,
        assignments: SpecialistAssignmentsSection,
        verification: VerificationPlanSection,
        recovery: RecoveryPlanSection,
        risks: RiskSection,
        completion: CompletionCriteriaSection,
        dependencies: DependencyAnalysisSection,
        governance: GovernanceAnalysisSection,
    ) -> Tuple[SelfReviewSection, float, List[str]]:
        """Returns (review, score, blocking_reasons)."""
        issues: List[SelfReviewIssue] = []
        strengths: List[str] = []

        # 1. Dependency completeness
        if not execution.critical_path:
            issues.append(SelfReviewIssue(
                description="No critical path identified â€” phases may have missing dependency edges",
                severity=RiskLevel.HIGH,
                suggested_fix="Verify that phases have proper prerequisite declarations",
            ))

        # 2. Phase coverage
        assigned_phases = {a.phase_id for a in assignments.assignments}
        for phase in execution.phases:
            if phase.id not in assigned_phases:
                issues.append(SelfReviewIssue(
                    description=f"Phase '{phase.name}' has no specialist assignment",
                    severity=RiskLevel.MEDIUM,
                    suggested_fix=f"Assign a specialist to phase '{phase.name}'",
                ))

        # 3. Verification completeness
        verified_phases = {c.phase_id for c in verification.checks}
        for phase in execution.phases:
            if phase.id not in verified_phases and "Synthesize" not in phase.name:
                issues.append(SelfReviewIssue(
                    description=f"Phase '{phase.name}' has no verification checks",
                    severity=RiskLevel.MEDIUM,
                    suggested_fix=f"Add verification for phase '{phase.name}'",
                ))

        # 4. Recovery completeness
        recovered_phases = {fs.phase_id for fs in recovery.failure_strategies}
        for phase in execution.phases:
            if phase.id not in recovered_phases and "Synthesize" not in phase.name:
                issues.append(SelfReviewIssue(
                    description=f"Phase '{phase.name}' has no recovery strategy",
                    severity=RiskLevel.LOW,
                    suggested_fix=f"Add failure mode strategies for phase '{phase.name}'",
                ))

        # 5. Specialist over-delegation
        spec_counts: Dict[str, int] = {}
        for a in assignments.assignments:
            name = a.specialist.value
            spec_counts[name] = spec_counts.get(name, 0) + 1
        for name, count in spec_counts.items():
            if count > 4:
                issues.append(SelfReviewIssue(
                    description=f"Specialist {name} is assigned {count} tasks â€” consider consolidation",
                    severity=RiskLevel.LOW,
                    suggested_fix=f"Merge {name}'s tasks where possible",
                ))

        # 6. Plan bloat
        if len(execution.phases) > 12:
            issues.append(SelfReviewIssue(
                description=f"Plan has {len(execution.phases)} phases â€” may be over-engineered",
                severity=RiskLevel.MEDIUM,
                suggested_fix="Consolidate related phases",
            ))

        # 7. Risk coverage
        if len(risks.risks) < 2:
            issues.append(SelfReviewIssue(
                description="Very few risks identified â€” may be under-estimated",
                severity=RiskLevel.MEDIUM,
                suggested_fix="Review risk categories systematically",
            ))

        # 8. Objective conflicts
        if "[EMPTY OBJECTIVE" in (objective.goal or ""):
            issues.append(SelfReviewIssue(
                description="Objective is empty â€” cannot execute without clarification",
                severity=RiskLevel.CRITICAL,
                suggested_fix="Clarify the objective before proceeding",
            ))

        # 9. Governance escalation
        if governance.escalation_required and not governance.requirements:
            issues.append(SelfReviewIssue(
                description="Governance escalation required but no requirements specified",
                severity=RiskLevel.HIGH,
                suggested_fix="Add specific governance requirements",
            ))

        # Strengths
        if execution.phases:
            strengths.append(f"{len(execution.phases)} phases provide clear structure")
        if execution.critical_path:
            strengths.append("Critical path is identified")
        if verification.checks:
            strengths.append(f"{len(verification.checks)} verification checks defined")
        if recovery.failure_strategies:
            strengths.append(f"{len(recovery.failure_strategies)} recovery strategies defined")
        if assignments.assignments:
            specialists_used = {a.specialist.value for a in assignments.assignments}
            strengths.append(f"{len(specialists_used)} specialists assigned with rationale")

        # Score
        score = 0.5
        if execution.phases:
            score += 0.1
        if execution.critical_path:
            score += 0.1
        if verification.checks:
            score += 0.05 * min(1.0, len(verification.checks) / 3)
        if recovery.failure_strategies:
            score += 0.05 * min(1.0, len(recovery.failure_strategies) / 3)
        if assignments.assignments:
            score += 0.05
        if completion.criteria:
            score += 0.05
        score -= 0.05 * min(1.0, len(issues) / 5)
        score = max(0.0, min(1.0, score))

        # Verdict
        major = [i for i in issues if i.severity in (RiskLevel.HIGH, RiskLevel.CRITICAL)]
        if major:
            verdict = f"Plan has {len(major)} major issue(s) that should be resolved"
        elif issues:
            verdict = f"Plan is acceptable with {len(issues)} minor issue(s)"
        else:
            verdict = "Plan is coherent, minimal, and ready for execution"

        # Blocking reasons
        blocking = [
            issue.description
            for issue in issues
            if issue.severity in (RiskLevel.HIGH, RiskLevel.CRITICAL)
        ]

        review = SelfReviewSection(
            is_coherent=len(major) == 0,
            is_minimal=len(execution.phases) <= 12,
            is_executable=len(major) == 0,
            missing_sections=[],
            issues=issues,
            strengths=strengths,
            verdict=verdict,
            score=round(score, 3),
        )

        return review, score, blocking


# ===========================================================================
# Architect Intelligence Brain â€” Orchestrator
# ===========================================================================


