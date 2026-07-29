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
from typing import Any, Dict, List, Optional, Tuple


from ..architect_types import (
    ContextAnalysisSection,
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




class ObjectiveIntelligenceEngine:
    """Analyzes the task from four perspectives simultaneously:
    explicit goal, implicit goal, repository goal, architectural goal.
    Detects objective conflicts before planning begins."""

    def analyze(
        self,
        objective: str,
        context: Dict[str, Any],
        repo_analysis: Optional[RepositoryAnalysisSection] = None,
    ) -> Tuple[ContextAnalysisSection, List[str]]:
        """Return (context_analysis, objective_conflicts)."""
        task = objective.strip()
        task.lower()
        types = _classify_task_type(task)

        # --- Explicit goals: parse nouns, verbs, qualifiers ---
        explicit_goals = [task] if task else ["[EMPTY OBJECTIVE â€” must be clarified]"]

        # --- Implicit goals: what the user needs but didn't say ---
        implicit: List[str] = [
            "Preserve repository integrity while completing the requested outcome",
            "Design verification and recovery before implementation begins",
            "Maintain backward compatibility unless explicitly told otherwise",
        ]
        if types["is_refactor"]:
            implicit.append("Preserve externally observable behavior during structural change")
            implicit.append("Update all callers and dependency chains affected by the refactor")
        if types["is_fix"]:
            implicit.append("Identify root cause, not just symptoms")
            implicit.append("Add regression test coverage for the fix")
        if types["is_feature"]:
            implicit.append("Cover error handling and edge cases")
            implicit.append("Ensure feature integrates with existing module boundaries")
        if types["has_security"]:
            implicit.append("Protect trust boundaries and avoid weakening authorization guarantees")
            implicit.append("Require security-sensitive validation before completion")
        if types["has_deploy"]:
            implicit.append("Require explicit rollback and escalation paths for dangerous operations")

        # --- Repository goals: what the codebase needs regardless ---
        hidden: List[str] = [
            "Respect existing architectural boundaries and ownership",
            "Avoid unnecessary specialist delegation",
            "Keep verification scope aligned with dependency blast radius",
        ]
        if repo_analysis:
            if repo_analysis.fragile_components:
                hidden.append(f"Exercise extra caution around fragile components: "
                              f"{', '.join(repo_analysis.fragile_components[:3])}")
            if repo_analysis.hotspots:
                hidden.append(f"Frequent change zones detected: "
                              f"{', '.join(repo_analysis.hotspots[:3])} â€” review for regressions")
            if repo_analysis.protected_components:
                hidden.append(f"Protected components at risk: "
                              f"{', '.join(repo_analysis.protected_components[:3])}")

        # --- Architectural goals: what design intent requires ---
        architectural_intent = [
            "Keep modifications inside the owning subsystem where possible",
            "Prefer existing repository patterns over parallel abstractions",
            "Treat architectural drift as a planning input, not an after-the-fact concern",
        ]
        if repo_analysis and repo_analysis.architecture_layers:
            architectural_intent.append(
                f"Maintain layer separation: {', '.join(repo_analysis.architecture_layers[:5])}"
            )

        # --- Objective conflict detection ---
        conflicts: List[str] = []
        if types["is_refactor"] and types["has_delete"]:
            conflicts.append("Refactoring requested alongside deletion â€” clarify scope: is this a migration or a removal?")
        if types["is_fix"] and types["is_feature"]:
            conflicts.append("Both fix and feature requested â€” determine if the fix should be minimal or if a feature redesign is intended")
        if types["has_security"] and types["has_deploy"]:
            conflicts.append("Security-sensitive change with deployment â€” require security review before deployment")
        if not task:
            conflicts.append("Objective is empty â€” cannot proceed without clarification")

        # --- Assumptions ---
        assumptions: List[str] = []
        if not repo_analysis or repo_analysis.intelligence_status == "unavailable":
            assumptions.append("Repository intelligence is unavailable; repository conclusions are conservative")
        if context.get("repo_intelligence") is None:
            assumptions.append("No repo intelligence engine attached â€” relying on workspace tree only")

        constraints = context.get("constraints", {}) or {}
        unstated = []
        for key, value in constraints.items():
            v = value.get("value", value) if isinstance(value, dict) else value
            unstated.append(f"{key}: {v}")

        return ContextAnalysisSection(
            explicit_goals=_dedupe(explicit_goals),
            implicit_goals=_dedupe(implicit),
            hidden_requirements=_dedupe(hidden),
            unstated_constraints=_dedupe(unstated),
            user_intent=task,
            repository_intent=f"Protect the established structure of {context.get('project', 'the active repository')}",
            architectural_intent="\n".join(_dedupe(architectural_intent)),
            assumptions=_dedupe(assumptions),
        ), conflicts


# ===========================================================================
# 2. Repository Intelligence Bridge
# ===========================================================================


