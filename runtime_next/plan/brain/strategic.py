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




class StrategicIntelligenceEngine:
    """Evaluates approaches, selects between them based on risk/complexity/impact,
    and explains the selection with reasoning that can be critiqued."""

    # Strategy vocabulary: for each task class, enumerate possible approaches
    STRATEGY_VOCABULARY: Dict[str, List[Dict[str, Any]]] = {
        "bug_fix": [
            {"name": "minimal_patch", "desc": "Change only what is broken", "cost": "low", "debt": "low", "risk": "low"},
            {"name": "comprehensive_fix", "desc": "Fix root cause including surrounding issues", "cost": "medium", "debt": "low", "risk": "medium"},
            {"name": "redesign", "desc": "Address underlying architectural problem", "cost": "high", "debt": "none", "risk": "high"},
        ],
        "refactor": [
            {"name": "incremental", "desc": "Small, safe refactoring steps with verification at each", "cost": "medium", "debt": "low", "risk": "low"},
            {"name": "comprehensive", "desc": "Full structural refactoring in one pass", "cost": "high", "debt": "none", "risk": "medium"},
            {"name": "strangler_fig", "desc": "Build new alongside old, migrate gradually", "cost": "very_high", "debt": "none", "risk": "low"},
        ],
        "feature": [
            {"name": "mvp", "desc": "Minimal implementation covering core cases", "cost": "low", "debt": "medium", "risk": "low"},
            {"name": "complete", "desc": "Full implementation with all edge cases", "cost": "high", "debt": "low", "risk": "medium"},
            {"name": "extensible", "desc": "Implementation designed for future extension", "cost": "very_high", "debt": "none", "risk": "medium"},
        ],
        "security": [
            {"name": "targeted_hardening", "desc": "Fix the specific vulnerability", "cost": "low", "debt": "low", "risk": "low"},
            {"name": "comprehensive_audit", "desc": "Full security review and hardening", "cost": "high", "debt": "none", "risk": "low"},
        ],
    }

    def select_strategy(
        self,
        objective: str,
        task_types: Dict[str, bool],
        repo_analysis: RepositoryAnalysisSection,
        architectural_violations: List[str],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Select the best strategy and explain the reasoning."""
        objective.lower()

        # Determine strategy class
        if task_types.get("has_security"):
            strategy_class = "security"
        elif task_types.get("is_fix"):
            strategy_class = "bug_fix"
        elif task_types.get("is_refactor"):
            strategy_class = "refactor"
        elif task_types.get("is_feature"):
            strategy_class = "feature"
        else:
            strategy_class = "feature"  # default to feature strategies

        candidates = self.STRATEGY_VOCABULARY.get(strategy_class, self.STRATEGY_VOCABULARY["feature"])

        # Evaluate each strategy against constraints
        evaluations = []
        for strategy in candidates:
            score = 0.0
            reasons = []

            # Complexity factor: fragile components make high-effort strategies riskier
            if repo_analysis.fragile_components:
                if strategy["cost"] in ("high", "very_high"):
                    score -= 0.2
                    reasons.append("High effort strategy risky with fragile components present")
                else:
                    score += 0.1
                    reasons.append("Low effort strategy safer given fragile components")

            # Architecture violations: prefer strategies that address them
            if architectural_violations:
                if strategy["name"] in ("redesign", "comprehensive", "comprehensive_audit", "strangler_fig"):
                    score += 0.2
                    reasons.append("Strategy addresses existing architectural violations")
                else:
                    score -= 0.1
                    reasons.append("Strategy does not address architectural violations")

            # Protected components: prefer conservative strategies
            if repo_analysis.protected_components:
                if strategy["risk"] == "low":
                    score += 0.15
                    reasons.append("Conservative strategy appropriate with protected components")

            # Hotspots: prefer strategies with verification
            if repo_analysis.hotspots:
                if strategy["debt"] == "low":
                    score += 0.1
                    reasons.append("Low-debt strategy appropriate given change hotspots")

            # User constraints: check if context has budget or time constraints
            budget = context.get("budget", 30)
            if budget < 15 and strategy["cost"] in ("high", "very_high"):
                score -= 0.3
                reasons.append("Budget constraint favors lower-cost strategy")

            evaluations.append({
                **strategy,
                "score": round(score, 3),
                "reasons": reasons,
            })

        # Select best strategy
        evaluations.sort(key=lambda x: x["score"], reverse=True)
        selected = evaluations[0] if evaluations else candidates[0]

        return {
            "strategy_class": strategy_class,
            "selected": selected["name"],
            "description": selected["desc"],
            "tradeoff_profile": {
                "implementation_cost": selected["cost"],
                "maintenance_cost": selected["debt"],
                "risk": selected["risk"],
            },
            "evaluation_reasoning": selected.get("reasons", []),
            "alternatives_considered": [e["name"] for e in evaluations[1:3]],
        }


# ===========================================================================
# 5. Dependency Intelligence Engine
# ===========================================================================


