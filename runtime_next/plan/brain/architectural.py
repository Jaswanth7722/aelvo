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
    RepositoryAnalysisSection,
    ArchitecturalAnalysisSection,
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




class ArchitecturalReasoningEngine:
    """Ensures every plan is architecturally sound.
    Evaluates boundary compliance, responsibility compliance, and evolution compliance."""

    def analyze(
        self,
        repository: RepositoryAnalysisSection,
        context: Dict[str, Any],
        task_types: Dict[str, bool],
    ) -> Tuple[ArchitecturalAnalysisSection, List[str]]:
        """Returns (analysis, architectural_violations)."""
        violations: List[str] = []
        repo = context.get("repo_intelligence")

        # Detect drift
        drift = _safe(repo, "detect_architectural_drift") if repo else None
        drift_indicators: List[str] = []
        if drift:
            score = _field(drift, "overall_drift_score")
            if score is not None:
                drift_indicators.append(f"Observed architectural drift score: {score}")
            vcount = _field(drift, "architectural_violations", 0)
            if vcount:
                drift_indicators.append(f"Architectural violations reported: {vcount}")

        # Responsibility map from layers
        responsibilities = {
            layer: f"Preserve the responsibilities and dependencies of the {layer} layer"
            for layer in repository.architecture_layers
        }

        # Boundary analysis
        boundaries = list(repository.architecture_layers)
        boundaries.extend(repository.subsystem_ownership.keys())

        # Quality constraints based on task type
        quality_constraints = [
            "Preserve architectural boundaries",
            "Avoid duplicate orchestration or planning layers",
            "Keep verification and recovery observable",
        ]
        if task_types.get("is_refactor"):
            quality_constraints.append("Preserve all external interfaces during refactoring")
            quality_constraints.append("Maintain existing module coupling direction")
        if task_types.get("has_security"):
            quality_constraints.append("Never weaken authentication or authorization boundaries")
            quality_constraints.append("Trust boundary crossings must be reviewed by SENTINEL")
        if task_types.get("is_feature"):
            quality_constraints.append("New features must be placed in the correct architectural layer")
        if repository.fragile_components:
            quality_constraints.append(
                f"Avoid direct modification of fragile components: {', '.join(repository.fragile_components[:3])}"
            )

        # Detect boundary violations
        for chain in repository.dependency_chains[:50]:
            parts = chain.split(" -> ")
            if len(parts) == 2:
                src, tgt = parts
                # Check for cross-layer violations
                for layer in repository.architecture_layers:
                    if src in layer and tgt not in layer:
                        violations.append(
                            f"Potential cross-layer dependency: {src} ({layer}) â†’ {tgt}"
                        )

        return ArchitecturalAnalysisSection(
            boundaries=_dedupe(boundaries, 30),
            subsystem_responsibilities=responsibilities,
            design_intent=[
                "Keep modifications inside the owning subsystem where possible",
                "Prefer existing repository patterns over parallel abstractions",
                "Treat architectural drift as a planning input",
            ],
            drift_indicators=_dedupe(drift_indicators, 10),
            quality_constraints=_dedupe(quality_constraints, 15),
        ), violations


# ===========================================================================
# 4. Strategic Intelligence Engine
# ===========================================================================


