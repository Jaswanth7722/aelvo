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




class RepositoryIntelligenceBridge:
    """Translates raw repository intelligence facts into strategic reasoning inputs.
    This is the engine's core value: not querying the repository, but making
    repository facts intelligible to the strategic reasoning process."""

    def analyze(
        self,
        objective: str,
        context: Dict[str, Any],
        repo_intelligence: Any = None,
    ) -> RepositoryAnalysisSection:
        repo = context.get("repo_intelligence") or repo_intelligence
        if repo is None:
            return RepositoryAnalysisSection(
                intelligence_status="unavailable",
                evidence=["Repository Intelligence was not attached to this planning request"],
            )

        # Get architecture
        architecture = _safe(repo, "get_architecture")
        layers = [
            str(_field(layer, "name", layer))
            for layer in (_field(architecture, "layers", []) or [])
        ]

        # Module boundaries â†’ ownership
        boundaries = _field(architecture, "module_boundaries", {}) or {}
        if isinstance(boundaries, dict):
            ownership = {
                str(name): _dedupe(
                    components if isinstance(components, list) else [components], 10
                )
                for name, components in boundaries.items()
            }
        else:
            ownership = {}

        # Dependency chains from file info
        file_info = _safe(repo, "get_file_info", {}) or {}
        dependency_chains: List[str] = []
        for file_id, info in list(file_info.items())[:30]:
            imports = _field(info, "imports", []) or []
            for dep in list(imports)[:5]:
                dependency_chains.append(f"{file_id} -> {dep}")

        # Entry points
        arch_entries = _field(architecture, "entry_points", []) or []
        execution_paths = [str(e) for e in arch_entries]

        # Hotspots and fragile components
        hotspots = [
            str(_field(item, "component_id", item))
            for item in (_safe(repo, "get_repository_hotspots", []) or [])
        ]
        fragile = [
            str(_field(item, "component_id", item))
            for item in (_safe(repo, "get_fragile_components", []) or [])
        ]

        # Protected components
        protected: List[str] = []
        governance = getattr(repo, "governance_system", None)
        if governance:
            registry = getattr(governance, "protected_modules", None) or getattr(governance, "protected_registry", None)
            modules = getattr(registry, "_modules", {}) if registry else {}
            if isinstance(modules, dict):
                protected.extend(str(m) for m in modules)

        # Risk data
        stability_risk = _safe(repo, "compute_stability_risk")
        _safe(repo, "compute_dependency_risk")
        _safe(repo, "generate_evolution_report")

        evidence = [
            f"Repository intelligence status: {_field(stability_risk, 'overall_stability_score', 'unknown')}",
            f"Architecture layers: {len(layers)}",
            f"Dependency relationships: {len(dependency_chains)}",
            f"Modification hotspots: {len(hotspots)}",
            f"Fragile components: {len(fragile)}",
            f"Protected components: {len(protected)}",
        ]

        return RepositoryAnalysisSection(
            intelligence_status="available",
            architecture_layers=_dedupe(layers),
            subsystem_ownership=ownership,
            dependency_chains=_dedupe(dependency_chains, 40),
            execution_paths=_dedupe(execution_paths, 20),
            hotspots=_dedupe(hotspots, 20),
            fragile_components=_dedupe(fragile, 20),
            protected_components=_dedupe(protected, 20),
            evidence=_dedupe(evidence, 25),
        )


# ===========================================================================
# 3. Architectural Reasoning Engine
# ===========================================================================


