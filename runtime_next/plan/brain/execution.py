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
from typing import Any, Dict, List, Set


from ..architect_types import (
    CurrentUnderstandingSection,
    ExecutionStrategySection,
    ExecutionPhase,
    DependencyEdge,
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




class ExecutionDesignEngine:
    """Translates the strategic plan into an execution specification with
    phases, parallelism, checkpoints, budget allocation, and milestones."""

    def design(
        self,
        objective: str,
        task_types: Dict[str, bool],
        understanding: CurrentUnderstandingSection,
        strategy_selection: Dict[str, Any],
        context: Dict[str, Any],
    ) -> ExecutionStrategySection:
        phases: List[ExecutionPhase] = []
        edges: List[DependencyEdge] = []
        order_counter = 0

        def add_phase(
            name: str,
            desc: str,
            prereqs: List[str] = None,
            effort: int = 1,
            criteria: List[str] = None,
        ) -> str:
            nonlocal order_counter
            order_counter += 1
            pid = f"phase_{order_counter:02d}"
            phases.append(ExecutionPhase(
                id=pid,
                name=name,
                description=desc,
                order=order_counter,
                estimated_effort=effort,
                prerequisites=prereqs or [],
                completion_criteria=criteria or [f"{name} completed successfully"],
            ))
            for prereq in (prereqs or []):
                edges.append(DependencyEdge(source=prereq, target=pid))
            return pid

        # --- Phase 1: Investigation ---
        inv = add_phase(
            "Investigate and Understand",
            "Read relevant files, analyze structure, query memory for prior context",
            effort=max(1, min(3, len(understanding.key_files) + 1)),
            criteria=["All relevant files read and understood", "Context from memory retrieved"],
        )

        # --- Phase 2: Design ---
        design_prereqs = [inv]
        design = add_phase(
            "Design the Approach",
            "Determine exact changes needed, design solution, select strategy",
            prereqs=design_prereqs,
            effort=2,
            criteria=[f"Strategy selected: {strategy_selection.get('selected', 'TBD')}"],
        )

        # --- Phase 3: Implementation (varies by task type) ---
        if task_types.get("is_refactor"):
            callers = add_phase(
                "Identify All Callers and Usages",
                "Search for all usages of the code being refactored",
                prereqs=[design],
                effort=2,
                criteria=["All callers identified and documented"],
            )
            apply_refactor = add_phase(
                "Apply Refactoring Changes",
                "Make the actual refactoring changes to the codebase",
                prereqs=[callers],
                effort=max(3, len(understanding.key_files) + 1),
                criteria=["Refactoring changes applied to all relevant files"],
            )
            update_callers = add_phase(
                "Update All Callers",
                "Update all identified callers to work with the new code",
                prereqs=[apply_refactor],
                effort=3,
                criteria=["All callers updated and consistent"],
            )
            impl_last = update_callers

        elif task_types.get("is_fix"):
            impl_last = add_phase(
                "Apply the Fix",
                "Diagnose root cause and apply the targeted fix",
                prereqs=[design],
                effort=3,
                criteria=["Root cause identified and fix applied"],
            )

        elif task_types.get("is_feature"):
            impl_last = add_phase(
                "Implement the Feature",
                "Write the new code for the feature with error handling",
                prereqs=[design],
                effort=max(3, len(understanding.key_files) * 2),
                criteria=["Feature implemented with tests and error handling"],
            )

        else:
            impl_last = add_phase(
                "Execute the Changes",
                "Make the necessary changes to the codebase",
                prereqs=[design],
                effort=3,
                criteria=["Changes applied correctly"],
            )

        # --- Security phase (if needed) ---
        if task_types.get("has_security"):
            sec_last = add_phase(
                "Security Review",
                "Review all changes for security vulnerabilities and trust boundary violations",
                prereqs=[impl_last],
                effort=3,
                criteria=["Security review completed, no vulnerabilities found"],
            )
        else:
            sec_last = impl_last

        # --- Verification phase ---
        verify = add_phase(
            "Verify Changes",
            "Run typechecks, tests, and verification pipelines",
            prereqs=[sec_last],
            effort=2 if not task_types.get("has_test") else 4,
            criteria=["All verifications pass", "No regressions detected"],
        )

        # --- Synthesis phase ---
        add_phase(
            "Synthesize Results",
            "Summarize what was done and communicate the outcome",
            prereqs=[verify],
            effort=1,
            criteria=["Results synthesized and communicated"],
        )

        # Build execution strategy
        strategy = ExecutionStrategySection(
            phases=phases,
            dependency_edges=edges,
        )

        # Compute critical path
        strategy.critical_path = strategy.compute_critical_path()

        # Identify parallelizable phases
        strategy.parallelizable_phases = self._find_parallel_groups(phases, edges)

        return strategy

    def _find_parallel_groups(
        self,
        phases: List[ExecutionPhase],
        edges: List[DependencyEdge],
    ) -> List[List[str]]:
        """Identify groups of phases that can run in parallel."""
        phase_ids = {p.id for p in phases}
        deps_map: Dict[str, Set[str]] = {pid: set() for pid in phase_ids}
        for edge in edges:
            if edge.source in phase_ids and edge.target in phase_ids:
                deps_map.setdefault(edge.target, set()).add(edge.source)

        tiers: List[List[str]] = []
        remaining = set(phase_ids)
        completed: Set[str] = set()

        while remaining:
            ready = {
                pid for pid in remaining
                if all(d in completed for d in deps_map.get(pid, set()))
            }
            if not ready:
                ready = {next(iter(remaining))}
            if len(ready) > 1:
                tiers.append(list(ready))
            completed.update(ready)
            remaining -= ready

        return [t for t in tiers if len(t) > 1]


# ===========================================================================
# 8. Specialist Intelligence Engine
# ===========================================================================


