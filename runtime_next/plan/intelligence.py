"""Cooperating strategic intelligence domains for ARCHITECT OMEGA.

This module deliberately does not plan or execute work. It gathers strategic
evidence, reasons about repository reality, and approves or blocks the plan
that the existing ArchitectOrchestrator designs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from pydantic import BaseModel

from .architect_types import (
    ArchitecturalAnalysisSection,
    ContextAnalysisSection,
    DependencyAnalysisSection,
    ExecutionStrategySection,
    FinalApprovedPlanSection,
    GovernanceAnalysisSection,
    ImpactAnalysisSection,
    LongTermImpactSection,
    RecoveryPlanSection,
    RepositoryAnalysisSection,
    RiskLevel,
    RiskSection,
    SelfReviewSection,
    SpecialistAssignmentsSection,
    VerificationPlanSection,
)


def _dedupe(values: Iterable[str], limit: int = 20) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
        if len(result) >= limit:
            break
    return result


def _model_value(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _safe_call(target: Any, method: str, default: Any = None) -> Any:
    if target is None:
        return default
    callback = getattr(target, method, None)
    if callback is None:
        return default
    try:
        return callback()
    except Exception:
        return default


class StrategicIntelligenceSnapshot(BaseModel):
    """Pre-execution strategic evidence assembled by cooperating domains."""

    context_analysis: ContextAnalysisSection
    repository_analysis: RepositoryAnalysisSection
    architectural_analysis: ArchitecturalAnalysisSection
    long_term_impact: LongTermImpactSection


class ObjectiveIntelligence:
    """Understands explicit goals, implicit goals, and unstated constraints."""

    def analyze(self, objective: str, context: Dict[str, Any]) -> ContextAnalysisSection:
        task = objective.strip()
        lower = task.lower()
        implicit: List[str] = [
            "Preserve repository integrity while completing the requested outcome",
            "Design verification and recovery before implementation begins",
        ]
        hidden: List[str] = [
            "Respect existing architectural boundaries and ownership",
            "Avoid unnecessary specialist delegation",
        ]

        if any(word in lower for word in ("refactor", "rewrite", "restructure")):
            implicit.append("Preserve externally observable behavior during structural change")
            hidden.append("Update callers and dependency chains affected by the refactor")
        if any(word in lower for word in ("security", "auth", "oauth", "permission", "credential")):
            implicit.append("Protect trust boundaries and avoid weakening authorization guarantees")
            hidden.append("Require security-sensitive validation before completion")
        if any(word in lower for word in ("deploy", "migration", "delete", "drop", "reset")):
            hidden.append("Require explicit rollback and escalation paths for dangerous operations")

        constraints = context.get("constraints", {}) or {}
        unstated = []
        for key, value in constraints.items():
            if isinstance(value, dict):
                value = value.get("value", "")
            unstated.append(f"{key}: {value}")

        assumptions: List[str] = []
        if context.get("repo_intelligence") is None:
            assumptions.append("Repository intelligence is unavailable; repository conclusions are conservative")
        if not task:
            assumptions.append("The objective is empty and must be clarified before execution")

        project = context.get("project", "")
        return ContextAnalysisSection(
            explicit_goals=[task or "Clarify the requested objective"],
            implicit_goals=_dedupe(implicit),
            hidden_requirements=_dedupe(hidden),
            unstated_constraints=_dedupe(unstated),
            user_intent=task,
            repository_intent=f"Protect the established structure of {project or 'the active repository'}",
            architectural_intent="Improve the requested behavior without introducing architectural drift",
            assumptions=assumptions,
        )


class RepositoryIntelligence:
    """Consumes Repository Intelligence Omega as strategic evidence."""

    def __init__(self, repo_intelligence: Any = None):
        self._repo = repo_intelligence

    def analyze(self, objective: str, context: Dict[str, Any]) -> RepositoryAnalysisSection:
        repo = context.get("repo_intelligence") or self._repo
        if repo is None:
            return RepositoryAnalysisSection(
                intelligence_status="unavailable",
                evidence=["Repository Intelligence Omega was not attached to this planning request"],
            )

        status = getattr(getattr(repo, "status", None), "value", None) or "available"
        architecture = _safe_call(repo, "get_architecture")
        layers = [
            str(_model_value(layer, "name", layer))
            for layer in (_model_value(architecture, "layers", []) or [])
        ]
        boundaries = _model_value(architecture, "module_boundaries", {}) or {}
        if isinstance(boundaries, dict):
            ownership = {
                str(name): _dedupe(components if isinstance(components, list) else [components], 10)
                for name, components in boundaries.items()
            }
        else:
            ownership = {"boundaries": _dedupe(boundaries if isinstance(boundaries, list) else [boundaries])}

        file_info = _safe_call(repo, "get_file_info", {}) or {}
        dependency_chains: List[str] = []
        for file_id, info in list(file_info.items())[:20]:
            imports = _model_value(info, "imports", []) or []
            for dependency in list(imports)[:4]:
                dependency_chains.append(f"{file_id} -> {dependency}")

        architecture_entries = _model_value(architecture, "entry_points", []) or []
        execution_paths = [str(entry) for entry in architecture_entries]

        hotspots = [
            str(_model_value(item, "component_id", item))
            for item in (_safe_call(repo, "get_repository_hotspots", []) or [])
        ]
        fragile = [
            str(_model_value(item, "component_id", item))
            for item in (_safe_call(repo, "get_fragile_components", []) or [])
        ]

        protected: List[str] = []
        governance = getattr(repo, "governance_system", None)
        registry = getattr(governance, "protected_modules", None) or getattr(governance, "protected_registry", None)
        modules = getattr(registry, "_modules", {}) if registry is not None else {}
        if isinstance(modules, dict):
            protected.extend(str(module_id) for module_id in modules)

        evidence = [
            f"Repository intelligence status: {status}",
            f"Architecture layers observed: {len(layers)}",
            f"Dependency relationships observed: {len(dependency_chains)}",
        ]
        repo_results = context.get("repo_intel_results", []) or []
        evidence.extend(str(result)[:160] for result in repo_results[:5])

        return RepositoryAnalysisSection(
            intelligence_status=str(status),
            architecture_layers=_dedupe(layers),
            subsystem_ownership=ownership,
            dependency_chains=_dedupe(dependency_chains),
            execution_paths=_dedupe(execution_paths),
            hotspots=_dedupe(hotspots),
            fragile_components=_dedupe(fragile),
            protected_components=_dedupe(protected),
            evidence=_dedupe(evidence),
        )


class ArchitecturalIntelligence:
    """Reasons about boundaries, design intent, and architectural drift."""

    def __init__(self, repo_intelligence: Any = None):
        self._repo = repo_intelligence

    def analyze(
        self,
        repository: RepositoryAnalysisSection,
        context: Dict[str, Any],
    ) -> ArchitecturalAnalysisSection:
        repo = context.get("repo_intelligence") or self._repo
        drift = _safe_call(repo, "detect_architectural_drift")
        drift_indicators: List[str] = []
        if drift is not None:
            score = _model_value(drift, "overall_drift_score")
            if score is not None:
                drift_indicators.append(f"Observed architectural drift score: {score}")
            violations = _model_value(drift, "architectural_violations", 0)
            if violations:
                drift_indicators.append(f"Architectural violations reported: {violations}")

        responsibilities = {
            layer: f"Preserve the responsibilities and dependencies of the {layer} layer"
            for layer in repository.architecture_layers
        }
        boundaries = list(repository.architecture_layers)
        boundaries.extend(repository.subsystem_ownership.keys())

        return ArchitecturalAnalysisSection(
            boundaries=_dedupe(boundaries),
            subsystem_responsibilities=responsibilities,
            design_intent=[
                "Keep modifications inside the owning subsystem where possible",
                "Prefer existing repository patterns over parallel abstractions",
                "Treat architectural drift as a planning input, not an after-the-fact concern",
            ],
            drift_indicators=_dedupe(drift_indicators),
            quality_constraints=[
                "Preserve architectural boundaries",
                "Avoid duplicate orchestration or planning layers",
                "Keep verification and recovery observable",
            ],
        )


class DependencyIntelligence:
    """Builds execution, repository, specialist, verification, and recovery dependencies."""

    def analyze(
        self,
        execution: ExecutionStrategySection,
        assignments: SpecialistAssignmentsSection,
        verification: VerificationPlanSection,
        recovery: RecoveryPlanSection,
        repository: RepositoryAnalysisSection,
    ) -> DependencyAnalysisSection:
        execution_dependencies = [
            f"{edge.source} -> {edge.target} [{edge.condition}]"
            for edge in execution.dependency_edges
        ]
        if not execution_dependencies:
            execution_dependencies.append("single-phase execution")

        specialist_dependencies = []
        for assignment in assignments.assignments:
            specialist_dependencies.append(
                f"{assignment.phase_id} -> {assignment.specialist.value}: {assignment.task}"
            )
            specialist_dependencies.extend(
                f"{dependency} -> {assignment.phase_id}" for dependency in assignment.dependencies
            )

        verification_dependencies = [
            f"{check.phase_id} -> {check.method.value}: {check.success_threshold}"
            for check in verification.checks
        ]
        recovery_dependencies = [
            f"{strategy.phase_id} -> {strategy.strategy.value}: {strategy.failure_mode}"
            for strategy in recovery.failure_strategies
        ]

        critical = set(execution.critical_path)
        critical_dependencies = [
            dependency for dependency in execution_dependencies
            if any(phase_id in dependency for phase_id in critical)
        ]

        return DependencyAnalysisSection(
            execution_dependencies=_dedupe(execution_dependencies, 40),
            repository_dependencies=_dedupe(repository.dependency_chains, 40),
            specialist_dependencies=_dedupe(specialist_dependencies, 40),
            verification_dependencies=_dedupe(verification_dependencies, 40),
            recovery_dependencies=_dedupe(recovery_dependencies, 40),
            critical_dependencies=_dedupe(critical_dependencies, 40),
        )


class GovernanceIntelligence:
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
        affected = {path.replace("\\", "/").lower() for path in impact.affected_files}
        targeted_protected = [
            component for component in repository.protected_components
            if any(component.replace("\\", "/").lower() in path or path in component.replace("\\", "/").lower()
                   for path in affected)
        ]
        critical_risks = [risk.description for risk in risks.risks if risk.level == RiskLevel.CRITICAL]
        dangerous = [hint for hint in self._DANGEROUS_HINTS if hint in lower]
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

        requirements = list(context.get("governance_requirements", []) or [])
        if escalation_required:
            requirements.extend([
                "Obtain explicit approval before dangerous execution",
                "Preserve a rollback point before modifying protected infrastructure",
                "Run blocking verification after the guarded change",
            ])

        return GovernanceAnalysisSection(
            protected_components=_dedupe(targeted_protected),
            security_sensitive_systems=_dedupe(
                hint for hint in self._SECURITY_HINTS if hint in lower
            ),
            requirements=_dedupe(requirements),
            escalation_required=escalation_required,
            rationale=_dedupe(rationale),
        )


class LongHorizonIntelligence:
    """Reasons about maintenance, scaling, evolution, and technical debt."""

    def __init__(self, repo_intelligence: Any = None):
        self._repo = repo_intelligence

    def analyze(
        self,
        repository: RepositoryAnalysisSection,
        context: Dict[str, Any],
    ) -> LongTermImpactSection:
        repo = context.get("repo_intelligence") or self._repo
        evolution = _safe_call(repo, "generate_evolution_report")
        evolution_effects: List[str] = []
        if evolution is not None:
            for name in (
                "overall_evolution_risk",
                "predicted_bottlenecks",
                "scaling_concerns",
                "technical_debt_score",
                "dependency_growth_risk",
            ):
                value = _model_value(evolution, name)
                if value is not None:
                    evolution_effects.append(f"{name}: {value}")

        recommendations = [
            "Prefer changes that reduce coupling and preserve subsystem ownership",
            "Record architectural decisions when the implementation changes design intent",
            "Keep verification scope aligned with dependency blast radius",
        ]
        if repository.fragile_components:
            recommendations.append("Use smaller checkpoints around fragile components")
        if repository.intelligence_status == "unavailable":
            recommendations.append("Refresh repository intelligence before high-risk implementation")

        return LongTermImpactSection(
            maintenance_effects=[
                "Keep the implementation understandable for future maintainers",
                "Avoid introducing parallel abstractions that increase maintenance cost",
            ],
            scaling_effects=[
                "Watch dependency growth on changed subsystem boundaries",
            ],
            evolution_effects=_dedupe(evolution_effects),
            technical_debt_effects=[
                "Prefer explicit cleanup or follow-up records for deferred compromises",
            ],
            recommendations=_dedupe(recommendations),
        )


class AutonomousCoordinationIntelligence:
    """Explains specialist participation and resolves obvious assignment conflicts."""

    def decisions(self, assignments: SpecialistAssignmentsSection) -> List[str]:
        decisions: List[str] = []
        by_phase: Dict[str, List[str]] = {}
        for assignment in assignments.assignments:
            by_phase.setdefault(assignment.phase_id, []).append(assignment.specialist.value)
        for phase_id, specialists in by_phase.items():
            decisions.append(f"{phase_id}: coordinate {', '.join(_dedupe(specialists))}")
        return _dedupe(decisions, 30)


class StrategicApprovalIntelligence:
    """Approves the final plan only after self-critique and governance review."""

    def approve(
        self,
        objective: str,
        repository: RepositoryAnalysisSection,
        governance: GovernanceAnalysisSection,
        verification: VerificationPlanSection,
        review: SelfReviewSection,
    ) -> FinalApprovedPlanSection:
        blocking_reasons: List[str] = []
        if governance.escalation_required:
            blocking_reasons.extend(governance.rationale or ["Governance approval is required"])
        blocking_reasons.extend(
            issue.description
            for issue in review.issues
            if issue.severity in (RiskLevel.HIGH, RiskLevel.CRITICAL)
        )
        if not review.passes_review():
            blocking_reasons.append("Self-critique did not approve execution")

        approved = not blocking_reasons
        conditions = [
            check.success_threshold
            for check in verification.checks
            if check.is_blocking
        ]
        confidence = review.score
        if repository.intelligence_status == "unavailable":
            confidence = max(0.0, confidence - 0.1)

        return FinalApprovedPlanSection(
            approved=approved,
            approval_status="approved_for_execution" if approved else "review_required",
            strategic_summary=f"Strategic execution design for: {objective[:160]}",
            blocking_reasons=_dedupe(blocking_reasons),
            conditions=_dedupe(conditions),
            confidence=round(confidence, 3),
            approved_at=datetime.now(timezone.utc) if approved else None,
        )


class ArchitectIntelligenceCoordinator:
    """Coordinates strategic domains without becoming another planner."""

    def __init__(self, repo_intelligence: Any = None):
        self.objective = ObjectiveIntelligence()
        self.repository = RepositoryIntelligence(repo_intelligence)
        self.architecture = ArchitecturalIntelligence(repo_intelligence)
        self.dependencies = DependencyIntelligence()
        self.governance = GovernanceIntelligence()
        self.long_horizon = LongHorizonIntelligence(repo_intelligence)
        self.coordination = AutonomousCoordinationIntelligence()
        self.approval = StrategicApprovalIntelligence()

    def preflight(self, objective: str, context: Dict[str, Any]) -> StrategicIntelligenceSnapshot:
        context_analysis = self.objective.analyze(objective, context)
        repository_analysis = self.repository.analyze(objective, context)
        architectural_analysis = self.architecture.analyze(repository_analysis, context)
        long_term_impact = self.long_horizon.analyze(repository_analysis, context)
        return StrategicIntelligenceSnapshot(
            context_analysis=context_analysis,
            repository_analysis=repository_analysis,
            architectural_analysis=architectural_analysis,
            long_term_impact=long_term_impact,
        )

