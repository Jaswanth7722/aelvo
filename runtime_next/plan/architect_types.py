"""
architect_types.py — ARCHITECT Plan Data Models for AELVO OMEGA

Defines the ARCHITECT plan contracts for AELVO OMEGA.

Legacy callers can still construct the original 10-section planning artifact.
Plans produced by Architect Intelligence use the 14-section Omega strategic
contract and are validated more strictly before execution.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


# ===========================================================================
# Enums
# ===========================================================================


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class BlastRadius(str, Enum):
    ISOLATED = "isolated"           # Single file/module
    LOCALIZED = "localized"         # Few files in same subsystem
    WIDESPREAD = "widespread"       # Cross-subsystem
    SYSTEMIC = "systemic"           # Affects entire codebase


class RecoveryStrategyType(str, Enum):
    RETRY = "retry"
    ROLLBACK = "rollback"
    SUBSTITUTE = "substitute"
    ESCALATE = "escalate"
    DECOMPOSE = "decompose"
    ABORT = "abort"


class VerificationMethod(str, Enum):
    UNIT_TEST = "unit_test"
    INTEGRATION_TEST = "integration_test"
    TYPECHECK = "typecheck"
    LINT = "lint"
    SECURITY_SCAN = "security_scan"
    MANUAL_REVIEW = "manual_review"
    COMPARISON = "comparison"
    ARCHITECTURE_CHECK = "architecture_check"


class SpecialistRole(str, Enum):
    FORGE = "FORGE"           # Implementation, refactoring, test generation
    SENTINEL = "SENTINEL"     # Security review, policy risk, attack surface
    ORACLE = "ORACLE"         # Repository knowledge, research, evidence
    TERMINUS = "TERMINUS"     # Tool execution, environment actions
    HERALD = "HERALD"         # Explanation, reporting, communication
    HERMES = "HERMES"         # User modeling, personalization
    ARCHITECT = "ARCHITECT"   # Planning (self)


class PlanStatus(str, Enum):
    DRAFT = "draft"
    REVIEW_REQUIRED = "review_required"
    VALIDATED = "validated"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SUPERSEDED = "superseded"


# ===========================================================================
# Section 1: Objective
# ===========================================================================


class ObjectiveSection(BaseModel):
    """What is being solved and what success looks like."""
    goal: str = Field(..., description="Concise statement of the primary goal")
    success_criteria: List[str] = Field(
        default_factory=list,
        description="Concrete, verifiable outcomes that define success",
    )
    hidden_constraints: List[str] = Field(
        default_factory=list,
        description="Constraints inferred from context (not explicitly stated)",
    )
    ambiguities: List[str] = Field(
        default_factory=list,
        description="Gaps or unclear requirements that must be resolved",
    )

    @field_validator("success_criteria")
    @classmethod
    def success_criteria_not_empty(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one success criterion must be defined")
        return v


# ===========================================================================
# Section 2: Current Understanding
# ===========================================================================


class CurrentUnderstandingSection(BaseModel):
    """What the repository and system context imply."""
    summary: str = Field(
        ..., description="Brief summary of the current state of the system"
    )
    relevant_modules: List[str] = Field(
        default_factory=list,
        description="Modules identified as relevant to this objective",
    )
    key_files: List[str] = Field(
        default_factory=list,
        description="Specific files that are directly relevant",
    )
    architectural_context: str = Field(
        default="",
        description="Architectural layers, patterns, and relationships identified",
    )


# ===========================================================================
# Section 3: Impact Analysis
# ===========================================================================


class ImpactItem(BaseModel):
    """A single impact the plan may have on the system."""
    target: str = Field(..., description="Module, file, or behavior affected")
    description: str = Field(..., description="Nature of the impact")
    severity: RiskLevel = Field(default=RiskLevel.LOW)
    is_backward_compatible: bool = True


class ImpactAnalysisSection(BaseModel):
    """What modules, subsystems, and behaviors may be affected."""
    blast_radius: BlastRadius = Field(
        ..., description="Estimated scope of changes"
    )
    affected_files: List[str] = Field(
        default_factory=list,
        description="Files that will be created, modified, or deleted",
    )
    affected_modules: List[str] = Field(
        default_factory=list,
        description="Subsystems or modules that will be affected",
    )
    impacts: List[ImpactItem] = Field(
        default_factory=list,
        description="Detailed impact items with severity assessment",
    )


# ===========================================================================
# Section 4: Risks
# ===========================================================================


class RiskItem(BaseModel):
    """A single identified risk with mitigation strategy."""
    description: str = Field(..., description="What could go wrong")
    category: str = Field(
        ..., description="security | architecture | implementation | runtime | maintenance | coordination"
    )
    level: RiskLevel = Field(default=RiskLevel.MEDIUM)
    likelihood: float = Field(default=0.5, ge=0.0, le=1.0)
    impact: float = Field(default=0.5, ge=0.0, le=1.0)
    mitigation: str = Field(
        default="", description="How to reduce or eliminate this risk"
    )
    contingency: str = Field(
        default="", description="What to do if the risk materializes"
    )

    @property
    def risk_score(self) -> float:
        return round(self.likelihood * self.impact, 3)


class RiskSection(BaseModel):
    """What could go wrong technically, architecturally, or operationally."""
    risks: List[RiskItem] = Field(default_factory=list)
    overall_level: RiskLevel = Field(default=RiskLevel.MEDIUM)

    @field_validator("risks")
    @classmethod
    def risks_not_empty(cls, v: List[RiskItem]) -> List[RiskItem]:
        if not v:
            raise ValueError("At least one risk must be identified")
        return v

    def compute_overall_level(self) -> RiskLevel:
        if not self.risks:
            return RiskLevel.LOW
        avg_score = sum(r.risk_score for r in self.risks) / len(self.risks)
        if avg_score >= 0.7:
            return RiskLevel.CRITICAL
        elif avg_score >= 0.5:
            return RiskLevel.HIGH
        elif avg_score >= 0.3:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW


# ===========================================================================
# Section 5: Execution Strategy
# ===========================================================================


class ExecutionPhase(BaseModel):
    """A phase of work within the execution strategy."""
    id: str = Field(..., description="Unique phase identifier")
    name: str = Field(..., description="Human-readable phase name")
    description: str = Field(..., description="What this phase accomplishes")
    order: int = Field(..., ge=1, description="Execution order")
    estimated_effort: int = Field(default=1, ge=1, description="Estimated steps")
    prerequisites: List[str] = Field(
        default_factory=list,
        description="IDs of phases that must complete first",
    )
    completion_criteria: List[str] = Field(
        default_factory=list,
        description="What must be true for this phase to be complete",
    )


class DependencyEdge(BaseModel):
    """A dependency relationship between two execution nodes."""
    source: str = Field(..., description="ID of the node that must complete first")
    target: str = Field(..., description="ID of the node that depends on source")
    condition: str = Field(
        default="completion",
        description="What must happen at source for target to proceed",
    )


class ExecutionStrategySection(BaseModel):
    """The ordered strategy to solve the problem."""
    phases: List[ExecutionPhase] = Field(
        default_factory=list,
        description="Ordered phases of work",
    )
    dependency_edges: List[DependencyEdge] = Field(
        default_factory=list,
        description="Dependency structure between phases",
    )
    critical_path: List[str] = Field(
        default_factory=list,
        description="IDs of phases on the critical path",
    )
    parallelizable_phases: List[List[str]] = Field(
        default_factory=list,
        description="Groups of phases that can run in parallel",
    )

    @field_validator("phases")
    @classmethod
    def phases_not_empty(cls, v: List[ExecutionPhase]) -> List[ExecutionPhase]:
        if not v:
            raise ValueError("At least one execution phase must be defined")
        return v

    def has_cycles(self) -> List[str]:
        """Detect cycles in the dependency graph. Returns problematic node IDs."""
        phase_ids = {p.id for p in self.phases}
        in_degree: Dict[str, int] = {pid: 0 for pid in phase_ids}
        adj: Dict[str, List[str]] = {pid: [] for pid in phase_ids}

        for dep in self.dependency_edges:
            if dep.source in phase_ids and dep.target in phase_ids:
                adj[dep.source].append(dep.target)
                in_degree[dep.target] = in_degree.get(dep.target, 0) + 1

        queue = [pid for pid, deg in in_degree.items() if deg == 0]
        visited = 0
        while queue:
            pid = queue.pop(0)
            visited += 1
            for neighbor in adj.get(pid, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if visited != len(phase_ids):
            return [pid for pid in phase_ids if in_degree.get(pid, 0) > 0]
        return []

    def compute_critical_path(self) -> List[str]:
        """Compute the critical path through the phase DAG."""
        phase_ids = {p.id for p in self.phases}
        in_degree: Dict[str, int] = {pid: 0 for pid in phase_ids}
        adj: Dict[str, List[str]] = {pid: [] for pid in phase_ids}
        effort: Dict[str, int] = {p.id: p.estimated_effort for p in self.phases}

        for dep in self.dependency_edges:
            if dep.source in phase_ids and dep.target in phase_ids:
                adj[dep.source].append(dep.target)
                in_degree[dep.target] = in_degree.get(dep.target, 0) + 1

        # Forward pass: compute earliest start and longest path
        topo = []
        queue = [pid for pid, deg in in_degree.items() if deg == 0]
        while queue:
            pid = queue.pop(0)
            topo.append(pid)
            for neighbor in adj.get(pid, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        longest_path: Dict[str, List[str]] = {}
        for pid in topo:
            predecessors = [dep.source for dep in self.dependency_edges if dep.target == pid]
            if not predecessors:
                longest_path[pid] = [pid]
            else:
                best = max(predecessors, key=lambda p: len(longest_path.get(p, [])))
                longest_path[pid] = longest_path.get(best, []) + [pid]

        if not longest_path:
            return []
        return max(longest_path.values(), key=len)


# ===========================================================================
# Section 6: Specialist Assignments
# ===========================================================================


class SpecialistAssignment(BaseModel):
    """Which specialist should handle each part and why."""
    specialist: SpecialistRole = Field(
        ..., description="The assigned specialist"
    )
    phase_id: str = Field(
        ..., description="Which phase this assignment belongs to"
    )
    task: str = Field(..., description="What the specialist should do")
    rationale: str = Field(
        ..., description="Why this specialist was chosen"
    )
    dependencies: List[str] = Field(
        default_factory=list,
        description="IDs of tasks/nodes this depends on",
    )
    output_contract: str = Field(
        default="",
        description="What the specialist must produce",
    )
    estimated_effort: int = Field(default=1, ge=1)
    critical: bool = Field(default=False)


class SpecialistAssignmentsSection(BaseModel):
    """Which specialist should handle each part and why."""
    assignments: List[SpecialistAssignment] = Field(
        default_factory=list,
    )

    def get_by_specialist(self, role: SpecialistRole) -> List[SpecialistAssignment]:
        return [a for a in self.assignments if a.specialist == role]


# ===========================================================================
# Section 7: Verification Plan
# ===========================================================================


class VerificationCheck(BaseModel):
    """A single verification step."""
    description: str = Field(..., description="What is being verified")
    method: VerificationMethod = Field(..., description="How to verify")
    phase_id: str = Field(
        ..., description="Which phase this verification applies to"
    )
    is_blocking: bool = Field(
        default=True,
        description="If True, downstream work waits for this",
    )
    success_threshold: str = Field(
        default="all tests pass",
        description="What counts as passing",
    )


class VerificationPlanSection(BaseModel):
    """How the result will be validated."""
    checks: List[VerificationCheck] = Field(default_factory=list)

    @field_validator("checks")
    @classmethod
    def checks_not_empty(cls, v: List[VerificationCheck]) -> List[VerificationCheck]:
        if not v:
            raise ValueError("At least one verification check must be defined")
        return v


# ===========================================================================
# Section 8: Recovery Plan
# ===========================================================================


class FailureModeStrategy(BaseModel):
    """A predicted failure mode and how to recover from it."""
    failure_mode: str = Field(..., description="What could fail")
    phase_id: str = Field(
        ..., description="Which phase this failure could occur in"
    )
    strategy: RecoveryStrategyType = Field(
        ..., description="What recovery action to take"
    )
    fallback_description: str = Field(
        ..., description="What the fallback actually does"
    )
    triggers_human_review: bool = Field(default=False)
    max_retries: int = Field(default=2, ge=0)


class RecoveryPlanSection(BaseModel):
    """What happens if something fails midway."""
    failure_strategies: List[FailureModeStrategy] = Field(
        default_factory=list,
    )
    rollback_points: List[str] = Field(
        default_factory=list,
        description="Phase IDs where rollback can occur",
    )
    general_approach: str = Field(
        default=""
    )

    @field_validator("failure_strategies")
    @classmethod
    def strategies_not_empty(cls, v: List[FailureModeStrategy]) -> List[FailureModeStrategy]:
        if not v:
            raise ValueError("At least one failure mode strategy must be defined")
        return v


# ===========================================================================
# Section 9: Completion Criteria
# ===========================================================================


class CompletionCriteriaSection(BaseModel):
    """What must be true before the task is considered done."""
    criteria: List[str] = Field(default_factory=list)
    verification_required: bool = Field(default=True)
    human_review_before_merge: bool = Field(default=False)

    @field_validator("criteria")
    @classmethod
    def criteria_not_empty(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one completion criterion must be defined")
        return v


# ===========================================================================
# Section 10: Self-Review
# ===========================================================================


class SelfReviewIssue(BaseModel):
    """An issue found during self-review."""
    description: str = Field(..., description="What is wrong")
    severity: RiskLevel = Field(default=RiskLevel.MEDIUM)
    suggested_fix: str = Field(default="", description="How to fix it")


class SelfReviewSection(BaseModel):
    """A final check that the plan is coherent, minimal, and executable."""
    is_coherent: bool = Field(default=False)
    is_minimal: bool = Field(default=False)
    is_executable: bool = Field(default=False)
    missing_sections: List[str] = Field(default_factory=list)
    issues: List[SelfReviewIssue] = Field(default_factory=list)
    strengths: List[str] = Field(default_factory=list)
    verdict: str = Field(default="")
    score: float = Field(default=0.0, ge=0.0, le=1.0)

    def passes_review(self, min_score: float = 0.6) -> bool:
        return (
            self.is_coherent
            and self.is_executable
            and self.score >= min_score
            and len(self.missing_sections) == 0
        )


# ===========================================================================
# Omega Strategic Intelligence Sections
# ===========================================================================


class ContextAnalysisSection(BaseModel):
    """Objective understanding beyond the literal request."""
    explicit_goals: List[str] = Field(default_factory=list)
    implicit_goals: List[str] = Field(default_factory=list)
    hidden_requirements: List[str] = Field(default_factory=list)
    unstated_constraints: List[str] = Field(default_factory=list)
    user_intent: str = ""
    repository_intent: str = ""
    architectural_intent: str = ""
    assumptions: List[str] = Field(default_factory=list)


class RepositoryAnalysisSection(BaseModel):
    """Repository reality consumed as a primary strategic input."""
    intelligence_status: str = "unavailable"
    architecture_layers: List[str] = Field(default_factory=list)
    subsystem_ownership: Dict[str, List[str]] = Field(default_factory=dict)
    dependency_chains: List[str] = Field(default_factory=list)
    execution_paths: List[str] = Field(default_factory=list)
    hotspots: List[str] = Field(default_factory=list)
    fragile_components: List[str] = Field(default_factory=list)
    protected_components: List[str] = Field(default_factory=list)
    evidence: List[str] = Field(default_factory=list)


class ArchitecturalAnalysisSection(BaseModel):
    """Architectural boundaries, design intent, and drift reasoning."""
    boundaries: List[str] = Field(default_factory=list)
    subsystem_responsibilities: Dict[str, str] = Field(default_factory=dict)
    design_intent: List[str] = Field(default_factory=list)
    drift_indicators: List[str] = Field(default_factory=list)
    quality_constraints: List[str] = Field(default_factory=list)


class DependencyAnalysisSection(BaseModel):
    """Dependencies that shape safe execution design."""
    execution_dependencies: List[str] = Field(default_factory=list)
    repository_dependencies: List[str] = Field(default_factory=list)
    specialist_dependencies: List[str] = Field(default_factory=list)
    verification_dependencies: List[str] = Field(default_factory=list)
    recovery_dependencies: List[str] = Field(default_factory=list)
    critical_dependencies: List[str] = Field(default_factory=list)


class GovernanceAnalysisSection(BaseModel):
    """Strategic governance decision for protected or dangerous work."""
    protected_components: List[str] = Field(default_factory=list)
    security_sensitive_systems: List[str] = Field(default_factory=list)
    requirements: List[str] = Field(default_factory=list)
    escalation_required: bool = False
    rationale: List[str] = Field(default_factory=list)


class Milestone(BaseModel):
    """A strategic milestone with timeframe and verification criteria."""
    id: str = Field(..., description="Unique milestone identifier")
    description: str = Field(..., description="What this milestone accomplishes")
    target_session: int = Field(default=1, ge=1, description="Which session this targets")
    dependencies: List[str] = Field(default_factory=list, description="Milestone IDs this depends on")
    verification: str = Field(default="", description="How to verify this milestone is complete")
    estimated_effort: int = Field(default=1, ge=1, description="Estimated effort in steps")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0, description="Confidence in achieving this milestone")


class GoalHierarchyNode(BaseModel):
    """A node in the strategic goal hierarchy tree."""
    id: str = Field(..., description="Unique node identifier")
    objective: str = Field(..., description="The strategic objective at this level")
    sub_goals: List[GoalHierarchyNode] = Field(default_factory=list)
    is_active: bool = Field(default=True)


class StrategicRoadmapSection(BaseModel):
    """Long-horizon strategic roadmap beyond immediate task completion.

    Transforms planning from task decomposition into strategic roadmap
    intelligence with milestones, goal trees, resource budgets, and
    multi-session awareness.
    """
    milestones: List[Milestone] = Field(
        default_factory=list,
        description="Strategic milestones with dependencies and timeframes",
    )
    goal_hierarchy: Optional[GoalHierarchyNode] = Field(
        default=None,
        description="Tree of strategic objectives decomposed into sub-objectives",
    )
    resource_budget: Dict[str, int] = Field(
        default_factory=dict,
        description="Estimated resource allocation by category (e.g. {'analysis': 5, 'implementation': 10})",
    )
    completion_confidence: float = Field(
        default=0.5, ge=0.0, le=1.0,
        description="Overall confidence in completing the strategic objectives",
    )
    multi_session: bool = Field(
        default=False,
        description="Whether this plan spans multiple sessions",
    )
    plan_evolution_path: str = Field(
        default="",
        description="How the plan is expected to evolve over time",
    )


class LongTermImpactSection(BaseModel):
    """Expected effects beyond immediate task completion."""
    maintenance_effects: List[str] = Field(default_factory=list)
    scaling_effects: List[str] = Field(default_factory=list)
    evolution_effects: List[str] = Field(default_factory=list)
    technical_debt_effects: List[str] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    roadmap: StrategicRoadmapSection = Field(
        default_factory=StrategicRoadmapSection,
        description="Strategic roadmap with milestones, goal trees, and resource budgets",
    )


class FinalApprovedPlanSection(BaseModel):
    """Final strategic approval after self-critique and governance review."""
    approved: bool = True
    approval_status: str = "legacy_compatible"
    strategic_summary: str = ""
    blocking_reasons: List[str] = Field(default_factory=list)
    conditions: List[str] = Field(default_factory=list)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    approved_at: Optional[datetime] = None


# ===========================================================================
# Complete Architect Plan
# ===========================================================================


class ArchitectPlan(BaseModel):
    """Complete ARCHITECT plan with legacy and Omega strategic sections."""
    id: str = Field(..., description="Unique plan identifier")
    title: str = Field(default="", description="Short plan title")
    status: PlanStatus = Field(default=PlanStatus.DRAFT)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    # The 10 sections
    objective: ObjectiveSection
    current_understanding: CurrentUnderstandingSection
    impact_analysis: ImpactAnalysisSection
    risks: RiskSection
    execution_strategy: ExecutionStrategySection
    specialist_assignments: SpecialistAssignmentsSection
    verification_plan: VerificationPlanSection
    recovery_plan: RecoveryPlanSection
    completion_criteria: CompletionCriteriaSection
    self_review: SelfReviewSection

    # Omega strategic intelligence sections
    context_analysis: ContextAnalysisSection = Field(default_factory=ContextAnalysisSection)
    repository_analysis: RepositoryAnalysisSection = Field(default_factory=RepositoryAnalysisSection)
    architectural_analysis: ArchitecturalAnalysisSection = Field(default_factory=ArchitecturalAnalysisSection)
    dependency_analysis: DependencyAnalysisSection = Field(default_factory=DependencyAnalysisSection)
    governance_analysis: GovernanceAnalysisSection = Field(default_factory=GovernanceAnalysisSection)
    long_term_impact: LongTermImpactSection = Field(default_factory=LongTermImpactSection)
    final_approved_plan: FinalApprovedPlanSection = Field(default_factory=FinalApprovedPlanSection)

    # Metadata
    source_goal_id: Optional[str] = Field(
        default=None,
        description="Link to the CognitiveEngine goal this plan serves",
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def validate_complete(self) -> List[str]:
        """Validate the applicable plan contract. Returns list of issues."""
        issues: List[str] = []
        if not self.objective.goal:
            issues.append("Section 1 (Objective): goal is empty")
        if not self.objective.success_criteria:
            issues.append("Section 1 (Objective): no success criteria defined")
        if not self.current_understanding.summary:
            issues.append("Section 2 (Current Understanding): summary is empty")
        if not self.execution_strategy.phases:
            issues.append("Section 5 (Execution Strategy): no phases defined")
        if not self.specialist_assignments.assignments:
            issues.append("Section 6 (Specialist Assignments): no assignments defined")
        if not self.verification_plan.checks:
            issues.append("Section 7 (Verification Plan): no checks defined")
        if not self.recovery_plan.failure_strategies:
            issues.append("Section 8 (Recovery Plan): no failure strategies defined")
        if not self.completion_criteria.criteria:
            issues.append("Section 9 (Completion Criteria): no criteria defined")
        if not self.self_review.verdict:
            issues.append("Section 10 (Self-Review): verdict is empty")

        if self.metadata.get("architect_intelligence_contract") == "omega-14":
            if not self.context_analysis.explicit_goals:
                issues.append("Omega Section 2 (Context Analysis): no explicit goals")
            if self.repository_analysis.intelligence_status == "unknown":
                issues.append("Omega Section 3 (Repository Analysis): intelligence status is unknown")
            if not self.architectural_analysis.quality_constraints:
                issues.append("Omega Section 4 (Architectural Analysis): no quality constraints")
            if not self.dependency_analysis.execution_dependencies:
                issues.append("Omega Section 5 (Dependency Analysis): no execution dependencies")
            if not self.long_term_impact.recommendations:
                issues.append("Omega Section 12 (Long-Term Impact): no recommendations")
            if not self.final_approved_plan.approval_status:
                issues.append("Omega Section 14 (Final Approved Plan): approval status is empty")

        # Check for cycle in execution strategy
        cycles = self.execution_strategy.has_cycles()
        if cycles:
            issues.append(f"Execution strategy contains cycles: {cycles}")

        return issues

    def to_execution_plan(self) -> Dict[str, Any]:
        """Convert to a format consumable by the Execution Plan models.

        Returns a dict that can be used to build runtime_next ExecutionPlan nodes.
        """
        return {
            "plan_id": self.id,
            "task_description": self.objective.goal,
            "phases": [
                {
                    "id": p.id,
                    "description": p.description,
                    "estimated_effort": p.estimated_effort,
                    "prerequisites": p.prerequisites,
                }
                for p in self.execution_strategy.phases
            ],
            "edges": [
                {"source": e.source, "target": e.target, "condition": e.condition}
                for e in self.execution_strategy.dependency_edges
            ],
            "critical_path": self.execution_strategy.critical_path,
            "specialist_assignments": [
                {
                    "specialist": a.specialist.value,
                    "task": a.task,
                    "phase_id": a.phase_id,
                    "critical": a.critical,
                }
                for a in self.specialist_assignments.assignments
            ],
            "verification_checks": [
                {"description": c.description, "method": c.method.value, "is_blocking": c.is_blocking}
                for c in self.verification_plan.checks
            ],
            "dependency_analysis": self.dependency_analysis.model_dump(mode="json"),
            "governance_analysis": self.governance_analysis.model_dump(mode="json"),
            "final_approved_plan": self.final_approved_plan.model_dump(mode="json"),
        }

    def to_terminal_display(self) -> str:
        """Render a human-readable terminal display of the plan."""
        lines = [
            f"╔══ ARCHITECT PLAN: {self.id[:16]} ══╗",
            f"  Title: {self.title or 'Untitled'}",
            f"  Status: {self.status.value}",
            f"  Score: {self.self_review.score:.2f}",
            "",
            "  ── OBJECTIVE ──",
            f"  {self.objective.goal[:120]}",
        ]

        if self.objective.success_criteria:
            lines.append("  Success Criteria:")
            for sc in self.objective.success_criteria:
                lines.append(f"    ✓ {sc[:80]}")

        if self.current_understanding.relevant_modules:
            lines.append("")
            lines.append("  ── RELEVANT MODULES ──")
            for m in self.current_understanding.relevant_modules[:8]:
                lines.append(f"    • {m}")

        lines.append("")
        lines.append("  ── EXECUTION STRATEGY ({len(self.execution_strategy.phases)} phases) ──")
        critical_set = set(self.execution_strategy.critical_path)
        for phase in self.execution_strategy.phases:
            critical = " ★" if phase.id in critical_set else ""
            lines.append(f"    [{phase.order}] {phase.name}{critical}")
            lines.append(f"        {phase.description[:80]}")
            if phase.prerequisites:
                lines.append(f"        depends: {', '.join(phase.prerequisites)}")

        if self.specialist_assignments.assignments:
            lines.append("")
            lines.append("  ── SPECIALIST ASSIGNMENTS ──")
            for a in self.specialist_assignments.assignments:
                critical = " ★" if a.critical else ""
                lines.append(f"    @{a.specialist.value}{critical} → {a.task[:70]}")

        if self.verification_plan.checks:
            lines.append("")
            lines.append("  ── VERIFICATION CHECKS ──")
            for c in self.verification_plan.checks:
                blocking = " [BLOCKING]" if c.is_blocking else ""
                lines.append(f"    ✓ {c.description[:70]} ({c.method.value}){blocking}")

        if self.recovery_plan.failure_strategies:
            lines.append("")
            lines.append("  ── FAILURE RECOVERY ──")
            for fs in self.recovery_plan.failure_strategies[:5]:
                strategy = fs.strategy.value
                human = " [HUMAN]" if fs.triggers_human_review else ""
                lines.append(f"    {fs.failure_mode[:50]} → {strategy}{human}")

        if self.risks.risks:
            lines.append("")
            lines.append(f"  ── RISKS (overall: {self.risks.overall_level.value}) ──")
            for r in self.risks.risks[:5]:
                lines.append(f"    ⚠ {r.description[:70]} [{r.category}] (score={r.risk_score:.2f})")

        lines.append("")
        lines.append("  ── COMPLETION CRITERIA ──")
        if self.metadata.get("architect_intelligence_contract") == "omega-14":
            lines.append("")
            lines.append("  -- STRATEGIC INTELLIGENCE --")
            lines.append(f"    Repository: {self.repository_analysis.intelligence_status}")
            lines.append(
                f"    Dependencies: {len(self.dependency_analysis.execution_dependencies)} execution, "
                f"{len(self.dependency_analysis.repository_dependencies)} repository"
            )
            lines.append(f"    Governance escalation: {self.governance_analysis.escalation_required}")
            lines.append(f"    Final approval: {self.final_approved_plan.approval_status}")

        for c in self.completion_criteria.criteria:
            lines.append(f"    ✓ {c[:80]}")

        lines.append("")
        lines.append("  ── SELF-REVIEW ──")
        lines.append(f"    Coherent: {self.self_review.is_coherent}")
        lines.append(f"    Executable: {self.self_review.is_executable}")
        lines.append(f"    Score: {self.self_review.score:.2f}")
        lines.append(f"    Verdict: {self.self_review.verdict[:100]}")
        if self.self_review.issues:
            lines.append("    Issues:")
            for issue in self.self_review.issues:
                lines.append(f"      [{issue.severity.value}] {issue.description[:60]}")

        lines.append(f"╚══ {'═' * (len(self.id[:16]) + 16)}══╝")
        return "\n".join(lines)
