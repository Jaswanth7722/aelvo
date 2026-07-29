"""Tests for ARCHITECT Plan types and ArchitectOrchestrator.

Tests cover:
- Plan structure: all 10 sections present and validated
- Plan validation: missing sections, empty fields, cycles detected
- Execution strategy: phase ordering, dependency edges, critical path
- Risk analysis: risk scoring, categories, mitigations
- Specialist selection: correct assignments per task type
- Self-critique: coherent plans pass, incomplete plans fail
- Cost estimation: effort, complexity, regression probability
- Deterministic verification: validate_complete() and self_critique()
"""

from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime

import pytest

from runtime_next.plan.architect_types import (
    ArchitectPlan,
    ObjectiveSection,
    CurrentUnderstandingSection,
    ImpactAnalysisSection,
    ImpactItem,
    RiskSection,
    RiskItem,
    RiskLevel,
    BlastRadius,
    ExecutionStrategySection,
    ExecutionPhase,
    DependencyEdge,
    SpecialistAssignment,
    SpecialistAssignmentsSection,
    SpecialistRole,
    VerificationPlanSection,
    VerificationCheck,
    VerificationMethod,
    RecoveryPlanSection,
    FailureModeStrategy,
    RecoveryStrategyType,
    CompletionCriteriaSection,
    SelfReviewSection,
    SelfReviewIssue,
    PlanStatus,
)

from runtime_next.plan.architect import ArchitectOrchestrator


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def minimal_objective():
    return ObjectiveSection(
        goal="Refactor the authentication module to async",
        success_criteria=[
            "Existing functionality is preserved",
            "All references to changed code are updated",
            "No regressions in test suite",
        ],
        hidden_constraints=["Must maintain backward compatibility"],
        ambiguities=[],
    )


@pytest.fixture
def minimal_understanding():
    return CurrentUnderstandingSection(
        summary="Project uses Flask with SQLAlchemy. Auth module handles login/logout/session.",
        relevant_modules=["auth", "user", "session"],
        key_files=["auth/login.py", "auth/session.py", "auth/decorators.py"],
        architectural_context="Monolithic Flask app with blueprint structure.",
    )


@pytest.fixture
def minimal_impact():
    return ImpactAnalysisSection(
        blast_radius=BlastRadius.LOCALIZED,
        affected_files=["auth/login.py", "auth/session.py", "auth/decorators.py"],
        affected_modules=["auth"],
        impacts=[
            ImpactItem(target="auth/login.py", description="Convert to async SQLAlchemy session"),
            ImpactItem(target="auth/session.py", description="Update session management to async"),
        ],
    )


@pytest.fixture
def minimal_risks():
    return RiskSection(
        risks=[
            RiskItem(
                description="Async conversion may introduce race conditions",
                category="implementation",
                level=RiskLevel.MEDIUM,
                likelihood=0.4,
                impact=0.6,
                mitigation="Use proper async locking patterns",
                contingency="Revert to sync if race conditions cannot be resolved",
            ),
            RiskItem(
                description="Session management changes may break existing sessions",
                category="architecture",
                level=RiskLevel.HIGH,
                likelihood=0.3,
                impact=0.8,
                mitigation="Test with existing session tokens before and after",
                contingency="Rollback and keep old session system",
            ),
        ],
        overall_level=RiskLevel.MEDIUM,
    )


@pytest.fixture
def minimal_execution():
    phases = [
        ExecutionPhase(id="p1", name="Investigate", description="Read auth module files", order=1, estimated_effort=2),
        ExecutionPhase(id="p2", name="Design", description="Plan async conversion", order=2, estimated_effort=2, prerequisites=["p1"]),
        ExecutionPhase(id="p3", name="Implement", description="Convert to async SQLAlchemy", order=3, estimated_effort=5, prerequisites=["p2"]),
        ExecutionPhase(id="p4", name="Verify", description="Run tests and type checks", order=4, estimated_effort=2, prerequisites=["p3"]),
    ]
    edges = [
        DependencyEdge(source="p1", target="p2"),
        DependencyEdge(source="p2", target="p3"),
        DependencyEdge(source="p3", target="p4"),
    ]
    strategy = ExecutionStrategySection(phases=phases, dependency_edges=edges)
    strategy.critical_path = strategy.compute_critical_path()
    return strategy


@pytest.fixture
def minimal_assignments():
    return SpecialistAssignmentsSection(
        assignments=[
            SpecialistAssignment(
                specialist=SpecialistRole.ORACLE,
                phase_id="p1",
                task="Read and analyze auth module files",
                rationale="ORACLE has repository knowledge",
                critical=True,
            ),
            SpecialistAssignment(
                specialist=SpecialistRole.ARCHITECT,
                phase_id="p2",
                task="Design async conversion approach",
                rationale="ARCHITECT designs the solution",
                critical=True,
            ),
            SpecialistAssignment(
                specialist=SpecialistRole.FORGE,
                phase_id="p3",
                task="Implement async SQLAlchemy conversion",
                rationale="FORGE handles code generation",
                critical=True,
            ),
        ]
    )


@pytest.fixture
def minimal_verification():
    return VerificationPlanSection(
        checks=[
            VerificationCheck(
                description="Type check modified files",
                method=VerificationMethod.TYPECHECK,
                phase_id="p4",
                is_blocking=True,
                success_threshold="No type errors",
            ),
            VerificationCheck(
                description="Run unit tests",
                method=VerificationMethod.UNIT_TEST,
                phase_id="p4",
                is_blocking=True,
                success_threshold="All tests pass",
            ),
        ]
    )


@pytest.fixture
def minimal_recovery():
    return RecoveryPlanSection(
        failure_strategies=[
            FailureModeStrategy(
                failure_mode="Type errors in implementation",
                phase_id="p3",
                strategy=RecoveryStrategyType.RETRY,
                fallback_description="Fix type errors and retry",
                max_retries=3,
            ),
            FailureModeStrategy(
                failure_mode="Test failures",
                phase_id="p4",
                strategy=RecoveryStrategyType.DECOMPOSE,
                fallback_description="Isolate failing tests and fix individually",
                max_retries=2,
            ),
        ],
        rollback_points=["p1", "p2", "p3"],
    )


@pytest.fixture
def minimal_completion():
    return CompletionCriteriaSection(
        criteria=[
            "All auth module files converted to async",
            "All existing tests pass",
            "No type errors in modified files",
            "Existing functionality is preserved",
        ],
        verification_required=True,
        human_review_before_merge=True,
    )


@pytest.fixture
def passing_review():
    return SelfReviewSection(
        is_coherent=True,
        is_minimal=True,
        is_executable=True,
        strengths=["Objective is clearly defined", "Critical path identified"],
        verdict="Plan is coherent, minimal, and ready for execution",
        score=0.85,
    )


@pytest.fixture
def complete_plan(
    minimal_objective,
    minimal_understanding,
    minimal_impact,
    minimal_risks,
    minimal_execution,
    minimal_assignments,
    minimal_verification,
    minimal_recovery,
    minimal_completion,
    passing_review,
):
    return ArchitectPlan(
        id="test_plan_001",
        title="Refactor auth module",
        objective=minimal_objective,
        current_understanding=minimal_understanding,
        impact_analysis=minimal_impact,
        risks=minimal_risks,
        execution_strategy=minimal_execution,
        specialist_assignments=minimal_assignments,
        verification_plan=minimal_verification,
        recovery_plan=minimal_recovery,
        completion_criteria=minimal_completion,
        self_review=passing_review,
    )


# ===========================================================================
# Tests: Plan Structure (Section 1-10)
# ===========================================================================


class TestArchitectPlanStructure:
    """Verify that all 10 sections are present and properly structured."""

    def test_complete_plan_has_all_sections(self, complete_plan):
        """All 10 sections must be present."""
        assert complete_plan.objective.goal
        assert complete_plan.current_understanding.summary
        assert complete_plan.impact_analysis.blast_radius
        assert complete_plan.risks.risks
        assert complete_plan.execution_strategy.phases
        assert complete_plan.specialist_assignments.assignments
        assert complete_plan.verification_plan.checks
        assert complete_plan.recovery_plan.failure_strategies
        assert complete_plan.completion_criteria.criteria
        assert complete_plan.self_review.verdict

    def test_valid_plan_passes_validation(self, complete_plan):
        """A complete plan should pass validate_complete()."""
        issues = complete_plan.validate_complete()
        assert not issues, f"Expected no issues, got: {issues}"

    def test_plan_includes_metadata(self, complete_plan):
        """Plan must have ID, title, status, and timestamps."""
        assert complete_plan.id
        assert complete_plan.title
        assert complete_plan.created_at
        assert complete_plan.updated_at
        assert complete_plan.status == PlanStatus.DRAFT

    def test_plan_to_execution_plan(self, complete_plan):
        """Plan must convert to execution plan format."""
        ep = complete_plan.to_execution_plan()
        assert ep["plan_id"] == "test_plan_001"
        assert ep["task_description"] == "Refactor the authentication module to async"
        assert len(ep["phases"]) == 4
        assert len(ep["edges"]) == 3
        assert len(ep["critical_path"]) > 0


# ===========================================================================
# Tests: Plan Validation
# ===========================================================================


class TestArchitectPlanValidation:
    """Verify that validation catches missing/incomplete sections."""

    def test_missing_objective_fails_validation(self):
        plan = ArchitectPlan(
            id="test",
            objective=ObjectiveSection(goal="", success_criteria=["test"]),
            current_understanding=CurrentUnderstandingSection(summary="test"),
            impact_analysis=ImpactAnalysisSection(blast_radius=BlastRadius.ISOLATED),
            risks=RiskSection(risks=[RiskItem(description="test", category="test")]),
            execution_strategy=ExecutionStrategySection(
                phases=[ExecutionPhase(id="p1", name="test", description="test", order=1)]
            ),
            specialist_assignments=SpecialistAssignmentsSection(
                assignments=[SpecialistAssignment(
                    specialist=SpecialistRole.FORGE, phase_id="p1", task="test", rationale="test"
                )]
            ),
            verification_plan=VerificationPlanSection(
                checks=[VerificationCheck(description="test", method=VerificationMethod.LINT, phase_id="p1")]
            ),
            recovery_plan=RecoveryPlanSection(
                failure_strategies=[FailureModeStrategy(
                    failure_mode="test", phase_id="p1", strategy=RecoveryStrategyType.RETRY,
                    fallback_description="test"
                )]
            ),
            completion_criteria=CompletionCriteriaSection(criteria=["test"]),
            self_review=SelfReviewSection(verdict="test", score=0.5),
        )
        issues = plan.validate_complete()
        assert any("goal is empty" in i for i in issues)

    def test_missing_success_criteria_fails(self):
        with pytest.raises(ValueError, match="At least one success criterion"):
            ObjectiveSection(goal="test", success_criteria=[])

    def test_missing_phases_fails_validation(self):
        with pytest.raises(ValueError, match="At least one execution phase"):
            ExecutionStrategySection(phases=[])

    def test_empty_risks_raises(self):
        with pytest.raises(ValueError, match="At least one risk"):
            RiskSection(risks=[])

    def test_empty_verification_raises(self):
        with pytest.raises(ValueError, match="At least one verification check"):
            VerificationPlanSection(checks=[])

    def test_empty_recovery_raises(self):
        with pytest.raises(ValueError, match="At least one failure mode strategy"):
            RecoveryPlanSection(failure_strategies=[])

    def test_empty_completion_raises(self):
        with pytest.raises(ValueError, match="At least one completion criterion"):
            CompletionCriteriaSection(criteria=[])


# ===========================================================================
# Tests: Execution Strategy
# ===========================================================================


class TestExecutionStrategy:
    """Verify execution strategy correctness."""

    def test_phase_ordering(self, minimal_execution):
        """Phases must be in order 1, 2, 3, ..."""
        for i, phase in enumerate(minimal_execution.phases):
            assert phase.order == i + 1

    def test_dependency_edges_match_phases(self, minimal_execution):
        """Dependency edges must reference valid phase IDs."""
        phase_ids = {p.id for p in minimal_execution.phases}
        for edge in minimal_execution.dependency_edges:
            assert edge.source in phase_ids
            assert edge.target in phase_ids

    def test_prerequisites_are_valid(self, minimal_execution):
        """Prerequisites must reference existing phases."""
        phase_ids = {p.id for p in minimal_execution.phases}
        for phase in minimal_execution.phases:
            for prereq in phase.prerequisites:
                assert prereq in phase_ids

    def test_no_cycles(self, minimal_execution):
        """Dependency graph must have no cycles."""
        cycles = minimal_execution.has_cycles()
        assert not cycles, f"Cycles detected: {cycles}"

    def test_cycle_detected(self):
        """A cycle must be detected in a cyclic graph."""
        phases = [
            ExecutionPhase(id="p1", name="P1", description="First", order=1),
            ExecutionPhase(id="p2", name="P2", description="Second", order=2, prerequisites=["p3"]),
            ExecutionPhase(id="p3", name="P3", description="Third", order=3, prerequisites=["p2"]),
        ]
        edges = [
            DependencyEdge(source="p1", target="p2"),
            DependencyEdge(source="p2", target="p3"),
            DependencyEdge(source="p3", target="p2"),  # This creates a cycle
        ]
        strategy = ExecutionStrategySection(phases=phases, dependency_edges=edges)
        cycles = strategy.has_cycles()
        assert cycles, "Cycle should have been detected"

    def test_critical_path_computed(self, minimal_execution):
        """Critical path must be a valid path through the DAG."""
        critical = minimal_execution.critical_path
        assert len(critical) > 0
        assert critical[0] == minimal_execution.phases[0].id
        assert critical[-1] == minimal_execution.phases[-1].id

    def test_compute_critical_path(self):
        """Critical path computation must find the longest path."""
        phases = [
            ExecutionPhase(id="a", name="A", description="Start", order=1, estimated_effort=1),
            ExecutionPhase(id="b", name="B", description="Middle", order=2, estimated_effort=3, prerequisites=["a"]),
            ExecutionPhase(id="c", name="C", description="Middle", order=3, estimated_effort=1, prerequisites=["a"]),
            ExecutionPhase(id="d", name="D", description="End", order=4, estimated_effort=1, prerequisites=["b", "c"]),
        ]
        edges = [
            DependencyEdge(source="a", target="b"),
            DependencyEdge(source="a", target="c"),
            DependencyEdge(source="b", target="d"),
            DependencyEdge(source="c", target="d"),
        ]
        strategy = ExecutionStrategySection(phases=phases, dependency_edges=edges)
        cp = strategy.compute_critical_path()
        assert "a" in cp
        assert "b" in cp or "c" in cp
        assert "d" in cp
        # The path through b (effort=3) should be on critical path, not c (effort=1)
        assert "b" in cp

    def test_prerequisite_order_must_precede(self, minimal_execution):
        """A phase's prerequisites must have a lower order number."""
        for phase in minimal_execution.phases:
            for prereq_id in phase.prerequisites:
                prereq = next(p for p in minimal_execution.phases if p.id == prereq_id)
                assert prereq.order < phase.order


# ===========================================================================
# Tests: Risk Analysis
# ===========================================================================


class TestRiskAnalysis:
    """Verify risk analysis correctness."""

    def test_risk_score_computed(self):
        risk = RiskItem(
            description="Test risk",
            category="implementation",
            level=RiskLevel.MEDIUM,
            likelihood=0.5,
            impact=0.8,
        )
        assert risk.risk_score == 0.4  # 0.5 * 0.8

    def test_risk_score_zero_if_likelihood_zero(self):
        risk = RiskItem(
            description="Test risk",
            category="implementation",
            likelihood=0.0,
            impact=0.8,
        )
        assert risk.risk_score == 0.0

    def test_overall_risk_level_computed(self):
        section = RiskSection(
            risks=[
                RiskItem(description="R1", category="test", likelihood=0.9, impact=0.9),
                RiskItem(description="R2", category="test", likelihood=0.8, impact=0.8),
            ]
        )
        assert section.compute_overall_level() == RiskLevel.CRITICAL

    def test_overall_risk_level_low(self):
        section = RiskSection(
            risks=[
                RiskItem(description="R1", category="test", likelihood=0.1, impact=0.1),
                RiskItem(description="R2", category="test", likelihood=0.2, impact=0.2),
            ]
        )
        assert section.compute_overall_level() == RiskLevel.LOW

    def test_risk_categories_covered(self):
        """Risk categories must be specific (security, architecture, implementation, etc.)."""
        valid_categories = {"security", "architecture", "implementation", "runtime", "maintenance", "coordination"}
        risks = [
            RiskItem(description="R1", category="security"),
            RiskItem(description="R2", category="implementation"),
        ]
        for r in risks:
            assert r.category in valid_categories


# ===========================================================================
# Tests: Specialist Selection
# ===========================================================================


class TestSpecialistSelection:
    """Verify specialist assignment correctness."""

    def test_default_specialist_map(self):
        """Common task types map to expected specialists."""
        assignments = SpecialistAssignmentsSection(
            assignments=[
                SpecialistAssignment(
                    specialist=SpecialistRole.FORGE, phase_id="impl",
                    task="Implement feature", rationale="FORGE writes code", critical=True,
                ),
                SpecialistAssignment(
                    specialist=SpecialistRole.SENTINEL, phase_id="sec",
                    task="Security review", rationale="SENTINEL checks security", critical=True,
                ),
                SpecialistAssignment(
                    specialist=SpecialistRole.ORACLE, phase_id="inv",
                    task="Research", rationale="ORACLE knows the repo", critical=True,
                ),
            ]
        )
        forge_assignments = assignments.get_by_specialist(SpecialistRole.FORGE)
        assert len(forge_assignments) == 1
        assert forge_assignments[0].task == "Implement feature"

    def test_multiple_specialists_for_same_phase(self):
        """A phase can have multiple specialist assignments."""
        assignments = SpecialistAssignmentsSection(
            assignments=[
                SpecialistAssignment(
                    specialist=SpecialistRole.FORGE, phase_id="p1",
                    task="Write code", rationale="FORGE implements", critical=True,
                ),
                SpecialistAssignment(
                    specialist=SpecialistRole.SENTINEL, phase_id="p1",
                    task="Review code", rationale="SENTINEL reviews", critical=True,
                ),
            ]
        )
        p1_assignments = [a for a in assignments.assignments if a.phase_id == "p1"]
        assert len(p1_assignments) == 2

    def test_specialist_role_enum(self):
        """SpecialistRole must contain all expected roles."""
        roles = {r.value for r in SpecialistRole}
        expected = {"FORGE", "SENTINEL", "ORACLE", "TERMINUS", "HERALD", "HERMES", "ARCHITECT"}
        assert roles == expected


# ===========================================================================
# Tests: Self-Review
# ===========================================================================


class TestSelfReview:
    """Verify self-critique logic."""

    def test_passing_review(self, passing_review):
        """A good review should pass."""
        assert passing_review.passes_review()
        assert passing_review.is_coherent
        assert passing_review.is_executable
        assert passing_review.score >= 0.6

    def test_failing_review_not_enough_score(self):
        """A low score should fail review."""
        review = SelfReviewSection(
            is_coherent=True,
            is_minimal=True,
            is_executable=True,
            score=0.3,
            verdict="Too many issues",
        )
        assert not review.passes_review()

    def test_failing_review_not_coherent(self):
        """Incoherent plans should fail review."""
        review = SelfReviewSection(
            is_coherent=False,
            is_minimal=True,
            is_executable=True,
            score=0.8,
            verdict="Incoherent",
        )
        assert not review.passes_review()

    def test_failing_review_not_executable(self):
        """Non-executable plans should fail review."""
        review = SelfReviewSection(
            is_coherent=True,
            is_minimal=True,
            is_executable=False,
            score=0.8,
            verdict="Not executable",
        )
        assert not review.passes_review()

    def test_failing_review_missing_sections(self):
        """Plans with missing sections should fail review."""
        review = SelfReviewSection(
            is_coherent=True,
            is_minimal=True,
            is_executable=True,
            missing_sections=["Recovery Plan"],
            score=0.8,
            verdict="Missing sections",
        )
        assert not review.passes_review()

    def test_review_with_issues(self):
        """Review should report issues with severity."""
        review = SelfReviewSection(
            is_coherent=False,
            is_minimal=True,
            is_executable=False,
            issues=[
                SelfReviewIssue(
                    description="No recovery strategy",
                    severity=RiskLevel.HIGH,
                    suggested_fix="Add recovery strategy",
                ),
                SelfReviewIssue(
                    description="Over-delegation to FORGE",
                    severity=RiskLevel.LOW,
                    suggested_fix="Consolidate tasks",
                ),
            ],
            strengths=["Critical path identified"],
            verdict="Plan has 1 major issue(s)",
            score=0.5,
        )
        assert len(review.issues) == 2
        assert any(i.severity == RiskLevel.HIGH for i in review.issues)
        assert len(review.strengths) == 1


# ===========================================================================
# Tests: ArchitectOrchestrator
# ===========================================================================


class TestArchitectOrchestrator:
    """Verify the ArchitectOrchestrator planning engine."""

    def test_create_plan_refactor(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Refactor the authentication module to use async SQLAlchemy",
            context={"active_specialists": ["ARCHITECT", "FORGE", "ORACLE"]},
        )
        assert plan.id
        assert plan.objective.goal
        assert len(plan.objective.success_criteria) > 0
        assert len(plan.execution_strategy.phases) > 0
        assert len(plan.specialist_assignments.assignments) > 0
        assert len(plan.verification_plan.checks) > 0
        assert len(plan.recovery_plan.failure_strategies) > 0

    def test_create_plan_fix(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Fix the login bug in auth.py",
        )
        assert plan.id
        assert plan.objective.goal
        # Fix plans should have success criteria about root cause
        assert any("Root cause" in sc for sc in plan.objective.success_criteria)

    def test_create_plan_feature(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Implement a new user dashboard component",
        )
        assert plan.id
        assert any("Feature" in sc for sc in plan.objective.success_criteria)

    def test_create_plan_security(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Add OAuth2 authentication with security review",
        )
        assert plan.id
        assert plan.objective.goal
        # Security-related plans should have security risks
        risk_categories = {r.category for r in plan.risks.risks}
        assert "security" in risk_categories

    def test_create_plan_with_constraints(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Add rate limiting to API",
            context={
                "constraints": {
                    "max_requests_per_second": {"value": "100"},
                    "database": "PostgreSQL",
                }
            },
        )
        # Constraints should appear in hidden constraints
        assert any("max_requests_per_second" in c for c in plan.objective.hidden_constraints)

    def test_self_critique_on_valid_plan(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Refactor the database layer",
        )
        issues = orchestrator.self_critique(plan)
        # A well-formed plan should have few issues
        # (may have minor issues like recovery gaps for non-critical phases)
        major_issues = [
            i for i in issues
            if "major" in i.lower() or "empty" in i.lower() or "empty" in i.lower()
        ]
        assert len(major_issues) == 0

    def test_self_critique_detects_orphan_edges(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Refactor the authentication module",
        )
        # Manually add a bad edge to test detection
        plan.execution_strategy.dependency_edges.append(
            type(plan.execution_strategy.dependency_edges[0])(
                source="nonexistent_phase", target="p1"
            )
        )
        issues = orchestrator.self_critique(plan)
        assert any("nonexistent_phase" in i for i in issues)

    def test_finalize_valid_plan(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Refactor the logging module",
        )
        plan_id = plan.id
        result = orchestrator.finalize(plan_id)
        assert result
        finalized_plan = orchestrator.get_plan(plan_id)
        assert finalized_plan
        assert finalized_plan.status == PlanStatus.VALIDATED

    def test_finalize_incomplete_plan_fails(self):
        orchestrator = ArchitectOrchestrator()
        # Create a plan and then corrupt it
        plan = orchestrator.create_plan(
            objective="Test task",
        )
        plan.objective.goal = ""  # Remove required content

        # finalize should fail validation
        result = orchestrator.finalize(plan.id)
        assert not result

    def test_estimate_cost(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Refactor the API layer",
        )
        cost = orchestrator.estimate_cost(plan)
        assert cost["total_estimated_effort"] > 0
        assert cost["average_risk_score"] >= 0.0
        assert cost["estimated_regression_probability"] >= 0.0
        assert cost["critical_path_length"] > 0

    def test_list_plans(self):
        orchestrator = ArchitectOrchestrator()
        orchestrator.create_plan(objective="Task 1")
        orchestrator.create_plan(objective="Task 2")
        assert len(orchestrator.list_plans()) == 2

    def test_get_plan(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(objective="Test")
        retrieved = orchestrator.get_plan(plan.id)
        assert retrieved is not None
        assert retrieved.id == plan.id

    def test_enrich_context_with_plan(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Refactor auth module",
        )
        context = {"existing_key": "value"}
        enriched = orchestrator.enrich_context_with_plan(plan, context)
        assert enriched["existing_key"] == "value"
        assert "architect_plan" in enriched
        assert enriched["architect_plan_id"] == plan.id
        # Verify per-specialist assignments are injected
        assert any(key.endswith("_assignments") for key in enriched.keys())


# ===========================================================================
# Tests: Terminal Display
# ===========================================================================


class TestPlanTerminalDisplay:
    """Verify terminal display formatting."""

    def test_to_terminal_display(self, complete_plan):
        display = complete_plan.to_terminal_display()
        assert "ARCHITECT PLAN" in display
        assert "OBJECTIVE" in display
        assert "EXECUTION STRATEGY" in display
        assert "SPECIALIST ASSIGNMENTS" in display
        assert "VERIFICATION CHECKS" in display
        assert "FAILURE RECOVERY" in display
        assert "RISKS" in display
        assert "COMPLETION CRITERIA" in display
        assert "SELF-REVIEW" in display
        assert display.startswith("╔══")


# ===========================================================================
# Tests: Objective Interpretation
# ===========================================================================


class TestObjectiveInterpretation:
    """Verify objective interpretation through the brain."""

    def test_refactor_objective_has_preservation_criteria(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            "Refactor the database layer to use connection pooling",
            {},
        )
        success_text = "\n".join(plan.objective.success_criteria)
        assert "preserved" in success_text.lower()

    def test_fix_objective_has_root_cause_criteria(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            "Fix the memory leak in the event loop",
            {},
        )
        success_text = "\n".join(plan.objective.success_criteria)
        assert "root cause" in success_text.lower()

    def test_security_objective_has_security_criteria(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            "Add authentication to the admin panel",
            {},
        )
        success_text = "\n".join(plan.objective.success_criteria)
        assert "security" in success_text.lower()

    def test_ambiguity_detection(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            "Fix it so that this works correctly with they system",
            {},
        )
        # The brain's objective intelligence should detect ambiguity
        assert plan.objective.ambiguities


# ===========================================================================
# Tests: Risk Analysis (via full plan creation)
# ===========================================================================


class TestOrchestratorRiskAnalysis:
    """Verify risk analysis through the brain's engines."""

    def test_security_risks_identified(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            "Implement OAuth2 authentication flow",
            {"active_specialists": []},
        )
        categories = {r.category for r in plan.risks.risks}
        assert "security" in categories

    def test_refactor_risks_identified(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            "Refactor the core module to use new patterns",
            {"affected_files": ["a.py", "b.py", "c.py", "d.py", "e.py", "f.py"]},
        )
        categories = {r.category for r in plan.risks.risks}
        assert "architecture" in categories

    def test_coordination_risks_with_multiple_specialists(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            "Implement feature",
            {"active_specialists": ["FORGE", "SENTINEL", "ORACLE"]},
        )
        categories = {r.category for r in plan.risks.risks}
        assert "coordination" in categories


# ===========================================================================
# Tests: Specialist Assignment Generation
# ===========================================================================


class TestOrchestratorSpecialistAssignment:
    """Verify specialist assignment generation in the orchestrator."""

    def test_security_task_assigns_sentinel(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Review security of auth system",
            context={"active_specialists": ["ARCHITECT", "SENTINEL"]},
        )
        assigned = {a.specialist.value for a in plan.specialist_assignments.assignments}
        assert "SENTINEL" in assigned

    def test_implementation_task_assigns_forge(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Implement new caching layer",
        )
        assigned = {a.specialist.value for a in plan.specialist_assignments.assignments}
        assert "FORGE" in assigned

    def test_research_task_assigns_oracle(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Research best practices for database sharding",
        )
        assigned = {a.specialist.value for a in plan.specialist_assignments.assignments}
        assert "ORACLE" in assigned


# ===========================================================================
# Tests: Build Plan from Conversation
# ===========================================================================


class TestBuildPlanFromConversation:
    """Verify build_plan_from_conversation convenience method."""

    def test_basic_plan_from_conversation(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.build_plan_from_conversation(
            objective="Refactor the authentication module",
            constraints={"max_changes": "5 files"},
        )
        assert plan.objective.goal == "Refactor the authentication module"
        assert any("max_changes" in c for c in plan.objective.hidden_constraints)

    def test_plan_from_conversation_with_repo_intel(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.build_plan_from_conversation(
            objective="Fix the login bug",
            repo_intel_output={
                "files": ["auth/login.py", "auth/token.py"],
                "modules": ["auth", "user"],
            },
        )
        assert plan.impact_analysis.affected_files
        assert "auth/login.py" in plan.impact_analysis.affected_files

# ===========================================================================
# Tests: Event Bus Integration
# ===========================================================================


class MockEventBus:
    """Async mock event bus that captures published events for testing."""

    def __init__(self):
        self.events: List = []

    async def publish(self, event) -> None:
        """Capture published events."""
        self.events.append(event)


class TestPlanLifecycleEvents:
    """Verify plan lifecycle events are emitted through the event bus."""

    def test_plan_created_event_emitted(self):
        """Creating a plan must emit a PLAN_CREATED event."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Refactor the authentication module",
        )

        assert len(bus.events) == 1
        event = bus.events[0]
        from runtime_next.models.events import EventType
        assert event.type == EventType.PLAN_CREATED
        assert event.plan_id == plan.id
        assert event.phase_count == len(plan.execution_strategy.phases)
        assert event.verification_count == len(plan.verification_plan.checks)
        assert event.self_review_score == plan.self_review.score
        assert event.failure_reason is None

    def test_plan_validated_event_emitted_on_finalize(self):
        """Finalizing a plan must emit a PLAN_VALIDATED event."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Add rate limiting middleware",
        )

        # create_plan emits PLAN_CREATED, so clear and test finalize
        bus.events.clear()

        result = orchestrator.finalize(plan.id)
        assert result, "Plan should finalize successfully"

        assert len(bus.events) == 1
        event = bus.events[0]
        from runtime_next.models.events import EventType
        assert event.type == EventType.PLAN_VALIDATED
        assert event.plan_id == plan.id
        assert event.failure_reason is None

    def test_plan_failed_event_on_broken_finalize(self):
        """Finalizing a broken plan must emit a PLAN_FAILED event."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Test task for failure",
        )

        # Corrupt the plan so it fails validation
        plan.objective.goal = ""

        bus.events.clear()

        result = orchestrator.finalize(plan.id)
        assert not result, "Broken plan should fail finalization"

        assert len(bus.events) == 1
        event = bus.events[0]
        from runtime_next.models.events import EventType
        assert event.type == EventType.PLAN_FAILED
        assert event.plan_id == plan.id
        assert event.failure_reason is not None

    def test_plan_failed_event_on_broken_self_review(self):
        """Finalizing a plan with failing self-review must emit PLAN_FAILED."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Test",
        )

        # Force self-review to fail
        plan.self_review.score = 0.1
        plan.self_review.is_coherent = False
        plan.self_review.is_executable = False

        bus.events.clear()

        result = orchestrator.finalize(plan.id)
        assert not result, "Plan with failing self-review should fail finalization"

        assert len(bus.events) == 1
        event = bus.events[0]
        from runtime_next.models.events import EventType
        assert event.type == EventType.PLAN_FAILED
        assert event.failure_reason is not None
        assert "self-review" in event.failure_reason.lower()

    def test_no_events_without_event_bus(self):
        """Orchestrator without event_bus should not emit events."""
        orchestrator = ArchitectOrchestrator()  # No event_bus
        plan = orchestrator.create_plan(
            objective="Test without event bus",
        )
        # Should not crash — no events expected
        assert plan.id

    def test_multiple_plans_multiple_events(self):
        """Multiple plans should each emit their own events."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)

        plan1 = orchestrator.create_plan(objective="Plan one")
        assert len(bus.events) == 1

        plan2 = orchestrator.create_plan(objective="Plan two")
        assert len(bus.events) == 2

        events = bus.events
        assert events[0].plan_id != events[1].plan_id
        assert all(e.type.value == "plan_created" for e in events)


    def test_architect_plan_event_fields(self):
        """ArchitectPlanEvent must have correct field types and defaults."""
        from runtime_next.models.events import ArchitectPlanEvent, EventType
        event = ArchitectPlanEvent(
            id="test_event_001",
            type=EventType.PLAN_CREATED,
            plan_id="plan_001",
        )
        assert event.id == "test_event_001"
        assert event.type == EventType.PLAN_CREATED
        assert event.plan_id == "plan_001"
        assert event.plan_title == ""  # default
        assert event.phase_count == 0  # default
        assert event.specialist_roles == []  # default
        assert event.self_review_score == 0.0  # default
        assert event.failure_reason is None  # default

    def test_event_has_timestamp(self):
        """Events must have a valid timestamp."""
        from runtime_next.models.events import ArchitectPlanEvent, EventType
        from datetime import datetime
        event = ArchitectPlanEvent(
            id="test_event_002",
            type=EventType.PLAN_CREATED,
            plan_id="plan_002",
        )
        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)


    def test_architect_plan_event_failure_reason(self):
        """PLAN_FAILED events must carry failure_reason."""
        from runtime_next.models.events import ArchitectPlanEvent, EventType
        event = ArchitectPlanEvent(
            id="fail_event",
            type=EventType.PLAN_FAILED,
            plan_id="plan_fail",
            failure_reason="Self-review failed: score too low",
        )
        assert event.failure_reason == "Self-review failed: score too low"


    def test_plan_finalize_revalidates_self_review(self):
        """finalize() should call passes_review() and fail if score is low."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Edge case test",
        )

        # Make self-review fail
        plan.self_review.score = 0.0
        plan.self_review.is_coherent = False

        bus.events.clear()

        result = orchestrator.finalize(plan.id)
        assert not result

        # Should emit PLAN_FAILED
        assert len(bus.events) == 1
        assert bus.events[0].type.value == "plan_failed"


class TestEventPayloadContents:
    """Verify event payloads carry meaningful data."""

    def test_created_event_has_plan_summary(self):
        """PLAN_CREATED event must include plan summary data."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Refactor auth module to async",
            context={"active_specialists": ["ARCHITECT", "FORGE", "SENTINEL"]},
        )

        event = bus.events[0]
        assert event.plan_title
        assert event.objective
        assert event.phase_count > 0
        assert len(event.specialist_roles) > 0
        assert event.risk_level in ("", "low", "medium", "high", "critical")
        assert event.verification_count > 0
        assert 0.0 <= event.self_review_score <= 1.0

    def test_event_payload_includes_plan_id(self):
        """Event payload dict must contain plan_id."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(objective="Test")

        event = bus.events[0]
        payload = event.payload
        assert payload.get("plan_id") == plan.id
        assert payload.get("event_type") == "plan_created"


    def test_validated_event_has_correct_specialist_roles(self):
        """PLAN_VALIDATED events must list assigned specialists."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Implement OAuth2 with security review",
        )

        bus.events.clear()
        orchestrator.finalize(plan.id)

        event = bus.events[0]
        assigned_roles = {a.specialist.value for a in plan.specialist_assignments.assignments}
        for role in event.specialist_roles:
            assert role in assigned_roles, f"Role {role} not in assigned roles {assigned_roles}"


    def test_failed_event_payload_includes_reason(self):
        """PLAN_FAILED event payload must include failure_reason."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(objective="Fail test")

        plan.objective.goal = ""
        bus.events.clear()
        orchestrator.finalize(plan.id)

        event = bus.events[0]
        assert event.failure_reason is not None
        assert "goal is empty" in event.failure_reason.lower() or len(event.failure_reason) > 0


    def test_events_have_unique_ids(self):
        """Consecutive events should have different IDs."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)

        plan = orchestrator.create_plan(objective="Test")
        created_event = bus.events[0]

        bus.events.clear()
        orchestrator.finalize(plan.id)
        validated_event = bus.events[0]

        assert created_event.id != validated_event.id

    def test_plan_finalize_with_skip_self_review_pass(self):
        """Plan with valid self-review should finalize successfully."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="A sufficiently detailed objective for planning",
        )

        # Ensure self-review passes
        plan.self_review.score = 0.85
        plan.self_review.is_coherent = True
        plan.self_review.is_executable = True
        plan.self_review.is_minimal = True

        bus.events.clear()
        result = orchestrator.finalize(plan.id)
        assert result, "Plan should finalize with passing self-review"

        assert len(bus.events) == 1
        assert bus.events[0].type.value == "plan_validated"

    def test_plan_finalize_with_valid_self_review_emits_validated(self):
        """A healthy plan finalize should not emit PLAN_FAILED."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Refactor logging infrastructure",
        )

        bus.events.clear()
        result = orchestrator.finalize(plan.id)
        assert result

        event_types = [e.type.value for e in bus.events]
        assert "plan_failed" not in event_types
        assert "plan_validated" in event_types


    def test_build_plan_from_conversation_emits_event(self):
        """build_plan_from_conversation must also emit PLAN_CREATED."""
        bus = MockEventBus()
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.build_plan_from_conversation(
            objective="Refactor the authentication module",
            constraints={"max_changes": "5 files"},
        )

        assert len(bus.events) == 1
        assert bus.events[0].type.value == "plan_created"
        assert bus.events[0].plan_id == plan.id


    def test_plan_from_conversation_with_memory(self):
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.build_plan_from_conversation(
            objective="Implement rate limiting",
            memory_context={
                "system_decisions": [{"doc": "Use Redis for rate limiting"}],
                "active_specialists": ["FORGE", "SENTINEL"],
            },
        )
        assert plan.current_understanding.relevant_modules is not None
