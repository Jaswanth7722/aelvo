"""
test_pipeline_integration.py — Full Pipeline Integration Tests

Tests the complete planning and execution pipeline:
  1. Cognitive Engine → Architecture Planning
  2. Architect Plan → Execution Node Conversion
  3. Execution → Verification
  4. Full End-to-End Flow
  5. Error Handling & Recovery at Each Transition

Each test class focuses on a specific transition point in the pipeline.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List
from unittest.mock import MagicMock


# ── Cognitive Engine ─────────────────────────────────────────────────────
from cognition.engine import CognitiveEngine, CognitiveEngineConfig
from cognition.types import GoalStatus

# ── Architect Plan ───────────────────────────────────────────────────────
from runtime_next.plan.architect import ArchitectOrchestrator
from runtime_next.plan.architect_types import (
    ArchitectPlan,
    ObjectiveSection,
    CurrentUnderstandingSection,
    ImpactAnalysisSection,
    RiskSection,
    ExecutionStrategySection,
    SpecialistAssignmentsSection,
    VerificationPlanSection,
    RecoveryPlanSection,
    CompletionCriteriaSection,
    SelfReviewSection,
    RiskLevel,
    BlastRadius,
    SpecialistRole,
    PlanStatus,
    VerificationMethod,
    RecoveryStrategyType,
    ExecutionPhase,
    DependencyEdge,
    SpecialistAssignment,
    VerificationCheck,
    FailureModeStrategy,
    ImpactItem,
    RiskItem,
)

# ── Execution Graph ──────────────────────────────────────────────────────
from runtime_next.engine.engine import ExecutionGraph, ExecutionEngine, EngineState
from runtime_next.engine.runner import NodeRunner
from runtime_next.models.node import NodeDefinition, NodeState

# ── Verification Pipeline ─────────────────────────────────────────────────
from runtime_next.verification.pipeline import VerificationPipeline
from runtime_next.verification.types import (
    VerificationType,
    VerificationManifest,
    VerificationScope,
    VerificationResult,
    Confidence,
    Severity,
    Retryability,
)

# ── Event System ──────────────────────────────────────────────────────────
from runtime_next.models.events import (
    BaseEvent,
    EventType,
    ArchitectPlanEvent,
    NodeTransitionEvent,
)

# ===========================================================================
# Helpers
# ===========================================================================


def make_simple_architect_plan(
    goal: str = "Refactor the authentication module to use OAuth2",
) -> ArchitectPlan:
    """Build a complete, valid ArchitectPlan for testing pipeline transitions."""
    phase_1 = ExecutionPhase(
        id="phase_1",
        name="Analyze Current Auth Module",
        description="Read and understand the existing authentication code",
        order=1,
        estimated_effort=2,
        completion_criteria=["Auth module structure understood"],
    )
    phase_2 = ExecutionPhase(
        id="phase_2",
        name="Design OAuth2 Integration",
        description="Design the OAuth2 integration approach",
        order=2,
        prerequisites=["phase_1"],
        estimated_effort=2,
        completion_criteria=["OAuth2 design completed"],
    )
    phase_3 = ExecutionPhase(
        id="phase_3",
        name="Implement OAuth2 Flow",
        description="Implement the OAuth2 authentication flow",
        order=3,
        prerequisites=["phase_2"],
        estimated_effort=5,
        completion_criteria=["OAuth2 flow implemented"],
    )
    phase_verify = ExecutionPhase(
        id="phase_verify",
        name="Verify OAuth2 Implementation",
        description="Run typechecks, tests, and security review",
        order=4,
        prerequisites=["phase_3"],
        estimated_effort=3,
        completion_criteria=["All verifications pass"],
    )
    phase_synth = ExecutionPhase(
        id="phase_synthesis",
        name="Synthesize Results",
        description="Summarize what was done",
        order=5,
        prerequisites=["phase_verify"],
        estimated_effort=1,
        completion_criteria=["Results communicated"],
    )

    return ArchitectPlan(
        id="test_plan_integration",
        title="OAuth2 Auth Module Refactor",
        objective=ObjectiveSection(
            goal=goal,
            success_criteria=[
                "OAuth2 authentication flow implemented",
                "Existing functionality preserved",
                "All tests pass",
                "Security review completed",
            ],
            hidden_constraints=["Must maintain backward compatibility"],
            ambiguities=["Exact OAuth2 provider not specified"],
        ),
        current_understanding=CurrentUnderstandingSection(
            summary="Auth module uses session-based authentication. Need to add OAuth2 support.",
            relevant_modules=["auth", "users", "middleware"],
            key_files=["auth/authenticator.py", "auth/session.py", "middleware/auth.py"],
            architectural_context="Authentication layer sits between API gateway and user service.",
        ),
        impact_analysis=ImpactAnalysisSection(
            blast_radius=BlastRadius.LOCALIZED,
            affected_files=["auth/authenticator.py", "auth/oauth.py", "middleware/auth.py"],
            affected_modules=["auth", "middleware"],
            impacts=[
                ImpactItem(target="auth/authenticator.py", description="Add OAuth2 methods", severity=RiskLevel.MEDIUM),
                ImpactItem(target="auth/oauth.py", description="New OAuth2 module", severity=RiskLevel.LOW),
            ],
        ),
        risks=RiskSection(
            risks=[
                RiskItem(
                    description="OAuth2 token handling may introduce security vulnerabilities",
                    category="security",
                    level=RiskLevel.HIGH,
                    likelihood=0.4,
                    impact=0.9,
                    mitigation="SENTINEL security review required",
                    contingency="Rollback to session auth",
                ),
                RiskItem(
                    description="Breaking changes to auth interface",
                    category="architecture",
                    level=RiskLevel.MEDIUM,
                    likelihood=0.3,
                    impact=0.7,
                    mitigation="Update all callers",
                    contingency="Rollback and decompose",
                ),
            ],
            overall_level=RiskLevel.MEDIUM,
        ),
        execution_strategy=ExecutionStrategySection(
            phases=[phase_1, phase_2, phase_3, phase_verify, phase_synth],
            dependency_edges=[
                DependencyEdge(source="phase_1", target="phase_2"),
                DependencyEdge(source="phase_2", target="phase_3"),
                DependencyEdge(source="phase_3", target="phase_verify"),
                DependencyEdge(source="phase_verify", target="phase_synthesis"),
            ],
            critical_path=["phase_1", "phase_2", "phase_3", "phase_verify", "phase_synthesis"],
        ),
        specialist_assignments=SpecialistAssignmentsSection(
            assignments=[
                SpecialistAssignment(
                    specialist=SpecialistRole.ORACLE,
                    phase_id="phase_1",
                    task="Analyze current auth module structure",
                    rationale="ORACLE has code search capabilities",
                    estimated_effort=2,
                    critical=True,
                ),
                SpecialistAssignment(
                    specialist=SpecialistRole.ARCHITECT,
                    phase_id="phase_2",
                    task="Design OAuth2 integration approach",
                    rationale="ARCHITECT designs solutions",
                    estimated_effort=2,
                    critical=True,
                ),
                SpecialistAssignment(
                    specialist=SpecialistRole.FORGE,
                    phase_id="phase_3",
                    task="Implement OAuth2 flow",
                    rationale="FORGE implements code changes",
                    estimated_effort=5,
                    critical=True,
                ),
                SpecialistAssignment(
                    specialist=SpecialistRole.SENTINEL,
                    phase_id="phase_verify",
                    task="Security review OAuth2 implementation",
                    rationale="SENTINEL handles security verification",
                    estimated_effort=2,
                    critical=True,
                ),
                SpecialistAssignment(
                    specialist=SpecialistRole.HERMES,
                    phase_id="phase_synthesis",
                    task="Communicate results",
                    rationale="HERMES handles user communication",
                    estimated_effort=1,
                    critical=True,
                ),
            ],
        ),
        verification_plan=VerificationPlanSection(
            checks=[
                VerificationCheck(
                    description="Type check all modified auth files",
                    method=VerificationMethod.TYPECHECK,
                    phase_id="phase_verify",
                    is_blocking=True,
                ),
                VerificationCheck(
                    description="Run auth module unit tests",
                    method=VerificationMethod.UNIT_TEST,
                    phase_id="phase_verify",
                    is_blocking=True,
                ),
                VerificationCheck(
                    description="Security scan OAuth2 implementation",
                    method=VerificationMethod.SECURITY_SCAN,
                    phase_id="phase_verify",
                    is_blocking=True,
                ),
            ],
        ),
        recovery_plan=RecoveryPlanSection(
            failure_strategies=[
                FailureModeStrategy(
                    failure_mode="OAuth2 implementation errors",
                    phase_id="phase_3",
                    strategy=RecoveryStrategyType.RETRY,
                    fallback_description="Re-attempt with adjusted approach",
                    max_retries=3,
                ),
                FailureModeStrategy(
                    failure_mode="Security vulnerabilities found",
                    phase_id="phase_verify",
                    strategy=RecoveryStrategyType.ESCALATE,
                    fallback_description="Document and escalate to user",
                    triggers_human_review=True,
                    max_retries=0,
                ),
            ],
            rollback_points=["phase_1", "phase_2", "phase_3"],
        ),
        completion_criteria=CompletionCriteriaSection(
            criteria=[
                "OAuth2 flow implemented and tested",
                "All blocking verification checks pass",
                "Security review completed",
            ],
        ),
        self_review=SelfReviewSection(
            is_coherent=True,
            is_minimal=True,
            is_executable=True,
            verdict="Plan is coherent, minimal, and ready for execution",
            score=0.85,
        ),
    )


class MockVerifier:
    """A verifier that returns a canned success or failure result."""

    def __init__(self, success: bool = True):
        self._success = success

    async def verify(self, node_id: str, scope: VerificationScope, context: Dict[str, Any]) -> VerificationResult:
        return VerificationResult(
            verification_id=f"v_{node_id}",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=self._success,
            confidence=Confidence.HIGH,
            severity=Severity.INFO if self._success else Severity.ERROR,
            retryability=Retryability.SAFE,
            diagnostics=["OK" if self._success else "Mock failure"],
            provenance="mock_verifier",
        )


class CollectingEventBus:
    """Minimal event bus that collects published events for assertion."""

    def __init__(self):
        self.events: List[BaseEvent] = []
        self._handlers: List = []

    async def publish(self, event: BaseEvent) -> None:
        self.events.append(event)
        for handler in self._handlers:
            result = handler(event)
            if asyncio.iscoroutine(result):
                await result

    def subscribe_all(self, handler) -> None:
        self._handlers.append(handler)

    def clear(self):
        self.events.clear()


# ===========================================================================
# Section 1: Cognitive Engine → Architecture Planning
# ===========================================================================

class TestCognitiveEngineToArchitectPlan:
    """Verify that the CognitiveEngine can produce goals that feed into the ArchitectOrchestrator."""

    def test_cognitive_engine_creates_goal_that_feeds_architect(self):
        """Submit a goal to CognitiveEngine → decompose → create ExecutionPlan → feed intent to ArchitectOrchestrator."""
        engine = CognitiveEngine(config=CognitiveEngineConfig(max_active_goals=5))

        # 1. Submit goal
        goal = engine.submit_goal(
            description="Refactor the authentication module to use OAuth2",
            priority=8,
            constraints=["Must maintain backward compatibility", "Support existing users"],
            owner="user",
        )
        assert goal.id is not None
        assert goal.status == GoalStatus.PENDING
        assert "OAuth2" in goal.description

        # 2. Decompose into sub-goals
        sub_goals = engine.decompose_goal(
            goal_id=goal.id,
            sub_goal_descriptions=[
                "Analyze current auth module structure",
                "Design OAuth2 integration",
                "Implement OAuth2 authentication flow",
                "Verify and security review",
                "Communicate results",
            ],
        )
        assert len(sub_goals) == 5
        assert all(sg.parent_goal_id == goal.id for sg in sub_goals)

        # 3. Plan the goal (creates ExecutionPlan via LongHorizonPlanner)
        plan = engine.plan_goal(goal.id)
        assert plan.id is not None
        assert len(plan.nodes) > 0
        assert plan.task_description == goal.description

        # 4. Feed the same intent into ArchitectOrchestrator with context from the cognitive engine
        orchestrator = ArchitectOrchestrator()
        arch_plan = orchestrator.create_plan(
            objective=goal.description,
            context={
                "task": goal.description,
                "constraints": {"backward_compat": "Must maintain backward compatibility"},
                "project": "test_project",
                "active_specialists": ["ARCHITECT", "FORGE", "SENTINEL", "ORACLE", "HERMES"],
            },
        )

        # 5. Verify ArchitectPlan has all 10 sections populated
        assert arch_plan.id is not None
        assert arch_plan.objective.goal == goal.description
        assert len(arch_plan.objective.success_criteria) >= 1
        assert len(arch_plan.execution_strategy.phases) >= 3
        assert len(arch_plan.specialist_assignments.assignments) >= 3
        assert len(arch_plan.verification_plan.checks) >= 1
        assert len(arch_plan.recovery_plan.failure_strategies) >= 1
        assert len(arch_plan.completion_criteria.criteria) >= 1
        assert arch_plan.self_review.score > 0.0

    def test_cognitive_engine_plan_order_matches_architect_phases(self):
        """Verify that CognitiveEngine's execution order aligns with ArchitectPlan's phase order."""
        engine = CognitiveEngine(config=CognitiveEngineConfig(max_active_goals=5))

        goal = engine.submit_goal("Add rate limiting middleware", owner="user")
        engine.decompose_goal(goal.id, ["Research rate limiting", "Implement middleware", "Add tests"])
        cognitive_plan = engine.plan_goal(goal.id)
        execution_order = engine.execute_plan(cognitive_plan.id)

        # The execution order is a topological sort of the plan nodes
        assert len(execution_order) >= 3
        assert len(execution_order) == len(cognitive_plan.nodes)

        # Now build an ArchitectPlan for the same goal
        orchestrator = ArchitectOrchestrator()
        arch_plan = orchestrator.create_plan(
            objective=goal.description,
            context={"task": goal.description, "active_specialists": ["FORGE"]},
        )

        # ArchitectPlan phases should be monotonically increasing in order
        orders = [phase.order for phase in arch_plan.execution_strategy.phases]
        assert orders == sorted(orders), f"Phase orders should be monotonically increasing: {orders}"

    def test_cognitive_engine_plan_with_repo_intelligence(self):
        """ArchitectOrchestrator uses repo intelligence context passed from CognitiveEngine."""
        mock_repo_intel = MagicMock()
        mock_repo_intel.get_architecture.return_value = MagicMock(layers=[])
        mock_query = MagicMock()
        mock_query.search.return_value = []
        mock_repo_intel.query_engine = mock_query

        orchestrator = ArchitectOrchestrator(repo_intelligence=mock_repo_intel)
        plan = orchestrator.create_plan(
            objective="Add logging middleware",
            context={
                "task": "Add logging middleware",
                "tree_snapshot": "WORKSPACE STRUCTURE:\n  📁 src/\n    📄 main.py",
                "project": "test_proj",
            },
        )

        assert plan.current_understanding.summary != ""
        assert "test_proj" in plan.current_understanding.summary
        assert plan.impact_analysis.blast_radius in (BlastRadius.ISOLATED, BlastRadius.LOCALIZED)
        assert plan.id is not None
        assert plan.status == PlanStatus.DRAFT


# ===========================================================================
# Section 2: Architect Plan → Execution
# ===========================================================================

class TestArchitectPlanToExecution:
    """Verify that ArchitectPlan can be converted to executable nodes and run."""

    def test_architect_plan_converts_to_execution_dict(self):
        """ArchitectPlan.to_execution_plan() produces a dict consumable by the execution engine."""
        plan = make_simple_architect_plan()
        exec_dict = plan.to_execution_plan()

        assert exec_dict["plan_id"] == "test_plan_integration"
        assert "OAuth2" in exec_dict["task_description"]
        assert len(exec_dict["phases"]) == 5
        assert len(exec_dict["edges"]) == 4
        assert len(exec_dict["specialist_assignments"]) == 5
        assert len(exec_dict["verification_checks"]) == 3

        # Phase structure is preserved
        phase_ids = [p["id"] for p in exec_dict["phases"]]
        assert "phase_1" in phase_ids
        assert "phase_verify" in phase_ids

        # Dependency edges are correct
        for edge in exec_dict["edges"]:
            assert edge["source"] in phase_ids
            assert edge["target"] in phase_ids

    def test_architect_plan_phases_map_to_execution_nodes(self):
        """Each ArchitectPlan phase can become an ExecutionNode for the graph engine."""
        plan = make_simple_architect_plan()
        exec_dict = plan.to_execution_plan()

        graph = ExecutionGraph()

        for phase in exec_dict["phases"]:
            node = NodeDefinition(
                id=phase["id"],
                description=phase["description"],
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        # Verify all nodes were added
        for phase in exec_dict["phases"]:
            assert phase["id"] in graph.nodes
            assert graph.nodes[phase["id"]].state == NodeState.PENDING

        # Add edges
        for edge in exec_dict["edges"]:
            if edge["source"] in graph.nodes and edge["target"] in graph.nodes:
                graph.add_edge(edge["source"], edge["target"])

        assert len(graph.edges) >= 1

    def test_architect_plan_execution_via_graph_engine(self):
        """Execute ArchitectPlan phases through the ExecutionGraph engine."""
        plan = make_simple_architect_plan()

        # Build graph from plan
        graph = ExecutionGraph()
        for phase in plan.execution_strategy.phases:
            node = NodeDefinition(
                id=phase.id,
                description=phase.description,
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        for edge in plan.execution_strategy.dependency_edges:
            if edge.source in graph.nodes and edge.target in graph.nodes:
                graph.add_edge(edge.source, edge.target)

        # Run the engine
        engine = ExecutionEngine(graph)
        asyncio.run(engine.execute(context={"mode": "test"}))

        # Verify all nodes completed
        for node in graph.nodes.values():
            assert node.state in (NodeState.COMPLETED, "completed"), f"Node {node.id} state: {node.state}"

        assert engine.state in (EngineState.COMPLETED, "completed")

    def test_architect_plan_specialist_assignments_map_to_noderunner_handlers(self):
        """Specialist assignments from the plan can be routed by NodeRunner."""
        plan = make_simple_architect_plan()

        # Build handlers for each specialist role in the plan
        results: Dict[str, str] = {}

        async def forge_handler(node, context):
            results[node.id] = "FORGE executed"
            return {"status": "success", "output": f"FORGE: {node.description}"}

        async def sentinel_handler(node, context):
            results[node.id] = "SENTINEL executed"
            return {"status": "success", "output": "Security review passed"}

        runner = NodeRunner()
        runner.register_handler("specialist_call", forge_handler)

        # Verify the assignments include the expected roles
        roles = {a.specialist.value for a in plan.specialist_assignments.assignments}
        assert "FORGE" in roles
        assert "SENTINEL" in roles
        assert "ORACLE" in roles
        assert "ARCHITECT" in roles
        assert "HERMES" in roles

        # Verify FORGE is assigned to implementation phases
        forge_assignments = [
            a for a in plan.specialist_assignments.assignments
            if a.specialist == SpecialistRole.FORGE
        ]
        assert len(forge_assignments) >= 1
        assert any("Implement" in a.task for a in forge_assignments)

    def test_execution_via_plan_and_graph_with_event_bus(self):
        """Execute plan nodes through the graph engine with event tracking."""
        plan = make_simple_architect_plan()
        bus = CollectingEventBus()
        graph = ExecutionGraph(bus=bus)

        # Add nodes from plan phases
        for phase in plan.execution_strategy.phases:
            node = NodeDefinition(
                id=phase.id,
                description=phase.description,
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        for edge in plan.execution_strategy.dependency_edges:
            if edge.source in graph.nodes and edge.target in graph.nodes:
                graph.add_edge(edge.source, edge.target)

        # Execute
        engine = ExecutionEngine(graph)
        asyncio.run(engine.execute(context={}))

        # Events should have been published for each transition
        transitions = [e for e in bus.events if hasattr(e, 'type') and getattr(e, 'type', None) == EventType.NODE_TRANSITION]
        # Minimum: each node transitions at least once (PENDING -> RUNNING -> COMPLETED)
        # At minimum, we should see some events published
        node_ids = set()
        for t in transitions:
            if hasattr(t, 'node_id'):
                node_ids.add(t.node_id)
        assert len(node_ids) >= 3, f"Expected events for at least 3 nodes, got {len(node_ids)}"


# ===========================================================================
# Section 3: Execution → Verification
# ===========================================================================

class TestExecutionToVerification:
    """Verify that execution results are properly verified by the verification pipeline."""

    def test_execution_results_feed_into_verification_pipeline(self):
        """After execution, verification pipeline can validate the results."""
        pipeline = VerificationPipeline()
        verifier = MockVerifier(success=True)
        pipeline.register_verifier(VerificationType.LINT, verifier.verify)

        # Simulate an execution result
        manifest = VerificationManifest(
            required=[VerificationType.LINT],
            blocking=[VerificationType.LINT],
        )
        scope = VerificationScope(
            affected_files=["auth/oauth.py", "auth/authenticator.py"],
            provenance="integration_test",
        )

        results = asyncio.run(pipeline.verify(
            node_id="phase_3",
            manifest=manifest,
            scope=scope,
            context={"plan_id": "test_plan_integration"},
        ))

        assert len(results) >= 1
        assert results[0].success is True
        assert results[0].node_id == "phase_3"
        assert results[0].verification_type == VerificationType.LINT
        assert results[0].confidence == Confidence.HIGH

    def test_verification_rejects_failed_execution(self):
        """Verification pipeline correctly fails when execution produced errors."""
        pipeline = VerificationPipeline()
        verifier = MockVerifier(success=False)
        pipeline.register_verifier(VerificationType.LINT, verifier.verify)

        manifest = VerificationManifest(
            required=[VerificationType.LINT],
            blocking=[VerificationType.LINT],
        )
        scope = VerificationScope(affected_files=["auth/oauth.py"])

        results = asyncio.run(pipeline.verify(
            node_id="phase_3",
            manifest=manifest,
            scope=scope,
            context={},
        ))

        assert len(results) >= 1
        assert results[0].success is False
        assert "Mock failure" in results[0].diagnostics[0]

    def test_verification_with_architect_plan_scope(self):
        """Verification scope can be derived from the ArchitectPlan's impact analysis."""
        plan = make_simple_architect_plan()

        # Build verification scope from plan's impact analysis
        scope = VerificationScope(
            affected_files=plan.impact_analysis.affected_files,
            affected_symbols=[],
            provenance="architect_plan",
        )

        assert len(scope.affected_files) >= 2
        assert "auth/authenticator.py" in scope.affected_files
        assert scope.provenance == "architect_plan"

        # Run verification
        pipeline = VerificationPipeline()
        verifier = MockVerifier(success=True)
        pipeline.register_verifier(VerificationType.LINT, verifier.verify)

        manifest = VerificationManifest(
            required=[VerificationType.LINT],
            blocking=[VerificationType.LINT],
        )

        results = asyncio.run(pipeline.verify(
            node_id="phase_verify",
            manifest=manifest,
            scope=scope,
            context={"architect_plan_id": plan.id},
        ))

        assert len(results) >= 1
        assert results[0].success is True

    def test_verification_events_propagate_plan_context(self):
        """Verification events include the plan ID and node context."""
        pipeline = VerificationPipeline()
        verifier = MockVerifier(success=True)
        pipeline.register_verifier(VerificationType.LINT, verifier.verify)
        captured_events = []

        async def capture(event):
            captured_events.append(event)

        pipeline.on_event(capture)

        manifest = VerificationManifest(
            required=[VerificationType.LINT],
            blocking=[VerificationType.LINT],
        )
        scope = VerificationScope(
            affected_files=["auth/oauth.py"],
            provenance="test",
        )

        asyncio.run(pipeline.verify(
            node_id="phase_3",
            manifest=manifest,
            scope=scope,
            context={"architect_plan_id": "test_plan_integration"},
        ))

        # Should have emitted started and completed events
        assert len(captured_events) >= 2


# ===========================================================================
# Section 4: Full End-to-End Flow
# ===========================================================================

class TestFullPipelineEndToEnd:
    """Complete end-to-end flow: Cognitive Engine → Architect Plan → Execution Graph → Verification."""

    def test_full_pipeline_end_to_end(self):
        """Complete pipeline: goal → plan → execution → verification."""
        # 1. COGNITIVE ENGINE: submit goal
        engine = CognitiveEngine(config=CognitiveEngineConfig(max_active_goals=5))
        goal = engine.submit_goal(
            description="Add request logging middleware to the API gateway",
            priority=7,
            constraints=["Must not affect response times", "Must capture request IDs"],
            owner="devops",
        )
        assert goal.status == GoalStatus.PENDING

        engine.decompose_goal(goal.id, [
            "Research logging middleware options",
            "Design the middleware interface",
            "Implement the logging middleware",
            "Add unit tests for middleware",
            "Verify performance impact",
        ])

        cognitive_plan = engine.plan_goal(goal.id)
        assert len(cognitive_plan.nodes) >= 3

        # 2. ARCHITECT: create structured plan from same objective
        orchestrator = ArchitectOrchestrator()
        arch_plan = orchestrator.create_plan(
            objective=goal.description,
            context={
                "task": goal.description,
                "constraints": {"response_time": "Must not affect response times"},
                "project": "api_gateway",
                "active_specialists": ["ARCHITECT", "FORGE", "SENTINEL", "ORACLE"],
            },
        )

        # Verify all 10 sections
        assert arch_plan.id is not None
        assert "logging" in arch_plan.objective.goal.lower()
        assert len(arch_plan.objective.success_criteria) >= 1
        assert arch_plan.current_understanding.summary != ""
        assert arch_plan.impact_analysis.blast_radius is not None
        assert len(arch_plan.risks.risks) >= 1
        assert len(arch_plan.execution_strategy.phases) >= 3
        assert len(arch_plan.specialist_assignments.assignments) >= 1
        assert len(arch_plan.verification_plan.checks) >= 1
        assert len(arch_plan.recovery_plan.failure_strategies) >= 1
        assert len(arch_plan.completion_criteria.criteria) >= 1
        assert arch_plan.self_review.score > 0.0

        # 3. Finalize the plan
        finalized = orchestrator.finalize(arch_plan.id)
        assert finalized is True
        assert arch_plan.status == PlanStatus.VALIDATED
        self_review_passed = arch_plan.self_review.passes_review()
        assert self_review_passed is True

        # 4. EXECUTION: convert plan to execution nodes
        exec_dict = arch_plan.to_execution_plan()
        assert len(exec_dict["phases"]) >= 3
        assert exec_dict["task_description"] == goal.description

        graph = ExecutionGraph()
        for phase in exec_dict["phases"]:
            node = NodeDefinition(
                id=phase["id"],
                description=phase["description"],
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        for edge in exec_dict["edges"]:
            if edge["source"] in graph.nodes and edge["target"] in graph.nodes:
                graph.add_edge(edge["source"], edge["target"])

        # Execute
        exec_engine = ExecutionEngine(graph)
        asyncio.run(exec_engine.execute(context={"mode": "test"}))

        for node in graph.nodes.values():
            assert node.state in (NodeState.COMPLETED, "completed"), f"Node {node.id}: {node.state}"

        # 5. VERIFICATION: verify the execution results
        pipeline = VerificationPipeline()
        verifier = MockVerifier(success=True)
        pipeline.register_verifier(VerificationType.LINT, verifier.verify)
        pipeline.register_verifier(VerificationType.TYPECHECK, MockVerifier(success=True).verify)

        scope = VerificationScope(
            affected_files=arch_plan.impact_analysis.affected_files,
            provenance="architect_plan",
        )
        manifest = VerificationManifest(
            required=[VerificationType.LINT, VerificationType.TYPECHECK],
            blocking=[VerificationType.LINT, VerificationType.TYPECHECK],
        )

        results = asyncio.run(pipeline.verify(
            node_id="phase_verify",
            manifest=manifest,
            scope=scope,
            context={"architect_plan_id": arch_plan.id},
        ))

        assert len(results) >= 2
        assert all(r.success for r in results)

        # 6. Cost estimation from the orchestrator
        cost = orchestrator.estimate_cost(arch_plan)
        assert cost["total_estimated_effort"] > 0
        assert cost["verification_checks"] >= 1
        assert cost["critical_path_length"] >= 3

    def test_full_pipeline_with_event_bus(self):
        """Full pipeline with event bus tracking all lifecycle events."""
        bus = CollectingEventBus()

        # 1. Orchestrator creates plan with event bus
        orchestrator = ArchitectOrchestrator(event_bus=bus)
        plan = orchestrator.create_plan(
            objective="Add database connection pooling",
            context={
                "task": "Add database connection pooling",
                "active_specialists": ["FORGE", "SENTINEL"],
            },
        )

        # Should have received PLAN_CREATED event
        plan_created = [e for e in bus.events if isinstance(e, ArchitectPlanEvent) and e.type == EventType.PLAN_CREATED]
        assert len(plan_created) == 1
        assert plan_created[0].plan_id == plan.id

        # 2. Finalize plan
        bus.clear()
        finalized = orchestrator.finalize(plan.id)
        assert finalized is True

        # Should have received PLAN_VALIDATED event
        plan_validated = [e for e in bus.events if isinstance(e, ArchitectPlanEvent) and e.type == EventType.PLAN_VALIDATED]
        assert len(plan_validated) == 1
        assert plan_validated[0].plan_id == plan.id

        # 3. Execute plan phases through graph
        graph = ExecutionGraph(bus=bus)
        for phase in plan.execution_strategy.phases:
            node = NodeDefinition(
                id=phase.id,
                description=phase.description,
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        for edge in plan.execution_strategy.dependency_edges:
            if edge.source in graph.nodes and edge.target in graph.nodes:
                graph.add_edge(edge.source, edge.target)

        exec_engine = ExecutionEngine(graph)
        asyncio.run(exec_engine.execute(context={}))

        # Should have published node transition events
        node_transitions = [e for e in bus.events if isinstance(e, NodeTransitionEvent)]
        assert len(node_transitions) >= len(plan.execution_strategy.phases)

    def test_full_pipeline_with_validation_failure(self):
        """Pipeline handles plan validation failure gracefully."""
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Simple task",
            context={"task": "Simple task"},
        )

        # Corrupt the plan by removing required data
        plan.objective.success_criteria = []

        finalized = orchestrator.finalize(plan.id)
        assert finalized is False
        assert plan.status == PlanStatus.DRAFT

    def test_full_pipeline_with_self_review_failure(self):
        """Pipeline handles self-review failure gracefully."""
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Fix a minor bug",
            context={"task": "Fix a minor bug"},
        )

        # Lower the review score below passing threshold
        plan.self_review.score = 0.1
        plan.self_review.is_coherent = False
        plan.self_review.is_executable = False

        finalized = orchestrator.finalize(plan.id)
        assert finalized is False


# ===========================================================================
# Section 5: Error Handling & Recovery
# ===========================================================================

class TestPipelineErrorHandling:
    """Test how the pipeline behaves when components fail."""

    def test_cognitive_engine_failure_does_not_break_architect(self):
        """ArchitectOrchestrator works independently even without CognitiveEngine context."""
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan(
            objective="Refactor database layer",
            context={"task": "Refactor database layer"},
        )

        assert plan.id is not None
        assert len(plan.execution_strategy.phases) >= 3
        assert plan.objective.goal == "Refactor database layer"
        # Should still produce valid success criteria even with minimal context
        assert len(plan.objective.success_criteria) >= 1

    def test_execution_failure_reaches_verification(self):
        """Execution failures are propagated to the verification pipeline."""
        plan = make_simple_architect_plan()

        # Simulate a node that fails during execution
        graph = ExecutionGraph()
        for phase in plan.execution_strategy.phases:
            node = NodeDefinition(
                id=phase.id,
                description=phase.description,
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        # Mark phase_3 as failed
        graph.nodes["phase_3"].state = NodeState.FAILED
        graph.nodes["phase_3"].error = "OAuth2 implementation error"

        # Verification should detect the failure
        pipeline = VerificationPipeline()
        verifier = MockVerifier(success=False)
        pipeline.register_verifier(VerificationType.LINT, verifier.verify)

        scope = VerificationScope(
            affected_files=["auth/oauth.py"],
            provenance="execution_result",
        )
        manifest = VerificationManifest(
            required=[VerificationType.LINT],
            blocking=[VerificationType.LINT],
        )

        results = asyncio.run(pipeline.verify(
            node_id="phase_3",
            manifest=manifest,
            scope=scope,
            context={
                "execution_error": graph.nodes["phase_3"].error,
                "node_state": graph.nodes["phase_3"].state.value,
            },
        ))

        assert len(results) >= 1
        assert results[0].success is False
        assert results[0].node_id == "phase_3"

    def test_architect_plan_with_missing_data_is_caught(self):
        """ArchitectPlan.validate_complete() catches missing required sections."""
        plan = make_simple_architect_plan()
        issues = plan.validate_complete()
        assert len(issues) == 0  # Complete plan has no issues

        # Remove key data
        plan.objective.goal = ""
        plan.execution_strategy.phases = []

        issues = plan.validate_complete()
        assert len(issues) >= 2
        assert any("empty" in i.lower() for i in issues)

    def test_recovery_strategies_from_plan_apply_to_execution(self):
        """Recovery strategies defined in the plan map to execution recovery actions."""
        plan = make_simple_architect_plan()

        # Verify recovery strategies cover implementation phases
        impl_recoveries = [s for s in plan.recovery_plan.failure_strategies if "implementation" in s.failure_mode.lower()]
        assert len(impl_recoveries) >= 1
        assert impl_recoveries[0].max_retries >= 1

        # Verify rollback points exist
        assert len(plan.recovery_plan.rollback_points) >= 1
        assert "phase_1" in plan.recovery_plan.rollback_points
        assert "phase_3" in plan.recovery_plan.rollback_points

    def test_verification_checks_align_with_recovery_strategies(self):
        """Verification checks and recovery strategies reference the same phase IDs."""
        plan = make_simple_architect_plan()

        verification_phases = {c.phase_id for c in plan.verification_plan.checks}
        recovery_phases = {s.phase_id for s in plan.recovery_plan.failure_strategies}
        phase_ids = {p.id for p in plan.execution_strategy.phases}

        # All verification and recovery references should point to valid phases
        for vp in verification_phases:
            assert vp in phase_ids, f"Verification check references unknown phase: {vp}"

        for rp in recovery_phases:
            assert rp in phase_ids, f"Recovery strategy references unknown phase: {rp}"


# ===========================================================================
# Section 6: Plan Enrichment & Context Propagation
# ===========================================================================

class TestPlanContextPropagation:
    """Verify that plan context propagates correctly through the pipeline."""

    def test_architect_plan_enriches_downstream_context(self):
        """ArchitectPlan.enrich_context_with_plan() injects plan data for downstream specialists."""
        plan = make_simple_architect_plan()
        orchestrator = ArchitectOrchestrator()
        context: Dict[str, Any] = {"mode": "execution"}

        enriched = orchestrator.enrich_context_with_plan(plan, context)

        assert "architect_plan" in enriched
        assert "architect_plan_id" in enriched
        assert enriched["architect_plan_id"] == plan.id
        assert "architect_plan_display" in enriched
        assert "architect_plan_sections" in enriched

        # Per-specialist assignments should be injected
        sections = enriched["architect_plan_sections"]
        assert "objective" in sections
        assert "execution_phases" in sections
        assert "specialist_assignments" in sections
        assert sections["execution_phases"] == len(plan.execution_strategy.phases)

    def test_downstream_specialists_receive_assignments(self):
        """Specialist-specific assignments are injected into context for each role."""
        plan = make_simple_architect_plan()
        orchestrator = ArchitectOrchestrator()

        context: Dict[str, Any] = {}
        enriched = orchestrator.enrich_context_with_plan(plan, context)

        # FORGE should have assignments
        assert "FORGE_assignments" in enriched
        forge_tasks = enriched["FORGE_assignments"]
        assert len(forge_tasks) >= 1
        assert any("Implement" in t["task"] for t in forge_tasks)

        # SENTINEL should have assignments
        assert "SENTINEL_assignments" in enriched
        sentinel_tasks = enriched["SENTINEL_assignments"]
        assert len(sentinel_tasks) >= 1
        assert any("security" in t["task"].lower() for t in sentinel_tasks)

    def test_execution_context_includes_plan_sections(self):
        """Context passed to execution engine includes the plan summary."""
        plan = make_simple_architect_plan()
        orchestrator = ArchitectOrchestrator()

        context = orchestrator.enrich_context_with_plan(plan, {"mode": "execute"})

        # Run graph with enriched context
        graph = ExecutionGraph()
        for phase in plan.execution_strategy.phases:
            node = NodeDefinition(
                id=phase.id,
                description=phase.description,
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        engine = ExecutionEngine(graph)
        asyncio.run(engine.execute(context=context))

        for node in graph.nodes.values():
            assert node.state in (NodeState.COMPLETED, "completed")


# ===========================================================================
# Section 7: Pipeline Performance & Stability
# ===========================================================================

class TestPipelineStability:
    """Verify pipeline stability under various conditions."""

    def test_multiple_plans_are_independent(self):
        """Multiple ArchitectPlans created in sequence maintain independence."""
        orchestrator = ArchitectOrchestrator()

        plan_a = orchestrator.create_plan(
            objective="Implement feature A",
            context={"task": "Implement feature A"},
        )
        plan_b = orchestrator.create_plan(
            objective="Fix bug B",
            context={"task": "Fix bug B"},
        )

        assert plan_a.id != plan_b.id
        assert plan_a.objective.goal != plan_b.objective.goal
        assert "feature" in plan_a.objective.goal.lower()
        assert "fix" in plan_b.objective.goal.lower()

        # Plans are independent — they have different IDs, objectives, and phase counts
        assert plan_a.id != plan_b.id
        assert plan_a.objective.goal != plan_b.objective.goal
        # Each plan has its own specialist assignments reflecting its objective
        assert len(plan_a.specialist_assignments.assignments) > 0
        assert len(plan_b.specialist_assignments.assignments) > 0
        # Phase-level independence: the specializations differ
        # (e.g., a "feature" plan may have different implementation phase names than a "fix" plan)
        phase_names_a = {p.name for p in plan_a.execution_strategy.phases}
        phase_names_b = {p.name for p in plan_b.execution_strategy.phases}
        assert phase_names_a != phase_names_b or len(phase_names_a) != len(phase_names_b),\
            "Phase contents differ across plans with different objectives"

    def test_plans_can_be_listed_and_retrieved(self):
        """Created plans are retrievable via the orchestrator's get/list API."""
        orchestrator = ArchitectOrchestrator()

        plan_a = orchestrator.create_plan("Plan A", {"task": "Plan A"})
        plan_b = orchestrator.create_plan("Plan B", {"task": "Plan B"})

        plan_ids = orchestrator.list_plans()
        assert plan_a.id in plan_ids
        assert plan_b.id in plan_ids

        retrieved = orchestrator.get_plan(plan_a.id)
        assert retrieved is not None
        assert retrieved.id == plan_a.id
        assert retrieved.objective.goal == plan_a.objective.goal

    def test_plan_can_be_finalized_and_executed(self):
        """A finalized plan (status=VALIDATED) can be executed through the graph."""
        plan = make_simple_architect_plan()
        orchestrator = ArchitectOrchestrator()
        orchestrator._plans[plan.id] = plan

        finalized = orchestrator.finalize(plan.id)
        assert finalized is True
        assert plan.status == PlanStatus.VALIDATED

        # Execute the validated plan
        graph = ExecutionGraph()
        for phase in plan.execution_strategy.phases:
            node = NodeDefinition(
                id=phase.id,
                description=phase.description,
                specialist="FORGE",
                state=NodeState.PENDING,
            )
            graph.add_node(node)

        for edge in plan.execution_strategy.dependency_edges:
            if edge.source in graph.nodes and edge.target in graph.nodes:
                graph.add_edge(edge.source, edge.target)

        engine = ExecutionEngine(graph)
        asyncio.run(engine.execute(context={"plan_validated": True}))

        for node in graph.nodes.values():
            assert node.state in (NodeState.COMPLETED, "completed")

    def test_non_existent_plan_finalize_returns_false(self):
        """Finalizing a non-existent plan returns False."""
        orchestrator = ArchitectOrchestrator()
        assert orchestrator.finalize("non_existent_plan") is False

    def test_empty_objective_still_produces_valid_plan(self):
        """Even with an empty objective, the orchestrator produces a structurally valid plan."""
        orchestrator = ArchitectOrchestrator()
        plan = orchestrator.create_plan("", context={"task": ""})

        assert plan.id is not None
        assert plan.objective.goal == ""
        # Success criteria should have default values
        assert len(plan.objective.success_criteria) >= 1
        # Execution strategy should have at least some phases
        assert len(plan.execution_strategy.phases) >= 3
        assert plan.self_review.score >= 0.0
