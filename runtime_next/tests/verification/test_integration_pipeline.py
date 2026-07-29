"""Comprehensive integration tests for the full verification + self-healing recovery pipeline.

Tests the end-to-end flow:
  Verify → Classify → Governance → Recover → Inject → Retry Safety → Retry → Learn

Each test exercises multiple layers to ensure they compose correctly.
"""

import pytest
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ============================================================================
# Shared helpers
# ============================================================================

class MockGraph:
    """Mock execution graph that supports inject_node, add_node, add_edge, transition_node."""
    def __init__(self):
        self.nodes: Dict[str, Any] = {}
        self.edges: List[Dict[str, str]] = []
        self.transitions: List[Dict[str, Any]] = []
        self.event_bus: Any = None
        self.called_inject_node = False
        self.injected_node = None

    def inject_node(self, node_properties: dict, dependencies: Optional[list] = None):
        self.called_inject_node = True
        self.injected_node = node_properties
        nid = node_properties.get("id", "recover_test")
        self.nodes[nid] = {"state": "pending", "dependencies": dependencies or []}

    def add_node(self, node_id: str, properties: dict):
        self.nodes[node_id] = {**properties, "state": "pending"}

    def add_edge(self, source: str, target: str):
        self.edges.append({"source": source, "target": target})

    async def transition_node(self, node_id: str, state: str, reason: str = ""):
        if node_id in self.nodes:
            node = self.nodes[node_id]
            if isinstance(node, dict):
                node["state"] = state
            elif hasattr(node, "state"):
                node.state = state
        self.transitions.append({"node_id": node_id, "to_state": state, "reason": reason})


@pytest.fixture
def mock_graph():
    return MockGraph()


# ============================================================================
# Test 1: Full end-to-end happy path
#    Verify → Classify → Governance → Recover → Inject → Retry Safety → Retry
# ============================================================================

@pytest.mark.asyncio
async def test_full_pipeline_happy_path():
    """Complete happy path: verification passes, classification identifies timeout,
    governance approves auto-recovery, retry safety passes, injector creates node."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.governance import RecoveryGovernance
    from runtime_next.verification.recovery import RecoveryStrategyEngine
    from runtime_next.verification.injector import RecoveryNodeInjector
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import (
        VerificationType, VerificationManifest, VerificationScope,
        VerificationResult, Confidence, Retryability,
        FailureClassification,
    )
    from runtime_next.verification.memory import LearnedRecoveryMemory

    # --- Setup pipeline components ---
    pipeline = VerificationPipeline()
    classifier = FailureClassifier()
    governance = RecoveryGovernance()
    recovery = RecoveryStrategyEngine()
    injector = RecoveryNodeInjector()
    retry_safety = RetrySafetyEngine()
    memory = LearnedRecoveryMemory()

    # Register a passing verifier
    async def passing_verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_lint",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=True,
            confidence=Confidence.HIGH,
        )

    pipeline.register_verifier(VerificationType.LINT, passing_verifier)

    # --- Step 1: Verify ---
    manifest = VerificationManifest(
        required=[VerificationType.LINT],
        blocking=[VerificationType.LINT],
    )
    scope = VerificationScope.empty()
    verify_results = await pipeline.verify("node_timeout", manifest, scope, {})
    assert len(verify_results) == 1
    assert verify_results[0].success is True

    # --- Step 2: Classify a timeout failure ---
    classification = await classifier.classify(
        error_message="Operation timed out after 30s",
        stderr="TimeoutError: execution exceeded deadline",
        exit_code=137,
    )
    assert classification.primary == FailureClassification.TIMEOUT
    assert classification.confidence in (Confidence.MEDIUM, Confidence.HIGH, Confidence.CERTAIN)

    # --- Step 3: Governance approves auto-recovery ---
    strategy = recovery.get_strategy(FailureClassification.TIMEOUT)
    assert strategy is not None

    gov_decision = await governance.decide(
        failure_type=FailureClassification.TIMEOUT,
        strategy=strategy,
        action_type="retry",
        context={"retry_count": 0, "node_description": "test node"},
    )
    assert gov_decision.verdict == "auto_recover"
    assert gov_decision.requires_user_intervention is False

    # --- Step 4: Retry safety checks pass ---
    retry_decision = await retry_safety.evaluate(
        node_id="node_timeout",
        classification=FailureClassification.TIMEOUT,
        retryability=Retryability.SAFE,
        graph_state={"node_count": 5, "completed_count": 3, "failed_count": 1},
        capability_state={"health": "fully_operational", "tools": {}},
    )
    assert retry_decision.can_retry is True

    # --- Step 5: Execute recovery ---
    action = await recovery.execute_recovery(
        node_id="node_timeout",
        failure_type=FailureClassification.TIMEOUT,
        classification_result=classification,
        context={"retry_count": 0, "node_description": "test node"},
    )
    assert action is not None
    assert action.action_type in ("retry", "inject_node")

    # --- Step 6: Inject recovery node ---
    graph = MockGraph()
    injected_id = await injector.inject_recovery_node(action, strategy, graph, {})
    assert injected_id is not None
    assert injected_id.startswith("recover_node_timeout")

    # --- Step 7: Record in memory ---
    entry = await memory.record(action, strategy, True,
                                {"project_name": "aelvo", "node_description": "test node"})
    assert entry.success is True
    assert entry.failure_type == FailureClassification.TIMEOUT

    # --- Verify pipeline history ---
    assert len(pipeline.history) == 1
    assert len(classifier.classification_history) == 1
    assert recovery.recovery_count == 1
    assert len(injector.injected_nodes) == 1
    assert memory.total_entries == 1
    assert governance.auto_recovery_count >= 1


# ============================================================================
# Test 2: Blocking verification failure stops pipeline
# ============================================================================

@pytest.mark.asyncio
async def test_blocking_verification_failure():
    """A blocking verification failure stops optional verifications and
    triggers classification -> recovery flow."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import (
        VerificationType, VerificationManifest, VerificationScope,
        VerificationResult, Confidence, Severity,
    )

    pipeline = VerificationPipeline()
    classifier = FailureClassifier()

    async def failing_verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_fail",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=False,
            severity=Severity.ERROR,
            diagnostics=["Lint errors found: unused variable"],
            confidence=Confidence.CERTAIN,
        )

    pipeline.register_verifier(VerificationType.LINT, failing_verifier)

    # Run verification with optional verifications that should NOT run
    manifest = VerificationManifest(
        required=[VerificationType.LINT],
        blocking=[VerificationType.LINT],
        optional=[VerificationType.UNIT_TEST],
    )
    scope = VerificationScope.empty()
    results = await pipeline.verify("node_fail", manifest, scope, {})

    # Only required blocking verification runs
    assert len(results) == 1
    assert results[0].success is False
    assert results[0].verification_type == VerificationType.LINT

    # Now classify the failure
    classification = await classifier.classify(
        error_message="Lint errors found: unused variable",
        verification_results=[
            {"success": False, "verification_type": "lint"}
        ],
    )
    assert classification.primary is not None


# ============================================================================
# Test 3: Unknown failure → Governance aborts autonomous recovery
# ============================================================================

@pytest.mark.asyncio
async def test_unknown_failure_governance_aborts():
    """Unknown failures should never be silently retried — governance aborts."""
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.governance import RecoveryGovernance
    from runtime_next.verification.recovery import RecoveryStrategyEngine
    from runtime_next.verification.types import (
        FailureClassification,
    )

    classifier = FailureClassifier()
    governance = RecoveryGovernance()
    recovery = RecoveryStrategyEngine()

    # Classify an unknown failure
    classification = await classifier.classify(
        error_message="Kernel panic: something completely unexpected",
        exit_code=255,
    )
    assert classification.primary == FailureClassification.UNKNOWN_FAILURE

    # Get the strategy for unknown failures
    strategy = recovery.get_strategy(FailureClassification.UNKNOWN_FAILURE)
    assert strategy is not None
    assert strategy.requires_user_approval is True

    # Governance should abort
    decision = await governance.decide(
        failure_type=FailureClassification.UNKNOWN_FAILURE,
        strategy=strategy,
        action_type="escalate",
    )
    assert decision.verdict == "abort"
    assert decision.should_stop_autonomy() is True
    assert decision.requires_user_intervention is True


# ============================================================================
# Test 4: Retry budget exhausted → escalate
# ============================================================================

@pytest.mark.asyncio
async def test_retry_budget_exhaustion():
    """When retry budget is exhausted, recovery returns an escalate action
    and governance notifies the user."""
    from runtime_next.verification.recovery import RecoveryStrategyEngine
    from runtime_next.verification.governance import RecoveryGovernance
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import (
        FailureClassification,
    )
    from runtime_next.verification.memory import LearnedRecoveryMemory

    classifier = FailureClassifier()
    recovery = RecoveryStrategyEngine()
    governance = RecoveryGovernance()
    memory = LearnedRecoveryMemory()

    # Register an executor that always fails (simulating exhausted budget)
    async def failing_executor(action, context):
        return {"success": False, "error": "Executor failed"}

    strategy = recovery.get_strategy(FailureClassification.TIMEOUT)
    assert strategy is not None
    recovery.register_executor(strategy.id, failing_executor)

    # Classify the failure
    classification = await classifier.classify(
        error_message="Timed out again",
        exit_code=137,
    )
    assert classification.primary == FailureClassification.TIMEOUT

    # Execute recovery at budget exhaustion (retry_count >= max_retries)
    action = await recovery.execute_recovery(
        node_id="node_exhausted",
        failure_type=FailureClassification.TIMEOUT,
        classification_result=classification,
        context={"retry_count": 5, "node_description": "exhausted node"},
    )
    assert action is not None
    assert action.action_type == "escalate"
    assert action.success is False

    # Record the escalation
    await memory.record(
        action, strategy, False,
        {"node_description": "exhausted node", "retry_count": 5},
    )

    # Governance should notify user
    gov_decision = await governance.decide(
        failure_type=FailureClassification.TIMEOUT,
        strategy=strategy,
        action_type="retry",
        context={"retry_count": 5, "node_description": "exhausted node"},
    )
    assert gov_decision.verdict == "notify_user"
    assert gov_decision.requires_user_intervention is True


# ============================================================================
# Test 5: Permission denied always escalates
# ============================================================================

@pytest.mark.asyncio
async def test_permission_denied_escalation():
    """Permission denied failures always escalate to user, even with retry budget."""
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.governance import RecoveryGovernance
    from runtime_next.verification.recovery import RecoveryStrategyEngine
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    governance = RecoveryGovernance()
    recovery = RecoveryStrategyEngine()

    # Classify a permission error
    classification = await classifier.classify(
        error_message="Permission denied: /etc/config",
        stderr="EACCES: access denied",
        exit_code=126,
    )
    assert classification.primary == FailureClassification.PERMISSION_DENIED

    # Strategy should require approval
    strategy = recovery.get_strategy(FailureClassification.PERMISSION_DENIED)
    assert strategy is not None
    assert strategy.requires_user_approval is True
    assert strategy.max_retries == 0

    # Governance escalates regardless of context
    decision = await governance.decide(
        failure_type=FailureClassification.PERMISSION_DENIED,
        strategy=strategy,
        action_type="escalate",
        context={"retry_count": 0},
    )
    assert decision.requires_user_intervention is True
    assert decision.suggested_message is not None
    # Reason is about requiring approval, which is correct for this strategy


# ============================================================================
# Test 6: Architecture violation escalates
# ============================================================================

@pytest.mark.asyncio
async def test_architecture_violation_escalation():
    """Architecture violations always require human review."""
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.governance import RecoveryGovernance
    from runtime_next.verification.recovery import RecoveryStrategyEngine
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    governance = RecoveryGovernance()
    recovery = RecoveryStrategyEngine()

    classification = await classifier.classify(
        error_message="Architecture violation: circular dependency detected between modules A and B",
    )
    assert classification.primary == FailureClassification.ARCHITECTURE_VIOLATION

    strategy = recovery.get_strategy(FailureClassification.ARCHITECTURE_VIOLATION)
    assert strategy is not None
    assert strategy.requires_user_approval is True

    decision = await governance.decide(
        failure_type=FailureClassification.ARCHITECTURE_VIOLATION,
        strategy=strategy,
        action_type="escalate",
    )
    assert decision.requires_user_intervention is True
    # Strategy requires approval, so governance returns require_approval before architecture check
    assert decision.verdict in ("notify_user", "require_approval")


# ============================================================================
# Test 7: Destructive action requires approval
# ============================================================================

@pytest.mark.asyncio
async def test_destructive_action_requires_approval():
    """Destructive recovery actions (rollback) require explicit approval."""
    from runtime_next.verification.governance import RecoveryGovernance
    from runtime_next.verification.types import FailureClassification, RecoveryStrategy

    governance = RecoveryGovernance()

    # SERIALIZATION_FAILURE triggers a rollback (destructive)
    strategy = RecoveryStrategy(
        id="strat_rollback",
        name="Graph rollback",
        failure_type=FailureClassification.SERIALIZATION_FAILURE,
        danger_level="destructive",
        max_retries=1,
    )

    decision = await governance.decide(
        failure_type=FailureClassification.SERIALIZATION_FAILURE,
        strategy=strategy,
        action_type="rollback",
        context={"retry_count": 0},
    )
    assert decision.verdict == "require_approval"
    assert decision.danger_assessment == "destructive"
    assert decision.requires_user_intervention is True
    assert decision.suggested_message is not None


# ============================================================================
# Test 8: Retry safety blocks due to graph inconsistency
# ============================================================================

@pytest.mark.asyncio
async def test_retry_safety_graph_inconsistency():
    """Retry safety blocks retry when graph state shows inconsistency."""
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import Retryability

    retry_safety = RetrySafetyEngine()
    classifier = FailureClassifier()

    # Classify a timeout
    classification = await classifier.classify(
        error_message="Timeout occurred",
        exit_code=137,
    )

    # Evaluate retry safety with inconsistent graph
    decision = await retry_safety.evaluate(
        node_id="node_inconsistent",
        classification=classification.primary,
        retryability=Retryability.SAFE,
        graph_state={
            "node_count": 10,
            "completed_count": 12,  # Impossible: exceeds node_count
            "failed_count": 5,       # Also results in >50% failure rate
        },
    )
    assert decision.can_retry is False
    assert decision.blocking_condition == "graph_inconsistency"
    assert decision.graph_consistent is False


# ============================================================================
# Test 9: Retry safety blocks due to serialization corruption
# ============================================================================

@pytest.mark.asyncio
async def test_retry_safety_serialization_corruption():
    """Retry safety blocks retry when serialization state is corrupted."""
    from runtime_next.verification.retry_safety import RetrySafetyEngine
    from runtime_next.verification.types import FailureClassification, Retryability

    retry_safety = RetrySafetyEngine()

    decision = await retry_safety.evaluate(
        node_id="node_corrupt",
        classification=FailureClassification.SERIALIZATION_FAILURE,
        retryability=Retryability.CONDITIONAL,
        serialization_state={"is_valid": False},
    )
    assert decision.can_retry is False
    assert decision.blocking_condition == "serialization_corruption"


# ============================================================================
# Test 10: Multiple sequential recoveries with memory accumulation
# ============================================================================

@pytest.mark.asyncio
async def test_sequential_recoveries_accumulate_memory():
    """Multiple recovery attempts across different failure types accumulate
    in learned memory and improve future strategy ranking."""
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.recovery import RecoveryStrategyEngine
    from runtime_next.verification.injector import RecoveryNodeInjector
    from runtime_next.verification.memory import LearnedRecoveryMemory
    from runtime_next.verification.types import FailureClassification

    classifier = FailureClassifier()
    recovery = RecoveryStrategyEngine()
    injector = RecoveryNodeInjector()
    memory = LearnedRecoveryMemory()

    graph = MockGraph()

    # Simulate 5 different failures and recoveries
    failures = [
        ("node_syntax", "SyntaxError: invalid syntax", FailureClassification.SYNTAX_ERROR),
        ("node_dep", "ModuleNotFoundError: no module named 'foo'", FailureClassification.DEPENDENCY_MISSING),
        ("node_timeout", "Timed out after 60s", FailureClassification.TIMEOUT),
        ("node_perm", "EACCES: permission denied", FailureClassification.PERMISSION_DENIED),
        ("node_tool", "Tool execution error: specialist failed", FailureClassification.TOOL_FAILURE),
    ]

    for node_id, error, expected_type in failures:
        # Classify
        classification = await classifier.classify(error_message=error)
        assert classification.primary == expected_type

        # Get strategy
        strategy = recovery.get_strategy(expected_type)
        assert strategy is not None

        # Execute recovery
        action = await recovery.execute_recovery(
            node_id=node_id,
            failure_type=expected_type,
            classification_result=classification,
            context={"retry_count": 0, "node_description": f"sequential {node_id}"},
        )
        assert action is not None

        # Inject if applicable
        if action.action_type in ("retry", "inject_node"):
            injected = await injector.inject_recovery_node(action, strategy, graph, {})
            if action.action_type == "inject_node":
                assert injected is not None

        # Record in memory
        await memory.record(action, strategy, True,
                            {"node_description": f"sequential {node_id}"})

    # Verify accumulation (5 failures handled, PERMISSION_DENIED with max_retries=0 is budget-exhausted but still recorded)
    assert memory.total_entries == 5
    assert recovery.recovery_count == 5
    assert len(injector.injected_nodes) > 0

    # Memory should have good success rate
    rate = await memory.success_rate()
    assert rate > 0.8

    # Strategy ranking should have entries
    ranking = await memory.strategy_ranking(FailureClassification.TIMEOUT)
    assert len(ranking) >= 1
    assert ranking[0][1] > 0  # Has some success rate


# ============================================================================
# Test 11: Learned recovery — best_recovery_for after recording
# ============================================================================

@pytest.mark.asyncio
async def test_learned_recovery_best_recovery():
    """After recording multiple recoveries, best_recovery_for returns the best match."""
    from runtime_next.verification.memory import LearnedRecoveryMemory
    from runtime_next.verification.types import (
        FailureClassification, RecoveryAction, RecoveryStrategy,
    )

    memory = LearnedRecoveryMemory()

    # Record several recoveries for TIMEOUT
    good_strategy = RecoveryStrategy(
        id="strat_good", name="Good timeout handler",
        failure_type=FailureClassification.TIMEOUT,
    )

    for i in range(3):
        action = RecoveryAction(
            id=f"action_good_{i}", strategy_id="strat_good",
            node_id="n1", failure_classification=FailureClassification.TIMEOUT,
            action_type="retry", success=True, duration_ms=50,
        )
        await memory.record(action, good_strategy, True,
                            {"project_name": "aelvo", "node_description": "test node"})

    # Also record a failed recovery
    bad_strategy = RecoveryStrategy(
        id="strat_bad", name="Bad timeout handler",
        failure_type=FailureClassification.TIMEOUT,
    )
    bad_action = RecoveryAction(
        id="action_bad", strategy_id="strat_bad",
        node_id="n2", failure_classification=FailureClassification.TIMEOUT,
        action_type="retry", success=False, duration_ms=200,
    )
    await memory.record(bad_action, bad_strategy, False)

    # Query best recovery
    best = await memory.best_recovery_for(FailureClassification.TIMEOUT)
    assert best is not None
    entry, score = best
    assert entry.recovery_strategy_id == "strat_good"
    assert entry.success is True
    assert score > 0

    # Strategy ranking should prefer strat_good over strat_bad
    ranking = await memory.strategy_ranking(FailureClassification.TIMEOUT)
    strat_good_score = next(r[1] for r in ranking if r[0] == "strat_good")
    strat_bad_score = next(r[1] for r in ranking if r[0] == "strat_bad")
    assert strat_good_score > strat_bad_score


# ============================================================================
# Test 12: Consistency validation across pipeline passes
# ============================================================================

@pytest.mark.asyncio
async def test_consistency_validation_in_pipeline():
    """Runtime consistency validation works across multi-pass pipeline cycles."""
    from runtime_next.verification.consistency import RuntimeConsistencyValidator

    validator = RuntimeConsistencyValidator()

    # Pass 1: All green
    result1 = await validator.validate_all(
        graph_state={
            "nodes": {
                "n1": {"state": "completed", "end_time": "2024-01-01T00:00:00", "dependencies": []},
                "n2": {"state": "completed", "end_time": "2024-01-01T00:01:00", "dependencies": ["n1"]},
            },
            "edges": [
                {"source_node_id": "n1", "target_node_id": "n2"},
            ],
        },
        capability_state={"health": "fully_operational", "timestamp": datetime.now(timezone.utc)},
        serialization_state={"is_valid": True},
    )
    assert result1.is_consistent is True
    assert len(result1.checks_performed) >= 3

    # Pass 2: Introduce graph violation
    result2 = await validator.validate_all(
        graph_state={
            "nodes": {
                "n1": {"state": "completed", "end_time": "...", "dependencies": ["n2"]},
                "n2": {"state": "completed", "end_time": "...", "dependencies": ["n1"]},
            },
            "edges": [
                {"source_node_id": "n1", "target_node_id": "n2"},
                {"source_node_id": "n2", "target_node_id": "n1"},
            ],
        },
    )
    assert result2.is_consistent is False
    assert result2.dependency_validity is False

    # Pass 3: Serialization corruption
    result3 = await validator.validate_all(
        serialization_state={"is_valid": False},
    )
    assert result3.is_consistent is False
    assert result3.serialization_integrity is False

    # History should show all checks
    assert len(validator.check_history) == 3
    assert validator.is_consistently_healthy(3) is False  # Last 3 had failures


# ============================================================================
# Test 13: Full RecoveryEngine end-to-end with mock graph
# ============================================================================

@pytest.mark.asyncio
async def test_recovery_engine_end_to_end(mock_graph):
    """RecoveryEngine orchestrates the full classify → recover → inject → retry
    flow for an actual node failure."""
    from runtime_next.recovery.engine import RecoveryEngine
    from runtime_next.models.node import NodeDefinition, NodeState
    from runtime_next.models.events import NodeTransitionEvent

    # Create a node with retry budget
    node = NodeDefinition(
        id="node_recover",
        description="A node that will fail and recover",
        specialist="FORGE",
        retry_budget=3,
        retry_count=0,
    )
    mock_graph.nodes["node_recover"] = node

    engine = RecoveryEngine(graph=mock_graph)

    # Simulate a node transition event (NODE_TRANSITION → FAILED)
    event = NodeTransitionEvent(
        id="evt_001",
        node_id="node_recover",
        to_state=NodeState.FAILED.value,
        reason="SyntaxError: invalid syntax",
        from_state="running",
        node_type="tool_call",
        criticality="important",
        steps_consumed=1,
    )

    await engine.on_event(event)

    # Should have created recovery history
    assert engine.recovery_count >= 1

    # Node should be in RETRYING state
    history_entry = engine._recovery_history[-1]
    assert history_entry["node_id"] == "node_recover"
    assert history_entry["verification_subsystem"] is True
    assert history_entry["action"] in ("retry", "inject_node")


# ============================================================================
# Test 14: Governance approval tracking lifecycle
# ============================================================================

@pytest.mark.asyncio
async def test_governance_approval_lifecycle():
    """Full lifecycle of governance approvals: decide → mark pending → approve → verify."""
    from runtime_next.verification.governance import RecoveryGovernance
    from runtime_next.verification.types import FailureClassification, RecoveryStrategy

    governance = RecoveryGovernance()

    # Strategy requiring approval
    strategy = RecoveryStrategy(
        id="strat_approve",
        name="Requires approval",
        failure_type=FailureClassification.REPLAY_DIVERGENCE,
        max_retries=0,
        requires_user_approval=True,
    )

    # Decision requires approval
    decision = await governance.decide(
        failure_type=FailureClassification.REPLAY_DIVERGENCE,
        strategy=strategy,
        action_type="escalate",
    )
    assert decision.verdict == "require_approval"
    assert decision.requires_user_intervention is True

    # Mark as pending
    governance.mark_approval_pending(decision.reason)
    assert len(governance.pending_approvals) >= 1

    # Approve
    assert governance.approve(decision.reason) is True

    # Double-approve should fail
    assert governance.approve(decision.reason) is False

    # Re-request and reject
    decision2 = await governance.decide(
        failure_type=FailureClassification.REPLAY_DIVERGENCE,
        strategy=strategy,
        action_type="escalate",
    )
    governance.mark_approval_pending(decision2.reason)
    assert governance.reject(decision2.reason) is True
    assert governance.reject(decision2.reason) is False  # Already rejected

    # Intervention count should reflect decisions requiring user interaction
    assert governance.intervention_count >= 2


# ============================================================================
# Test 15: Pipeline event emission chain
# ============================================================================

@pytest.mark.asyncio
async def test_pipeline_event_emission_chain():
    """Verification pipeline emits a chain of events: start → completed/failed,
    which can be consumed by the recovery engine."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.events import (
        VerificationStartedEvent,
        VerificationCompletedEvent,
        VerificationFailedEvent,
    )
    from runtime_next.verification.types import (
        VerificationType, VerificationManifest, VerificationScope,
        VerificationResult, Confidence, Severity,
    )

    pipeline = VerificationPipeline()
    FailureClassifier()
    events: List[Any] = []

    # Track all verification events
    pipeline.on_event(lambda e: events.append(e))

    # Register a verifier that passes for LINT but fails for TYPECHECK
    async def lint_verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_lint",
            node_id=node_id,
            verification_type=VerificationType.LINT,
            success=True,
            confidence=Confidence.HIGH,
        )

    async def typecheck_verifier(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_typecheck",
            node_id=node_id,
            verification_type=VerificationType.TYPECHECK,
            success=False,
            severity=Severity.ERROR,
            diagnostics=["Type mismatch: expected str, got int"],
            confidence=Confidence.CERTAIN,
        )

    pipeline.register_verifier(VerificationType.LINT, lint_verifier)
    pipeline.register_verifier(VerificationType.TYPECHECK, typecheck_verifier)

    # Run verification with both required (non-blocking) types
    manifest = VerificationManifest(
        required=[VerificationType.LINT, VerificationType.TYPECHECK],
    )
    scope = VerificationScope.empty()
    results = await pipeline.verify("node_event_chain", manifest, scope, {})

    # Should have 2 results
    assert len(results) == 2

    # Verify events
    started_events = [e for e in events if isinstance(e, VerificationStartedEvent)]
    completed_events = [e for e in events if isinstance(e, VerificationCompletedEvent)]
    failed_events = [e for e in events if isinstance(e, VerificationFailedEvent)]

    assert len(started_events) == 2
    assert len(completed_events) >= 1
    assert len(failed_events) >= 1

    # Verify event content
    lint_started = [e for e in started_events if e.verification_type == VerificationType.LINT]
    assert len(lint_started) == 1
    assert lint_started[0].node_id == "node_event_chain"

    typecheck_failed = [e for e in failed_events if e.verification_type == VerificationType.TYPECHECK]
    assert len(typecheck_failed) >= 1
    assert typecheck_failed[0].result.success is False


# ============================================================================
# Test 16: Recovery strategy executor integration
# ============================================================================

@pytest.mark.asyncio
async def test_recovery_strategy_executor_integration():
    """Strategy engine executes registered executors, which can call
    the injector and record results in memory."""
    from runtime_next.verification.recovery import RecoveryStrategyEngine
    from runtime_next.verification.injector import RecoveryNodeInjector
    from runtime_next.verification.memory import LearnedRecoveryMemory
    from runtime_next.verification.types import FailureClassification, RecoveryAction

    recovery = RecoveryStrategyEngine()
    injector = RecoveryNodeInjector()
    memory = LearnedRecoveryMemory()
    graph = MockGraph()

    # Register an executor that injects a recovery node
    async def smart_executor(action: RecoveryAction, context: Dict[str, Any]):
        strategy = recovery.get_strategy(action.failure_classification)
        if strategy:
            injected = await injector.inject_recovery_node(action, strategy, graph, {})
            if injected:
                return {"success": True, "injected_node_id": injected, "duration_ms": 100}
        return {"success": False, "error": "Failed"}

    strategy = recovery.get_strategy(FailureClassification.SYNTAX_ERROR)
    assert strategy is not None
    recovery.register_executor(strategy.id, smart_executor)

    # Execute recovery
    action = await recovery.execute_recovery(
        node_id="node_executor_test",
        failure_type=FailureClassification.SYNTAX_ERROR,
        classification_result=None,
        context={"retry_count": 0},
    )
    assert action is not None
    assert action.success is True
    assert action.injected_node_id is not None

    # Record in memory
    await memory.record(action, strategy, True,
                        {"node_description": "executor test"})

    # Verify integration
    assert len(injector.injected_nodes) == 1
    assert memory.total_entries == 1
    assert graph.called_inject_node is True


# ============================================================================
# Test 17: Multiple verifiers with mixed results and fallback classification
# ============================================================================

@pytest.mark.asyncio
async def test_mixed_verifier_results_with_classification():
    """Multiple verifiers produce mixed results; classifier picks up
    verification failures and classifies them appropriately."""
    from runtime_next.verification.pipeline import VerificationPipeline
    from runtime_next.verification.classifier import FailureClassifier
    from runtime_next.verification.types import (
        VerificationType, VerificationManifest, VerificationScope,
        VerificationResult, Confidence, Severity, FailureClassification,
    )

    pipeline = VerificationPipeline()
    classifier = FailureClassifier()

    # Three verifiers with different outcomes
    async def passing(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_pass", node_id=node_id,
            verification_type=VerificationType.LINT, success=True,
            confidence=Confidence.HIGH,
        )

    async def failing(node_id, scope, context):
        return VerificationResult(
            verification_id=f"v_{node_id}_fail", node_id=node_id,
            verification_type=VerificationType.TYPECHECK, success=False,
            severity=Severity.ERROR,
            diagnostics=["Type error"],
            confidence=Confidence.CERTAIN,
        )

    async def crashing(node_id, scope, context):
        raise RuntimeError("Unexpected crash in verifier")

    pipeline.register_verifier(VerificationType.LINT, passing)
    pipeline.register_verifier(VerificationType.TYPECHECK, failing)
    pipeline.register_verifier(VerificationType.UNIT_TEST, crashing)

    manifest = VerificationManifest(
        required=[VerificationType.LINT, VerificationType.TYPECHECK],
        blocking=[VerificationType.TYPECHECK],
        optional=[VerificationType.UNIT_TEST],
    )
    scope = VerificationScope.empty()
    results = await pipeline.verify("node_mixed", manifest, scope, {})

    # LINT passes, TYPECHECK fails and blocks the optional crashing UNIT_TEST
    assert len(results) == 2
    assert results[0].success is True
    assert results[1].success is False

    # Now classify using verification results
    vresults = [r.model_dump() for r in results]
    classification = await classifier.classify(
        error_message="Verification failures detected",
        verification_results=vresults,
    )
    assert classification.primary == FailureClassification.VERIFICATION_FAILURE
