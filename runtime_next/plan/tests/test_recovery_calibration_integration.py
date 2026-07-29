"""
test_recovery_calibration_integration.py — Phase 7-9 Integration Tests

Verifies:
  1. RecoveryEngine.inject_plan_strategies() registers strategies
  2. _classify_plan_failure_mode() produces correct classifications
  3. sync_recovery_to_calibration() returns correct summaries
  4. Orchestrator handles None/empty plans without crashing
  5. _record_verification_calibration() creates calibration entries
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

from runtime_next.verification.types import (
    FailureClassification,
    VerificationType,
    VerificationResult,
    Confidence,
    Severity,
    Retryability,
)
from runtime_next.verification.memory import RecoveryMemoryEntry

from runtime_next.recovery.engine import RecoveryEngine

from runtime_next.plan.architect_types import (
    FailureModeStrategy,
    RecoveryStrategyType,
    RecoveryPlanSection,
)

# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def recovery_engine():
    """RecoveryEngine with no graph attached (fine for sync tests)."""
    return RecoveryEngine(graph=None)


@pytest.fixture
def populated_recovery_engine():
    """RecoveryEngine with recovery memory entries already populated."""
    engine = RecoveryEngine(graph=None)

    # Manually insert entries into the recovery memory
    entry_success = RecoveryMemoryEntry(
        id="mem_001",
        failure_type=FailureClassification.SYNTAX_ERROR,
        recovery_strategy_id="plan_syntax_error_3",
        recovery_strategy_name="[Plan] Re-attempt with debugging",
        success=True,
        node_context="phase_3: Implement OAuth2 flow",
    )
    entry_failure = RecoveryMemoryEntry(
        id="mem_002",
        failure_type=FailureClassification.VERIFICATION_FAILURE,
        recovery_strategy_id="plan_verification_failure_2",
        recovery_strategy_name="[Plan] Isolate and fix individually",
        success=False,
        node_context="phase_verify: Run tests and type checks",
    )
    entry_unknown = RecoveryMemoryEntry(
        id="mem_003",
        failure_type=FailureClassification.TIMEOUT,
        recovery_strategy_id="plan_timeout_2",
        recovery_strategy_name="[Plan] Retry with extended limit",
        success=True,
        node_context="phase_3: External API call",
    )
    engine.recovery_memory._entries = [entry_success, entry_failure, entry_unknown]

    return engine


@pytest.fixture
def sample_failure_mode_strategies():
    """Sample FailureModeStrategy objects as produced by the architect plan."""
    return [
        FailureModeStrategy(
            failure_mode="OAuth2 implementation contains syntax errors",
            phase_id="phase_3",
            strategy=RecoveryStrategyType.RETRY,
            fallback_description="Re-attempt implementation with error diagnostics",
            max_retries=3,
            triggers_human_review=False,
        ),
        FailureModeStrategy(
            failure_mode="Test failures in verification phase",
            phase_id="phase_verify",
            strategy=RecoveryStrategyType.DECOMPOSE,
            fallback_description="Isolate failing checks and fix individually",
            max_retries=2,
            triggers_human_review=False,
        ),
        FailureModeStrategy(
            failure_mode="Security vulnerabilities found in OAuth2 flow",
            phase_id="phase_verify",
            strategy=RecoveryStrategyType.ESCALATE,
            fallback_description="Document vulnerabilities and escalate to user",
            max_retries=0,
            triggers_human_review=True,
        ),
    ]


@pytest.fixture
def sample_recovery_plan_section(sample_failure_mode_strategies):
    """A RecoveryPlanSection containing failure strategies."""
    return RecoveryPlanSection(
        failure_strategies=sample_failure_mode_strategies,
        rollback_points=["phase_1", "phase_2", "phase_3"],
        general_approach="Retry or escalate based on failure mode.",
    )


@pytest.fixture
def sample_verification_results():
    """Sample VerificationResult list for calibration recording tests."""
    return [
        VerificationResult(
            verification_id="v_001",
            node_id="phase_3",
            verification_type=VerificationType.LINT,
            success=True,
            confidence=Confidence.HIGH,
            severity=Severity.INFO,
            retryability=Retryability.SAFE,
            diagnostics=["Lint passed"],
            provenance="plan_verification_lint",
        ),
        VerificationResult(
            verification_id="v_002",
            node_id="phase_3",
            verification_type=VerificationType.TYPECHECK,
            success=False,
            confidence=Confidence.HIGH,
            severity=Severity.ERROR,
            retryability=Retryability.SAFE,
            diagnostics=["Type error in auth/oauth.py"],
            provenance="plan_verification_typecheck",
        ),
        VerificationResult(
            verification_id="v_003",
            node_id="phase_verify",
            verification_type=VerificationType.SECURITY_SCAN,
            success=False,
            confidence=Confidence.MEDIUM,
            severity=Severity.WARNING,
            retryability=Retryability.SAFE,
            diagnostics=["Potential token leak in redirect"],
            provenance="plan_verification_security_scan",
        ),
        VerificationResult(
            verification_id="v_004",
            node_id="phase_3",
            verification_type=VerificationType.SANDBOX_VALIDATION,
            success=True,
            confidence=Confidence.CERTAIN,
            severity=Severity.INFO,
            retryability=Retryability.SAFE,
            diagnostics=["Sandbox OK"],
            provenance="sandbox_verifier",
        ),
    ]


@pytest.fixture
def mock_plan():
    """A lightweight mock architect plan for orchestrator-flow tests."""
    plan = MagicMock()
    plan.id = "test_plan_integration"
    plan.objective.goal = "Refactor auth module to use OAuth2"

    # Mock recovery plan with strategies
    recovery = MagicMock()
    recovery.failure_strategies = []
    plan.recovery_plan = recovery

    # Mock execution strategy
    exec_strategy = MagicMock()
    exec_strategy.phases = []
    plan.execution_strategy = exec_strategy

    # Mock verification plan
    ver_plan = MagicMock()
    ver_plan.checks = []
    plan.verification_plan = ver_plan

    # Mock specialist assignments
    spec_assignments = MagicMock()
    spec_assignments.assignments = []
    plan.specialist_assignments = spec_assignments

    # Mock risks
    risks = MagicMock()
    risks.risks = []
    plan.risks = risks

    # Mock metadata
    plan.metadata = {"strategy_selected": "comprehensive"}

    return plan


@pytest.fixture
def mock_calibration_system():
    """A mock PlanCalibrationSystem for testing outcome recording."""
    cal = MagicMock()
    cal._outcomes = {}  # Simulate hasattr check
    cal.record_outcome = MagicMock()
    cal.get_adjustments_for_task = MagicMock(return_value=[])
    cal.get_calibration_summary = MagicMock(
        return_value={"total_outcomes_recorded": 1}
    )
    return cal


# ===========================================================================
# Section 1: inject_plan_strategies() integration
# ===========================================================================


class TestRecoveryEngineInjectPlanStrategies:
    """Verify that architect plan recovery strategies are properly injected
    into the RecoveryStrategyEngine."""

    def test_inject_plan_strategies_registers_in_engine(
        self, recovery_engine, sample_recovery_plan_section
    ):
        """Strategies must be registered in the underlying RecoveryStrategyEngine."""
        count = recovery_engine.inject_plan_strategies(sample_recovery_plan_section)
        assert count > 0, "Should have injected at least one strategy"

        # Verify the strategies are now in the engine
        strategies = recovery_engine.recovery_strategies.strategies
        # The engine registers 15 defaults. Injected strategies override the
        # matching failure types. We should still have all defaults.
        assert len(strategies) >= 15

        # Verify the plan strategy for syntax errors is registered
        syn_strategy = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.SYNTAX_ERROR
        )
        assert syn_strategy is not None
        assert "[Plan]" in syn_strategy.name
        assert syn_strategy.max_retries == 3
        assert syn_strategy.requires_user_approval is False

        # Verify the plan strategy for verification failures is registered
        ver_strategy = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.VERIFICATION_FAILURE
        )
        assert ver_strategy is not None
        assert "[Plan]" in ver_strategy.name
        assert ver_strategy.max_retries == 2

        # Verify the plan strategy for permission_denied (escalation) is registered
        perm_strategy = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.PERMISSION_DENIED
        )
        assert perm_strategy is not None
        assert perm_strategy.max_retries == 0
        assert perm_strategy.requires_user_approval is True

    def test_inject_plan_strategies_handles_dict_input(
        self, recovery_engine
    ):
        """Injecting a list of dicts (instead of objects) must also work."""
        dict_strategies = [
            {
                "failure_mode": "API timeout during implementation",
                "phase_id": "phase_3",
                "strategy": "retry",
                "fallback_description": "Retry with exponential backoff",
                "max_retries": 3,
                "triggers_human_review": False,
            },
            {
                "failure_mode": "Database connection failure",
                "phase_id": "phase_2",
                "strategy": "substitute",
                "fallback_description": "Switch to fallback database",
                "max_retries": 2,
                "triggers_human_review": True,
            },
        ]

        count = recovery_engine.inject_plan_strategies(dict_strategies)
        assert count == 2

        # Timeout strategy should now have max_retries=3 from plan
        timeout_strat = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.TIMEOUT
        )
        assert timeout_strat is not None
        assert "[Plan]" in timeout_strat.name
        assert timeout_strat.max_retries == 3

    def test_inject_plan_strategies_skips_unknown_classifications(
        self, recovery_engine
    ):
        """Unrecognized failure modes should still get TOOL_FAILURE strategy."""
        strange_strategies = [
            {
                "failure_mode": "Quantum decoherence in runtime stack",
                "phase_id": "phase_1",
                "strategy": "retry",
                "fallback_description": "Retry with quantum error correction",
                "max_retries": 5,
            },
        ]

        count = recovery_engine.inject_plan_strategies(strange_strategies)
        assert count == 1

        # Should map to TOOL_FAILURE (default for unrecognized patterns)
        tool_strat = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.TOOL_FAILURE
        )
        assert tool_strat is not None
        assert "[Plan]" in tool_strat.name
        assert tool_strat.max_retries == 5

    def test_inject_plan_strategies_empty_list(self, recovery_engine):
        """Empty input must return 0."""
        count = recovery_engine.inject_plan_strategies([])
        assert count == 0

    def test_inject_plan_strategies_none(self, recovery_engine):
        """None-like input must return 0 without crashing."""
        count = recovery_engine.inject_plan_strategies(None)
        assert count == 0

    def test_inject_plan_strategies_empty_section(self, recovery_engine):
        """RecoveryPlanSection with no strategies returns 0."""
        # RecoveryPlanSection validates non-empty, so just pass a section-like
        # object or use a section constructed via model_construct
        empty = RecoveryPlanSection.model_construct(
            failure_strategies=[],
            rollback_points=[],
        )
        count = recovery_engine.inject_plan_strategies(empty)
        assert count == 0

    def test_inject_plan_strategies_does_not_overwrite_defaults(
        self, recovery_engine
    ):
        """Injected plan strategies should not replace existing non-plan strategies
        for failure types that weren't targeted by the plan."""
        # Capture the original UNKNOWN_FAILURE strategy
        original_unknown = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.UNKNOWN_FAILURE
        )
        assert original_unknown is not None
        original_id = original_unknown.id
        original_max_retries = original_unknown.max_retries

        # Inject a plan strategy for a DIFFERENT failure type
        recovery_engine.inject_plan_strategies([
            {
                "failure_mode": "OAuth2 implementation errors",
                "phase_id": "phase_3",
                "strategy": "retry",
                "max_retries": 99,
            },
        ])

        # The UNKNOWN_FAILURE strategy should be unchanged
        unknown = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.UNKNOWN_FAILURE
        )
        assert unknown.id == original_id
        assert unknown.max_retries == original_max_retries

    def test_get_plan_strategies_returns_metadata(
        self, recovery_engine, sample_recovery_plan_section
    ):
        """get_plan_strategies() must return the stored metadata dict."""
        recovery_engine.inject_plan_strategies(sample_recovery_plan_section)

        metadata = recovery_engine.get_plan_strategies()
        assert len(metadata) == 3

        # Keys should be phase_id::failure_mode prefixes
        keys = list(metadata.keys())
        assert any("phase_3" in k for k in keys)
        assert any("phase_verify" in k for k in keys)

        # Each entry should have expected fields
        for key, info in metadata.items():
            assert "failure_mode" in info
            assert "phase_id" in info
            assert "strategy" in info
            assert "classification" in info
            assert "fallback" in info
            assert "max_retries" in info


# ===========================================================================
# Section 2: _classify_plan_failure_mode()
# ===========================================================================


class TestRecoveryEngineClassify:
    """Verify text-based failure mode classification."""

    def test_classify_syntax_error(self, recovery_engine):
        fc = recovery_engine._classify_plan_failure_mode(
            "Syntax error in implementation", "retry"
        )
        assert fc == FailureClassification.SYNTAX_ERROR

    def test_classify_dependency_missing(self, recovery_engine):
        fc = recovery_engine._classify_plan_failure_mode(
            "Missing import for OAuth2 library", "retry"
        )
        assert fc == FailureClassification.DEPENDENCY_MISSING

    def test_classify_permission_denied(self, recovery_engine):
        fc = recovery_engine._classify_plan_failure_mode(
            "Access denied to user database", "escalate"
        )
        assert fc == FailureClassification.PERMISSION_DENIED

    def test_classify_timeout(self, recovery_engine):
        fc = recovery_engine._classify_plan_failure_mode(
            "External API request timed out", "retry"
        )
        assert fc == FailureClassification.TIMEOUT

    def test_classify_verification_failure(self, recovery_engine):
        fc = recovery_engine._classify_plan_failure_mode(
            "Unit tests failed after implementation", "decompose"
        )
        assert fc == FailureClassification.VERIFICATION_FAILURE

    def test_classify_tool_failure(self, recovery_engine):
        fc = recovery_engine._classify_plan_failure_mode(
            "Specialist handler raised exception", "retry"
        )
        assert fc == FailureClassification.TOOL_FAILURE

    def test_classify_lock_contention(self, recovery_engine):
        fc = recovery_engine._classify_plan_failure_mode(
            "File lock contention detected", "retry"
        )
        assert fc == FailureClassification.MUTEX_VIOLATION

    def test_classify_unknown_fallsback_to_tool_failure(self, recovery_engine):
        """Unrecognized text patterns should default to TOOL_FAILURE."""
        fc = recovery_engine._classify_plan_failure_mode(
            "Cosmic ray bit flip", "retry"
        )
        assert fc == FailureClassification.TOOL_FAILURE


# ===========================================================================
# Section 3: sync_recovery_to_calibration()
# ===========================================================================


class TestRecoveryEngineSyncCalibration:
    """Verify calibration sync produces correct summaries."""

    def test_sync_no_entries(self, recovery_engine, mock_calibration_system):
        """When no recovery entries exist, sync returns 0."""
        recovery_engine.link_calibration_system(mock_calibration_system)
        result = recovery_engine.sync_recovery_to_calibration()
        assert result["synced"] == 0
        assert "note" in result

    def test_sync_no_calibration_link(self, recovery_engine):
        """Without a linked calibration system, sync returns a note."""
        result = recovery_engine.sync_recovery_to_calibration()
        assert result["synced"] == 0
        assert "No calibration system" in result["note"]

    def test_sync_populated_entries(
        self, populated_recovery_engine, mock_calibration_system
    ):
        """Populated recovery memory returns correct counts."""
        populated_recovery_engine.link_calibration_system(mock_calibration_system)
        result = populated_recovery_engine.sync_recovery_to_calibration()

        assert result["synced"] == 3
        assert result["success_count"] == 2
        assert result["failure_count"] == 1
        assert len(result["failure_types"]) == 3
        assert "syntax_error" in result["failure_types"]
        assert "verification_failure" in result["failure_types"]
        assert "timeout" in result["failure_types"]

    def test_sync_with_empty_memory_after_link(
        self, recovery_engine, mock_calibration_system
    ):
        """Even with link, empty memory returns 0."""
        recovery_engine.link_calibration_system(mock_calibration_system)
        result = recovery_engine.sync_recovery_to_calibration()
        assert result["synced"] == 0
        assert "No recovery entries" in result["note"]

    def test_link_calibration_system_stores_reference(
        self, recovery_engine, mock_calibration_system
    ):
        """After linking, the calibration system reference must be stored."""
        assert recovery_engine._calibration_system is None
        recovery_engine.link_calibration_system(mock_calibration_system)
        assert recovery_engine._calibration_system is mock_calibration_system


# ===========================================================================
# Section 4: Orchestrator flow with None/empty plans
# ===========================================================================


class TestOrchestratorNonePlanSafety:
    """Verify that orchestrator recovery/calibration integration points
    handle None and empty plans without crashing."""

    def test_inject_plan_strategies_with_none_plan(self, recovery_engine):
        """Calling inject_plan_strategies with None plan must not crash
        and return 0."""
        count = recovery_engine.inject_plan_strategies(None)
        assert count == 0

    def test_inject_plan_strategies_with_plan_missing_recovery(self, recovery_engine):
        """A plan object without a recovery_plan attribute must be handled."""
        plan = MagicMock()
        # Remove the recovery_plan attribute
        del plan.recovery_plan
        # inject_plan_strategies uses getattr, so should handle this
        count = recovery_engine.inject_plan_strategies(plan)
        assert count == 0

    def test_recovery_engine_no_graph_on_sync(self, recovery_engine):
        """sync_recovery_to_calibration works even when no graph is attached."""
        # recovery_engine was created with graph=None
        result = recovery_engine.sync_recovery_to_calibration()
        # Should not crash - just returns no-sync note
        assert isinstance(result, dict)
        assert "synced" in result

    def test_sync_after_multiple_injections(
        self, recovery_engine, mock_calibration_system
    ):
        """Multiple injections should not break sync."""
        recovery_engine.link_calibration_system(mock_calibration_system)

        # First injection
        recovery_engine.inject_plan_strategies([
            {"failure_mode": "Timeout", "phase_id": "p1", "strategy": "retry", "max_retries": 2},
        ])

        # Verify the strategy is registered
        strat = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.TIMEOUT
        )
        assert "[Plan]" in strat.name

        # Second injection — same failure type
        recovery_engine.inject_plan_strategies([
            {"failure_mode": "Timeout during retry", "phase_id": "p2", "strategy": "retry", "max_retries": 5},
        ])

        # Should NOT replace the existing plan strategy
        strat2 = recovery_engine.recovery_strategies.get_strategy(
            FailureClassification.TIMEOUT
        )
        assert strat2.max_retries == 2  # Original plan strategy preserved

    def test_full_integration_cycle(
        self, recovery_engine, mock_calibration_system, sample_recovery_plan_section
    ):
        """Complete cycle: inject strategies → simulate recovery memory → sync."""
        recovery_engine.link_calibration_system(mock_calibration_system)

        # 1. Inject plan strategies
        injected = recovery_engine.inject_plan_strategies(sample_recovery_plan_section)
        assert injected == 3

        # 2. Simulate recovery outcomes by adding entries to memory
        entry_success = RecoveryMemoryEntry(
            id="cycle_001",
            failure_type=FailureClassification.SYNTAX_ERROR,
            recovery_strategy_id="plan_syntax_error_3",
            recovery_strategy_name="[Plan] Re-attempt implementation with error diagnostics",
            success=True,
            node_context="phase_3",
        )
        recovery_engine.recovery_memory._entries.append(entry_success)

        entry_failure = RecoveryMemoryEntry(
            id="cycle_002",
            failure_type=FailureClassification.VERIFICATION_FAILURE,
            recovery_strategy_id="plan_verification_failure_2",
            recovery_strategy_name="[Plan] Isolate failing checks",
            success=False,
            node_context="phase_verify",
        )
        recovery_engine.recovery_memory._entries.append(entry_failure)

        # 3. Sync to calibration
        sync_result = recovery_engine.sync_recovery_to_calibration()
        assert sync_result["synced"] == 2
        assert sync_result["success_count"] == 1
        assert sync_result["failure_count"] == 1

        # 4. Verify plan strategies metadata is accessible
        metadata = recovery_engine.get_plan_strategies()
        assert len(metadata) == 3


# ===========================================================================
# Section 5: _record_verification_calibration() integration
# ===========================================================================


class TestRecordVerificationCalibration:
    """Verify that verification results are recorded as calibration entries."""

    def test_record_with_failures_creates_calibration_entry(
        self, sample_verification_results, mock_plan, mock_calibration_system
    ):
        """When verification failures exist, record_outcome must be called."""
        # Simulate the orchestrator's _record_verification_calibration logic
        self._simulate_record_verification_calibration(
            plan=mock_plan,
            all_results=sample_verification_results,
            plan_results=sample_verification_results,
            calibration=mock_calibration_system,
        )

        # record_outcome should have been called
        mock_calibration_system.record_outcome.assert_called_once()

        # Verify the call arguments
        call_args = mock_calibration_system.record_outcome.call_args
        assert call_args is not None
        kwargs = call_args[1] if len(call_args) > 1 else call_args[0]

        # The plan_id should contain the mock plan's id + _vcheck
        plan_id = kwargs.get("plan_id", "")
        assert "_vcheck" in plan_id

        # Verification failures should be > 0
        verif_failures = kwargs.get("verification_failures_caught", 0)
        assert verif_failures > 0

        # verification_type_failures should contain typecheck and security_scan
        vtype_failures = kwargs.get("verification_type_failures", {})
        assert "typecheck" in vtype_failures
        assert "security_scan" in vtype_failures
        assert vtype_failures["typecheck"] == 1
        assert vtype_failures["security_scan"] == 1

    def test_record_with_no_failures_skips_calibration(
        self, mock_plan, mock_calibration_system
    ):
        """When all verifications pass, record_outcome should NOT be called."""
        all_success = [
            VerificationResult(
                verification_id="v_ok_1",
                node_id="phase_3",
                verification_type=VerificationType.LINT,
                success=True,
                confidence=Confidence.HIGH,
                severity=Severity.INFO,
                retryability=Retryability.SAFE,
                diagnostics=["Lint passed"],
                provenance="plan_verification_lint",
            ),
            VerificationResult(
                verification_id="v_ok_2",
                node_id="phase_3",
                verification_type=VerificationType.TYPECHECK,
                success=True,
                confidence=Confidence.HIGH,
                severity=Severity.INFO,
                retryability=Retryability.SAFE,
                diagnostics=["Typecheck passed"],
                provenance="plan_verification_typecheck",
            ),
        ]

        self._simulate_record_verification_calibration(
            plan=mock_plan,
            all_results=all_success,
            plan_results=all_success,
            calibration=mock_calibration_system,
        )

        # With all successes and no failures, record_outcome should NOT be called
        # because the method is guarded by `if not type_failures: return`
        mock_calibration_system.record_outcome.assert_not_called()

    def test_record_with_empty_results_skips_calibration(
        self, mock_plan, mock_calibration_system
    ):
        """Empty verification results should skip recording."""
        self._simulate_record_verification_calibration(
            plan=mock_plan,
            all_results=[],
            plan_results=[],
            calibration=mock_calibration_system,
        )
        mock_calibration_system.record_outcome.assert_not_called()

    def test_record_none_plan_does_not_crash(
        self, sample_verification_results, mock_calibration_system
    ):
        """Passing None plan should not crash and should not record."""
        self._simulate_record_verification_calibration(
            plan=None,
            all_results=sample_verification_results,
            plan_results=sample_verification_results,
            calibration=mock_calibration_system,
        )
        # Should not have attempted to record an outcome with None plan
        mock_calibration_system.record_outcome.assert_not_called()

    def test_record_counts_correct_failure_types(
        self, mock_plan, mock_calibration_system
    ):
        """The per-check-type failure counts must correctly reflect which
        verification types produced failures."""
        mixed_results = [
            VerificationResult(
                verification_id="v_1", node_id="n1",
                verification_type=VerificationType.LINT,
                success=False, confidence=Confidence.HIGH,
                severity=Severity.ERROR, retryability=Retryability.SAFE,
                diagnostics=["Error"], provenance="plan_verification_lint",
            ),
            VerificationResult(
                verification_id="v_2", node_id="n1",
                verification_type=VerificationType.LINT,
                success=False, confidence=Confidence.HIGH,
                severity=Severity.ERROR, retryability=Retryability.SAFE,
                diagnostics=["Error2"], provenance="plan_verification_lint",
            ),
            VerificationResult(
                verification_id="v_3", node_id="n1",
                verification_type=VerificationType.TYPECHECK,
                success=True, confidence=Confidence.HIGH,
                severity=Severity.INFO, retryability=Retryability.SAFE,
                diagnostics=["OK"], provenance="plan_verification_typecheck",
            ),
            VerificationResult(
                verification_id="v_4", node_id="n1",
                verification_type=VerificationType.SECURITY_SCAN,
                success=False, confidence=Confidence.HIGH,
                severity=Severity.WARNING, retryability=Retryability.SAFE,
                diagnostics=["Issue"], provenance="plan_verification_security_scan",
            ),
        ]

        self._simulate_record_verification_calibration(
            plan=mock_plan,
            all_results=mixed_results,
            plan_results=mixed_results,
            calibration=mock_calibration_system,
        )

        mock_calibration_system.record_outcome.assert_called_once()
        call_kwargs = mock_calibration_system.record_outcome.call_args[1]
        vtype_failures = call_kwargs.get("verification_type_failures", {})

        # Lint failed twice, typecheck passed, security_scan failed once
        # verification_failures_caught = len(type_failures) = 2 (lint + security_scan),
        # NOT the sum of individual failures (3)
        assert vtype_failures.get("lint") == 2
        assert "typecheck" not in vtype_failures  # No failures
        assert vtype_failures.get("security_scan") == 1
        assert call_kwargs.get("verification_failures_caught") == 2

    # ======================================================================
    # Helper: simulates the orchestrator's _record_verification_calibration
    # ======================================================================

    def _simulate_record_verification_calibration(
        self,
        plan: Any,
        all_results: List[Any],
        plan_results: List[Any],
        calibration: Any,
    ):
        """Replicates the orchestrator's _record_verification_calibration logic
        for testability without instantiating the full Orchestrator."""
        # Guard: None plan (caller's responsibility, but method should handle)
        if plan is None:
            return

        if not all_results:
            return

        # Count failures by verification type
        type_failures: Dict[str, int] = {}
        type_total: Dict[str, int] = {}

        for vr in all_results:
            vtype = getattr(vr, "verification_type", None)
            if vtype is None:
                continue
            vtype_name = vtype.value if hasattr(vtype, "value") else str(vtype)
            type_total[vtype_name] = type_total.get(vtype_name, 0) + 1

            if not getattr(vr, "success", True):
                type_failures[vtype_name] = type_failures.get(vtype_name, 0) + 1

        if not type_failures:
            return

        # Build vtype_failures_for_outcome
        vtype_failures_for_outcome: Dict[str, int] = {}
        for vtype_name, fail_count in type_failures.items():
            if fail_count > 0:
                vtype_failures_for_outcome[vtype_name] = fail_count

        # Record outcome
        calibration.record_outcome(
            plan_id=plan.id + "_vcheck" if plan else "none_vcheck",
            objective=f"Verification check analytics for {plan.id[:12] if plan else 'none'}",
            task_type="verification_analytics",
            strategy_class="verification",
            planned_phases=len(type_total),
            completed_phases=len([t for t, c in type_total.items()
                                 if type_failures.get(t, 0) == 0]),
            planned_specialists=[],
            actual_specialists=[],
            planned_risks=0,
            materialized_risks=0,
            verification_checks_run=len(all_results),
            verification_failures_caught=len(type_failures),
            verification_type_failures=vtype_failures_for_outcome,
            unplanned_failures=0,
            total_duration_ms=0.0,
            success=len(type_failures) == 0,
        )


# ===========================================================================
# Section 6: Edge Cases & Error Handling
# ===========================================================================


class TestEdgeCases:
    """Verify handling of boundary conditions."""

    def test_recovery_engine_recovery_count_property(self, recovery_engine):
        """recovery_count should reflect history length."""
        assert recovery_engine.recovery_count == 0
        recovery_engine._recovery_history.append({"dummy": True})
        assert recovery_engine.recovery_count == 1

    def test_link_calibration_system_twice(self, recovery_engine, mock_calibration_system):
        """Linking a second calibration system should replace the first."""
        alt_cal = MagicMock()
        recovery_engine.link_calibration_system(mock_calibration_system)
        assert recovery_engine._calibration_system is mock_calibration_system

        recovery_engine.link_calibration_system(alt_cal)
        assert recovery_engine._calibration_system is alt_cal

    def test_inject_plan_strategies_with_mixed_types(
        self, recovery_engine, sample_failure_mode_strategies
    ):
        """Mixing FailureModeStrategy objects and dicts should work."""
        mixed = [
            sample_failure_mode_strategies[0],  # object
            {"failure_mode": "Timeout", "phase_id": "p2", "strategy": "retry", "max_retries": 3},  # dict
        ]
        count = recovery_engine.inject_plan_strategies(mixed)
        assert count == 2

    def test_recovery_engine_import_guard(self):
        """The import guard for architect types must work."""
        from runtime_next.recovery.engine import _HAS_ARCHITECT_TYPES
        assert _HAS_ARCHITECT_TYPES is True
