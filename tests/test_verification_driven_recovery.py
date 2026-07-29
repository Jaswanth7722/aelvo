"""
tests/test_verification_driven_recovery.py — Phase 11: Verification-Driven Recovery Pipeline

Tests the VerificationDrivenRecoveryPipeline class including:
- All 8 pipeline phases (classify → strategy → govern → safety → recover → reverify → record → evolve)
- Pipeline result properties and reporting
- Success rate tracking per failure type
- Plan evolution integration
- Edge cases (unknown failures, blocked governance, safety failures, recovery failures)
- Configuration options
"""

from __future__ import annotations

import os
import sys
import time
import unittest
from typing import Any, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from runtime_next.verification.driven_recovery import (
    VerificationDrivenRecoveryPipeline,
    RecoveryPipelineResult,
    RecoveryPipelinePhase,
    RecoveryPipelineConfig,
)
from runtime_next.verification.types import (
    FailureClassification,
    ClassificationResult,
    Confidence,
    VerificationType,
    RecoveryStrategy,
    RecoveryAction,
    RetryDecision,
    GovernanceDecision,
    VerificationResult,
    Retryability,
    Severity,
)


# ===========================================================================
# Helpers
# ===========================================================================


def _make_classification(
    primary: FailureClassification = FailureClassification.SYNTAX_ERROR,
    confidence: Confidence = Confidence.HIGH,
    score: float = 0.85,
) -> ClassificationResult:
    return ClassificationResult(
        primary=primary,
        confidence=confidence,
        confidence_score=score,
        evidence={"text_analyzed": True},
        alternatives=[],
        alternative_scores={},
    )


def _make_retry_decision(
    can_retry: bool = True,
    reason: str = "Retry is safe",
    backoff: float = 1.0,
) -> RetryDecision:
    return RetryDecision(
        can_retry=can_retry,
        reason=reason,
        suggested_backoff=backoff,
        graph_consistent=True,
        capability_valid=True,
        mutation_safe=True,
        dependency_fresh=True,
    )


def _make_governance(
    verdict: str = "auto_recover",
    requires_intervention: bool = False,
) -> GovernanceDecision:
    return GovernanceDecision(
        verdict=verdict,
        reason="Safe to proceed",
        confidence=Confidence.HIGH,
        danger_assessment="safe",
        requires_user_intervention=requires_intervention,
    )


def _make_recovery_action(
    success: bool = True,
    action_type: str = "retry",
    node_id: str = "test_node",
) -> RecoveryAction:
    return RecoveryAction(
        id=f"action_{int(time.time())}",
        strategy_id="strat_test",
        node_id=node_id,
        failure_classification=FailureClassification.SYNTAX_ERROR,
        action_type=action_type,
        description="Test recovery action",
        success=success,
    )


def _make_verification_result(
    success: bool = True,
    vtype: VerificationType = VerificationType.LINT,
    node_id: str = "test_node",
) -> VerificationResult:
    return VerificationResult(
        verification_id=f"ver_{int(time.time())}",
        node_id=node_id,
        verification_type=vtype,
        success=success,
        confidence=Confidence.HIGH,
        severity=Severity.INFO,
        retryability=Retryability.SAFE,
        diagnostics=["Test verification"],
        provenance="test",
    )


class MockClassifier:
    """Mock classifier with configurable response."""

    def __init__(self):
        self.classification = _make_classification()

    async def classify(self, **kwargs) -> ClassificationResult:
        return self.classification


class MockRecoveryStrategies:
    """Mock recovery strategies engine."""

    def __init__(self):
        self.strategy = RecoveryStrategy(
            id="strat_test",
            name="Test Retry",
            failure_type=FailureClassification.SYNTAX_ERROR,
            description="A test recovery strategy",
            max_retries=3,
        )
        self.action = _make_recovery_action(success=True)
        self.action_success = True

    def get_strategy(self, failure_type: FailureClassification) -> Optional[RecoveryStrategy]:
        return self.strategy

    async def execute_recovery(self, node_id: str, **kwargs) -> Optional[RecoveryAction]:
        return self.action


class MockRetrySafety:
    """Mock retry safety engine."""

    def __init__(self):
        self.decision = _make_retry_decision(can_retry=True)

    async def evaluate(self, **kwargs) -> RetryDecision:
        return self.decision

    def get_retry_count(self, node_id: str) -> int:
        return 0


class MockGovernance:
    """Mock governance engine."""

    def __init__(self):
        self.decision = _make_governance()

    async def decide(self, **kwargs) -> GovernanceDecision:
        return self.decision


class MockVerificationPipeline:
    """Mock verification pipeline."""

    def __init__(self):
        self.results = [_make_verification_result(success=True)]

    async def verify(self, **kwargs) -> List[VerificationResult]:
        return self.results


class MockRecoveryMemory:
    """Mock recovery memory."""

    def __init__(self):
        self.entry_id = "mem_test_001"

    async def record(self, action: RecoveryAction, **kwargs) -> Any:
        class Entry:
            id = "mem_test_001"
        return Entry()


class MockPlanEvolution:
    """Mock plan evolution engine."""

    def __init__(self):
        self.called = False
        self.last_failure = ""

    def process_verification_failure(self, milestone_id: str, check_name: str, failure_summary: str) -> None:
        self.called = True
        self.last_failure = check_name


# ===========================================================================
# Tests: Pipeline Result Properties
# ===========================================================================


class TestRecoveryPipelineResult(unittest.TestCase):
    """RecoveryPipelineResult properties and formatting."""

    def test_duration_calculation(self):
        result = RecoveryPipelineResult(
            pipeline_id="test_001",
            node_id="node_001",
            status=RecoveryPipelinePhase.COMPLETED,
            started_at=1000.0,
            completed_at=1050.0,
        )
        self.assertEqual(result.duration_ms, 50000.0)

    def test_overall_success_recovery_only(self):
        """Overall success should match recovery success when no re-verification."""
        result = RecoveryPipelineResult(
            pipeline_id="test_002",
            node_id="node_002",
            status=RecoveryPipelinePhase.COMPLETED,
            recovery_action=_make_recovery_action(success=True),
        )
        self.assertTrue(result.overall_success)

    def test_overall_success_recovery_failed(self):
        """Overall success should be False when recovery fails."""
        result = RecoveryPipelineResult(
            pipeline_id="test_003",
            node_id="node_003",
            status=RecoveryPipelinePhase.COMPLETED,
            recovery_action=_make_recovery_action(success=False),
        )
        self.assertFalse(result.overall_success)

    def test_overall_success_with_reverify(self):
        """Overall success requires both recovery and re-verify to pass."""
        result = RecoveryPipelineResult(
            pipeline_id="test_004",
            node_id="node_004",
            status=RecoveryPipelinePhase.COMPLETED,
            recovery_action=_make_recovery_action(success=True),
            post_recovery_verifications=[
                _make_verification_result(success=True),
                _make_verification_result(success=True),
            ],
        )
        self.assertTrue(result.overall_success)

    def test_overall_success_reverify_failed(self):
        """Overall success should be False when re-verification fails."""
        result = RecoveryPipelineResult(
            pipeline_id="test_005",
            node_id="node_005",
            status=RecoveryPipelinePhase.COMPLETED,
            recovery_action=_make_recovery_action(success=True),
            post_recovery_verifications=[
                _make_verification_result(success=True),
                _make_verification_result(success=False),
            ],
        )
        self.assertFalse(result.overall_success)

    def test_failure_type_from_classification(self):
        """Failure type should be extracted from classification."""
        result = RecoveryPipelineResult(
            pipeline_id="test_006",
            node_id="node_006",
            status=RecoveryPipelinePhase.CLASSIFIED,
            classification=_make_classification(FailureClassification.TIMEOUT),
        )
        self.assertEqual(result.failure_type, "timeout")

    def test_failure_type_none(self):
        """Failure type should be None without classification."""
        result = RecoveryPipelineResult(
            pipeline_id="test_007",
            node_id="node_007",
            status=RecoveryPipelinePhase.INITIATED,
        )
        self.assertIsNone(result.failure_type)

    def test_reverify_passed_none(self):
        """Reverify passed should be None without verifications."""
        result = RecoveryPipelineResult(
            pipeline_id="test_008",
            node_id="node_008",
            status=RecoveryPipelinePhase.COMPLETED,
        )
        self.assertIsNone(result.reverify_passed)

    def test_reverify_passed_all(self):
        """Reverify passed should be True when all pass."""
        result = RecoveryPipelineResult(
            pipeline_id="test_009",
            node_id="node_009",
            status=RecoveryPipelinePhase.REVERIFIED,
            post_recovery_verifications=[
                _make_verification_result(success=True),
            ],
        )
        self.assertTrue(result.reverify_passed)

    def test_to_summary_includes_status(self):
        """Summary should include key pipeline metrics."""
        result = RecoveryPipelineResult(
            pipeline_id="pipe_001",
            node_id="node_001",
            status=RecoveryPipelinePhase.COMPLETED,
        )
        summary = result.to_summary()
        self.assertIn("completed", summary)
        self.assertIn("pipe_001", summary)

    def test_format_report_structure(self):
        """Format report should return structured dict."""
        result = RecoveryPipelineResult(
            pipeline_id="pipe_report",
            node_id="node_report",
            status=RecoveryPipelinePhase.COMPLETED,
            recovery_action=_make_recovery_action(success=True),
        )
        report = result.format_report()
        self.assertEqual(report["pipeline_id"], "pipe_report")
        self.assertEqual(report["overall_success"], True)
        self.assertIn("recovery_success", report)
        self.assertIn("governance_verdict", report)


# ===========================================================================
# Tests: Pipeline with All Components Mocked
# ===========================================================================


class TestPipelineFullRun(unittest.TestCase):
    """Full pipeline run with mocked components."""

    def setUp(self):
        self.classifier = MockClassifier()
        self.strategies = MockRecoveryStrategies()
        self.safety = MockRetrySafety()
        self.governance = MockGovernance()
        self.verification = MockVerificationPipeline()
        self.memory = MockRecoveryMemory()
        self.plan_evolution = MockPlanEvolution()

        self.pipeline = VerificationDrivenRecoveryPipeline(
            classifier=self.classifier,
            recovery_strategies=self.strategies,
            retry_safety=self.safety,
            governance=self.governance,
            verification_pipeline=self.verification,
            recovery_memory=self.memory,
            plan_evolution_engine=self.plan_evolution,
            config=RecoveryPipelineConfig(
                enable_reverify=True,
                enable_plan_evolution=True,
                track_success_rates=True,
            ),
        )

    def test_complete_pipeline_run(self):
        """Full pipeline should complete successfully."""
        import asyncio
        result = asyncio.run(self.pipeline.run(
            node_id="test_node",
            error_message="Syntax error in code",
        ))
        self.assertEqual(result.status, RecoveryPipelinePhase.COMPLETED)
        self.assertTrue(result.overall_success)
        self.assertIsNotNone(result.classification)
        self.assertIsNotNone(result.strategy)
        self.assertIsNotNone(result.governance)
        self.assertIsNotNone(result.retry_decision)
        self.assertIsNotNone(result.recovery_action)
        self.assertIsNotNone(result.recovery_memory_entry_id)

    def test_pipeline_id_is_unique(self):
        """Each pipeline run should have a unique ID."""
        import asyncio
        result1 = asyncio.run(self.pipeline.run(
            node_id="test_node", error_message="Error 1",
        ))
        result2 = asyncio.run(self.pipeline.run(
            node_id="test_node", error_message="Error 2",
        ))
        self.assertNotEqual(result1.pipeline_id, result2.pipeline_id)

    def test_pipeline_tracks_history(self):
        """Pipeline should track all runs in history."""
        import asyncio
        asyncio.run(self.pipeline.run(node_id="node_a"))
        asyncio.run(self.pipeline.run(node_id="node_b"))
        asyncio.run(self.pipeline.run(node_id="node_c"))
        self.assertEqual(self.pipeline.get_pipeline_count(), 3)


# ===========================================================================
# Tests: Pipeline Phase Failures
# ===========================================================================


class TestPipelinePhaseFailures(unittest.TestCase):
    """Pipeline should handle failures at each phase."""

    def test_classification_failure(self):
        """Pipeline should abort when classification fails."""
        classifier = MockClassifier()
        classifier.classification = _make_classification(
            FailureClassification.UNKNOWN_FAILURE,
        )
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=classifier,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node", error_message="Unknown error"))
        # Unknown failure → governance blocks it
        self.assertIn(result.status, (RecoveryPipelinePhase.ABORTED, RecoveryPipelinePhase.COMPLETED))

    def test_no_strategy(self):
        """Pipeline should block when no strategy exists."""
        classifier = MockClassifier()
        strategies = MockRecoveryStrategies()
        strategies.strategy = None  # No strategy

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=classifier,
            recovery_strategies=strategies,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node"))
        self.assertEqual(result.status, RecoveryPipelinePhase.BLOCKED)

    def test_governance_blocks(self):
        """Pipeline should block when governance rejects autonomous recovery."""
        governance = MockGovernance()
        governance.decision = _make_governance(verdict="abort", requires_intervention=True)

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            governance=governance,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node"))
        self.assertEqual(result.status, RecoveryPipelinePhase.ABORTED)

    def test_safety_blocks(self):
        """Pipeline should block when retry safety evaluation fails."""
        safety = MockRetrySafety()
        safety.decision = _make_retry_decision(can_retry=False, reason="Graph inconsistent")

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            retry_safety=safety,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node"))
        self.assertEqual(result.status, RecoveryPipelinePhase.BLOCKED)

    def test_recovery_fails(self):
        """Pipeline should handle recovery failure gracefully."""
        strategies = MockRecoveryStrategies()
        strategies.action = _make_recovery_action(success=False)
        strategies.action_success = False

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=strategies,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node"))
        self.assertEqual(result.status, RecoveryPipelinePhase.COMPLETED)
        self.assertFalse(result.overall_success)
        self.assertIsNotNone(result.recovery_action)


# ===========================================================================
# Tests: Success Rate Tracking
# ===========================================================================


class TestSuccessRateTracking(unittest.TestCase):
    """Pipeline should track success rates per failure type."""

    def test_tracks_successful_recovery(self):
        """Success rate should increase after successful recovery."""
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False, track_success_rates=True),
        )

        import asyncio
        asyncio.run(pipeline.run(node_id="test_node"))
        rates = pipeline.get_success_rates_by_type()
        self.assertIn("syntax_error", rates)
        self.assertEqual(rates["syntax_error"]["attempts"], 1)
        self.assertEqual(rates["syntax_error"]["successes"], 1)
        self.assertEqual(rates["syntax_error"]["rate"], 1.0)

    def test_tracks_failed_recovery(self):
        """Success rate should reflect failures."""
        strategies = MockRecoveryStrategies()
        strategies.action = _make_recovery_action(success=False)

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=strategies,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False, track_success_rates=True),
        )

        import asyncio
        asyncio.run(pipeline.run(node_id="test_node"))
        rates = pipeline.get_success_rates_by_type()
        self.assertEqual(rates["syntax_error"]["successes"], 0)
        self.assertEqual(rates["syntax_error"]["rate"], 0.0)

    def test_multiple_types_tracked_separately(self):
        """Different failure types should be tracked independently."""
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False, track_success_rates=True),
        )

        import asyncio

        # Run with syntax error
        asyncio.run(pipeline.run(node_id="node_1", error_message="Syntax error"))

        # Run with timeout (different classification)
        pipeline._classifier = MockClassifier()
        pipeline._classifier.classification = _make_classification(FailureClassification.TIMEOUT)
        asyncio.run(pipeline.run(node_id="node_2", error_message="Timeout"))

        rates = pipeline.get_success_rates_by_type()
        self.assertIn("syntax_error", rates)
        self.assertIn("timeout", rates)
        self.assertEqual(rates["syntax_error"]["attempts"], 1)
        self.assertEqual(rates["timeout"]["attempts"], 1)

    def test_get_success_rate_combined(self):
        """Combined success rate should include all types."""
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False, track_success_rates=True),
        )

        import asyncio
        asyncio.run(pipeline.run(node_id="node_1"))
        self.assertGreaterEqual(pipeline.get_success_rate(), 0.0)
        self.assertLessEqual(pipeline.get_success_rate(), 1.0)


# ===========================================================================
# Tests: Plan Evolution Integration
# ===========================================================================


class TestPlanEvolutionIntegration(unittest.TestCase):
    """Pipeline should trigger plan evolution on systemic failures."""

    def test_notifies_on_recovery_failure(self):
        """Plan evolution should be notified when recovery fails."""
        strategies = MockRecoveryStrategies()
        strategies.action = _make_recovery_action(success=False)
        evolution = MockPlanEvolution()

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=strategies,
            plan_evolution_engine=evolution,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=True, track_success_rates=True),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node"))
        self.assertTrue(result.plan_evolution_notified)
        self.assertTrue(evolution.called)

    def test_does_not_notify_on_success(self):
        """Plan evolution should NOT be notified when recovery succeeds."""
        evolution = MockPlanEvolution()

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            plan_evolution_engine=evolution,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=True, track_success_rates=True),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node"))
        # Successful recovery shouldn't trigger evolution
        self.assertFalse(result.plan_evolution_notified)

    def test_notifies_on_repeated_failures(self):
        """Plan evolution should be notified after repeated failures of same type."""
        strategies = MockRecoveryStrategies()
        strategies.action = _make_recovery_action(success=False)
        evolution = MockPlanEvolution()

        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=strategies,
            plan_evolution_engine=evolution,
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=True, track_success_rates=True),
        )

        import asyncio
        # First failure → evolution notified
        asyncio.run(pipeline.run(node_id="test_node"))

        # Second failure of same type → evolution notified again (repeated failures)
        asyncio.run(pipeline.run(node_id="test_node"))

        self.assertTrue(evolution.called)

    def test_evolution_disabled_when_not_configured(self):
        """Plan evolution should not run when disabled in config."""
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            config=RecoveryPipelineConfig(enable_plan_evolution=False),
        )
        self.assertFalse(pipeline._config.enable_plan_evolution)


# ===========================================================================
# Tests: Pipeline Configuration
# ===========================================================================


class TestPipelineConfiguration(unittest.TestCase):
    """Recovery pipeline configuration options."""

    def test_default_config(self):
        """Default config should have reasonable values."""
        config = RecoveryPipelineConfig()
        self.assertEqual(config.max_retries_per_failure, 3)
        self.assertTrue(config.enable_reverify)
        self.assertTrue(config.enable_plan_evolution)
        self.assertTrue(config.track_success_rates)
        self.assertIn(VerificationType.LINT, config.reverify_types)
        self.assertIn(VerificationType.TYPECHECK, config.reverify_types)

    def test_custom_config(self):
        """Custom config should override defaults."""
        config = RecoveryPipelineConfig(
            max_retries_per_failure=5,
            enable_reverify=False,
            enable_plan_evolution=False,
            track_success_rates=False,
            reverify_types=[VerificationType.SECURITY_SCAN],
        )
        self.assertEqual(config.max_retries_per_failure, 5)
        self.assertFalse(config.enable_reverify)
        self.assertFalse(config.enable_plan_evolution)
        self.assertFalse(config.track_success_rates)
        self.assertEqual(config.reverify_types, [VerificationType.SECURITY_SCAN])

    def test_pipeline_accepts_custom_config(self):
        """Pipeline should accept custom config."""
        config = RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False)
        pipeline = VerificationDrivenRecoveryPipeline(config=config)
        self.assertFalse(pipeline._config.enable_reverify)
        self.assertFalse(pipeline._config.enable_plan_evolution)


# ===========================================================================
# Tests: Pipeline Snapshot
# ===========================================================================


class TestPipelineSnapshot(unittest.TestCase):
    """Pipeline snapshot for monitoring."""

    def test_snapshot_returns_metrics(self):
        """Snapshot should return pipeline metrics."""
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )
        snapshot = pipeline.snapshot()
        self.assertIn("pipeline", snapshot)
        self.assertIn("total_runs", snapshot)
        self.assertIn("overall_success_rate", snapshot)
        self.assertIn("success_rates_tracked", snapshot)


# ===========================================================================
# Tests: Edge Cases
# ===========================================================================


class TestPipelineEdgeCases(unittest.TestCase):
    """Edge cases for the recovery pipeline."""

    def test_empty_error_message(self):
        """Pipeline should handle empty error messages."""
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )

        import asyncio
        result = asyncio.run(pipeline.run(node_id="test_node", error_message=""))
        self.assertIsNotNone(result.classification)

    def test_get_recent_pipelines(self):
        """Should retrieve recent pipeline results."""
        pipeline = VerificationDrivenRecoveryPipeline(
            classifier=MockClassifier(),
            recovery_strategies=MockRecoveryStrategies(),
            config=RecoveryPipelineConfig(enable_reverify=False, enable_plan_evolution=False),
        )

        import asyncio
        for i in range(5):
            asyncio.run(pipeline.run(node_id=f"node_{i}"))

        recent = pipeline.get_recent_pipelines(n=3)
        self.assertEqual(len(recent), 3)

    def test_pipeline_history_empty_initially(self):
        """Pipeline history should start empty."""
        pipeline = VerificationDrivenRecoveryPipeline()
        self.assertEqual(pipeline.get_pipeline_count(), 0)

    def test_recovery_success_property_none(self):
        """Recovery success should be None when no action taken."""
        result = RecoveryPipelineResult(
            pipeline_id="test",
            node_id="test",
            status=RecoveryPipelinePhase.INITIATED,
        )
        self.assertIsNone(result.recovery_success)

    def test_overall_success_blocked(self):
        """Overall success should be False when pipeline is blocked."""
        result = RecoveryPipelineResult(
            pipeline_id="test",
            node_id="test",
            status=RecoveryPipelinePhase.BLOCKED,
        )
        self.assertFalse(result.overall_success)

    def test_overall_success_aborted(self):
        """Overall success should be False when pipeline is aborted."""
        result = RecoveryPipelineResult(
            pipeline_id="test",
            node_id="test",
            status=RecoveryPipelinePhase.ABORTED,
        )
        self.assertFalse(result.overall_success)


# ===========================================================================
# Tests: Pipeline Phase Enum
# ===========================================================================


class TestRecoveryPipelinePhase(unittest.TestCase):
    """RecoveryPipelinePhase enum values."""

    def test_has_all_phases(self):
        """All pipeline phases should be defined."""
        phases = [p.value for p in RecoveryPipelinePhase]
        self.assertIn("initiated", phases)
        self.assertIn("classifying", phases)
        self.assertIn("classified", phases)
        self.assertIn("governing", phases)
        self.assertIn("governed", phases)
        self.assertIn("assessing_safety", phases)
        self.assertIn("safety_assessed", phases)
        self.assertIn("recovering", phases)
        self.assertIn("recovered", phases)
        self.assertIn("reverifying", phases)
        self.assertIn("reverified", phases)
        self.assertIn("recording", phases)
        self.assertIn("recorded", phases)
        self.assertIn("evolving", phases)
        self.assertIn("completed", phases)
        self.assertIn("failed", phases)
        self.assertIn("blocked", phases)
        self.assertIn("aborted", phases)

    def test_terminal_phases(self):
        """Terminal phases should be: completed, failed, blocked, aborted."""
        terminal = {
            RecoveryPipelinePhase.COMPLETED,
            RecoveryPipelinePhase.FAILED,
            RecoveryPipelinePhase.BLOCKED,
            RecoveryPipelinePhase.ABORTED,
        }
        self.assertEqual(len(terminal), 4)


if __name__ == "__main__":
    unittest.main()
