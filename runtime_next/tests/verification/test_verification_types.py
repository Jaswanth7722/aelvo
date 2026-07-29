"""Tests for Layer 1 — Verification Type System."""

import pytest
from pydantic import ValidationError


class TestVerificationTypes:
    """Verify the complete type system works correctly."""

    def test_verification_type_enum(self):
        from runtime_next.verification.types import VerificationType
        assert VerificationType.LINT.value == "lint"
        assert VerificationType.TYPECHECK.value == "typecheck"
        assert VerificationType.UNIT_TEST.value == "unit_test"
        assert VerificationType.GRAPH_CONSISTENCY.value == "graph_consistency"
        assert VerificationType.REPLAY_CONSISTENCY.value == "replay_consistency"

    def test_failure_classification_enum(self):
        from runtime_next.verification.types import FailureClassification
        assert FailureClassification.SYNTAX_ERROR.value == "syntax_error"
        assert FailureClassification.UNKNOWN_FAILURE.value == "unknown_failure"
        # Unknown failures are NEVER silently retried
        assert FailureClassification.UNKNOWN_FAILURE != FailureClassification.TIMEOUT

    def test_verification_result_immutable(self):
        from runtime_next.verification.types import VerificationResult, VerificationType
        result = VerificationResult(
            verification_id="v_test_001",
            node_id="node_001",
            verification_type=VerificationType.LINT,
            success=True,
        )
        with pytest.raises(ValidationError):
            result.success = False

    def test_verification_result_minimal(self):
        from runtime_next.verification.types import VerificationResult, VerificationType, Confidence
        result = VerificationResult(
            verification_id="v_test_001",
            node_id="node_001",
            verification_type=VerificationType.LINT,
            success=True,
        )
        assert result.verification_id == "v_test_001"
        assert result.node_id == "node_001"
        assert result.success is True
        assert result.confidence == Confidence.HIGH  # default
        assert result.diagnostics == []
        assert result.artifacts == {}

    def test_verification_result_with_diagnostics(self):
        from runtime_next.verification.types import VerificationResult, VerificationType, Severity
        result = VerificationResult(
            verification_id="v_test_002",
            node_id="node_001",
            verification_type=VerificationType.SECURITY_SCAN,
            success=False,
            severity=Severity.CRITICAL,
            diagnostics=["Security vulnerability found in auth module"],
            affected_files=["auth.py"],
        )
        assert result.success is False
        assert result.severity == Severity.CRITICAL
        assert "auth.py" in result.affected_files

    def test_verification_scope_full_project(self):
        from runtime_next.verification.types import VerificationScope
        scope = VerificationScope.full_project()
        assert scope.is_minimal is False
        assert scope.provenance == "full_project_fallback"

    def test_verification_scope_empty(self):
        from runtime_next.verification.types import VerificationScope
        scope = VerificationScope.empty()
        assert scope.is_empty() is True

    def test_verification_scope_custom(self):
        from runtime_next.verification.types import VerificationScope
        scope = VerificationScope(
            affected_files=["src/main.py", "src/utils.py"],
            affected_symbols=["AuthService", "authenticate"],
            affected_tests=["tests/test_auth.py"],
        )
        assert len(scope.affected_files) == 2
        assert "AuthService" in scope.affected_symbols
        assert not scope.is_empty()

    def test_classification_result_unknown(self):
        from runtime_next.verification.types import (
            ClassificationResult, FailureClassification, Confidence,
        )
        result = ClassificationResult(
            primary=FailureClassification.UNKNOWN_FAILURE,
            confidence=Confidence.HIGH,
            confidence_score=0.85,
            evidence={},
        )
        assert result.is_unknown() is True

    def test_classification_result_known(self):
        from runtime_next.verification.types import (
            ClassificationResult, FailureClassification, Confidence,
        )
        result = ClassificationResult(
            primary=FailureClassification.SYNTAX_ERROR,
            confidence=Confidence.CERTAIN,
            confidence_score=0.95,
            evidence={"pattern": "SyntaxError found"},
        )
        assert result.is_unknown() is False

    def test_classification_with_alternatives(self):
        from runtime_next.verification.types import (
            ClassificationResult, FailureClassification, Confidence,
        )
        result = ClassificationResult(
            primary=FailureClassification.SYNTAX_ERROR,
            confidence=Confidence.HIGH,
            confidence_score=0.82,
            evidence={"pattern": "invalid syntax"},
            alternatives=[FailureClassification.ENVIRONMENT_FAILURE],
            alternative_scores={FailureClassification.ENVIRONMENT_FAILURE: 0.15},
        )
        assert len(result.alternatives) == 1
        assert result.alternative_scores[FailureClassification.ENVIRONMENT_FAILURE] == 0.15

    def test_recovery_strategy_requires_approval(self):
        from runtime_next.verification.types import RecoveryStrategy, FailureClassification
        strategy = RecoveryStrategy(
            id="strat_test",
            name="Destructive rollback",
            failure_type=FailureClassification.SERIALIZATION_FAILURE,
            danger_level="destructive",
            requires_user_approval=True,
        )
        assert strategy.requires_approval() is True

    def test_recovery_strategy_safe(self):
        from runtime_next.verification.types import RecoveryStrategy, FailureClassification
        strategy = RecoveryStrategy(
            id="strat_safe",
            name="Safe retry",
            failure_type=FailureClassification.TIMEOUT,
            danger_level="safe",
            max_retries=3,
        )
        assert strategy.requires_approval() is False

    def test_recovery_action_record(self):
        from runtime_next.verification.types import RecoveryAction, FailureClassification
        action = RecoveryAction(
            id="action_001",
            strategy_id="strat_syntax",
            node_id="node_001",
            failure_classification=FailureClassification.SYNTAX_ERROR,
            action_type="retry",
            description="Retry with diagnostics",
            success=True,
        )
        assert action.action_type == "retry"
        assert action.success is True
        assert action.injected_node_id is None

    def test_retry_decision_blocked(self):
        from runtime_next.verification.types import RetryDecision
        decision = RetryDecision(
            can_retry=False,
            reason="Unknown failure — retry forbidden",
            retry_count=1,
            blocking_condition="unknown_failure",
        )
        assert decision.can_retry is False
        assert decision.blocking_condition == "unknown_failure"

    def test_retry_decision_safe(self):
        from runtime_next.verification.types import RetryDecision
        decision = RetryDecision(
            can_retry=True,
            reason="All checks passed",
            suggested_backoff=2.0,
            graph_consistent=True,
            capability_valid=True,
            mutation_safe=True,
            replay_divergence_risk=0.1,
        )
        assert decision.can_retry is True

    def test_governance_decision_stop_autonomy(self):
        from runtime_next.verification.types import GovernanceDecision
        decision = GovernanceDecision(
            verdict="abort",
            reason="Unknown failure",
            danger_assessment="destructive",
            requires_user_intervention=True,
        )
        assert decision.should_stop_autonomy() is True

    def test_governance_decision_auto(self):
        from runtime_next.verification.types import GovernanceDecision
        decision = GovernanceDecision(
            verdict="auto_recover",
            reason="Safe retry",
        )
        assert decision.should_stop_autonomy() is False

    def test_consistency_result_violations(self):
        from runtime_next.verification.types import ConsistencyResult
        result = ConsistencyResult(
            is_consistent=False,
            checks_performed=["graph_integrity"],
            violations=[{"check": "graph_integrity", "detail": "Node count mismatch"}],
            graph_integrity=False,
        )
        assert result.is_consistent is False
        assert len(result.violations) == 1

    def test_verification_manifest(self):
        from runtime_next.verification.types import VerificationManifest, VerificationType
        manifest = VerificationManifest(
            required=[VerificationType.LINT, VerificationType.TYPECHECK],
            blocking=[VerificationType.LINT],
            optional=[VerificationType.UNIT_TEST],
        )
        assert len(manifest.required) == 2
        assert VerificationType.LINT in manifest.blocking

    def test_classify_exit_code(self):
        from runtime_next.verification.types import classify_exit_code, FailureClassification
        assert classify_exit_code(0) is None
        assert classify_exit_code(127) == FailureClassification.DEPENDENCY_MISSING
        assert classify_exit_code(126) == FailureClassification.PERMISSION_DENIED
        assert classify_exit_code(1) is None  # Generic error
        assert classify_exit_code(139) == FailureClassification.ENVIRONMENT_FAILURE
        assert classify_exit_code(999) == FailureClassification.UNKNOWN_FAILURE

    def test_default_recovery_map(self):
        from runtime_next.verification.types import DEFAULT_RECOVERY_MAP, FailureClassification
        assert FailureClassification.SYNTAX_ERROR in DEFAULT_RECOVERY_MAP
        assert FailureClassification.UNKNOWN_FAILURE in DEFAULT_RECOVERY_MAP
        assert DEFAULT_RECOVERY_MAP[FailureClassification.UNKNOWN_FAILURE] == "abort_and_notify"


class TestVerificationEvents:
    """Tests for verification event models."""

    def test_verification_started_event(self):
        from runtime_next.verification.events import VerificationStartedEvent
        from runtime_next.verification.types import VerificationType
        event = VerificationStartedEvent(
            event_id="ve_start_001",
            node_id="node_001",
            verification_type=VerificationType.LINT,
        )
        assert event.node_id == "node_001"
        assert event.verification_type == VerificationType.LINT

    def test_verification_completed_event(self):
        from runtime_next.verification.events import VerificationCompletedEvent
        from runtime_next.verification.types import VerificationResult, VerificationType, Confidence
        result = VerificationResult(
            verification_id="v_001",
            node_id="n_001",
            verification_type=VerificationType.LINT,
            success=True,
            confidence=Confidence.HIGH,
        )
        event = VerificationCompletedEvent(
            event_id="ve_done_001",
            node_id="n_001",
            verification_type=VerificationType.LINT,
            result=result,
        )
        assert event.result.success is True

    def test_retry_blocked_event(self):
        from runtime_next.verification.events import RetryBlockedEvent
        from runtime_next.verification.types import RetryDecision, FailureClassification
        decision = RetryDecision(
            can_retry=False,
            reason="Graph inconsistent",
            blocking_condition="graph_inconsistency",
        )
        event = RetryBlockedEvent(
            event_id="rb_001",
            node_id="n_001",
            classification=FailureClassification.GRAPH_INCONSISTENCY,
            decision=decision,
            retry_attempt=3,
        )
        assert event.retry_attempt == 3
        assert event.decision.can_retry is False

    def test_replay_divergence_event(self):
        from runtime_next.verification.events import ReplayDivergenceEvent
        event = ReplayDivergenceEvent(
            event_id="rd_001",
            expected_hash="abc123",
            actual_hash="def456",
            divergent_nodes=["node_001", "node_002"],
        )
        assert event.expected_hash == "abc123"
        assert event.actual_hash == "def456"
        assert len(event.divergent_nodes) == 2
