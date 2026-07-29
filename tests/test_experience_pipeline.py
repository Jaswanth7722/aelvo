"""tests/test_experience_pipeline.py — Phase 15: Experience Learning Pipeline

Tests the ExperienceLearningPipeline, ExperienceRecord, FailurePattern,
and RetrySuggestion classes.
"""

import time
import pytest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, AsyncMock

from core.execution.tool_registry import (
    ToolExecutionRegistry,
    ToolSpec,
    ToolResult,
    RetryPolicy,
    ToolCategory,
    ExecutionStrategy,
)
from core.execution.experience_pipeline import (
    ExperienceLearningPipeline,
    ExperienceRecord,
    FailurePattern,
    RetrySuggestion,
    ErrorCategory,
    PatternSeverity,
    _classify_error,
    _compute_severity,
    _suggest_policy_for_pattern,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def registry() -> ToolExecutionRegistry:
    reg = ToolExecutionRegistry()
    spec = ToolSpec(name="test_tool", retry_policy=RetryPolicy.NO_RETRY, timeout=10.0)
    async def handler(): return "ok"
    reg.register(spec, handler)
    return reg


@pytest.fixture
def pipeline(registry) -> ExperienceLearningPipeline:
    return ExperienceLearningPipeline(registry=registry)


@pytest.fixture
def success_result() -> ToolResult:
    return ToolResult(
        tool_name="test_tool",
        status="success",
        output="Completed successfully",
        duration_ms=15.0,
    )


@pytest.fixture
def timeout_result() -> ToolResult:
    return ToolResult(
        tool_name="test_tool",
        status="error",
        error="Tool timed out after 10s",
        duration_ms=10000.0,
    )


@pytest.fixture
def connection_result() -> ToolResult:
    return ToolResult(
        tool_name="network_tool",
        status="error",
        error="Connection refused: target host unavailable",
        duration_ms=5000.0,
    )


@pytest.fixture
def rate_limit_result() -> ToolResult:
    return ToolResult(
        tool_name="api_tool",
        status="error",
        error="Rate limit exceeded: too many requests",
        duration_ms=200.0,
    )


# ============================================================================
# Helper Function Tests
# ============================================================================


class TestClassifyError:
    """_classify_error classifies error messages correctly."""

    def test_timeout(self):
        assert _classify_error("timed out after 30s") == ErrorCategory.TIMEOUT
        assert _classify_error("TimeoutError: connection timed out") == ErrorCategory.TIMEOUT

    def test_connection(self):
        assert _classify_error("Connection refused: 127.0.0.1") == ErrorCategory.CONNECTION
        assert _classify_error("Connection reset by peer") == ErrorCategory.CONNECTION
        assert _classify_error("Network error: DNS resolution failed") == ErrorCategory.CONNECTION

    def test_rate_limit(self):
        assert _classify_error("Rate limit exceeded: 429") == ErrorCategory.RATE_LIMIT
        assert _classify_error("Too many requests, try again later") == ErrorCategory.RATE_LIMIT

    def test_permission(self):
        assert _classify_error("Permission denied: /etc/passwd") == ErrorCategory.PERMISSION
        assert _classify_error("Access denied: resource forbidden") == ErrorCategory.PERMISSION

    def test_not_found(self):
        assert _classify_error("File not found: /tmp/missing.txt") == ErrorCategory.NOT_FOUND
        assert _classify_error("No such file or directory") == ErrorCategory.NOT_FOUND

    def test_resource_exhausted(self):
        assert _classify_error("Out of memory: cannot allocate") == ErrorCategory.RESOURCE_EXHAUSTED
        assert _classify_error("Resource temporarily unavailable") == ErrorCategory.RESOURCE_EXHAUSTED

    def test_default_logic_error(self):
        assert _classify_error("Unexpected syntax error at line 42") == ErrorCategory.LOGIC_ERROR
        assert _classify_error("") == ErrorCategory.LOGIC_ERROR

    def test_unknown(self):
        assert _classify_error("Something completely unexpected") == ErrorCategory.LOGIC_ERROR


class TestComputeSeverity:
    """_compute_severity computes severity correctly."""

    def test_critical(self):
        assert _compute_severity(20, 0.2, 5) == PatternSeverity.CRITICAL
        assert _compute_severity(5, 0.2, 3) == PatternSeverity.CRITICAL

    def test_high(self):
        assert _compute_severity(10, 0.4, 2) == PatternSeverity.HIGH
        assert _compute_severity(7, 0.3, 4) == PatternSeverity.HIGH

    def test_medium(self):
        assert _compute_severity(6, 0.6, 4) == PatternSeverity.MEDIUM
        assert _compute_severity(2, 0.8, 3) == PatternSeverity.MEDIUM

    def test_low(self):
        assert _compute_severity(2, 0.8, 1) == PatternSeverity.LOW
        assert _compute_severity(1, 1.0, 1) == PatternSeverity.LOW


class TestSuggestPolicyForPattern:
    """_suggest_policy_for_pattern suggests correct policies."""

    def test_timeout_suggests_retry_on_timeout(self):
        assert _suggest_policy_for_pattern(ErrorCategory.TIMEOUT, 0.0) == RetryPolicy.RETRY_ON_TIMEOUT

    def test_connection_suggests_retry_on_condition(self):
        assert _suggest_policy_for_pattern(ErrorCategory.CONNECTION, 0.0) == RetryPolicy.RETRY_ON_CONDITION

    def test_rate_limit_suggests_retry_on_condition(self):
        assert _suggest_policy_for_pattern(ErrorCategory.RATE_LIMIT, 0.0) == RetryPolicy.RETRY_ON_CONDITION

    def test_permission_suggests_no_retry(self):
        assert _suggest_policy_for_pattern(ErrorCategory.PERMISSION, 0.0) == RetryPolicy.NO_RETRY

    def test_not_found_suggests_no_retry(self):
        assert _suggest_policy_for_pattern(ErrorCategory.NOT_FOUND, 0.0) == RetryPolicy.NO_RETRY

    def test_partial_success_suggests_retry_on_failure(self):
        assert _suggest_policy_for_pattern(ErrorCategory.LOGIC_ERROR, 0.5) == RetryPolicy.RETRY_ON_FAILURE


# ============================================================================
# ExperienceRecord Tests
# ============================================================================


class TestExperienceRecord:
    """ExperienceRecord properties and error_signature."""

    def test_is_success(self, success_result):
        record = ExperienceRecord(
            tool_name="test",
            session_id="s1",
            status="success",
        )
        assert record.is_success is True
        assert record.is_error is False

    def test_is_error(self, timeout_result):
        record = ExperienceRecord(
            tool_name="test",
            session_id="s1",
            status="error",
            error="Timed out",
        )
        assert record.is_success is False
        assert record.is_error is True

    def test_error_signature_timeout(self):
        record = ExperienceRecord(
            tool_name="test",
            session_id="s1",
            status="error",
            error="Tool timed out after 10s",
            error_category=ErrorCategory.TIMEOUT,
        )
        sig = record.error_signature
        assert "timeout" in sig
        assert "timed out" in sig

    def test_error_signature_empty_on_success(self):
        record = ExperienceRecord(
            tool_name="test",
            session_id="s1",
            status="success",
            error="",
        )
        assert record.error_signature == ""

    def test_error_signature_fallback(self):
        record = ExperienceRecord(
            tool_name="test",
            session_id="s1",
            status="error",
            error="Some very unique and specific error message that doesn't match any known patterns",
            error_category=ErrorCategory.LOGIC_ERROR,
        )
        sig = record.error_signature
        assert sig.startswith("logic_error:")
        assert "unique" in sig


# ============================================================================
# FailurePattern Tests
# ============================================================================


class TestFailurePattern:
    """FailurePattern dataclass and to_dict."""

    def test_create_minimal(self):
        pattern = FailurePattern(error_signature="timeout:timed out")
        assert pattern.error_signature == "timeout:timed out"
        assert pattern.severity == PatternSeverity.LOW
        assert pattern.occurrence_count == 0

    def test_to_dict(self):
        pattern = FailurePattern(
            error_signature="timeout:timed out",
            error_category=ErrorCategory.TIMEOUT,
            tool_names={"tool_a", "tool_b"},
            occurrence_count=10,
            success_rate=0.5,
            severity=PatternSeverity.HIGH,
            suggested_retry_policy=RetryPolicy.RETRY_ON_TIMEOUT,
        )
        d = pattern.to_dict()
        assert d["error_category"] == "timeout"
        assert d["occurrence_count"] == 10
        assert d["severity"] == "high"
        assert d["suggested_retry_policy"] == "retry_on_timeout"


# ============================================================================
# RetrySuggestion Tests
# ============================================================================


class TestRetrySuggestion:
    """RetrySuggestion dataclass and to_dict."""

    def test_create(self):
        suggestion = RetrySuggestion(
            tool_name="test_tool",
            current_policy=RetryPolicy.NO_RETRY,
            suggested_policy=RetryPolicy.RETRY_ON_FAILURE,
            confidence=0.8,
            evidence="timeout(5x), connection(3x)",
            failure_count=8,
            success_rate=0.3,
            reason="Too many failures, retry would help",
        )
        assert suggestion.tool_name == "test_tool"
        assert suggestion.confidence == 0.8

    def test_to_dict(self):
        suggestion = RetrySuggestion(
            tool_name="test",
            current_policy=RetryPolicy.NO_RETRY,
            suggested_policy=RetryPolicy.RETRY_ON_FAILURE,
            confidence=0.75,
        )
        d = suggestion.to_dict()
        assert d["tool_name"] == "test"
        assert d["current_policy"] == "no_retry"
        assert d["suggested_policy"] == "retry_on_failure"
        assert d["confidence"] == 0.75


# ============================================================================
# ExperienceLearningPipeline Tests
# ============================================================================


class TestExperienceLearningPipeline:
    """ExperienceLearningPipeline ingestion and analysis."""

    def test_initialize(self, registry):
        """Pipeline initializes with registry."""
        pipeline = ExperienceLearningPipeline(registry=registry)
        summary = pipeline.get_experience_summary()
        assert summary["total_executions"] == 0
        assert summary["patterns_detected"] == 0

    def test_initialize_without_registry(self):
        """Pipeline works without a registry."""
        pipeline = ExperienceLearningPipeline()
        summary = pipeline.get_experience_summary()
        assert summary["total_executions"] == 0

    def test_record_execution_success(self, pipeline, success_result):
        """Record a successful execution."""
        record = pipeline.record_execution(success_result, session_id="s1")
        assert record.is_success
        assert record.session_id == "s1"
        assert record.error_category == ErrorCategory.UNKNOWN

        summary = pipeline.get_experience_summary()
        assert summary["total_executions"] == 1
        assert summary["success_rate"] == 1.0

    def test_record_execution_error(self, pipeline, timeout_result):
        """Record a failed execution with error classification."""
        record = pipeline.record_execution(timeout_result, session_id="s1")
        assert record.is_error
        assert record.error_category == ErrorCategory.TIMEOUT

        summary = pipeline.get_experience_summary()
        assert summary["total_executions"] == 1
        assert summary["success_rate"] == 0.0

    def test_record_multiple_executions(self, pipeline, success_result, timeout_result):
        """Multiple executions are tracked correctly."""
        pipeline.record_execution(success_result, session_id="s1")
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_execution(success_result, session_id="s1")

        summary = pipeline.get_experience_summary()
        assert summary["total_executions"] == 3
        assert summary["success_rate"] == pytest.approx(2 / 3, rel=1e-3)

    def test_record_rollback(self, pipeline, success_result):
        """Rollback events are recorded."""
        pipeline.record_execution(success_result, session_id="s1")
        pipeline.record_rollback(
            session_id="s1",
            checkpoint_id="ckpt_1",
            reason="Verification failed",
            tool_name="test_tool",
        )

        summary = pipeline.get_experience_summary()
        assert summary["total_rollbacks"] == 1

    def test_record_rollback_marks_execution(self, pipeline, timeout_result):
        """Rollback marks the matching execution as rollback-triggering."""
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_rollback(session_id="s1", reason="failed", tool_name="test_tool")

        records = pipeline._tool_records.get("test_tool", [])
        assert len(records) == 1
        assert records[0].rollback_triggered is True

    def test_analyze_failure_patterns_timeout(self, pipeline, timeout_result):
        """Timeout failures cluster into a timeout pattern."""
        for _ in range(5):
            pipeline.record_execution(timeout_result, session_id="s1")

        patterns = pipeline.analyze_failure_patterns(min_occurrences=2)
        assert len(patterns) >= 1

        timeout_patterns = [p for p in patterns if p.error_category == ErrorCategory.TIMEOUT]
        assert len(timeout_patterns) >= 1
        assert timeout_patterns[0].occurrence_count >= 5

    def test_analyze_failure_patterns_connection(self, pipeline, connection_result):
        """Connection failures cluster into a connection pattern."""
        for _ in range(3):
            pipeline.record_execution(connection_result, session_id="s2")

        patterns = pipeline.analyze_failure_patterns(min_occurrences=2)
        conn_patterns = [p for p in patterns if p.error_category == ErrorCategory.CONNECTION]
        assert len(conn_patterns) >= 1

    def test_analyze_failure_patterns_mixed(self, pipeline, timeout_result, connection_result):
        """Multiple error types produce separate patterns."""
        for _ in range(4):
            pipeline.record_execution(timeout_result, session_id="s1")
        for _ in range(3):
            pipeline.record_execution(connection_result, session_id="s1")

        patterns = pipeline.analyze_failure_patterns(min_occurrences=2)
        categories = set(p.error_category for p in patterns)
        assert ErrorCategory.TIMEOUT in categories
        assert ErrorCategory.CONNECTION in categories

    def test_analyze_failure_patterns_min_occurrences(self, pipeline, timeout_result):
        """min_occurrences filters out infrequent failures."""
        pipeline.record_execution(timeout_result, session_id="s1")

        patterns = pipeline.analyze_failure_patterns(min_occurrences=3)
        assert len(patterns) == 0

        patterns = pipeline.analyze_failure_patterns(min_occurrences=1)
        assert len(patterns) >= 1

    def test_suggest_retry_policies_timeout(self, pipeline, timeout_result):
        """Timeout failures suggest RETRY_ON_TIMEOUT policy."""
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_execution(timeout_result, session_id="s1")

        suggestions = pipeline.suggest_retry_policies(min_confidence=0.1)
        timeout_suggestions = [
            s for s in suggestions
            if s.suggested_policy == RetryPolicy.RETRY_ON_TIMEOUT
        ]
        assert len(timeout_suggestions) >= 1

    def test_suggest_retry_policies_connection(self, pipeline, connection_result):
        """Connection failures suggest RETRY_ON_CONDITION policy."""
        for _ in range(5):
            pipeline.record_execution(connection_result, session_id="s1")

        suggestions = pipeline.suggest_retry_policies(min_confidence=0.1)
        conn_suggestions = [
            s for s in suggestions
            if s.suggested_policy == RetryPolicy.RETRY_ON_CONDITION
        ]
        assert len(conn_suggestions) >= 1

    def test_suggest_retry_policies_skips_permission(self, pipeline):
        """Permission failures do NOT suggest retrying (NO_RETRY)."""
        perm_result = ToolResult(
            tool_name="test_tool",
            status="error",
            error="Permission denied: /etc/config",
        )
        for _ in range(3):
            pipeline.record_execution(perm_result, session_id="s1")

        suggestions = pipeline.suggest_retry_policies(min_confidence=0.1)
        # Current policy is already NO_RETRY, so no suggestion would be made
        # (because best_policy would equal current_policy)
        perm_suggestions = [
            s for s in suggestions
            if s.tool_name == "test_tool" and s.suggested_policy == RetryPolicy.NO_RETRY
        ]
        assert len(perm_suggestions) == 0

    def test_suggest_retry_policies_skips_same_policy(self, pipeline, registry):
        """Skipped when suggested policy matches current policy."""
        # Register a tool already using RETRY_ON_FAILURE
        spec = ToolSpec(
            name="already_retry",
            retry_policy=RetryPolicy.RETRY_ON_FAILURE,
        )
        async def handler(): return "ok"
        registry.register(spec, handler)

        # Feed some failures that would suggest RETRY_ON_FAILURE
        fail_result = ToolResult(
            tool_name="already_retry",
            status="error",
            error="Temporary error, please retry",
        )
        for _ in range(3):
            pipeline.record_execution(fail_result, session_id="s1")

        suggestions = pipeline.suggest_retry_policies(min_confidence=0.1)
        already_suggestions = [s for s in suggestions if s.tool_name == "already_retry"]
        assert len(already_suggestions) == 0  # Skipped because same policy

    def test_suggest_retry_policies_min_confidence(self, pipeline, timeout_result):
        """min_confidence filters out low-confidence suggestions."""
        for _ in range(3):
            pipeline.record_execution(timeout_result, session_id="s1")

        suggestions_high = pipeline.suggest_retry_policies(min_confidence=0.9)
        suggestions_low = pipeline.suggest_retry_policies(min_confidence=0.0)

        assert len(suggestions_high) <= len(suggestions_low)

    def test_get_tool_learning_curve(self, pipeline, success_result, timeout_result):
        """get_tool_learning_curve returns per-tool stats."""
        pipeline.record_execution(success_result, session_id="s1")
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_execution(success_result, session_id="s1")

        curve = pipeline.get_tool_learning_curve("test_tool")
        assert curve["tool_name"] == "test_tool"
        assert curve["total"] == 3
        assert curve["success_rate"] == pytest.approx(2 / 3, rel=1e-3)

    def test_get_tool_learning_curve_no_data(self, pipeline):
        """get_tool_learning_curve returns empty for unknown tool."""
        curve = pipeline.get_tool_learning_curve("unknown_tool")
        assert curve["tool_name"] == "unknown_tool"
        assert curve["total"] == 0

    def test_get_failure_patterns_filtered(self, pipeline, timeout_result):
        """get_failure_patterns filters by severity."""
        for _ in range(5):
            pipeline.record_execution(timeout_result, session_id="s1")

        # 5 timeouts with 0% success rate → HIGH severity (since occurrence >= 5)
        high_patterns = pipeline.get_failure_patterns(min_severity=PatternSeverity.HIGH)
        low_patterns = pipeline.get_failure_patterns(min_severity=PatternSeverity.LOW)

        assert len(high_patterns) >= 1
        assert len(low_patterns) >= len(high_patterns)

    def test_get_retry_suggestions_filtered(self, pipeline, timeout_result):
        """get_retry_suggestions filters by confidence."""
        for _ in range(3):
            pipeline.record_execution(timeout_result, session_id="s1")

        all_suggestions = pipeline.get_retry_suggestions(min_confidence=0.0)
        filtered = pipeline.get_retry_suggestions(min_confidence=0.9)

        assert len(filtered) <= len(all_suggestions)

    def test_multiple_sessions(self, pipeline, timeout_result, success_result):
        """Records across sessions are tracked properly."""
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_execution(success_result, session_id="s2")
        pipeline.record_execution(timeout_result, session_id="s3")

        summary = pipeline.get_experience_summary()
        assert summary["total_sessions"] == 3

    def test_ingest_registry_history(self, pipeline):
        """ingest_registry_history pulls from registry."""
        # Manually add history to the registry
        pipeline._registry._record_history(ToolResult(
            tool_name="test_tool", status="success", output="ok",
        ))
        pipeline._registry._record_history(ToolResult(
            tool_name="test_tool", status="error", error="timed out",
        ))

        count = pipeline.ingest_registry_history(limit=10)
        assert count == 2

        summary = pipeline.get_experience_summary()
        assert summary["total_executions"] == 2

    def test_ingest_registry_history_no_registry(self):
        """ingest_registry_history returns 0 without registry."""
        pipeline = ExperienceLearningPipeline()
        assert pipeline.ingest_registry_history() == 0

    def test_reset(self, pipeline, success_result):
        """Reset clears all data."""
        pipeline.record_execution(success_result, session_id="s1")
        assert pipeline.get_experience_summary()["total_executions"] == 1

        pipeline.reset()
        assert pipeline.get_experience_summary()["total_executions"] == 0

    def test_snapshot(self, pipeline, timeout_result):
        """Snapshot returns full state."""
        pipeline.record_execution(timeout_result, session_id="s1")
        snap = pipeline.snapshot()
        assert "summary" in snap
        assert "failure_patterns" in snap
        assert "retry_suggestions" in snap

    def test_to_terminal_display(self, pipeline, timeout_result):
        """to_terminal_display returns human-readable output."""
        pipeline.record_execution(timeout_result, session_id="s1")
        display = pipeline.to_terminal_display()
        assert "EXPERIENCE LEARNING PIPELINE" in display

    def test_caching_patterns(self, pipeline, timeout_result):
        """Patterns are cached and recomputed on new data."""
        pipeline.record_execution(timeout_result, session_id="s1")
        p1 = pipeline.analyze_failure_patterns()
        # Cache hit
        p2 = pipeline.analyze_failure_patterns()
        assert len(p1) == len(p2)

        # New data invalidates cache
        pipeline.record_execution(timeout_result, session_id="s1")
        p3 = pipeline.analyze_failure_patterns()
        # At least one pattern should remain
        assert len(p3) >= 1

    def test_severity_critical(self, pipeline):
        """Many failures with low success rate → CRITICAL severity."""
        fail = ToolResult(tool_name="test_tool", status="error", error="Broke")
        for _ in range(25):
            pipeline.record_execution(fail, session_id="s1")

        patterns = pipeline.analyze_failure_patterns(min_occurrences=5)
        critical = [p for p in patterns if p.severity == PatternSeverity.CRITICAL]
        assert len(critical) >= 1

    def test_rollback_rate_calculation(self, pipeline, success_result, timeout_result):
        """Rollback rate is calculated correctly."""
        pipeline.record_execution(success_result, session_id="s1")
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_execution(timeout_result, session_id="s1")
        pipeline.record_rollback(session_id="s1", reason="test")

        summary = pipeline.get_experience_summary()
        assert summary["rollback_rate"] == pytest.approx(1 / 3, rel=1e-3)

    def test_analytics_integration(self):
        """Pipeline integrates with AnalyticsEngine."""
        analytics = MagicMock()
        pipeline = ExperienceLearningPipeline(analytics_engine=analytics)

        result = ToolResult(
            tool_name="test", status="success", output="ok",
        )
        pipeline.record_execution(result, session_id="s1")

        # Should have called record_first_attempt on the analytics
        analytics.record_first_attempt.assert_called_once()

    def test_analytics_integration_no_session(self, success_result):
        """No analytics call when session_id is empty."""
        analytics = MagicMock()
        pipeline = ExperienceLearningPipeline(analytics_engine=analytics)

        pipeline.record_execution(success_result, session_id="")
        analytics.record_first_attempt.assert_not_called()

    def test_analytics_integration_retry_attempt(self, success_result):
        """No analytics call on retry attempts (not first attempt)."""
        analytics = MagicMock()
        pipeline = ExperienceLearningPipeline(analytics_engine=analytics)

        # retry_attempt > 0 means it's not a first attempt
        result = ToolResult(
            tool_name="test", status="success", output="ok", retry_attempt=2,
        )
        pipeline.record_execution(result, session_id="s1")
        analytics.record_first_attempt.assert_not_called()

    def test_analytics_exception_handling(self, success_result):
        """Analytics exception doesn't break pipeline."""
        analytics = MagicMock()
        analytics.record_first_attempt.side_effect = RuntimeError("Analytics down")
        pipeline = ExperienceLearningPipeline(analytics_engine=analytics)

        # Should not raise
        pipeline.record_execution(success_result, session_id="s1")
        assert pipeline.get_experience_summary()["total_executions"] == 1

    def test_history_cap(self):
        """History is capped at max_history."""
        pipeline = ExperienceLearningPipeline(max_history=5)
        for i in range(10):
            result = ToolResult(
                tool_name="test",
                status="success" if i % 2 == 0 else "error",
                output=f"result_{i}",
            )
            pipeline.record_execution(result, session_id="s1")

        assert len(pipeline._records) == 5  # Capped at 5

    def test_ingest_registry_statistics(self, pipeline):
        """ingest_registry_statistics returns tool stats."""
        # Add some history to the registry first
        pipeline._registry._record_history(ToolResult(
            tool_name="test_tool", status="success", output="ok",
        ))
        stats = pipeline.ingest_registry_statistics()
        assert "test_tool" in stats

    def test_rollback_without_tool_name(self, pipeline, success_result):
        """Rollback without tool_name still works."""
        pipeline.record_execution(success_result, session_id="s1")
        pipeline.record_rollback(session_id="s1", reason="generic failure")

        summary = pipeline.get_experience_summary()
        assert summary["total_rollbacks"] == 1

    def test_tool_confidence_computation(self, pipeline, success_result, timeout_result):
        """_compute_tool_confidence returns reasonable values."""
        # All successes
        for _ in range(10):
            pipeline.record_execution(success_result, session_id="s1")
        assert pipeline._compute_tool_confidence("test_tool") > 0.9

        # Mix of success/failure
        pipeline = ExperienceLearningPipeline()
        for _ in range(5):
            pipeline.record_execution(success_result, session_id="s1")
        for _ in range(5):
            pipeline.record_execution(timeout_result, session_id="s1")
        assert 0.3 < pipeline._compute_tool_confidence("test_tool") < 0.8

    def test_empty_display(self):
        """to_terminal_display works with no data."""
        pipeline = ExperienceLearningPipeline()
        display = pipeline.to_terminal_display()
        assert "EXPERIENCE LEARNING PIPELINE" in display
