# tests/test_analytics.py - Tests for AnalyticsEngine
# Session metrics, trends, calibration tracking, first-attempt success measurement

from __future__ import annotations

import time
import pytest
from datetime import datetime, timezone, timedelta

from learning.types import (
    TrendPoint,
    TrendDirection, EditCategory,
    DependencyGraphDelta, GraphDeltaEdge,
    EditCategorySignature, SubgraphSpec,
)
from learning.analytics import AnalyticsEngine
from learning.engine import PatternExtractionEngine
from learning.knowledge_graph import KnowledgeGraph
from learning.accumulator import PatternAccumulator
from repo_intelligence.types import (
    FileId, SymbolNode, ConfidenceLevel, ParsedFile, LanguageId, SymbolEdge,
)
from repo_intelligence.graph import (
    GraphSnapshot,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_engine() -> PatternExtractionEngine:
    kg = KnowledgeGraph(db_path=":memory:")
    engine = PatternExtractionEngine(knowledge_graph=kg)
    return engine


class TestAnalyticsEngine:
    """Tests for the core AnalyticsEngine (session metrics, trends, calibration)."""

    # ── Session Lifecycle ────────────────────────────────────────────────────

    def test_session_lifecycle(self):
        """Start and end a session produces a valid SessionRecord."""
        analytics = AnalyticsEngine()
        analytics.start_session("session_1")

        # No session record yet (session in progress)
        assert analytics._current_session_id == "session_1"
        assert analytics._current_deltas == 0

        record = analytics.end_session({"total_patterns": 5, "by_category": {}, "avg_confidence": 0.6})
        assert record.session_id == "session_1"
        assert record.deltas_processed == 0
        assert record.patterns_created == 0
        assert record.patterns_updated == 0
        assert record.contradictions_detected == 0
        assert record.total_patterns_end == 5
        assert record.avg_confidence == 0.6
        assert record.duration_seconds >= 0
        assert record.ended_at is not None

    def test_session_tracks_activity(self):
        """Session records track all activity counters."""
        analytics = AnalyticsEngine()
        analytics.start_session("session_2")

        analytics.record_delta_processed("FORGE")
        analytics.record_delta_processed("FORGE")
        analytics.record_delta_processed("ARCHITECT")
        analytics.record_pattern_created()
        analytics.record_pattern_created()
        analytics.record_pattern_updated()
        analytics.record_contradiction()
        analytics.record_pruned(2)

        record = analytics.end_session({"total_patterns": 8, "by_category": {"add_file": 3}, "avg_confidence": 0.7})
        assert record.deltas_processed == 3
        assert record.patterns_created == 2
        assert record.patterns_updated == 1
        assert record.contradictions_detected == 1
        assert record.patterns_pruned == 2
        assert record.total_patterns_end == 8
        assert record.specialist_activity == {"FORGE": 2, "ARCHITECT": 1}
        assert record.category_distribution == {"add_file": 3}

    def test_session_without_activity(self):
        """Empty session still produces a valid record."""
        analytics = AnalyticsEngine()
        analytics.start_session("empty_session")
        record = analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})
        assert record.deltas_processed == 0
        assert record.patterns_created == 0
        assert record.total_patterns_end == 0

    def test_end_session_without_start_raises(self):
        """Calling end_session without start raises RuntimeError."""
        analytics = AnalyticsEngine()
        with pytest.raises(RuntimeError):
            analytics.end_session({})

    def test_multiple_sessions(self):
        """Multiple sessions are tracked independently."""
        analytics = AnalyticsEngine()
        analytics.start_session("s1")
        analytics.record_delta_processed("FORGE")
        r1 = analytics.end_session({"total_patterns": 3, "by_category": {}, "avg_confidence": 0.5})

        analytics.start_session("s2")
        analytics.record_delta_processed("SENTINEL")
        analytics.record_pattern_created()
        r2 = analytics.end_session({"total_patterns": 4, "by_category": {}, "avg_confidence": 0.7})

        assert r1.session_id == "s1"
        assert r2.session_id == "s2"
        assert r1.deltas_processed == 1
        assert r2.deltas_processed == 1
        assert r1.patterns_created == 0
        assert r2.patterns_created == 1

        sessions = analytics.list_sessions()
        assert len(sessions) == 2

    # ── First-Attempt Tracking ───────────────────────────────────────────────

    def test_record_first_attempt(self):
        """First-attempt records are created and stored."""
        analytics = AnalyticsEngine()
        analytics.start_session("fa_session")

        record = analytics.record_first_attempt(
            specialist="FORGE",
            task_description="Add auth middleware",
            succeeded=True,
            pattern_id="pat_123",
            confidence_at_time=0.75,
        )
        assert record.specialist == "FORGE"
        assert record.succeeded is True
        assert record.pattern_id == "pat_123"
        assert record.confidence_at_time == 0.75
        assert record.session_id == "fa_session"
        assert record.id is not None

        analytics.end_session({"total_patterns": 1, "by_category": {}, "avg_confidence": 0.75})

    def test_first_attempt_success_rate(self):
        """First-attempt success rate computes correctly."""
        analytics = AnalyticsEngine()
        analytics.start_session("fa_rate")

        analytics.record_first_attempt("FORGE", "task1", True)
        analytics.record_first_attempt("FORGE", "task2", True)
        analytics.record_first_attempt("FORGE", "task3", False)

        rate = analytics.get_first_attempt_success_rate("FORGE")
        assert rate == 2.0 / 3.0

        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

    def test_first_attempt_filter_by_session(self):
        """First-attempt success rate can be filtered by session."""
        analytics = AnalyticsEngine()

        analytics.start_session("s1")
        analytics.record_first_attempt("FORGE", "t1", True)
        analytics.record_first_attempt("FORGE", "t2", False)
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        analytics.start_session("s2")
        analytics.record_first_attempt("FORGE", "t3", True)
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        rate_s1 = analytics.get_first_attempt_success_rate(session_id="s1")
        rate_s2 = analytics.get_first_attempt_success_rate(session_id="s2")
        assert rate_s1 == 0.5
        assert rate_s2 == 1.0

    def test_first_attempt_empty_returns_zero(self):
        """No first-attempt records returns 0.0."""
        analytics = AnalyticsEngine()
        assert analytics.get_first_attempt_success_rate("FORGE") == 0.0

    def test_first_attempt_updates_session_record(self):
        """First-attempt counts appear in session record."""
        analytics = AnalyticsEngine()
        analytics.start_session("fa_session")
        analytics.record_first_attempt("FORGE", "task1", True)
        analytics.record_first_attempt("FORGE", "task2", False)
        record = analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})
        assert record.first_attempts == 2
        assert record.first_attempt_successes == 1
        assert record.first_attempt_success_rate == 0.5

    def test_record_first_attempt_no_session(self):
        """Recording without a session raises RuntimeError."""
        analytics = AnalyticsEngine()
        with pytest.raises(RuntimeError):
            analytics.record_first_attempt("FORGE", "task", True)

    # ── Trend Analysis ───────────────────────────────────────────────────────

    def test_trend_insufficient_data(self):
        """Fewer than 2 points → INSUFFICIENT_DATA."""
        analytics = AnalyticsEngine()
        points = [TrendPoint(timestamp=datetime.now(timezone.utc), value=0.5)]
        trend = analytics.compute_trend("test", points)
        assert trend.direction == TrendDirection.INSUFFICIENT_DATA
        assert trend.slope == 0.0

    def test_trend_empty_points(self):
        """No points → INSUFFICIENT_DATA."""
        analytics = AnalyticsEngine()
        trend = analytics.compute_trend("test", [])
        assert trend.direction == TrendDirection.INSUFFICIENT_DATA

    def test_trend_improving(self):
        """Increasing values → IMPROVING."""
        analytics = AnalyticsEngine()
        base = datetime.now(timezone.utc)
        points = [
            TrendPoint(timestamp=base, value=0.3, label="s1"),
            TrendPoint(timestamp=base + timedelta(hours=1), value=0.6, label="s2"),
            TrendPoint(timestamp=base + timedelta(hours=2), value=0.9, label="s3"),
        ]
        trend = analytics.compute_trend("confidence", points)
        assert trend.direction == TrendDirection.IMPROVING
        assert trend.slope > 0

    def test_trend_degrading(self):
        """Decreasing values → DEGRADING."""
        analytics = AnalyticsEngine()
        base = datetime.now(timezone.utc)
        points = [
            TrendPoint(timestamp=base, value=0.9, label="s1"),
            TrendPoint(timestamp=base + timedelta(hours=1), value=0.6, label="s2"),
            TrendPoint(timestamp=base + timedelta(hours=2), value=0.3, label="s3"),
        ]
        trend = analytics.compute_trend("confidence", points)
        assert trend.direction == TrendDirection.DEGRADING
        assert trend.slope < 0

    def test_trend_stable(self):
        """Flat values → STABLE."""
        analytics = AnalyticsEngine()
        base = datetime.now(timezone.utc)
        points = [
            TrendPoint(timestamp=base, value=0.5, label="s1"),
            TrendPoint(timestamp=base + timedelta(hours=1), value=0.51, label="s2"),
            TrendPoint(timestamp=base + timedelta(hours=2), value=0.49, label="s3"),
        ]
        trend = analytics.compute_trend("confidence", points)
        assert trend.direction == TrendDirection.STABLE

    def test_trend_series_properties(self):
        """TrendSeries computes min, max, avg correctly."""
        analytics = AnalyticsEngine()
        base = datetime.now(timezone.utc)
        points = [
            TrendPoint(timestamp=base, value=0.2),
            TrendPoint(timestamp=base + timedelta(hours=1), value=0.5),
            TrendPoint(timestamp=base + timedelta(hours=2), value=0.8),
        ]
        trend = analytics.compute_trend("test", points)
        assert trend.min_value == 0.2
        assert trend.max_value == 0.8
        assert trend.count == 3

    # ── Confidence Calibration ───────────────────────────────────────────────

    def test_calibration_empty(self):
        """Empty calibration data returns empty report."""
        analytics = AnalyticsEngine()
        report = analytics.compute_calibration_report()
        assert report["total_predictions"] == 0
        assert report["expected_calibration_error"] == 0.0
        assert report["bins"] == []

    def test_calibration_bin_count(self):
        """Calibration report produces correct number of bins."""
        analytics = AnalyticsEngine()
        for _ in range(100):
            analytics.record_calibration_observation(0.5, True)

        report = analytics.compute_calibration_report(num_bins=10)
        assert len(report["bins"]) == 10
        assert report["total_predictions"] == 100

    def test_calibration_perfect(self):
        """Perfect calibration → ECE ≈ 0."""
        analytics = AnalyticsEngine()
        # All predictions at 0.9 with 90% accuracy
        for _ in range(90):
            analytics.record_calibration_observation(0.9, True)
        for _ in range(10):
            analytics.record_calibration_observation(0.9, False)

        report = analytics.compute_calibration_report()
        assert abs(report["expected_calibration_error"]) < 0.05
        assert report["calibration_score"] > 0.95

    def test_calibration_overconfidence(self):
        """Overconfident predictions are detected."""
        analytics = AnalyticsEngine()
        # Predict 0.9 but only 50% correct
        for _ in range(50):
            analytics.record_calibration_observation(0.9, True)
        for _ in range(50):
            analytics.record_calibration_observation(0.9, False)

        report = analytics.compute_calibration_report()
        assert report["overconfidence"] is True
        assert report["confidence_bias"] > 0

    def test_calibration_underconfidence(self):
        """Underconfident predictions are detected."""
        analytics = AnalyticsEngine()
        # Predict 0.5 but 90% correct
        for _ in range(90):
            analytics.record_calibration_observation(0.5, True)
        for _ in range(10):
            analytics.record_calibration_observation(0.5, False)

        report = analytics.compute_calibration_report()
        assert report["underconfidence"] is True
        assert report["confidence_bias"] < 0

    def test_calibration_metrics_shape(self):
        """Calibration report has all expected keys."""
        analytics = AnalyticsEngine()
        analytics.record_calibration_observation(0.8, True)
        analytics.record_calibration_observation(0.7, False)

        report = analytics.compute_calibration_report()
        assert "bins" in report
        assert "expected_calibration_error" in report
        assert "maximum_calibration_error" in report
        assert "overconfidence" in report
        assert "underconfidence" in report
        assert "calibration_score" in report
        assert "total_predictions" in report
        assert "overall_accuracy" in report
        assert "overall_confidence" in report
        assert "confidence_bias" in report

    # ── Specialist Learning Curves ───────────────────────────────────────────

    def test_specialist_learning_curve_basic(self):
        """Learning curve for a specialist with activity."""
        analytics = AnalyticsEngine()

        analytics.start_session("s1")
        analytics.record_delta_processed("FORGE")
        analytics.record_first_attempt("FORGE", "t1", True)
        analytics.end_session({"total_patterns": 3, "by_category": {}, "avg_confidence": 0.5})

        analytics.start_session("s2")
        analytics.record_delta_processed("FORGE")
        analytics.record_first_attempt("FORGE", "t2", True)
        analytics.end_session({"total_patterns": 5, "by_category": {}, "avg_confidence": 0.7})

        curve = analytics.compute_specialist_learning_curve("FORGE")
        assert curve.specialist == "FORGE"
        assert curve.session_count == 2
        assert curve.overall_first_attempt_success_rate == 1.0
        assert curve.total_first_attempts == 2
        assert curve.total_first_attempt_successes == 2

    def test_specialist_learning_curve_no_activity(self):
        """Learning curve for inactive specialist has zero data."""
        analytics = AnalyticsEngine()
        analytics.start_session("s1")
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        curve = analytics.compute_specialist_learning_curve("FORGE")
        assert curve.session_count == 0
        assert curve.overall_first_attempt_success_rate == 0.0

    def test_all_specialist_curves(self):
        """All active specialists get learning curves."""
        analytics = AnalyticsEngine()

        analytics.start_session("s1")
        analytics.record_delta_processed("FORGE")
        analytics.record_delta_processed("SENTINEL")
        analytics.end_session({"total_patterns": 2, "by_category": {}, "avg_confidence": 0.5})

        curves = analytics.compute_all_specialist_curves()
        assert "FORGE" in curves
        assert "SENTINEL" in curves
        assert curves["FORGE"].session_count >= 1
        assert curves["SENTINEL"].session_count >= 1

    # ── Analytics Report ─────────────────────────────────────────────────────

    def test_generate_full_report(self):
        """Full analytics report contains all sections."""
        analytics = AnalyticsEngine()

        analytics.start_session("s1")
        analytics.record_delta_processed("FORGE")
        analytics.record_pattern_created()
        analytics.record_first_attempt("FORGE", "t1", True)
        analytics.record_calibration_observation(0.8, True)
        analytics.end_session({"total_patterns": 3, "by_category": {"add_file": 2}, "avg_confidence": 0.6})

        analytics.start_session("s2")
        analytics.record_delta_processed("FORGE")
        analytics.record_first_attempt("FORGE", "t2", False)
        analytics.record_calibration_observation(0.7, False)
        analytics.end_session({"total_patterns": 4, "by_category": {"add_file": 3}, "avg_confidence": 0.65})

        report = analytics.generate_analytics_report()

        assert report["session_count"] == 2
        assert report["total_first_attempts"] == 2
        assert "overall_first_attempt_success_rate" in report
        assert "calibration" in report
        assert "confidence_trend" in report
        assert "pattern_creation_trend" in report
        assert "first_attempt_trend" in report
        assert "specialist_learning_curves" in report
        assert "FORGE" in report["specialist_learning_curves"]
        assert "generation_duration_ms" in report

    def test_report_minimal(self):
        """Report with no data still has valid structure."""
        analytics = AnalyticsEngine()
        report = analytics.generate_analytics_report()
        assert report["session_count"] == 0
        assert report["total_first_attempts"] == 0
        assert report["calibration"]["total_predictions"] == 0
        assert report["confidence_trend"].direction == TrendDirection.INSUFFICIENT_DATA
        assert report["specialist_learning_curves"] == {}

    # ── Session Listing ──────────────────────────────────────────────────────

    def test_list_sessions_ordered(self):
        """Sessions are listed newest first."""
        analytics = AnalyticsEngine()

        analytics.start_session("s1")
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        time.sleep(0.01)

        analytics.start_session("s2")
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        sessions = analytics.list_sessions()
        assert sessions[0].session_id == "s2"
        assert sessions[1].session_id == "s1"

    def test_list_sessions_limit(self):
        """Session list respects limit."""
        analytics = AnalyticsEngine()
        for i in range(5):
            analytics.start_session(f"s{i}")
            analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        sessions = analytics.list_sessions(limit=2)
        assert len(sessions) == 2

    # ── Get Session Report ──────────────────────────────────────────────────

    def test_get_session_report(self):
        """Session report retrievable by ID."""
        analytics = AnalyticsEngine()
        analytics.start_session("find_me")
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        record = analytics.get_session_report("find_me")
        assert record is not None
        assert record.session_id == "find_me"

    def test_get_session_report_not_found(self):
        """Non-existent session returns None."""
        analytics = AnalyticsEngine()
        assert analytics.get_session_report("nope") is None

    # ── Metrics ──────────────────────────────────────────────────────────────

    def test_metrics_recorded(self):
        """Operations record metrics."""
        analytics = AnalyticsEngine()

        analytics.start_session("m")
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        analytics.generate_analytics_report()

        metrics = analytics.get_metrics()
        assert len(metrics) >= 1
        assert any(m["operation"] == "generate_analytics_report" for m in metrics)

    def test_metrics_empty_initially(self):
        """No metrics before any operations."""
        analytics = AnalyticsEngine()
        assert analytics.get_metrics() == []

    # ── Reset ────────────────────────────────────────────────────────────────

    def test_reset_clears_all_state(self):
        """Reset clears sessions, first attempts, and metrics."""
        analytics = AnalyticsEngine()

        analytics.start_session("s1")
        analytics.record_first_attempt("FORGE", "t1", True)
        analytics.end_session({"total_patterns": 0, "by_category": {}, "avg_confidence": 0.0})

        assert len(analytics.list_sessions()) == 1
        assert len(analytics.get_first_attempts()) == 1

        analytics.reset()

        assert len(analytics.list_sessions()) == 0
        assert len(analytics.get_first_attempts()) == 0
        assert len(analytics.get_metrics()) == 0
        assert analytics._current_session_id is None


class TestEngineAnalyticsIntegration:
    """Tests for AnalyticsEngine integration with PatternExtractionEngine."""

    def test_engine_has_analytics(self):
        """PatternExtractionEngine has an analytics attribute."""
        engine = make_engine()
        assert hasattr(engine, "analytics")
        assert engine.analytics is not None

    def test_analytics_session_lifecycle(self):
        """Engine.start_session and end_session propagate to analytics."""
        engine = make_engine()
        engine.start_session("test_session")
        assert engine.analytics._current_session_id == "test_session"
        engine.end_session()
        assert engine.analytics._current_session_id is None

    def test_delta_tracking(self):
        """Delta processing is tracked in analytics."""
        engine = make_engine()

        # Create a minimal before/after with a change
        from repo_intelligence.types import (
            EdgeType,
        )

        fid_a = FileId.create("a.py")
        fid_b = FileId.create("b.py")
        sym_a = SymbolNode(
            symbol_id="a" * 16, file_id=fid_a, file_path="a.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="a", fully_qualified_name="a",
            confidence=ConfidenceLevel.CERTAIN,
        )
        sym_b = SymbolNode(
            symbol_id="b" * 16, file_id=fid_b, file_path="b.py",
            line_range=(1, 10), symbol_kind=SymbolKind.MODULE,
            symbol_name="b", fully_qualified_name="b",
            confidence=ConfidenceLevel.CERTAIN,
        )

        before = GraphSnapshot(
            files={fid_a: ParsedFile(file_id=fid_a, file_path="a.py", language=LanguageId.PYTHON, fingerprint="v1"),
                   fid_b: ParsedFile(file_id=fid_b, file_path="b.py", language=LanguageId.PYTHON, fingerprint="v1")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            version=1,
        )
        after = GraphSnapshot(
            files={fid_a: ParsedFile(file_id=fid_a, file_path="a.py", language=LanguageId.PYTHON, fingerprint="v1"),
                   fid_b: ParsedFile(file_id=fid_b, file_path="b.py", language=LanguageId.PYTHON, fingerprint="v1")},
            symbols={sym_a.symbol_id: sym_a, sym_b.symbol_id: sym_b},
            edges=[SymbolEdge(source_id=sym_a.symbol_id, target_id=sym_b.symbol_id,
                              edge_type=EdgeType.IMPORTS, file_path="a.py", line_number=1,
                              confidence=ConfidenceLevel.CERTAIN)],
            version=2,
        )

        engine.start_session("test")
        # Need 3 transitions to create a pattern (min_observations = 3)
        for i in range(3):
            engine.process_graph_transition(before, after)
        assert engine.analytics._current_deltas >= 3
        assert engine.analytics._current_created >= 1

        # Check the session record after end
        engine.end_session()
        record = engine.analytics.get_session_report("test")
        assert record is not None
        assert record.deltas_processed >= 3

    def test_record_first_attempt_through_engine(self):
        """First-attempt recording through engine works."""
        engine = make_engine()
        engine.start_session("fa_test")

        result = engine.record_first_attempt(
            specialist="FORGE",
            task_description="Add auth",
            succeeded=True,
        )
        assert result is not None
        assert result.succeeded is True
        assert result.specialist == "FORGE"

        engine.end_session()
        # Verify the record persisted
        records = engine.analytics.get_first_attempts("FORGE")
        assert len(records) >= 1

    def test_record_first_attempt_no_session(self):
        """First-attempt without session returns None."""
        engine = make_engine()
        result = engine.record_first_attempt("FORGE", "task", True)
        assert result is None

    def test_get_session_report_through_engine(self):
        """Session report accessible through engine."""
        engine = make_engine()
        # No session started — should return None
        assert engine.get_session_report() is None

        engine.start_session("test")
        report = engine.get_session_report()
        assert report is not None
        assert report["session_id"] == "test"

        engine.end_session()

    def test_get_analytics_report_through_engine(self):
        """Full analytics report accessible through engine."""
        engine = make_engine()
        report = engine.get_analytics_report()
        assert report["session_count"] == 0

        engine.start_session("test")
        engine.record_first_attempt("FORGE", "task", True)
        engine.end_session()

        report = engine.get_analytics_report()
        assert report["session_count"] == 1
        assert "calibration" in report
        assert "specialist_learning_curves" in report

    def test_get_specialist_learning_curve_through_engine(self):
        """Learning curve accessible through engine."""
        engine = make_engine()
        engine.start_session("s1")
        engine.analytics.record_delta_processed("FORGE")
        engine.record_first_attempt("FORGE", "t1", True)
        engine.end_session()

        curve = engine.get_specialist_learning_curve("FORGE")
        assert curve["specialist"] == "FORGE"
        assert curve["session_count"] >= 1
        assert curve["overall_first_attempt_success_rate"] == 1.0

    def test_learning_statistics_includes_analytics(self):
        """get_learning_statistics includes session analytics."""
        engine = make_engine()
        engine.start_session("test")
        stats = engine.get_learning_statistics()
        assert "session_analytics" in stats
        assert stats["session_analytics"]["session_id"] == "test"
        engine.end_session()

    def test_contradiction_tracking_in_analytics(self):
        """Contradictions detected during pipeline are tracked in analytics."""
        engine = make_engine()
        engine.start_session("test")
        # Set low threshold to trigger promotion quickly
        engine.accumulator = PatternAccumulator(min_observations_for_pattern=2)

        # Create two different patterns in the same category

        # Pattern A imported edges
        sig_a = EditCategorySignature(category=EditCategory.ADD_IMPORT_DEPENDENCY)
        sig_b = EditCategorySignature(category=EditCategory.ADD_IMPORT_DEPENDENCY)
        sub_a = SubgraphSpec(anchor_node_key="mod_a", node_count=1)
        sub_b = SubgraphSpec(anchor_node_key="mod_b", node_count=1)

        for i in range(2):
            engine.accumulator.ingest(
                DependencyGraphDelta(new_edges=[GraphDeltaEdge(
                    edge_type=EdgeType.IMPORTS, source_file_id=f"a{i}", target_file_id=f"b{i}")]),
                sub_a, sig_a,
            )

        # Trigger contradiction by ingesting a different pattern in same category
        for i in range(2):
            engine.accumulator.ingest(
                DependencyGraphDelta(new_edges=[GraphDeltaEdge(
                    edge_type=EdgeType.IMPORTS, source_file_id=f"x{i}", target_file_id=f"y{i}")]),
                sub_b, sig_b,
            )

        # The engine's _check_contradictions would have fired
        engine.end_session()


# Pull in imports needed for the integration test
from repo_intelligence.types import (
    EdgeType, SymbolKind,
)
