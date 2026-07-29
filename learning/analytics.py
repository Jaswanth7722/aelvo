# learning/analytics.py - AnalyticsEngine
# Session-level learning metrics, trend analysis, confidence calibration tracking,
# and first-attempt success rate improvement measurement across sessions.

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from collections import defaultdict

from learning.types import (
    SessionRecord, CalibrationBin, TrendPoint,
    TrendDirection, TrendSeries, FirstAttemptRecord,
    SpecialistLearningCurve,
)

log = logging.getLogger("aelvo.learning.analytics")


class AnalyticsEngine:
    """Computes session-level analytics, trends, calibration, and
    first-attempt success improvement across sessions.

    Four analytical pillars:

    1. **Session Metrics** — Per-session KPIs: deltas processed, patterns
       created/updated, contradictions, average confidence, category
       distribution, specialist activity.

    2. **Trend Analysis** — Across-session trend lines with linear regression
       for: confidence growth, pattern creation rate, first-attempt success
       rate. Detects improving/degrading/stable directions.

    3. **Confidence Calibration** — Detailed ECE (Expected Calibration Error)
       computation with 10-bin histograms, per-bin accuracy tracking,
       overconfidence/underconfidence detection.

    4. **First-Attempt Success Measurement** — Tracks whether each specialist's
       first attempt at a task succeeds, and whether that rate improves as
       patterns are learned across sessions.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._sessions: Dict[str, SessionRecord] = {}
        self._first_attempts: List[FirstAttemptRecord] = []
        self._all_pattern_confidences: List[Tuple[float, bool]] = []
            # (predicted_confidence, actually_correct)

        # Current session tracking
        self._current_session_id: Optional[str] = None
        self._current_session_start: Optional[datetime] = None
        self._current_deltas = 0
        self._current_created = 0
        self._current_updated = 0
        self._current_contradictions = 0
        self._current_pruned = 0
        self._current_specialist_activity: Dict[str, int] = defaultdict(int)
        self._metrics: List[Dict] = []

    # ── Session Lifecycle ────────────────────────────────────────────────────

    def start_session(self, session_id: str) -> None:
        """Begin tracking a new learning session."""
        with self._lock:
            self._current_session_id = session_id
            self._current_session_start = datetime.now(timezone.utc)
            self._current_deltas = 0
            self._current_created = 0
            self._current_updated = 0
            self._current_contradictions = 0
            self._current_pruned = 0
            self._current_specialist_activity.clear()

            log.info(f"Analytics session started: {session_id}")

    def end_session(self, accumulator_stats: Dict) -> SessionRecord:
        """End the session, compute final metrics, and return the record.

        Args:
            accumulator_stats: Statistics dict from PatternAccumulator.get_statistics().

        Returns:
            The completed SessionRecord.
        """
        with self._lock:
            if not self._current_session_id:
                raise RuntimeError("No active session to end.")

            ended_at = datetime.now(timezone.utc)
            duration = (ended_at - self._current_session_start).total_seconds() if self._current_session_start else 0.0

            # Compute first-attempt metrics for this session
            session_first_attempts = [
                fa for fa in self._first_attempts
                if fa.session_id == self._current_session_id
            ]
            first_try_count = len(session_first_attempts)
            first_try_successes = sum(1 for fa in session_first_attempts if fa.succeeded)

            record = SessionRecord(
                session_id=self._current_session_id,
                started_at=self._current_session_start or datetime.now(timezone.utc),
                ended_at=ended_at,
                duration_seconds=round(duration, 2),
                deltas_processed=self._current_deltas,
                patterns_created=self._current_created,
                patterns_updated=self._current_updated,
                contradictions_detected=self._current_contradictions,
                patterns_pruned=self._current_pruned,
                avg_confidence=round(accumulator_stats.get("avg_confidence", 0.0), 4),
                total_patterns_end=accumulator_stats.get("total_patterns", 0),
                specialist_activity=dict(self._current_specialist_activity),
                category_distribution=accumulator_stats.get("by_category", {}),
                first_attempts=first_try_count,
                first_attempt_successes=first_try_successes,
            )

            self._sessions[self._current_session_id] = record

            self._current_session_id = None
            self._current_session_start = None

            log.info(
                f"Analytics session ended: {record.session_id} "
                f"({record.deltas_processed} deltas, "
                f"{record.patterns_created} created, "
                f"{record.patterns_updated} updated, "
                f"first-attempt success: {record.first_attempt_success_rate:.0%})"
            )

            return record

    def record_delta_processed(self, specialist: Optional[str] = None) -> None:
        """Record that a delta was processed in the current session."""
        with self._lock:
            self._current_deltas += 1
            if specialist:
                self._current_specialist_activity[specialist] += 1

    def record_pattern_created(self) -> None:
        with self._lock:
            self._current_created += 1

    def record_pattern_updated(self) -> None:
        with self._lock:
            self._current_updated += 1

    def record_contradiction(self) -> None:
        with self._lock:
            self._current_contradictions += 1

    def record_pruned(self, count: int = 1) -> None:
        with self._lock:
            self._current_pruned += count

    # ── First-Attempt Tracking ───────────────────────────────────────────────

    def record_first_attempt(
        self,
        specialist: str,
        task_description: str,
        succeeded: bool,
        pattern_id: Optional[str] = None,
        confidence_at_time: float = 0.0,
    ) -> FirstAttemptRecord:
        """Record whether a specialist succeeded on their first attempt.

        Returns:
            The created FirstAttemptRecord.
        """
        with self._lock:
            if not self._current_session_id:
                log.warning("No active session — first attempt not recorded")
                raise RuntimeError("No active analytics session.")

            record = FirstAttemptRecord(
                specialist=specialist,
                task_description=task_description,
                succeeded=succeeded,
                session_id=self._current_session_id,
                pattern_id=pattern_id,
                confidence_at_time=confidence_at_time,
            )
            record.to_id()
            self._first_attempts.append(record)
            return record

    def get_first_attempts(
        self,
        specialist: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[FirstAttemptRecord]:
        """Get first-attempt records, optionally filtered."""
        with self._lock:
            results = self._first_attempts.copy()
            if specialist:
                results = [fa for fa in results if fa.specialist == specialist]
            if session_id:
                results = [fa for fa in results if fa.session_id == session_id]
            return results

    def get_first_attempt_success_rate(
        self,
        specialist: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> float:
        """Compute first-attempt success rate, optionally filtered."""
        with self._lock:
            records = self.get_first_attempts(specialist=specialist, session_id=session_id)
            if not records:
                return 0.0
            successes = sum(1 for r in records if r.succeeded)
            return successes / len(records)

    # ── Trend Analysis ───────────────────────────────────────────────────────

    def compute_trend(self, name: str, points: List[TrendPoint]) -> TrendSeries:
        """Compute a trend series from time-ordered data points.

        Uses linear regression to determine slope and direction.
        """
        with self._lock:
            if len(points) < 2:
                avg = points[0].value if points else 0.0
                return TrendSeries(
                    name=name,
                    points=points,
                    direction=TrendDirection.INSUFFICIENT_DATA,
                    slope=0.0,
                    r_squared=0.0,
                    min_value=avg,
                    max_value=avg,
                    avg_value=avg,
                )

            # Sort by timestamp
            sorted_points = sorted(points, key=lambda p: p.timestamp)

            # Convert timestamps to numeric (seconds since first point)
            t0 = sorted_points[0].timestamp.timestamp()
            x = [p.timestamp.timestamp() - t0 for p in sorted_points]
            y = [p.value for p in sorted_points]

            n = len(x)
            x_mean = sum(x) / n
            y_mean = sum(y) / n

            # Linear regression
            numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
            denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

            slope = numerator / denominator if denominator != 0 else 0.0

            intercept = y_mean - slope * x_mean

            # R-squared
            ss_res = sum((y[i] - (slope * x[i] + intercept)) ** 2 for i in range(n))
            ss_tot = sum((y[i] - y_mean) ** 2 for i in range(n))
            r_squared = 1.0 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

            # Determine direction using normalized total change over time range.
            # Uses max(y_range, |y_mean|) as the normalization reference so that
            # tiny oscillations around a stable mean are not misinterpreted as trends.
            x_range = max(x) - min(x) if max(x) > min(x) else 1.0
            y_range = max(y) - min(y) if max(y) > min(y) else 1.0
            y_ref = max(y_range, abs(y_mean))
            effective_change = slope * x_range

            if n < 3:
                direction = TrendDirection.STABLE
            elif abs(effective_change) < 0.05 * y_ref:
                direction = TrendDirection.STABLE
            elif effective_change > 0:
                direction = TrendDirection.IMPROVING
            else:
                direction = TrendDirection.DEGRADING

            return TrendSeries(
                name=name,
                points=sorted_points,
                direction=direction,
                slope=round(slope, 6),
                r_squared=round(r_squared, 4),
                min_value=round(min(y), 4),
                max_value=round(max(y), 4),
                avg_value=round(y_mean, 4),
            )

    def compute_confidence_trend(self) -> TrendSeries:
        """Compute the trend of average confidence across sessions."""
        with self._lock:
            if not self._sessions:
                return TrendSeries(name="avg_confidence")

            points = [
                TrendPoint(
                    timestamp=s.started_at,
                    value=s.avg_confidence,
                    label=s.session_id,
                )
                for s in sorted(self._sessions.values(), key=lambda s: s.started_at)
            ]
            return self.compute_trend("avg_confidence", points)

    def compute_pattern_creation_trend(self) -> TrendSeries:
        """Compute the trend of pattern creation rate across sessions."""
        with self._lock:
            if not self._sessions:
                return TrendSeries(name="pattern_creation_rate")

            points = [
                TrendPoint(
                    timestamp=s.started_at,
                    value=float(s.patterns_created),
                    label=s.session_id,
                )
                for s in sorted(self._sessions.values(), key=lambda s: s.started_at)
            ]
            return self.compute_trend("pattern_creation_rate", points)

    def compute_first_attempt_trend(
        self, specialist: Optional[str] = None
    ) -> TrendSeries:
        """Compute the trend of first-attempt success rate across sessions."""
        with self._lock:
            sessions = sorted(self._sessions.values(), key=lambda s: s.started_at)

            if specialist:
                # Per-specialist: compute per-session first-attempt rates
                points = []
                for s in sessions:
                    fa_records = [
                        r for r in self._first_attempts
                        if r.session_id == s.session_id and r.specialist == specialist
                    ]
                    if fa_records:
                        rate = sum(1 for r in fa_records if r.succeeded) / len(fa_records)
                        points.append(TrendPoint(
                            timestamp=s.started_at,
                            value=rate,
                            label=f"{s.session_id}({specialist})",
                        ))
            else:
                # Overall: use session-level first-attempt rates
                points = [
                    TrendPoint(
                        timestamp=s.started_at,
                        value=s.first_attempt_success_rate,
                        label=s.session_id,
                    )
                    for s in sessions
                    if s.first_attempts > 0
                ]

            return self.compute_trend("first_attempt_success_rate", points)

    # ── Confidence Calibration ──────────────────────────────────────────────

    def record_calibration_observation(
        self, predicted_confidence: float, actually_correct: bool
    ) -> None:
        """Record a single prediction for calibration tracking."""
        with self._lock:
            self._all_pattern_confidences.append((predicted_confidence, actually_correct))

    def compute_calibration_report(
        self, num_bins: int = 10,
    ) -> Dict[str, Any]:
        """Compute detailed confidence calibration report.

        Args:
            num_bins: Number of equal-width bins for the histogram (default 10).

        Returns:
            Dict with keys:
            - bins: List[CalibrationBin] — per-bin accuracy data
            - expected_calibration_error: float — ECE
            - maximum_calibration_error: float — max per-bin |confidence - accuracy|
            - overconfidence: bool — if confidence > accuracy on average
            - underconfidence: bool — if confidence < accuracy on average
            - calibration_score: float — 1.0 - ECE (higher is better)
            - total_predictions: int
            - overall_accuracy: float
        """
        with self._lock:
            if not self._all_pattern_confidences:
                return {
                    "bins": [],
                    "expected_calibration_error": 0.0,
                    "maximum_calibration_error": 0.0,
                    "overconfidence": False,
                    "underconfidence": False,
                    "calibration_score": 0.0,
                    "total_predictions": 0,
                    "overall_accuracy": 0.0,
                }

            # Initialize bins
            bin_size = 1.0 / num_bins
            bins: List[CalibrationBin] = []
            for i in range(num_bins):
                lower = round(i * bin_size, 4)
                upper = round((i + 1) * bin_size, 4)
                bins.append(CalibrationBin(
                    bin_lower=lower,
                    bin_upper=upper,
                    bin_center=round((lower + upper) / 2, 4),
                ))

            # Assign predictions to bins
            for conf, correct in self._all_pattern_confidences:
                bin_idx = min(int(conf / bin_size), num_bins - 1)
                bins[bin_idx].count += 1
                bins[bin_idx].confidence += conf
                bins[bin_idx].accuracy += (1.0 if correct else 0.0)

            # Compute per-bin metrics
            ece = 0.0
            mce = 0.0
            total = len(self._all_pattern_confidences)
            overall_correct = 0

            for b in bins:
                if b.count > 0:
                    b.accuracy = round(b.accuracy / b.count, 4)
                    b.confidence = round(b.confidence / b.count, 4)
                    bin_ece = abs(b.accuracy - b.confidence)
                    ece += bin_ece * (b.count / total)
                    mce = max(mce, bin_ece)
                    overall_correct += int(b.accuracy * b.count)
                else:
                    b.accuracy = 0.0
                    b.confidence = b.bin_center

            overall_accuracy = overall_correct / total if total > 0 else 0.0
            overall_confidence = sum(c for c, _ in self._all_pattern_confidences) / total

            return {
                "bins": bins,
                "expected_calibration_error": round(ece, 4),
                "maximum_calibration_error": round(mce, 4),
                "overconfidence": overall_confidence > overall_accuracy,
                "underconfidence": overall_confidence < overall_accuracy,
                "calibration_score": round(1.0 - ece, 4),
                "total_predictions": total,
                "overall_accuracy": round(overall_accuracy, 4),
                "overall_confidence": round(overall_confidence, 4),
                "confidence_bias": round(overall_confidence - overall_accuracy, 4),
            }

    # ── Specialist Learning Curves ───────────────────────────────────────────

    def compute_specialist_learning_curve(
        self, specialist: str
    ) -> SpecialistLearningCurve:
        """Compute the complete learning curve for a specialist across sessions.

        Args:
            specialist: The specialist name (e.g., "FORGE", "ARCHITECT").

        Returns:
            SpecialistLearningCurve with session records, first-attempt trend,
            confidence trend, and pattern count trend.
        """
        with self._lock:
            # Find sessions where this specialist was active
            relevant_sessions = [
                s for s in self._sessions.values()
                if specialist in s.specialist_activity
            ]
            relevant_sessions.sort(key=lambda s: s.started_at)

            # Build first-attempt trend
            first_attempt_points = []
            for s in relevant_sessions:
                fa_records = [
                    r for r in self._first_attempts
                    if r.session_id == s.session_id and r.specialist == specialist
                ]
                if fa_records:
                    rate = sum(1 for r in fa_records if r.succeeded) / len(fa_records)
                    first_attempt_points.append(TrendPoint(
                        timestamp=s.started_at,
                        value=rate,
                        label=s.session_id,
                    ))

            # Build confidence trend (use session avg_confidence as proxy)
            conf_points = [
                TrendPoint(
                    timestamp=s.started_at,
                    value=s.avg_confidence,
                    label=s.session_id,
                )
                for s in relevant_sessions
            ]

            # Build pattern count trend
            pattern_points = [
                TrendPoint(
                    timestamp=s.started_at,
                    value=float(s.total_patterns_end),
                    label=s.session_id,
                )
                for s in relevant_sessions
            ]

            # Total first-attempt stats
            total_fa = sum(1 for r in self._first_attempts if r.specialist == specialist)
            total_fa_success = sum(
                1 for r in self._first_attempts
                if r.specialist == specialist and r.succeeded
            )

            return SpecialistLearningCurve(
                specialist=specialist,
                session_records=relevant_sessions,
                first_attempt_trend=self.compute_trend(
                    "first_attempt_success_rate", first_attempt_points
                ),
                confidence_trend=self.compute_trend("avg_confidence", conf_points),
                pattern_count_trend=self.compute_trend("pattern_count", pattern_points),
                total_first_attempts=total_fa,
                total_first_attempt_successes=total_fa_success,
            )

    def compute_all_specialist_curves(
        self,
    ) -> Dict[str, SpecialistLearningCurve]:
        """Compute learning curves for all specialists that have been active."""
        with self._lock:
            specialists = set()
            for s in self._sessions.values():
                specialists.update(s.specialist_activity.keys())

            return {
                sp: self.compute_specialist_learning_curve(sp)
                for sp in sorted(specialists)
            }

    # ── Reports ──────────────────────────────────────────────────────────────

    def get_session_report(self, session_id: str) -> Optional[SessionRecord]:
        """Get the complete record for a specific session.

        If the session is currently active (not yet ended), builds a
        best-effort live record from current state.
        """
        with self._lock:
            if session_id in self._sessions:
                return self._sessions[session_id]

            # Return a live record if this is the active session
            if session_id == self._current_session_id and self._current_session_start:
                now = datetime.now(timezone.utc)
                return SessionRecord(
                    session_id=session_id,
                    started_at=self._current_session_start,
                    deltas_processed=self._current_deltas,
                    patterns_created=self._current_created,
                    patterns_updated=self._current_updated,
                    contradictions_detected=self._current_contradictions,
                    patterns_pruned=self._current_pruned,
                    specialist_activity=dict(self._current_specialist_activity),
                    duration_seconds=round((now - self._current_session_start).total_seconds(), 2),
                )

            return None

    def list_sessions(
        self,
        limit: int = 20,
        sort_by: str = "started_at",
    ) -> List[SessionRecord]:
        """List session records, newest first."""
        with self._lock:
            sessions = sorted(
                self._sessions.values(),
                key=lambda s: s.started_at,
                reverse=True,
            )
            return sessions[:limit]

    def generate_analytics_report(
        self,
        include_calibration: bool = True,
        include_trends: bool = True,
        include_learning_curves: bool = True,
    ) -> Dict[str, Any]:
        """Generate a comprehensive analytics report.

        Args:
            include_calibration: Include confidence calibration analysis.
            include_trends: Include trend analysis across sessions.
            include_learning_curves: Include per-specialist learning curves.

        Returns:
            A comprehensive dict with all analytics data.
        """
        with self._lock:
            start = time.time()

            report: Dict[str, Any] = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "session_count": len(self._sessions),
                "total_first_attempts": len(self._first_attempts),
                "total_first_attempt_successes": sum(1 for r in self._first_attempts if r.succeeded),
                "sessions": self.list_sessions(limit=50),
                "first_attempts_recorded": len(self._first_attempts),
            }

            # Overall first-attempt rate
            if self._first_attempts:
                report["overall_first_attempt_success_rate"] = round(
                    sum(1 for r in self._first_attempts if r.succeeded)
                    / len(self._first_attempts),
                    4,
                )
            else:
                report["overall_first_attempt_success_rate"] = 0.0

            # Calibration
            if include_calibration:
                report["calibration"] = self.compute_calibration_report()

            # Trends
            if include_trends:
                report["confidence_trend"] = self.compute_confidence_trend()
                report["pattern_creation_trend"] = self.compute_pattern_creation_trend()
                report["first_attempt_trend"] = self.compute_first_attempt_trend()

            # Learning curves
            if include_learning_curves:
                curves = self.compute_all_specialist_curves()
                report["specialist_learning_curves"] = {
                    k: {
                        "specialist": v.specialist,
                        "session_count": v.session_count,
                        "overall_first_attempt_success_rate": v.overall_first_attempt_success_rate,
                        "first_attempt_trend_direction": v.first_attempt_trend.direction.value,
                        "first_attempt_trend_slope": v.first_attempt_trend.slope,
                        "confidence_trend_direction": v.confidence_trend.direction.value,
                        "pattern_count_trend_direction": v.pattern_count_trend.direction.value,
                        "total_first_attempts": v.total_first_attempts,
                        "total_first_attempt_successes": v.total_first_attempt_successes,
                    }
                    for k, v in curves.items()
                }

            elapsed = (time.time() - start) * 1000
            report["generation_duration_ms"] = round(elapsed, 2)
            self._record_metric("generate_analytics_report", elapsed)

            return report

    # ── Metrics ───────────────────────────────────────────────────────────────

    def _record_metric(
        self, operation: str, duration_ms: float, extra: Optional[Dict] = None
    ) -> None:
        with self._lock:
            metric = {"operation": operation, "duration_ms": round(duration_ms, 2)}
            if extra:
                metric.update(extra)
            self._metrics.append(metric)

    def get_metrics(self) -> List[Dict]:
        with self._lock:
            return self._metrics.copy()

    def reset(self) -> None:
        with self._lock:
            self._sessions.clear()
            self._first_attempts.clear()
            self._all_pattern_confidences.clear()
            self._current_session_id = None
            self._current_session_start = None
            self._current_deltas = 0
            self._current_created = 0
            self._current_updated = 0
            self._current_contradictions = 0
            self._current_pruned = 0
            self._current_specialist_activity.clear()
            self._metrics.clear()
