"""SecurityAnalytics — Event Frequency, Blocked vs Allowed Ratios, and Threat Trends.

Provides actionable security posture visibility including:
- Security event frequency over time
- Blocked vs allowed execution ratios
- Recurring threat identification and classification
- Approval workflow outcomes
- Sandbox escape attempts (prevented vs attempted)
- Security posture trend analysis

All analytics are computed from the SecurityMemory and ExecutionGovernance subsystems.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .execution_governance import ExecutionGovernance, PolicyDecision, RiskLevel
from .security_memory import SecurityMemory, SecurityMemoryEntry, MemoryEntryType

log = logging.getLogger("aelvo.security.analytics")


# ============================================================================
# Data Types
# ============================================================================


@dataclass
class SecurityAnalyticsReport:
    """A complete security analytics report."""

    generated_at: float = 0.0
    """When the report was generated."""

    time_window_hours: float = 0.0
    """The time window covered by this report."""

    # Event frequency
    total_events: int = 0
    events_by_type: Dict[str, int] = field(default_factory=dict)
    events_by_risk: Dict[str, int] = field(default_factory=dict)
    events_by_hour: Dict[str, int] = field(default_factory=dict)

    # Ratios
    total_decisions: int = 0
    allowed_count: int = 0
    blocked_count: int = 0
    approval_required_count: int = 0
    blocked_ratio: float = 0.0
    approval_ratio: float = 0.0

    # Threats
    active_threats_count: int = 0
    top_threats: List[Dict[str, Any]] = field(default_factory=list)
    hostile_entities_count: int = 0

    # Approvals
    approval_requested: int = 0
    approval_granted: int = 0
    approval_denied: int = 0
    approval_compliance_rate: float = 0.0

    # Trends
    trend_blocked_24h: float = 0.0
    trend_blocked_7d: float = 0.0
    threat_trend: str = "stable"
    """stable | increasing | decreasing"""

    # Safety
    sandbox_escape_attempts: int = 0
    sandbox_escape_prevented: int = 0

    # Posture
    posture_score: float = 0.0
    """Overall security posture score (0.0–1.0). Higher is better."""

    recommendations: List[str] = field(default_factory=list)
    """Actionable security recommendations."""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# SecurityAnalytics
# ============================================================================


class SecurityAnalytics:
    """Computes security analytics from ExecutionGovernance and SecurityMemory.

    Usage:
        analytics = SecurityAnalytics(governance, security_memory)
        report = analytics.generate_report(hours=24)
        print(report.posture_score, report.recommendations)
    """

    def __init__(
        self,
        governance: Optional[ExecutionGovernance] = None,
        security_memory: Optional[SecurityMemory] = None,
    ):
        self._governance = governance
        self._memory = security_memory

    # ------------------------------------------------------------------
    # Report Generation
    # ------------------------------------------------------------------

    def generate_report(
        self,
        hours: float = 24.0,
    ) -> SecurityAnalyticsReport:
        """Generate a comprehensive security analytics report.

        Args:
            hours: Time window for the report (hours before now).

        Returns:
            A SecurityAnalyticsReport with all computed metrics.
        """
        start = time.time()
        since = start - (hours * 3600)

        report = SecurityAnalyticsReport(
            generated_at=start,
            time_window_hours=hours,
        )

        # Collect data from governance
        if self._governance:
            decisions = self._governance.recent_decisions(10000)
            recent_decisions = [d for d in decisions if d.timestamp >= since]

            self._compute_decision_metrics(report, recent_decisions, since)
            self._compute_event_frequency(report, recent_decisions, since)

        # Collect data from memory
        if self._memory:
            self._compute_threat_metrics(report, since)
            self._compute_approval_metrics(report, since)

        # Compute trends and posture
        self._compute_trends(report, hours)
        report.posture_score = self._compute_posture_score(report)
        report.recommendations = self._generate_recommendations(report)

        report.time_window_hours = (time.time() - since) / 3600
        log.info(f"Security analytics report generated in {(time.time() - start)*1000:.1f}ms")
        return report

    def _compute_decision_metrics(
        self,
        report: SecurityAnalyticsReport,
        decisions: List[PolicyDecision],
        since: float,
    ) -> None:
        """Compute metrics from policy decisions."""
        report.total_decisions = len(decisions)
        report.allowed_count = sum(1 for d in decisions if d.allowed)
        report.blocked_count = sum(1 for d in decisions if not d.allowed)
        report.approval_required_count = sum(
            1 for d in decisions if d.requires_approval
        )

        if report.total_decisions > 0:
            report.blocked_ratio = round(report.blocked_count / report.total_decisions, 4)
            report.approval_ratio = round(report.approval_required_count / report.total_decisions, 4)

        # By risk level
        for d in decisions:
            report.events_by_risk[d.risk_level.value] = report.events_by_risk.get(
                d.risk_level.value, 0
            ) + 1

    def _compute_event_frequency(
        self,
        report: SecurityAnalyticsReport,
        decisions: List[PolicyDecision],
        since: float,
    ) -> None:
        """Compute event frequency over time."""
        # Bucket by hour
        now = time.time()
        hours_count = int((now - since) / 3600) + 1
        for h in range(hours_count):
            hour_start = since + (h * 3600)
            hour_end = min(hour_start + 3600, now)
            hour_label = datetime.fromtimestamp(hour_start).strftime("%Y-%m-%d %H:00")

            # Use a small epsilon to handle low timer resolution (e.g., 15ms on Windows)
            # where d.timestamp == now and the strict < would exclude the decision
            count = sum(
                1 for d in decisions
                if hour_start <= d.timestamp < hour_end + 1e-6
            )
            if count > 0:
                report.events_by_hour[hour_label] = count

        # By type (from decisions)
        for d in decisions:
            report.events_by_type[d.action_type] = report.events_by_type.get(
                d.action_type, 0
            ) + 1

        report.total_events = len(decisions)

    def _compute_threat_metrics(
        self,
        report: SecurityAnalyticsReport,
        since: float,
    ) -> None:
        """Compute threat-related metrics from security memory."""
        if not self._memory:
            return

        # Active threats (recurring patterns with recent activity)
        threats = self._memory.get_recurring_threats(min_recurrence=2)
        report.active_threats_count = len(threats)

        report.top_threats = [
            {
                "reason": t.reason,
                "recurrence": t.recurrence_count,
                "importance": round(t.importance, 3),
                "risk_level": t.risk_level.value,
            }
            for t in threats[:10]
        ]

        # Hostile entities
        hostile = self._memory.get_hostile_entities()
        report.hostile_entities_count = len(hostile)

        # Sandbox escape attempts
        escape_entries = self._memory.query(
            entry_type=MemoryEntryType.POLICY_VIOLATION,
            min_importance=0.0,
        )
        for entry in escape_entries:
            if "escape" in entry.tags or "sandbox" in entry.tags:
                report.sandbox_escape_attempts += 1
                if entry.risk_level == RiskLevel.BLOCKED:
                    report.sandbox_escape_prevented += 1

    def _compute_approval_metrics(
        self,
        report: SecurityAnalyticsReport,
        since: float,
    ) -> None:
        """Compute approval workflow metrics."""
        if not self._memory:
            return

        approved = self._memory.query(
            entry_type=MemoryEntryType.APPROVED_RISKY_ACTION,
            min_importance=0.0,
        )
        violations = self._memory.query(
            entry_type=MemoryEntryType.POLICY_VIOLATION,
            min_importance=0.0,
        )

        report.approval_requested = len(approved) + len(violations)
        report.approval_granted = len(approved)

        if report.approval_requested > 0:
            report.approval_compliance_rate = round(
                report.approval_granted / report.approval_requested, 4
            )

    def _compute_trends(
        self,
        report: SecurityAnalyticsReport,
        hours: float,
    ) -> None:
        """Compute security trends over different time windows."""
        if not self._governance:
            return

        now = time.time()
        decisions = self._governance.recent_decisions(10000)

        # Blocked ratio in last 24h vs last 7d (if we have data)
        last_24h = [d for d in decisions if d.timestamp >= now - 86400]
        last_7d = [d for d in decisions if d.timestamp >= now - 604800]

        if last_24h:
            report.trend_blocked_24h = round(
                sum(1 for d in last_24h if not d.allowed) / len(last_24h), 4
            )
        if last_7d:
            report.trend_blocked_7d = round(
                sum(1 for d in last_7d if not d.allowed) / len(last_7d), 4
            )

        # Threat trend
        if report.trend_blocked_7d > 0 and report.trend_blocked_24h > report.trend_blocked_7d * 1.2:
            report.threat_trend = "increasing"
        elif report.trend_blocked_7d > 0 and report.trend_blocked_24h < report.trend_blocked_7d * 0.8:
            report.threat_trend = "decreasing"
        else:
            report.threat_trend = "stable"

    def _compute_posture_score(self, report: SecurityAnalyticsReport) -> float:
        """Compute an overall security posture score (0.0–1.0).

        Factors:
        - Low blocked ratio is good (means few violations)
        - Low approval needs is good (means appropriate trust levels)
        - Few threats is good
        - High approval compliance is good
        """
        score = 0.9  # Start high

        # Penalize high blocked ratio
        if report.blocked_ratio > 0.1:
            score -= 0.2 * min(1.0, report.blocked_ratio)

        # Penalize high threat count
        if report.active_threats_count > 5:
            score -= 0.1 * min(1.0, report.active_threats_count / 20)

        # Penalize high approval needs (suggests too many risky actions)
        if report.approval_ratio > 0.3:
            score -= 0.1 * min(1.0, report.approval_ratio)

        # Boost for good approval compliance
        if report.approval_compliance_rate > 0.8:
            score += 0.05

        # Penalize increasing threats
        if report.threat_trend == "increasing":
            score -= 0.1

        # Ensure bounds
        return max(0.1, min(1.0, score))

    def _generate_recommendations(self, report: SecurityAnalyticsReport) -> List[str]:
        """Generate actionable security recommendations."""
        recommendations = []

        if report.blocked_ratio > 0.2:
            recommendations.append(
                f"High blocked ratio ({report.blocked_ratio:.1%}): Review policy rules "
                f"— legitimate actions may be blocked."
            )

        if report.active_threats_count > 5:
            recommendations.append(
                f"{report.active_threats_count} active recurring threats: "
                f"Consider hardening policy against these patterns."
            )

        if report.threat_trend == "increasing":
            recommendations.append(
                "Threat trend is INCREASING: Immediate security posture review recommended."
            )

        if report.approval_ratio > 0.3:
            recommendations.append(
                f"High approval ratio ({report.approval_ratio:.1%}): "
                f"Consider adding more tools to allowlists to reduce friction."
            )

        if report.sandbox_escape_attempts > 0:
            recommendations.append(
                f"{report.sandbox_escape_attempts} sandbox escape attempts detected "
                f"({report.sandbox_escape_prevented} prevented). Verify sandbox integrity."
            )

        if report.posture_score < 0.5:
            recommendations.append(
                "Security posture is LOW. Recommend audit of all policy rules, "
                "workspace boundaries, and execution paths."
            )

        if not recommendations:
            recommendations.append("Security posture is healthy. No immediate action needed.")

        return recommendations

    # ------------------------------------------------------------------
    # Quick Queries
    # ------------------------------------------------------------------

    def blocked_actions_over_time(
        self,
        hours: float = 24.0,
    ) -> List[Tuple[str, int]]:
        """Get blocked action counts per hour."""
        report = self.generate_report(hours=hours)
        result = []
        for hour_label in sorted(report.events_by_hour.keys()):
            result.append((hour_label, report.events_by_hour.get(hour_label, 0)))
        return result

    def top_threat_sources(self, n: int = 5) -> List[Dict[str, Any]]:
        """Get the top n threat sources by frequency."""
        if not self._memory:
            return []
        threats = self._memory.get_recurring_threats(min_recurrence=2)
        return [
            {
                "target": t.target[:100],
                "recurrence": t.recurrence_count,
                "importance": round(t.importance, 3),
            }
            for t in threats[:n]
        ]

    def get_posture_summary(self) -> str:
        """Get a one-line security posture summary."""
        report = self.generate_report(hours=24)
        score_pct = round(report.posture_score * 100)

        if score_pct >= 80:
            return f"🟢 SECURE (score: {score_pct}%) — {report.allowed_count} allowed, {report.blocked_count} blocked"
        elif score_pct >= 50:
            return f"🟡 MODERATE (score: {score_pct}%) — {report.active_threats_count} threats, trend: {report.threat_trend}"
        else:
            return f"🔴 CRITICAL (score: {score_pct}%) — {report.blocked_count} blocked, immediate review needed"
