"""Alert management — severity-level alerting with deduplication,
escalation, and handler support for runtime subsystems.

Provides:
- Alert creation with severity levels
- Automatic deduplication (suppress repeats within cooldown period)
- Alert handler registration (callbacks for notifications)
- Escalation path management
- Alert history and statistics
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("aelvo.runtime.monitoring.alerting")


class AlertSeverity(str, Enum):
    """Severity levels for runtime alerts."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """A single runtime alert."""

    alert_id: str
    title: str
    message: str
    severity: AlertSeverity
    subsystem: str
    source: str = ""
    timestamp: float = field(default_factory=time.time)
    acknowledged: bool = False
    acknowledged_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    suppressed: bool = False
    """If True, this alert was suppressed due to deduplication cooldown."""

    def acknowledge(self) -> None:
        """Mark the alert as acknowledged."""
        self.acknowledged = True
        self.acknowledged_at = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "title": self.title,
            "message": self.message,
            "severity": self.severity.value,
            "subsystem": self.subsystem,
            "source": self.source,
            "timestamp": self.timestamp,
            "acknowledged": self.acknowledged,
            "acknowledged_at": self.acknowledged_at,
            "suppressed": self.suppressed,
        }


@dataclass
class AlertRule:
    """Defines when an alert should be raised.

    Rules are evaluated against health check results or metric thresholds
    to determine if an alert should fire.
    """

    rule_id: str
    name: str
    description: str
    subsystem: str
    severity: AlertSeverity = AlertSeverity.WARNING
    metric_name: str = ""
    """Metric name to monitor (empty = manual alerts only)."""
    threshold_min: Optional[float] = None
    """Alert if metric drops below this value."""
    threshold_max: Optional[float] = None
    """Alert if metric exceeds this value."""
    consecutive_count: int = 1
    """Number of consecutive violations before alerting."""
    enabled: bool = True
    cooldown_seconds: float = 300.0
    """Minimum time between duplicate alerts."""
    message_template: str = "{metric_name} is {value} (threshold: {threshold})"

    def matches(self, metric_name: str, value: float) -> bool:
        """Check if a metric value triggers this rule."""
        if not self.enabled:
            return False
        if self.metric_name and metric_name != self.metric_name:
            return False
        if self.threshold_min is not None and value < self.threshold_min:
            return True
        if self.threshold_max is not None and value > self.threshold_max:
            return True
        return False

    def format_message(self, metric_name: str, value: float) -> str:
        threshold = self.threshold_max if self.threshold_max is not None else self.threshold_min
        return self.message_template.format(
            metric_name=metric_name,
            value=value,
            threshold=threshold or "N/A",
        )


class AlertManager:
    """Manages alert lifecycle: creation, deduplication, suppression,
    escalation, and handler dispatch.

    Usage:
        manager = AlertManager()
        manager.add_handler(lambda alert: print(f"ALERT: {alert.title}"))
        alert = manager.create_alert(
            title="Recovery failure rate high",
            message="Consensus recovery failed 5 times in 60s",
            severity=AlertSeverity.WARNING,
            subsystem="recovery",
        )
    """

    def __init__(self):
        self._alerts: List[Alert] = []
        self._rules: Dict[str, AlertRule] = {}
        self._handlers: List[Callable[[Alert], None]] = []
        self._last_alert_time: Dict[str, float] = {}
        self._violation_counts: Dict[str, int] = {}
        self._max_alerts: int = 5000

    # ── Alert Handlers ──────────────────────────────────────────────────

    def add_handler(self, handler: Callable[[Alert], None]) -> None:
        """Register an alert handler (called for every non-suppressed alert)."""
        self._handlers.append(handler)

    def remove_handler(self, handler: Callable[[Alert], None]) -> bool:
        """Remove a registered alert handler."""
        if handler in self._handlers:
            self._handlers.remove(handler)
            return True
        return False

    # ── Alert Rules ─────────────────────────────────────────────────────

    def add_rule(self, rule: AlertRule) -> None:
        """Register an alert rule."""
        self._rules[rule.rule_id] = rule
        self._violation_counts[rule.rule_id] = 0
        log.info(
            "Alert rule added: '%s' [%s] — severity=%s",
            rule.name, rule.subsystem, rule.severity.value,
        )

    def remove_rule(self, rule_id: str) -> bool:
        """Remove an alert rule."""
        if rule_id in self._rules:
            del self._rules[rule_id]
            self._violation_counts.pop(rule_id, None)
            return True
        return False

    def get_rules(
        self, subsystem: Optional[str] = None,
    ) -> List[AlertRule]:
        rules = list(self._rules.values())
        if subsystem:
            rules = [r for r in rules if r.subsystem == subsystem]
        return rules

    # ── Alert Creation ──────────────────────────────────────────────────

    def create_alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity,
        subsystem: str,
        source: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        dedup_key: Optional[str] = None,
    ) -> Alert:
        """Create and dispatch an alert.

        Args:
            title: Short alert title.
            message: Detailed alert message.
            severity: Alert severity level.
            subsystem: Subsystem name (e.g., "recovery").
            source: Specific component name.
            metadata: Optional metadata dict.
            dedup_key: Optional key for deduplication. If provided and
                       an alert with the same key was created within the
                       cooldown period (300s), the new alert is suppressed.

        Returns:
            The created Alert (may be marked as suppressed).
        """
        alert_id = self._generate_alert_id(title, subsystem)

        # Deduplication check
        suppressed = False
        if dedup_key:
            last_time = self._last_alert_time.get(dedup_key, 0.0)
            if time.time() - last_time < 300.0:  # 5 min cooldown
                suppressed = True

        alert = Alert(
            alert_id=alert_id,
            title=title,
            message=message,
            severity=severity,
            subsystem=subsystem,
            source=source,
            metadata=metadata or {},
            suppressed=suppressed,
        )

        if dedup_key:
            self._last_alert_time[dedup_key] = alert.timestamp

        if not suppressed:
            self._alerts.append(alert)
            self._dispatch(alert)

        # Trim history
        if len(self._alerts) > self._max_alerts:
            self._alerts = self._alerts[-self._max_alerts:]

        return alert

    def evaluate_metric(
        self, metric_name: str, value: float,
        subsystem: Optional[str] = None,
    ) -> List[Alert]:
        """Evaluate a metric value against all matching alert rules.

        Args:
            metric_name: The metric name to evaluate.
            value: The current metric value.
            subsystem: Optional subsystem filter.

        Returns:
            List of alerts that were triggered (may be empty).
        """
        triggered: List[Alert] = []

        for rule in self._rules.values():
            if subsystem and rule.subsystem != subsystem:
                continue
            if not rule.enabled:
                continue

            if rule.matches(metric_name, value):
                self._violation_counts[rule.rule_id] += 1

                if self._violation_counts[rule.rule_id] >= rule.consecutive_count:
                    alert = self.create_alert(
                        title=rule.name,
                        message=rule.format_message(metric_name, value),
                        severity=rule.severity,
                        subsystem=rule.subsystem,
                        source="alert_rule",
                        metadata={
                            "rule_id": rule.rule_id,
                            "metric_name": metric_name,
                            "metric_value": value,
                            "threshold_min": rule.threshold_min,
                            "threshold_max": rule.threshold_max,
                        },
                        dedup_key=f"rule:{rule.rule_id}:{metric_name}",
                    )
                    triggered.append(alert)
                    # Reset counter after alerting
                    self._violation_counts[rule.rule_id] = 0
            else:
                # Reset counter if value is back in range
                self._violation_counts[rule.rule_id] = 0

        return triggered

    # ── Alert Queries ───────────────────────────────────────────────────

    def get_alerts(
        self,
        subsystem: Optional[str] = None,
        severity: Optional[AlertSeverity] = None,
        limit: int = 50,
        include_suppressed: bool = False,
    ) -> List[Alert]:
        """Get alert history.

        Args:
            subsystem: Optional subsystem filter.
            severity: Optional severity filter.
            limit: Maximum number of alerts to return.
            include_suppressed: If True, include suppressed alerts.

        Returns:
            List of alerts, newest first.
        """
        alerts = self._alerts if include_suppressed else [
            a for a in self._alerts if not a.suppressed
        ]
        if subsystem:
            alerts = [a for a in alerts if a.subsystem == subsystem]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]

        alerts = sorted(alerts, key=lambda a: a.timestamp, reverse=True)
        return alerts[:limit]

    def get_unacknowledged_alerts(
        self, subsystem: Optional[str] = None,
    ) -> List[Alert]:
        """Get all unacknowledged (non-suppressed) alerts."""
        alerts = [
            a for a in self._alerts
            if not a.acknowledged and not a.suppressed
        ]
        if subsystem:
            alerts = [a for a in alerts if a.subsystem == subsystem]
        return sorted(alerts, key=lambda a: a.timestamp, reverse=True)

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge a specific alert by ID."""
        for alert in self._alerts:
            if alert.alert_id == alert_id:
                alert.acknowledge()
                return True
        return False

    def acknowledge_all(self, subsystem: Optional[str] = None) -> int:
        """Acknowledge all unacknowledged alerts."""
        count = 0
        for alert in self._alerts:
            if alert.acknowledged or alert.suppressed:
                continue
            if subsystem and alert.subsystem != subsystem:
                continue
            alert.acknowledge()
            count += 1
        return count

    # ── Statistics ──────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Get alert manager statistics."""
        total = len(self._alerts)
        non_suppressed = [a for a in self._alerts if not a.suppressed]
        return {
            "total_alerts": total,
            "non_suppressed": len(non_suppressed),
            "critical": sum(1 for a in non_suppressed if a.severity == AlertSeverity.CRITICAL),
            "error": sum(1 for a in non_suppressed if a.severity == AlertSeverity.ERROR),
            "warning": sum(1 for a in non_suppressed if a.severity == AlertSeverity.WARNING),
            "info": sum(1 for a in non_suppressed if a.severity == AlertSeverity.INFO),
            "unacknowledged": len(self.get_unacknowledged_alerts()),
            "active_rules": sum(1 for r in self._rules.values() if r.enabled),
            "total_rules": len(self._rules),
        }

    def reset(self) -> None:
        """Reset all alert state."""
        self._alerts.clear()
        self._last_alert_time.clear()
        self._violation_counts.clear()

    # ── Internal ────────────────────────────────────────────────────────

    def _dispatch(self, alert: Alert) -> None:
        """Dispatch an alert to all registered handlers."""
        log.log(
            logging.CRITICAL if alert.severity == AlertSeverity.CRITICAL
            else logging.ERROR if alert.severity == AlertSeverity.ERROR
            else logging.WARNING if alert.severity == AlertSeverity.WARNING
            else logging.INFO,
            "ALERT [%s] %s: %s",
            alert.severity.value.upper(),
            alert.subsystem,
            alert.title,
        )
        for handler in self._handlers:
            try:
                handler(alert)
            except Exception as e:
                log.warning("Alert handler failed: %s", e)

    def _generate_alert_id(self, title: str, subsystem: str) -> str:
        import hashlib
        raw = f"{title}|{subsystem}|{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
