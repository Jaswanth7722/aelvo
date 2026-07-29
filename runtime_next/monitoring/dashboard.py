"""Runtime dashboard — periodic health snapshots combining metrics, health
status, and alert data into a single comprehensive view.

Produces DashboardSnapshot objects that can be serialised for display
or stored for historical trending.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .metrics import RuntimeMetricsCollector
from .health import RuntimeHealthMonitor, HealthStatus
from .alerting import AlertManager

log = logging.getLogger("aelvo.runtime.monitoring.dashboard")


@dataclass
class SubsystemHealth:
    """Health snapshot for a single subsystem."""

    name: str
    status: str
    checks_passing: int = 0
    checks_failing: int = 0
    total_checks: int = 0
    metrics_summary: Dict[str, Any] = field(default_factory=dict)
    active_alerts: int = 0
    description: str = ""


@dataclass
class DashboardSnapshot:
    """A complete dashboard snapshot at a point in time."""

    timestamp: float = field(default_factory=time.time)
    overall_status: str = "unknown"
    subsystems: Dict[str, SubsystemHealth] = field(default_factory=dict)
    alerts_summary: Dict[str, Any] = field(default_factory=dict)
    metrics_highlights: Dict[str, Any] = field(default_factory=dict)
    generation_duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "overall_status": self.overall_status,
            "subsystems": {
                name: {
                    "status": sh.status,
                    "checks_passing": sh.checks_passing,
                    "checks_failing": sh.checks_failing,
                    "total_checks": sh.total_checks,
                    "active_alerts": sh.active_alerts,
                    "description": sh.description,
                }
                for name, sh in self.subsystems.items()
            },
            "alerts_summary": self.alerts_summary,
            "metrics_highlights": self.metrics_highlights,
            "generation_duration_ms": self.generation_duration_ms,
        }


class RuntimeDashboard:
    """Generates comprehensive runtime dashboard snapshots.

    Combines data from:
    - RuntimeMetricsCollector (KPI metrics)
    - RuntimeHealthMonitor (health check status)
    - AlertManager (active alerts)

    Usage:
        dashboard = RuntimeDashboard(metrics, health, alerts)
        snapshot = dashboard.generate_snapshot()
        print(snapshot.overall_status)
    """

    def __init__(
        self,
        metrics_collector: Optional[RuntimeMetricsCollector] = None,
        health_monitor: Optional[RuntimeHealthMonitor] = None,
        alert_manager: Optional[AlertManager] = None,
    ):
        self._metrics = metrics_collector
        self._health = health_monitor
        self._alerts = alert_manager

    @property
    def metrics_collector(self) -> Optional[RuntimeMetricsCollector]:
        return self._metrics

    @property
    def health_monitor(self) -> Optional[RuntimeHealthMonitor]:
        return self._health

    @property
    def alert_manager(self) -> Optional[AlertManager]:
        return self._alerts

    def set_metrics_collector(self, collector: RuntimeMetricsCollector) -> None:
        self._metrics = collector

    def set_health_monitor(self, monitor: RuntimeHealthMonitor) -> None:
        self._health = monitor

    def set_alert_manager(self, manager: AlertManager) -> None:
        self._alerts = manager

    def generate_snapshot(self) -> DashboardSnapshot:
        """Generate a complete dashboard snapshot.

        Gathers data from all connected monitoring components
        and produces a structured DashboardSnapshot.
        """
        start = time.time()

        # Determine overall status
        overall_status = "healthy"
        if self._health:
            overall_status = self._health.get_overall_status().value

        # Build subsystem health entries
        subsystems: Dict[str, SubsystemHealth] = {}
        subsystem_names = ["recovery", "governance", "scaling"]

        for name in subsystem_names:
            checks_passing = 0
            checks_failing = 0
            total_checks = 0
            subsystem_status = "unknown"

            if self._health:
                subsystem_status = self._health.get_subsystem_status(name).value
                policies = self._health.get_policies(subsystem=name)
                total_checks = len(policies)
                for p in policies:
                    check_status = self._health.get_check_status(p.subsystem, p.check_id)
                    if check_status in (HealthStatus.HEALTHY,):
                        checks_passing += 1
                    elif check_status in (HealthStatus.UNHEALTHY, HealthStatus.DEGRADED):
                        checks_failing += 1

            active_alerts = 0
            if self._alerts:
                active_alerts = len(self._alerts.get_unacknowledged_alerts(subsystem=name))

            subsystems[name] = SubsystemHealth(
                name=name,
                status=subsystem_status,
                checks_passing=checks_passing,
                checks_failing=checks_failing,
                total_checks=total_checks,
                active_alerts=active_alerts,
                description=self._get_subsystem_description(name),
            )

        # Alert summary
        alerts_summary: Dict[str, Any] = {
            "total_unacknowledged": 0,
            "critical": 0,
            "error": 0,
            "warning": 0,
            "info": 0,
        }
        if self._alerts:
            stats = self._alerts.get_stats()
            alerts_summary = {
                "total_unacknowledged": stats.get("unacknowledged", 0),
                "critical": stats.get("critical", 0),
                "error": stats.get("error", 0),
                "warning": stats.get("warning", 0),
                "info": stats.get("info", 0),
            }

        # Metrics highlights
        metrics_highlights: Dict[str, Any] = {}
        if self._metrics:
            metrics_highlights = {
                "recovery": self._metrics.recovery_summary(),
                "governance": self._metrics.governance_summary(),
                "scaling": self._metrics.scaling_summary(),
            }

        duration = (time.time() - start) * 1000

        return DashboardSnapshot(
            overall_status=overall_status,
            subsystems=subsystems,
            alerts_summary=alerts_summary,
            metrics_highlights=metrics_highlights,
            generation_duration_ms=round(duration, 2),
        )

    def generate_report(self) -> Dict[str, Any]:
        """Generate a human-readable health report string."""
        snapshot = self.generate_snapshot()
        return snapshot.to_dict()

    @staticmethod
    def _get_subsystem_description(name: str) -> str:
        descriptions = {
            "recovery": "Consensus, specialist, and task-level failure recovery",
            "governance": "Policy enforcement and approval management",
            "scaling": "Resource pooling, async pipelines, and batch processing",
        }
        return descriptions.get(name, "")
