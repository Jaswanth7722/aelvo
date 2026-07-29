"""Runtime health monitoring — proactive health checks, degradation detection,
and status reporting for recovery, governance, and scaling subsystems.

Provides:
- Periodic health checks per subsystem
- Degradation detection (consecutive failures)
- Health status aggregation
- Health check registration
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("aelvo.runtime.monitoring.health")


class HealthStatus(str, Enum):
    """Overall health status of a subsystem."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""

    healthy: bool
    message: str = ""
    metric_value: Optional[float] = None
    timestamp: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckPolicy:
    """Configuration for a periodic health check."""

    subsystem: str
    check_id: str
    description: str = ""
    interval_seconds: float = 60.0
    failure_threshold: int = 3
    success_threshold: int = 2
    enabled: bool = True
    check_fn: Optional[Callable[[], HealthCheckResult]] = None

    @property
    def key(self) -> str:
        return f"{self.subsystem}:{self.check_id}"


class RuntimeHealthMonitor:
    """Monitors the health of runtime subsystems.

    Supports:
    - Registering health checks per subsystem
    - Running checks on demand
    - Tracking consecutive failures for degradation detection
    - Computing subsystem health status
    - Health report generation

    Usage:
        monitor = RuntimeHealthMonitor()
        monitor.register_check(HealthCheckPolicy(
            subsystem="recovery",
            check_id="consensus_engine",
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        results = monitor.run_all_checks()
        status = monitor.get_subsystem_status("recovery")
    """

    def __init__(self):
        self._policies: Dict[str, HealthCheckPolicy] = {}
        self._results: Dict[str, List[HealthCheckResult]] = {}
        self._consecutive_failures: Dict[str, int] = {}
        self._consecutive_successes: Dict[str, int] = {}
        self._metrics_collector: Any = None

    def set_metrics_collector(self, collector: Any) -> None:
        """Link a metrics collector for automatic instrumentation."""
        self._metrics_collector = collector

    # ── Check Registration ──────────────────────────────────────────────

    def register_check(self, policy: HealthCheckPolicy) -> None:
        """Register a health check policy."""
        self._policies[policy.key] = policy
        self._results.setdefault(policy.key, [])
        self._consecutive_failures[policy.key] = 0
        self._consecutive_successes[policy.key] = 0
        log.info(
            "Health check registered: %s [%s] — interval=%ss, threshold=%d",
            policy.check_id, policy.subsystem,
            policy.interval_seconds, policy.failure_threshold,
        )

    def unregister_check(self, subsystem: str, check_id: str) -> bool:
        """Remove a health check by subsystem and ID."""
        key = f"{subsystem}:{check_id}"
        if key in self._policies:
            del self._policies[key]
            self._results.pop(key, None)
            self._consecutive_failures.pop(key, None)
            self._consecutive_successes.pop(key, None)
            return True
        return False

    def get_policies(
        self, subsystem: Optional[str] = None,
    ) -> List[HealthCheckPolicy]:
        """Get registered health check policies."""
        policies = list(self._policies.values())
        if subsystem:
            policies = [p for p in policies if p.subsystem == subsystem]
        return policies

    # ── Check Execution ─────────────────────────────────────────────────

    def run_check(self, subsystem: str, check_id: str) -> Optional[HealthCheckResult]:
        """Run a specific health check by subsystem and ID."""
        key = f"{subsystem}:{check_id}"
        policy = self._policies.get(key)
        if not policy or not policy.enabled:
            return None
        return self._execute_and_record(policy)

    def run_all_checks(
        self, subsystems: Optional[List[str]] = None,
    ) -> Dict[str, List[HealthCheckResult]]:
        """Run all registered health checks.

        Args:
            subsystems: Optional list of subsystem names to filter by.

        Returns:
            Dict mapping subsystem names to lists of health check results.
        """
        results: Dict[str, List[HealthCheckResult]] = {}

        for key, policy in self._policies.items():
            if not policy.enabled:
                continue
            if subsystems and policy.subsystem not in subsystems:
                continue

            result = self._execute_and_record(policy)
            if policy.subsystem not in results:
                results[policy.subsystem] = []
            results[policy.subsystem].append(result)

        return results

    def _execute_and_record(self, policy: HealthCheckPolicy) -> HealthCheckResult:
        """Execute a health check and record the result."""
        result: HealthCheckResult

        if policy.check_fn:
            try:
                result = policy.check_fn()
            except Exception as e:
                result = HealthCheckResult(
                    healthy=False,
                    message=f"Health check raised exception: {e}",
                )
        else:
            # No check function — assume healthy (passive check)
            result = HealthCheckResult(healthy=True, message="No check function registered")

        # Record result
        self._results[policy.key].append(result)
        if len(self._results[policy.key]) > 100:
            self._results[policy.key] = self._results[policy.key][-100:]

        # Track consecutive successes/failures
        if result.healthy:
            self._consecutive_successes[policy.key] += 1
            self._consecutive_failures[policy.key] = 0
        else:
            self._consecutive_failures[policy.key] += 1
            self._consecutive_successes[policy.key] = 0

        # Record health check result as a metric if collector is linked
        if self._metrics_collector:
            try:
                self._metrics_collector.record(
                    "health.check",
                    tags={
                        "subsystem": policy.subsystem,
                        "check_id": policy.check_id,
                        "healthy": str(result.healthy),
                    },
                )
                self._metrics_collector.record("health.check")  # Aggregate
            except Exception:
                pass  # Don't let metrics interfere with health checks

        return result

    # ── Status Computation ──────────────────────────────────────────────

    def get_check_status(self, subsystem: str, check_id: str) -> HealthStatus:
        """Get the health status of a specific check."""
        key = f"{subsystem}:{check_id}"
        failures = self._consecutive_failures.get(key, 0)
        policy = self._policies.get(key)

        if not policy:
            return HealthStatus.UNKNOWN
        if failures >= policy.failure_threshold:
            return HealthStatus.UNHEALTHY
        if failures > 0:
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    def get_subsystem_status(self, subsystem: str) -> HealthStatus:
        """Aggregate health status for a subsystem.

        UNHEALTHY if any check in the subsystem is UNHEALTHY.
        DEGRADED if any check is DEGRADED.
        HEALTHY if all checks are HEALTHY.
        UNKNOWN if no checks registered.
        """
        policies = self.get_policies(subsystem)
        if not policies:
            return HealthStatus.UNKNOWN

        has_degraded = False
        for p in policies:
            status = self.get_check_status(p.subsystem, p.check_id)
            if status == HealthStatus.UNHEALTHY:
                return HealthStatus.UNHEALTHY
            if status == HealthStatus.DEGRADED:
                has_degraded = True

        return HealthStatus.DEGRADED if has_degraded else HealthStatus.HEALTHY

    def get_overall_status(self) -> HealthStatus:
        """Get overall runtime health status.

        Returns the worst status across all subsystems.
        """
        subsystems = set(p.subsystem for p in self._policies.values())
        overall = HealthStatus.HEALTHY
        for sub in subsystems:
            status = self.get_subsystem_status(sub)
            if self._rank_status(status) > self._rank_status(overall):
                overall = status
        return overall

    @staticmethod
    def _rank_status(status: HealthStatus) -> int:
        return {
            HealthStatus.HEALTHY: 0,
            HealthStatus.DEGRADED: 1,
            HealthStatus.UNHEALTHY: 2,
            HealthStatus.UNKNOWN: -1,
        }.get(status, -1)

    def get_recent_results(
        self, subsystem: Optional[str] = None, limit: int = 10,
    ) -> List[Tuple[str, HealthCheckResult]]:
        """Get recent health check results."""
        results: List[Tuple[str, HealthCheckResult]] = []
        for key, res_list in self._results.items():
            for res in res_list[-limit:]:
                sub, check_id = key.split(":", 1)
                if subsystem and sub != subsystem:
                    continue
                results.append((check_id, res))
        # Sort by timestamp descending
        results.sort(key=lambda r: r[1].timestamp, reverse=True)
        return results[:limit]

    # ── Reporting ───────────────────────────────────────────────────────

    def generate_health_report(self) -> Dict[str, Any]:
        """Generate a comprehensive health report."""
        subsystems = set(p.subsystem for p in self._policies.values())

        report: Dict[str, Any] = {
            "overall_status": self.get_overall_status().value,
            "generated_at": time.time(),
            "subsystems": {},
        }

        for sub in sorted(subsystems):
            policies = self.get_policies(sub)
            check_statuses = {}
            for p in policies:
                status = self.get_check_status(p.subsystem, p.check_id)
                failures = self._consecutive_failures.get(p.key, 0)
                check_statuses[p.check_id] = {
                    "status": status.value,
                    "consecutive_failures": failures,
                    "enabled": p.enabled,
                    "description": p.description,
                    "last_result": (
                        self._results[p.key][-1].to_dict()
                        if self._results.get(p.key)
                        else None
                    ),
                }

            report["subsystems"][sub] = {
                "status": self.get_subsystem_status(sub).value,
                "total_checks": len(policies),
                "checks": check_statuses,
            }

        return report

    def reset(self) -> None:
        """Reset all health state."""
        self._policies.clear()
        self._results.clear()
        self._consecutive_failures.clear()
        self._consecutive_successes.clear()


# ── Helper: Add to_dict to HealthCheckResult ──────────────────────────────

def _health_check_to_dict(self: HealthCheckResult) -> Dict[str, Any]:
    return {
        "healthy": self.healthy,
        "message": self.message,
        "metric_value": self.metric_value,
        "timestamp": self.timestamp,
        "details": self.details,
    }

HealthCheckResult.to_dict = _health_check_to_dict  # type: ignore

