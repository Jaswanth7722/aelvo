"""Health monitoring system — proactive degradation detection and alerting."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Optional

from ..types import ProviderStatus

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    INFO = auto()
    WARNING = auto()
    CRITICAL = auto()


@dataclass
class Alert:
    """A health alert for a provider."""

    provider_id: str
    level: AlertLevel
    message: str
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckPolicy:
    """Policy for running health checks on a provider."""

    provider_id: str
    check_interval: float = 60.0  # seconds
    timeout: float = 10.0
    consecutive_failures_threshold: int = 3
    enabled: bool = True
    endpoints: list[str] = field(default_factory=list)
    check_fn: Optional[Callable[[], Awaitable[bool]]] = None
    """
    Optional async callable that performs the actual health check.

    If set, this will be called instead of the default endpoint-guessing
    logic in _run_health_check. This allows HealthCheckRunner-registered
    checks (connectivity, DNS, etc.) to drive the monitoring loop.

    The callable should return True if the provider is healthy, False otherwise.
    """


class HealthMonitor:
    """Proactive health monitoring with periodic checks and alerts.

    Runs configurable health checks on registered providers,
    detects degradation patterns, and emits alerts.
    """

    def __init__(self) -> None:
        self._policies: dict[str, HealthCheckPolicy] = {}
        self._alerts: list[Alert] = []
        self._consecutive_failures: dict[str, int] = {}
        self._check_results: dict[str, list[bool]] = {}
        self._check_tasks: dict[str, asyncio.Task[Any]] = {}
        self._alert_handlers: list[Callable[[Alert], None]] = []
        self._running = False

    def register_policy(self, policy: HealthCheckPolicy) -> None:
        self._policies[policy.provider_id] = policy
        self._consecutive_failures[policy.provider_id] = 0
        self._check_results[policy.provider_id] = []

    def add_alert_handler(
        self, handler: Callable[[Alert], None]
    ) -> None:
        self._alert_handlers.append(handler)

    async def start(self) -> None:
        """Start health check loops for all registered providers."""
        if self._running:
            return
        self._running = True

        for policy in self._policies.values():
            if policy.enabled:
                task = asyncio.create_task(
                    self._check_loop(policy)
                )
                self._check_tasks[policy.provider_id] = task

        logger.info(
            "Health monitor started with %d policies",
            len(self._policies),
        )

    async def stop(self) -> None:
        """Stop all health check loops."""
        self._running = False
        for task in self._check_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._check_tasks.clear()

    async def _check_loop(
        self, policy: HealthCheckPolicy
    ) -> None:
        """Periodic health check loop for a single provider."""
        while self._running:
            try:
                healthy = await self._run_health_check(policy)
                self._record_check(policy.provider_id, healthy)

                if not healthy:
                    self._consecutive_failures[policy.provider_id] += 1
                    failures = self._consecutive_failures[
                        policy.provider_id
                    ]

                    if failures >= policy.consecutive_failures_threshold:
                        alert = Alert(
                            provider_id=policy.provider_id,
                            level=AlertLevel.CRITICAL,
                            message=(
                                f"Provider {policy.provider_id} has {failures} "
                                f"consecutive health check failures"
                            ),
                            metadata={
                                "consecutive_failures": failures,
                                "threshold": policy.consecutive_failures_threshold,
                            },
                        )
                        await self._emit_alert(alert)
                else:
                    if self._consecutive_failures[policy.provider_id] > 0:
                        # Recovery alert
                        alert = Alert(
                            provider_id=policy.provider_id,
                            level=AlertLevel.INFO,
                            message=(
                                f"Provider {policy.provider_id} recovered after "
                                f"{self._consecutive_failures[policy.provider_id]} failures"
                            ),
                        )
                        await self._emit_alert(alert)
                    self._consecutive_failures[policy.provider_id] = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Health check error for %s: %s",
                    policy.provider_id,
                    e,
                )

            await asyncio.sleep(policy.check_interval)

    async def _run_health_check(
        self, policy: HealthCheckPolicy
    ) -> bool:
        """Run a health check against a provider.

        Uses policy.check_fn if available (registered via HealthCheckRunner),
        otherwise falls back to guessing provider endpoints.
        """
        # Use registered check function if available
        if policy.check_fn is not None:
            try:
                return await policy.check_fn()
            except Exception:
                return False

        # Fallback: guess provider endpoint
        try:
            import httpx

            async with httpx.AsyncClient(timeout=policy.timeout) as client:
                endpoints = policy.endpoints or [
                    f"https://api.{policy.provider_id}.com/health"
                ]
                for endpoint in endpoints:
                    try:
                        response = await client.get(endpoint)
                        if response.status_code < 500:
                            return True
                    except Exception:
                        continue
                return False
        except Exception:
            return False

    def _record_check(
        self, provider_id: str, healthy: bool
    ) -> None:
        results = self._check_results.setdefault(provider_id, [])
        results.append(healthy)
        # Keep last 100 results
        if len(results) > 100:
            self._check_results[provider_id] = results[-100:]

    async def _emit_alert(self, alert: Alert) -> None:
        self._alerts.append(alert)
        logger.log(
            logging.WARNING
            if alert.level in (AlertLevel.WARNING, AlertLevel.CRITICAL)
            else logging.INFO,
            "Health alert [%s] %s: %s",
            alert.level.name,
            alert.provider_id,
            alert.message,
        )
        for handler in self._alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error("Alert handler error: %s", e)

    def get_alerts(
        self,
        provider_id: Optional[str] = None,
        level: Optional[AlertLevel] = None,
        limit: int = 50,
    ) -> list[Alert]:
        alerts = self._alerts
        if provider_id:
            alerts = [a for a in alerts if a.provider_id == provider_id]
        if level:
            alerts = [a for a in alerts if a.level == level]
        return alerts[-limit:]

    def get_health_score(self, provider_id: str) -> float:
        """Get a health score (0.0 to 1.0) for a provider."""
        results = self._check_results.get(provider_id, [])
        if not results:
            return 1.0
        return sum(results) / len(results)

    def get_status(self, provider_id: str) -> ProviderStatus:
        """Derive provider status from health check results."""
        score = self.get_health_score(provider_id)
        failures = self._consecutive_failures.get(provider_id, 0)

        if failures >= 5:
            return ProviderStatus.DOWN
        if failures >= 3:
            return ProviderStatus.DEGRADED
        if score < 0.5:
            return ProviderStatus.ERROR
        if score < 0.8:
            return ProviderStatus.DEGRADED
        return ProviderStatus.HEALTHY

    def summary(self) -> dict[str, Any]:
        return {
            "providers_monitored": len(self._policies),
            "total_alerts": len(self._alerts),
            "critical_alerts": len(
                [a for a in self._alerts if a.level == AlertLevel.CRITICAL]
            ),
            "is_running": self._running,
            "status": {
                pid: self.get_status(pid).name
                for pid in self._policies
            },
        }
