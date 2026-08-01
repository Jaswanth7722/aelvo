"""Provider Doctor — comprehensive diagnostic tool for provider health."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from ..runtime.registry import ProviderRegistry
from ..runtime.health import ProviderHealthRuntime
from ..runtime.usage import UsageTracker
from ..runtime.capability import CapabilityRegistry
from ..types import ProviderStatus

logger = logging.getLogger(__name__)


@dataclass
class DoctorReport:
    """Comprehensive diagnostic report from Provider Doctor."""

    provider_id: str
    overall_health: str
    connectivity: Optional[bool] = None
    auth_status: Optional[bool] = None
    latency_grade: str = "unknown"
    uptime_grade: str = "unknown"
    capabilities: list[str] = field(default_factory=list)
    models: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)


class ProviderDoctor:
    """Comprehensive provider diagnostic tool.

    Runs full diagnostics on a provider and generates actionable
    reports with health status, recommendations, and issues.
    """

    def __init__(
        self,
        registry: ProviderRegistry,
        health: ProviderHealthRuntime,
        usage: UsageTracker,
        capability: CapabilityRegistry,
    ) -> None:
        self._registry = registry
        self._health = health
        self._usage = usage
        self._capability = capability

    async def diagnose(self, provider_id: str) -> DoctorReport:
        """Run full diagnostics on a provider."""
        issues: list[str] = []
        recommendations: list[str] = []

        # Check connectivity
        entry = self._registry.get(provider_id)
        connectivity = entry is not None and entry.is_active
        if not connectivity:
            issues.append(f"Provider {provider_id} is not registered or is inactive")
            recommendations.append(f"Register {provider_id} with ProviderRegistry.register()")

        # Check auth status
        auth_health = self._health.get_health(provider_id)
        auth_ok = auth_health is not None and auth_health.status != ProviderStatus.UNKNOWN
        if not auth_ok:
            issues.append(f"Provider {provider_id} has not been health-checked")
            recommendations.append("Run a health check on this provider")

        # Check latency
        latency = self._health.average_latency(provider_id)
        if latency > 2000:
            issues.append(f"High latency: {latency:.0f}ms")
            recommendations.append("Consider a faster provider or region")
            latency_grade = "poor"
        elif latency > 500:
            latency_grade = "fair"
        elif latency > 0:
            latency_grade = "good"
        else:
            latency_grade = "unknown"

        # Check uptime
        uptime = self._health.uptime_percentage(provider_id, 60)
        if uptime < 0.9 and uptime > 0:
            issues.append(f"Low uptime: {uptime:.1%}")
            recommendations.append("Configure a fallback provider")
            uptime_grade = "poor"
        elif uptime >= 0.99:
            uptime_grade = "excellent"
        elif uptime >= 0.95:
            uptime_grade = "good"
        elif uptime > 0:
            uptime_grade = "fair"
        else:
            uptime_grade = "unknown"

        # Get capabilities
        caps = self._capability.get_provider_capabilities(provider_id)
        capabilities = [c.name for c in caps.capabilities] if caps else []

        # Get models
        entry = self._registry.get(provider_id)
        models = [m.id for m in entry.info.models] if entry and entry.info.models else []

        # Usage metrics
        usage = self._usage.summary()

        # Determine overall health
        if not issues:
            overall_health = "healthy"
            recommendations.append(f"Provider {provider_id} is operating normally")
        elif len(issues) <= 2:
            overall_health = "degraded"
        else:
            overall_health = "unhealthy"

        return DoctorReport(
            provider_id=provider_id,
            overall_health=overall_health,
            connectivity=connectivity,
            auth_status=auth_ok,
            latency_grade=latency_grade,
            uptime_grade=uptime_grade,
            capabilities=capabilities,
            models=models,
            recommendations=recommendations,
            issues=issues,
            metrics={
                "avg_latency_ms": round(latency, 1),
                "uptime_1h": round(uptime * 100, 1),
                "total_cost": usage.get("total_cost", 0),
                "total_requests": usage.get("total_requests", 0),
            },
        )

    async def run_full_scan(self) -> dict[str, DoctorReport]:
        """Run diagnostics on all registered providers."""
        reports = {}
        for pid in self._registry.list_provider_ids():
            try:
                reports[pid] = await self.diagnose(pid)
            except Exception as e:
                logger.error("Doctor scan failed for %s: %s", pid, e)
        return reports

    def summary_table(self, reports: dict[str, DoctorReport]) -> list[dict[str, str]]:
        """Generate a summary table from diagnostic reports."""
        return [
            {
                "provider": pid,
                "health": r.overall_health,
                "latency": r.latency_grade,
                "uptime": r.uptime_grade,
                "issues": str(len(r.issues)),
                "connectivity": "✅" if r.connectivity else "❌",
            }
            for pid, r in reports.items()
        ]
