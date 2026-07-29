"""Runtime Diagnostics Engine — introspection and diagnostics for provider runtime."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ..types import ProviderStatus
from .registry import ProviderRegistry
from .health import ProviderHealthRuntime
from .usage import UsageTracker
from .capability import CapabilityRegistry

logger = logging.getLogger(__name__)


@dataclass
class DiagnosticReport:
    """Comprehensive diagnostic report for a provider."""

    provider_id: str
    status: str
    health: dict[str, Any]
    auth: Optional[dict[str, Any]] = None
    capabilities: Optional[list[str]] = None
    recent_errors: list[dict[str, Any]] = field(default_factory=list)
    latency_stats: dict[str, float] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    routing: Optional[dict[str, Any]] = None
    recommendations: list[str] = field(default_factory=list)


class RuntimeDiagnostics:
    """Provides comprehensive diagnostics and introspection for the provider runtime."""

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

    async def diagnose_provider(
        self, provider_id: str
    ) -> DiagnosticReport:
        """Generate a comprehensive diagnostic report for a provider."""
        health = self._health.summary(provider_id)
        caps = self._capability.get_provider_capabilities(provider_id)
        records = self._health.get_records(provider_id, limit=20)

        recent_errors = [
            {
                "timestamp": r.timestamp,
                "error": r.error or "Unknown",
                "latency_ms": r.latency_ms,
            }
            for r in records
            if r.error
        ]

        latency_stats = {
            "avg": self._health.average_latency(provider_id),
            "min": (
                min(r.latency_ms for r in records if r.latency_ms > 0)
                if any(r.latency_ms > 0 for r in records)
                else 0.0
            ),
            "max": (
                max(r.latency_ms for r in records)
                if records
                else 0.0
            ),
        }

        usage = self._usage.summary()

        recommendations = self._generate_recommendations(
            provider_id, health, recent_errors
        )

        return DiagnosticReport(
            provider_id=provider_id,
            status=self._health.get_status(provider_id).name,
            health=health,
            capabilities=(
                [c.name for c in caps.capabilities] if caps else None
            ),
            recent_errors=recent_errors[-10:],
            latency_stats=latency_stats,
            usage=usage,
            recommendations=recommendations,
        )

    def _generate_recommendations(
        self,
        provider_id: str,
        health: dict[str, Any],
        recent_errors: list[dict[str, Any]],
    ) -> list[str]:
        """Generate actionable recommendations based on diagnostics."""
        recommendations = []

        status = self._health.get_status(provider_id)

        if status == ProviderStatus.DOWN:
            recommendations.append(
                f"Provider {provider_id} is down. Check network connectivity "
                "and provider status page."
            )
            recommendations.append(
                "Consider configuring a fallback provider."
            )

        if status == ProviderStatus.RATE_LIMITED:
            recommendations.append(
                "Provider is rate-limited. Consider reducing request rate "
                "or upgrading your plan."
            )

        if status == ProviderStatus.ERROR and recent_errors:
            last_error = recent_errors[-1]["error"]
            if "auth" in last_error.lower() or "key" in last_error.lower():
                recommendations.append(
                    "Authentication error detected. Check your API key or "
                    "credentials."
                )
            if "timeout" in last_error.lower():
                recommendations.append(
                    "Timeouts detected. Consider increasing timeout or "
                    "using a faster provider."
                )

        latency = self._health.average_latency(provider_id)
        if latency > 2000:
            recommendations.append(
                f"High latency ({latency:.0f}ms). Consider using a "
                "provider with lower latency."
            )

        uptime = self._health.uptime_percentage(provider_id, 60)
        if uptime < 0.9:
            recommendations.append(
                f"Low uptime ({uptime:.1%}). Consider a more reliable provider."
            )

        if not recommendations:
            recommendations.append(
                f"Provider {provider_id} is operating normally."
            )

        return recommendations

    def compare_providers(
        self, provider_ids: list[str]
    ) -> dict[str, Any]:
        """Compare multiple providers side by side."""
        comparison: dict[str, Any] = {}

        for pid in provider_ids:
            comparison[pid] = {
                "status": self._health.get_status(pid).name,
                "latency_ms": round(
                    self._health.average_latency(pid), 1
                ),
                "uptime_1h": f"{self._health.uptime_percentage(pid, 60):.1%}",
                "error_rate": f"{self._health.error_rate(pid):.1%}",
                "recommendation": self._health.get_recommendation(pid),
            }

        return comparison

    def runtime_health_summary(self) -> dict[str, Any]:
        """Get overall runtime health summary."""
        registry_summary = self._registry.summary()

        return {
            "providers": {
                pid: self._health.summary(pid)
                for pid in self._registry.list_provider_ids()
            },
            "registry": registry_summary,
            "total_errors": sum(
                len(self._health.get_records(pid, 100))
                for pid in self._registry.list_provider_ids()
            ),
            "uptime_by_provider": {
                pid: f"{self._health.uptime_percentage(pid, 60):.1%}"
                for pid in self._registry.list_provider_ids()
            },
        }
