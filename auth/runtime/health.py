"""Provider Health Runtime — tracks provider health, latency, and availability."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ..types import ProviderHealth, ProviderStatus

logger = logging.getLogger(__name__)


@dataclass
class HealthRecord:
    """A single health check observation for a provider."""

    status: ProviderStatus
    latency_ms: float
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    is_degraded: bool = False


class ProviderHealthRuntime:
    """Tracks provider health, latency, reliability, and degradation.

    Maintains rolling history of health checks and computes
    aggregate health metrics for routing decisions.
    """

    def __init__(self, window_size: int = 100) -> None:
        self._window_size = window_size
        self._records: dict[str, list[HealthRecord]] = {}
        self._current_status: dict[str, ProviderHealth] = {}
        self._downtime_start: dict[str, float] = {}
        self._degraded_since: dict[str, float] = {}

    # ── Recording ─────────────────────────────────────────────────

    def record_health(
        self,
        provider_id: str,
        status: ProviderStatus,
        latency_ms: float = 0.0,
        error: Optional[str] = None,
    ) -> None:
        """Record a health observation for a provider."""
        record = HealthRecord(
            status=status,
            latency_ms=latency_ms,
            error=error,
        )

        if provider_id not in self._records:
            self._records[provider_id] = []

        records = self._records[provider_id]
        records.append(record)

        # Trim window
        if len(records) > self._window_size:
            self._records[provider_id] = records[-self._window_size:]

        # Update current status
        self._current_status[provider_id] = ProviderHealth(
            provider_id=provider_id,
            status=status,
            last_check=time.time(),
            last_latency_ms=latency_ms,
            last_error=error,
        )

        # Track degradation
        if status == ProviderStatus.DEGRADED:
            if provider_id not in self._degraded_since:
                self._degraded_since[provider_id] = time.time()
        else:
            self._degraded_since.pop(provider_id, None)

        # Track downtime
        if status == ProviderStatus.DOWN:
            if provider_id not in self._downtime_start:
                self._downtime_start[provider_id] = time.time()
        else:
            self._downtime_start.pop(provider_id, None)

        logger.debug(
            "Health record for %s: status=%s, latency=%.1fms",
            provider_id,
            status.name,
            latency_ms,
        )

    def record_success(
        self, provider_id: str, latency_ms: float = 0.0
    ) -> None:
        self.record_health(provider_id, ProviderStatus.HEALTHY, latency_ms)

    def record_error(
        self,
        provider_id: str,
        error: str,
        latency_ms: float = 0.0,
    ) -> None:
        self.record_health(
            provider_id, ProviderStatus.ERROR, latency_ms, error
        )

    def record_timeout(
        self, provider_id: str, timeout_s: float
    ) -> None:
        self.record_health(
            provider_id,
            ProviderStatus.DOWN,
            timeout_s * 1000,
            f"Timeout after {timeout_s}s",
        )

    def record_rate_limit(self, provider_id: str) -> None:
        self.record_health(
            provider_id,
            ProviderStatus.RATE_LIMITED,
            0.0,
            "Rate limited",
        )

    # ── Queries ───────────────────────────────────────────────────

    def get_health(self, provider_id: str) -> Optional[ProviderHealth]:
        return self._current_status.get(provider_id)

    def get_records(
        self, provider_id: str, limit: int = 10
    ) -> list[HealthRecord]:
        records = self._records.get(provider_id, [])
        return records[-limit:]

    def get_status(self, provider_id: str) -> ProviderStatus:
        health = self._current_status.get(provider_id)
        return health.status if health else ProviderStatus.UNKNOWN

    def is_healthy(self, provider_id: str) -> bool:
        return self.get_status(provider_id) == ProviderStatus.HEALTHY

    def is_degraded(self, provider_id: str) -> bool:
        return self.get_status(provider_id) == ProviderStatus.DEGRADED

    def is_available(self, provider_id: str) -> bool:
        status = self.get_status(provider_id)
        return status in (
            ProviderStatus.HEALTHY,
            ProviderStatus.DEGRADED,
        )

    # ── Aggregates ────────────────────────────────────────────────

    def uptime_percentage(
        self, provider_id: str, window_minutes: int = 60
    ) -> float:
        """Calculate uptime percentage within a time window."""
        records = self._records.get(provider_id, [])
        if not records:
            return 1.0

        cutoff = time.time() - (window_minutes * 60)
        recent = [r for r in records if r.timestamp >= cutoff]
        if not recent:
            return 1.0

        healthy = sum(
            1
            for r in recent
            if r.status in (
                ProviderStatus.HEALTHY,
                ProviderStatus.DEGRADED,
            )
        )
        return healthy / len(recent)

    def average_latency(
        self, provider_id: str, window_minutes: int = 10
    ) -> float:
        """Calculate average latency within a time window."""
        records = self._records.get(provider_id, [])
        if not records:
            return 0.0

        cutoff = time.time() - (window_minutes * 60)
        recent = [r for r in records if r.timestamp >= cutoff and r.latency_ms > 0]
        if not recent:
            return 0.0

        return sum(r.latency_ms for r in recent) / len(recent)

    def error_rate(
        self, provider_id: str, window_minutes: int = 10
    ) -> float:
        """Calculate error rate within a time window."""
        records = self._records.get(provider_id, [])
        if not records:
            return 0.0

        cutoff = time.time() - (window_minutes * 60)
        recent = [r for r in records if r.timestamp >= cutoff]
        if not recent:
            return 0.0

        errors = sum(
            1
            for r in recent
            if r.status == ProviderStatus.ERROR
        )
        return errors / len(recent)

    def degradation_duration(self, provider_id: str) -> float:
        """How long a provider has been degraded (seconds)."""
        since = self._degraded_since.get(provider_id)
        if since is None:
            return 0.0
        return time.time() - since

    def downtime_duration(self, provider_id: str) -> float:
        """How long a provider has been down (seconds)."""
        since = self._downtime_start.get(provider_id)
        if since is None:
            return 0.0
        return time.time() - since

    def get_recommendation(self, provider_id: str) -> str:
        """Get a human-readable health recommendation."""
        status = self.get_status(provider_id)
        uptime = self.uptime_percentage(provider_id)
        latency = self.average_latency(provider_id)

        if status == ProviderStatus.HEALTHY:
            if latency < 500:
                return "Recommended"
            elif latency < 2000:
                return "Usable (high latency)"
            else:
                return "Available (very high latency)"
        elif status == ProviderStatus.DEGRADED:
            return "Use with caution (degraded)"
        elif status == ProviderStatus.RATE_LIMITED:
            return "Avoid (rate limited)"
        elif status == ProviderStatus.ERROR:
            return "Avoid (errors)"
        elif status == ProviderStatus.DOWN:
            return "Unavailable (down)"
        else:
            return "Unknown"

    def summary(self, provider_id: str) -> dict[str, Any]:
        return {
            "provider_id": provider_id,
            "status": self.get_status(provider_id).name,
            "is_available": self.is_available(provider_id),
            "uptime_1h": f"{self.uptime_percentage(provider_id, 60):.1%}",
            "uptime_24h": f"{self.uptime_percentage(provider_id, 1440):.1%}",
            "avg_latency_10m": f"{self.average_latency(provider_id):.0f}ms",
            "error_rate_10m": f"{self.error_rate(provider_id):.1%}",
            "recommendation": self.get_recommendation(provider_id),
            "records_count": len(self._records.get(provider_id, [])),
        }

    def all_healthy(self) -> list[str]:
        return [
            pid
            for pid in self._current_status
            if self.is_healthy(pid)
        ]

    def all_available(self) -> list[str]:
        return [
            pid
            for pid in self._current_status
            if self.is_available(pid)
        ]
