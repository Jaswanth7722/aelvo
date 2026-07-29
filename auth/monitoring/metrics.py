"""Provider metrics — granular performance and reliability metrics."""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """A single metric data point."""

    name: str
    value: float
    tags: dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class MetricSeries:
    """A time series of metric points."""

    name: str
    points: deque[MetricPoint] = field(
        default_factory=lambda: deque(maxlen=1000)
    )
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return len(self.points)

    @property
    def latest(self) -> Optional[float]:
        if not self.points:
            return None
        return self.points[-1].value

    @property
    def min(self) -> Optional[float]:
        if not self.points:
            return None
        return min(p.value for p in self.points)

    @property
    def max(self) -> Optional[float]:
        if not self.points:
            return None
        return max(p.value for p in self.points)

    @property
    def avg(self) -> Optional[float]:
        if not self.points:
            return None
        return sum(p.value for p in self.points) / len(self.points)

    @property
    def sum(self) -> float:
        return sum(p.value for p in self.points)

    def percentile(self, pct: float) -> Optional[float]:
        if not self.points:
            return None
        sorted_vals = sorted(p.value for p in self.points)
        idx = int(len(sorted_vals) * pct / 100)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]


class MetricsCollector:
    """Collects and aggregates provider performance metrics.

    Tracks latency percentiles, error rates, request counts,
    and custom provider metrics with tagging support.
    """

    def __init__(self) -> None:
        self._series: dict[str, MetricSeries] = {}

    def record(
        self,
        name: str,
        value: float,
        tags: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a metric point."""
        key = self._make_key(name, tags or {})

        if key not in self._series:
            self._series[key] = MetricSeries(
                name=name, tags=tags or {}
            )

        self._series[key].points.append(
            MetricPoint(
                name=name,
                value=value,
                tags=tags or {},
            )
        )

    def _make_key(
        self, name: str, tags: dict[str, str]
    ) -> str:
        tag_str = ",".join(
            f"{k}={v}" for k, v in sorted(tags.items())
        )
        return f"{name}[{tag_str}]" if tag_str else name

    def get_series(
        self,
        name: str,
        tags: Optional[dict[str, str]] = None,
    ) -> Optional[MetricSeries]:
        key = self._make_key(name, tags or {})
        return self._series.get(key)

    def record_latency(
        self,
        provider_id: str,
        latency_ms: float,
        model_id: str = "",
    ) -> None:
        self.record(
            "latency",
            latency_ms,
            tags={
                "provider": provider_id,
                "model": model_id or "unknown",
            },
        )

    def record_token_usage(
        self,
        provider_id: str,
        prompt_tokens: int,
        completion_tokens: int,
        model_id: str = "",
    ) -> None:
        self.record(
            "tokens.prompt",
            prompt_tokens,
            tags={"provider": provider_id, "model": model_id},
        )
        self.record(
            "tokens.completion",
            completion_tokens,
            tags={"provider": provider_id, "model": model_id},
        )

    def record_request(
        self,
        provider_id: str,
        success: bool,
        latency_ms: float = 0.0,
    ) -> None:
        self.record(
            "request.count",
            1.0,
            tags={"provider": provider_id, "success": str(success)},
        )
        if latency_ms:
            self.record(
                "request.latency",
                latency_ms,
                tags={
                    "provider": provider_id,
                    "success": str(success),
                },
            )

    def record_error(
        self, provider_id: str, error_type: str
    ) -> None:
        self.record(
            "error.count",
            1.0,
            tags={"provider": provider_id, "error_type": error_type},
        )

    def summary(
        self,
        provider_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Get a summary of all metrics."""
        series_list = list(self._series.values())

        if provider_id:
            series_list = [
                s
                for s in series_list
                if s.tags.get("provider") == provider_id
            ]

        result: dict[str, Any] = {}
        for series in series_list:
            if series.count == 0:
                continue
            result[series.name] = {
                "count": series.count,
                "avg": series.avg,
                "min": series.min,
                "max": series.max,
                "latest": series.latest,
                "p50": series.percentile(50),
                "p95": series.percentile(95),
                "p99": series.percentile(99),
                "sum": series.sum,
                "tags": series.tags,
            }

        return result
