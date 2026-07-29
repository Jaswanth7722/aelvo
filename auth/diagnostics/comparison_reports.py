"""Provider comparison reports — side-by-side provider comparisons."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..runtime.health import ProviderHealthRuntime
from ..runtime.usage import UsageTracker
from ..runtime.capability import CapabilityRegistry


@dataclass
class ComparisonMetric:
    """A single metric for provider comparison."""

    name: str
    values: dict[str, Any]
    unit: str = ""
    higher_is_better: bool = False


@dataclass
class ComparisonReport:
    """Side-by-side comparison of multiple providers."""

    providers: list[str]
    metrics: list[ComparisonMetric] = field(default_factory=list)
    winner: str = ""
    summary: str = ""


class ComparisonReportGenerator:
    """Generates provider comparison reports."""

    def __init__(
        self,
        health: ProviderHealthRuntime,
        usage: UsageTracker,
        capability: CapabilityRegistry,
    ) -> None:
        self._health = health
        self._usage = usage
        self._capability = capability

    def compare(self, provider_ids: list[str]) -> ComparisonReport:
        """Generate a comparison report for the given providers."""
        metrics: list[ComparisonMetric] = []
        scores: dict[str, float] = {pid: 0.0 for pid in provider_ids}

        # Latency comparison
        latency_values = {}
        for pid in provider_ids:
            latency_values[pid] = round(self._health.average_latency(pid), 1)
        metrics.append(ComparisonMetric(
            name="Avg Latency", values=latency_values, unit="ms", higher_is_better=False
        ))
        if latency_values:
            best_pid = min(latency_values, key=latency_values.get)
            scores[best_pid] = scores.get(best_pid, 0) + 1

        # Uptime comparison
        uptime_values = {}
        for pid in provider_ids:
            uptime_values[pid] = round(self._health.uptime_percentage(pid, 60) * 100, 1)
        metrics.append(ComparisonMetric(
            name="Uptime (1h)", values=uptime_values, unit="%", higher_is_better=True
        ))
        if uptime_values:
            best_pid = max(uptime_values, key=uptime_values.get)
            scores[best_pid] = scores.get(best_pid, 0) + 1

        # Error rate comparison
        error_values = {}
        for pid in provider_ids:
            error_values[pid] = round(self._health.error_rate(pid) * 100, 1)
        metrics.append(ComparisonMetric(
            name="Error Rate", values=error_values, unit="%", higher_is_better=False
        ))
        if error_values:
            best_pid = min(error_values, key=error_values.get)
            scores[best_pid] = scores.get(best_pid, 0) + 1

        # Cost comparison
        cost_values = {}
        for pid in provider_ids:
            cost_values[pid] = round(self._usage.total_cost(provider_id=pid), 4)
        metrics.append(ComparisonMetric(
            name="Total Cost", values=cost_values, unit="USD", higher_is_better=False
        ))

        # Capabilities count
        cap_values = {}
        for pid in provider_ids:
            caps = self._capability.get_provider_capabilities(pid)
            cap_values[pid] = len(caps.capabilities) if caps else 0
        metrics.append(ComparisonMetric(
            name="Capabilities", values=cap_values, unit="count", higher_is_better=True
        ))

        # Determine winner
        winner = max(scores, key=scores.get) if scores else ""
        summary = (
            f"**{winner}** is the top performer across latency, "
            f"uptime, and reliability metrics."
        ) if winner else "No clear winner."

        return ComparisonReport(
            providers=provider_ids,
            metrics=metrics,
            winner=winner,
            summary=summary,
        )

    def to_table(self, report: ComparisonReport) -> list[dict[str, Any]]:
        """Convert comparison report to a table format."""
        table = []
        for metric in report.metrics:
            row = {"metric": f"{metric.name} ({metric.unit})"}
            for pid in report.providers:
                value = metric.values.get(pid, "N/A")
                row[pid] = value
            table.append(row)
        return table
