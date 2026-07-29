"""Usage and Cost Tracking — provider-level usage metrics and cost management."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from ..types import TokenUsage

logger = logging.getLogger(__name__)


@dataclass
class UsageRecord:
    """A single usage record for a provider call."""

    provider_id: str
    model_id: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost: float = 0.0
    latency_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProviderPricing:
    """Pricing information for a provider model."""

    provider_id: str
    model_id: str
    input_cost_per_1k: float = 0.0
    output_cost_per_1k: float = 0.0
    currency: str = "USD"


class UsageTracker:
    """Tracks token usage and costs across all providers."""

    def __init__(self) -> None:
        self._records: list[UsageRecord] = []
        self._pricing: dict[str, ProviderPricing] = {}
        self._max_records: int = 10000

    def register_pricing(self, pricing: ProviderPricing) -> None:
        key = f"{pricing.provider_id}:{pricing.model_id}"
        self._pricing[key] = pricing

    def register_pricing_batch(
        self, pricings: list[ProviderPricing]
    ) -> None:
        for p in pricings:
            self.register_pricing(p)

    def record(
        self,
        provider_id: str,
        model_id: str,
        usage: TokenUsage,
        latency_ms: float = 0.0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> UsageRecord:
        """Record token usage for a provider call."""
        cost = self._calculate_cost(
            provider_id,
            model_id,
            usage.prompt_tokens,
            usage.completion_tokens,
        )

        record = UsageRecord(
            provider_id=provider_id,
            model_id=model_id,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens or (usage.prompt_tokens + usage.completion_tokens),
            cost=cost,
            latency_ms=latency_ms,
            metadata=metadata or {},
        )

        self._records.append(record)

        # Trim if needed
        if len(self._records) > self._max_records:
            self._records = self._records[-self._max_records:]

        return record

    def _calculate_cost(
        self,
        provider_id: str,
        model_id: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> float:
        key = f"{provider_id}:{model_id}"
        pricing = self._pricing.get(key)
        if pricing is None:
            return 0.0

        input_cost = (prompt_tokens / 1000.0) * pricing.input_cost_per_1k
        output_cost = (completion_tokens / 1000.0) * pricing.output_cost_per_1k
        return round(input_cost + output_cost, 6)

    # ── Queries ───────────────────────────────────────────────────

    def total_cost(
        self,
        provider_id: Optional[str] = None,
        since: Optional[float] = None,
    ) -> float:
        records = self._filter(provider_id=provider_id, since=since)
        return sum(r.cost for r in records)

    def total_tokens(
        self,
        provider_id: Optional[str] = None,
        since: Optional[float] = None,
    ) -> int:
        records = self._filter(provider_id=provider_id, since=since)
        return sum(r.total_tokens for r in records)

    def cost_by_provider(
        self, since: Optional[float] = None
    ) -> dict[str, float]:
        result: dict[str, float] = {}
        for r in self._filter(since=since):
            result[r.provider_id] = result.get(r.provider_id, 0.0) + r.cost
        return result

    def cost_by_model(
        self, since: Optional[float] = None
    ) -> dict[str, float]:
        result: dict[str, float] = {}
        for r in self._filter(since=since):
            key = f"{r.provider_id}/{r.model_id}"
            result[key] = result.get(key, 0.0) + r.cost
        return result

    def average_latency(
        self,
        provider_id: Optional[str] = None,
        since: Optional[float] = None,
    ) -> float:
        records = self._filter(provider_id=provider_id, since=since)
        if not records:
            return 0.0
        return sum(r.latency_ms for r in records) / len(records)

    def get_records(
        self,
        provider_id: Optional[str] = None,
        model_id: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> list[UsageRecord]:
        records = self._filter(
            provider_id=provider_id, model_id=model_id, since=since
        )
        return records[-limit:]

    def summary(
        self, since: Optional[float] = None
    ) -> dict[str, Any]:
        records = self._filter(since=since)
        return {
            "total_cost": round(self.total_cost(since=since), 4),
            "total_tokens": self.total_tokens(since=since),
            "total_requests": len(records),
            "by_provider": self.cost_by_provider(since),
            "avg_latency_ms": round(
                self.average_latency(since=since), 1
            ),
        }

    def _filter(
        self,
        provider_id: Optional[str] = None,
        model_id: Optional[str] = None,
        since: Optional[float] = None,
    ) -> list[UsageRecord]:
        records = self._records
        if provider_id:
            records = [r for r in records if r.provider_id == provider_id]
        if model_id:
            records = [r for r in records if r.model_id == model_id]
        if since:
            records = [r for r in records if r.timestamp >= since]
        return records

    def export_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(
                [
                    {
                        "provider_id": r.provider_id,
                        "model_id": r.model_id,
                        "prompt_tokens": r.prompt_tokens,
                        "completion_tokens": r.completion_tokens,
                        "total_tokens": r.total_tokens,
                        "cost": r.cost,
                        "latency_ms": r.latency_ms,
                        "timestamp": r.timestamp,
                    }
                    for r in self._records
                ],
                f,
                indent=2,
            )

    def clear(self) -> None:
        self._records.clear()
