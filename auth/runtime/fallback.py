"""Provider Fallback Router — intelligent routing between providers on failure."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from .health import ProviderHealthRuntime
from .retry import RetryEngine

logger = logging.getLogger(__name__)


@dataclass
class FallbackConfig:
    """Configuration for fallback routing behavior."""

    enabled: bool = True
    max_fallbacks: int = 3
    prefer_same_model_family: bool = True
    prefer_same_capabilities: bool = True
    respect_health_status: bool = True
    min_uptime_threshold: float = 0.8  # 80% uptime required
    max_latency_threshold: float = 5000.0  # 5 seconds max
    timeout_per_provider: float = 30.0


@dataclass
class FallbackDecision:
    """Record of a fallback routing decision."""

    original_provider: str
    selected_provider: str
    reason: str
    attempted_providers: list[str] = field(default_factory=list)
    candidate_providers: list[str] = field(default_factory=list)


class FallbackRouter:
    """Routes provider requests with intelligent fallback selection.

    Uses health signals, capability matching, and configurable
    policies to select the best fallback provider.
    """

    def __init__(
        self,
        health_runtime: ProviderHealthRuntime,
        config: Optional[FallbackConfig] = None,
    ) -> None:
        self.health = health_runtime
        self.config = config or FallbackConfig()
        self._decisions: list[FallbackDecision] = []
        self._provider_families: dict[str, list[str]] = {}
        self._provider_capabilities: dict[str, list[str]] = {}
        self._retry_engine = RetryEngine()

    def register_family_group(
        self, family: str, provider_ids: list[str]
    ) -> None:
        """Register a group of providers that serve the same model family."""
        self._provider_families[family] = provider_ids

    def register_capability_group(
        self, capability: str, provider_ids: list[str]
    ) -> None:
        """Register providers with a specific capability."""
        self._provider_capabilities[capability] = provider_ids

    async def execute_with_fallback(
        self,
        primary_provider: str,
        operation: Callable[..., Awaitable[Any]],
        *args: Any,
        model_family: Optional[str] = None,
        required_capabilities: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> tuple[bool, Any, FallbackDecision]:
        """Execute an operation with automatic fallback.

        Returns:
            Tuple of (success, result_or_error, decision_record).
        """
        attempted: list[str] = []
        candidates = self._get_candidates(
            primary_provider,
            model_family=model_family,
            required_capabilities=required_capabilities,
        )

        # Try primary first
        result = await self._try_provider(
            primary_provider, operation, *args, **kwargs
        )
        if result[0]:
            decision = FallbackDecision(
                original_provider=primary_provider,
                selected_provider=primary_provider,
                reason="Primary succeeded",
                attempted_providers=[primary_provider],
                candidate_providers=candidates,
            )
            self._decisions.append(decision)
            return True, result[1], decision

        attempted.append(primary_provider)

        # Try fallbacks
        for fallback_provider in candidates:
            if fallback_provider in attempted:
                continue
            if len(attempted) >= self.config.max_fallbacks + 1:
                break

            result = await self._try_provider(
                fallback_provider, operation, *args, **kwargs
            )
            attempted.append(fallback_provider)

            if result[0]:
                decision = FallbackDecision(
                    original_provider=primary_provider,
                    selected_provider=fallback_provider,
                    reason=(
                        f"Primary failed, fallback to {fallback_provider} succeeded"
                    ),
                    attempted_providers=attempted,
                    candidate_providers=candidates,
                )
                self._decisions.append(decision)
                logger.info(
                    "Fallback from %s to %s succeeded after %d attempts",
                    primary_provider,
                    fallback_provider,
                    len(attempted),
                )
                return True, result[1], decision

        # All failed
        decision = FallbackDecision(
            original_provider=primary_provider,
            selected_provider="",
            reason=(
                f"All {len(attempted)} providers failed: "
                + ", ".join(attempted)
            ),
            attempted_providers=attempted,
            candidate_providers=candidates,
        )
        self._decisions.append(decision)
        return False, result[1] if result else None, decision

    async def _try_provider(
        self,
        provider_id: str,
        operation: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> tuple[bool, Any]:
        """Try an operation on a specific provider with timeout."""
        try:
            result = await asyncio.wait_for(
                operation(provider_id, *args, **kwargs),
                timeout=self.config.timeout_per_provider,
            )
            self.health.record_success(provider_id)
            return True, result
        except asyncio.TimeoutError:
            self.health.record_timeout(
                provider_id, self.config.timeout_per_provider
            )
            return False, TimeoutError(
                f"Provider {provider_id} timed out after "
                f"{self.config.timeout_per_provider}s"
            )
        except Exception as e:
            self.health.record_error(provider_id, str(e))
            return False, e

    def _get_candidates(
        self,
        primary: str,
        model_family: Optional[str] = None,
        required_capabilities: Optional[list[str]] = None,
    ) -> list[str]:
        """Get ordered list of fallback candidates."""
        candidates: set[str] = set()

        if model_family and self.config.prefer_same_model_family:
            family_providers = self._provider_families.get(
                model_family, []
            )
            candidates.update(family_providers)

        if (
            required_capabilities
            and self.config.prefer_same_capabilities
        ):
            for cap in required_capabilities:
                cap_providers = self._provider_capabilities.get(cap, [])
                candidates.update(cap_providers)

        # Fallback to all tracked providers if no specific candidates
        if not candidates:
            candidates = set(self.health.all_available())

        # Remove primary
        candidates.discard(primary)

        # Filter by health if configured
        if self.config.respect_health_status:
            candidates = {
                p
                for p in candidates
                if self._is_viable_fallback(p)
            }

        # Sort by health (healthiest first)
        return sorted(
            candidates,
            key=lambda p: (
                self.health.uptime_percentage(p),
                -self.health.average_latency(p),
            ),
            reverse=True,
        )

    def _is_viable_fallback(self, provider_id: str) -> bool:
        """Check if a provider is a viable fallback target."""
        if not self.health.is_available(provider_id):
            return False

        uptime = self.health.uptime_percentage(provider_id)
        if uptime < self.config.min_uptime_threshold:
            return False

        latency = self.health.average_latency(provider_id)
        if latency > self.config.max_latency_threshold:
            return False

        return True

    def get_decisions(
        self, limit: int = 10
    ) -> list[FallbackDecision]:
        return self._decisions[-limit:]

    def explain_routing(
        self, provider_id: str
    ) -> dict[str, Any]:
        """Explain why a provider was chosen or rejected for routing."""
        health = self.health.summary(provider_id)
        is_viable = self._is_viable_fallback(provider_id)

        return {
            "provider_id": provider_id,
            "is_viable_fallback": is_viable,
            "health": health,
            "rejection_reasons": (
                []
                if is_viable
                else [
                    reason
                    for reason in [
                        "Not available"
                        if not self.health.is_available(provider_id)
                        else None,
                        "Low uptime"
                        if self.health.uptime_percentage(provider_id)
                        < self.config.min_uptime_threshold
                        else None,
                        "High latency"
                        if self.health.average_latency(provider_id)
                        > self.config.max_latency_threshold
                        else None,
                    ]
                    if reason
                ]
            ),
        }
