"""Capability Registry — provider capability management and queries."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from ..types import (
    CapabilityFlag,
    ProviderCapabilities,
)

logger = logging.getLogger(__name__)


@dataclass
class CapabilityEntry:
    """Tracks a specific capability across providers."""

    capability: CapabilityFlag
    provider_ids: list[str] = field(default_factory=list)
    description: str = ""


class CapabilityRegistry:
    """Registry for provider capabilities with advanced query support.

    Enables runtime capability discovery, intersection/union queries,
    and capability-aware provider selection.
    """

    def __init__(self) -> None:
        self._capabilities: dict[CapabilityFlag, CapabilityEntry] = {}
        self._provider_caps: dict[str, ProviderCapabilities] = {}

    # ── Registration ──────────────────────────────────────────────

    def register_capability(
        self,
        capability: CapabilityFlag,
        description: str = "",
    ) -> None:
        """Register a capability flag."""
        if capability not in self._capabilities:
            self._capabilities[capability] = CapabilityEntry(
                capability=capability,
                description=description or capability.value,
            )

    def register_provider_capabilities(
        self,
        provider_id: str,
        capabilities: ProviderCapabilities,
    ) -> None:
        """Register capabilities for a provider."""
        self._provider_caps[provider_id] = capabilities

        for cap in capabilities.capabilities:
            self.register_capability(cap)
            if provider_id not in self._capabilities[cap].provider_ids:
                self._capabilities[cap].provider_ids.append(provider_id)

    def unregister_provider(self, provider_id: str) -> None:
        """Remove a provider's capabilities."""
        self._provider_caps.pop(provider_id, None)
        for entry in self._capabilities.values():
            if provider_id in entry.provider_ids:
                entry.provider_ids.remove(provider_id)

    # ── Queries ───────────────────────────────────────────────────

    def get_capability(self, cap: CapabilityFlag) -> Optional[CapabilityEntry]:
        return self._capabilities.get(cap)

    def has_capability(self, provider_id: str, cap: CapabilityFlag) -> bool:
        caps = self._provider_caps.get(provider_id)
        if caps is None:
            return False
        return cap in caps.capabilities

    def providers_with_capability(
        self, cap: CapabilityFlag
    ) -> list[str]:
        entry = self._capabilities.get(cap)
        return list(entry.provider_ids) if entry else []

    def providers_with_all(
        self, *caps: CapabilityFlag
    ) -> list[str]:
        """Find providers that have ALL specified capabilities."""
        if not caps:
            return list(self._provider_caps.keys())

        result: set[str] = set()
        for i, cap in enumerate(caps):
            providers = set(self.providers_with_capability(cap))
            if i == 0:
                result = providers
            else:
                result &= providers
        return sorted(result)

    def providers_with_any(
        self, *caps: CapabilityFlag
    ) -> list[str]:
        """Find providers that have ANY of the specified capabilities."""
        result: set[str] = set()
        for cap in caps:
            result |= set(self.providers_with_capability(cap))
        return sorted(result)

    def providers_without(self, cap: CapabilityFlag) -> list[str]:
        """Find providers that do NOT have the specified capability."""
        excluded = set(self.providers_with_capability(cap))
        return [
            pid
            for pid in self._provider_caps
            if pid not in excluded
        ]

    def get_provider_capabilities(
        self, provider_id: str
    ) -> Optional[ProviderCapabilities]:
        """Get all capabilities for a provider."""
        return self._provider_caps.get(provider_id)

    def list_all_capabilities(self) -> list[CapabilityFlag]:
        return list(self._capabilities.keys())

    def list_providers_with_context(
        self,
        min_context: Optional[int] = None,
        supports_streaming: Optional[bool] = None,
        supports_tool_calling: Optional[bool] = None,
        supports_multimodal: Optional[bool] = None,
        is_local: Optional[bool] = None,
    ) -> list[str]:
        """Advanced provider query with multiple filters."""
        results = set(self._provider_caps.keys())

        if min_context is not None:
            results = {
                pid
                for pid in results
                if (
                    caps := self._provider_caps.get(pid)
                )
                and caps.max_context_length is not None
                and caps.max_context_length >= min_context
            }

        if supports_streaming is True:
            results &= set(
                self.providers_with_capability(CapabilityFlag.STREAMING)
            )
        elif supports_streaming is False:
            results -= set(
                self.providers_with_capability(CapabilityFlag.STREAMING)
            )

        if supports_tool_calling is True:
            results &= set(
                self.providers_with_capability(CapabilityFlag.TOOL_CALLING)
            )
        elif supports_tool_calling is False:
            results -= set(
                self.providers_with_capability(CapabilityFlag.TOOL_CALLING)
            )

        if supports_multimodal is True:
            results &= set(
                self.providers_with_capability(CapabilityFlag.MULTIMODAL)
            )
        elif supports_multimodal is False:
            results -= set(
                self.providers_with_capability(CapabilityFlag.MULTIMODAL)
            )

        if is_local is True:
            results &= set(
                self.providers_with_capability(CapabilityFlag.LOCAL)
            )
        elif is_local is False:
            results -= set(
                self.providers_with_capability(CapabilityFlag.LOCAL)
            )

        return sorted(results)

    def capability_matrix(self) -> dict[str, list[str]]:
        """Generate a capability matrix for all providers."""
        matrix: dict[str, list[str]] = {}
        for pid, caps in self._provider_caps.items():
            matrix[pid] = [c.name for c in caps.capabilities]
        return matrix

    @property
    def provider_count(self) -> int:
        return len(self._provider_caps)

    @property
    def capability_count(self) -> int:
        return len(self._capabilities)
