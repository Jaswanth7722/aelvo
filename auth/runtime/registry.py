"""Provider Registry — central registry for all provider implementations."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from ..types import (
    CapabilityFlag,
    ModelFamily,
    ProviderCapabilities,
    ProviderConfig,
    ProviderHealth,
    ProviderInfo,
    ProviderStatus,
    ProviderType,
)

logger = logging.getLogger(__name__)


@dataclass
class ProviderEntry:
    """Internal entry for a registered provider."""

    info: ProviderInfo
    config: ProviderConfig
    capabilities: ProviderCapabilities
    health: ProviderHealth
    client: Any = None  # Provider client instance
    is_active: bool = True
    registered_at: float = 0.0

    def __post_init__(self) -> None:
        import time
        self.registered_at = time.time()


class ProviderRegistry:
    """Central registry for all provider implementations.

    Manages provider lifecycle, lookup, capability queries, and health tracking.
    """

    def __init__(self) -> None:
        self._providers: dict[str, ProviderEntry] = {}
        self._by_capability: dict[CapabilityFlag, list[str]] = {}
        self._by_model_family: dict[ModelFamily, list[str]] = {}
        self._by_provider_type: dict[ProviderType, list[str]] = {}
        self._aliases: dict[str, str] = {}

    # ── Registration ──────────────────────────────────────────────

    def register(
        self,
        info: ProviderInfo,
        config: ProviderConfig,
        capabilities: ProviderCapabilities,
        client: Any = None,
        aliases: Optional[list[str]] = None,
    ) -> None:
        """Register a provider with its capabilities and config."""
        provider_id = info.provider_id

        entry = ProviderEntry(
            info=info,
            config=config,
            capabilities=capabilities,
            health=ProviderHealth(
                provider_id=provider_id,
                status=ProviderStatus.UNKNOWN,
            ),
            client=client,
        )

        self._providers[provider_id] = entry

        # Index by capability
        for cap in capabilities.capabilities:
            if cap not in self._by_capability:
                self._by_capability[cap] = []
            if provider_id not in self._by_capability[cap]:
                self._by_capability[cap].append(provider_id)

        # Index by model family
        for family in capabilities.model_families:
            if family not in self._by_model_family:
                self._by_model_family[family] = []
            if provider_id not in self._by_model_family[family]:
                self._by_model_family[family].append(provider_id)

        # Index by provider type
        ptype = info.provider_type
        if ptype not in self._by_provider_type:
            self._by_provider_type[ptype] = []
        self._by_provider_type[ptype].append(provider_id)

        # Index aliases
        self._aliases[provider_id] = provider_id
        if aliases:
            for alias in aliases:
                self._aliases[alias] = provider_id

        logger.debug("Registered provider: %s (%s)", provider_id, info.name)

    def unregister(self, provider_id: str) -> None:
        """Remove a provider from the registry."""
        resolved = self.resolve_alias(provider_id)
        entry = self._providers.pop(resolved, None)
        if entry is None:
            return

        # Remove from capability index
        for cap in entry.capabilities.capabilities:
            if cap in self._by_capability and resolved in self._by_capability[cap]:
                self._by_capability[cap].remove(resolved)

        # Remove from model family index
        for family in entry.capabilities.model_families:
            if family in self._by_model_family and resolved in self._by_model_family[family]:
                self._by_model_family[family].remove(resolved)

        # Remove from type index
        ptype = entry.info.provider_type
        if ptype in self._by_provider_type and resolved in self._by_provider_type[ptype]:
            self._by_provider_type[ptype].remove(resolved)

        # Remove aliases
        self._aliases = {
            k: v for k, v in self._aliases.items() if v != resolved and k != resolved
        }

        logger.info("Unregistered provider: %s", resolved)

    # ── Lookup ────────────────────────────────────────────────────

    def resolve_alias(self, provider_id: str) -> str:
        """Resolve an alias to its canonical provider ID."""
        return self._aliases.get(provider_id, provider_id)

    def get(self, provider_id: str) -> Optional[ProviderEntry]:
        """Get a provider entry by ID (or alias)."""
        resolved = self.resolve_alias(provider_id)
        return self._providers.get(resolved)

    def get_info(self, provider_id: str) -> Optional[ProviderInfo]:
        entry = self.get(provider_id)
        return entry.info if entry else None

    def get_config(self, provider_id: str) -> Optional[ProviderConfig]:
        entry = self.get(provider_id)
        return entry.config if entry else None

    def get_capabilities(self, provider_id: str) -> Optional[ProviderCapabilities]:
        entry = self.get(provider_id)
        return entry.capabilities if entry else None

    def has_provider(self, provider_id: str) -> bool:
        return self.resolve_alias(provider_id) in self._providers

    # ── Queries ───────────────────────────────────────────────────

    def list_providers(
        self,
        capability: Optional[CapabilityFlag] = None,
        model_family: Optional[ModelFamily] = None,
        provider_type: Optional[ProviderType] = None,
        active_only: bool = True,
    ) -> list[ProviderEntry]:
        """List providers matching the given filters."""
        entries = list(self._providers.values())

        if active_only:
            entries = [e for e in entries if e.is_active]

        if capability:
            pids = set(self._by_capability.get(capability, []))
            entries = [e for e in entries if e.info.provider_id in pids]

        if model_family:
            pids = set(self._by_model_family.get(model_family, []))
            entries = [e for e in entries if e.info.provider_id in pids]

        if provider_type:
            entries = [
                e for e in entries if e.info.provider_type == provider_type
            ]

        return entries

    def list_provider_ids(
        self,
        capability: Optional[CapabilityFlag] = None,
        model_family: Optional[ModelFamily] = None,
        provider_type: Optional[ProviderType] = None,
    ) -> list[str]:
        return [
            e.info.provider_id
            for e in self.list_providers(
                capability=capability,
                model_family=model_family,
                provider_type=provider_type,
            )
        ]

    def find_by_capability(
        self, capability: CapabilityFlag
    ) -> list[str]:
        return list(self._by_capability.get(capability, []))

    def find_by_model_family(
        self, family: ModelFamily
    ) -> list[str]:
        return list(self._by_model_family.get(family, []))

    def find_by_type(
        self, provider_type: ProviderType
    ) -> list[str]:
        return list(self._by_provider_type.get(provider_type, []))

    # ── Health ────────────────────────────────────────────────────

    def update_health(
        self, provider_id: str, health: ProviderHealth
    ) -> None:
        entry = self.get(provider_id)
        if entry:
            entry.health = health

    def get_health(self, provider_id: str) -> Optional[ProviderHealth]:
        entry = self.get(provider_id)
        return entry.health if entry else None

    def get_active_providers(self) -> list[str]:
        return [
            pid
            for pid, entry in self._providers.items()
            if entry.is_active
        ]

    def set_active(self, provider_id: str, active: bool) -> None:
        entry = self.get(provider_id)
        if entry:
            entry.is_active = active

    # ── Stats ─────────────────────────────────────────────────────

    @property
    def count(self) -> int:
        return len(self._providers)

    def summary(self) -> dict[str, Any]:
        return {
            "total_providers": self.count,
            "active_providers": len(self.get_active_providers()),
            "by_type": {
                ptype.name: len(pids)
                for ptype, pids in self._by_provider_type.items()
            },
            "by_capability": {
                cap.name: len(pids)
                for cap, pids in self._by_capability.items()
            },
            "providers": [
                {
                    "id": e.info.provider_id,
                    "name": e.info.name,
                    "type": e.info.provider_type.name,
                    "active": e.is_active,
                    "status": e.health.status.name,
                }
                for e in self._providers.values()
            ],
        }
