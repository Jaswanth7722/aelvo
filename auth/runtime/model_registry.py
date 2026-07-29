"""Model Registry — canonical model definitions and capability mapping."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from ..types import (
    CapabilityFlag,
    ModelCapability,
    ModelFamily,
    ModelInfo,
)

logger = logging.getLogger(__name__)


@dataclass
class ModelEntry:
    """Internal entry for a registered model."""

    info: ModelInfo
    provider_ids: list[str] = field(default_factory=list)
    registered_at: float = 0.0

    def __post_init__(self) -> None:
        import time
        self.registered_at = time.time()


class ModelRegistry:
    """Central registry for model definitions and capabilities.

    Maintains canonical model entries, maps models to providers,
    and tracks model-level capabilities across all providers.
    """

    def __init__(self) -> None:
        self._models: dict[str, ModelEntry] = {}
        self._by_family: dict[ModelFamily, list[str]] = {}
        self._by_capability: dict[CapabilityFlag, list[str]] = {}
        self._by_provider: dict[str, list[str]] = {}
        self._aliases: dict[str, str] = {}

    # ── Registration ──────────────────────────────────────────────

    def register(
        self,
        info: ModelInfo,
        provider_ids: Optional[list[str]] = None,
        aliases: Optional[list[str]] = None,
    ) -> None:
        """Register a model with its metadata."""
        model_id = info.model_id

        entry = ModelEntry(info=info, provider_ids=provider_ids or [])

        self._models[model_id] = entry

        # Index by family
        family = info.family
        if family not in self._by_family:
            self._by_family[family] = []
        if model_id not in self._by_family[family]:
            self._by_family[family].append(model_id)

        # Index by capability
        for cap in info.capabilities:
            if cap not in self._by_capability:
                self._by_capability[cap] = []
            if model_id not in self._by_capability[cap]:
                self._by_capability[cap].append(model_id)

        # Index by provider
        for pid in entry.provider_ids:
            if pid not in self._by_provider:
                self._by_provider[pid] = []
            if model_id not in self._by_provider[pid]:
                self._by_provider[pid].append(model_id)

        # Index aliases
        self._aliases[model_id] = model_id
        if aliases:
            for alias in aliases:
                self._aliases[alias] = model_id

        logger.debug("Registered model: %s (family=%s)", model_id, family.name)

    def register_batch(
        self,
        models: list[tuple[ModelInfo, Optional[list[str]]]],
    ) -> None:
        """Register multiple models at once."""
        for info, provider_ids in models:
            self.register(info=info, provider_ids=provider_ids)

    # ── Lookup ────────────────────────────────────────────────────

    def resolve_alias(self, model_id: str) -> str:
        return self._aliases.get(model_id, model_id)

    def get(self, model_id: str) -> Optional[ModelEntry]:
        resolved = self.resolve_alias(model_id)
        return self._models.get(resolved)

    def get_info(self, model_id: str) -> Optional[ModelInfo]:
        entry = self.get(model_id)
        return entry.info if entry else None

    def has_model(self, model_id: str) -> bool:
        return self.resolve_alias(model_id) in self._models

    # ── Queries ───────────────────────────────────────────────────

    def list_models(
        self,
        family: Optional[ModelFamily] = None,
        capability: Optional[CapabilityFlag] = None,
        provider: Optional[str] = None,
    ) -> list[ModelEntry]:
        """List models matching the given filters."""
        entries = list(self._models.values())

        if family:
            entries = [e for e in entries if e.info.family == family]

        if capability:
            entries = [
                e for e in entries if capability in e.info.capabilities
            ]

        if provider:
            entries = [
                e for e in entries if provider in e.provider_ids
            ]

        return entries

    def list_model_ids(
        self,
        family: Optional[ModelFamily] = None,
        capability: Optional[CapabilityFlag] = None,
        provider: Optional[str] = None,
    ) -> list[str]:
        return [
            e.info.model_id
            for e in self.list_models(
                family=family,
                capability=capability,
                provider=provider,
            )
        ]

    def find_by_family(self, family: ModelFamily) -> list[str]:
        return list(self._by_family.get(family, []))

    def find_by_capability(self, cap: CapabilityFlag) -> list[str]:
        return list(self._by_capability.get(cap, []))

    def find_by_provider(self, provider_id: str) -> list[str]:
        return list(self._by_provider.get(provider_id, []))

    def find_reasoning_models(self) -> list[str]:
        """Find all models with reasoning capability."""
        return self.find_by_capability(CapabilityFlag.REASONING)

    def find_tool_calling_models(self) -> list[str]:
        """Find all models with tool-calling capability."""
        return self.find_by_capability(CapabilityFlag.TOOL_CALLING)

    def find_streaming_models(self) -> list[str]:
        """Find all models with streaming capability."""
        return self.find_by_capability(CapabilityFlag.STREAMING)

    def find_multimodal_models(self) -> list[str]:
        """Find all models with multimodal capability."""
        return self.find_by_capability(CapabilityFlag.MULTIMODAL)

    def find_embedding_models(self) -> list[str]:
        """Find all models with embedding capability."""
        return self.find_by_capability(CapabilityFlag.EMBEDDINGS)

    # ── Provider Mapping ─────────────────────────────────────────

    def add_provider_for_model(
        self, model_id: str, provider_id: str
    ) -> None:
        entry = self.get(model_id)
        if entry and provider_id not in entry.provider_ids:
            entry.provider_ids.append(provider_id)
            if provider_id not in self._by_provider:
                self._by_provider[provider_id] = []
            if model_id not in self._by_provider[provider_id]:
                self._by_provider[provider_id].append(model_id)

    def get_providers_for_model(self, model_id: str) -> list[str]:
        entry = self.get(model_id)
        return list(entry.provider_ids) if entry else []

    def get_models_for_provider(
        self, provider_id: str
    ) -> list[ModelInfo]:
        return [
            self._models[mid].info
            for mid in self._by_provider.get(provider_id, [])
            if mid in self._models
        ]

    # ── Stats ─────────────────────────────────────────────────────

    @property
    def count(self) -> int:
        return len(self._models)

    def summary(self) -> dict[str, Any]:
        return {
            "total_models": self.count,
            "by_family": {
                f.name: len(mids)
                for f, mids in self._by_family.items()
            },
            "by_capability": {
                c.name: len(mids)
                for c, mids in self._by_capability.items()
            },
            "by_provider": {
                pid: len(mids)
                for pid, mids in self._by_provider.items()
            },
        }
