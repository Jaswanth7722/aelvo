"""Capability Inspector — inspect and compare provider capabilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from ..runtime.capability import CapabilityRegistry
from ..runtime.model_registry import ModelRegistry


@dataclass
class CapabilityReport:
    """Report of a provider's capabilities."""

    provider_id: str
    capabilities: list[str] = field(default_factory=list)
    model_families: list[str] = field(default_factory=list)
    missing_capabilities: list[str] = field(default_factory=list)
    max_context: int = 0
    supports_streaming: bool = False
    supports_tool_calling: bool = False
    supports_multimodal: bool = False
    is_local: bool = False
    models_count: int = 0


class CapabilityInspector:
    """Inspects and compares provider capabilities."""

    def __init__(
        self,
        capability_registry: CapabilityRegistry,
        model_registry: ModelRegistry,
    ) -> None:
        self._capability = capability_registry
        self._models = model_registry

    def inspect(self, provider_id: str) -> CapabilityReport:
        """Inspect a single provider's capabilities."""
        caps = self._capability.get_provider_capabilities(provider_id)
        models = self._models.find_by_provider(provider_id)

        all_caps = {c for c in self._capability.list_all_capabilities()}

        return CapabilityReport(
            provider_id=provider_id,
            capabilities=[c.name for c in caps.capabilities] if caps else [],
            model_families=[f.name for f in caps.model_families] if caps else [],
            missing_capabilities=sorted(
                c.name for c in all_caps - (caps.capabilities if caps else set())
            ) if all_caps else [],
            max_context=caps.max_context_length if caps else 0,
            supports_streaming=caps.supports_streaming if caps else False,
            supports_tool_calling=caps.supports_tool_calling if caps else False,
            supports_multimodal=caps.supports_multimodal if caps else False,
            is_local=caps.is_local if caps else False,
            models_count=len(models),
        )

    def capability_matrix(self) -> dict[str, list[str]]:
        """Generate a capability matrix for all providers."""
        return self._capability.capability_matrix()

    def find_providers_for_task(self, task_type: str) -> list[str]:
        """Find providers suitable for a given task type."""
        task_caps = {
            "chat": ["CHAT_COMPLETION", "TEXT_GENERATION"],
            "streaming": ["CHAT_COMPLETION", "STREAMING"],
            "tool_calling": ["CHAT_COMPLETION", "TOOL_CALLING"],
            "vision": ["CHAT_COMPLETION", "VISION"],
            "embeddings": ["EMBEDDINGS"],
            "reasoning": ["REASONING"],
            "local": ["LOCAL"],
            "image_generation": ["IMAGE_GENERATION"],
            "audio": ["AUDIO_TRANSCRIPTION"],
            "all": [],
        }

        required_caps = task_caps.get(task_type, [])
        if not required_caps:
            return sorted(self._capability._provider_caps.keys())

        from ..types import CapabilityFlag
        flags = []
        for cap_name in required_caps:
            try:
                flags.append(CapabilityFlag[cap_name])
            except KeyError:
                continue

        return self._capability.providers_with_all(*flags) if flags else []

    def compare_providers(self, provider_ids: list[str]) -> dict[str, CapabilityReport]:
        """Compare multiple providers side by side."""
        return {pid: self.inspect(pid) for pid in provider_ids}
