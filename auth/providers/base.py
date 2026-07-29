"""Abstract base class for all provider implementations."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Optional

from ..types import (
    ModelInfo,
    ProviderCapabilities,
    ProviderConfig,
    ProviderInfo,
    TokenUsage,
)

logger = logging.getLogger(__name__)


class BaseProvider(ABC):
    """Abstract base class for all provider implementations.

    All providers must implement these methods to integrate with
    AELVO's provider runtime. The runtime handles auth, retry,
    fallback, streaming normalization, etc.
    """

    def __init__(
        self,
        info: ProviderInfo,
        config: ProviderConfig,
        capabilities: ProviderCapabilities,
    ) -> None:
        self.info = info
        self.config = config
        self.capabilities = capabilities
        self._is_initialized = False
        self._models: list[ModelInfo] = []
        logger.info(
            "Initialized provider: %s (%s)",
            self.info.provider_id,
            self.info.name,
        )

    @abstractmethod
    async def chat_completion(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: Optional[list[dict[str, Any]]] = None,
        stream: bool = False,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Send a chat completion request to the provider."""
        ...

    @abstractmethod
    async def chat_completion_stream(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: Optional[list[dict[str, Any]]] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream a chat completion from the provider."""
        if False:
            yield  # type: ignore[unreachable]

    async def embed(
        self,
        model: str,
        inputs: list[str],
        **kwargs: Any,
    ) -> list[list[float]]:
        """Generate embeddings. Override if provider supports embeddings."""
        raise NotImplementedError(
            f"Provider {self.info.provider_id} does not support embeddings"
        )

    async def health_check(self) -> bool:
        """Check if the provider is reachable and healthy."""
        try:
            await self.chat_completion(
                model=self._models[0].model_id if self._models else "gpt-3.5-turbo",
                messages=[{"role": "user", "content": "ping"}],
                max_tokens=1,
            )
            return True
        except Exception:
            return False

    async def initialize(self) -> None:
        """Initialize provider (fetch models, validate credentials, etc.)."""
        if self._is_initialized:
            return
        self._is_initialized = True

    @property
    def provider_id(self) -> str:
        return self.info.provider_id

    @property
    def name(self) -> str:
        return self.info.name

    @property
    def supported_models(self) -> list[ModelInfo]:
        return list(self._models)

    def add_model(self, model: ModelInfo) -> None:
        self._models.append(model)

    def extract_usage(self, response: dict[str, Any]) -> TokenUsage:
        """Extract token usage from a provider response."""
        usage = response.get("usage", {})
        return TokenUsage(
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.provider_id}, name={self.name})"
