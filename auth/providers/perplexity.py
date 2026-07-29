"""Perplexity AI provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
from openai import AsyncOpenAI
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class PerplexityProvider(BaseProvider):
    def __init__(self, api_key: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="perplexity", name="Perplexity AI", provider_type=ProviderType.FOUNDATION, description="Perplexity AI API", docs_url="https://docs.perplexity.ai", website="https://perplexity.ai")
        config = ProviderConfig(provider_id="perplexity", api_key=api_key, base_url="https://api.perplexity.ai", default_model="sonar-pro", timeout_seconds=60.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="perplexity", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.SYSTEM_PROMPTS, CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P,
        }, model_families={ModelFamily.PERPLEXITY}, max_context_length=200000, supports_streaming=True)
        super().__init__(info, config, capabilities)
        self._client = AsyncOpenAI(api_key=api_key or config.api_key, base_url=config.base_url)

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        params = {"model": model, "messages": messages, "stream": stream}
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        response = await self._client.chat.completions.create(**params)
        return response.model_dump(mode="json") if hasattr(response, "model_dump") else response

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        params = {"model": model, "messages": messages, "stream": True}
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        stream_response = await self._client.chat.completions.create(**params)
        async for chunk in stream_response:
            yield chunk.model_dump(mode="json") if hasattr(chunk, "model_dump") else chunk

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="sonar-pro", family=ModelFamily.PERPLEXITY, provider_id="perplexity", context_length=200000),
            ModelInfo(model_id="sonar-small", family=ModelFamily.PERPLEXITY, provider_id="perplexity", context_length=200000),
        ]
        self._is_initialized = True
