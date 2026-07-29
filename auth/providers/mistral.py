"""Mistral AI provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

from openai import AsyncOpenAI

from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class MistralProvider(BaseProvider):
    def __init__(self, api_key: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="mistral", name="Mistral AI", provider_type=ProviderType.FOUNDATION, description="Mistral AI API provider", docs_url="https://docs.mistral.ai", website="https://mistral.ai")
        config = ProviderConfig(provider_id="mistral", api_key=api_key, base_url="https://api.mistral.ai/v1", default_model="mistral-large-latest", timeout_seconds=60.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="mistral", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.FUNCTION_CALLING, CapabilityFlag.JSON_MODE,
            CapabilityFlag.SYSTEM_PROMPTS, CapabilityFlag.VISION, CapabilityFlag.TEMPERATURE,
            CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P, CapabilityFlag.STOP_SEQUENCES,
        }, model_families={ModelFamily.MISTRAL}, max_context_length=128000, max_output_tokens=8192,
            supports_streaming=True, supports_tool_calling=True)
        super().__init__(info, config, capabilities)
        self._client = AsyncOpenAI(api_key=api_key or config.api_key, base_url=config.base_url)

    async def chat_completion(self, model: str, messages: list[dict[str, Any]], tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        params = {"model": model, "messages": messages, "stream": stream}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        response = await self._client.chat.completions.create(**params)  # type: ignore[call-overload]
        return response.model_dump(mode="json") if hasattr(response, "model_dump") else response

    async def chat_completion_stream(self, model: str, messages: list[dict[str, Any]], tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        params = {"model": model, "messages": messages, "stream": True}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        stream_response = await self._client.chat.completions.create(**params)  # type: ignore[call-overload]
        async for chunk in stream_response:
            yield chunk.model_dump(mode="json") if hasattr(chunk, "model_dump") else chunk

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="mistral-large-latest", family=ModelFamily.MISTRAL, provider_id="mistral", context_length=128000),
            ModelInfo(model_id="mistral-small-latest", family=ModelFamily.MISTRAL, provider_id="mistral", context_length=32000),
            ModelInfo(model_id="codestral-latest", family=ModelFamily.MISTRAL, provider_id="mistral", context_length=256000),
        ]
        self._is_initialized = True
