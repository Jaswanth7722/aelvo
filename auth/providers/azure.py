"""Azure OpenAI provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
from openai import AsyncAzureOpenAI
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class AzureProvider(BaseProvider):
    def __init__(self, endpoint: str = "", api_key: Optional[str] = None, api_version: str = "2024-06-01") -> None:
        info = ProviderInfo(provider_id="azure", name="Azure OpenAI", provider_type=ProviderType.CLOUD, description="Azure OpenAI Service", docs_url="https://learn.microsoft.com/azure/ai-services/openai", website="https://azure.microsoft.com/products/ai-services/openai-service")
        config = ProviderConfig(provider_id="azure", api_key=api_key, base_url=endpoint, default_model="gpt-4o", timeout_seconds=60.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="azure", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.FUNCTION_CALLING, CapabilityFlag.STRUCTURED_OUTPUT,
            CapabilityFlag.SYSTEM_PROMPTS, CapabilityFlag.VISION, CapabilityFlag.EMBEDDINGS,
            CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P, CapabilityFlag.STOP_SEQUENCES,
            CapabilityFlag.FREQUENCY_PENALTY, CapabilityFlag.PRESENCE_PENALTY,
        }, model_families={ModelFamily.GPT4O, ModelFamily.GPT4O_MINI, ModelFamily.GPT4, ModelFamily.GPT4_TURBO, ModelFamily.EMBEDDING},
            max_context_length=128000, supports_streaming=True, supports_tool_calling=True, is_local=False)
        super().__init__(info, config, capabilities)
        self._client = AsyncAzureOpenAI(azure_endpoint=endpoint or (config.base_url or ""), api_key=api_key or config.api_key, api_version=api_version)  # type: ignore[arg-type]

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        params = {"model": model, "messages": messages, "stream": stream}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        response = await self._client.chat.completions.create(**params)
        return response.model_dump(mode="json") if hasattr(response, "model_dump") else response

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        params = {"model": model, "messages": messages, "stream": True}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        stream_response = await self._client.chat.completions.create(**params)
        async for chunk in stream_response:
            yield chunk.model_dump(mode="json") if hasattr(chunk, "model_dump") else chunk

    async def embed(self, model: str, inputs: list[str], **kwargs) -> list[list[float]]:
        response = await self._client.embeddings.create(model=model, input=inputs, **kwargs)
        return [item.embedding for item in response.data]

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="gpt-4o", family=ModelFamily.GPT4O, provider_id="azure", context_length=128000),
            ModelInfo(model_id="gpt-4o-mini", family=ModelFamily.GPT4O_MINI, provider_id="azure", context_length=128000),
            ModelInfo(model_id="text-embedding-3-small", family=ModelFamily.EMBEDDING, provider_id="azure"),
        ]
        self._is_initialized = True
