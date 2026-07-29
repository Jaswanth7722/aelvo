"""DeepSeek provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
from openai import AsyncOpenAI
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class DeepSeekProvider(BaseProvider):
    def __init__(self, api_key: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="deepseek", name="DeepSeek", provider_type=ProviderType.FOUNDATION, description="DeepSeek API provider", docs_url="https://platform.deepseek.com/docs", website="https://deepseek.com")
        config = ProviderConfig(provider_id="deepseek", api_key=api_key, base_url="https://api.deepseek.com/v1", default_model="deepseek-chat", timeout_seconds=60.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="deepseek", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.FUNCTION_CALLING, CapabilityFlag.SYSTEM_PROMPTS,
            CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P, CapabilityFlag.STOP_SEQUENCES,
            CapabilityFlag.FREQUENCY_PENALTY, CapabilityFlag.PRESENCE_PENALTY,
        }, model_families={ModelFamily.DEEPSEEK}, max_context_length=65536, supports_streaming=True, supports_tool_calling=True)
        super().__init__(info, config, capabilities)
        self._client = AsyncOpenAI(api_key=api_key or config.api_key, base_url=config.base_url)

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

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="deepseek-chat", family=ModelFamily.DEEPSEEK, provider_id="deepseek", context_length=65536),
            ModelInfo(model_id="deepseek-reasoner", family=ModelFamily.DEEPSEEK, provider_id="deepseek", context_length=65536),
        ]
        self._is_initialized = True
