"""Fireworks AI provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
from openai import AsyncOpenAI
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class FireworksProvider(BaseProvider):
    def __init__(self, api_key: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="fireworks", name="Fireworks AI", provider_type=ProviderType.FOUNDATION, description="Fireworks AI inference", docs_url="https://docs.fireworks.ai", website="https://fireworks.ai")
        config = ProviderConfig(provider_id="fireworks", api_key=api_key, base_url="https://api.fireworks.ai/inference/v1", default_model="accounts/fireworks/models/llama-v3p3-70b-instruct", timeout_seconds=60.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="fireworks", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.FUNCTION_CALLING, CapabilityFlag.SYSTEM_PROMPTS,
            CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P, CapabilityFlag.STOP_SEQUENCES,
            CapabilityFlag.IMAGE_GENERATION,
        }, model_families={ModelFamily.LLAMA, ModelFamily.MIXTRAL, ModelFamily.DEEPSEEK, ModelFamily.QWEN, ModelFamily.FIREWORKS},
            max_context_length=131072, supports_streaming=True, supports_tool_calling=True)
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
            ModelInfo(model_id="accounts/fireworks/models/llama-v3p3-70b-instruct", family=ModelFamily.LLAMA, provider_id="fireworks", context_length=131072),
            ModelInfo(model_id="accounts/fireworks/models/llama-v3p1-405b-instruct", family=ModelFamily.LLAMA, provider_id="fireworks", context_length=131072),
            ModelInfo(model_id="accounts/fireworks/models/mixtral-8x7b-instruct", family=ModelFamily.MIXTRAL, provider_id="fireworks", context_length=32768),
            ModelInfo(model_id="accounts/fireworks/models/deepseek-v3", family=ModelFamily.DEEPSEEK, provider_id="fireworks", context_length=65536),
        ]
        self._is_initialized = True
