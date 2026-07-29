"""LM Studio local provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator
from openai import AsyncOpenAI
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class LMStudioProvider(BaseProvider):
    def __init__(self, base_url: str = "http://localhost:1234") -> None:
        info = ProviderInfo(provider_id="lm_studio", name="LM Studio", provider_type=ProviderType.LOCAL, description="LM Studio local inference server", docs_url="https://lmstudio.ai/docs", website="https://lmstudio.ai")
        config = ProviderConfig(provider_id="lm_studio", api_key="__local__", base_url=f"{base_url}/v1", default_model="local-model", timeout_seconds=300.0, max_retries=2)
        capabilities = ProviderCapabilities(provider_id="lm_studio", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.LOCAL, CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P,
        }, model_families={ModelFamily.LOCAL}, max_context_length=65536, supports_streaming=True, supports_tool_calling=True, is_local=True)
        super().__init__(info, config, capabilities)
        self._client = AsyncOpenAI(api_key="not-needed", base_url=config.base_url)

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
        try:
            models = await self._client.models.list()
            for m in models.data:
                self._models.append(ModelInfo(model_id=m.id, family=ModelFamily.LOCAL, provider_id="lm_studio"))
        except Exception:
            self._models = [ModelInfo(model_id="local-model", family=ModelFamily.LOCAL, provider_id="lm_studio")]
        self._is_initialized = True
