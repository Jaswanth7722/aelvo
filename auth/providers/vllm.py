"""vLLM local provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
from openai import AsyncOpenAI
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class VLLMProvider(BaseProvider):
    def __init__(self, base_url: str = "http://localhost:8000") -> None:
        info = ProviderInfo(provider_id="vllm", name="vLLM", provider_type=ProviderType.LOCAL, description="vLLM inference server", docs_url="https://docs.vllm.ai", website="https://vllm.ai")
        config = ProviderConfig(provider_id="vllm", api_key="__local__", base_url=f"{base_url}/v1", default_model="local-model", timeout_seconds=300.0, max_retries=2)
        capabilities = ProviderCapabilities(provider_id="vllm", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.LOCAL, CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P,
            CapabilityFlag.STOP_SEQUENCES, CapabilityFlag.FREQUENCY_PENALTY, CapabilityFlag.PRESENCE_PENALTY,
        }, model_families={ModelFamily.LOCAL}, max_context_length=131072, supports_streaming=True, supports_tool_calling=True, is_local=True)
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
                self._models.append(ModelInfo(model_id=m.id, family=ModelFamily.LOCAL, provider_id="vllm"))
        except Exception:
            self._models = [ModelInfo(model_id="local-model", family=ModelFamily.LOCAL, provider_id="vllm")]
        self._is_initialized = True
