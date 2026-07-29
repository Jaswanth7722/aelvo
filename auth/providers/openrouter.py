"""OpenRouter provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
from openai import AsyncOpenAI
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class OpenRouterProvider(BaseProvider):
    def __init__(self, api_key: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="openrouter", name="OpenRouter", provider_type=ProviderType.GATEWAY, description="OpenRouter unified API", docs_url="https://openrouter.ai/docs", website="https://openrouter.ai")
        config = ProviderConfig(provider_id="openrouter", api_key=api_key, base_url="https://openrouter.ai/api/v1", default_model="openai/gpt-4o", timeout_seconds=60.0, max_retries=3,
                                extra_headers={"HTTP-Referer": "https://aelvo.dev", "X-Title": "AELVO"})
        capabilities = ProviderCapabilities(provider_id="openrouter", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.FUNCTION_CALLING, CapabilityFlag.SYSTEM_PROMPTS,
            CapabilityFlag.VISION, CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P,
            CapabilityFlag.STOP_SEQUENCES,
        }, model_families={ModelFamily.ANY}, max_context_length=200000, supports_streaming=True, supports_tool_calling=True)
        super().__init__(info, config, capabilities)
        self._client = AsyncOpenAI(api_key=api_key or config.api_key, base_url=config.base_url)

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        params = {"model": model, "messages": messages, "stream": stream}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        response = await self._client.chat.completions.create(**params, extra_headers=self.config.extra_headers)
        return response.model_dump(mode="json") if hasattr(response, "model_dump") else response

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        params = {"model": model, "messages": messages, "stream": True}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        stream_response = await self._client.chat.completions.create(**params, extra_headers=self.config.extra_headers)
        async for chunk in stream_response:
            yield chunk.model_dump(mode="json") if hasattr(chunk, "model_dump") else chunk

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="openai/gpt-4o", family=ModelFamily.GPT4O, provider_id="openrouter", context_length=128000),
            ModelInfo(model_id="openai/gpt-4o-mini", family=ModelFamily.GPT4O_MINI, provider_id="openrouter", context_length=128000),
            ModelInfo(model_id="anthropic/claude-3.5-sonnet", family=ModelFamily.CLAUDE_3_5, provider_id="openrouter", context_length=200000),
            ModelInfo(model_id="google/gemini-2.0-flash-001", family=ModelFamily.GEMINI_2, provider_id="openrouter", context_length=1048576),
            ModelInfo(model_id="meta-llama/llama-3.3-70b-instruct", family=ModelFamily.LLAMA, provider_id="openrouter", context_length=131072),
            ModelInfo(model_id="deepseek/deepseek-chat", family=ModelFamily.DEEPSEEK, provider_id="openrouter", context_length=65536),
        ]
        self._is_initialized = True
