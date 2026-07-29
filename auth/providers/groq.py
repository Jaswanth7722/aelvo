"""Groq provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

from openai import AsyncOpenAI

from ..types import (
    CapabilityFlag,
    ModelFamily,
    ModelInfo,
    ProviderCapabilities,
    ProviderConfig,
    ProviderInfo,
    ProviderType,
)
from .base import BaseProvider


class GroqProvider(BaseProvider):
    """Provider implementation for Groq."""

    def __init__(
        self,
        api_key: Optional[str] = None,
    ) -> None:
        info = ProviderInfo(
            provider_id="groq",
            name="Groq",
            provider_type=ProviderType.FOUNDATION,
            description="Groq LPU inference provider",
            docs_url="https://console.groq.com/docs",
            website="https://groq.com",
        )
        config = ProviderConfig(
            provider_id="groq",
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1",
            default_model="llama-3.3-70b-versatile",
            timeout_seconds=30.0,
            max_retries=3,
        )
        capabilities = ProviderCapabilities(
            provider_id="groq",
            capabilities={
                CapabilityFlag.TEXT_GENERATION,
                CapabilityFlag.CHAT_COMPLETION,
                CapabilityFlag.STREAMING,
                CapabilityFlag.TOOL_CALLING,
                CapabilityFlag.FUNCTION_CALLING,
                CapabilityFlag.SYSTEM_PROMPTS,
                CapabilityFlag.JSON_MODE,
                CapabilityFlag.TEMPERATURE,
                CapabilityFlag.MAX_TOKENS,
                CapabilityFlag.TOP_P,
                CapabilityFlag.STOP_SEQUENCES,
            },
            model_families={ModelFamily.LLAMA, ModelFamily.MIXTRAL, ModelFamily.GEMMA},
            max_context_length=131072,
            max_output_tokens=8192,
            supports_streaming=True,
            supports_tool_calling=True,
        )
        super().__init__(info, config, capabilities)

        self._client = AsyncOpenAI(
            api_key=api_key or config.api_key,
            base_url=config.base_url,
        )

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
        params = {"model": model, "messages": messages, "stream": stream}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        response = await self._client.chat.completions.create(**params)  # type: ignore[call-overload]
        return response.model_dump(mode="json") if hasattr(response, "model_dump") else response

    async def chat_completion_stream(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: Optional[list[dict[str, Any]]] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
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
            ModelInfo(model_id="llama-3.3-70b-versatile", family=ModelFamily.LLAMA, provider_id="groq", context_length=131072),
            ModelInfo(model_id="llama-3.1-8b-instant", family=ModelFamily.LLAMA, provider_id="groq", context_length=131072),
            ModelInfo(model_id="mixtral-8x7b-32768", family=ModelFamily.MIXTRAL, provider_id="groq", context_length=32768),
            ModelInfo(model_id="gemma2-9b-it", family=ModelFamily.GEMMA, provider_id="groq", context_length=8192),
        ]
        self._is_initialized = True
