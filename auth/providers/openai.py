"""OpenAI provider implementation."""

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


class OpenAIProvider(BaseProvider):
    """Provider implementation for OpenAI."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        organization: Optional[str] = None,
    ) -> None:
        info = ProviderInfo(
            provider_id="openai",
            name="OpenAI",
            provider_type=ProviderType.FOUNDATION,
            description="OpenAI API provider",
            docs_url="https://platform.openai.com/docs",
            website="https://openai.com",
        )
        config = ProviderConfig(
            provider_id="openai",
            api_key=api_key,
            base_url=base_url or "https://api.openai.com/v1",
            organization=organization,
            default_model="gpt-4o",
            timeout_seconds=60.0,
            max_retries=3,
        )
        capabilities = ProviderCapabilities(
            provider_id="openai",
            capabilities={
                CapabilityFlag.TEXT_GENERATION,
                CapabilityFlag.CHAT_COMPLETION,
                CapabilityFlag.STREAMING,
                CapabilityFlag.TOOL_CALLING,
                CapabilityFlag.FUNCTION_CALLING,
                CapabilityFlag.STRUCTURED_OUTPUT,
                CapabilityFlag.JSON_MODE,
                CapabilityFlag.SYSTEM_PROMPTS,
                CapabilityFlag.VISION,
                CapabilityFlag.IMAGE_GENERATION,
                CapabilityFlag.AUDIO_TRANSCRIPTION,
                CapabilityFlag.EMBEDDINGS,
                CapabilityFlag.LONG_CONTEXT,
                CapabilityFlag.REASONING,
                CapabilityFlag.MULTIPLE_FUNCTIONS,
                CapabilityFlag.PARALLEL_TOOL_CALLS,
                CapabilityFlag.LOGPROBS,
                CapabilityFlag.SEED_CONTROL,
                CapabilityFlag.STOP_SEQUENCES,
                CapabilityFlag.FREQUENCY_PENALTY,
                CapabilityFlag.PRESENCE_PENALTY,
                CapabilityFlag.TEMPERATURE,
                CapabilityFlag.TOP_P,
                CapabilityFlag.MAX_TOKENS,
            },
            model_families={ModelFamily.GPT4, ModelFamily.GPT4O, ModelFamily.GPT4O_MINI, ModelFamily.O1, ModelFamily.O3, ModelFamily.GPT4_TURBO, ModelFamily.EMBEDDING},
            max_context_length=128000,
            max_output_tokens=16384,
            supports_streaming=True,
            supports_tool_calling=True,
            supports_structured_output=True,
        )
        super().__init__(info, config, capabilities)

        self._client = AsyncOpenAI(
            api_key=api_key or config.api_key,
            base_url=config.base_url,
            organization=config.organization,
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
        params = {
            "model": model,
            "messages": messages,
            "stream": stream,
        }
        if tools:
            params["tools"] = tools
        if temperature is not None:
            params["temperature"] = temperature
        if max_tokens is not None:
            params["max_tokens"] = max_tokens
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
        params = {
            "model": model,
            "messages": messages,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if tools:
            params["tools"] = tools
        if temperature is not None:
            params["temperature"] = temperature
        if max_tokens is not None:
            params["max_tokens"] = max_tokens
        params.update(kwargs)

        stream_response = await self._client.chat.completions.create(**params)  # type: ignore[call-overload]
        async for chunk in stream_response:
            yield chunk.model_dump(mode="json") if hasattr(chunk, "model_dump") else chunk

    async def embed(
        self,
        model: str = "text-embedding-3-small",
        inputs: Optional[list[str]] = None,
        **kwargs: Any,
    ) -> list[list[float]]:
        if inputs is None:
            inputs = []
        response = await self._client.embeddings.create(
            model=model,
            input=inputs,
            **kwargs,
        )
        return [item.embedding for item in response.data]

    async def initialize(self) -> None:
        if self._is_initialized:
            return
        self._models = [
            ModelInfo(model_id="gpt-4o", family=ModelFamily.GPT4O, provider_id="openai", context_length=128000),
            ModelInfo(model_id="gpt-4o-mini", family=ModelFamily.GPT4O_MINI, provider_id="openai", context_length=128000),
            ModelInfo(model_id="o1", family=ModelFamily.O1, provider_id="openai", context_length=200000),
            ModelInfo(model_id="o3-mini", family=ModelFamily.O3, provider_id="openai", context_length=200000),
            ModelInfo(model_id="gpt-4-turbo", family=ModelFamily.GPT4_TURBO, provider_id="openai", context_length=128000),
            ModelInfo(model_id="text-embedding-3-small", family=ModelFamily.EMBEDDING, provider_id="openai"),
            ModelInfo(model_id="text-embedding-3-large", family=ModelFamily.EMBEDDING, provider_id="openai"),
        ]
        self._is_initialized = True
