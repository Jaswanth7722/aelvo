"""Anthropic provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

try:
    from anthropic import AsyncAnthropic
except ImportError:
    AsyncAnthropic = None  # type: ignore

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


class AnthropicProvider(BaseProvider):
    """Provider implementation for Anthropic."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ) -> None:
        info = ProviderInfo(
            provider_id="anthropic",
            name="Anthropic",
            provider_type=ProviderType.FOUNDATION,
            description="Anthropic Claude API provider",
            docs_url="https://docs.anthropic.com",
            website="https://anthropic.com",
        )
        config = ProviderConfig(
            provider_id="anthropic",
            api_key=api_key,
            base_url=base_url or "https://api.anthropic.com/v1",
            default_model="claude-3-5-sonnet-20241022",
            timeout_seconds=120.0,
            max_retries=3,
        )
        capabilities = ProviderCapabilities(
            provider_id="anthropic",
            capabilities={
                CapabilityFlag.TEXT_GENERATION,
                CapabilityFlag.CHAT_COMPLETION,
                CapabilityFlag.STREAMING,
                CapabilityFlag.TOOL_CALLING,
                CapabilityFlag.VISION,
                CapabilityFlag.LONG_CONTEXT,
                CapabilityFlag.SYSTEM_PROMPTS,
                CapabilityFlag.STRUCTURED_OUTPUT,
                CapabilityFlag.TEMPERATURE,
                CapabilityFlag.MAX_TOKENS,
                CapabilityFlag.STOP_SEQUENCES,
                CapabilityFlag.TOP_P,
                CapabilityFlag.TOP_K,
            },
            model_families={ModelFamily.CLAUDE_3, ModelFamily.CLAUDE_3_5, ModelFamily.CLAUDE_4},
            max_context_length=200000,
            max_output_tokens=8192,
            supports_streaming=True,
            supports_tool_calling=True,
        )
        super().__init__(info, config, capabilities)

        self._client = AsyncAnthropic(
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
        system = None
        chat_messages = messages
        if messages and messages[0].get("role") == "system":
            system = messages[0]["content"]
            chat_messages = messages[1:]

        params = {
            "model": model,
            "messages": chat_messages,
            "max_tokens": max_tokens or 4096,
        }
        if system:
            params["system"] = system
        if tools:
            params["tools"] = tools
        if temperature is not None:
            params["temperature"] = temperature
        params.update(kwargs)

        response = await self._client.messages.create(**params)  # type: ignore[call-overload, arg-type]
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
        system = None
        chat_messages = messages
        if messages and messages[0].get("role") == "system":
            system = messages[0]["content"]
            chat_messages = messages[1:]

        params = {
            "model": model,
            "messages": chat_messages,
            "max_tokens": max_tokens or 4096,
            "stream": True,
        }
        if system:
            params["system"] = system
        if tools:
            params["tools"] = tools
        if temperature is not None:
            params["temperature"] = temperature
        params.update(kwargs)

        async with self._client.messages.stream(**params) as stream:  # type: ignore[arg-type]
            async for event in stream:
                yield event.model_dump(mode="json") if hasattr(event, "model_dump") else event  # type: ignore[misc]

    async def initialize(self) -> None:
        if self._is_initialized:
            return
        self._models = [
            ModelInfo(model_id="claude-3-5-sonnet-20241022", family=ModelFamily.CLAUDE_3_5, provider_id="anthropic", context_length=200000),
            ModelInfo(model_id="claude-3-5-haiku-20241022", family=ModelFamily.CLAUDE_3_5, provider_id="anthropic", context_length=200000),
            ModelInfo(model_id="claude-3-opus-20240229", family=ModelFamily.CLAUDE_3, provider_id="anthropic", context_length=200000),
            ModelInfo(model_id="claude-3-sonnet-20240229", family=ModelFamily.CLAUDE_3, provider_id="anthropic", context_length=200000),
            ModelInfo(model_id="claude-3-haiku-20240307", family=ModelFamily.CLAUDE_3, provider_id="anthropic", context_length=200000),
        ]
        self._is_initialized = True
