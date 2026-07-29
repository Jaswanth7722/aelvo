"""Cohere provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

try:
    import cohere
except ImportError:
    cohere = None  # type: ignore

from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class CohereProvider(BaseProvider):
    def __init__(self, api_key: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="cohere", name="Cohere", provider_type=ProviderType.FOUNDATION, description="Cohere API provider", docs_url="https://docs.cohere.com", website="https://cohere.com")
        config = ProviderConfig(provider_id="cohere", api_key=api_key, base_url="https://api.cohere.com/v2", default_model="command-r-plus-08-2024", timeout_seconds=60.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="cohere", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.FUNCTION_CALLING, CapabilityFlag.SYSTEM_PROMPTS,
            CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P,
            CapabilityFlag.STOP_SEQUENCES, CapabilityFlag.EMBEDDINGS, CapabilityFlag.RERANKING,
        }, model_families={ModelFamily.COMMAND_R}, max_context_length=128000, max_output_tokens=4096,
            supports_streaming=True, supports_tool_calling=True)
        super().__init__(info, config, capabilities)
        self._client = cohere.AsyncClientV2(api_key=api_key or config.api_key)

    async def chat_completion(self, model: str, messages: list[dict[str, Any]], tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        params = {"model": model, "messages": messages}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        response = await self._client.chat(**params)
        return response.model_dump(mode="json") if hasattr(response, "model_dump") else response

    async def chat_completion_stream(self, model: str, messages: list[dict[str, Any]], tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        params = {"model": model, "messages": messages, "stream": True}
        if tools: params["tools"] = tools
        if temperature is not None: params["temperature"] = temperature
        if max_tokens is not None: params["max_tokens"] = max_tokens
        params.update(kwargs)
        stream_response = await self._client.chat(**params)
        async for event in stream_response:
            yield event.model_dump(mode="json") if hasattr(event, "model_dump") else event

    async def embed(self, model: str = "embed-english-v3.0", inputs: Optional[list[str]] = None, **kwargs) -> list[list[float]]:
        if inputs is None: inputs = []
        response = await self._client.embed(texts=inputs, model=model, **kwargs)  # type: ignore[call-arg]
        return response.embeddings

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="command-r-plus-08-2024", family=ModelFamily.COMMAND_R, provider_id="cohere", context_length=128000),
            ModelInfo(model_id="command-r-08-2024", family=ModelFamily.COMMAND_R, provider_id="cohere", context_length=128000),
            ModelInfo(model_id="embed-english-v3.0", family=ModelFamily.EMBEDDING, provider_id="cohere"),
            ModelInfo(model_id="embed-multilingual-v3.0", family=ModelFamily.EMBEDDING, provider_id="cohere"),
        ]
        self._is_initialized = True
