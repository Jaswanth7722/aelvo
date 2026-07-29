"""HuggingFace Inference API provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
try:
    from huggingface_hub import AsyncInferenceClient
except ImportError:
    AsyncInferenceClient = None  # type: ignore
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class HuggingFaceProvider(BaseProvider):
    def __init__(self, api_key: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="huggingface", name="HuggingFace Inference", provider_type=ProviderType.FOUNDATION, description="HuggingFace Inference API", docs_url="https://huggingface.co/docs/api-inference", website="https://huggingface.co")
        config = ProviderConfig(provider_id="huggingface", api_key=api_key, base_url="https://api-inference.huggingface.co", default_model="meta-llama/Llama-3.3-70B-Instruct", timeout_seconds=120.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="huggingface", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.SYSTEM_PROMPTS, CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P,
            CapabilityFlag.EMBEDDINGS, CapabilityFlag.IMAGE_GENERATION, CapabilityFlag.AUDIO_TRANSCRIPTION,
            CapabilityFlag.OBJECT_DETECTION, CapabilityFlag.TEXT_CLASSIFICATION, CapabilityFlag.TOKEN_CLASSIFICATION,
            CapabilityFlag.QUESTION_ANSWERING, CapabilityFlag.SUMMARIZATION, CapabilityFlag.TRANSLATION,
        }, model_families={ModelFamily.ANY, ModelFamily.EMBEDDING}, max_context_length=131072, supports_streaming=True, supports_tool_calling=False)
        super().__init__(info, config, capabilities)
        self._client = AsyncInferenceClient(token=api_key or config.api_key)

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        params = {"model": model, "messages": messages, "max_tokens": max_tokens or 4096}
        if temperature is not None: params["temperature"] = temperature
        params.update(kwargs)
        response = await self._client.chat_completion(**params)
        return {"choices": [{"message": {"content": response.choices[0].message.content if hasattr(response, "choices") else str(response), "role": "assistant"}}]}

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        params = {"model": model, "messages": messages, "max_tokens": max_tokens or 4096, "stream": True}
        if temperature is not None: params["temperature"] = temperature
        params.update(kwargs)
        async for chunk in await self._client.chat_completion(**params):
            yield {"choices": [{"delta": {"content": getattr(chunk.choices[0].delta, "content", "") if hasattr(chunk, "choices") else str(chunk), "role": "assistant"}}]}

    async def embed(self, model: str, inputs: list[str], **kwargs) -> list[list[float]]:
        response = await self._client.feature_extraction(model=model, inputs=inputs)  # type: ignore[call-arg]
        return response if isinstance(response, list) else [response]  # type: ignore[list-item]

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [ModelInfo(model_id="meta-llama/Llama-3.3-70B-Instruct", family=ModelFamily.LLAMA, provider_id="huggingface", context_length=131072)]
        self._is_initialized = True
