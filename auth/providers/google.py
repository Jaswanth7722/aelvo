"""Google Gemini provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

try:
    import google.generativeai as genai
except ImportError:
    genai = None  # type: ignore

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


class GoogleProvider(BaseProvider):
    """Provider implementation for Google Gemini."""

    def __init__(
        self,
        api_key: Optional[str] = None,
    ) -> None:
        info = ProviderInfo(
            provider_id="google",
            name="Google Gemini",
            provider_type=ProviderType.FOUNDATION,
            description="Google Gemini API provider",
            docs_url="https://ai.google.dev/docs",
            website="https://ai.google.dev",
        )
        config = ProviderConfig(
            provider_id="google",
            api_key=api_key,
            base_url="https://generativelanguage.googleapis.com",
            default_model="gemini-2.0-flash",
            timeout_seconds=60.0,
            max_retries=3,
        )
        capabilities = ProviderCapabilities(
            provider_id="google",
            capabilities={
                CapabilityFlag.TEXT_GENERATION,
                CapabilityFlag.CHAT_COMPLETION,
                CapabilityFlag.STREAMING,
                CapabilityFlag.TOOL_CALLING,
                CapabilityFlag.VISION,
                CapabilityFlag.MULTIMODAL,
                CapabilityFlag.LONG_CONTEXT,
                CapabilityFlag.SYSTEM_PROMPTS,
                CapabilityFlag.STRUCTURED_OUTPUT,
                CapabilityFlag.TEMPERATURE,
                CapabilityFlag.MAX_TOKENS,
                CapabilityFlag.TOP_P,
                CapabilityFlag.TOP_K,
                CapabilityFlag.STOP_SEQUENCES,
                CapabilityFlag.FUNCTION_CALLING,
                CapabilityFlag.EMBEDDINGS,
            },
            model_families={ModelFamily.GEMINI_2, ModelFamily.GEMINI_1_5, ModelFamily.GEMINI_1},
            max_context_length=1048576,
            max_output_tokens=8192,
            supports_streaming=True,
            supports_tool_calling=True,
            supports_multimodal=True,
        )
        super().__init__(info, config, capabilities)

        genai.configure(api_key=api_key or config.api_key)

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
        genai_model = genai.GenerativeModel(model_name=model)
        contents = self._convert_messages(messages)

        gen_kwargs = {}
        if temperature is not None:
            gen_kwargs["temperature"] = temperature
        if max_tokens is not None:
            gen_kwargs["max_output_tokens"] = max_tokens

        response = await genai_model.generate_content_async(
            contents,  # type: ignore[arg-type]
            generation_config=genai.types.GenerationConfig(**gen_kwargs) if gen_kwargs else None,  # type: ignore[arg-type]
            stream=False,
        )
        return {"choices": [{"message": {"content": response.text, "role": "assistant"}}]}

    async def chat_completion_stream(
        self,
        model: str,
        messages: list[dict[str, Any]],
        tools: Optional[list[dict[str, Any]]] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        genai_model = genai.GenerativeModel(model_name=model)
        contents = self._convert_messages(messages)

        gen_kwargs = {}
        if temperature is not None:
            gen_kwargs["temperature"] = temperature
        if max_tokens is not None:
            gen_kwargs["max_output_tokens"] = max_tokens

        response = await genai_model.generate_content_async(
            contents,  # type: ignore[arg-type]
            generation_config=genai.types.GenerationConfig(**gen_kwargs) if gen_kwargs else None,  # type: ignore[arg-type]
            stream=True,
        )
        async for chunk in response:
            yield {"choices": [{"delta": {"content": chunk.text, "role": "assistant"}}]}

    def _convert_messages(self, messages: list[dict[str, Any]]) -> list[dict[str, str]]:
        contents = []
        for msg in messages:
            role = "user" if msg["role"] in ("user", "system") else "model"
            text = msg.get("content", "")
            if isinstance(text, list):
                text = " ".join(p.get("text", "") for p in text if isinstance(p, dict))
            contents.append({"role": role, "parts": [{"text": text}]})
        return contents  # type: ignore[return-value]

    async def initialize(self) -> None:
        if self._is_initialized:
            return
        self._models = [
            ModelInfo(model_id="gemini-2.0-flash", family=ModelFamily.GEMINI_2, provider_id="google", context_length=1048576),
            ModelInfo(model_id="gemini-2.0-flash-lite", family=ModelFamily.GEMINI_2, provider_id="google", context_length=1048576),
            ModelInfo(model_id="gemini-1.5-pro", family=ModelFamily.GEMINI_1_5, provider_id="google", context_length=1048576),
            ModelInfo(model_id="gemini-1.5-flash", family=ModelFamily.GEMINI_1_5, provider_id="google", context_length=1048576),
            ModelInfo(model_id="gemini-1.0-pro", family=ModelFamily.GEMINI_1, provider_id="google", context_length=32768),
        ]
        self._is_initialized = True
