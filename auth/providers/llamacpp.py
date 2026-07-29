"""llama.cpp server provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator
import httpx
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class LlamaCppProvider(BaseProvider):
    def __init__(self, base_url: str = "http://localhost:8080") -> None:
        info = ProviderInfo(provider_id="llamacpp", name="llama.cpp", provider_type=ProviderType.LOCAL, description="llama.cpp inference server", docs_url="https://github.com/ggerganov/llama.cpp", website="https://github.com/ggerganov/llama.cpp")
        config = ProviderConfig(provider_id="llamacpp", api_key="__local__", base_url=f"{base_url}/v1", default_model="local-model", timeout_seconds=300.0, max_retries=2)
        capabilities = ProviderCapabilities(provider_id="llamacpp", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.LOCAL, CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P, CapabilityFlag.TOP_K,
            CapabilityFlag.STOP_SEQUENCES, CapabilityFlag.FREQUENCY_PENALTY, CapabilityFlag.PRESENCE_PENALTY,
        }, model_families={ModelFamily.LOCAL}, max_context_length=65536, supports_streaming=True, is_local=True)
        super().__init__(info, config, capabilities)
        self._http = httpx.AsyncClient(base_url=str(config.base_url), timeout=config.timeout_seconds)  # type: ignore[arg-type]

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        payload = {"messages": messages, "stream": stream}
        if temperature is not None: payload["temperature"] = temperature
        if max_tokens is not None: payload["max_tokens"] = max_tokens
        payload.update(kwargs)
        response = await self._http.post("/chat/completions", json=payload)
        response.raise_for_status()
        return response.json()

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        payload = {"messages": messages, "stream": True}
        if temperature is not None: payload["temperature"] = temperature
        if max_tokens is not None: payload["max_tokens"] = max_tokens
        payload.update(kwargs)
        async with self._http.stream("POST", "/chat/completions", json=payload) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: ") and line != "data: [DONE]":
                    try:
                        import json
                        chunk = json.loads(line[6:])
                        choices = chunk.get("choices", [])
                        if choices:
                            delta = choices[0].get("delta", {})
                            content = delta.get("content", "")
                            yield {
                                "choices": [
                                    {
                                        "delta": {
                                            "content": content,
                                            "role": delta.get("role")
                                        },
                                        "finish_reason": choices[0].get("finish_reason")
                                    }
                                ]
                            }
                    except Exception:
                        yield {"choices": [{"delta": {"content": line[6:]}}]}

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [ModelInfo(model_id="local-model", family=ModelFamily.LOCAL, provider_id="llamacpp")]
        self._is_initialized = True
