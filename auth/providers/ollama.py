"""Ollama local provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional
import httpx
from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class OllamaProvider(BaseProvider):
    def __init__(self, base_url: str = "http://localhost:11434") -> None:
        info = ProviderInfo(provider_id="ollama", name="Ollama", provider_type=ProviderType.LOCAL, description="Local Ollama runtime", docs_url="https://github.com/ollama/ollama", website="https://ollama.ai")
        config = ProviderConfig(provider_id="ollama", api_key="__local__", base_url=base_url, default_model="llama3.2", timeout_seconds=300.0, max_retries=2)
        capabilities = ProviderCapabilities(provider_id="ollama", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.LOCAL, CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS,
            CapabilityFlag.TOP_P, CapabilityFlag.TOP_K, CapabilityFlag.STOP_SEQUENCES, CapabilityFlag.VISION,
        }, model_families={ModelFamily.LLAMA, ModelFamily.QWEN, ModelFamily.MISTRAL, ModelFamily.GEMMA, ModelFamily.LOCAL},
            max_context_length=131072, supports_streaming=True, supports_tool_calling=True, is_local=True)
        super().__init__(info, config, capabilities)
        self._http = httpx.AsyncClient(base_url=str(config.base_url), timeout=config.timeout_seconds)  # type: ignore[arg-type]

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        payload = {"model": model, "messages": messages, "stream": stream}
        if tools: payload["tools"] = tools
        if temperature is not None: payload["temperature"] = temperature
        if max_tokens is not None: payload["options"] = {"num_predict": max_tokens}
        payload.update(kwargs)
        response = await self._http.post("/api/chat", json=payload)
        response.raise_for_status()
        return response.json()

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        payload = {"model": model, "messages": messages, "stream": True}
        if tools: payload["tools"] = tools
        if temperature is not None: payload["temperature"] = temperature
        if max_tokens is not None: payload["options"] = {"num_predict": max_tokens}
        payload.update(kwargs)
        async with self._http.stream("POST", "/api/chat", json=payload) as response:
            async for line in response.aiter_lines():
                if line.strip():
                    try:
                        import json
                        chunk = json.loads(line)
                        content = chunk.get("message", {}).get("content", "")
                        yield {
                            "choices": [
                                {
                                    "delta": {
                                        "content": content,
                                        "role": chunk.get("message", {}).get("role")
                                    },
                                    "finish_reason": "stop" if chunk.get("done") else None
                                }
                            ]
                        }
                    except Exception:
                        yield {"choices": [{"delta": {"content": line}}]}

    async def initialize(self) -> None:
        if self._is_initialized: return
        try:
            response = await self._http.get("/api/tags")
            if response.status_code == 200:
                data = response.json()
                for model_data in data.get("models", []):
                    self._models.append(ModelInfo(
                        model_id=model_data.get("name", "unknown"),
                        family=ModelFamily.LOCAL, provider_id="ollama",
                    ))
        except Exception:
            self._models = [ModelInfo(model_id="llama3.2", family=ModelFamily.LOCAL, provider_id="ollama")]
        self._is_initialized = True
