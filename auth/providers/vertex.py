"""Google Vertex AI provider implementation."""

from __future__ import annotations

from typing import Any, AsyncIterator, Optional

from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class VertexProvider(BaseProvider):
    """Provider implementation for Google Vertex AI."""

    def __init__(self, project_id: str = "", location: str = "us-central1") -> None:
        info = ProviderInfo(provider_id="vertex", name="Vertex AI", provider_type=ProviderType.CLOUD, description="Google Vertex AI managed service", docs_url="https://cloud.google.com/vertex-ai/docs", website="https://cloud.google.com/vertex-ai")
        config = ProviderConfig(provider_id="vertex", api_key="", base_url="", default_model="gemini-2.0-flash-001", timeout_seconds=120.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="vertex", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.VISION, CapabilityFlag.MULTIMODAL,
            CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P, CapabilityFlag.TOP_K,
            CapabilityFlag.STOP_SEQUENCES, CapabilityFlag.EMBEDDINGS,
        }, model_families={ModelFamily.GEMINI_2, ModelFamily.GEMINI_1_5, ModelFamily.GEMINI_1, ModelFamily.EMBEDDING},
            max_context_length=1048576, supports_streaming=True, supports_tool_calling=True, supports_multimodal=True, is_local=False)
        super().__init__(info, config, capabilities)
        self._project_id = project_id
        self._location = location
        self._model = None

    def _get_model(self):
        if self._model is None:
            try:
                import vertexai
                from vertexai.generative_models import GenerativeModel
                vertexai.init(project=self._project_id, location=self._location)
                self._model = GenerativeModel
            except ImportError:
                raise ImportError("vertexai required for Vertex AI provider. Install with: pip install google-cloud-aiplatform")
        return self._model

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        ModelClass = self._get_model()
        gen_model = ModelClass(model_name=model)

        system_instruction = None
        chat_messages = messages
        if messages and messages[0].get("role") == "system":
            system_instruction = messages[0]["content"]
            chat_messages = messages[1:]

        contents = []
        for msg in chat_messages:
            role = "user" if msg["role"] == "user" else "model"
            text = msg.get("content", "")
            if isinstance(text, list):
                text = " ".join(p.get("text", "") for p in text if isinstance(p, dict))
            contents.append({"role": role, "parts": [{"text": text}]})

        gen_kwargs = {}
        if temperature is not None: gen_kwargs["temperature"] = temperature
        if max_tokens is not None: gen_kwargs["max_output_tokens"] = max_tokens

        response = await gen_model.generate_content_async(
            contents,
            generation_config=gen_kwargs if gen_kwargs else None,
        )
        return {"choices": [{"message": {"content": response.text, "role": "assistant"}}]}

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        ModelClass = self._get_model()
        gen_model = ModelClass(model_name=model)

        chat_messages = messages
        if messages and messages[0].get("role") == "system":
            chat_messages = messages[1:]

        contents = []
        for msg in chat_messages:
            role = "user" if msg["role"] == "user" else "model"
            text = msg.get("content", "")
            if isinstance(text, list):
                text = " ".join(p.get("text", "") for p in text if isinstance(p, dict))
            contents.append({"role": role, "parts": [{"text": text}]})

        response = await gen_model.generate_content_async(contents, stream=True)
        async for chunk in response:
            yield {"choices": [{"delta": {"content": chunk.text, "role": "assistant"}}]}

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="gemini-2.0-flash-001", family=ModelFamily.GEMINI_2, provider_id="vertex", context_length=1048576),
            ModelInfo(model_id="gemini-1.5-pro-001", family=ModelFamily.GEMINI_1_5, provider_id="vertex", context_length=1048576),
            ModelInfo(model_id="gemini-1.5-flash-001", family=ModelFamily.GEMINI_1_5, provider_id="vertex", context_length=1048576),
            ModelInfo(model_id="text-embedding-004", family=ModelFamily.EMBEDDING, provider_id="vertex"),
        ]
        self._is_initialized = True
