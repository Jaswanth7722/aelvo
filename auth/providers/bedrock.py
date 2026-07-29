"""AWS Bedrock provider implementation."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator, Optional

from ..types import CapabilityFlag, ModelFamily, ModelInfo, ProviderCapabilities, ProviderConfig, ProviderInfo, ProviderType
from .base import BaseProvider


class BedrockProvider(BaseProvider):
    """Provider implementation for AWS Bedrock."""

    def __init__(self, region: str = "us-east-1", profile: Optional[str] = None) -> None:
        info = ProviderInfo(provider_id="bedrock", name="AWS Bedrock", provider_type=ProviderType.CLOUD, description="AWS Bedrock managed service", docs_url="https://docs.aws.amazon.com/bedrock", website="https://aws.amazon.com/bedrock")
        config = ProviderConfig(provider_id="bedrock", api_key="", base_url="", default_model="anthropic.claude-3-5-sonnet-20241022-v2:0", timeout_seconds=120.0, max_retries=3)
        capabilities = ProviderCapabilities(provider_id="bedrock", capabilities={
            CapabilityFlag.TEXT_GENERATION, CapabilityFlag.CHAT_COMPLETION, CapabilityFlag.STREAMING,
            CapabilityFlag.TOOL_CALLING, CapabilityFlag.VISION, CapabilityFlag.EMBEDDINGS,
            CapabilityFlag.TEMPERATURE, CapabilityFlag.MAX_TOKENS, CapabilityFlag.TOP_P, CapabilityFlag.TOP_K,
            CapabilityFlag.STOP_SEQUENCES,
        }, model_families={ModelFamily.CLAUDE_3, ModelFamily.CLAUDE_3_5, ModelFamily.LLAMA, ModelFamily.MISTRAL},
            max_context_length=200000, supports_streaming=True, supports_tool_calling=True, is_local=False)
        super().__init__(info, config, capabilities)
        self._region = region
        self._profile = profile
        self._runtime = None

    def _get_runtime(self):
        if self._runtime is None:
            try:
                import boto3
                session = boto3.Session(profile_name=self._profile, region_name=self._region)
                self._runtime = session.client("bedrock-runtime")
            except ImportError:
                raise ImportError("boto3 required for Bedrock provider. Install with: pip install boto3")
        return self._runtime

    async def chat_completion(self, model, messages, tools=None, stream=False, temperature=None, max_tokens=None, **kwargs) -> dict[str, Any]:
        runtime = self._get_runtime()
        body = {"anthropic_version": "bedrock-2023-05-31", "messages": messages, "max_tokens": max_tokens or 4096}
        if temperature is not None: body["temperature"] = temperature
        if tools: body["tools"] = tools
        body.update(kwargs)

        import aioboto3
        session = aioboto3.Session(profile_name=self._profile, region_name=self._region)
        async with session.client("bedrock-runtime") as client:
            response = await client.invoke_model(modelId=model, body=json.dumps(body))
            result = json.loads(await response["body"].read())
            return {"choices": [{"message": {"content": result.get("content", [{}])[0].get("text", ""), "role": "assistant"}}]}

    async def chat_completion_stream(self, model, messages, tools=None, temperature=None, max_tokens=None, **kwargs) -> AsyncIterator[dict[str, Any]]:
        body = {"anthropic_version": "bedrock-2023-05-31", "messages": messages, "max_tokens": max_tokens or 4096}
        if temperature is not None: body["temperature"] = temperature
        if tools: body["tools"] = tools
        body.update(kwargs)

        import aioboto3
        session = aioboto3.Session(profile_name=self._profile, region_name=self._region)
        async with session.client("bedrock-runtime") as client:
            response = await client.invoke_model_with_response_stream(modelId=model, body=json.dumps(body))
            async for event in response["body"]:
                chunk = json.loads(event["chunk"]["bytes"])
                if chunk.get("type") == "content_block_delta":
                    yield {"choices": [{"delta": {"content": chunk["delta"].get("text", ""), "role": "assistant"}}]}

    async def initialize(self) -> None:
        if self._is_initialized: return
        self._models = [
            ModelInfo(model_id="anthropic.claude-3-5-sonnet-20241022-v2:0", family=ModelFamily.CLAUDE_3_5, provider_id="bedrock", context_length=200000),
            ModelInfo(model_id="anthropic.claude-3-haiku-20240307-v1:0", family=ModelFamily.CLAUDE_3, provider_id="bedrock", context_length=200000),
            ModelInfo(model_id="meta.llama3-70b-instruct-v1:0", family=ModelFamily.LLAMA, provider_id="bedrock", context_length=8192),
            ModelInfo(model_id="mistral.mixtral-8x7b-instruct-v0:1", family=ModelFamily.MISTRAL, provider_id="bedrock", context_length=32768),
        ]
        self._is_initialized = True
