"""Streaming format normalization across providers."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator


class StreamingAdapter:
    """Normalizes streaming responses from different providers."""

    @staticmethod
    def normalize_openai_chunk(chunk: dict[str, Any]) -> dict[str, Any]:
        """Normalize an OpenAI stream chunk to canonical format."""
        choice = (chunk.get("choices") or [{}])[0]
        delta = choice.get("delta", {})
        return {
            "type": "token",
            "content": delta.get("content", ""),
            "finish_reason": choice.get("finish_reason"),
            "tool_calls": delta.get("tool_calls"),
            "usage": chunk.get("usage"),
        }

    @staticmethod
    def normalize_anthropic_chunk(chunk: dict[str, Any]) -> dict[str, Any]:
        """Normalize an Anthropic stream chunk to canonical format."""
        chunk_type = chunk.get("type", "")
        if chunk_type == "content_block_delta":
            delta = chunk.get("delta", {})
            return {
                "type": "token",
                "content": delta.get("text", ""),
                "finish_reason": None,
            }
        elif chunk_type == "message_stop":
            return {"type": "done", "content": "", "finish_reason": "end_turn"}
        elif chunk_type == "message_delta":
            delta = chunk.get("delta", {})
            stop_reason = delta.get("stop_reason")
            usage = chunk.get("usage")
            result = {"type": "done", "content": "", "finish_reason": stop_reason}
            if usage:
                result["usage"] = usage
            return result
        return {"type": "metadata", "content": json.dumps(chunk)}

    @staticmethod
    def normalize_google_chunk(chunk: dict[str, Any]) -> dict[str, Any]:
        """Normalize a Google/Gemini stream chunk to canonical format."""
        candidates = chunk.get("candidates", [{}])
        content = candidates[0].get("content", {}) if candidates else {}
        parts = content.get("parts", [{}])
        text = parts[0].get("text", "") if parts else ""
        finish_reason = candidates[0].get("finishReason") if candidates else None
        return {"type": "token", "content": text, "finish_reason": finish_reason}

    @staticmethod
    def normalize_generic_chunk(chunk: Any) -> dict[str, Any]:
        """Fallback normalizer for unknown chunk formats."""
        if isinstance(chunk, str):
            return {"type": "token", "content": chunk}
        if isinstance(chunk, dict):
            return {"type": "token", "content": chunk.get("content", json.dumps(chunk))}
        return {"type": "token", "content": str(chunk)}

    @staticmethod
    def get_provider_normalizer(provider_id: str):
        """Get the appropriate normalizer for a provider."""
        normalizers = {
            "openai": StreamingAdapter.normalize_openai_chunk,
            "groq": StreamingAdapter.normalize_openai_chunk,
            "deepseek": StreamingAdapter.normalize_openai_chunk,
            "together": StreamingAdapter.normalize_openai_chunk,
            "fireworks": StreamingAdapter.normalize_openai_chunk,
            "perplexity": StreamingAdapter.normalize_openai_chunk,
            "openrouter": StreamingAdapter.normalize_openai_chunk,
            "xai": StreamingAdapter.normalize_openai_chunk,
            "mistral": StreamingAdapter.normalize_openai_chunk,
            "lm_studio": StreamingAdapter.normalize_openai_chunk,
            "vllm": StreamingAdapter.normalize_openai_chunk,
            "azure": StreamingAdapter.normalize_openai_chunk,
            "anthropic": StreamingAdapter.normalize_anthropic_chunk,
            "google": StreamingAdapter.normalize_google_chunk,
            "vertex": StreamingAdapter.normalize_google_chunk,
        }
        return normalizers.get(provider_id, StreamingAdapter.normalize_generic_chunk)

    @staticmethod
    async def normalize_stream(
        provider_id: str,
        stream: AsyncIterator[Any],
    ) -> AsyncIterator[dict[str, Any]]:
        """Normalize an entire stream from a provider."""
        normalizer = StreamingAdapter.get_provider_normalizer(provider_id)
        async for chunk in stream:
            yield normalizer(chunk)
