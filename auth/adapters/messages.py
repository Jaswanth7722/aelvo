"""Message format normalization across providers."""

from __future__ import annotations

from typing import Any, Optional


class MessageAdapter:
    """Normalizes message formats across different providers.

    Converts between provider-specific message formats and
    AELVO's canonical message format.
    """

    @staticmethod
    def to_canonical(messages: list[dict[str, Any]], source_provider: str = "openai") -> list[dict[str, Any]]:
        """Convert provider-specific messages to canonical format."""
        normalizer = getattr(MessageAdapter, f"_from_{source_provider}", None)
        if normalizer:
            return normalizer(messages)
        return MessageAdapter._from_openai(messages)

    @staticmethod
    def from_canonical(messages: list[dict[str, Any]], target_provider: str = "openai") -> list[dict[str, Any]]:
        """Convert canonical messages to provider-specific format."""
        converter = getattr(MessageAdapter, f"_to_{target_provider}", None)
        if converter:
            return converter(messages)
        return MessageAdapter._to_openai(messages)

    @staticmethod
    def _from_openai(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """OpenAI format is the canonical format."""
        return messages

    @staticmethod
    def _to_openai(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return messages

    @staticmethod
    def _from_anthropic(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert Anthropic messages to canonical format."""
        result = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")

            # System messages
            if role == "system":
                result.append({"role": "system", "content": content})
                continue

            # Content blocks
            if isinstance(content, list):
                text_parts = []
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                        elif block.get("type") == "image":
                            text_parts.append("[IMAGE]")
                        elif block.get("type") == "tool_use":
                            result.append({
                                "role": "assistant",
                                "content": text_parts.pop() if text_parts else "",
                                "tool_calls": [{
                                    "id": block.get("id", ""),
                                    "type": "function",
                                    "function": {
                                        "name": block.get("name", ""),
                                        "arguments": block.get("input", {}),
                                    },
                                }],
                            })
                            continue
                        elif block.get("type") == "tool_result":
                            result.append({
                                "role": "tool",
                                "tool_call_id": block.get("tool_use_id", ""),
                                "content": block.get("content", ""),
                            })
                            continue
                content = "\n".join(text_parts) if text_parts else ""

            if role == "assistant":
                result.append({"role": "assistant", "content": content})
            elif role == "user":
                result.append({"role": "user", "content": content})

        return result

    @staticmethod
    def _to_anthropic(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert canonical messages to Anthropic format."""
        result = []
        for msg in messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            tool_calls = msg.get("tool_calls")

            if role == "system":
                result.append({"role": "system", "content": content})
                continue

            if role == "tool":
                result.append({
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": msg.get("tool_call_id", ""),
                            "content": content,
                        }
                    ],
                })
                continue

            anthropic_role = "assistant" if role == "assistant" else "user"
            blocks = [{"type": "text", "text": content}]

            if tool_calls:
                for tc in tool_calls:
                    blocks.append({
                        "type": "tool_use",
                        "id": tc.get("id", ""),
                        "name": tc.get("function", {}).get("name", ""),
                        "input": tc.get("function", {}).get("arguments", {}),
                    })

            result.append({"role": anthropic_role, "content": blocks})

        return result

    @staticmethod
    def _from_google(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert Google/Gemini messages to canonical format."""
        result = []
        for msg in messages:
            role = "user" if msg.get("role") in ("user", "system") else "assistant"
            parts = msg.get("parts", [])
            text = " ".join(p.get("text", "") for p in parts if isinstance(p, dict))
            result.append({"role": role, "content": text})
        return result

    @staticmethod
    def _to_google(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert canonical messages to Google format."""
        result = []
        for msg in messages:
            role = "user" if msg.get("role") in ("user", "system") else "model"
            text = msg.get("content", "")
            result.append({"role": role, "parts": [{"text": text}]})
        return result
