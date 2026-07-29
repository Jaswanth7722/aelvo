"""Tool call format normalization across providers."""

from __future__ import annotations

import json
from typing import Any


class ToolCallAdapter:
    """Normalizes tool call formats across different providers."""

    @staticmethod
    def normalize_tool_call(raw: dict[str, Any], provider_id: str = "openai") -> dict[str, Any]:
        """Normalize a provider-specific tool call to canonical format."""
        normalizer = getattr(ToolCallAdapter, f"_normalize_{provider_id}", None)
        if normalizer:
            return normalizer(raw)
        return ToolCallAdapter._normalize_openai(raw)

    @staticmethod
    def normalize_tool_calls(raw_calls: list[dict[str, Any]], provider_id: str = "openai") -> list[dict[str, Any]]:
        return [ToolCallAdapter.normalize_tool_call(tc, provider_id) for tc in raw_calls]

    @staticmethod
    def _normalize_openai(raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": raw.get("id", ""),
            "type": "function",
            "function": {
                "name": raw.get("function", {}).get("name", ""),
                "arguments": raw.get("function", {}).get("arguments", "{}"),
            },
        }

    @staticmethod
    def _normalize_anthropic(raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": raw.get("id", ""),
            "type": "function",
            "function": {
                "name": raw.get("name", ""),
                "arguments": json.dumps(raw.get("input", {})),
            },
        }

    @staticmethod
    def _normalize_google(raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": raw.get("name", ""),
            "type": "function",
            "function": {
                "name": raw.get("name", ""),
                "arguments": json.dumps(raw.get("args", raw.get("arguments", {}))),
            },
        }

    @staticmethod
    def format_for_provider(tool_call: dict[str, Any], target_provider: str = "openai") -> dict[str, Any]:
        """Format a canonical tool call for a specific provider."""
        converter = getattr(ToolCallAdapter, f"_to_{target_provider}", None)
        if converter:
            return converter(tool_call)
        return ToolCallAdapter._to_openai(tool_call)

    @staticmethod
    def _to_openai(tc: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": tc.get("id", ""),
            "type": "function",
            "function": {
                "name": tc.get("function", {}).get("name", ""),
                "arguments": tc.get("function", {}).get("arguments", "{}"),
            },
        }

    @staticmethod
    def _to_anthropic(tc: dict[str, Any]) -> dict[str, Any]:
        args = tc.get("function", {}).get("arguments", "{}")
        if isinstance(args, str):
            args = json.loads(args)
        return {
            "type": "tool_use",
            "id": tc.get("id", ""),
            "name": tc.get("function", {}).get("name", ""),
            "input": args,
        }

    @staticmethod
    def format_tool_definition(tool: dict[str, Any], target_provider: str = "openai") -> dict[str, Any]:
        """Format a tool definition for a specific provider."""
        if target_provider == "anthropic":
            return {
                "name": tool.get("function", {}).get("name", ""),
                "description": tool.get("function", {}).get("description", ""),
                "input_schema": tool.get("function", {}).get("parameters", {}),
            }
        return tool
