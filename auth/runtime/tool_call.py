"""Tool Call Normalization Layer — canonical tool call formats across all providers."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional


logger = logging.getLogger(__name__)


@dataclass
class ToolCallDefinition:
    """Canonical definition of a tool a model can call."""

    name: str
    description: str = ""
    parameters: dict[str, Any] = field(default_factory=dict)
    strict: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolCall:
    """Canonical tool call made by a model."""

    id: str
    name: str
    arguments: dict[str, Any]
    provider_id: str = ""
    model_id: str = ""
    index: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    """Canonical result from executing a tool call."""

    tool_call_id: str
    name: str
    content: str
    is_error: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


class ToolCallNormalizer:
    """Normalizes tool call formats across different providers.

    Handles:
    - OpenAI-style function calling
    - Anthropic-style tool use
    - Google Gemini function declarations
    - Cohere tool calls
    - Mistral tool calls
    - Generic structured outputs as tool calls
    """

    def __init__(self) -> None:
        self._providers: dict[str, str] = {}

    # ── Normalization: Provider → Canonical ───────────────────────

    def normalize_tool_call(
        self,
        raw: Any,
        provider_id: str = "openai",
    ) -> ToolCall:
        """Normalize a provider-specific tool call to canonical form."""
        normalizer = getattr(
            self, f"_normalize_{provider_id.replace('-', '_')}", None
        )
        if normalizer:
            return normalizer(raw)
        return self._normalize_openai(raw)

    def normalize_tool_calls(
        self,
        raw_calls: list[Any],
        provider_id: str = "openai",
    ) -> list[ToolCall]:
        return [
            self.normalize_tool_call(c, provider_id) for c in raw_calls
        ]

    def normalize_tool_definition(
        self,
        tool: ToolCallDefinition,
        target_provider: str = "openai",
    ) -> dict[str, Any]:
        """Convert a canonical tool definition to provider-specific format."""
        converter = getattr(
            self,
            f"_to_{target_provider.replace('-', '_')}_format",
            None,
        )
        if converter:
            return converter(tool)
        return self._to_openai_format(tool)

    # ── Provider-specific normalizers ────────────────────────────

    def _normalize_openai(self, raw: dict[str, Any]) -> ToolCall:
        return ToolCall(
            id=raw.get("id", ""),
            name=raw.get("function", {}).get("name", ""),
            arguments=json.loads(
                raw.get("function", {}).get("arguments", "{}")
            ),
            index=raw.get("index", 0),
        )

    def _normalize_anthropic(self, raw: dict[str, Any]) -> ToolCall:
        return ToolCall(
            id=raw.get("id", ""),
            name=raw.get("name", ""),
            arguments=raw.get("input", {}),
        )

    def _normalize_google(self, raw: dict[str, Any]) -> ToolCall:
        return ToolCall(
            id=raw.get("name", ""),
            name=raw.get("name", ""),
            arguments=raw.get("args", raw.get("arguments", {})),
        )

    def _normalize_cohere(self, raw: dict[str, Any]) -> ToolCall:
        return ToolCall(
            id=raw.get("id", ""),
            name=raw.get("name", ""),
            arguments=raw.get("parameters", {}),
        )

    def _normalize_mistral(self, raw: dict[str, Any]) -> ToolCall:
        return ToolCall(
            id=raw.get("id", ""),
            name=raw.get("function", {}).get("name", ""),
            arguments=raw.get("function", {}).get("arguments", {}),
        )

    def _normalize_deepseek(self, raw: dict[str, Any]) -> ToolCall:
        return self._normalize_openai(raw)

    # ── Provider-specific format converters ──────────────────────

    def _to_openai_format(
        self, tool: ToolCallDefinition
    ) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
                "strict": tool.strict,
            },
        }

    def _to_anthropic_format(
        self, tool: ToolCallDefinition
    ) -> dict[str, Any]:
        return {
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.parameters,
        }

    def _to_google_format(
        self, tool: ToolCallDefinition
    ) -> dict[str, Any]:
        return {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        }

    def _to_cohere_format(
        self, tool: ToolCallDefinition
    ) -> dict[str, Any]:
        return {
            "name": tool.name,
            "description": tool.description,
            "parameter_definitions": {
                k: {
                    "description": v.get("description", ""),
                    "type": v.get("type", "string"),
                    "required": k
                    in tool.parameters.get("required", []),
                }
                for k, v in tool.parameters.get("properties", {}).items()
            },
        }

    def _to_mistral_format(
        self, tool: ToolCallDefinition
    ) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            },
        }

    def validate_tool_call(
        self, tool_call: ToolCall, definition: ToolCallDefinition
    ) -> tuple[bool, Optional[str]]:
        """Validate a tool call against its definition."""
        props = definition.parameters.get("properties", {})
        required = definition.parameters.get("required", [])

        # Check required params
        for param in required:
            if param not in tool_call.arguments:
                return False, f"Missing required parameter: {param}"

        # Check types
        for key, value in tool_call.arguments.items():
            if key in props:
                expected_type = props[key].get("type", "")
                if expected_type == "string" and not isinstance(value, str):
                    return (
                        False,
                        f"Parameter '{key}' should be string, got {type(value).__name__}",
                    )
                if expected_type == "integer" and not isinstance(value, int):
                    return (
                        False,
                        f"Parameter '{key}' should be integer, got {type(value).__name__}",
                    )
                if expected_type == "number" and not isinstance(
                    value, (int, float)
                ):
                    return (
                        False,
                        f"Parameter '{key}' should be number, got {type(value).__name__}",
                    )
                if expected_type == "boolean" and not isinstance(value, bool):
                    return (
                        False,
                        f"Parameter '{key}' should be boolean, got {type(value).__name__}",
                    )
                if expected_type == "array" and not isinstance(value, list):
                    return (
                        False,
                        f"Parameter '{key}' should be array, got {type(value).__name__}",
                    )
                if expected_type == "object" and not isinstance(value, dict):
                    return (
                        False,
                        f"Parameter '{key}' should be object, got {type(value).__name__}",
                    )

        return True, None


class ToolCallManager:
    """Manages tool call lifecycle: execution, validation, result tracking."""

    def __init__(self) -> None:
        self._normalizer = ToolCallNormalizer()
        self._tool_definitions: dict[str, ToolCallDefinition] = {}
        self._execution_history: list[dict[str, Any]] = []

    def register_tool(self, tool: ToolCallDefinition) -> None:
        self._tool_definitions[tool.name] = tool

    def register_tools(
        self, tools: list[ToolCallDefinition]
    ) -> None:
        for tool in tools:
            self.register_tool(tool)

    def get_tool(self, name: str) -> Optional[ToolCallDefinition]:
        return self._tool_definitions.get(name)

    def list_tools(self) -> list[ToolCallDefinition]:
        return list(self._tool_definitions.values())

    def to_provider_format(
        self, provider_id: str
    ) -> list[dict[str, Any]]:
        return [
            self._normalizer.normalize_tool_definition(t, provider_id)
            for t in self._tool_definitions.values()
        ]

    def record_execution(
        self,
        tool_call: ToolCall,
        result: ToolResult,
        duration_ms: float = 0.0,
    ) -> None:
        self._execution_history.append(
            {
                "tool_call_id": tool_call.id,
                "name": tool_call.name,
                "arguments": tool_call.arguments,
                "result": result.content,
                "is_error": result.is_error,
                "duration_ms": duration_ms,
                "timestamp": time.time(),
            }
        )

    def get_history(
        self, tool_name: Optional[str] = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        history = self._execution_history
        if tool_name:
            history = [h for h in history if h["name"] == tool_name]
        return history[-limit:]
