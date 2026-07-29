"""Structured output validation and normalization across providers."""

from __future__ import annotations

import json
from typing import Any, Optional


class StructuredOutputAdapter:
    """Normalizes structured output (JSON mode, response format) across providers."""

    @staticmethod
    def format_response_format(
        schema: dict[str, Any],
        target_provider: str = "openai",
        strict: bool = True,
    ) -> dict[str, Any]:
        """Format a response schema for a specific provider."""
        formatter = getattr(StructuredOutputAdapter, f"_for_{target_provider}", None)
        if formatter:
            return formatter(schema, strict)
        return StructuredOutputAdapter._for_openai(schema, strict)

    @staticmethod
    def _for_openai(schema: dict[str, Any], strict: bool = True) -> dict[str, Any]:
        return {
            "type": "json_schema",
            "json_schema": {
                "name": schema.get("name", "response"),
                "strict": strict,
                "schema": schema.get("schema", schema),
            },
        }

    @staticmethod
    def _for_anthropic(schema: dict[str, Any], strict: bool = True) -> dict[str, Any]:
        """Anthropic uses tool definitions for structured output."""
        return {
            "type": "tool_use",
            "name": schema.get("name", "format_response"),
            "description": schema.get("description", "Format the response according to the schema"),
            "input_schema": schema.get("schema", schema),
        }

    @staticmethod
    def _for_google(schema: dict[str, Any], strict: bool = True) -> dict[str, Any]:
        return {
            "response_mime_type": "application/json",
            "response_schema": schema.get("schema", schema),
        }

    @staticmethod
    def validate_response(
        response: str,
        schema: dict[str, Any],
    ) -> tuple[bool, Optional[Any], Optional[str]]:
        """Validate a response against a JSON schema."""
        try:
            data = json.loads(response) if isinstance(response, str) else response
        except json.JSONDecodeError as e:
            return False, None, f"Invalid JSON: {e}"

        required = schema.get("required", [])
        properties = schema.get("properties", {})

        for field in required:
            if field not in data:
                return False, data, f"Missing required field: {field}"

        for key, value in data.items():
            if key in properties:
                expected_type = properties[key].get("type", "")
                type_map = {
                    "string": str, "number": (int, float), "integer": int,
                    "boolean": bool, "array": list, "object": dict,
                }
                if expected_type in type_map:
                    if not isinstance(value, type_map[expected_type]):
                        return False, data, f"Field '{key}' should be {expected_type}, got {type(value).__name__}"

        return True, data, None

    @staticmethod
    def parse_json_mode(response: str) -> dict[str, Any]:
        """Extract JSON from a response, handling markdown code blocks."""
        response = response.strip()
        if response.startswith("```"):
            lines = response.split("\n")
            code_lines = []
            in_code = False
            for line in lines:
                if line.startswith("```"):
                    in_code = not in_code
                    continue
                if in_code:
                    code_lines.append(line)
            response = "\n".join(code_lines)
        return json.loads(response)
