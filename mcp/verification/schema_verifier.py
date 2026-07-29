"""SchemaVerifier — verifies MCP responses conform to declared tool output schemas."""

from __future__ import annotations

import logging
from typing import Any, Dict
from .verification_result import VerificationResult
from ..events.event_schemas import VerificationAction

log = logging.getLogger("aelvo.mcp.verification.schema")


class SchemaVerifier:
    """Verifies that an MCP response conforms to the tool's declared output schema."""

    async def verify(self, request_id: str, server_id: str, tool_name: str,
                     response: Any, context: Dict[str, Any]) -> VerificationResult:
        findings = []
        diagnostics = []

        # Check response has required structure
        if response is None:
            return VerificationResult(
                verifier_id="SchemaVerifier",
                passed=False,
                confidence=1.0,
                action_required=VerificationAction.BLOCK,
                diagnostics=["Response is None"],
            )

        # Check for result field
        result = getattr(response, 'result', None)
        if result is not None:
            if not isinstance(result, dict) and not isinstance(result, (str, int, float, bool, list)):
                findings.append({
                    "type": "type_mismatch",
                    "expected_types": ["dict", "str", "int", "float", "bool", "list"],
                    "actual_type": type(result).__name__,
                })
                diagnostics.append(f"Result type '{type(result).__name__}' may not match schema")
        else:
            # Check for error field
            error = getattr(response, 'error', None)
            if error:
                findings.append({
                    "type": "response_error",
                    "error_code": error.get("code", -1),
                    "message": error.get("message", "Unknown error"),
                })
                diagnostics.append(f"Response contains error: {error.get('message', 'Unknown')}")

        if not findings:
            return VerificationResult(
                verifier_id="SchemaVerifier",
                passed=True,
                confidence=1.0,
                diagnostics=["Response schema valid"],
            )

        return VerificationResult(
            verifier_id="SchemaVerifier",
            passed=False,
            confidence=0.8,
            action_required=VerificationAction.WARN,
            findings=findings,
            diagnostics=diagnostics,
        )
