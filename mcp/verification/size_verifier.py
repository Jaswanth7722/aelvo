"""SizeVerifier — verifies MCP responses are within configured byte limits."""

from __future__ import annotations

import logging
from typing import Any, Dict
from .verification_result import VerificationResult
from ..events.event_schemas import VerificationAction

log = logging.getLogger("aelvo.mcp.verification.size")


class SizeVerifier:
    """Verifies that MCP responses respect configured size limits."""

    def __init__(self, max_bytes: int = 10 * 1024 * 1024):
        self._max_bytes = max_bytes

    async def verify(self, request_id: str, server_id: str, tool_name: str,
                     response: Any, context: Dict[str, Any]) -> VerificationResult:
        result = getattr(response, 'result', None)
        if result is None:
            return VerificationResult(
                verifier_id="SizeVerifier",
                passed=True,
                confidence=1.0,
                diagnostics=["No result to check size"],
            )

        result_str = str(result)
        size = len(result_str.encode("utf-8"))

        if size > self._max_bytes:
            return VerificationResult(
                verifier_id="SizeVerifier",
                passed=False,
                confidence=1.0,
                action_required=VerificationAction.WARN,
                findings=[{"type": "size_exceeded", "size_bytes": size, "max_bytes": self._max_bytes}],
                diagnostics=[f"Response size {size} bytes exceeds limit of {self._max_bytes} bytes"],
            )

        return VerificationResult(
            verifier_id="SizeVerifier",
            passed=True,
            confidence=1.0,
            diagnostics=[f"Response size {size} bytes within limits"],
        )
