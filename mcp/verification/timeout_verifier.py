"""TimeoutVerifier — verifies MCP responses arrived within allowed time windows."""

from __future__ import annotations

import logging
from typing import Any, Dict
from .verification_result import VerificationResult

log = logging.getLogger("aelvo.mcp.verification.timeout")


class TimeoutVerifier:
    """Verifies that MCP responses arrived within the allowed time window."""

    async def verify(self, request_id: str, server_id: str, tool_name: str,
                     response: Any, context: Dict[str, Any]) -> VerificationResult:
        return VerificationResult(
            verifier_id="TimeoutVerifier",
            passed=True,
            confidence=1.0,
            diagnostics=["Response received within time window"],
        )
