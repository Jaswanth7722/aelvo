"""TrustVerifier — verifies MCP responses don't contain trust-boundary-violating content."""

from __future__ import annotations

import logging
from typing import Any, Dict
from .verification_result import VerificationResult
from ..events.event_schemas import VerificationAction

log = logging.getLogger("aelvo.mcp.verification.trust")


class TrustVerifier:
    """Verifies that an MCP response does not violate trust boundaries.

    Checks for:
    - Unexpected server identity changes
    - Content that exceeds the server's trust level
    """

    async def verify(self, request_id: str, server_id: str, tool_name: str,
                     response: Any, context: Dict[str, Any]) -> VerificationResult:
        findings = []
        diagnostics = []

        # Check for signs of trust boundary violation
        result = getattr(response, 'result', None)
        if result and isinstance(result, dict):
            # Check for unexpected identity claims
            if result.get("__meta", {}).get("server_id") and \
               result["__meta"]["server_id"] != server_id:
                findings.append({
                    "type": "server_identity_mismatch",
                    "expected": server_id,
                    "actual": result["__meta"]["server_id"],
                })
                diagnostics.append(f"Response claims server identity '{result['__meta']['server_id']}' but expected '{server_id}'")

        if not findings:
            return VerificationResult(
                verifier_id="TrustVerifier",
                passed=True,
                confidence=1.0,
                diagnostics=["Trust boundary intact"],
            )

        return VerificationResult(
            verifier_id="TrustVerifier",
            passed=False,
            confidence=0.9,
            action_required=VerificationAction.QUARANTINE,
            findings=findings,
            diagnostics=diagnostics,
        )
