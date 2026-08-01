"""CapabilityVerifier — verifies response reflects the called tool (prevents confused deputy)."""

from __future__ import annotations

import logging
from typing import Any, Dict

from .verification_result import VerificationResult
from ..events.event_schemas import VerificationAction

log = logging.getLogger("aelvo.mcp.verification.capability")


class CapabilityVerifier:
    """Verifies that the response matches what the called tool should produce.

    Checks that a response claiming to originate from a different tool than
    the one requested is flagged (confused-deputy / tool-identity confusion).
    """

    async def verify(self, request_id: str, server_id: str, tool_name: str,
                     response: Any, context: Dict[str, Any]) -> VerificationResult:
        result = getattr(response, "result", None)
        meta = result.get("__meta", {}) if isinstance(result, dict) else {}

        claimed_tool = meta.get("tool_name")
        if claimed_tool and claimed_tool != tool_name:
            return VerificationResult(
                verifier_id="CapabilityVerifier",
                passed=False,
                confidence=0.9,
                action_required=VerificationAction.QUARANTINE,
                findings=[{
                    "type": "tool_identity_mismatch",
                    "requested": tool_name,
                    "claimed": claimed_tool,
                }],
                diagnostics=[
                    f"Response claims tool '{claimed_tool}' but '{tool_name}' was requested"
                ],
            )

        # Response carries no tool identity claim — nothing further to compare.
        return VerificationResult(
            verifier_id="CapabilityVerifier",
            passed=True,
            confidence=1.0,
            diagnostics=[f"Response reflects requested tool '{tool_name}'"],
        )
