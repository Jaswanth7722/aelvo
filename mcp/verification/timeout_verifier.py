"""TimeoutVerifier — verifies MCP responses arrived within allowed time windows."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict

from .verification_result import VerificationResult
from ..events.event_schemas import VerificationAction

log = logging.getLogger("aelvo.mcp.verification.timeout")


class TimeoutVerifier:
    """Verifies that MCP responses arrived within the allowed time window.

    The execution engine enforces a hard timeout via asyncio.wait_for;
    this verifier additionally checks any timing context supplied by the
    caller (e.g. start_time / timeout_ms) so late-but-not-hung responses
    are surfaced as findings.
    """

    async def verify(self, request_id: str, server_id: str, tool_name: str,
                     response: Any, context: Dict[str, Any]) -> VerificationResult:
        start_time = context.get("start_time")
        timeout_ms = context.get("timeout_ms")

        if start_time is None:
            # No timing context — nothing to verify. Engine-enforced timeout applies.
            return VerificationResult(
                verifier_id="TimeoutVerifier",
                passed=True,
                confidence=1.0,
                diagnostics=["No timing context supplied; hard timeout enforced by execution engine"],
            )

        elapsed_ms = (time.monotonic() - start_time) * 1000 if isinstance(start_time, float) else None
        if elapsed_ms is None:
            return VerificationResult(
                verifier_id="TimeoutVerifier",
                passed=True,
                confidence=1.0,
                diagnostics=["Timing context present but unusable; skipping check"],
            )

        if timeout_ms and elapsed_ms > timeout_ms:
            return VerificationResult(
                verifier_id="TimeoutVerifier",
                passed=False,
                confidence=0.9,
                action_required=VerificationAction.QUARANTINE,
                diagnostics=[
                    f"Response exceeded allowed window: {elapsed_ms:.0f}ms > {timeout_ms}ms"
                ],
            )

        return VerificationResult(
            verifier_id="TimeoutVerifier",
            passed=True,
            confidence=1.0,
            diagnostics=[f"Response received within window ({elapsed_ms:.0f}ms)"],
        )
