"""MCPVerificationPipeline — validates every MCP output before it reaches a specialist.

Verifiers are applied in sequence:
1. SchemaVerifier — response conforms to declared tool output schema
2. TrustVerifier — response doesn't include trust-boundary-violating content
3. TimeoutVerifier — response arrived within allowed time window
4. CapabilityVerifier — response reflects the tool that was called (no confused deputy)
5. ContentSafetyVerifier — response doesn't contain known injection patterns
6. SizeVerifier — response within configured byte limits

On verification failure:
- Findings are logged to audit trail
- MCP_VERIFICATION_FAILED event published
- Recovery Engine is notified
- Specialist receives a typed error, not the raw response
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Callable

from .verification_result import VerificationResult
from ..events.event_schemas import VerificationAction
from ..events.event_publisher import MCPEventPublisher
from ..events.mcp_events import MCPVerificationPassed, MCPVerificationFailed
from .schema_verifier import SchemaVerifier
from .trust_verifier import TrustVerifier
from .timeout_verifier import TimeoutVerifier
from .capability_verifier import CapabilityVerifier
from .content_safety_verifier import ContentSafetyVerifier
from .size_verifier import SizeVerifier

log = logging.getLogger("aelvo.mcp.verification.pipeline")


class MCPVerificationPipeline:
    """Validates every MCP output through a sequence of verifiers.

    Verifiers can be registered dynamically. Built-in verifiers run
    in a fixed order to guarantee consistent validation.
    """

    def __init__(self, event_publisher: Optional[MCPEventPublisher] = None):
        self._verifiers: List[Callable] = [
            SchemaVerifier(),
            TrustVerifier(),
            TimeoutVerifier(),
            CapabilityVerifier(),
            ContentSafetyVerifier(),
            SizeVerifier(),
        ]
        self._custom_verifiers: List[Callable] = []
        self._event_publisher = event_publisher
        self._history: List[VerificationResult] = []

    def add_verifier(self, verifier: Callable) -> None:
        """Add a custom verifier to the pipeline."""
        self._custom_verifiers.append(verifier)

    async def verify(
        self,
        request_id: str,
        server_id: str,
        tool_name: str,
        response: Any,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[VerificationResult]:
        """Run all verifiers on a response.

        Args:
            request_id: The execution request ID.
            server_id: The server that produced the response.
            tool_name: The tool that was called.
            response: The raw response to verify.
            context: Optional verification context.

        Returns:
            List of VerificationResult objects (one per verifier).
        """
        results = []
        ctx = context or {}

        # Run built-in verifiers
        for verifier in self._verifiers:
            start = time.monotonic()
            try:
                if asyncio.iscoroutinefunction(verifier.verify):
                    result = await verifier.verify(request_id, server_id, tool_name, response, ctx)
                else:
                    result = verifier.verify(request_id, server_id, tool_name, response, ctx)
            except Exception as e:
                result = VerificationResult(
                    verifier_id=type(verifier).__name__,
                    passed=False,
                    confidence=0.5,
                    action_required=VerificationAction.BLOCK,
                    diagnostics=[f"Verifier raised exception: {e}"],
                )

            result.duration_ms = (time.monotonic() - start) * 1000
            results.append(result)
            self._history.append(result)

            # On BLOCK action, stop and don't run remaining verifiers
            if result.action_required == VerificationAction.BLOCK:
                log.warning("MCPVerificationPipeline: BLOCKED by %s for %s/%s",
                            result.verifier_id, server_id, request_id)
                break

        # Run custom verifiers (non-blocking, informational)
        for verifier in self._custom_verifiers:
            try:
                result = await verifier(request_id, server_id, tool_name, response, ctx)
                if isinstance(result, VerificationResult):
                    results.append(result)
                    self._history.append(result)
            except Exception as e:
                log.debug("Custom verifier error: %s", e)

        # Publish verification completed event
        if self._event_publisher:
            all_passed = all(r.passed for r in results)
            verifier_names = [r.verifier_id for r in results]
            if all_passed:
                await self._event_publisher.publish(
                    MCPVerificationPassed(
                        event_id=f"vpass_{request_id}",
                        request_id=request_id,
                        verifiers_run=verifier_names,
                    )
                )
            else:
                failed = next((r for r in results if not r.passed), None)
                await self._event_publisher.publish(
                    MCPVerificationFailed(
                        event_id=f"vfail_{request_id}",
                        request_id=request_id,
                        failing_verifier=failed.verifier_id if failed else "unknown",
                        action_taken=failed.action_required.value if failed else "unknown",
                        diagnostics=failed.diagnostics if failed else [],
                    )
                )

        return results

    def get_history(self, limit: int = 100) -> List[VerificationResult]:
        return self._history[-limit:]
