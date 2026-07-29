"""ContentSafetyVerifier — verifies responses don't contain known injection patterns."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict
from .verification_result import VerificationResult
from ..events.event_schemas import VerificationAction

log = logging.getLogger("aelvo.mcp.verification.safety")


class ContentSafetyVerifier:
    """Scans MCP responses for known injection patterns and unsafe content."""

    INJECTION_PATTERNS = [
        (r"<script[^>]*>.*?</script>", "HTML script injection"),
        (r"javascript\s*:", "JavaScript protocol injection"),
        (r"on\w+\s*=\s*['\"].*?['\"]", "Event handler injection"),
    ]

    async def verify(self, request_id: str, server_id: str, tool_name: str,
                     response: Any, context: Dict[str, Any]) -> VerificationResult:
        findings = []
        diagnostics = []

        result = getattr(response, 'result', None)
        if result:
            result_str = str(result)
            for pattern, description in self.INJECTION_PATTERNS:
                if re.search(pattern, result_str, re.IGNORECASE):
                    findings.append({"type": "injection_pattern", "description": description})
                    diagnostics.append(f"Detected: {description}")

        if not findings:
            return VerificationResult(
                verifier_id="ContentSafetyVerifier",
                passed=True,
                confidence=1.0,
                diagnostics=["Content safety check passed"],
            )

        return VerificationResult(
            verifier_id="ContentSafetyVerifier",
            passed=False,
            confidence=0.9,
            action_required=VerificationAction.BLOCK,
            findings=findings,
            diagnostics=diagnostics,
        )
