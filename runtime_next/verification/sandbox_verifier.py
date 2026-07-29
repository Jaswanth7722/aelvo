"""Sandbox Verifier â€” maps sandbox execution results to verification results.

Registered as a plugin in the VerificationPipeline. When a sandbox operation
completes (or fails), this verifier interprets the result and produces a
structured VerificationResult with proper failure classification.

Sandbox Error Types â†’ FailureClassification mapping:
  sandbox_denied/path_traversal  â†’ PERMISSION_DENIED
  sandbox_denied/blocked_command â†’ PERMISSION_DENIED
  sandbox_denied/injection       â†’ PERMISSION_DENIED
  timeout                         â†’ TIMEOUT
  resource_limit                  â†’ ENVIRONMENT_FAILURE
  write_mode                     â†’ PERMISSION_DENIED
  invalid_action                 â†’ TOOL_FAILURE
  workspace_escape               â†’ SANDBOX_ESCAPE (new)
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict

from .types import (
    VerificationType,
    VerificationResult,
    VerificationScope,
    FailureClassification,
    Confidence,
    Severity,
    Retryability,
)

log = logging.getLogger("aelvo.runtime.verification.sandbox")


# Mapping from sandbox error types to failure classifications
SANDBOX_ERROR_MAP: Dict[str, Dict[str, Any]] = {
    "sandbox_denied": {
        "classification": FailureClassification.PERMISSION_DENIED,
        "severity": Severity.CRITICAL,
        "retryability": Retryability.NEVER,
        "confidence": Confidence.CERTAIN,
    },
    "path_traversal": {
        "classification": FailureClassification.PERMISSION_DENIED,
        "severity": Severity.CRITICAL,
        "retryability": Retryability.NEVER,
        "confidence": Confidence.CERTAIN,
    },
    "blocked_command": {
        "classification": FailureClassification.PERMISSION_DENIED,
        "severity": Severity.ERROR,
        "retryability": Retryability.NEVER,
        "confidence": Confidence.CERTAIN,
    },
    "injection_blocked": {
        "classification": FailureClassification.PERMISSION_DENIED,
        "severity": Severity.CRITICAL,
        "retryability": Retryability.NEVER,
        "confidence": Confidence.CERTAIN,
    },
    "timeout": {
        "classification": FailureClassification.TIMEOUT,
        "severity": Severity.ERROR,
        "retryability": Retryability.CONDITIONAL,
        "confidence": Confidence.HIGH,
    },
    "resource_limit": {
        "classification": FailureClassification.ENVIRONMENT_FAILURE,
        "severity": Severity.ERROR,
        "retryability": Retryability.CONDITIONAL,
        "confidence": Confidence.HIGH,
    },
    "workspace_escape": {
        "classification": FailureClassification.SANDBOX_ESCAPE,
        "severity": Severity.CRITICAL,
        "retryability": Retryability.NEVER,
        "confidence": Confidence.CERTAIN,
    },
    "write_mode": {
        "classification": FailureClassification.PERMISSION_DENIED,
        "severity": Severity.WARNING,
        "retryability": Retryability.SAFE,
        "confidence": Confidence.HIGH,
    },
    "invalid_action": {
        "classification": FailureClassification.TOOL_FAILURE,
        "severity": Severity.ERROR,
        "retryability": Retryability.SAFE,
        "confidence": Confidence.HIGH,
    },
}


def classify_sandbox_error(error_type: str, error_detail: str) -> Dict[str, Any]:
    """Classify a sandbox error into a failure classification with metadata.

    Args:
        error_type: The sandbox error type (sandbox_denied, timeout, etc.)
        error_detail: Human-readable error detail for diagnostic matching

    Returns:
        Dict with keys: classification, severity, retryability, confidence
    """
    # Exact match first
    if error_type in SANDBOX_ERROR_MAP:
        return dict(SANDBOX_ERROR_MAP[error_type])

    # Check for path traversal in detail text
    detail_lower = error_detail.lower()
    if "traversal" in detail_lower or "escape" in detail_lower:
        return {
            "classification": FailureClassification.PERMISSION_DENIED,
            "severity": Severity.CRITICAL,
            "retryability": Retryability.NEVER,
            "confidence": Confidence.HIGH,
        }

    # Check for resource exhaustion
    if "resource" in detail_lower and ("limit" in detail_lower or "exceed" in detail_lower):
        return {
            "classification": FailureClassification.ENVIRONMENT_FAILURE,
            "severity": Severity.ERROR,
            "retryability": Retryability.CONDITIONAL,
            "confidence": Confidence.HIGH,
        }

    # Default: generic tool failure
    return {
        "classification": FailureClassification.TOOL_FAILURE,
        "severity": Severity.ERROR,
        "retryability": Retryability.SAFE,
        "confidence": Confidence.MEDIUM,
    }


class SandboxVerifier:
    """Verifier plugin that interprets sandbox execution results.

    Usage:
        pipeline = VerificationPipeline()
        verifier = SandboxVerifier()
        pipeline.register_verifier(
            VerificationType.SANDBOX_VALIDATION,
            verifier.create_handler(),
        )
    """

    def create_handler(self):
        """Create a verifier handler for the VerificationPipeline.

        Returns a callable matching the signature:
            (node_id: str, scope: VerificationScope, context: Dict[str, Any])
            -> Awaitable[VerificationResult]

        The context dict must contain a 'sandbox_result' key with the
        sandbox execution result dict.
        """
        async def handler(
            node_id: str,
            scope: VerificationScope,
            context: Dict[str, Any],
        ) -> VerificationResult:
            return self.verify(node_id, scope, context)

        return handler

    def verify(
        self,
        node_id: str,
        scope: VerificationScope,
        context: Dict[str, Any],
    ) -> VerificationResult:
        """Verify a sandbox execution result.

        Args:
            node_id: The execution node being verified
            scope: Verification scope (affected files/symbols)
            context: Must contain 'sandbox_result' with the sandbox output

        Returns:
            VerificationResult with appropriate classification
        """
        start = time.monotonic()

        sandbox_result = context.get("sandbox_result", {})
        if not sandbox_result:
            return self._no_result_result(node_id, start)

        status = sandbox_result.get("status", "error")

        if status == "success":
            return self._success_result(node_id, sandbox_result, start)

        # Extract error information
        error_type = sandbox_result.get("error_type", "unknown")
        error_detail = sandbox_result.get("error_detail", "")
        action = sandbox_result.get("action", "unknown")
        exit_code = sandbox_result.get("exit_code")

        # Classify the error
        classification_info = classify_sandbox_error(error_type, error_detail)
        classification = classification_info["classification"]

        # Build diagnostics
        diagnostics = [
            f"Sandbox {action} failed: {error_type}",
        ]
        if error_detail:
            diagnostics.append(f"Detail: {error_detail}")
        if exit_code is not None:
            diagnostics.append(f"Exit code: {exit_code}")

        # Determine affected files from scope
        affected_files = list(scope.affected_files) if scope else []

        # Build runtime implications
        runtime_implications = []
        if classification == FailureClassification.PERMISSION_DENIED:
            runtime_implications.append("Sandbox enforced security policy â€” operation blocked")
        elif classification == FailureClassification.TIMEOUT:
            runtime_implications.append("Sandbox terminated process â€” execution time exceeded limit")
        elif classification == FailureClassification.ENVIRONMENT_FAILURE:
            runtime_implications.append("Sandbox detected resource constraint â€” limits enforced")
        elif classification == FailureClassification.SANDBOX_ESCAPE:
            runtime_implications.append("CRITICAL: Possible sandbox escape attempted")

        duration = (time.monotonic() - start) * 1000

        return VerificationResult(
            verification_id=hashlib.sha256(
                f"sbox_{node_id}_{action}_{time.time()}".encode()
            ).hexdigest()[:16],
            node_id=node_id,
            verification_type=VerificationType.SANDBOX_VALIDATION,
            duration_ms=duration,
            success=False,
            confidence=classification_info.get("confidence", Confidence.HIGH),
            severity=classification_info.get("severity", Severity.ERROR),
            retryability=classification_info.get("retryability", Retryability.SAFE),
            diagnostics=diagnostics,
            affected_files=affected_files,
            runtime_implications=runtime_implications,
            artifacts={
                "sandbox_error_type": error_type,
                "sandbox_action": action,
                "sandbox_exit_code": exit_code,
                "failure_classification": classification.value,
            },
            provenance="sandbox_verifier",
        )

    def _success_result(
        self,
        node_id: str,
        sandbox_result: Dict[str, Any],
        start: float,
    ) -> VerificationResult:
        """Build a success VerificationResult for a successful sandbox operation."""
        action = sandbox_result.get("action", "unknown")
        duration = (time.monotonic() - start) * 1000

        # Even successful operations may have stderr warnings
        stderr = sandbox_result.get("stderr", "")
        diagnostics = []
        if stderr and len(stderr) > 0:
            diagnostics.append(f"Sandbox {action}: stderr produced ({len(stderr)} chars)")

        return VerificationResult(
            verification_id=hashlib.sha256(
                f"sbox_ok_{node_id}_{action}_{time.time()}".encode()
            ).hexdigest()[:16],
            node_id=node_id,
            verification_type=VerificationType.SANDBOX_VALIDATION,
            duration_ms=duration,
            success=True,
            confidence=Confidence.CERTAIN,
            severity=Severity.INFO,
            retryability=Retryability.SAFE,
            diagnostics=diagnostics,
            artifacts={
                "sandbox_action": action,
                "sandbox_status": "success",
                "exit_code": sandbox_result.get("exit_code"),
            },
            provenance="sandbox_verifier",
        )

    def _no_result_result(
        self,
        node_id: str,
        start: float,
    ) -> VerificationResult:
        """Build a result when no sandbox result was provided."""
        duration = (time.monotonic() - start) * 1000
        return VerificationResult(
            verification_id=hashlib.sha256(
                f"sbox_nodata_{node_id}_{time.time()}".encode()
            ).hexdigest()[:16],
            node_id=node_id,
            verification_type=VerificationType.SANDBOX_VALIDATION,
            duration_ms=duration,
            success=False,
            confidence=Confidence.MEDIUM,
            severity=Severity.WARNING,
            retryability=Retryability.SAFE,
            diagnostics=["No sandbox result data available for verification"],
            provenance="sandbox_verifier",
        )


# Convenience: register the sandbox verifier on a pipeline
def register_sandbox_verifier(pipeline):
    """Register the SandboxVerifier on a VerificationPipeline instance."""
    verifier = SandboxVerifier()
    pipeline.register_verifier(
        VerificationType.SANDBOX_VALIDATION,
        verifier.create_handler(),
    )
    log.info("SandboxVerifier registered on pipeline")
    return verifier
