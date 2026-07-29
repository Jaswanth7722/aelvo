"""Layer 2 Ã¢â‚¬â€ Verification Pipeline.

Every execution node may declare required, optional, and blocking verification types.
Execution completion is provisional until verification succeeds.

Flow: Generate Ã¢â€ â€™ Execute Ã¢â€ â€™ Verify Ã¢â€ â€™ Classify Ã¢â€ â€™ Recover or Complete Ã¢â€ â€™ Persist Learning
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Callable, Dict, List, Awaitable

from .types import (
    VerificationType,
    VerificationResult,
    VerificationScope,
    VerificationManifest,
    Confidence,
    Severity,
    Retryability,
    VerificationNotImplementedError,
)
from .events import (
    VerificationStartedEvent,
    VerificationCompletedEvent,
    VerificationFailedEvent,
)

log = logging.getLogger("aelvo.runtime.verification.pipeline")


class VerificationPipeline:
    """Manages the verification lifecycle for execution nodes.

    Features:
    - Plugin-based verifier registration
    - Scope-determined verification targeting
    - Provisional execution (node not complete until verified)
    - Rich result collection
    """

    def __init__(self):
        self._verifiers: Dict[VerificationType, Callable[
            [str, VerificationScope, Dict[str, Any]],
            Awaitable[VerificationResult],
        ]] = {}
        self._event_callbacks: List[Callable[[Any], Awaitable[None]]] = []
        self._history: List[VerificationResult] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_verifier(
        self,
        verification_type: VerificationType,
        handler: Callable[
            [str, VerificationScope, Dict[str, Any]],
            Awaitable[VerificationResult],
        ],
    ):
        """Register a verifier plugin for a specific verification type."""
        self._verifiers[verification_type] = handler
        log.info(f"Registered verifier for {verification_type.value}")

    def on_event(self, callback: Callable[[Any], Awaitable[None]]):
        """Register a callback for verification events."""
        self._event_callbacks.append(callback)

    # ------------------------------------------------------------------
    # Core verification flow
    # ------------------------------------------------------------------

    async def verify(
        self,
        node_id: str,
        manifest: VerificationManifest,
        scope: VerificationScope,
        context: Dict[str, Any],
    ) -> List[VerificationResult]:
        """Run all required and optional verifications for a node.

        Args:
            node_id: The execution node being verified
            manifest: What verifications to run
            scope: Scoped verification targets
            context: Runtime context for verifiers

        Returns:
            List of VerificationResult objects (immutable once returned)
        """
        results: List[VerificationResult] = []

        # 1. Determine effective verifications
        required = self._resolve_types(manifest.required)
        optional = self._resolve_types(manifest.optional)
        blocking = set(manifest.blocking)

        # 2. Run required verifications first
        for vtype in required:
            result = await self._run_single(node_id, vtype, scope, context)
            results.append(result)
            if not result.success and vtype in blocking:
                log.warning(
                    f"Blocking verification {vtype.value} failed for {node_id}"
                )
                # Still run optional verifications for diagnostics,
                # but mark the block
                break

        # 3. Run optional verifications (unless a blocking one failed)
        if not any(
            r.verification_type in blocking and not r.success for r in results
        ):
            for vtype in optional:
                if vtype not in [r.verification_type for r in results]:
                    result = await self._run_single(
                        node_id, vtype, scope, context
                    )
                    results.append(result)

        # 4. Store immutable results
        self._history.extend(results)
        return results

    async def _run_single(
        self,
        node_id: str,
        vtype: VerificationType,
        scope: VerificationScope,
        context: Dict[str, Any],
    ) -> VerificationResult:
        """Run a single verification and emit events.

        Raises:
            VerificationNotImplementedError: If no handler is registered for vtype.
        """
        start = time.monotonic()

        # Check handler before emitting started event to avoid orphaned events
        handler = self._verifiers.get(vtype)
        if handler is None:
            log.error(
                "Verification type '%s' has no registered handler for node '%s' — raising VerificationNotImplementedError",
                vtype.value, node_id,
            )
            raise VerificationNotImplementedError(vtype, node_id)

        # Emit started event
        started = VerificationStartedEvent(
            event_id=hashlib.sha256(
                f"vstart_{node_id}_{vtype.value}_{time.time()}".encode()
            ).hexdigest()[:16],
            node_id=node_id,
            verification_type=vtype,
            scope=scope.model_dump() if scope else {},
        )
        await self._emit_event(started)

        try:
            result = await handler(node_id, scope, context)
            duration = (time.monotonic() - start) * 1000
            # Ensure duration is set (Pydantic v2 model_copy)
            if not result.duration_ms:
                result = result.model_copy(update={"duration_ms": duration})

            if result.success:
                event = VerificationCompletedEvent(
                    event_id=result.verification_id,
                    node_id=node_id,
                    verification_type=vtype,
                    result=result,
                    duration_ms=result.duration_ms,
                )
                await self._emit_event(event)
            else:
                event = VerificationFailedEvent(
                    event_id=result.verification_id,
                    node_id=node_id,
                    verification_type=vtype,
                    result=result,
                    duration_ms=result.duration_ms,
                )
                await self._emit_event(event)

            return result

        except VerificationNotImplementedError:
            # Re-raise — must be handled by caller
            raise
        except Exception as e:
            duration = (time.monotonic() - start) * 1000
            result = VerificationResult(
                verification_id=hashlib.sha256(
                    f"verr_{node_id}_{vtype.value}_{time.time()}".encode()
                ).hexdigest()[:16],
                node_id=node_id,
                verification_type=vtype,
                duration_ms=duration,
                success=False,
                confidence=Confidence.MEDIUM,
                severity=Severity.ERROR,
                retryability=Retryability.CONDITIONAL,
                diagnostics=[f"Verifier raised exception: {str(e)}"],
                provenance="verifier_exception",
            )
            failed = VerificationFailedEvent(
                event_id=result.verification_id,
                node_id=node_id,
                verification_type=vtype,
                result=result,
                duration_ms=duration,
            )
            await self._emit_event(failed)
            return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_types(
        self, types: List[VerificationType]
    ) -> List[VerificationType]:
        """Resolve verification types, filtering unknown ones."""
        known = set(VerificationType)
        return [t for t in types if t in known]

    async def _emit_event(self, event: Any):
        """Emit an event to all registered callbacks."""
        for cb in self._event_callbacks:
            try:
                await cb(event)
            except Exception as e:
                log.error(f"Event callback error: {e}")

    @property
    def history(self) -> List[VerificationResult]:
        """Read-only access to verification history."""
        return list(self._history)

    def get_results_for_node(self, node_id: str) -> List[VerificationResult]:
        """Get all verification results for a specific node."""
        return [r for r in self._history if r.node_id == node_id]

    def all_passed(self, node_id: str) -> bool:
        """Check if all verifications for a node passed."""
        results = self.get_results_for_node(node_id)
        return all(r.success for r in results)


