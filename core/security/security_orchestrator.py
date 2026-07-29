"""SecurityOrchestrator â€” Integration Layer for the Execution Security Platform.

Connects the security subsystems (ExecutionGovernance, SecurityMemory,
SecurityAnalytics, ApprovalManager) with the AELVO orchestrator,
verification pipeline, recovery engine, and event bus.

Every execution flow goes through:
  Request â†’ RiskClassification â†’ PolicyDecision â†’ ApprovalCheck
  â†’ Verification â†’ Recovery â†’ Audit â†’ MemoryUpdate
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from .execution_governance import (
    ExecutionGovernance,
    PolicyDecision,
    RiskLevel,
    TrustLevel,
)
from .security_memory import SecurityMemory, MemoryEntryType
from .security_analytics import SecurityAnalytics, SecurityAnalyticsReport
from .approval_manager import ApprovalManager, ApprovalRequest, ApprovalState

log = logging.getLogger("aelvo.security.orchestrator")


# ============================================================================
# Security Context â€” passed through the execution pipeline
# ============================================================================


@dataclass
class SecurityContext:
    """Security context for an execution action, passed through the pipeline."""

    decision: PolicyDecision
    """The policy decision for this action."""

    approval: Optional[ApprovalRequest] = None
    """The approval request, if required."""

    is_blocked: bool = False
    """Whether the action was blocked by policy."""

    is_approved: bool = False
    """Whether the action was approved."""

    verification_passed: bool = False
    """Whether post-execution verification passed."""

    recovery_attempted: bool = False
    """Whether recovery was attempted."""

    recovery_successful: bool = False
    """Whether recovery succeeded."""

    audit_record_id: str = ""
    """ID of the audit record for this action."""

    elapsed_ms: float = 0.0
    """Total elapsed time through the security pipeline."""

    error: Optional[str] = None
    """Any error that occurred during security processing."""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_id": self.decision.decision_id if self.decision else "",
            "action_type": self.decision.action_type if self.decision else "",
            "action_target": self.decision.action_target if self.decision else "",
            "risk_level": self.decision.risk_level.value if self.decision else "",
            "allowed": self.decision.allowed if self.decision else False,
            "is_blocked": self.is_blocked,
            "is_approved": self.is_approved,
            "verification_passed": self.verification_passed,
            "recovery_attempted": self.recovery_attempted,
            "recovery_successful": self.recovery_successful,
            "audit_record_id": self.audit_record_id,
            "elapsed_ms": round(self.elapsed_ms, 2),
            "error": self.error,
        }


# ============================================================================
# SecurityOrchestrator
# ============================================================================


class SecurityOrchestrator:
    """Central security orchestrator that connects all security subsystems.

    Integrates with:
    - Orchestrator (via execute_turn hooks)
    - Verification pipeline (via verification lifecycle hooks)
    - Recovery engine (via recovery lifecycle hooks)
    - Event bus (via security event emission)
    - UI (via approval request callbacks)

    Usage:
        sec = SecurityOrchestrator(
            workspace_root="/workspace",
            governance=governance,
            memory=security_memory,
            analytics=analytics,
            approval=approval_manager,
        )

        # Pre-execution security check
        ctx = sec.check_execution("bash_exec", {"command": "rm -rf /tmp"})

        if ctx.is_blocked:
            return {"error": "Blocked by security policy", "reason": ctx.decision.reason}

        if ctx.decision.requires_approval and not ctx.is_approved:
            # Wait for approval...
            await sec.wait_for_approval(ctx.approval.id)

        # Execute...
        # Post-execution
        sec.record_execution_outcome(ctx, success=True, files_changed=[...])
    """

    def __init__(
        self,
        workspace_root: str = "",
        governance: Optional[ExecutionGovernance] = None,
        memory: Optional[SecurityMemory] = None,
        analytics: Optional[SecurityAnalytics] = None,
        approval: Optional[ApprovalManager] = None,
        event_bus: Any = None,
        audit_logger: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        """Initialize the security orchestrator.

        Args:
            workspace_root: Root of the workspace.
            governance: ExecutionGovernance instance (created fresh if None).
            memory: SecurityMemory instance (created fresh if None).
            analytics: SecurityAnalytics instance (created fresh if None).
            approval: ApprovalManager instance (created fresh if None).
            event_bus: Optional event bus for emitting security events.
            audit_logger: Optional callable for audit logging.
        """
        self._workspace_root = workspace_root

        # Create subsystems if not provided
        self.governance = governance or ExecutionGovernance(
            workspace_root=workspace_root,
        )
        self.memory = memory or SecurityMemory(project_name=workspace_root)
        self.approval = approval or ApprovalManager()
        self.analytics = analytics or SecurityAnalytics(
            governance=self.governance,
            security_memory=self.memory,
        )

        self._event_bus = event_bus
        self._audit_logger = audit_logger or self._default_audit_logger

        # Integration hooks (set by orchestrator)
        self._pre_execution_hooks: List[Callable[
            [str, Dict[str, Any]], None
        ]] = []
        self._post_execution_hooks: List[Callable[
            [SecurityContext], None
        ]] = []

        # Pending approval futures (for async wait)
        self._approval_futures: Dict[str, asyncio.Future] = {}

        # Track active contexts
        self._active_contexts: Dict[str, SecurityContext] = {}

        log.info("SecurityOrchestrator initialized with all subsystems")

    # ------------------------------------------------------------------
    # Pre-Execution Security Check
    # ------------------------------------------------------------------

    async def check_execution(
        self,
        tool_name: str,
        args: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> SecurityContext:
        """Perform a full security check before execution.

        This is the primary entry point for the execution pipeline.

        Args:
            tool_name: The tool being called.
            args: The tool arguments.
            context: Optional execution context (specialist, user info, etc.).

        Returns:
            SecurityContext with the policy decision, approval state, etc.
        """
        start = time.perf_counter()
        ctx = context or {}

        # 1. Make policy decision
        decision = self.governance.decide(tool_name, args, ctx)

        # 2. Check if blocked
        if not decision.allowed:
            sec_ctx = SecurityContext(
                decision=decision,
                is_blocked=True,
                elapsed_ms=(time.perf_counter() - start) * 1000,
            )
            # Violation is recorded in record_execution_outcome (single source of truth)

            # Run pre-execution hooks
            for hook in self._pre_execution_hooks:
                try:
                    hook(tool_name, args)
                except Exception as e:
                    log.warning(f"Pre-execution hook error: {e}")

            self._active_contexts[decision.decision_id] = sec_ctx
            return sec_ctx

        # 3. Check if approval required
        if decision.requires_approval:
            try:
                approval_request = self.approval.request_approval(
                    decision=decision,
                    specialist=ctx.get("specialist", ""),
                    user_context=ctx.get("user_context", ""),
                )

                sec_ctx = SecurityContext(
                    decision=decision,
                    approval=approval_request,
                    is_approved=approval_request.state == ApprovalState.APPROVED,
                    elapsed_ms=(time.perf_counter() - start) * 1000,
                )

                # Set up async wait for approval
                if approval_request.state == ApprovalState.REQUESTED:
                    future = asyncio.get_event_loop().create_future()
                    self._approval_futures[approval_request.id] = future

                self._active_contexts[decision.decision_id] = sec_ctx
                return sec_ctx

            except RuntimeError as e:
                # Too many pending approvals
                sec_ctx = SecurityContext(
                    decision=decision,
                    is_blocked=True,
                    error=str(e),
                    elapsed_ms=(time.perf_counter() - start) * 1000,
                )
                # Record blocking error
                self.memory.record_violation(decision)
                self._active_contexts[decision.decision_id] = sec_ctx
                return sec_ctx

        # 4. Action is allowed without approval
        sec_ctx = SecurityContext(
            decision=decision,
            is_approved=True,
            elapsed_ms=(time.perf_counter() - start) * 1000,
        )

        # Run pre-execution hooks
        for hook in self._pre_execution_hooks:
            try:
                hook(tool_name, args)
            except Exception as e:
                log.warning(f"Pre-execution hook error: {e}")

        self._active_contexts[decision.decision_id] = sec_ctx
        return sec_ctx

    async def wait_for_approval(
        self,
        approval_id: str,
        timeout: float = 300.0,
    ) -> bool:
        """Wait asynchronously for an approval decision.

        Args:
            approval_id: The approval request ID.
            timeout: Maximum wait time in seconds.

        Returns:
            True if approved, False if denied or timed out.
        """
        future = self._approval_futures.get(approval_id)
        if future is None:
            request = self.approval.get_request(approval_id)
            if request is None:
                return False
            if request.state == ApprovalState.APPROVED:
                return True
            return False

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            # Expire the approval
            self.approval.expire_stale_requests()
            return False

    # ------------------------------------------------------------------
    # Post-Execution Recording
    # ------------------------------------------------------------------

    def record_execution_outcome(
        self,
        sec_ctx: SecurityContext,
        success: bool,
        files_changed: Optional[List[str]] = None,
        error_message: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record the outcome of an execution for security tracking.

        Args:
            sec_ctx: The SecurityContext from the pre-execution check.
            success: Whether the execution succeeded.
            files_changed: List of files that were modified.
            error_message: Any error that occurred during execution.
            metadata: Additional metadata.
        """
        decision = sec_ctx.decision

        # Record audit
        audit_data = {
            "decision_id": decision.decision_id,
            "action_type": decision.action_type,
            "action_target": decision.action_target[:200],
            "risk_level": decision.risk_level.value,
            "allowed": decision.allowed,
            "requires_approval": decision.requires_approval,
            "approved": sec_ctx.is_approved,
            "success": success,
            "files_changed": files_changed or [],
            "error": error_message or sec_ctx.error,
            "elapsed_ms": sec_ctx.elapsed_ms,
            **({} if metadata is None else metadata),
        }
        audit_id = self._audit_logger("security_execution", audit_data)
        sec_ctx.audit_record_id = audit_id

        # Update security memory
        if sec_ctx.is_blocked or not success:
            self.memory.record_violation(decision)

        if decision.risk_level in (RiskLevel.APPROVAL_REQUIRED, RiskLevel.RESTRICTED) and success:
            self.memory.record_risky_action(
                target=decision.action_target,
                specialist="",
                tool_name=decision.action_type,
                risk_level=decision.risk_level,
                reason=decision.reason,
            )

        # Run post-execution hooks
        for hook in self._post_execution_hooks:
            try:
                hook(sec_ctx)
            except Exception as e:
                log.warning(f"Post-execution hook error: {e}")

        # Clean up approval future
        if sec_ctx.approval:
            self._approval_futures.pop(sec_ctx.approval.id, None)

        # Clean up active context
        self._active_contexts.pop(decision.decision_id, None)

    # ------------------------------------------------------------------
    # Verification Integration
    # ------------------------------------------------------------------

    def create_verification_context(
        self,
        sec_ctx: SecurityContext,
    ) -> Dict[str, Any]:
        """Create a verification context from a security context.

        This is used by the VerificationPipeline to include security
        information in verification decisions.

        Args:
            sec_ctx: The SecurityContext from the pre-execution check.

        Returns:
            Dict with security information for the verifier.
        """
        return {
            "security_decision": sec_ctx.decision.to_dict(),
            "risk_level": sec_ctx.decision.risk_level.value,
            "requires_approval": sec_ctx.decision.requires_approval,
            "approved": sec_ctx.is_approved,
            "blocked": sec_ctx.is_blocked,
        }

    def on_verification_result(
        self,
        sec_ctx: SecurityContext,
        verification_passed: bool,
        diagnostics: List[str],
    ) -> None:
        """Handle verification results for a security-checked execution.

        Args:
            sec_ctx: The SecurityContext from the check.
            verification_passed: Whether verification passed.
            diagnostics: Verification diagnostic messages.
        """
        sec_ctx.verification_passed = verification_passed

        if not verification_passed and sec_ctx.is_approved:
            log.warning(
                f"Verification failed for approved action "
                f"{sec_ctx.decision.action_type}: {'; '.join(diagnostics)}"
            )
            # Record as a violation for post-hoc analysis
            self.memory.record_violation(sec_ctx.decision)

    # ------------------------------------------------------------------
    # Recovery Integration
    # ------------------------------------------------------------------

    def on_recovery_attempt(
        self,
        sec_ctx: SecurityContext,
        recovery_success: bool,
        strategy: str,
    ) -> None:
        """Handle recovery outcomes for security-related failures.

        Args:
            sec_ctx: The SecurityContext from the check.
            recovery_success: Whether recovery succeeded.
            strategy: The recovery strategy that was used.
        """
        sec_ctx.recovery_attempted = True
        sec_ctx.recovery_successful = recovery_success

        # Find the violation entry in security memory
        violations = self.memory.get_recent_violations(n=50)
        for v in violations:
            if sec_ctx.decision.action_target[:100] in v.target:
                self.memory.record_recovery_outcome(
                    violation_id=v.id,
                    success=recovery_success,
                    strategy=strategy,
                    details={
                        "action_type": sec_ctx.decision.action_type,
                        "risk_level": sec_ctx.decision.risk_level.value,
                    },
                )
                break

    # ------------------------------------------------------------------
    # Event Bus Integration
    # ------------------------------------------------------------------

    async def emit_security_event(
        self,
        event_type: str,
        data: Dict[str, Any],
    ) -> None:
        """Emit a security event to the event bus.

        Args:
            event_type: The type of security event.
            data: Event data payload.
        """
        if self._event_bus is None:
            return

        try:
            from runtime_next.models.events import BaseEvent, EventType

            event = BaseEvent(
                id=f"sec_{event_type}_{time.time()}",
                type=EventType.LOG_MESSAGE,
                payload={
                    "security_event": event_type,
                    "workspace": self._workspace_root,
                    "timestamp": time.time(),
                    **data,
                },
            )
            await self._event_bus.publish(event)
        except Exception as e:
            log.debug(f"Failed to emit security event: {e}")

    # ------------------------------------------------------------------
    # Security Report
    # ------------------------------------------------------------------

    def generate_security_report(self, hours: float = 24.0) -> SecurityAnalyticsReport:
        """Generate a comprehensive security analytics report.

        Args:
            hours: Time window for the report.

        Returns:
            A SecurityAnalyticsReport with all metrics.
        """
        return self.analytics.generate_report(hours=hours)

    def get_posture_summary(self) -> str:
        """Get a one-line security posture summary."""
        return self.analytics.get_posture_summary()

    # ------------------------------------------------------------------
    # Hook Registration
    # ------------------------------------------------------------------

    def register_pre_execution_hook(
        self,
        hook: Callable[[str, Dict[str, Any]], None],
    ) -> None:
        """Register a hook called before execution.

        Args:
            hook: Called with (tool_name, args) before execution.
        """
        self._pre_execution_hooks.append(hook)

    def register_post_execution_hook(
        self,
        hook: Callable[[SecurityContext], None],
    ) -> None:
        """Register a hook called after execution.

        Args:
            hook: Called with the SecurityContext after execution.
        """
        self._post_execution_hooks.append(hook)

    # ------------------------------------------------------------------
    # Audit Integration
    # ------------------------------------------------------------------

    @staticmethod
    def _default_audit_logger(
        event_type: str,
        data: Dict[str, Any],
    ) -> str:
        """Default audit logger â€” logs to Python logger.

        Args:
            event_type: The type of audit event.
            data: Event data.

        Returns:
            An audit record identifier.
        """
        record_id = f"audit_{event_type}_{time.time()}"
        log.info(f"[AUDIT] {event_type}: {json.dumps(data, default=str)[:500]}")
        return record_id

    # ------------------------------------------------------------------
    # Verification Security Verifier
    # ------------------------------------------------------------------

    async def verify_sandbox_security(
        self,
        node_id: str,
        scope: Any,
        context: Dict[str, Any],
    ) -> Any:
        """Verifier plugin for sandbox security validation.

        This can be registered with the VerificationPipeline as a
        SANDBOX_VALIDATION verifier.

        Args:
            node_id: The node being verified.
            scope: The verification scope.
            context: Verification context (should include security_context).

        Returns:
            A VerificationResult (compatible with VerificationPipeline).
        """
        from runtime_next.verification.types import (
            VerificationResult,
            Confidence,
            Severity,
            Retryability,
            VerificationType,
        )

        import hashlib

        # Check for security context
        sec_ctx = context.get("security_context")
        if sec_ctx is None:
            # No security context â€” this is a warning
            return VerificationResult(
                verification_id=hashlib.sha256(
                    f"sec_verify_{node_id}".encode()
                ).hexdigest()[:16],
                node_id=node_id,
                verification_type=VerificationType.SANDBOX_VALIDATION,
                success=True,
                confidence=Confidence.LOW,
                severity=Severity.INFO,
                retryability=Retryability.SAFE,
                diagnostics=["No security context available â€” skipping security verification"],
                provenance="security_orchestrator",
            )

        # Verify that blocked actions were not executed
        if sec_ctx.is_blocked:
            return VerificationResult(
                verification_id=hashlib.sha256(
                    f"sec_blocked_{node_id}".encode()
                ).hexdigest()[:16],
                node_id=node_id,
                verification_type=VerificationType.SANDBOX_VALIDATION,
                success=False,
                confidence=Confidence.HIGH,
                severity=Severity.CRITICAL,
                retryability=Retryability.NEVER,
                diagnostics=[
                    f"Action was BLOCKED by security policy but reached verification: "
                    f"{sec_ctx.decision.action_type} - {sec_ctx.decision.reason}"
                ],
                provenance="security_orchestrator",
            )

        # Verify that approval was obtained for approval-required actions
        if sec_ctx.decision.requires_approval and not sec_ctx.is_approved:
            return VerificationResult(
                verification_id=hashlib.sha256(
                    f"sec_no_approval_{node_id}".encode()
                ).hexdigest()[:16],
                node_id=node_id,
                verification_type=VerificationType.SANDBOX_VALIDATION,
                success=False,
                confidence=Confidence.HIGH,
                severity=Severity.ERROR,
                retryability=Retryability.NEVER,
                diagnostics=[
                    f"Action required approval but was not approved: "
                    f"{sec_ctx.decision.action_type}"
                ],
                provenance="security_orchestrator",
            )

        # All security checks passed
        return VerificationResult(
            verification_id=hashlib.sha256(
                f"sec_passed_{node_id}".encode()
            ).hexdigest()[:16],
            node_id=node_id,
            verification_type=VerificationType.SANDBOX_VALIDATION,
            success=True,
            confidence=Confidence.HIGH,
            severity=Severity.INFO,
            retryability=Retryability.SAFE,
            diagnostics=["Security verification passed"],
            provenance="security_orchestrator",
        )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def get_active_contexts(self) -> List[Dict[str, Any]]:
        """Get all active security contexts (for diagnostics)."""
        return [
            ctx.to_dict()
            for ctx in self._active_contexts.values()
        ]

    def clear_active_contexts(self) -> None:
        """Clear all active security contexts."""
        self._active_contexts.clear()

    def get_system_status(self) -> Dict[str, Any]:
        """Get the status of all security subsystems."""
        return {
            "governance": self.governance.get_stats(),
            "approval": self.approval.get_stats(),
            "memory": self.memory.get_summary(),
        }
