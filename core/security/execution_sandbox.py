"""core/security/execution_sandbox.py — Execution Sandboxing & Security Integration

Phase 17: Security hardening for execution with:
  1. ExecutionSandbox — Capability-based tool access per session (allowed tools/categories)
  2. RollbackApprovalGate — Approval required before destructive rollbacks
  3. SecurityIntegration — Hooks wiring sandbox and approval gates into
     ToolExecutionRegistry and PersistentSandboxSession via SecurityOrchestrator

Design principles:
  - Fail closed: any error in capability checks blocks execution
  - Least privilege: sessions get minimum capabilities needed
  - Every decision is auditable through SecurityOrchestrator
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from core.execution.tool_registry import (
    ToolSpec,
    ToolCategory,
)
from core.execution.sandbox_session import (
    PersistentSandboxSession,
)
from core.security.execution_governance import PolicyDecision, RiskLevel
from core.security.security_orchestrator import (
    SecurityOrchestrator,
    SecurityContext,
)

log = logging.getLogger("aelvo.security.execution_sandbox")


# ============================================================================
# Enums
# ============================================================================


class SandboxPolicyAction(str, Enum):
    """What action the sandbox policy takes on a tool call."""
    ALLOW = "allow"
    """The tool is allowed to execute."""

    BLOCK = "block"
    """The tool is blocked from executing."""

    REQUIRE_APPROVAL = "require_approval"
    """The tool requires approval before executing."""


class RollbackRiskLevel(str, Enum):
    """Risk level of a rollback operation."""
    SAFE = "safe"
    """Rolling back a few recently modified files — low risk."""

    MODERATE = "moderate"
    """Rolling back multiple files or a checkpoint — moderate risk."""

    DESTRUCTIVE = "destructive"
    """Rolling back many files or deleting post-checkpoint files — high risk."""


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class SandboxCapability:
    """Capabilities granted to a session for tool execution.

    Attributes:
        allowed_tools: Set of tool names explicitly allowed.
        allowed_categories: Set of tool categories explicitly allowed.
        blocked_tools: Set of tool names explicitly denied (overrides allowed).
        max_concurrent_executions: Max parallel tool executions (0 = unlimited).
        require_approval_for: Set of tool names that require approval.
        bypass_approval: Whether this session can bypass approval (trusted sessions).
    """

    allowed_tools: Set[str] = field(default_factory=set)
    allowed_categories: Set[ToolCategory] = field(default_factory=set)
    blocked_tools: Set[str] = field(default_factory=set)
    max_concurrent_executions: int = 0
    require_approval_for: Set[str] = field(default_factory=set)
    bypass_approval: bool = False

    def allows_tool(self, tool_name: str, spec: Optional[ToolSpec] = None) -> bool:
        """Check if a tool is allowed by this capability set.

        Args:
            tool_name: The tool name to check.
            spec: Optional ToolSpec for category-based checks.

        Returns:
            True if the tool is allowed.
        """
        # Blocked tools take precedence
        if tool_name in self.blocked_tools:
            return False

        # Explicitly allowed tools
        if tool_name in self.allowed_tools:
            return True

        # Category-based check
        if spec and spec.category in self.allowed_categories:
            return True

        # If both allowed lists are empty, all tools are allowed (no restriction)
        if not self.allowed_tools and not self.allowed_categories:
            return True

        return False

    def requires_approval(self, tool_name: str) -> bool:
        """Check if a tool requires approval for this session."""
        return tool_name in self.require_approval_for and not self.bypass_approval

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed_tools": sorted(self.allowed_tools),
            "allowed_categories": [c.value for c in self.allowed_categories],
            "blocked_tools": sorted(self.blocked_tools),
            "max_concurrent_executions": self.max_concurrent_executions,
            "require_approval_for": sorted(self.require_approval_for),
            "bypass_approval": self.bypass_approval,
        }


@dataclass
class RollbackApprovalRequest:
    """A pending approval request for a destructive rollback.

    Attributes:
        session_id: The session requesting the rollback.
        checkpoint_id: The checkpoint being rolled back to.
        reason: Why the rollback is needed.
        file_count: Number of files affected by the rollback.
        risk_level: Computed risk level of the rollback.
        approved: Whether the rollback has been approved.
        approved_by: Who approved the rollback.
        timestamp: When the request was created.
    """

    session_id: str
    checkpoint_id: str
    reason: str = ""
    file_count: int = 0
    risk_level: RollbackRiskLevel = RollbackRiskLevel.SAFE
    approved: bool = False
    approved_by: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id[:12] if self.session_id else "",
            "checkpoint_id": self.checkpoint_id[:12] if self.checkpoint_id else "",
            "reason": self.reason[:100],
            "file_count": self.file_count,
            "risk_level": self.risk_level.value,
            "approved": self.approved,
            "approved_by": self.approved_by,
            "timestamp": self.timestamp,
        }


@dataclass
class SandboxAuditRecord:
    """Audit record for a sandbox-checked execution.

    Attributes:
        tool_name: The tool that was checked.
        session_id: The session making the request.
        action: What the sandbox decided (ALLOW/BLOCK/REQUIRE_APPROVAL).
        reason: Why the decision was made.
        capability_snapshot: Snapshot of capabilities at decision time.
        security_context: Optional SecurityContext from SecurityOrchestrator.
        timestamp: When the decision was made.
    """

    tool_name: str
    session_id: str
    action: SandboxPolicyAction
    reason: str = ""
    capability_snapshot: Dict[str, Any] = field(default_factory=dict)
    security_context: Optional[Dict[str, Any]] = None
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "session_id": self.session_id[:12] if self.session_id else "",
            "action": self.action.value,
            "reason": self.reason[:200],
            "timestamp": self.timestamp,
        }


# ============================================================================
# ExecutionSandbox
# ============================================================================


class ExecutionSandbox:
    """Capability-based execution sandbox that controls tool access per session.

    Each session gets a SandboxCapability defining which tools/tool categories
    are allowed, blocked, or require approval. Integrates with SecurityOrchestrator
    for policy-backed decisions and audit logging.

    Usage:
        sandbox = ExecutionSandbox()
        sandbox.set_session_capabilities("session_1", SandboxCapability(
            allowed_categories={ToolCategory.FILE_OPERATION},
            blocked_tools={"bash_exec"},
        ))

        # Check before execution
        action = sandbox.check_tool("bash_exec", "session_1")

        if action == SandboxPolicyAction.BLOCK:
            return {"error": "Tool blocked by sandbox policy"}
    """

    def __init__(
        self,
        security_orchestrator: Optional[SecurityOrchestrator] = None,
        default_capability: Optional[SandboxCapability] = None,
        max_audit_records: int = 1000,
    ):
        self._orchestrator = security_orchestrator
        self._default = default_capability or SandboxCapability(
            allowed_categories={ToolCategory.FILE_OPERATION, ToolCategory.CODE_ANALYSIS},
            blocked_tools=set(),
        )
        self._session_capabilities: Dict[str, SandboxCapability] = {}
        self._audit_records: List[SandboxAuditRecord] = []
        self._max_audit = max_audit_records
        self._concurrent_counts: Dict[str, int] = {}

        log.info(
            "ExecutionSandbox initialized (default_categories=%s)",
            [c.value for c in self._default.allowed_categories],
        )

    # ── Capability Management ───────────────────────────────────────

    def set_session_capabilities(
        self,
        session_id: str,
        capabilities: SandboxCapability,
    ) -> None:
        """Set or update capabilities for a session.

        Args:
            session_id: The session identifier.
            capabilities: The capabilities to assign.
        """
        self._session_capabilities[session_id] = capabilities
        log.info(
            "Capabilities set for session %s: %d tools, %d categories",
            session_id[:12] if len(session_id) > 12 else session_id,
            len(capabilities.allowed_tools),
            len(capabilities.allowed_categories),
        )

    def get_session_capabilities(
        self,
        session_id: str,
    ) -> SandboxCapability:
        """Get the capabilities for a session.

        Falls back to default capabilities if not explicitly set.

        Args:
            session_id: The session identifier.

        Returns:
            The SandboxCapability for the session.
        """
        return self._session_capabilities.get(session_id, self._default)

    def remove_session(self, session_id: str) -> None:
        """Remove a session's capabilities.

        Args:
            session_id: The session to remove.
        """
        self._session_capabilities.pop(session_id, None)
        self._concurrent_counts.pop(session_id, None)

    def update_default_capabilities(self, capabilities: SandboxCapability) -> None:
        """Update the default capabilities for sessions without explicit config.

        Args:
            capabilities: The new default capabilities.
        """
        self._default = capabilities

    # ── Tool Checks ─────────────────────────────────────────────────

    async def check_tool(
        self,
        tool_name: str,
        session_id: str,
        args: Optional[Dict[str, Any]] = None,
        spec: Optional[ToolSpec] = None,
    ) -> Tuple[SandboxPolicyAction, str, Optional[SecurityContext]]:
        """Check whether a tool is allowed for a session.

        The check evaluates in order:
        1. Is the tool in the blocklist?
        2. Is the tool in the allowlist or allowed category?
        3. Does the tool require approval?
        4. Are concurrent execution limits exceeded?
        5. Is there a SecurityOrchestrator policy decision?

        Args:
            tool_name: The tool to check.
            session_id: The session requesting execution.
            args: Optional tool arguments (passed to SecurityOrchestrator).
            spec: Optional ToolSpec for category-based checks.

        Returns:
            Tuple of (action, reason, optional security_context).
        """
        caps = self.get_session_capabilities(session_id)
        security_ctx: Optional[SecurityContext] = None

        # 1. Check blocklist
        if tool_name in caps.blocked_tools:
            action = SandboxPolicyAction.BLOCK
            reason = f"Tool '{tool_name}' is blocked by session sandbox policy"
            self._record_audit(tool_name, session_id, action, reason, caps)
            return action, reason, None

        # 2. Check allowlist
        if not caps.allows_tool(tool_name, spec):
            action = SandboxPolicyAction.BLOCK
            reason = (
                f"Tool '{tool_name}' is not in allowed tools or categories "
                f"for this session"
            )
            self._record_audit(tool_name, session_id, action, reason, caps)
            return action, reason, None

        # 3. Check approval requirement
        if caps.requires_approval(tool_name):
            action = SandboxPolicyAction.REQUIRE_APPROVAL
            reason = f"Tool '{tool_name}' requires approval for this session"

            # Check with SecurityOrchestrator if available
            if self._orchestrator:
                try:
                    security_ctx = await self._orchestrator.check_execution(
                        tool_name=tool_name,
                        args=args or {},
                        context={"session_id": session_id, "sandbox_check": True},
                    )
                    if security_ctx.is_blocked:
                        action = SandboxPolicyAction.BLOCK
                        reason = security_ctx.decision.reason
                    elif security_ctx.is_approved:
                        action = SandboxPolicyAction.ALLOW
                        reason = "Tool approved by security policy"
                except Exception as e:
                    log.warning("SecurityOrchestrator check failed: %s", e)
                    # Fail closed
                    action = SandboxPolicyAction.BLOCK
                    reason = f"Security check failed: {e}"

            self._record_audit(tool_name, session_id, action, reason, caps, security_ctx)
            return action, reason, security_ctx

        # 4. Check concurrent execution limit
        if caps.max_concurrent_executions > 0:
            current = self._concurrent_counts.get(session_id, 0)
            if current >= caps.max_concurrent_executions:
                action = SandboxPolicyAction.BLOCK
                reason = (
                    f"Concurrent execution limit reached "
                    f"({current}/{caps.max_concurrent_executions})"
                )
                self._record_audit(tool_name, session_id, action, reason, caps)
                return action, reason, None

        # 5. SecurityOrchestrator policy check (even for allowed tools)
        if self._orchestrator:
            try:
                security_ctx = await self._orchestrator.check_execution(
                    tool_name=tool_name,
                    args=args or {},
                    context={"session_id": session_id, "sandbox_check": True},
                )
                if security_ctx.is_blocked:
                    action = SandboxPolicyAction.BLOCK
                    reason = security_ctx.decision.reason
                    self._record_audit(tool_name, session_id, action, reason, caps, security_ctx)
                    return action, reason, security_ctx
                if not security_ctx.is_approved:
                    action = SandboxPolicyAction.REQUIRE_APPROVAL
                    reason = security_ctx.decision.reason
                    self._record_audit(tool_name, session_id, action, reason, caps, security_ctx)
                    return action, reason, security_ctx
            except Exception as e:
                log.warning("SecurityOrchestrator check failed: %s", e)
                # Fail closed
                action = SandboxPolicyAction.BLOCK
                reason = f"Security check failed: {e}"
                self._record_audit(tool_name, session_id, action, reason, caps)
                return action, reason, None

        # All checks passed
        action = SandboxPolicyAction.ALLOW
        reason = f"Tool '{tool_name}' is allowed by session sandbox policy"
        self._record_audit(tool_name, session_id, action, reason, caps, security_ctx)
        return action, reason, security_ctx

    def release_execution_slot(self, session_id: str) -> None:
        """Release a concurrent execution slot for a session.

        Args:
            session_id: The session that finished execution.
        """
        current = self._concurrent_counts.get(session_id, 0)
        if current > 0:
            self._concurrent_counts[session_id] = current - 1

    def acquire_execution_slot(self, session_id: str) -> bool:
        """Try to acquire a concurrent execution slot for a session.

        Args:
            session_id: The session requesting a slot.

        Returns:
            True if a slot was acquired.
        """
        caps = self.get_session_capabilities(session_id)
        if caps.max_concurrent_executions == 0:
            return True  # Unlimited

        current = self._concurrent_counts.get(session_id, 0)
        if current >= caps.max_concurrent_executions:
            return False

        self._concurrent_counts[session_id] = current + 1
        return True

    # ── Audit ───────────────────────────────────────────────────────

    def _record_audit(
        self,
        tool_name: str,
        session_id: str,
        action: SandboxPolicyAction,
        reason: str,
        capabilities: SandboxCapability,
        security_ctx: Optional[SecurityContext] = None,
    ) -> None:
        """Record a sandbox audit entry."""
        record = SandboxAuditRecord(
            tool_name=tool_name,
            session_id=session_id,
            action=action,
            reason=reason,
            capability_snapshot=capabilities.to_dict(),
            security_context=security_ctx.to_dict() if security_ctx else None,
        )
        self._audit_records.append(record)
        if len(self._audit_records) > self._max_audit:
            self._audit_records = self._audit_records[-self._max_audit:]

    def get_audit_records(
        self,
        session_id: Optional[str] = None,
        action: Optional[SandboxPolicyAction] = None,
        limit: int = 50,
    ) -> List[SandboxAuditRecord]:
        """Get audit records, optionally filtered.

        Args:
            session_id: Filter by session.
            action: Filter by action type.
            limit: Max results.

        Returns:
            List of matching audit records.
        """
        records = self._audit_records
        if session_id:
            records = [r for r in records if r.session_id == session_id]
        if action:
            records = [r for r in records if r.action == action]
        return records[-limit:]

    # ── Reporting ───────────────────────────────────────────────────

    def get_statistics(self) -> Dict[str, Any]:
        """Get sandbox statistics."""
        total_checks = len(self._audit_records)
        allowed = sum(1 for r in self._audit_records if r.action == SandboxPolicyAction.ALLOW)
        blocked = sum(1 for r in self._audit_records if r.action == SandboxPolicyAction.BLOCK)
        approval = sum(
            1 for r in self._audit_records
            if r.action == SandboxPolicyAction.REQUIRE_APPROVAL
        )

        return {
            "total_checks": total_checks,
            "allowed": allowed,
            "blocked": blocked,
            "approval_required": approval,
            "blocked_ratio": round(blocked / total_checks, 4) if total_checks else 0.0,
            "active_sessions": len(self._session_capabilities),
            "audit_records": len(self._audit_records),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        stats = self.get_statistics()
        lines = [
            "  ── EXECUTION SANDBOX ──",
            f"  Checks: {stats['total_checks']} total  |  "
            f"Allowed: {stats['allowed']}  |  "
            f"Blocked: {stats['blocked']}  |  "
            f"Approval: {stats['approval_required']}",
            f"  Active sessions: {stats['active_sessions']}",
            f"  Audit records: {stats['audit_records']}",
        ]

        if stats["total_checks"] > 0:
            lines.append(f"  Blocked ratio: {stats['blocked_ratio']:.1%}")

        lines.append("  ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)


# ============================================================================
# RollbackApprovalGate
# ============================================================================


class RollbackApprovalGate:
    """Approval gate for destructive rollback operations.

    Before a PersistentSandboxSession can roll back to a checkpoint,
    this gate evaluates the rollback's risk and, if necessary, requires
    approval through the SecurityOrchestrator's ApprovalManager.

    Thresholds determine when approval is required:
      - SAFE: < 3 files affected
      - MODERATE: 3-10 files affected
      - DESTRUCTIVE: > 10 files, or deleting post-checkpoint files

    Usage:
        gate = RollbackApprovalGate(security_orchestrator)
        request = gate.evaluate_rollback(session, checkpoint_id)

        if request.risk_level == RollbackRiskLevel.DESTRUCTIVE:
            if not request.approved:
                return {"error": "Rollback requires approval"}

        gate.proceed_with_rollback(session, checkpoint_id)
    """

    def __init__(
        self,
        security_orchestrator: Optional[SecurityOrchestrator] = None,
        moderate_threshold: int = 3,
        destructive_threshold: int = 10,
        auto_approve_safe: bool = True,
    ):
        self._orchestrator = security_orchestrator
        self._moderate_threshold = moderate_threshold
        self._destructive_threshold = destructive_threshold
        self._auto_approve_safe = auto_approve_safe

        # Track pending requests
        self._pending_requests: Dict[str, RollbackApprovalRequest] = {}
        self._history: List[RollbackApprovalRequest] = []

        log.info(
            "RollbackApprovalGate initialized (moderate=%d, destructive=%d)",
            moderate_threshold, destructive_threshold,
        )

    def evaluate_rollback(
        self,
        session: PersistentSandboxSession,
        checkpoint_id: str,
        reason: str = "",
    ) -> RollbackApprovalRequest:
        """Evaluate a rollback request and determine if approval is needed.

        Args:
            session: The session requesting the rollback.
            checkpoint_id: The checkpoint to roll back to.
            reason: Why the rollback is needed.

        Returns:
            A RollbackApprovalRequest with risk level and approval status.
        """
        # Count files affected
        total_files = (
            len(session.state.files_created)
            + len(session.state.files_modified)
            + len(session.state.files_deleted)
        )

        # Determine risk level
        if total_files >= self._destructive_threshold:
            risk = RollbackRiskLevel.DESTRUCTIVE
        elif total_files >= self._moderate_threshold:
            risk = RollbackRiskLevel.MODERATE
        else:
            risk = RollbackRiskLevel.SAFE

        request = RollbackApprovalRequest(
            session_id=session.session_id,
            checkpoint_id=checkpoint_id,
            reason=reason or "No reason provided",
            file_count=total_files,
            risk_level=risk,
        )

        # Auto-approve safe rollbacks
        if risk == RollbackRiskLevel.SAFE and self._auto_approve_safe:
            request.approved = True
            request.approved_by = "auto:safe_rollback"
            self._history.append(request)
            log.info(
                "Rollback auto-approved (safe): session=%s checkpoint=%s",
                session.session_id[:12], checkpoint_id[:12],
            )
            return request

        # Check with ApprovalManager via SecurityOrchestrator is done
        # asynchronously by the caller (async check_rollback_allowed).
        # Here we only set up the request for DESTRUCTIVE risk level.
        if risk == RollbackRiskLevel.DESTRUCTIVE and not request.approved:
            # DESTRUCTIVE rollbacks require explicit approval
            # The caller should use check_rollback_allowed with pre_approved
            # after performing an async SecurityOrchestrator check.
            pass

        # MODERATE rollbacks are approved by default (but tracked)
        if risk == RollbackRiskLevel.MODERATE and not request.approved:
            request.approved = True
            request.approved_by = "auto:moderate_rollback"

        # Store pending if not approved
        if not request.approved:
            self._pending_requests[session.session_id] = request
            log.info(
                "Rollback requires approval: session=%s files=%d risk=%s",
                session.session_id[:12], total_files, risk.value,
            )

        self._history.append(request)
        return request

    def approve_rollback(
        self,
        session_id: str,
        approved_by: str = "operator",
    ) -> Optional[RollbackApprovalRequest]:
        """Approve a pending rollback request.

        Args:
            session_id: The session whose rollback to approve.
            approved_by: Who approved the rollback.

        Returns:
            The approved request, or None if no pending request.
        """
        request = self._pending_requests.pop(session_id, None)
        if request is None:
            return None

        request.approved = True
        request.approved_by = approved_by
        log.info(
            "Rollback approved: session=%s by=%s",
            session_id[:12], approved_by,
        )
        return request

    def deny_rollback(
        self,
        session_id: str,
        denied_by: str = "operator",
    ) -> Optional[RollbackApprovalRequest]:
        """Deny a pending rollback request.

        Args:
            session_id: The session whose rollback to deny.
            denied_by: Who denied the rollback.

        Returns:
            The denied request (removed from pending), or None.
        """
        request = self._pending_requests.pop(session_id, None)
        if request:
            log.info(
                "Rollback denied: session=%s by=%s",
                session_id[:12], denied_by,
            )
        return request

    def check_rollback_allowed(
        self,
        session: PersistentSandboxSession,
        checkpoint_id: str,
        reason: str = "",
        pre_approved: bool = False,
    ) -> Tuple[bool, str, RollbackApprovalRequest]:
        """Complete check: evaluate risk, check approval, return result.

        This is the primary API for session rollback gates.

        Args:
            session: The session requesting rollback.
            checkpoint_id: The target checkpoint.
            reason: Why rollback is needed.
            pre_approved: If True, the rollback has been pre-approved
                (e.g., by an async SecurityOrchestrator check).

        Returns:
            Tuple of (allowed, message, request).
        """
        request = self.evaluate_rollback(session, checkpoint_id, reason)

        # Apply pre-approval from async orchestration check
        if pre_approved and not request.approved:
            request.approved = True
            request.approved_by = "security_orchestrator"
            # Remove from pending if it was added
            self._pending_requests.pop(session.session_id, None)

        if request.risk_level == RollbackRiskLevel.SAFE and request.approved:
            return True, "Safe rollback auto-approved", request

        if request.approved:
            return True, f"Rollback approved by {request.approved_by}", request

        # Not approved — check if there's a pending request
        pending = self._pending_requests.get(session.session_id)
        if pending:
            return False, (
                f"Rollback requires approval: {pending.file_count} files, "
                f"risk={pending.risk_level.value}"
            ), request

        return False, "Rollback requires approval", request

    # ── Reporting ───────────────────────────────────────────────────

    def get_pending_requests(self) -> List[RollbackApprovalRequest]:
        """Get all pending rollback approval requests."""
        return list(self._pending_requests.values())

    def get_history(
        self,
        limit: int = 50,
    ) -> List[RollbackApprovalRequest]:
        """Get rollback approval history."""
        return self._history[-limit:]

    def get_statistics(self) -> Dict[str, Any]:
        """Get gate statistics."""
        total = len(self._history)
        approved = sum(1 for r in self._history if r.approved)
        denied = sum(1 for r in self._history if not r.approved)
        destructive = sum(
            1 for r in self._history
            if r.risk_level == RollbackRiskLevel.DESTRUCTIVE
        )
        moderate = sum(
            1 for r in self._history
            if r.risk_level == RollbackRiskLevel.MODERATE
        )
        safe = sum(
            1 for r in self._history
            if r.risk_level == RollbackRiskLevel.SAFE
        )

        return {
            "total_rollbacks": total,
            "approved": approved,
            "denied": denied,
            "destructive": destructive,
            "moderate": moderate,
            "safe": safe,
            "pending": len(self._pending_requests),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        stats = self.get_statistics()
        lines = [
            "  ── ROLLBACK APPROVAL GATE ──",
            f"  Rollbacks: {stats['total_rollbacks']} total  |  "
            f"Approved: {stats['approved']}  |  "
            f"Denied: {stats['denied']}",
            f"  Risk: {stats['safe']} safe  |  "
            f"{stats['moderate']} moderate  |  "
            f"{stats['destructive']} destructive",
            f"  Pending approvals: {stats['pending']}",
        ]
        lines.append("  ── ── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)


# ============================================================================
# SecurityIntegration Hooks
# ============================================================================


class SecurityIntegration:
    """Factory for creating security hooks that wire ExecutionSandbox and
    RollbackApprovalGate into ToolExecutionRegistry and PersistentSandboxSession.

    Usage:
        integration = SecurityIntegration(sandbox, rollback_gate)

        # Create registry execution hook
        registry.register_pre_execution_hook(
            integration.create_execution_hook("session_1")
        )

        # Create session rollback gate
        session.create_checkpoint("before_risky_op")
        if not integration.check_rollback(session, checkpoint_id, reason):
            return {"error": "Rollback not approved"}
    """

    def __init__(
        self,
        sandbox: ExecutionSandbox,
        rollback_gate: RollbackApprovalGate,
        security_orchestrator: Optional[SecurityOrchestrator] = None,
    ):
        self._sandbox = sandbox
        self._rollback_gate = rollback_gate
        self._orchestrator = security_orchestrator

    # ── Execution Hooks ─────────────────────────────────────────────

    def create_pre_execution_hook(
        self,
        session_id: str,
    ) -> Callable[[str, Dict[str, Any]], None]:
        """Create a pre-execution hook for a session.

        The hook checks the sandbox policy before each tool execution.
        Raises PermissionError if the tool is blocked.

        Args:
            session_id: The session to check.

        Returns:
            A callable hook for ToolExecutionRegistry pre-execution.
        """
        sandbox = self._sandbox
        sid = session_id

        def hook(tool_name: str, args: Dict[str, Any]) -> None:
            # Check the sandbox (synchronous check for sync hooks)
            caps = sandbox.get_session_capabilities(sid)

            if tool_name in caps.blocked_tools:
                raise PermissionError(
                    f"Tool '{tool_name}' is blocked by sandbox policy for session {sid[:12]}"
                )

            if not caps.allows_tool(tool_name):
                raise PermissionError(
                    f"Tool '{tool_name}' is not allowed by sandbox policy for session {sid[:12]}"
                )

        return hook

    def create_post_execution_hook(
        self,
        session_id: str,
    ) -> Callable[[Any], None]:
        """Create a post-execution hook for a session.

        Currently releases the concurrent execution slot.

        Args:
            session_id: The session that executed.

        Returns:
            A callable hook.
        """
        sandbox = self._sandbox
        sid = session_id

        def hook(context: Any) -> None:
            sandbox.release_execution_slot(sid)

        return hook

    # ── Rollback Hooks ──────────────────────────────────────────────

    def check_rollback(
        self,
        session: PersistentSandboxSession,
        checkpoint_id: str,
        reason: str = "",
    ) -> Tuple[bool, str]:
        """Check if a rollback is allowed by policy.

        Uses the RollbackApprovalGate to evaluate risk and approval status.

        Args:
            session: The session requesting rollback.
            checkpoint_id: The checkpoint to roll back to.
            reason: Why the rollback is needed.

        Returns:
            Tuple of (allowed, message).
        """
        allowed, message, request = self._rollback_gate.check_rollback_allowed(
            session, checkpoint_id, reason,
        )

        if not allowed:
            log.warning(
                "Rollback blocked: session=%s reason='%s'",
                session.session_id[:12], message,
            )

        # Record outcome with SecurityOrchestrator
        if self._orchestrator:
            decision = PolicyDecision(
                action_type="rollback",
                action_target=f"checkpoint:{checkpoint_id}",
                risk_level=(
                    RiskLevel.SAFE
                    if request.risk_level == RollbackRiskLevel.SAFE
                    else RiskLevel.RESTRICTED
                ),
                allowed=allowed,
                requires_approval=not allowed,
                reason=reason or "Rollback check",
            )
            sec_ctx = SecurityContext(
                decision=decision,
                is_blocked=not allowed,
                is_approved=allowed,
            )
            self._orchestrator.record_execution_outcome(
                sec_ctx=sec_ctx,
                success=allowed,
                error_message="" if allowed else "Rollback blocked by policy",
            )

        return allowed, message

    # ── Session Setup ───────────────────────────────────────────────

    def configure_session(
        self,
        session_id: str,
        capabilities: Optional[SandboxCapability] = None,
    ) -> None:
        """Configure a session with sandbox capabilities.

        Sets capabilities on the ExecutionSandbox so that
        `ExecutionSandbox.check_tool()` can enforce session-specific
        policies.  Hook registration uses `create_pre_execution_hook`
        / `create_post_execution_hook` methods directly — they are
        NOT registered globally on the SecurityOrchestrator to avoid
        multi-session conflicts.

        Args:
            session_id: The session to configure.
            capabilities: Optional capabilities for the session.
        """
        if capabilities:
            self._sandbox.set_session_capabilities(session_id, capabilities)

    # ── Reports ─────────────────────────────────────────────────────

    def get_system_status(self) -> Dict[str, Any]:
        """Get combined status of all security integration components."""
        return {
            "sandbox": self._sandbox.get_statistics(),
            "rollback_gate": self._rollback_gate.get_statistics(),
        }

    def to_terminal_display(self) -> str:
        """Combined terminal display for all components."""
        lines = [
            "  ── SECURITY INTEGRATION ──",
        ]
        lines.append(self._sandbox.to_terminal_display())
        lines.append(self._rollback_gate.to_terminal_display())
        lines.append("  ── ── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)
