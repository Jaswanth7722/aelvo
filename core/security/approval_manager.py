"""ApprovalManager — High-Risk Action Approval Workflows and Escalation.

Manages the lifecycle of approval requests for high-risk actions:
  Requested → Reviewing → Approved | Denied → Escalated

Every approval decision is auditable and attributable.
Integrates with the UI for user-facing approval prompts.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Awaitable

from .execution_governance import PolicyDecision, RiskLevel

log = logging.getLogger("aelvo.security.approval")


# ============================================================================
# Enums
# ============================================================================


class ApprovalState(str, Enum):
    """Lifecycle state of an approval request."""

    REQUESTED = "requested"
    """Approval has been requested but not yet reviewed."""

    REVIEWING = "reviewing"
    """The approval request is being actively reviewed."""

    APPROVED = "approved"
    """The action has been approved."""

    DENIED = "denied"
    """The action has been denied."""

    ESCALATED = "escalated"
    """The request has been escalated to a higher authority."""

    EXPIRED = "expired"
    """The request expired without a decision."""


class EscalationPath(str, Enum):
    """Available escalation paths for denied or stuck approvals."""

    USER = "user"
    """Escalate to the end user."""

    SPECIALIST = "specialist"
    """Escalate to a senior specialist (e.g., SENTINEL)."""

    ADMIN = "admin"
    """Escalate to the system administrator."""

    AUTOMATIC = "automatic"
    """Automatically resolve based on policy (higher trust override)."""


# ============================================================================
# Data Types
# ============================================================================


@dataclass
class ApprovalRequest:
    """A request for approval to execute a high-risk action."""

    id: str = ""
    """Unique request identifier."""

    policy_decision: Optional[PolicyDecision] = None
    """The policy decision that triggered this approval request."""

    action_type: str = ""
    """Type of action (tool name, command, etc.)."""

    action_target: str = ""
    """The target of the action."""

    risk_level: RiskLevel = RiskLevel.APPROVAL_REQUIRED
    """The risk level that triggered the approval."""

    state: ApprovalState = ApprovalState.REQUESTED
    """Current state of the request."""

    reason: str = ""
    """Why the approval was requested."""

    specialist: str = ""
    """The specialist requesting the action."""

    user_context: str = ""
    """Context to present to the approver."""

    escalation_path: EscalationPath = EscalationPath.USER
    """Available escalation path."""

    created_at: float = 0.0
    """When the request was created."""

    decided_at: Optional[float] = None
    """When the request was approved or denied."""

    decided_by: str = ""
    """Who made the decision."""

    decision_reason: str = ""
    """Why the decision was made."""

    timeout_seconds: float = 300.0
    """How long before the request auto-expires."""

    metadata: Dict[str, Any] = field(default_factory=dict)
    """Additional metadata."""

    def is_expired(self) -> bool:
        """Check if the request has timed out."""
        if self.state in (ApprovalState.APPROVED, ApprovalState.DENIED):
            return False
        return (time.time() - self.created_at) > self.timeout_seconds

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================================
# ApprovalManager
# ============================================================================


class ApprovalManager:
    """Manages approval workflows for high-risk actions.

    Features:
    - Request lifecycle management (requested → approved/denied)
    - Automatic timeout and expiration
    - Escalation paths
    - Decision audit trail
    - Async interface for UI integration
    - Configurable auto-approval policies

    Usage:
        mgr = ApprovalManager()
        request = mgr.request_approval(decision, specialist="FORGE")
        # ... user reviews ...
        mgr.approve(request.id, user="operator")
        # or
        mgr.deny(request.id, user="operator", reason="Not appropriate")
        # or
        mgr.escalate(request.id)
    """

    def __init__(
        self,
        auto_approve_for_specialists: Optional[Set[str]] = None,
        default_timeout: float = 300.0,
        max_pending: int = 50,
    ):
        """Initialize the approval manager.

        Args:
            auto_approve_for_specialists: Specialists that can auto-approve
                certain actions (e.g., SENTINEL for policy-related actions).
            default_timeout: Default timeout in seconds before auto-expiry.
            max_pending: Maximum number of pending requests.
        """
        self._requests: Dict[str, ApprovalRequest] = {}
        self._auto_approve_specialists: Set[str] = auto_approve_for_specialists or {
            "SENTINEL", "TERMINUS",
        }
        self._default_timeout = default_timeout
        self._max_pending = max_pending

        # Callbacks for UI integration
        self._on_request_callbacks: List[Callable[[ApprovalRequest], None]] = []
        self._on_decision_callbacks: List[Callable[[ApprovalRequest], None]] = []

        log.info(f"ApprovalManager initialized (timeout={default_timeout}s)")

    # ------------------------------------------------------------------
    # Request Lifecycle
    # ------------------------------------------------------------------

    def request_approval(
        self,
        decision: PolicyDecision,
        specialist: str = "",
        user_context: str = "",
        timeout_seconds: Optional[float] = None,
        escalation_path: EscalationPath = EscalationPath.USER,
    ) -> ApprovalRequest:
        """Request approval for a high-risk action.

        Args:
            decision: The policy decision that triggered this request.
            specialist: The specialist making the request.
            user_context: Contextual information for the approver.
            timeout_seconds: Custom timeout (defaults to global default).
            escalation_path: How to escalate if needed.

        Returns:
            The created ApprovalRequest.

        Raises:
            RuntimeError: If too many requests are pending.
        """
        # Check pending limit
        pending = self._count_pending()
        if pending >= self._max_pending:
            raise RuntimeError(
                f"Too many pending approval requests ({pending}/{self._max_pending}). "
                f"Resolve outstanding requests first."
            )

        # Check if the specialist can auto-approve
        if specialist in self._auto_approve_specialists and decision.risk_level != RiskLevel.BLOCKED:
            request = self._create_request(
                decision=decision,
                specialist=specialist,
                user_context=user_context,
                timeout_seconds=timeout_seconds or self._default_timeout,
                escalation_path=escalation_path,
            )
            self._auto_approve(request, specialist)
            return request

        request = self._create_request(
            decision=decision,
            specialist=specialist,
            user_context=user_context,
            timeout_seconds=timeout_seconds or self._default_timeout,
            escalation_path=escalation_path,
        )
        self._requests[request.id] = request

        # Notify callbacks
        self._notify_request(request)

        log.info(f"Approval requested: {request.id} for {decision.action_type} "
                 f"({decision.risk_level.value}) by {specialist}")
        return request

    def _create_request(
        self,
        decision: PolicyDecision,
        specialist: str,
        user_context: str,
        timeout_seconds: float,
        escalation_path: EscalationPath,
    ) -> ApprovalRequest:
        """Create an approval request from a policy decision."""
        return ApprovalRequest(
            id=f"apr_{uuid.uuid4().hex[:12]}",
            policy_decision=decision,
            action_type=decision.action_type,
            action_target=decision.action_target[:500],
            risk_level=decision.risk_level,
            state=ApprovalState.REQUESTED,
            reason=decision.reason,
            specialist=specialist,
            user_context=user_context,
            escalation_path=escalation_path,
            created_at=time.time(),
            timeout_seconds=timeout_seconds,
            metadata={
                "decision_id": decision.decision_id,
                "policy_rules": decision.policy_rules_matched,
            },
        )

    def approve(
        self,
        request_id: str,
        decided_by: str = "system",
        reason: str = "",
    ) -> Optional[ApprovalRequest]:
        """Approve a pending approval request.

        Args:
            request_id: The ID of the request to approve.
            decided_by: Who approved the request.
            reason: Why the request was approved.

        Returns:
            The updated ApprovalRequest, or None if not found.
        """
        request = self._requests.get(request_id)
        if request is None:
            log.warning(f"Approval request not found: {request_id}")
            return None

        if request.state != ApprovalState.REQUESTED:
            log.warning(f"Cannot approve request {request_id} in state {request.state.value}")
            return request

        request.state = ApprovalState.APPROVED
        request.decided_at = time.time()
        request.decided_by = decided_by
        request.decision_reason = reason or "Approved by {decided_by}"

        self._notify_decision(request)
        log.info(f"Approval GRANTED: {request_id} by {decided_by}")
        return request

    def deny(
        self,
        request_id: str,
        decided_by: str = "system",
        reason: str = "Action denied",
    ) -> Optional[ApprovalRequest]:
        """Deny a pending approval request.

        Args:
            request_id: The ID of the request to deny.
            decided_by: Who denied the request.
            reason: Why the request was denied.

        Returns:
            The updated ApprovalRequest, or None if not found.
        """
        request = self._requests.get(request_id)
        if request is None:
            log.warning(f"Approval request not found: {request_id}")
            return None

        if request.state != ApprovalState.REQUESTED:
            log.warning(f"Cannot deny request {request_id} in state {request.state.value}")
            return request

        request.state = ApprovalState.DENIED
        request.decided_at = time.time()
        request.decided_by = decided_by
        request.decision_reason = reason

        self._notify_decision(request)
        log.info(f"Approval DENIED: {request_id} by {decided_by}: {reason}")
        return request

    def escalate(
        self,
        request_id: str,
        reason: str = "",
    ) -> Optional[ApprovalRequest]:
        """Escalate a pending approval request to a higher authority.

        Args:
            request_id: The ID of the request to escalate.
            reason: Why the request is being escalated.

        Returns:
            The updated ApprovalRequest, or None if not found.
        """
        request = self._requests.get(request_id)
        if request is None:
            log.warning(f"Escalation target not found: {request_id}")
            return None

        request.state = ApprovalState.ESCALATED
        request.decision_reason = reason or "Escalated for higher authority review"

        self._notify_decision(request)
        log.info(f"Approval ESCALATED: {request_id} - {reason}")
        return request

    def _auto_approve(
        self,
        request: ApprovalRequest,
        specialist: str,
    ) -> None:
        """Automatically approve a request from a trusted specialist.

        This avoids unnecessary user interruptions for trusted entities
        performing appropriately scoped actions.
        """
        request.state = ApprovalState.APPROVED
        request.decided_at = time.time()
        request.decided_by = f"auto:{specialist}"
        request.decision_reason = f"Auto-approved for trusted specialist {specialist}"

        self._requests[request.id] = request
        self._notify_decision(request)
        log.info(f"Auto-approved {request.id} for specialist {specialist}")

    # ------------------------------------------------------------------
    # Expiry Check
    # ------------------------------------------------------------------

    def expire_stale_requests(self) -> List[ApprovalRequest]:
        """Expire all pending requests that have timed out.

        Returns:
            List of expired requests.
        """
        expired: List[ApprovalRequest] = []
        for request in list(self._requests.values()):
            if request.state == ApprovalState.REQUESTED and request.is_expired():
                request.state = ApprovalState.EXPIRED
                request.decided_at = time.time()
                request.decided_by = "system"
                request.decision_reason = "Request expired without decision"
                expired.append(request)
                self._notify_decision(request)

        if expired:
            log.info(f"Expired {len(expired)} stale approval requests")
        return expired

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def get_request(self, request_id: str) -> Optional[ApprovalRequest]:
        """Get a specific approval request by ID."""
        return self._requests.get(request_id)

    def get_pending_requests(
        self,
        specialist: Optional[str] = None,
    ) -> List[ApprovalRequest]:
        """Get all pending (requested) approval requests.

        Args:
            specialist: If provided, filter by requesting specialist.

        Returns:
            List of pending requests.
        """
        pending = [
            r for r in self._requests.values()
            if r.state == ApprovalState.REQUESTED
        ]
        if specialist:
            pending = [r for r in pending if r.specialist == specialist]
        return sorted(pending, key=lambda r: r.created_at)

    def get_recent_decisions(
        self,
        n: int = 20,
    ) -> List[ApprovalRequest]:
        """Get the n most recent approval decisions."""
        decided = [
            r for r in self._requests.values()
            if r.state in (ApprovalState.APPROVED, ApprovalState.DENIED)
        ]
        decided.sort(key=lambda r: r.decided_at or r.created_at, reverse=True)
        return decided[:n]

    def get_stats(self) -> Dict[str, Any]:
        """Get approval manager statistics."""
        pending = self._count_pending()
        approved = sum(1 for r in self._requests.values() if r.state == ApprovalState.APPROVED)
        denied = sum(1 for r in self._requests.values() if r.state == ApprovalState.DENIED)
        expired = sum(1 for r in self._requests.values() if r.state == ApprovalState.EXPIRED)
        escalated = sum(1 for r in self._requests.values() if r.state == ApprovalState.ESCALATED)

        # By specialist
        by_specialist: Dict[str, int] = {}
        for r in self._requests.values():
            by_specialist[r.specialist] = by_specialist.get(r.specialist, 0) + 1

        return {
            "total_requests": len(self._requests),
            "pending": pending,
            "approved": approved,
            "denied": denied,
            "expired": expired,
            "escalated": escalated,
            "compliance_rate": round(approved / max(1, approved + denied), 4),
            "by_specialist": by_specialist,
        }

    # ------------------------------------------------------------------
    # Callbacks (UI Integration)
    # ------------------------------------------------------------------

    def on_request(self, callback: Callable[[ApprovalRequest], None]) -> None:
        """Register a callback for new approval requests.

        Args:
            callback: Called with the ApprovalRequest when a new request is created.
        """
        self._on_request_callbacks.append(callback)

    def on_decision(self, callback: Callable[[ApprovalRequest], None]) -> None:
        """Register a callback for approval decisions.

        Args:
            callback: Called with the (updated) ApprovalRequest when a decision is made.
        """
        self._on_decision_callbacks.append(callback)

    def _notify_request(self, request: ApprovalRequest) -> None:
        """Notify all request callbacks."""
        for cb in self._on_request_callbacks:
            try:
                cb(request)
            except Exception as e:
                log.error(f"Approval request callback error: {e}")

    def _notify_decision(self, request: ApprovalRequest) -> None:
        """Notify all decision callbacks."""
        for cb in self._on_decision_callbacks:
            try:
                cb(request)
            except Exception as e:
                log.error(f"Approval decision callback error: {e}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _count_pending(self) -> int:
        """Count pending (requested) requests."""
        return sum(1 for r in self._requests.values() if r.state == ApprovalState.REQUESTED)

    def clear_expired(self) -> int:
        """Clear expired requests from the store.

        Returns:
            Number of requests removed.
        """
        expired = self.expire_stale_requests()
        for req in expired:
            self._requests.pop(req.id, None)
        return len(expired)

    def clear_decisions(self) -> int:
        """Clear all decision records (keep pending).

        Returns:
            Number of requests removed.
        """
        to_remove = [
            rid for rid, r in self._requests.items()
            if r.state in (ApprovalState.APPROVED, ApprovalState.DENIED, ApprovalState.EXPIRED, ApprovalState.ESCALATED)
        ]
        for rid in to_remove:
            self._requests.pop(rid, None)
        return len(to_remove)

    def has_pending(self) -> bool:
        """Check if there are any pending approval requests."""
        return self._count_pending() > 0
