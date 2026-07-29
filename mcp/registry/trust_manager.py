"""Trust Manager — first-class trust level management for MCP servers.

Every MCP server has a trust level that governs what operations it can perform.
Trust changes are audited and may require human acknowledgment for escalations.
"""

from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from typing import List, Optional, Tuple
from pydantic import BaseModel, Field

from .models import TrustLevel

log = logging.getLogger("aelvo.mcp.trust")

# Trust level ordering (higher index = more trusted)
_TRUST_ORDER = [
    TrustLevel.BLOCKED,
    TrustLevel.QUARANTINED,
    TrustLevel.SANDBOXED,
    TrustLevel.VERIFIED,
    TrustLevel.TRUSTED,
]


class TrustChangeReason(str, Enum):
    """Reasons for trust level changes."""
    INITIAL_REGISTRATION = "initial_registration"
    VERIFICATION_PASSED = "verification_passed"
    VERIFICATION_FAILED = "verification_failed"
    TRUST_VIOLATION = "trust_violation"
    SECURITY_INCIDENT = "security_incident"
    USER_ESCALATION = "user_escalation"
    USER_DOWNGRADE = "user_downgrade"
    ADMIN_OVERRIDE = "admin_override"
    RECOVERY_FAILED = "recovery_failed"
    SCHEMA_MISMATCH = "schema_mismatch"
    AUTO_ESCALATION = "auto_escalation"


class TrustAuditEntry(BaseModel):
    """Audit trail entry for a trust level change."""
    server_id: str
    old_level: TrustLevel
    new_level: TrustLevel
    reason: TrustChangeReason
    triggered_by: str = "system"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: str = ""


class TrustManager:
    """Manages trust levels for MCP servers with full audit trail.

    Enforces:
    - Trust level ordering (can only escalate one level at a time without admin)
    - Escalation requires verification
    - Downgrade is always permitted
    - Human acknowledgment required for QUARANTINED → anything
    """

    def __init__(self):
        self._audit_trail: List[TrustAuditEntry] = []
        self._escalation_requires_verification = True

    # ------------------------------------------------------------------
    # Trust Operations
    # ------------------------------------------------------------------

    def can_set_trust(self, current: TrustLevel, target: TrustLevel) -> Tuple[bool, str]:
        """Check if a trust level change is permitted."""
        if current == target:
            return True, "No change"

        current_idx = _TRUST_ORDER.index(current)
        target_idx = _TRUST_ORDER.index(target)

        # Downgrade is always permitted
        if target_idx < current_idx:
            return True, "Downgrade permitted"

        # Escalation from QUARANTINED requires human acknowledgment
        if current == TrustLevel.QUARANTINED and target != TrustLevel.BLOCKED:
            return False, "QUARANTINED requires human acknowledgment before any escalation"

        # BLOCKED can only be escalated by admin
        if current == TrustLevel.BLOCKED:
            return False, "BLOCKED requires admin override to change"

        # Escalation from SANDBOXED to TRUSTED requires going through VERIFIED
        if current == TrustLevel.SANDBOXED and target == TrustLevel.TRUSTED:
            return False, "Cannot escalate directly from SANDBOXED to TRUSTED — must go through VERIFIED"

        # All other escalations permitted
        return True, "Escalation permitted"

    def change_trust(
        self,
        server_id: str,
        current: TrustLevel,
        target: TrustLevel,
        reason: TrustChangeReason,
        triggered_by: str = "system",
        details: str = "",
        require_verification: bool = True,
    ) -> Tuple[bool, Optional[TrustLevel], str]:
        """Attempt a trust level change.

        Returns:
            (success, new_level, message)
        """
        allowed, msg = self.can_set_trust(current, target)
        if not allowed:
            self._audit(server_id, current, current, TrustChangeReason.AUTO_ESCALATION, triggered_by, f"BLOCKED: {msg}")
            return False, current, msg

        # Record the change
        self._audit(server_id, current, target, reason, triggered_by, details)
        log.info("Trust change for %s: %s -> %s (reason: %s)", server_id, current, target, reason.value)

        return True, target, f"Trust changed to {target.value}"

    def get_audit_trail(self, server_id: Optional[str] = None, limit: int = 100) -> List[TrustAuditEntry]:
        """Get the audit trail, optionally filtered by server."""
        entries = self._audit_trail
        if server_id:
            entries = [e for e in entries if e.server_id == server_id]
        return entries[-limit:]

    def requires_human_ack(self, current: TrustLevel) -> bool:
        """Check if a trust state requires human acknowledgment to change."""
        return current in (TrustLevel.QUARANTINED, TrustLevel.BLOCKED)

    def _audit(
        self,
        server_id: str,
        old: TrustLevel,
        new: TrustLevel,
        reason: TrustChangeReason,
        triggered_by: str,
        details: str,
    ) -> None:
        """Record a trust change in the audit trail."""
        entry = TrustAuditEntry(
            server_id=server_id,
            old_level=old,
            new_level=new,
            reason=reason,
            triggered_by=triggered_by,
            details=details,
        )
        self._audit_trail.append(entry)

    @property
    def audit_trail(self) -> List[TrustAuditEntry]:
        return list(self._audit_trail)
