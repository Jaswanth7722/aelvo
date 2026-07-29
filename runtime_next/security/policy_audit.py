"""Policy Audit Trail — structured audit logging for all governance and
policy decisions within RuntimeNext.

Every policy evaluation, approval request, and governance decision is recorded
as an AuditRecord with a tamper-evident hash chain, enabling:
- Audit trail analysis of past decisions
- Compliance reporting
- Incident investigation
- Trend analysis of policy effectiveness

Audit records are immutable once created and linked via a hash chain
for integrity verification.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set, Tuple

log = logging.getLogger("aelvo.runtime.security.policy_audit")


class AuditAction(str, Enum):
    """Types of actions that can be audited."""
    POLICY_EVALUATION = "policy_evaluation"
    APPROVAL_REQUESTED = "approval_requested"
    APPROVAL_GRANTED = "approval_granted"
    APPROVAL_REJECTED = "approval_rejected"
    GOVERNANCE_DECISION = "governance_decision"
    RECOVERY_ACTION = "recovery_action"
    SECURITY_SCAN = "security_scan"
    SECURITY_FINDING = "security_finding"
    INTEGRITY_CHECK = "integrity_check"
    CONFIGURATION_CHANGE = "configuration_change"
    POLICY_CHANGE = "policy_change"
    SYSTEM_EVENT = "system_event"


class AuditDecision(str, Enum):
    """Outcome of an audited decision."""
    ALLOWED = "allowed"
    DENIED = "denied"
    APPROVAL_PENDING = "approval_pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    LOG_ONLY = "log_only"
    ESCALATED = "escalated"
    BLOCKED = "blocked"


@dataclass
class AuditRecord:
    """A single immutable audit record with hash-chain integrity.

    Each record contains the hash of the previous record, forming a
    tamper-evident chain. Records are never modified after creation.
    """

    record_id: str
    timestamp: float = field(default_factory=time.time)
    action: AuditAction = AuditAction.SYSTEM_EVENT
    decision: AuditDecision = AuditDecision.ALLOWED
    actor: str = ""
    """Who or what performed the action (e.g., 'policy_engine', 'user', 'recovery_engine')."""
    subsystem: str = ""
    """Which subsystem the action belongs to (e.g., 'governance', 'recovery', 'security')."""
    resource: str = ""
    """The resource being acted upon (e.g., policy_id, task_id, node_id)."""
    reason: str = ""
    message: str = ""
    severity: str = "info"
    metadata: Dict[str, Any] = field(default_factory=dict)
    previous_hash: str = ""
    """SHA-256 hash of the previous record in the chain."""
    record_hash: str = ""
    """SHA-256 hash of this record's content (self-verifying)."""

    def compute_hash(self) -> str:
        """Compute the cryptographic hash of this record's content."""
        content = {
            "record_id": self.record_id,
            "timestamp": self.timestamp,
            "action": self.action.value if isinstance(self.action, Enum) else self.action,
            "decision": self.decision.value if isinstance(self.decision, Enum) else self.decision,
            "actor": self.actor,
            "subsystem": self.subsystem,
            "resource": self.resource,
            "reason": self.reason,
            "message": self.message,
            "severity": self.severity,
            "metadata": self.metadata,
            "previous_hash": self.previous_hash,
        }
        raw = json.dumps(content, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def verify(self, previous_hash: str) -> bool:
        """Verify this record's integrity against the previous hash."""
        if self.record_hash and self.record_hash != self.compute_hash():
            return False
        if self.previous_hash and self.previous_hash != previous_hash:
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "timestamp": self.timestamp,
            "action": self.action.value if isinstance(self.action, Enum) else self.action,
            "decision": self.decision.value if isinstance(self.decision, Enum) else self.decision,
            "actor": self.actor,
            "subsystem": self.subsystem,
            "resource": self.resource,
            "reason": self.reason,
            "message": self.message,
            "severity": self.severity,
            "metadata": self.metadata,
            "previous_hash": self.previous_hash,
            "record_hash": self.record_hash,
        }


@dataclass
class AuditQuery:
    """Query parameters for filtering audit records."""
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    actions: Optional[List[AuditAction]] = None
    decisions: Optional[List[AuditDecision]] = None
    actor: Optional[str] = None
    subsystem: Optional[str] = None
    resource: Optional[str] = None
    severity: Optional[str] = None
    limit: int = 100
    offset: int = 0


class PolicyAuditTrail:
    """Structured audit trail for policy and governance decisions.

    Features:
    - Immutable audit records with hash-chain integrity
    - Queryable by time range, action type, decision, actor, subsystem
    - Automatic chain linking (each record hashes the previous)
    - Integrity verification (detect tampering)
    - Integration with governance hooks and policy engine

    Usage:
        audit = PolicyAuditTrail()
        audit.record(
            action=AuditAction.POLICY_EVALUATION,
            decision=AuditDecision.DENIED,
            actor="policy_engine",
            subsystem="governance",
            resource="policy:gov_deny_destructive_consensus",
            reason="Destructive consensus action blocked by policy",
        )
        assert audit.verify_chain_integrity()
    """

    def __init__(self, max_records: int = 10000):
        self._records: List[AuditRecord] = []
        self._max_records = max_records
        self._last_hash: str = ""

    # ── Recording ────────────────────────────────────────────────────────

    def record(
        self,
        action: AuditAction,
        decision: AuditDecision,
        actor: str = "",
        subsystem: str = "",
        resource: str = "",
        reason: str = "",
        message: str = "",
        severity: str = "info",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AuditRecord:
        """Record an auditable event.

        Automatically links the record into the hash chain.

        Args:
            action: The type of action performed.
            decision: The decision outcome.
            actor: Who/what performed the action.
            subsystem: Which subsystem.
            resource: The resource acted upon.
            reason: Why the decision was made.
            message: Human-readable description.
            severity: Severity level (info, warning, error, critical).
            metadata: Additional structured data.

        Returns:
            The created AuditRecord.
        """
        record_id = self._generate_id("audit")
        record = AuditRecord(
            record_id=record_id,
            timestamp=time.time(),
            action=action,
            decision=decision,
            actor=actor,
            subsystem=subsystem,
            resource=resource,
            reason=reason,
            message=message,
            severity=severity,
            metadata=metadata or {},
            previous_hash=self._last_hash,
        )
        record.record_hash = record.compute_hash()
        self._last_hash = record.record_hash
        self._records.append(record)

        # Trim history
        if len(self._records) > self._max_records:
            self._records = self._records[-self._max_records:]

        log.debug(
            "Audit record: action=%s decision=%s subsystem=%s resource=%s",
            action.value if isinstance(action, Enum) else action,
            decision.value if isinstance(decision, Enum) else decision,
            subsystem, resource,
        )
        return record

    # ── Convenience Methods ──────────────────────────────────────────────

    def record_policy_evaluation(
        self,
        policy_id: str,
        policy_name: str,
        scope: str,
        decision: AuditDecision,
        reason: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AuditRecord:
        """Record a policy evaluation result."""
        return self.record(
            action=AuditAction.POLICY_EVALUATION,
            decision=decision,
            actor="policy_engine",
            subsystem="governance",
            resource=f"policy:{policy_id}",
            reason=reason,
            message=f"Policy '{policy_name}' evaluated on scope={scope}: {decision.value}",
            severity="warning" if decision in (AuditDecision.DENIED, AuditDecision.BLOCKED) else "info",
            metadata={
                "policy_id": policy_id,
                "policy_name": policy_name,
                "scope": scope,
                **(metadata or {}),
            },
        )

    def record_approval(
        self,
        request_id: str,
        decision: AuditDecision,
        actor: str,
        reason: str = "",
    ) -> AuditRecord:
        """Record an approval decision."""
        return self.record(
            action=AuditAction.APPROVAL_GRANTED if decision == AuditDecision.APPROVED
            else AuditAction.APPROVAL_REJECTED,
            decision=decision,
            actor=actor,
            subsystem="governance",
            resource=f"approval:{request_id}",
            reason=reason,
            message=f"Approval request {request_id}: {decision.value} by {actor}",
            severity="info",
            metadata={"request_id": request_id},
        )

    def record_security_finding(
        self,
        finding_id: str,
        severity: str,
        category: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AuditRecord:
        """Record a security scanner finding."""
        return self.record(
            action=AuditAction.SECURITY_FINDING,
            decision=AuditDecision.BLOCKED if severity in ("critical", "high") else AuditDecision.ALLOWED,
            actor="security_scanner",
            subsystem="security",
            resource=f"finding:{finding_id}",
            reason=f"Security finding: {category} ({severity})",
            message=message[:200],
            severity=severity,
            metadata={
                "finding_id": finding_id,
                "category": category,
                **(metadata or {}),
            },
        )

    def record_integrity_check(
        self,
        check_id: str,
        passed: bool,
        details: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AuditRecord:
        """Record an integrity check result."""
        return self.record(
            action=AuditAction.INTEGRITY_CHECK,
            decision=AuditDecision.ALLOWED if passed else AuditDecision.BLOCKED,
            actor="integrity_verifier",
            subsystem="security",
            resource=f"integrity:{check_id}",
            reason=f"Integrity check {'passed' if passed else 'failed'}: {details}",
            message=details,
            severity="info" if passed else "critical",
            metadata={"check_id": check_id, "passed": passed, **(metadata or {})},
        )

    # ── Querying ─────────────────────────────────────────────────────────

    def query(self, query: AuditQuery) -> List[AuditRecord]:
        """Query audit records with filtering.

        Args:
            query: AuditQuery with filter parameters.

        Returns:
            List of matching AuditRecords, newest first.
        """
        results = list(self._records)

        if query.start_time is not None:
            results = [r for r in results if r.timestamp >= query.start_time]
        if query.end_time is not None:
            results = [r for r in results if r.timestamp <= query.end_time]
        if query.actions:
            results = [r for r in results if r.action in query.actions]
        if query.decisions:
            results = [r for r in results if r.decision in query.decisions]
        if query.actor:
            results = [r for r in results if query.actor.lower() in r.actor.lower()]
        if query.subsystem:
            results = [r for r in results if r.subsystem == query.subsystem]
        if query.resource:
            results = [r for r in results if query.resource.lower() in r.resource.lower()]
        if query.severity:
            results = [r for r in results if r.severity == query.severity]

        # Sort newest first
        results.sort(key=lambda r: r.timestamp, reverse=True)
        return results[query.offset:query.offset + query.limit]

    def get_records_by_subsystem(
        self, subsystem: str, limit: int = 50,
    ) -> List[AuditRecord]:
        """Get audit records for a specific subsystem."""
        return [
            r for r in reversed(self._records)
            if r.subsystem == subsystem
        ][:limit]

    def get_records_by_actor(
        self, actor: str, limit: int = 50,
    ) -> List[AuditRecord]:
        """Get audit records for a specific actor."""
        return [
            r for r in reversed(self._records)
            if actor.lower() in r.actor.lower()
        ][:limit]

    def get_recent(self, limit: int = 50) -> List[AuditRecord]:
        """Get the most recent audit records."""
        return list(reversed(self._records[-limit:]))

    def get_by_resource(self, resource: str) -> List[AuditRecord]:
        """Get all audit records for a specific resource."""
        return [r for r in self._records if r.resource == resource]

    # ── Integrity Verification ───────────────────────────────────────────

    def verify_chain_integrity(self) -> bool:
        """Verify the integrity of the entire audit trail.

        Checks that:
        1. Every record's hash matches its content
        2. Every record references the correct previous hash

        Returns:
            True if the entire chain is valid, False if tampering detected.
        """
        previous_hash = ""
        for record in self._records:
            if not record.verify(previous_hash):
                log.error(
                    "Audit chain integrity violation at record %s",
                    record.record_id[:8],
                )
                return False
            previous_hash = record.record_hash
        return True

    def get_chain_status(self) -> Dict[str, Any]:
        """Get the integrity status of the audit chain."""
        chain_valid = self.verify_chain_integrity()
        return {
            "chain_valid": chain_valid,
            "total_records": len(self._records),
            "first_record_id": self._records[0].record_id[:16] if self._records else None,
            "last_record_id": self._records[-1].record_id[:16] if self._records else None,
            "last_hash": self._last_hash[:16] if self._last_hash else None,
            "max_records": self._max_records,
        }

    # ── Statistics ───────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Get audit trail statistics."""
        return {
            "total_records": len(self._records),
            "by_action": {
                action.value: sum(1 for r in self._records if r.action == action)
                for action in AuditAction
            },
            "by_decision": {
                decision.value: sum(1 for r in self._records if r.decision == decision)
                for decision in AuditDecision
            },
            "by_subsystem": {
                sub: sum(1 for r in self._records if r.subsystem == sub)
                for sub in set(r.subsystem for r in self._records)
            },
            "by_severity": {
                sev: sum(1 for r in self._records if r.severity == sev)
                for sev in set(r.severity for r in self._records)
            },
            "chain_valid": self.verify_chain_integrity(),
            "oldest_record": min(r.timestamp for r in self._records) if self._records else 0,
            "newest_record": max(r.timestamp for r in self._records) if self._records else 0,
            "time_span_hours": (
                (max(r.timestamp for r in self._records) - min(r.timestamp for r in self._records)) / 3600
                if len(self._records) > 1 else 0
            ),
        }

    def reset(self) -> None:
        """Clear all audit records and reset the chain."""
        self._records.clear()
        self._last_hash = ""

    # ── Governance Integration ───────────────────────────────────────────

    def wrap_governance_hooks(self, hooks) -> None:
        """Attach audit recording to governance hooks.

        Monkey-patches the hooks' pre_* methods to auto-record audit entries
        for every governance evaluation. This method is idempotent — calling
        it multiple times will not double-wrap the hooks.

        Args:
            hooks: A RecoveryGovernanceHooks instance.
        """
        # Idempotency guard: avoid double-wrapping
        if getattr(hooks, '_audit_wrapped', False):
            return
        hooks._audit_wrapped = True

        audit = self

        original_pre_consensus = hooks.pre_consensus_recovery
        original_pre_specialist = hooks.pre_specialist_recovery
        original_pre_task = hooks.pre_task_recovery

        def _wrapped_pre_consensus(consensus_id, action_type, consensus_type, context=None):
            outcome = original_pre_consensus(consensus_id, action_type, consensus_type, context)
            audit.record(
                action=AuditAction.GOVERNANCE_DECISION,
                decision=AuditDecision(outcome.result.value),
                actor="governance_hooks",
                subsystem="governance",
                resource=f"consensus:{consensus_id}",
                reason=outcome.reason,
                message=f"Consensus recovery {action_type}: {outcome.result.value}",
                severity="critical" if outcome.result.value == "denied" else "info",
                metadata={
                    "consensus_id": consensus_id,
                    "action_type": action_type,
                    "consensus_type": consensus_type,
                    "duration_ms": outcome.duration_ms,
                },
            )
            return outcome

        def _wrapped_pre_specialist(task_id, action_type, specialist, context=None):
            outcome = original_pre_specialist(task_id, action_type, specialist, context)
            audit.record(
                action=AuditAction.GOVERNANCE_DECISION,
                decision=AuditDecision(outcome.result.value),
                actor="governance_hooks",
                subsystem="governance",
                resource=f"specialist:{task_id}",
                reason=outcome.reason,
                message=f"Specialist recovery {action_type} for {specialist}: {outcome.result.value}",
                severity="critical" if outcome.result.value == "denied" else "info",
                metadata={
                    "task_id": task_id,
                    "action_type": action_type,
                    "specialist": specialist,
                    "duration_ms": outcome.duration_ms,
                },
            )
            return outcome

        def _wrapped_pre_task(task_id, action_type, task_trigger, context=None):
            outcome = original_pre_task(task_id, action_type, task_trigger, context)
            audit.record(
                action=AuditAction.GOVERNANCE_DECISION,
                decision=AuditDecision(outcome.result.value),
                actor="governance_hooks",
                subsystem="governance",
                resource=f"task:{task_id}",
                reason=outcome.reason,
                message=f"Task recovery {action_type} ({task_trigger}): {outcome.result.value}",
                severity="critical" if outcome.result.value == "denied" else "info",
                metadata={
                    "task_id": task_id,
                    "action_type": action_type,
                    "task_trigger": task_trigger,
                    "duration_ms": outcome.duration_ms,
                },
            )
            return outcome

        hooks.pre_consensus_recovery = _wrapped_pre_consensus  # type: ignore
        hooks.pre_specialist_recovery = _wrapped_pre_specialist  # type: ignore
        hooks.pre_task_recovery = _wrapped_pre_task  # type: ignore

        log.info("Policy audit trail wrapped governance hooks for automatic recording")

    # ── Internal ─────────────────────────────────────────────────────────

    @staticmethod
    def _generate_id(prefix: str) -> str:
        raw = f"{prefix}_{time.time()}_{id(object())}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
