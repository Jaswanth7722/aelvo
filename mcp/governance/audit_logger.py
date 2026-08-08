"""MCPAuditLogger — governance audit trail for all MCP execution decisions."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.governance.audit")


class AuditRecord(BaseModel):
    """A single governance audit record."""
    id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    decision: str  # ALLOWED | DENIED | APPROVED | BLOCKED
    specialist_id: str
    server_id: str
    tool_name: str
    request_id: str
    reason: str = ""
    details: Dict[str, Any] = Field(default_factory=dict)


class MCPAuditLogger:
    """Logs all governance decisions to an audit trail.

    Every allow, deny, approve, and block decision is recorded
    with full context for compliance and debugging.
    """

    def __init__(self, max_records: int = 10000):
        self._records: List[AuditRecord] = []
        self._max_records = max_records

    async def log(self, decision: str, request: MCPExecutionRequest,
                  result: Any = None, details: Optional[Dict] = None) -> str:
        """Log a governance decision.

        Args:
            decision: The decision (ALLOWED, DENIED, APPROVED, BLOCKED).
            request: The execution request.
            result: Optional governance result.
            details: Optional additional details.

        Returns:
            The audit record ID.
        """
        record_id = f"mcp_audit_{request.request_id}_{int(time.time())}"

        record = AuditRecord(
            id=record_id,
            decision=decision,
            specialist_id=request.specialist_id,
            server_id=request.server_id,
            tool_name=request.tool_name,
            request_id=request.request_id,
            reason=getattr(result, 'reason', '') if result else '',
            details={
                "priority": request.priority.value if hasattr(request.priority, 'value') else str(request.priority),
                "trust_requirement": request.trust_requirement.value if hasattr(request.trust_requirement, 'value') else str(request.trust_requirement),
                "timeout_ms": request.timeout_ms,
                "result": result.to_dict() if hasattr(result, 'to_dict') else str(result),
                **(details or {}),
            },
        )

        self._records.append(record)

        # Enforce max records
        if len(self._records) > self._max_records:
            self._records = self._records[-self._max_records:]

        log.info(
            "[MCP AUDIT] %s: %s/%s/%s (%s)",
            decision, request.specialist_id, request.server_id, request.tool_name,
            getattr(result, 'reason', '') if result else '',
        )

        return record_id

    def get_records(self, limit: int = 100,
                    specialist_id: Optional[str] = None,
                    decision: Optional[str] = None) -> List[AuditRecord]:
        """Get audit records with optional filtering."""
        records = list(self._records)
        if specialist_id:
            records = [r for r in records if r.specialist_id == specialist_id]
        if decision:
            records = [r for r in records if r.decision == decision]
        return records[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get audit statistics."""
        total = len(self._records)
        by_decision = {}
        by_specialist = {}
        for r in self._records:
            by_decision[r.decision] = by_decision.get(r.decision, 0) + 1
            by_specialist[r.specialist_id] = by_specialist.get(r.specialist_id, 0) + 1
        return {
            "total_records": total,
            "by_decision": by_decision,
            "by_specialist": by_specialist,
        }
