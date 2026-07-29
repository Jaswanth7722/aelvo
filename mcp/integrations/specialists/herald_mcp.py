"""HERALD MCP specialist integration contract."""

from __future__ import annotations

import logging
from typing import Any, Dict, List
from ...governance.governance_layer import MCPGovernanceLayer
from ...memory.mcp_memory_store import MCPMemoryStore
from ...capability.capability_engine import CapabilityEngine

log = logging.getLogger("aelvo.mcp.specialist.herald")


class HeraldMCPInterface:
    """Specialist integration contract for HERALD.

    Permitted MCP usage:
    - Read-only access to MCP audit logs and metrics

    Key behavior:
    HERALD generates capability reports and MCP activity summaries for human operators.
    Reports include server reliability trends, capability gaps, and trust state changes.
    """

    def __init__(
        self,
        governance_layer: MCPGovernanceLayer,
        memory_store: MCPMemoryStore,
        capability_engine: CapabilityEngine,
    ):
        self._governance_layer = governance_layer
        self._memory_store = memory_store
        self._capability_engine = capability_engine

    async def get_audit_trail_summary(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Retrieve recent governance audit trail records."""
        records = self._governance_layer._audit_logger.get_records(limit=limit)
        return [
            {
                "id": r.id,
                "timestamp": r.timestamp.isoformat(),
                "decision": r.decision,
                "specialist_id": r.specialist_id,
                "server_id": r.server_id,
                "tool_name": r.tool_name,
                "reason": r.reason,
            }
            for r in records
        ]

    async def generate_mcp_report(self) -> str:
        """Generate a capability and activity report for human operators."""
        gaps = await self._capability_engine.get_capability_gaps()
        stats = self._governance_layer._audit_logger.get_stats()

        lines = [
            "============================================",
            "        HERALD MCP PLATFORM ACTIVITY REPORT",
            "============================================",
            f"Total Governance Decisions Logged: {stats.get('total_records', 0)}",
            "Decisions Breakdown:",
        ]

        for dec, count in stats.get("by_decision", {}).items():
            lines.append(f"  - {dec}: {count}")

        lines.append("\nCapability Gaps Identified:")
        if not gaps:
            lines.append("  - None (All required capabilities mapped)")
        else:
            for gap in gaps:
                lines.append(f"  - Task type: {gap.get('task_type')}, Tool: {gap.get('tool_name')}")

        lines.append("============================================")
        return "\n".join(lines)
