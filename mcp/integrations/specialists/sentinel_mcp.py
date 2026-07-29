"""SENTINEL MCP specialist integration contract."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from ...registry.server_registry import ServerRegistry
from ...governance.governance_layer import MCPGovernanceLayer
from ...registry.models import TrustLevel

log = logging.getLogger("aelvo.mcp.specialist.sentinel")


class SentinelMCPInterface:
    """Specialist integration contract for SENTINEL.

    Permitted MCP usage:
    - Trust inspection across all servers
    - Permission boundary queries

    Key behavior:
    SENTINEL does not execute MCP tools on behalf of tasks — it inspects and enforces.
    SENTINEL has read-only access to all MCP metadata.
    """

    def __init__(self, registry: ServerRegistry, governance_layer: MCPGovernanceLayer):
        self._registry = registry
        self._governance_layer = governance_layer

    def inspect_server_trust(self, server_id: str) -> Optional[TrustLevel]:
        """Inspect the trust level of a given server."""
        record = self._registry.get(server_id)
        return record.trust_level if record else None

    def query_permissions(self, specialist_id: str) -> List[Dict[str, Any]]:
        """Query permission boundaries for a specific specialist."""
        return [
            p for p in self._governance_layer._permission_model.list_permissions()
            if p.get("specialist_id") == specialist_id
        ]

    def check_allowlist(self, server_id: str) -> bool:
        """Check if a server is on the governance allowlist."""
        return self._governance_layer._allowlist.is_allowed(server_id)
