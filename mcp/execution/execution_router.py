"""ExecutionRouter — routes MCP execution requests to appropriate servers with failover support."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from ..registry.models import MCPServerRecord, HealthState, TrustLevel
from ..registry.server_registry import ServerRegistry
from ..capability.capability_engine import CapabilityEngine
from ..memory.routing_intelligence import RoutingIntelligence
from .execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.execution.router")


class ExecutionRouter:
    """Routes MCP execution requests to appropriate servers.

    Uses memory-informed routing when multiple servers can fulfill a request,
    with capability verification and failover support.
    """

    def __init__(
        self,
        registry: ServerRegistry,
        capability_engine: CapabilityEngine,
        routing_intelligence: Optional[RoutingIntelligence] = None,
    ):
        self._registry = registry
        self._capability_engine = capability_engine
        self._routing_intelligence = routing_intelligence

    async def route(self, request: MCPExecutionRequest) -> Tuple[Optional[str], Optional[str]]:
        """Route a request to the best server.

        Returns:
            (server_id, error_message) — server_id is None if routing fails.
        """
        # 1. If server is explicitly specified, verify it
        if request.server_id:
            record = self._registry.get(request.server_id)
            if not record:
                return None, f"Server '{request.server_id}' not found in registry"
            if not record.enabled:
                return None, f"Server '{request.server_id}' is disabled"
            if record.trust_level == TrustLevel.BLOCKED:
                return None, f"Server '{request.server_id}' is BLOCKED"
            return record.id, None

        # 2. Find servers that provide the requested tool
        candidates = await self._capability_engine.find_tool(request.tool_name)
        if not candidates:
            return None, f"No server provides tool '{request.tool_name}'"

        # 3. Filter by trust requirement
        eligible = [
            (rec, tool) for rec, tool in candidates
            if self._trust_meets_requirement(rec.trust_level, request.trust_requirement)
            and rec.enabled
            and rec.health_state in (HealthState.HEALTHY, HealthState.DEGRADED)
        ]

        if not eligible:
            return None, f"No eligible server for tool '{request.tool_name}' meeting trust requirement {request.trust_requirement.value}"

        if len(eligible) == 1:
            return eligible[0][0].id, None

        # 4. Multiple candidates — use routing intelligence if available
        if self._routing_intelligence:
            best = await self._routing_intelligence.select_server(
                candidate_servers=[rec for rec, _ in eligible],
                requesting_specialist=request.specialist_id,
                tool_type=request.tool_name,
            )
            if best:
                return best.id, None

        # 5. Default: return the first eligible (most trusted)
        eligible.sort(key=lambda x: self._trust_order(x[0].trust_level), reverse=True)
        return eligible[0][0].id, None

    @staticmethod
    def _trust_meets_requirement(server_trust: TrustLevel, requirement: TrustLevel) -> bool:
        order = ["blocked", "quarantined", "sandboxed", "verified", "trusted"]
        return order.index(server_trust.value) >= order.index(requirement.value)

    @staticmethod
    def _trust_order(level: TrustLevel) -> int:
        order = {"blocked": 0, "quarantined": 1, "sandboxed": 2, "verified": 3, "trusted": 4}
        return order.get(level.value, 0)
