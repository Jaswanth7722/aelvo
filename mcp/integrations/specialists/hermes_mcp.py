"""HERMES MCP specialist integration contract."""

from __future__ import annotations

import logging
from typing import Any, Dict
from ...execution.execution_engine import MCPExecutionEngine
from ...execution.execution_request import MCPExecutionRequest
from ...execution.execution_result import MCPExecutionResult
from ...registry.models import TrustLevel
from ...registry.server_registry import ServerRegistry
from ...events.event_schemas import ExecutionPriority

log = logging.getLogger("aelvo.mcp.specialist.hermes")


class HermesMCPInterface:
    """Specialist integration contract for HERMES.

    Permitted MCP usage:
    - Query user context servers
    - Fetch profile data from identity systems

    Restrictions:
    - No write operations unless trust_level >= TRUSTED
    - Max 2 MCP calls per conversation turn
    """

    def __init__(self, execution_engine: MCPExecutionEngine, registry: ServerRegistry):
        self._execution_engine = execution_engine
        self._registry = registry
        self._calls_this_turn = 0

    def reset_turn(self) -> None:
        """Reset the conversation turn call counter."""
        self._calls_this_turn = 0

    async def execute_user_context_query(
        self,
        server_id: str,
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> MCPExecutionResult:
        """Execute an MCP query for user context or identity data."""
        # 1. Enforce turn budget
        if self._calls_this_turn >= 2:
            return MCPExecutionResult(
                request_id="hermes_blocked",
                specialist_id="HERMES",
                server_id=server_id,
                tool_name=tool_name,
                success=False,
                error="HERMES execution blocked: turn limit of 2 calls exceeded",
            )

        # 2. Check for write operations and trust restrictions
        is_write = "write" in tool_name or "edit" in tool_name or "create" in tool_name or "delete" in tool_name
        if is_write:
            record = self._registry.get(server_id)
            if not record or record.trust_level != TrustLevel.TRUSTED:
                return MCPExecutionResult(
                    request_id="hermes_blocked",
                    specialist_id="HERMES",
                    server_id=server_id,
                    tool_name=tool_name,
                    success=False,
                    error=f"HERMES execution blocked: write operation '{tool_name}' requires TRUSTED server status",
                )

        self._calls_this_turn += 1

        request = MCPExecutionRequest(
            request_id=f"hermes_req_{self._calls_this_turn}",
            specialist_id="HERMES",
            server_id=server_id,
            tool_name=tool_name,
            arguments=arguments,
            trust_requirement=TrustLevel.TRUSTED if is_write else TrustLevel.SANDBOXED,
            priority=ExecutionPriority.NORMAL,
        )

        return await self._execution_engine.execute(request)
