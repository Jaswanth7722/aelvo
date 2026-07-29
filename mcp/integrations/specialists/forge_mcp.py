"""FORGE MCP specialist integration contract."""

from __future__ import annotations

import logging
from typing import Any, Dict
from ...execution.execution_engine import MCPExecutionEngine
from ...execution.execution_request import MCPExecutionRequest
from ...execution.execution_result import MCPExecutionResult
from ...registry.models import TrustLevel
from ...events.event_schemas import ExecutionPriority

log = logging.getLogger("aelvo.mcp.specialist.forge")


class ForgeMCPInterface:
    """Specialist integration contract for FORGE.

    Permitted MCP usage:
    - Repository tools (read + write with governance approval)
    - Code generation tools
    - Build/test execution tools

    Key behavior:
    FORGE must declare intent before executing any MCP tool with side effects.
    SENTINEL reviews all write-capable tool calls.
    """

    def __init__(self, execution_engine: MCPExecutionEngine):
        self._execution_engine = execution_engine

    async def execute_forge_action(
        self,
        server_id: str,
        tool_name: str,
        arguments: Dict[str, Any],
        intent_description: str = "",
    ) -> MCPExecutionResult:
        """Execute a repository, build, test, or code generation tool."""
        is_write = any(w in tool_name for w in ("write", "edit", "create", "delete", "modify", "patch"))

        # Enforce intent declaration for side-effect tools
        if is_write and not intent_description:
            return MCPExecutionResult(
                request_id="forge_blocked",
                specialist_id="FORGE",
                server_id=server_id,
                tool_name=tool_name,
                success=False,
                error="FORGE execution blocked: write actions must declare intent_description for SENTINEL review",
            )

        request = MCPExecutionRequest(
            request_id=f"forge_req_{hash(tool_name) % 10000}",
            specialist_id="FORGE",
            server_id=server_id,
            tool_name=tool_name,
            arguments=arguments,
            trust_requirement=TrustLevel.VERIFIED if is_write else TrustLevel.SANDBOXED,
            priority=ExecutionPriority.HIGH if is_write else ExecutionPriority.NORMAL,
            metadata={"intent": intent_description} if intent_description else {},
        )

        return await self._execution_engine.execute(request)
