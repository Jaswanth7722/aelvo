"""ORACLE MCP specialist integration contract."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List
from ...execution.execution_engine import MCPExecutionEngine
from ...execution.execution_request import MCPExecutionRequest
from ...execution.execution_result import MCPExecutionResult
from ...memory.mcp_memory_store import MCPMemoryStore
from ...registry.models import TrustLevel
from ...events.event_schemas import ExecutionPriority

log = logging.getLogger("aelvo.mcp.specialist.oracle")


class OracleMCPInterface:
    """Specialist integration contract for ORACLE.

    Permitted MCP usage:
    - Full read access to documentation, research, and knowledge servers
    - Batch queries permitted

    Key behavior:
    Memory tracks which servers produce the highest-quality results for ORACLE's query patterns.
    """

    def __init__(self, execution_engine: MCPExecutionEngine, memory_store: MCPMemoryStore):
        self._execution_engine = execution_engine
        self._memory_store = memory_store

    async def execute_knowledge_query(
        self,
        server_id: str,
        tool_name: str,
        arguments: Dict[str, Any],
    ) -> MCPExecutionResult:
        """Execute a documentation or research query."""
        request = MCPExecutionRequest(
            request_id=f"oracle_req_{hash(tool_name) % 10000}",
            specialist_id="ORACLE",
            server_id=server_id,
            tool_name=tool_name,
            arguments=arguments,
            trust_requirement=TrustLevel.SANDBOXED,
            priority=ExecutionPriority.NORMAL,
        )
        result = await self._execution_engine.execute(request)
        return result

    async def execute_batch_queries(
        self,
        queries: List[Dict[str, Any]],
    ) -> List[MCPExecutionResult]:
        """Execute multiple read queries in parallel."""
        tasks = []
        for q in queries:
            tasks.append(
                self.execute_knowledge_query(
                    server_id=q["server_id"],
                    tool_name=q["tool_name"],
                    arguments=q.get("arguments", {}),
                )
            )
        return await asyncio.gather(*tasks) if tasks else []
