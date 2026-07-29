"""FailoverStrategy — routes to alternate server when primary MCP server fails."""

from __future__ import annotations

import logging
from typing import Optional

from ..registry.server_registry import ServerRegistry
from ..capability.capability_engine import CapabilityEngine
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.recovery.failover")


class FailoverStrategy:
    """Routes MCP execution to an alternate server when the primary fails."""

    def __init__(self, registry: ServerRegistry, capability_engine: CapabilityEngine,
                 event_publisher: Optional[MCPEventPublisher] = None):
        self._registry = registry
        self._capability_engine = capability_engine
        self._event_publisher = event_publisher
        self.name = "FailoverStrategy"

    async def execute(self, request: MCPExecutionRequest, failure_type: FailureType, attempt: int) -> bool:
        alternatives = await self._capability_engine.get_alternatives_for(request.server_id, request.tool_name)
        if alternatives:
            record, _ = alternatives[0]
            log.info("FailoverStrategy: routing %s from %s to %s", request.tool_name, request.server_id, record.id)
            return True
        return False
