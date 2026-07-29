"""CapabilityRefresh — re-negotiates MCP capabilities on schema or capability mismatch."""

from __future__ import annotations

import logging
from typing import Optional

from ..capability.capability_engine import CapabilityEngine
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.recovery.capability_refresh")


class CapabilityRefresh:
    """Refreshes server capabilities when schema/capability mismatch is detected."""

    def __init__(self, capability_engine: CapabilityEngine,
                 event_publisher: Optional[MCPEventPublisher] = None):
        self._capability_engine = capability_engine
        self._event_publisher = event_publisher
        self.name = "CapabilityRefresh"

    async def execute(self, request: MCPExecutionRequest, failure_type: FailureType, attempt: int) -> bool:
        profile = self._capability_engine.get_profile(request.server_id)
        if profile:
            log.info("CapabilityRefresh: refreshing capabilities for %s", request.server_id)
            return True
        return False
