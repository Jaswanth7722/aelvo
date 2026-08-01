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
        if profile is None:
            log.warning("CapabilityRefresh: no profile available for %s — cannot refresh", request.server_id)
            return False
        refreshed = await self._capability_engine.refresh_capabilities(request.server_id, profile)
        log.info(
            "CapabilityRefresh: refreshed capabilities for %s (drift=%s)",
            request.server_id, refreshed,
        )
        if self._event_publisher:
            try:
                from ..events.mcp_events import MCPCapabilityDriftDetected
                await self._event_publisher.publish(MCPCapabilityDriftDetected(
                    event_id=f"cap_refresh_{request.server_id}",
                    server_id=request.server_id,
                    previous_checksum="unknown",
                    current_checksum="refreshed",
                    changes=["capabilities re-negotiated after failure"],
                ))
            except Exception as e:
                log.warning("CapabilityRefresh: failed to publish refresh event: %s", e)
        return True
