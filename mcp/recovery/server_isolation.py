"""ServerIsolation — isolates failing MCP servers to prevent cascading failures."""

from __future__ import annotations

import logging
from typing import Optional

from ..registry.server_registry import ServerRegistry
from ..registry.health_tracker import HealthTracker
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.recovery.isolation")


class ServerIsolation:
    """Isolates a failing server by marking it unreachable and disabling it."""

    def __init__(self, registry: ServerRegistry, health_tracker: HealthTracker,
                 event_publisher: Optional[MCPEventPublisher] = None):
        self._registry = registry
        self._health_tracker = health_tracker
        self._event_publisher = event_publisher
        self.name = "ServerIsolation"

    async def execute(self, request: MCPExecutionRequest, failure_type: FailureType, attempt: int) -> bool:
        self._registry.disable(request.server_id)
        self._health_tracker.mark_unreachable(request.server_id, f"Isolated after {failure_type.value}")
        log.warning("ServerIsolation: isolated '%s' after %s", request.server_id, failure_type.value)
        return False
