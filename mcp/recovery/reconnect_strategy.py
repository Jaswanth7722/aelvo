"""ReconnectStrategy — reconnection with exponential backoff for MCP servers."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from ..client.connection_manager import ConnectionManager
from ..client.reconnect_policy import ReconnectPolicy
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.recovery.reconnect")


class ReconnectStrategy:
    """Reconnects to a server with exponential backoff on connection loss."""

    def __init__(self, connection_manager: ConnectionManager, event_publisher: Optional[MCPEventPublisher] = None):
        self._connection_manager = connection_manager
        self._event_publisher = event_publisher
        self.name = "ReconnectStrategy"

    async def execute(self, request: MCPExecutionRequest, failure_type: FailureType, attempt: int) -> bool:
        policy = ReconnectPolicy(base_delay=1.0, max_delay=30.0, max_retries=3)
        try:
            await asyncio.sleep(policy.get_next_delay())
            from ..registry.models import MCPServerConfig, TransportType
            config = MCPServerConfig(id=request.server_id, name="", transport_type=TransportType.STDIO, connection_config={})
            return await self._connection_manager.connect(config)
        except Exception as e:
            log.warning("ReconnectStrategy failed for %s: %s", request.server_id, e)
            return False
