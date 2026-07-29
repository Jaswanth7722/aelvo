"""ManualRegistration — explicit manual registration of MCP servers via CLI or API."""

from __future__ import annotations

import logging
from typing import List

from ..registry.models import MCPServerConfig, MCPServerRecord
from ..registry.server_registry import ServerRegistry
from ..events.event_schemas import DiscoverySource

log = logging.getLogger("aelvo.mcp.discovery.manual")


class ManualRegistration:
    """Handles explicit manual registration of MCP servers.

    Manual registrations have highest priority — they override any
    auto-discovered configuration for the same server ID.
    """

    def __init__(self, registry: ServerRegistry):
        self._registry = registry
        self.source_type = DiscoverySource.MANUAL

    async def discover(self) -> List[str]:
        """Manual registration doesn't auto-discover.

        Servers are registered explicitly via register_server().
        """
        return []

    def register_server(self, config: MCPServerConfig) -> MCPServerRecord:
        """Register a server manually with explicit configuration.

        Args:
            config: The full server configuration.

        Returns:
            The registered server record.
        """
        record = self._registry.register(config)
        log.info("ManualRegistration: registered '%s' (%s)", config.name, config.id)
        return record

    def unregister_server(self, server_id: str) -> bool:
        """Unregister a manually registered server."""
        return self._registry.unregister(server_id)
