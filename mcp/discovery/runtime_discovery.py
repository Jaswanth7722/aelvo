"""RuntimeDiscovery — dynamic runtime discovery for MCP servers (future: mDNS, registry queries).

Currently a placeholder for future mDNS-based and registry-query-based discovery.
"""

from __future__ import annotations

import logging
from typing import List, Optional
from ..registry.server_registry import ServerRegistry
from ..events.event_schemas import DiscoverySource

log = logging.getLogger("aelvo.mcp.discovery.runtime")


class RuntimeDiscovery:
    """Discovers MCP servers at runtime through dynamic mechanisms.

    Future capabilities:
    - mDNS discovery of MCP servers on the local network
    - Registry query discovery (public MCP server registries)
    - Zero-config LAN server discovery
    """

    def __init__(self, registry: ServerRegistry):
        self._registry = registry
        self.source_type = DiscoverySource.RUNTIME

    async def discover(self) -> List[str]:
        """Run runtime discovery.

        Currently a placeholder — logs that discovery was attempted
        but no runtime-based mechanisms are active yet.

        Returns:
            Empty list (no runtime discovery mechanisms active).
        """
        log.info("RuntimeDiscovery: no active runtime discovery mechanisms")
        return []

    async def add_dynamic_server(self, server_id: str, config: dict) -> None:
        """Register a dynamically discovered server (for future use)."""
        from ..registry.models import MCPServerConfig, TransportType, TrustLevel

        mcp_config = MCPServerConfig(
            id=server_id,
            name=config.get("name", server_id),
            description=config.get("description", "Dynamically discovered"),
            transport_type=TransportType(config.get("transport", "websocket").lower()),
            connection_config=config.get("config", {}),
            trust_level=TrustLevel.SANDBOXED,
            tags=["dynamic", *config.get("tags", [])],
        )
        self._registry.register(mcp_config)
