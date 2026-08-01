"""DiscoveryEngine — orchestrates MCP server discovery from all configured sources."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ..registry.server_registry import ServerRegistry
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import DiscoverySource
from .filesystem_discovery import FilesystemDiscovery
from .config_discovery import ConfigDiscovery
from .runtime_discovery import RuntimeDiscovery
from .manual_registration import ManualRegistration

log = logging.getLogger("aelvo.mcp.discovery")


class DiscoveryEngine:
    """Orchestrates MCP server discovery from multiple sources.

    Discovery sources are scanned in priority order:
    1. Manual Registration (highest priority)
    2. Config Discovery (.aelvo/mcp_servers.yaml)
    3. Filesystem Discovery (PATH, known paths)
    4. Runtime Discovery (future: mDNS, registry queries)
    """

    def __init__(
        self,
        registry: ServerRegistry,
        event_publisher: Optional[MCPEventPublisher] = None,
        scan_paths: Optional[List[str]] = None,
        discover_path_executables: bool = False,
    ):
        self._registry = registry
        self._event_publisher = event_publisher
        self._sources = [
            ManualRegistration(registry),
            ConfigDiscovery(registry),
            FilesystemDiscovery(
                registry,
                scan_paths=scan_paths,
                discover_path_executables=discover_path_executables,
            ),
            RuntimeDiscovery(registry),
        ]

    # ------------------------------------------------------------------
    # Discovery Lifecycle
    # ------------------------------------------------------------------

    async def discover_all(self) -> Dict[str, List[str]]:
        """Run all discovery sources and return results by source.

        Returns:
            Dict mapping source name -> list of discovered server IDs.
        """
        results: Dict[str, List[str]] = {}

        for source in self._sources:
            try:
                discovered = await source.discover()
                source_name = type(source).__name__
                results[source_name] = discovered

                for server_id in discovered:
                    record = self._registry.get(server_id)
                    if record:
                        await self._publish_discovered(server_id, source.source_type, record.name)

                log.info("DiscoveryEngine: %s discovered %d servers", source_name, len(discovered))
            except Exception as e:
                log.warning("DiscoveryEngine: %s failed: %s", type(source).__name__, e)

        total = sum(len(v) for v in results.values())
        log.info("DiscoveryEngine: discovered %d total servers", total)
        return results

    async def discover_source(self, source_type: DiscoverySource) -> List[str]:
        """Run discovery from a specific source type."""
        for source in self._sources:
            if source.source_type == source_type:
                discovered = await source.discover()
                for server_id in discovered:
                    record = self._registry.get(server_id)
                    if record:
                        await self._publish_discovered(server_id, source_type, record.name)
                return discovered
        return []

    # ------------------------------------------------------------------
    # Source Access
    # ------------------------------------------------------------------

    def get_source(self, source_type: DiscoverySource) -> Optional[Any]:
        """Get a specific discovery source by type."""
        for source in self._sources:
            if source.source_type == source_type:
                return source
        return None

    def get_sources(self) -> List[Any]:
        """Get all discovery sources."""
        return list(self._sources)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _publish_discovered(self, server_id: str, source: DiscoverySource, name: str) -> None:
        """Publish discovery event."""
        if self._event_publisher:
            await self._event_publisher.server_discovered(server_id, source, name)
