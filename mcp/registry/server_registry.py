"""Central MCP Server Registry — single source of truth for all known MCP servers."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from .models import (
    MCPServerRecord,
    MCPServerConfig,
    ServerRegistryFilter,
    TrustLevel,
    HealthState,
    CapabilityProfile,
    TransportType,
)
from .registry_store import RegistryStore

log = logging.getLogger("aelvo.mcp.registry")


class ServerRegistry:
    """Central registry for all known MCP servers.

    Operations:
    - register / unregister servers
    - enable / disable servers
    - update health, trust, capabilities
    - query and filter servers
    - persist across restarts
    """

    def __init__(self, store: Optional[RegistryStore] = None):
        self._store = store or RegistryStore()
        self._servers: Dict[str, MCPServerRecord] = {}
        self._loaded = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def load(self) -> int:
        """Load persisted server records from store."""
        records = self._store.load_all()
        count = 0
        for record in records:
            self._servers[record.id] = record
            count += 1
        self._loaded = True
        log.info("MCP Registry: loaded %d server records", count)
        return count

    async def save(self) -> int:
        """Persist all server records to store."""
        records = list(self._servers.values())
        self._store.save_all(records)
        log.info("MCP Registry: saved %d server records", len(records))
        return len(records)

    # ------------------------------------------------------------------
    # CRUD Operations
    # ------------------------------------------------------------------

    def register(self, config: MCPServerConfig) -> MCPServerRecord:
        """Register a new MCP server from config."""
        if config.id in self._servers:
            log.warning("MCP Registry: server %s already registered, updating", config.id)
            existing = self._servers[config.id]
            existing.name = config.name
            existing.description = config.description
            existing.transport_type = config.transport_type
            existing.connection_config = config.connection_config
            existing.trust_level = config.trust_level
            existing.enabled = config.enabled
            existing.tags = config.tags
            existing.metadata.update(config.metadata)
            return existing

        record = MCPServerRecord(
            id=config.id,
            name=config.name,
            description=config.description,
            transport_type=config.transport_type,
            connection_config=config.connection_config,
            trust_level=config.trust_level,
            enabled=config.enabled,
            tags=config.tags,
            metadata=config.metadata,
            capabilities=CapabilityProfile(server_id=config.id),
        )
        self._servers[record.id] = record
        log.info("MCP Registry: registered server '%s' (%s)", record.name, record.id)
        return record

    def unregister(self, server_id: str) -> bool:
        """Remove a server from the registry."""
        if server_id in self._servers:
            del self._servers[server_id]
            log.info("MCP Registry: unregistered server %s", server_id)
            return True
        return False

    def enable(self, server_id: str) -> bool:
        """Activate a server."""
        server = self._servers.get(server_id)
        if server:
            server.enabled = True
            return True
        return False

    def disable(self, server_id: str) -> bool:
        """Deactivate a server without removing it."""
        server = self._servers.get(server_id)
        if server:
            server.enabled = False
            return True
        return False

    # ------------------------------------------------------------------
    # Status Updates
    # ------------------------------------------------------------------

    def update_health(self, server_id: str, state: HealthState) -> bool:
        """Update server health status."""
        server = self._servers.get(server_id)
        if server:
            server.health_state = state
            if state == HealthState.HEALTHY:
                server.last_seen = datetime.now(timezone.utc)
            return True
        return False

    def update_trust(self, server_id: str, level: TrustLevel) -> bool:
        """Modify server trust level."""
        server = self._servers.get(server_id)
        if server:
            old = server.trust_level
            server.trust_level = level
            log.info("MCP Registry: trust changed for %s: %s -> %s", server_id, old, level)
            return True
        return False

    def update_capabilities(self, server_id: str, profile: CapabilityProfile) -> bool:
        """Refresh cached capabilities for a server."""
        server = self._servers.get(server_id)
        if server:
            server.capabilities = profile
            profile.negotiated_at = datetime.now(timezone.utc)
            return True
        return False

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def query(self, filters: Optional[ServerRegistryFilter] = None) -> List[MCPServerRecord]:
        """Search the registry with optional filters."""
        results = list(self._servers.values())

        if not filters:
            return results

        if filters.trust_level is not None:
            results = [r for r in results if r.trust_level == filters.trust_level]
        if filters.health_state is not None:
            results = [r for r in results if r.health_state == filters.health_state]
        if filters.transport_type is not None:
            results = [r for r in results if r.transport_type == filters.transport_type]
        if filters.enabled is not None:
            results = [r for r in results if r.enabled == filters.enabled]
        if filters.tag is not None:
            results = [r for r in results if filters.tag in r.tags]
        if filters.capability_tool is not None:
            results = [
                r for r in results
                if any(t.name == filters.capability_tool for t in r.capabilities.tools)
            ]

        return results

    def get(self, server_id: str) -> Optional[MCPServerRecord]:
        """Fetch a specific record by ID."""
        return self._servers.get(server_id)

    def list_servers(self) -> List[MCPServerRecord]:
        """List all registered servers."""
        return list(self._servers.values())

    def count(self) -> int:
        return len(self._servers)

    @property
    def loaded(self) -> bool:
        return self._loaded
