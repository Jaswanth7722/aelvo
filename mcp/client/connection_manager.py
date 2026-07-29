"""ConnectionManager — manages connection lifecycle for all MCP servers.

Handles connect/disconnect lifecycle, reconnection, heartbeats,
and cleanup for all active MCP server connections.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from ..transport.base_transport import BaseTransport
from ..transport.transport_factory import TransportFactory
from ..transport.stdio_transport import MCPConnectionError
from ..registry.models import MCPServerConfig, MCPServerRecord, TransportType, HealthState
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import DisconnectReason

log = logging.getLogger("aelvo.mcp.client.connection")


class ConnectionManager:
    """Manages active MCP server connections.

    Each server gets a transport instance that is created, connected,
    and disconnected through this manager. Provides health checks,
    reconnection triggers, and connection state tracking.
    """

    def __init__(self, event_publisher: Optional[MCPEventPublisher] = None):
        self._transports: Dict[str, BaseTransport] = {}
        self._configs: Dict[str, MCPServerConfig] = {}
        self._connected_servers: Set[str] = set()
        self._heartbeat_tasks: Dict[str, asyncio.Task] = {}
        self._event_publisher = event_publisher or MCPEventPublisher()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Connection Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, config: MCPServerConfig) -> bool:
        """Establish a connection to an MCP server.

        Args:
            config: The server configuration.

        Returns:
            True if connection was successful.
        """
        async with self._lock:
            if config.id in self._connected_servers:
                log.debug("ConnectionManager: %s already connected", config.id)
                return True

            try:
                transport = TransportFactory.create(config)
                await transport.connect()
                self._transports[config.id] = transport
                self._configs[config.id] = config
                self._connected_servers.add(config.id)

                await self._event_publisher.connected(config.id, config.transport_type.value)
                log.info("ConnectionManager: connected to '%s' (%s)", config.name, config.id)

                if config.transport_type in (TransportType.WEBSOCKET, TransportType.STDIO):
                    self._heartbeat_tasks[config.id] = asyncio.create_task(
                        self._heartbeat_loop(config.id)
                    )

                return True

            except MCPConnectionError as e:
                await self._event_publisher.connection_failed(config.id, str(e), 1)
                log.error("ConnectionManager: failed to connect '%s': %s", config.id, e)
                return False

    async def disconnect(self, server_id: str, reason: DisconnectReason = DisconnectReason.GRACEFUL) -> bool:
        """Disconnect from an MCP server.

        Args:
            server_id: The server to disconnect.
            reason: The reason for disconnection.

        Returns:
            True if disconnected successfully.
        """
        async with self._lock:
            if server_id not in self._connected_servers:
                return False

            # Stop heartbeat
            task = self._heartbeat_tasks.pop(server_id, None)
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Disconnect transport
            transport = self._transports.get(server_id)
            if transport:
                try:
                    await transport.disconnect()
                except Exception as e:
                    log.warning("ConnectionManager: error disconnecting %s: %s", server_id, e)

            self._connected_servers.discard(server_id)
            self._transports.pop(server_id, None)
            self._configs.pop(server_id, None)

            await self._event_publisher.disconnected(server_id, reason)
            log.info("ConnectionManager: disconnected '%s'", server_id)
            return True

    async def disconnect_all(self) -> Dict[str, bool]:
        """Disconnect from all servers.

        Returns:
            Dict mapping server_id to disconnect success.
        """
        results = {}
        for server_id in list(self._connected_servers):
            results[server_id] = await self.disconnect(server_id)
        return results

    # ------------------------------------------------------------------
    # Transport Access
    # ------------------------------------------------------------------

    def get_transport(self, server_id: str) -> Optional[BaseTransport]:
        """Get the active transport for a server."""
        return self._transports.get(server_id)

    def is_connected(self, server_id: str) -> bool:
        """Check if a server is currently connected."""
        transport = self._transports.get(server_id)
        if transport:
            return transport.is_connected
        return False

    def list_connected(self) -> List[str]:
        """List all currently connected server IDs."""
        return list(self._connected_servers)

    @property
    def connected_count(self) -> int:
        return len(self._connected_servers)

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self, server_id: str) -> None:
        """Periodic heartbeat check for a server connection."""
        while True:
            try:
                await asyncio.sleep(30)
                transport = self._transports.get(server_id)
                if transport and not transport.is_connected:
                    log.warning("ConnectionManager: heartbeat lost for '%s'", server_id)
                    self._connected_servers.discard(server_id)
                    await self._event_publisher.disconnected(
                        server_id, DisconnectReason.TRANSPORT_FAILURE
                    )
                    break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug("ConnectionManager: heartbeat error for %s: %s", server_id, e)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def get_diagnostics(self) -> Dict[str, Any]:
        """Get connection diagnostics."""
        return {
            "total_connections": len(self._transports),
            "connected": list(self._connected_servers),
            "transports": {
                sid: type(t).__name__ for sid, t in self._transports.items()
            },
        }
