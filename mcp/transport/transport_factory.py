"""Transport Factory — instantiates the correct transport based on server configuration."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from .base_transport import BaseTransport
from .stdio_transport import StdioTransport
from .websocket_transport import WebSocketTransport
from .http_transport import HttpTransport
from ..registry.models import TransportType, MCPServerConfig

log = logging.getLogger("aelvo.mcp.transport.factory")


class TransportFactory:
    """Factory for creating transport instances from server configuration."""

    @staticmethod
    def create(config: MCPServerConfig) -> BaseTransport:
        """Create the appropriate transport for the given server config.

        Args:
            config: The MCP server configuration.

        Returns:
            An initialized transport instance (not yet connected).

        Raises:
            ValueError: If the transport type is unsupported.
        """
        conn = config.connection_config or {}

        if config.transport_type == TransportType.STDIO:
            command = conn.get("command", [])
            if isinstance(command, str):
                import shlex
                command = shlex.split(command)
            if not command:
                raise ValueError(f"Stdio transport for {config.id} requires 'command' in connection_config")
            return StdioTransport(
                command=command,
                env=conn.get("env"),
                cwd=conn.get("cwd"),
            )

        elif config.transport_type == TransportType.WEBSOCKET:
            url = conn.get("url")
            if not url:
                # Default URL pattern
                host = conn.get("host", "localhost")
                port = conn.get("port", 8080)
                path = conn.get("path", "/mcp")
                url = f"ws://{host}:{port}{path}"
            return WebSocketTransport(
                url=url,
                headers=conn.get("headers"),
                max_retries=conn.get("max_retries", 3),
            )

        elif config.transport_type == TransportType.HTTP:
            base_url = conn.get("base_url")
            if not base_url:
                host = conn.get("host", "localhost")
                port = conn.get("port", 8080)
                path = conn.get("path", "/mcp")
                base_url = f"http://{host}:{port}{path}"
            return HttpTransport(
                base_url=base_url,
                headers=conn.get("headers"),
                timeout_ms=config.timeout_ms,
                use_sse=conn.get("use_sse", True),
                sse_url=conn.get("sse_url"),
            )

        else:
            raise ValueError(f"Unsupported transport type: {config.transport_type}")
