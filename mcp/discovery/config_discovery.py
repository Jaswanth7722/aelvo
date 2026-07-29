"""ConfigDiscovery — discovers MCP servers from configuration files.

Scans ~/.aelvo/mcp_servers.yaml and project-local configs.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from ..registry.models import MCPServerConfig, TransportType, TrustLevel
from ..registry.server_registry import ServerRegistry
from ..events.event_schemas import DiscoverySource

log = logging.getLogger("aelvo.mcp.discovery.config")

DEFAULT_CONFIG_PATHS = [
    os.path.expanduser("~/.aelvo/mcp_servers.yaml"),
    os.path.expanduser("~/.aelvo/mcp_servers.yml"),
    os.path.expanduser("~/.aelvo/mcp_servers.json"),
]


class ConfigDiscovery:
    """Discovers MCP servers from configuration files.

    Scans well-known paths for YAML/JSON server definitions
    and registers them with the server registry.
    """

    def __init__(self, registry: ServerRegistry, config_paths: Optional[List[str]] = None):
        self._registry = registry
        self._config_paths = config_paths or DEFAULT_CONFIG_PATHS
        self.source_type = DiscoverySource.CONFIG

    async def discover(self) -> List[str]:
        """Scan configuration files for MCP server definitions."""
        discovered: List[str] = []
        seen_ids: set = set()

        for config_path in self._config_paths:
            if not os.path.isfile(config_path):
                continue

            try:
                if config_path.endswith(".json"):
                    with open(config_path, "r") as f:
                        data = json.load(f)
                else:
                    try:
                        import yaml
                        with open(config_path, "r") as f:
                            data = yaml.safe_load(f)
                    except ImportError:
                        log.warning("PyYAML not installed, skipping %s", config_path)
                        continue

                servers = data.get("servers", [])
                for server_def in servers:
                    server_id = self._register_from_config(server_def)
                    if server_id and server_id not in seen_ids:
                        seen_ids.add(server_id)
                        discovered.append(server_id)

            except Exception as e:
                log.warning("Failed to parse config %s: %s", config_path, e)

        return discovered

    def _register_from_config(self, server_def: Dict[str, Any]) -> Optional[str]:
        """Register a server from a config file definition."""
        server_id = server_def.get("id", "")
        if not server_id:
            return None

        if self._registry.get(server_id):
            return server_id

        transport_def = server_def.get("transport", {})
        transport_type_str = transport_def.get("type", "stdio").lower()
        if transport_type_str not in ("stdio", "websocket", "http"):
            transport_type_str = "stdio"

        config = MCPServerConfig(
            id=server_id,
            name=server_def.get("name", server_id),
            description=server_def.get("description", ""),
            transport_type=TransportType(transport_type_str),
            connection_config={
                "command": transport_def.get("command", []),
                "env": transport_def.get("env", {}),
                "url": transport_def.get("url", ""),
                "host": transport_def.get("host", ""),
                "port": transport_def.get("port", 0),
                "headers": transport_def.get("headers", {}),
            },
            trust_level=TrustLevel(server_def.get("trust_level", "sandboxed").lower()),
            auto_connect=server_def.get("auto_connect", False),
            tags=server_def.get("tags", []),
            metadata=server_def.get("metadata", {}),
            timeout_ms=server_def.get("capabilities", {}).get("timeout_ms", 30000),
            max_concurrent=server_def.get("capabilities", {}).get("max_concurrent", 5),
        )
        self._registry.register(config)
        return server_id
