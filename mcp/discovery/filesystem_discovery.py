"""FilesystemDiscovery — scans local filesystem for MCP server executables and manifests."""

from __future__ import annotations

import json
import logging
import os
from typing import List, Optional

from ..registry.models import MCPServerConfig, TransportType, TrustLevel
from ..registry.server_registry import ServerRegistry
from ..events.event_schemas import DiscoverySource

log = logging.getLogger("aelvo.mcp.discovery.filesystem")


class FilesystemDiscovery:
    """Discovers MCP servers by scanning the filesystem.

    Scans:
    - PATH entries for executables matching mcp-* or *.mcp
    - ~/.aelvo/servers/ directory for server definition files
    - Project roots for mcp.json / mcp.yaml manifests
    """

    def __init__(
        self,
        registry: ServerRegistry,
        scan_paths: Optional[List[str]] = None,
        discover_path_executables: bool = False,
    ):
        self._registry = registry
        self._scan_paths = scan_paths or []
        # PATH-executable auto-registration is opt-in: auto-registering any
        # `mcp-*`/`*.mcp` executable found on PATH would let an attacker who
        # can write to a PATH directory execute arbitrary code through MCP.
        # Require explicit user opt-in and register candidates as QUARANTINED
        # (inspection only) until the user promotes them.
        self.discover_path_executables = discover_path_executables
        self.source_type = DiscoverySource.FILESYSTEM

    async def discover(self) -> List[str]:
        """Scan the filesystem for MCP server executables and manifests."""
        discovered: List[str] = []
        seen_ids: set = set()

        # 1. Scan PATH for mcp-* executables (opt-in; requires user confirmation)
        if self.discover_path_executables:
            log.info(
                "PATH executable discovery is enabled; candidates are "
                "registered as QUARANTINED pending user approval"
            )
            for path_entry in os.environ.get("PATH", "").split(os.pathsep):
                if not path_entry:
                    continue
                try:
                    for filename in os.listdir(path_entry):
                        if filename.startswith("mcp-") or filename.endswith(".mcp"):
                            server_id = self._register_path_executable(path_entry, filename)
                            if server_id and server_id not in seen_ids:
                                seen_ids.add(server_id)
                                discovered.append(server_id)
                except (PermissionError, FileNotFoundError):
                    continue

        # 2. Scan ~/.aelvo/servers/ directory
        home_servers = os.path.expanduser("~/.aelvo/servers")
        if os.path.isdir(home_servers):
            for filename in os.listdir(home_servers):
                if filename.endswith(".json") or filename.endswith(".yaml") or filename.endswith(".yml"):
                    server_id = self._register_from_manifest(os.path.join(home_servers, filename))
                    if server_id and server_id not in seen_ids:
                        seen_ids.add(server_id)
                        discovered.append(server_id)

        # 3. Scan project roots for mcp.json / mcp.yaml
        for project_path in self._scan_paths:
            if os.path.isdir(project_path):
                for manifest_name in ("mcp.json", "mcp.yaml", "mcp.yml"):
                    manifest_path = os.path.join(project_path, manifest_name)
                    if os.path.isfile(manifest_path):
                        server_id = self._register_from_manifest(manifest_path)
                        if server_id and server_id not in seen_ids:
                            seen_ids.add(server_id)
                            discovered.append(server_id)

        return discovered

    def _register_path_executable(self, path_entry: str, filename: str) -> Optional[str]:
        """Register an executable found in PATH."""
        executable_path = os.path.join(path_entry, filename)
        if not os.access(executable_path, os.X_OK):
            return None

        server_id = f"fs_{filename}"
        if self._registry.get(server_id):
            return server_id

        config = MCPServerConfig(
            id=server_id,
            name=filename.replace(".mcp", "").replace("mcp-", "").title(),
            description=f"Auto-discovered MCP server (unvetted PATH executable): {filename}",
            transport_type=TransportType.STDIO,
            connection_config={"command": executable_path},
            # Unvetted executables from PATH are registered as QUARANTINED
            # (inspection only, no execution) until the user explicitly
            # promotes their trust level.
            trust_level=TrustLevel.QUARANTINED,
            auto_connect=False,
            tags=["autodiscovered", "filesystem", "unvetted"],
        )
        self._registry.register(config)
        log.warning(
            "Registered unvetted PATH executable as QUARANTINED "
            "(no execution until user approval): %s", executable_path,
        )
        return server_id

    def _register_from_manifest(self, manifest_path: str) -> Optional[str]:
        """Register a server from a manifest file."""
        try:
            if manifest_path.endswith(".json"):
                with open(manifest_path, "r") as f:
                    data = json.load(f)
            else:
                # YAML manifest
                try:
                    import yaml
                    with open(manifest_path, "r") as f:
                        data = yaml.safe_load(f)
                except ImportError:
                    log.warning("PyYAML not installed, skipping %s", manifest_path)
                    return None

            if not data or not isinstance(data, dict):
                return None

            server_id = data.get("id", f"manifest_{os.path.basename(manifest_path)}")
            if self._registry.get(server_id):
                return server_id

            config = MCPServerConfig(
                id=server_id,
                name=data.get("name", server_id),
                description=data.get("description", ""),
                transport_type=TransportType(data.get("transport", "stdio").lower()),
                connection_config=data.get("config", {}),
                trust_level=TrustLevel(data.get("trust", "sandboxed").lower()),
                auto_connect=data.get("auto_connect", False),
                tags=["autodiscovered", "manifest", *data.get("tags", [])],
                metadata=data.get("metadata", {}),
            )
            self._registry.register(config)
            return server_id

        except Exception as e:
            log.warning("Failed to parse manifest %s: %s", manifest_path, e)
            return None
