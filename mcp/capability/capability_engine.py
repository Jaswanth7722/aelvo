"""CapabilityEngine — builds and maintains a complete, queryable model of all MCP capabilities.

Supports cross-server queries, capability gap analysis, drift detection,
and specialist-facing capability discovery interfaces.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from ..registry.models import (
    MCPServerRecord,
    CapabilityProfile,
    ToolDefinition,
)
from ..registry.server_registry import ServerRegistry
from .capability_profile import CapabilityProfileBuilder
from .tool_catalog import ToolCatalog
from .prompt_catalog import PromptCatalog
from .resource_catalog import ResourceCatalog
from .capability_graph import CapabilityGraph

log = logging.getLogger("aelvo.mcp.capability")


class CapabilityEngine:
    """Central capability engine for the MCP Platform.

    Maintains:
    - Per-server capability profiles (tools, prompts, resources)
    - Cross-server capability graph (which servers can do what)
    - Capability drift detection (server capabilities changed)
    - Gap analysis (what capabilities are missing)
    """

    def __init__(self, registry: ServerRegistry):
        self._registry = registry
        self._profile_builder = CapabilityProfileBuilder()
        self._tool_catalog = ToolCatalog()
        self._prompt_catalog = PromptCatalog()
        self._resource_catalog = ResourceCatalog()
        self._capability_graph = CapabilityGraph()
        self._profiles: Dict[str, CapabilityProfile] = {}

    async def refresh_capabilities(self, server_id: str, profile: CapabilityProfile) -> bool:
        """Refresh cached capabilities for a server and update catalogs."""
        old_profile = self._profiles.get(server_id)
        drift_detected = self._detect_drift(server_id, old_profile, profile)

        self._profiles[server_id] = profile
        self._registry.update_capabilities(server_id, profile)

        # Update catalogs
        for tool in profile.tools:
            self._tool_catalog.register(server_id, tool)
        for prompt in profile.prompts:
            self._prompt_catalog.register(server_id, prompt)
        for resource in profile.resources:
            self._resource_catalog.register(server_id, resource)

        # Update capability graph
        self._capability_graph.update_server(server_id, profile)

        return drift_detected

    async def find_servers_for_task(self, task_type: str) -> List[MCPServerRecord]:
        """Find servers that can handle a specific task type."""
        tool_names = self._capability_graph.get_tools_for_task(task_type)
        server_ids = set()
        for tool_name in tool_names:
            servers = self._tool_catalog.find_servers_for_tool(tool_name)
            server_ids.update(servers)

        results = []
        for sid in server_ids:
            record = self._registry.get(sid)
            if record and record.enabled:
                results.append(record)
        return results

    async def find_tool(self, name: str) -> List[Tuple[MCPServerRecord, ToolDefinition]]:
        """Find all servers that provide a specific tool."""
        results = []
        entries = self._tool_catalog.find_tool(name)
        for server_id, tool in entries:
            record = self._registry.get(server_id)
            if record and record.enabled:
                results.append((record, tool))
        return results

    async def get_alternatives_for(self, server_id: str, tool_name: str) -> List[Tuple[MCPServerRecord, ToolDefinition]]:
        """Find alternative servers for a tool."""
        results = []
        for sid, tool in self._tool_catalog.find_alternatives(server_id, tool_name):
            if sid != server_id:
                record = self._registry.get(sid)
                if record and record.enabled:
                    results.append((record, tool))
        return results

    async def get_capability_gaps(self) -> List[Dict[str, Any]]:
        """Identify capability gaps across all registered servers."""
        return self._capability_graph.get_gaps()

    def get_profile(self, server_id: str) -> Optional[CapabilityProfile]:
        return self._profiles.get(server_id)

    def get_tool_catalog(self) -> ToolCatalog:
        return self._tool_catalog

    def get_prompt_catalog(self) -> PromptCatalog:
        return self._prompt_catalog

    def get_resource_catalog(self) -> ResourceCatalog:
        return self._resource_catalog

    def get_capability_graph(self) -> CapabilityGraph:
        return self._capability_graph

    # ------------------------------------------------------------------
    # Drift Detection
    # ------------------------------------------------------------------

    def _detect_drift(self, server_id: str, old: Optional[CapabilityProfile], new: CapabilityProfile) -> bool:
        """Detect if capabilities have changed (drift)."""
        if old is None:
            return False
        return old.checksum != new.checksum

    def list_drifted_servers(self) -> List[str]:
        """List servers where capabilities may have drifted from registry."""
        drifted = []
        for server_id, profile in self._profiles.items():
            record = self._registry.get(server_id)
            if record and record.capabilities.checksum != profile.checksum:
                drifted.append(server_id)
        return drifted
