"""CapabilityGraph — cross-server capability relationships for routing and gap analysis.

Maps semantic task types to capable servers, identifies overlaps for failover,
detects gaps, and tracks capability drift over time.
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from ..registry.models import CapabilityProfile, ToolDefinition, MCPServerRecord

log = logging.getLogger("aelvo.mcp.capability.graph")


class CapabilityGraph:
    """Cross-server capability graph for intelligent routing and gap analysis.

    Builds and maintains:
    - Task type → server mappings
    - Tool overlap clusters (servers that can do the same thing)
    - Capability gap detection
    """

    def __init__(self):
        self._task_to_servers: Dict[str, Set[str]] = defaultdict(set)  # task_type -> {server_ids}
        self._server_to_tasks: Dict[str, Set[str]] = defaultdict(set)  # server_id -> {task_types}
        self._tool_overlap: Dict[str, Set[str]] = defaultdict(set)    # tool_name -> {server_ids}
        self._capability_gaps: List[Dict[str, Any]] = []
        self._last_updated: Optional[datetime] = None

    def update_server(self, server_id: str, profile: CapabilityProfile) -> None:
        """Update the graph with a server's capabilities."""
        # Clear old entries for this server
        for task_type, servers in self._task_to_servers.items():
            servers.discard(server_id)
        for tool_name, servers in self._tool_overlap.items():
            servers.discard(server_id)
        self._server_to_tasks[server_id] = set()

        # Add new entries
        for tool in profile.tools:
            task_types = self._infer_task_types(tool)
            for tt in task_types:
                self._task_to_servers[tt].add(server_id)
                self._server_to_tasks[server_id].add(tt)
            self._tool_overlap[tool.name].add(server_id)

        self._last_updated = datetime.now(timezone.utc)
        self._detect_gaps()

    def remove_server(self, server_id: str) -> None:
        """Remove a server from the graph."""
        for task_type, servers in self._task_to_servers.items():
            servers.discard(server_id)
        self._server_to_tasks.pop(server_id, None)
        for tool_name, servers in self._tool_overlap.items():
            servers.discard(server_id)
        self._detect_gaps()

    def get_tools_for_task(self, task_type: str) -> List[str]:
        """Get tools that can handle a task type."""
        server_ids = self._task_to_servers.get(task_type, set())
        tools = set()
        for tool_name, servers in self._tool_overlap.items():
            if servers & server_ids:
                tools.add(tool_name)
        return list(tools)

    def find_overlapping_servers(self, tool_name: str, exclude: Optional[str] = None) -> List[str]:
        """Find servers that provide the same tool (for failover)."""
        servers = self._tool_overlap.get(tool_name, set())
        if exclude:
            servers = servers - {exclude}
        return list(servers)

    def get_server_capabilities(self, server_id: str) -> List[str]:
        """Get task types a server can handle."""
        return list(self._server_to_tasks.get(server_id, set()))

    def get_gaps(self) -> List[Dict[str, Any]]:
        """Get identified capability gaps."""
        return list(self._capability_gaps)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _infer_task_types(self, tool: ToolDefinition) -> List[str]:
        """Infer task types from tool metadata."""
        task_types = []
        name_lower = tool.name.lower()
        desc_lower = tool.description.lower()

        task_type_map = {
            "code_generation": ["generate", "create", "write", "code", "implement"],
            "code_analysis": ["analyze", "inspect", "examine", "review", "lint"],
            "documentation_search": ["search", "find", "lookup", "document", "reference"],
            "file_operation": ["read", "write", "edit", "file", "directory"],
            "data_query": ["query", "select", "fetch", "get", "retrieve"],
            "code_execution": ["run", "execute", "eval", "shell", "terminal"],
            "debugging": ["debug", "fix", "repair", "error", "exception"],
            "testing": ["test", "assert", "verify", "validate"],
            "deployment": ["deploy", "build", "publish", "release", "package"],
            "research": ["research", "explore", "investigate"],
        }

        for task_type, keywords in task_type_map.items():
            for kw in keywords:
                if kw in name_lower or kw in desc_lower:
                    task_types.append(task_type)
                    break

        return task_types or ["general"]

    def _detect_gaps(self) -> None:
        """Detect capability gaps (tools only available on one server, etc.)."""
        self._capability_gaps = []

        # Single point of failure: tools available on only one server
        for tool_name, servers in self._tool_overlap.items():
            if len(servers) == 1:
                self._capability_gaps.append({
                    "type": "single_point_of_failure",
                    "tool": tool_name,
                    "server": list(servers)[0],
                    "description": f"Tool '{tool_name}' only available on one server",
                })

        # Check for servers with no task type mapping
        for server_id, tasks in self._server_to_tasks.items():
            if not tasks:
                self._capability_gaps.append({
                    "type": "unmapped_server",
                    "server": server_id,
                    "description": f"Server '{server_id}' has no mapped task types",
                })

    @property
    def last_updated(self) -> Optional[datetime]:
        return self._last_updated
