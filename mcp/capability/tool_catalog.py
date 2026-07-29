"""ToolCatalog — central catalog of all MCP tools across all registered servers."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from ..registry.models import ToolDefinition

log = logging.getLogger("aelvo.mcp.capability.tools")


class ToolCatalog:
    """Registry of all MCP tools across all servers.

    Maintains:
    - Tool name → server(s) mapping
    - Tool metadata (schemas, descriptions)
    - Overlap detection (same tool on multiple servers)
    """

    def __init__(self):
        self._tools: Dict[str, List[Tuple[str, ToolDefinition]]] = defaultdict(list)  # tool_name -> [(server_id, tool)]

    def register(self, server_id: str, tool: ToolDefinition) -> None:
        """Register a tool in the catalog."""
        existing = self._tools.get(tool.name, [])
        # Avoid duplicates
        if any(sid == server_id for sid, _ in existing):
            return
        self._tools[tool.name].append((server_id, tool))

    def unregister_server(self, server_id: str) -> int:
        """Remove all tools for a specific server."""
        count = 0
        for tool_name in list(self._tools.keys()):
            self._tools[tool_name] = [(sid, t) for sid, t in self._tools[tool_name] if sid != server_id]
            if not self._tools[tool_name]:
                del self._tools[tool_name]
            count += 1
        return count

    def find_tool(self, name: str) -> List[Tuple[str, ToolDefinition]]:
        """Find all servers providing a specific tool."""
        return list(self._tools.get(name, []))

    def find_servers_for_tool(self, name: str) -> List[str]:
        """Find all server IDs providing a specific tool."""
        return [sid for sid, _ in self._tools.get(name, [])]

    def find_alternatives(self, exclude_server_id: str, tool_name: str) -> List[Tuple[str, ToolDefinition]]:
        """Find alternative servers providing a tool, excluding a specific server."""
        return [(sid, t) for sid, t in self._tools.get(tool_name, []) if sid != exclude_server_id]

    def search_tools(self, query: str) -> List[Tuple[str, str, ToolDefinition]]:
        """Search tools by name or description.

        Returns:
            List of (server_id, tool_name, ToolDefinition) matching the query.
        """
        results = []
        q = query.lower()
        for tool_name, entries in self._tools.items():
            if q in tool_name.lower():
                for sid, tool in entries:
                    results.append((sid, tool_name, tool))
            else:
                for sid, tool in entries:
                    if q in tool.description.lower():
                        results.append((sid, tool_name, tool))
        return results

    def list_tools(self) -> List[Dict[str, Any]]:
        """List all tools with their server mappings."""
        return [
            {"name": name, "servers": [sid for sid, _ in entries], "count": len(entries)}
            for name, entries in sorted(self._tools.items())
        ]

    def get_tool_count(self) -> int:
        return len(self._tools)

    def get_server_tool_count(self, server_id: str) -> int:
        return sum(1 for entries in self._tools.values() for sid, _ in entries if sid == server_id)
