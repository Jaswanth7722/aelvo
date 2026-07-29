"""ResourceCatalog — central catalog of all MCP resources across all registered servers."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from ..registry.models import ResourceDefinition

log = logging.getLogger("aelvo.mcp.capability.resources")


class ResourceCatalog:
    """Registry of all MCP resources across all servers."""

    def __init__(self):
        self._resources: Dict[str, List[Tuple[str, ResourceDefinition]]] = defaultdict(list)

    def register(self, server_id: str, resource: ResourceDefinition) -> None:
        existing = self._resources.get(resource.uri, [])
        if any(sid == server_id for sid, _ in existing):
            return
        self._resources[resource.uri].append((server_id, resource))

    def unregister_server(self, server_id: str) -> int:
        count = 0
        for uri in list(self._resources.keys()):
            self._resources[uri] = [(sid, r) for sid, r in self._resources[uri] if sid != server_id]
            if not self._resources[uri]:
                del self._resources[uri]
            count += 1
        return count

    def find_resource(self, uri: str) -> List[Tuple[str, ResourceDefinition]]:
        return list(self._resources.get(uri, []))

    def list_resources(self) -> List[Dict[str, Any]]:
        return [
            {"uri": uri, "servers": [sid for sid, _ in entries], "count": len(entries)}
            for uri, entries in sorted(self._resources.items())
        ]
