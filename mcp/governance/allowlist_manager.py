"""AllowlistManager — server allowlist management for MCP execution governance."""

from __future__ import annotations

import logging
from typing import List, Optional, Set

log = logging.getLogger("aelvo.mcp.governance.allowlist")


class AllowlistManager:
    """Manages the server allowlist for MCP execution.

    Only servers on the allowlist can receive execution requests.
    The allowlist can be configured per specialist or globally.
    """

    def __init__(self):
        self._global_allowlist: Set[str] = set()
        self._per_specialist: dict = {}  # specialist_id -> {server_ids}

    def add_server(self, server_id: str) -> None:
        """Add a server to the global allowlist."""
        self._global_allowlist.add(server_id)

    def remove_server(self, server_id: str) -> bool:
        """Remove a server from the global allowlist."""
        if server_id in self._global_allowlist:
            self._global_allowlist.discard(server_id)
            return True
        return False

    def add_for_specialist(self, specialist_id: str, server_id: str) -> None:
        """Allow a specialist to use a specific server."""
        if specialist_id not in self._per_specialist:
            self._per_specialist[specialist_id] = set()
        self._per_specialist[specialist_id].add(server_id)

    def remove_for_specialist(self, specialist_id: str, server_id: str) -> bool:
        """Remove a server from a specialist's allowlist."""
        servers = self._per_specialist.get(specialist_id)
        if servers and server_id in servers:
            servers.discard(server_id)
            return True
        return False

    def is_allowed(self, server_id: str, specialist_id: Optional[str] = None) -> bool:
        """Check if a server is allowed for a specialist.

        A server is allowed if:
        - It's on the global allowlist, OR
        - It's on the specialist's specific allowlist
        - If allowlists are empty, all servers are allowed (opt-in model)
        """
        # If no allowlists configured, allow everything (opt-in model)
        if not self._global_allowlist and not self._per_specialist:
            return True

        # Global allowlist check
        if server_id in self._global_allowlist:
            return True

        # Specialist-specific check
        if specialist_id:
            specialist_servers = self._per_specialist.get(specialist_id, set())
            if server_id in specialist_servers:
                return True

        return False

    def list_global(self) -> List[str]:
        return sorted(self._global_allowlist)

    def list_for_specialist(self, specialist_id: str) -> List[str]:
        return sorted(self._per_specialist.get(specialist_id, set()))

    def clear(self) -> None:
        self._global_allowlist.clear()
        self._per_specialist.clear()
