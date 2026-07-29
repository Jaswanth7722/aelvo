"""TimeoutManager — enforces configurable timeouts at connection, negotiation, and request levels."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger("aelvo.mcp.client.timeout")


class TimeoutManager:
    """Centralized timeout enforcement for all MCP operations.

    Timeouts are configurable per server and per operation type.
    Hard-stop timeouts cannot be overridden by individual requests.
    """

    def __init__(self, default_timeout_ms: int = 30000, hard_stop_ms: int = 120000):
        self._default_timeout_ms = default_timeout_ms
        self._hard_stop_ms = hard_stop_ms
        self._per_server: Dict[str, Dict[str, int]] = {}  # server_id -> {op_type -> timeout_ms}

    def configure_server(self, server_id: str, connection_timeout_ms: int = 10000,
                         negotiation_timeout_ms: int = 15000,
                         request_timeout_ms: int = 30000) -> None:
        """Configure timeouts for a specific server."""
        self._per_server[server_id] = {
            "connection": min(connection_timeout_ms, self._hard_stop_ms),
            "negotiation": min(negotiation_timeout_ms, self._hard_stop_ms),
            "request": min(request_timeout_ms, self._hard_stop_ms),
        }

    def get_timeout(self, server_id: str, op_type: str = "request") -> float:
        """Get the timeout in seconds for a specific server/operation."""
        server_config = self._per_server.get(server_id, {})
        timeout_ms = server_config.get(op_type, self._default_timeout_ms)
        return min(timeout_ms, self._hard_stop_ms) / 1000.0

    async def run_with_timeout(self, server_id: str, op_type: str,
                                 coro, timeout_override_ms: Optional[int] = None) -> Any:
        """Run an async operation with timeout enforcement.

        Args:
            server_id: The server being called.
            op_type: Operation type (connection, negotiation, request).
            coro: The async coroutine to run.
            timeout_override_ms: Optional per-call timeout override.

        Returns:
            The coroutine result.

        Raises:
            asyncio.TimeoutError: If the operation times out.
        """
        if timeout_override_ms:
            timeout_s = min(timeout_override_ms, self._hard_stop_ms) / 1000.0
        else:
            timeout_s = self.get_timeout(server_id, op_type)

        try:
            result = await asyncio.wait_for(coro, timeout=timeout_s)
            return result
        except asyncio.TimeoutError:
            log.warning("TimeoutManager: %s operation timed out for %s after %.1fs",
                        op_type, server_id, timeout_s)
            raise

    @property
    def hard_stop_ms(self) -> int:
        return self._hard_stop_ms

    def get_config(self) -> Dict[str, Any]:
        return {
            "default_timeout_ms": self._default_timeout_ms,
            "hard_stop_ms": self._hard_stop_ms,
            "per_server": self._per_server,
        }
