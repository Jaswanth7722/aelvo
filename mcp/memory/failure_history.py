"""FailureHistory — persistent storage and analysis of MCP execution failure events."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional
from .mcp_memory_store import MCPMemoryStore

log = logging.getLogger("aelvo.mcp.memory.failure")


class FailureHistory:
    """Tracks and analyzes failure events, causes, and recovery outcomes."""

    def __init__(self, memory_store: MCPMemoryStore):
        self._memory_store = memory_store

    async def record_failure(
        self,
        server_id: str,
        tool_name: str,
        failure_type: str,
        error: str,
        recovery_attempted: bool,
        recovery_successful: bool,
    ) -> None:
        """Store a failure event."""
        await self._memory_store.store_failure(
            server_id=server_id,
            tool_name=tool_name,
            failure_type=failure_type,
            error=error,
            recovery_attempted=recovery_attempted,
            recovery_successful=recovery_successful,
        )

    async def get_failures(self, server_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve recent failures, optionally filtered by server."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            if server_id:
                cursor = conn.execute(
                    """
                    SELECT server_id, tool_name, failure_type, error, recovery_attempted, recovery_successful, timestamp
                    FROM mcp_failures
                    WHERE server_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (server_id, limit),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT server_id, tool_name, failure_type, error, recovery_attempted, recovery_successful, timestamp
                    FROM mcp_failures
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (limit,),
                )

            return [
                {
                    "server_id": row[0],
                    "tool_name": row[1],
                    "failure_type": row[2],
                    "error": row[3],
                    "recovery_attempted": bool(row[4]),
                    "recovery_successful": bool(row[5]),
                    "timestamp": row[6],
                }
                for row in cursor.fetchall()
            ]
        except Exception as e:
            log.warning("Failed to retrieve failures: %s", e)
            return []
        finally:
            conn.close()

    async def get_server_failure_count(self, server_id: str, minutes_lookback: int = 60) -> int:
        """Count failures for a server within a lookback period."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            # Simple timestamp check assuming ISO format
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM mcp_failures
                WHERE server_id = ? AND datetime(timestamp) >= datetime('now', ?)
                """,
                (server_id, f"-{minutes_lookback} minutes"),
            )
            row = cursor.fetchone()
            return row[0] if row else 0
        except Exception as e:
            log.debug("Failed to count failures for %s: %s", server_id, e)
            return 0
        finally:
            conn.close()
