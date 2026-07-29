"""SpecialistPreference — tracks specialist preferences and affinity for specific MCP servers."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from .mcp_memory_store import MCPMemoryStore

log = logging.getLogger("aelvo.mcp.memory.preference")


class SpecialistPreference:
    """Manages specialist-server preference scores based on historical execution outcomes."""

    def __init__(self, memory_store: MCPMemoryStore):
        self._memory_store = memory_store

    async def record_routing_outcome(
        self,
        specialist_id: str,
        server_id: str,
        tool_name: str,
        success: bool,
    ) -> None:
        """Update preference scores based on routing outcome."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            # First, fetch existing record
            cursor = conn.execute(
                """
                SELECT preference_score, total_calls, success_calls
                FROM mcp_routing
                WHERE specialist_id = ? AND server_id = ? AND tool_name = ?
                """,
                (specialist_id, server_id, tool_name),
            )
            row = cursor.fetchone()

            if row:
                score, total, succs = row
                total += 1
                if success:
                    succs += 1
                # Incrementally update preference score (alpha = 0.1 for moving average)
                alpha = 0.1
                outcome_val = 1.0 if success else 0.0
                score = (1.0 - alpha) * score + alpha * outcome_val
            else:
                total = 1
                succs = 1 if success else 0
                score = 0.6 if success else 0.4

            conn.execute(
                """
                INSERT OR REPLACE INTO mcp_routing
                    (specialist_id, server_id, tool_name, preference_score, total_calls, success_calls, last_routed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    specialist_id,
                    server_id,
                    tool_name,
                    score,
                    total,
                    succs,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
        except Exception as e:
            log.warning("Failed to record routing outcome: %s", e)
        finally:
            conn.close()

    async def get_preference(self, specialist_id: str, server_id: str, tool_name: str) -> float:
        """Get the preference score for a specialist-server-tool combination."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                """
                SELECT preference_score FROM mcp_routing
                WHERE specialist_id = ? AND server_id = ? AND tool_name = ?
                """,
                (specialist_id, server_id, tool_name),
            )
            row = cursor.fetchone()
            return row[0] if row else 0.5
        except Exception as e:
            log.debug("Failed to get preference score: %s", e)
            return 0.5
        finally:
            conn.close()
