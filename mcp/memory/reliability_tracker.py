"""ReliabilityTracker — calculates and tracks reliability scores for MCP servers and tools."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, Optional
from ..execution.execution_result import MCPExecutionResult
from .mcp_memory_store import MCPMemoryStore

log = logging.getLogger("aelvo.mcp.memory.reliability")


class ReliabilityTracker:
    """Computes and updates reliability scores for MCP servers and tools.

    Reliability Score formula:
    reliability_score = (
        (success_rate * 0.4) +
        (avg_latency_score * 0.2) +
        (verification_pass_rate * 0.3) +
        (recovery_required_rate_inverse * 0.1)
    )
    """

    def __init__(self, memory_store: MCPMemoryStore):
        self._memory_store = memory_store

    async def get_reliability_score(self, server_id: str, tool_name: Optional[str] = None) -> float:
        """Compute the reliability score for a server, optionally for a specific tool.

        Returns a score between 0.0 and 1.0. Defaults to 1.0 for new servers.
        """
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            # 1. Gather aggregates
            if tool_name:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*),
                           SUM(success),
                           AVG(duration_ms),
                           SUM(verification_passed),
                           SUM(recovery_attempted)
                    FROM mcp_executions
                    WHERE server_id = ? AND tool_name = ?
                    """,
                    (server_id, tool_name),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*),
                           SUM(success),
                           AVG(duration_ms),
                           SUM(verification_passed),
                           SUM(recovery_attempted)
                    FROM mcp_executions
                    WHERE server_id = ?
                    """,
                    (server_id,),
                )

            row = cursor.fetchone()
            if not row or row[0] == 0:
                return 1.0  # Return default high reliability for new/untested servers

            total_calls = row[0]
            success_calls = row[1] or 0
            avg_latency = row[2] or 0.0
            verification_passes = row[3] or 0
            recovery_attempts = row[4] or 0

            # 2. Compute components
            success_rate = success_calls / total_calls

            # Latency score: 1.0 for <= 100ms, scaling down to 0.0 at >= 10000ms (10s)
            if avg_latency <= 100:
                avg_latency_score = 1.0
            else:
                avg_latency_score = max(0.0, 1.0 - (avg_latency - 100) / 9900)

            verification_pass_rate = verification_passes / total_calls
            recovery_required_rate_inverse = 1.0 - (recovery_attempts / total_calls)

            # 3. Compute final score
            score = (
                (success_rate * 0.4) +
                (avg_latency_score * 0.2) +
                (verification_pass_rate * 0.3) +
                (recovery_required_rate_inverse * 0.1)
            )
            return round(score, 4)
        except Exception as e:
            log.warning("Failed to compute reliability score for %s: %s", server_id, e)
            return 0.5
        finally:
            conn.close()

    async def get_metrics(self, server_id: str, tool_name: Optional[str] = None) -> Dict[str, Any]:
        """Fetch raw metrics for a server/tool combination."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            if tool_name:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*), SUM(success), AVG(duration_ms), SUM(verification_passed), SUM(recovery_attempted)
                    FROM mcp_executions WHERE server_id = ? AND tool_name = ?
                    """,
                    (server_id, tool_name),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*), SUM(success), AVG(duration_ms), SUM(verification_passed), SUM(recovery_attempted)
                    FROM mcp_executions WHERE server_id = ?
                    """,
                    (server_id,),
                )
            row = cursor.fetchone()
            if not row or row[0] == 0:
                return {"total_calls": 0, "success_rate": 1.0, "avg_duration_ms": 0.0, "score": 1.0}

            total = row[0]
            success = row[1] or 0
            avg_dur = row[2] or 0.0
            score = await self.get_reliability_score(server_id, tool_name)

            return {
                "total_calls": total,
                "success_calls": success,
                "success_rate": round(success / total, 4),
                "avg_duration_ms": round(avg_dur, 2),
                "score": score,
            }
        finally:
            conn.close()
