"""MCPMemoryStore — persists MCP execution results, capability knowledge, and reliability data."""

from __future__ import annotations

import json
import logging
import sqlite3
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..execution.execution_result import MCPExecutionResult
from ..registry.models import CapabilityProfile

log = logging.getLogger("aelvo.mcp.memory.store")

DEFAULT_MCP_DB = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "mcp_memory.db")


class MCPMemoryStore:
    """Persistent memory store for MCP infrastructure data.

    Stores:
    - Execution results and outcomes
    - Capability knowledge (what each server can do)
    - Reliability scores per server/tool
    - Failure history
    - Routing intelligence
    """

    def __init__(self, db_path: str = DEFAULT_MCP_DB):
        self._db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS mcp_executions (
                    id TEXT PRIMARY KEY,
                    specialist_id TEXT,
                    server_id TEXT,
                    tool_name TEXT,
                    success INTEGER,
                    error TEXT,
                    duration_ms INTEGER,
                    verification_passed INTEGER,
                    governance_passed INTEGER,
                    recovery_attempted INTEGER,
                    recovery_successful INTEGER,
                    trust_level TEXT,
                    timestamp TEXT
                );
                CREATE TABLE IF NOT EXISTS mcp_capabilities (
                    server_id TEXT,
                    tool_name TEXT,
                    description TEXT,
                    input_schema TEXT,
                    output_schema TEXT,
                    discovered_at TEXT,
                    PRIMARY KEY (server_id, tool_name)
                );
                CREATE TABLE IF NOT EXISTS mcp_reliability (
                    server_id TEXT,
                    tool_name TEXT,
                    total_calls INTEGER DEFAULT 0,
                    success_calls INTEGER DEFAULT 0,
                    failed_calls INTEGER DEFAULT 0,
                    total_duration_ms INTEGER DEFAULT 0,
                    last_seen TEXT,
                    PRIMARY KEY (server_id, tool_name)
                );
                CREATE TABLE IF NOT EXISTS mcp_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    server_id TEXT,
                    tool_name TEXT,
                    failure_type TEXT,
                    error TEXT,
                    recovery_attempted INTEGER,
                    recovery_successful INTEGER,
                    timestamp TEXT
                );
                CREATE TABLE IF NOT EXISTS mcp_routing (
                    specialist_id TEXT,
                    server_id TEXT,
                    tool_name TEXT,
                    preference_score REAL DEFAULT 0.5,
                    total_calls INTEGER DEFAULT 0,
                    success_calls INTEGER DEFAULT 0,
                    last_routed TEXT,
                    PRIMARY KEY (specialist_id, server_id, tool_name)
                );
            """)
            conn.commit()
        finally:
            conn.close()

    async def store_result(self, result: MCPExecutionResult) -> None:
        """Persist an execution result."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO mcp_executions
                    (id, specialist_id, server_id, tool_name, success, error,
                     duration_ms, verification_passed, governance_passed,
                     recovery_attempted, recovery_successful, trust_level, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result.request_id,
                result.specialist_id,
                result.server_id,
                result.tool_name,
                1 if result.success else 0,
                result.error or "",
                result.duration_ms,
                1 if result.verification_passed else 0,
                1 if result.governance_passed else 0,
                1 if result.recovery_attempted else 0,
                1 if result.recovery_successful else 0,
                result.trust_level_at_execution,
                datetime.now(timezone.utc).isoformat(),
            ))
            conn.commit()
        finally:
            conn.close()

        # Update reliability tracking
        await self._update_reliability(result.server_id, result.tool_name, result.success, result.duration_ms)

    async def store_capability(self, server_id: str, profile: CapabilityProfile) -> None:
        """Persist server capabilities."""
        conn = sqlite3.connect(self._db_path)
        try:
            for tool in profile.tools:
                conn.execute("""
                    INSERT OR REPLACE INTO mcp_capabilities
                        (server_id, tool_name, description, input_schema, output_schema, discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    server_id,
                    tool.name,
                    tool.description,
                    json.dumps(tool.input_schema),
                    json.dumps(tool.output_schema),
                    datetime.now(timezone.utc).isoformat(),
                ))
            conn.commit()
        finally:
            conn.close()

    async def store_failure(self, server_id: str, tool_name: str, failure_type: str,
                             error: str, recovery_attempted: bool, recovery_successful: bool) -> None:
        """Record a failure event."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                INSERT INTO mcp_failures
                    (server_id, tool_name, failure_type, error, recovery_attempted,
                     recovery_successful, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                server_id, tool_name, failure_type, error,
                1 if recovery_attempted else 0, 1 if recovery_successful else 0,
                datetime.now(timezone.utc).isoformat(),
            ))
            conn.commit()
        finally:
            conn.close()

    async def get_reliability(self, server_id: str, tool_name: Optional[str] = None) -> Dict[str, Any]:
        """Get reliability score for a server/tool combination."""
        conn = sqlite3.connect(self._db_path)
        try:
            if tool_name:
                cursor = conn.execute(
                    "SELECT * FROM mcp_reliability WHERE server_id = ? AND tool_name = ?",
                    (server_id, tool_name),
                )
            else:
                cursor = conn.execute(
                    "SELECT server_id, SUM(total_calls), SUM(success_calls), SUM(failed_calls) "
                    "FROM mcp_reliability WHERE server_id = ? GROUP BY server_id",
                    (server_id,),
                )
            row = cursor.fetchone()
            if row:
                return {
                    "server_id": row[0],
                    "total_calls": row[1] if len(row) > 1 else 0,
                    "success_calls": row[2] if len(row) > 2 else 0,
                    "failed_calls": row[3] if len(row) > 3 else 0,
                }
            return {"server_id": server_id, "total_calls": 0}
        finally:
            conn.close()

    async def _update_reliability(self, server_id: str, tool_name: str, success: bool, duration_ms: int) -> None:
        """Update reliability tracking for a server/tool."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                INSERT INTO mcp_reliability
                    (server_id, tool_name, total_calls, success_calls, failed_calls,
                     total_duration_ms, last_seen)
                VALUES (?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(server_id, tool_name) DO UPDATE SET
                    total_calls = total_calls + 1,
                    success_calls = success_calls + ?,
                    failed_calls = failed_calls + ?,
                    total_duration_ms = total_duration_ms + ?,
                    last_seen = ?
            """, (
                server_id, tool_name,
                1 if success else 0, 0 if success else 1,
                duration_ms, datetime.now(timezone.utc).isoformat(),
                1 if success else 0, 0 if success else 1,
                duration_ms, datetime.now(timezone.utc).isoformat(),
            ))
            conn.commit()
        finally:
            conn.close()
