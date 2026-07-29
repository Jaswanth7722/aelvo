"""CapabilityMemory — knowledge store for MCP server capabilities (tools, prompts, resources)."""

from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any, Dict, List, Optional
from ..registry.models import CapabilityProfile, ToolDefinition
from .mcp_memory_store import MCPMemoryStore

log = logging.getLogger("aelvo.mcp.memory.capability")


class CapabilityMemory:
    """Manages persistent capability knowledge for all registered MCP servers.

    Provides indexing and query interfaces for tools, prompts, resources, and templates.
    """

    def __init__(self, memory_store: MCPMemoryStore):
        self._memory_store = memory_store

    async def store_profile(self, server_id: str, profile: CapabilityProfile) -> None:
        """Store a server's complete capability profile."""
        await self._memory_store.store_capability(server_id, profile)

    async def get_profile(self, server_id: str) -> Optional[CapabilityProfile]:
        """Retrieve a server's complete capability profile."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                "SELECT tool_name, description, input_schema, output_schema FROM mcp_capabilities WHERE server_id = ?",
                (server_id,),
            )
            rows = cursor.fetchall()
            if not rows:
                return None

            tools = []
            for row in rows:
                tools.append(
                    ToolDefinition(
                        name=row[0],
                        description=row[1] or "",
                        input_schema=json.loads(row[2] or "{}"),
                        output_schema=json.loads(row[3] or "{}"),
                    )
                )

            return CapabilityProfile(
                server_id=server_id,
                tools=tools,
                protocol_version="unknown",
            )
        except Exception as e:
            log.warning("Failed to load capability profile for %s: %s", server_id, e)
            return None
        finally:
            conn.close()

    async def find_servers_with_tool(self, tool_name: str) -> List[str]:
        """Find all server IDs that support a given tool."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                "SELECT DISTINCT server_id FROM mcp_capabilities WHERE tool_name = ?",
                (tool_name,),
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            log.warning("Failed to find servers for tool %s: %s", tool_name, e)
            return []
        finally:
            conn.close()

    async def list_all_tools(self) -> List[Dict[str, Any]]:
        """List all tools across all servers."""
        db_path = self._memory_store._db_path
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.execute(
                "SELECT server_id, tool_name, description FROM mcp_capabilities"
            )
            return [
                {"server_id": row[0], "tool_name": row[1], "description": row[2]}
                for row in cursor.fetchall()
            ]
        except Exception as e:
            log.warning("Failed to list all tools: %s", e)
            return []
        finally:
            conn.close()
