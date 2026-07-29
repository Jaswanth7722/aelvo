"""Persistence layer for the MCP Server Registry.

Uses the existing AELVO SQLite-based persistence pattern
(compatible with MemoryEngine's DB approach).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from .models import (
    MCPServerRecord,
    TrustLevel,
    HealthState,
    TransportType,
    CapabilityProfile,
    ToolDefinition,
    PromptDefinition,
    ResourceDefinition,
    TemplateDefinition,
)

log = logging.getLogger("aelvo.mcp.registry.store")

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "mcp_registry.db")


class RegistryStore:
    """SQLite-backed persistence for MCP server records."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self._db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the SQLite schema."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS mcp_servers (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    transport_type TEXT NOT NULL,
                    connection_config TEXT DEFAULT '{}',
                    trust_level TEXT NOT NULL DEFAULT 'sandboxed',
                    enabled INTEGER DEFAULT 1,
                    health_state TEXT DEFAULT 'unknown',
                    capabilities_json TEXT DEFAULT '{}',
                    registered_at TEXT NOT NULL,
                    last_seen TEXT,
                    tags_json TEXT DEFAULT '[]',
                    metadata_json TEXT DEFAULT '{}'
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def load_all(self) -> List[MCPServerRecord]:
        """Load all server records from the database."""
        conn = sqlite3.connect(self._db_path)
        try:
            cursor = conn.execute("SELECT * FROM mcp_servers")
            records = []
            for row in cursor.fetchall():
                record = self._row_to_record(row)
                if record:
                    records.append(record)
            return records
        finally:
            conn.close()

    def save_all(self, records: List[MCPServerRecord]) -> None:
        """Save all server records to the database (full sync)."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("BEGIN")
            conn.execute("DELETE FROM mcp_servers")
            for record in records:
                self._insert_record(conn, record)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_one(self, record: MCPServerRecord) -> None:
        """Save a single server record (upsert)."""
        conn = sqlite3.connect(self._db_path)
        try:
            self._insert_record(conn, record, upsert=True)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def delete_one(self, server_id: str) -> None:
        """Delete a single server record."""
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("DELETE FROM mcp_servers WHERE id = ?", (server_id,))
            conn.commit()
        finally:
            conn.close()

    def _insert_record(self, conn: sqlite3.Connection, record: MCPServerRecord, upsert: bool = False) -> None:
        """Insert or upsert a server record."""
        sql = """
            INSERT OR REPLACE INTO mcp_servers
                (id, name, description, transport_type, connection_config,
                 trust_level, enabled, health_state, capabilities_json,
                 registered_at, last_seen, tags_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """ if upsert else """
            INSERT INTO mcp_servers
                (id, name, description, transport_type, connection_config,
                 trust_level, enabled, health_state, capabilities_json,
                 registered_at, last_seen, tags_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        conn.execute(sql, (
            record.id,
            record.name,
            record.description,
            record.transport_type.value,
            json.dumps(record.connection_config),
            record.trust_level.value,
            1 if record.enabled else 0,
            record.health_state.value,
            self._capabilities_to_json(record.capabilities),
            record.registered_at.isoformat() if record.registered_at else "",
            record.last_seen.isoformat() if record.last_seen else None,
            json.dumps(record.tags),
            json.dumps(record.metadata),
        ))

    def _row_to_record(self, row) -> Optional[MCPServerRecord]:
        """Convert a database row to an MCPServerRecord."""
        try:
            # Row format: (id, name, description, transport_type, connection_config,
            #              trust_level, enabled, health_state, capabilities_json,
            #              registered_at, last_seen, tags_json, metadata_json)
            return MCPServerRecord(
                id=row[0],
                name=row[1],
                description=row[2] or "",
                transport_type=TransportType(row[3]),
                connection_config=json.loads(row[4] or "{}"),
                trust_level=TrustLevel(row[5]),
                enabled=bool(row[6]),
                health_state=HealthState(row[7]),
                capabilities=self._json_to_capabilities(row[8] or "{}", server_id=row[0]),
                registered_at=datetime.fromisoformat(row[9]) if row[9] else datetime.now(timezone.utc),
                last_seen=datetime.fromisoformat(row[10]) if row[10] else None,
                tags=json.loads(row[11] or "[]"),
                metadata=json.loads(row[12] or "{}"),
            )
        except Exception as e:
            log.warning("Failed to deserialize server record %s: %s", row[0] if row else "?", e)
            return None

    @staticmethod
    def _capabilities_to_json(profile: CapabilityProfile) -> str:
        """Serialize capability profile to JSON."""
        return json.dumps({
            "server_id": profile.server_id,
            "protocol_version": profile.protocol_version,
            "negotiated_at": profile.negotiated_at.isoformat() if profile.negotiated_at else None,
            "checksum": profile.checksum,
            "tools": [
                {"name": t.name, "description": t.description, "input_schema": t.input_schema,
                 "output_schema": t.output_schema, "tags": t.tags,
                 "requires_approval": t.requires_approval, "timeout_ms": t.timeout_ms}
                for t in profile.tools
            ],
            "prompts": [
                {"name": p.name, "description": p.description,
                 "arguments": [{"name": a.name, "description": a.description, "required": a.required}
                               for a in p.arguments]}
                for p in profile.prompts
            ],
            "resources": [
                {"uri": r.uri, "name": r.name, "description": r.description, "mime_type": r.mime_type}
                for r in profile.resources
            ],
            "templates": [
                {"uri_template": t.uri_template, "name": t.name, "description": t.description,
                 "mime_type": t.mime_type}
                for t in profile.templates
            ],
        })

    @staticmethod
    def _json_to_capabilities(json_str: str, server_id: str) -> CapabilityProfile:
        """Deserialize capability profile from JSON."""
        try:
            data = json.loads(json_str)
            tools = [
                ToolDefinition(
                    name=t["name"],
                    description=t.get("description", ""),
                    input_schema=t.get("input_schema", {}),
                    output_schema=t.get("output_schema", {}),
                    tags=t.get("tags", []),
                    requires_approval=t.get("requires_approval", False),
                    timeout_ms=t.get("timeout_ms", 30000),
                )
                for t in data.get("tools", [])
            ]
            prompts = [
                PromptDefinition(
                    name=p["name"],
                    description=p.get("description", ""),
                    arguments=[
                        PromptArgument(name=a["name"], description=a.get("description", ""),
                                       required=a.get("required", False))
                        for a in p.get("arguments", [])
                    ],
                )
                for p in data.get("prompts", [])
            ]
            resources = [
                ResourceDefinition(
                    uri=r["uri"],
                    name=r.get("name", ""),
                    description=r.get("description", ""),
                    mime_type=r.get("mime_type", "text/plain"),
                )
                for r in data.get("resources", [])
            ]
            templates = [
                TemplateDefinition(
                    uri_template=t["uri_template"],
                    name=t.get("name", ""),
                    description=t.get("description", ""),
                    mime_type=t.get("mime_type", "text/plain"),
                )
                for t in data.get("templates", [])
            ]
            return CapabilityProfile(
                server_id=server_id,
                tools=tools,
                prompts=prompts,
                resources=resources,
                templates=templates,
                protocol_version=data.get("protocol_version", "unknown"),
                checksum=data.get("checksum", ""),
            )
        except Exception:
            return CapabilityProfile(server_id=server_id)
