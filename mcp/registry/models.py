"""Data models for the MCP Registry subsystem."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel, Field
from datetime import datetime


class TransportType(str, Enum):
    """Supported MCP transport protocols."""
    STDIO = "stdio"
    WEBSOCKET = "websocket"
    HTTP = "http"


class TrustLevel(str, Enum):
    """Trust classification for MCP servers."""
    TRUSTED = "trusted"          # Vetted, known server — full capability access
    VERIFIED = "verified"        # Validated structure, unknown origin — scoped access
    SANDBOXED = "sandboxed"      # Unknown origin — read-only, no side effects
    QUARANTINED = "quarantined"  # Failed verification — no execution, inspection only
    BLOCKED = "blocked"          # Explicitly denied — no connection


class HealthState(str, Enum):
    """Health state of an MCP server connection."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNREACHABLE = "unreachable"
    UNKNOWN = "unknown"


class CapabilityProfile(BaseModel):
    """Discovered capabilities for a single MCP server."""
    server_id: str
    tools: List[ToolDefinition] = Field(default_factory=list)
    prompts: List[PromptDefinition] = Field(default_factory=list)
    resources: List[ResourceDefinition] = Field(default_factory=list)
    templates: List[TemplateDefinition] = Field(default_factory=list)
    protocol_version: str = "unknown"
    negotiated_at: Optional[datetime] = None
    checksum: str = ""


class ToolDefinition(BaseModel):
    """Definition of an MCP tool."""
    name: str
    description: str = ""
    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    requires_approval: bool = False
    timeout_ms: int = 30000


class PromptDefinition(BaseModel):
    """Definition of an MCP prompt template."""
    name: str
    description: str = ""
    arguments: List[PromptArgument] = Field(default_factory=list)


class PromptArgument(BaseModel):
    """Argument definition for an MCP prompt."""
    name: str
    description: str = ""
    required: bool = False


class ResourceDefinition(BaseModel):
    """Definition of an MCP resource."""
    uri: str
    name: str = ""
    description: str = ""
    mime_type: str = "text/plain"


class TemplateDefinition(BaseModel):
    """Definition of an MCP resource template."""
    uri_template: str
    name: str = ""
    description: str = ""
    mime_type: str = "text/plain"


class MCPServerConfig(BaseModel):
    """Configuration for registering an MCP server."""
    id: str = Field(..., description="Unique stable identifier")
    name: str = Field(..., description="Human-readable name")
    description: str = ""
    transport_type: TransportType = TransportType.STDIO
    connection_config: Dict[str, Any] = Field(default_factory=dict)
    trust_level: TrustLevel = TrustLevel.SANDBOXED
    enabled: bool = True
    auto_connect: bool = False
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    timeout_ms: int = 30000
    max_concurrent: int = 5


class MCPServerRecord(BaseModel):
    """Persistent record for a registered MCP server."""
    id: str
    name: str
    description: str = ""
    transport_type: TransportType
    connection_config: Dict[str, Any] = Field(default_factory=dict)
    trust_level: TrustLevel = TrustLevel.SANDBOXED
    enabled: bool = True
    health_state: HealthState = HealthState.UNKNOWN
    capabilities: CapabilityProfile = Field(default_factory=lambda: CapabilityProfile(server_id=""))
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    last_seen: Optional[datetime] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ServerRegistryFilter(BaseModel):
    """Filter criteria for querying the server registry."""
    trust_level: Optional[TrustLevel] = None
    health_state: Optional[HealthState] = None
    transport_type: Optional[TransportType] = None
    enabled: Optional[bool] = None
    tag: Optional[str] = None
    capability_tool: Optional[str] = None
