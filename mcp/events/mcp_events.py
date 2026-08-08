"""Typed event classes for all MCP Platform activity.

All events inherit from MCPEvent base and integrate with AELVO's
EventBus via the event_publisher module.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from pydantic import BaseModel, Field

from .event_schemas import DiscoverySource, DisconnectReason


class MCPEvent(BaseModel):
    """Base class for all MCP Platform events."""
    event_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    payload: Dict[str, Any] = Field(default_factory=dict)


# ------------------------------------------------------------------
# Lifecycle Events
# ------------------------------------------------------------------

class MCPServerDiscovered(MCPEvent):
    """A new MCP server was discovered by a discovery source."""
    server_id: str
    source: DiscoverySource
    server_name: str = ""


class MCPServerRegistered(MCPEvent):
    """A server was registered in the registry."""
    server_id: str
    trust_level: str


class MCPServerEnabled(MCPEvent):
    """A server was enabled (activated)."""
    server_id: str


class MCPServerDisabled(MCPEvent):
    """A server was disabled (deactivated)."""
    server_id: str
    reason: str = ""


# ------------------------------------------------------------------
# Connection Events
# ------------------------------------------------------------------

class MCPConnected(MCPEvent):
    """A connection was established to an MCP server."""
    server_id: str
    transport_type: str


class MCPDisconnected(MCPEvent):
    """A connection was terminated."""
    server_id: str
    reason: DisconnectReason


class MCPConnectionFailed(MCPEvent):
    """A connection attempt failed."""
    server_id: str
    error: str
    attempt: int


# ------------------------------------------------------------------
# Execution Events
# ------------------------------------------------------------------

class MCPToolStarted(MCPEvent):
    """An MCP tool execution has started."""
    request_id: str
    server_id: str
    tool_name: str
    specialist_id: str
    timeout_ms: int


class MCPToolCompleted(MCPEvent):
    """An MCP tool execution completed successfully."""
    request_id: str
    server_id: str
    tool_name: str
    duration_ms: int
    verification_passed: bool


class MCPToolFailed(MCPEvent):
    """An MCP tool execution failed."""
    request_id: str
    server_id: str
    tool_name: str
    failure_type: str
    recovery_attempted: bool
    error: str = ""


# ------------------------------------------------------------------
# Verification Events
# ------------------------------------------------------------------

class MCPVerificationPassed(MCPEvent):
    """MCP output passed verification."""
    request_id: str
    verifiers_run: List[str] = Field(default_factory=list)


class MCPVerificationFailed(MCPEvent):
    """MCP output failed verification."""
    request_id: str
    failing_verifier: str
    action_taken: str
    diagnostics: List[str] = Field(default_factory=list)


# ------------------------------------------------------------------
# Recovery Events
# ------------------------------------------------------------------

class MCPRecoveryStarted(MCPEvent):
    """A recovery operation has started for an MCP server."""
    server_id: str
    strategy: str
    trigger: str


class MCPRecoverySucceeded(MCPEvent):
    """A recovery operation succeeded."""
    server_id: str
    strategy: str
    attempts: int


class MCPRecoveryFailed(MCPEvent):
    """A recovery operation failed."""
    server_id: str
    strategy: str
    server_isolated: bool = False


# ------------------------------------------------------------------
# Trust & Capability Events
# ------------------------------------------------------------------

class MCPTrustChanged(MCPEvent):
    """An MCP server's trust level was changed."""
    server_id: str
    old_level: str
    new_level: str
    reason: str


class MCPCapabilityNegotiated(MCPEvent):
    """Capabilities were negotiated with a server."""
    server_id: str
    protocol_version: str
    tool_count: int
    prompt_count: int
    resource_count: int


class MCPCapabilityDriftDetected(MCPEvent):
    """A server's capabilities have changed since last negotiation."""
    server_id: str
    previous_checksum: str
    current_checksum: str
    changes: List[str] = Field(default_factory=list)
