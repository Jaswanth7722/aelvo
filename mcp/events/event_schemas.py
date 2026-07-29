"""Shared schemas, enums, and data models used across MCP event definitions."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class DiscoverySource(str, Enum):
    """Sources that can discover MCP servers."""
    MANUAL = "manual"
    CONFIG = "config"
    FILESYSTEM = "filesystem"
    RUNTIME = "runtime"
    MDNS = "mdns"
    REGISTRY_QUERY = "registry_query"


class DisconnectReason(str, Enum):
    """Reasons for MCP server disconnection."""
    GRACEFUL = "graceful"
    TIMEOUT = "timeout"
    ERROR = "error"
    TRUST_VIOLATION = "trust_violation"
    SERVER_TERMINATED = "server_terminated"
    ADMIN_DISABLED = "admin_disabled"
    TRANSPORT_FAILURE = "transport_failure"
    UNKNOWN = "unknown"


class FailureType(str, Enum):
    """Types of MCP execution failures."""
    CONNECTION_LOST = "connection_lost"
    TRANSIENT_ERROR = "transient_error"
    TIMEOUT = "timeout"
    SCHEMA_VIOLATION = "schema_violation"
    TRUST_VIOLATION = "trust_violation"
    CAPABILITY_MISMATCH = "capability_mismatch"
    TOOL_NOT_FOUND = "tool_not_found"
    INVALID_ARGUMENTS = "invalid_arguments"
    PERMISSION_DENIED = "permission_denied"
    SERVER_ERROR = "server_error"
    RECOVERY_FAILED = "recovery_failed"
    UNKNOWN = "unknown"


class ExecutionPriority(str, Enum):
    """Priority levels for MCP execution requests."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class CapabilityQueryType(str, Enum):
    """Types of capability queries that can be made."""
    ALL_TOOLS = "all_tools"
    TOOL_BY_NAME = "tool_by_name"
    TOOLS_BY_TASK = "tools_by_task"
    ALL_PROMPTS = "all_prompts"
    ALL_RESOURCES = "all_resources"
    SERVER_CAPABILITIES = "server_capabilities"
    CAPABILITY_GAPS = "capability_gaps"
    ALTERNATIVES_FOR_TOOL = "alternatives_for_tool"


class VerificationAction(str, Enum):
    """Actions that can result from verification."""
    PASS = "pass"
    WARN = "warn"
    BLOCK = "block"
    QUARANTINE = "quarantine"
