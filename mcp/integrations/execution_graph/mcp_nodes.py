"""MCP nodes for AELVO Omega ExecutionGraph integration."""

from __future__ import annotations

from typing import Any, Dict
from pydantic import Field

from runtime_next.models.plan import ExecutionNode, NodeType
from ...events.event_schemas import CapabilityQueryType
from ...registry.models import HealthState


class MCPToolNode(ExecutionNode):
    """Executes a single MCP tool call."""

    server_id: str
    tool_name: str
    arguments_template: Dict[str, Any] = Field(default_factory=dict)
    node_type: NodeType = NodeType.TOOL_CALL


class MCPCapabilityQueryNode(ExecutionNode):
    """Queries available capabilities without executing."""

    query_type: CapabilityQueryType
    node_type: NodeType = NodeType.MEMORY_QUERY


class MCPServerHealthNode(ExecutionNode):
    """Checks server health before execution branches."""

    server_id: str
    required_health: HealthState
    node_type: NodeType = NodeType.DECISION
