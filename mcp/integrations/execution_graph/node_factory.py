"""Node factory for instantiating MCP ExecutionGraph nodes."""

from __future__ import annotations

from typing import Any, Dict
from .mcp_nodes import MCPToolNode, MCPCapabilityQueryNode, MCPServerHealthNode
from ...events.event_schemas import CapabilityQueryType
from ...registry.models import HealthState


class MCPNodeFactory:
    """Factory to instantiate MCP execution graph nodes with proper defaults."""

    @staticmethod
    def create_tool_node(
        node_id: str,
        server_id: str,
        tool_name: str,
        arguments_template: Dict[str, Any],
        description: str = "",
        **kwargs,
    ) -> MCPToolNode:
        """Create an MCPToolNode instance."""
        return MCPToolNode(
            id=node_id,
            server_id=server_id,
            tool_name=tool_name,
            arguments_template=arguments_template,
            description=description or f"Call tool {tool_name} on {server_id}",
            **kwargs,
        )

    @staticmethod
    def create_capability_query_node(
        node_id: str,
        query_type: CapabilityQueryType,
        description: str = "",
        **kwargs,
    ) -> MCPCapabilityQueryNode:
        """Create an MCPCapabilityQueryNode instance."""
        return MCPCapabilityQueryNode(
            id=node_id,
            query_type=query_type,
            description=description or f"Query capabilities: {query_type.value}",
            **kwargs,
        )

    @staticmethod
    def create_health_node(
        node_id: str,
        server_id: str,
        required_health: HealthState = HealthState.HEALTHY,
        description: str = "",
        **kwargs,
    ) -> MCPServerHealthNode:
        """Create an MCPServerHealthNode instance."""
        return MCPServerHealthNode(
            id=node_id,
            server_id=server_id,
            required_health=required_health,
            description=description or f"Check health state of {server_id}",
            **kwargs,
        )
