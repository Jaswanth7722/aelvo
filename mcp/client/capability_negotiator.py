"""CapabilityNegotiator â€” MCP protocol version and capability negotiation.

Handles the initial handshake with MCP servers to negotiate protocol
version, supported capabilities (tools, prompts, resources), and
detect capability drift over time.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional

from ..transport.base_transport import BaseTransport, MCPMessage
from ..registry.models import (
    CapabilityProfile,
    ToolDefinition,
    PromptDefinition,
    PromptArgument,
    ResourceDefinition,
)
from ..events.event_publisher import MCPEventPublisher
from ..events.mcp_events import MCPCapabilityNegotiated

log = logging.getLogger("aelvo.mcp.client.negotiation")

MCP_PROTOCOL_VERSIONS = ["2025-03-26", "2024-11-05"]


class CapabilityNegotiator:
    """Handles MCP protocol capability negotiation.

    Performs the initialize/handshake sequence defined in the MCP spec
    and parses server capabilities into typed data models.
    """

    def __init__(self, event_publisher: Optional[MCPEventPublisher] = None):
        self._event_publisher = event_publisher

    async def negotiate(self, server_id: str, transport: BaseTransport) -> CapabilityProfile:
        """Perform capability negotiation with an MCP server.

        Sends an initialize request and parses the server's capabilities.

        Args:
            server_id: The server identifier.
            transport: The connected transport.

        Returns:
            A populated CapabilityProfile.

        Raises:
            NegotiationError: If negotiation fails.
        """
        try:
            # Step 1: Send initialize request
            init_msg = MCPMessage(
                id=f"{server_id}:init:1",
                method="initialize",
                params={
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "aelvo-omega",
                        "version": "1.0.0",
                    },
                },
            )
            await transport.send(init_msg)

            # Step 2: Wait for response
            response = None
            async for msg in transport.receive():
                if msg.id == init_msg.id:
                    response = msg
                    break

            if response is None:
                raise NegotiationError(f"No response to initialize request from {server_id}")

            if response.error:
                raise NegotiationError(f"Initialize error from {server_id}: {response.error}")

            # Step 3: Parse capabilities
            capabilities = self._parse_initialize_result(server_id, response.result or {})

            # Step 4: Send initialized notification
            notif = MCPMessage(
                id=f"{server_id}:init:2",
                method="notifications/initialized",
                params={},
            )
            await transport.send(notif)

            # Step 5: List tools if available
            if capabilities.tools:
                tools = await self._list_tools(server_id, transport)
                capabilities.tools = tools

            # Step 6: Compute checksum
            profile_json = capabilities.model_dump_json()
            capabilities.checksum = hashlib.sha256(profile_json.encode()).hexdigest()[:16]

            log.info(
                "CapabilityNegotiator: negotiated with '%s' â€” "
                "%d tools, %d prompts, %d resources (proto: %s)",
                server_id,
                len(capabilities.tools),
                len(capabilities.prompts),
                len(capabilities.resources),
                capabilities.protocol_version,
            )

            if self._event_publisher:
                await self._event_publisher.publish(
                    MCPCapabilityNegotiated(
                        event_id=f"cap_neg_{server_id}",
                        server_id=server_id,
                        protocol_version=capabilities.protocol_version,
                        tool_count=len(capabilities.tools),
                        prompt_count=len(capabilities.prompts),
                        resource_count=len(capabilities.resources),
                    )
                )

            return capabilities

        except Exception as e:
            raise NegotiationError(f"Capability negotiation failed for {server_id}: {e}") from e

    # ------------------------------------------------------------------
    # Internal Parsing
    # ------------------------------------------------------------------

    def _parse_initialize_result(self, server_id: str, result: Dict[str, Any]) -> CapabilityProfile:
        """Parse the initialize result into a CapabilityProfile."""
        proto = result.get("protocolVersion", MCP_PROTOCOL_VERSIONS[-1])
        server_caps = result.get("capabilities", {})

        tools = self._parse_tool_capabilities(server_caps.get("tools", {}))
        prompts = self._parse_prompt_capabilities(server_caps.get("prompts", {}))
        resources = self._parse_resource_capabilities(server_caps.get("resources", {}))

        return CapabilityProfile(
            server_id=server_id,
            tools=tools,
            prompts=prompts,
            resources=resources,
            protocol_version=proto,
        )

    def _parse_tool_capabilities(self, tool_caps: Dict[str, Any]) -> List[ToolDefinition]:
        """Parse tool capabilities from the server."""
        tools = []
        for item in tool_caps.get("tools", tool_caps.get("list", [])):
            tools.append(ToolDefinition(
                name=item.get("name", "unknown"),
                description=item.get("description", ""),
                input_schema=item.get("inputSchema", {}),
                output_schema=item.get("outputSchema", {}),
                tags=item.get("tags", []),
                requires_approval=item.get("requiresApproval", False),
                timeout_ms=item.get("timeoutMs", 30000),
            ))
        return tools

    def _parse_prompt_capabilities(self, prompt_caps: Dict[str, Any]) -> List[PromptDefinition]:
        """Parse prompt capabilities from the server."""
        prompts = []
        for item in prompt_caps.get("prompts", prompt_caps.get("list", [])):
            prompts.append(PromptDefinition(
                name=item.get("name", "unknown"),
                description=item.get("description", ""),
                arguments=[
                    PromptArgument(
                        name=a["name"],
                        description=a.get("description", ""),
                        required=a.get("required", False),
                    )
                    for a in item.get("arguments", [])
                ],
            ))
        return prompts

    def _parse_resource_capabilities(self, resource_caps: Dict[str, Any]) -> List[ResourceDefinition]:
        """Parse resource capabilities from the server."""
        resources = []
        for item in resource_caps.get("resources", resource_caps.get("list", [])):
            resources.append(ResourceDefinition(
                uri=item.get("uri", ""),
                name=item.get("name", ""),
                description=item.get("description", ""),
                mime_type=item.get("mimeType", "text/plain"),
            ))
        return resources

    # ------------------------------------------------------------------
    # Tool Listing
    # ------------------------------------------------------------------

    async def _list_tools(self, server_id: str, transport: BaseTransport) -> List[ToolDefinition]:
        """List available tools from the server."""
        list_msg = MCPMessage(
            id=f"{server_id}:list_tools:1",
            method="tools/list",
            params={},
        )
        await transport.send(list_msg)

        async for msg in transport.receive():
            if msg.id == list_msg.id:
                if msg.result:
                    raw_tools = msg.result.get("tools", [])
                    return [
                        ToolDefinition(
                            name=t.get("name", "unknown"),
                            description=t.get("description", ""),
                            input_schema=t.get("inputSchema", {}),
                            output_schema=t.get("outputSchema", {}),
                            tags=t.get("tags", []),
                            requires_approval=t.get("requiresApproval", False),
                            timeout_ms=t.get("timeoutMs", 30000),
                        )
                        for t in raw_tools
                    ]
                break

        return []


class NegotiationError(Exception):
    """Raised when MCP capability negotiation fails."""
    pass
