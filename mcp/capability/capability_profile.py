"""CapabilityProfileBuilder â€” constructs typed capability profiles from raw server data."""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional
from ..registry.models import (
    CapabilityProfile,
    ToolDefinition,
    PromptDefinition,
    ResourceDefinition,
    TemplateDefinition,
)

log = logging.getLogger("aelvo.mcp.capability.profile")


class CapabilityProfileBuilder:
    """Constructs typed CapabilityProfile objects from raw capability data.

    Handles normalization, deduplication, and validation of
    tools, prompts, resources, and templates.
    """

    def build(self, server_id: str, raw_capabilities: Dict[str, Any]) -> CapabilityProfile:
        """Build a typed CapabilityProfile from raw capabilities data."""
        tools = self._build_tools(raw_capabilities.get("tools", {}))
        prompts = self._build_prompts(raw_capabilities.get("prompts", {}))
        resources = self._build_resources(raw_capabilities.get("resources", {}))
        templates = self._build_templates(raw_capabilities.get("templates", {}))

        profile = CapabilityProfile(
            server_id=server_id,
            tools=tools,
            prompts=prompts,
            resources=resources,
            templates=templates,
            protocol_version=raw_capabilities.get("protocolVersion", "unknown"),
        )

        # Compute checksum
        profile_json = profile.model_dump_json()
        profile.checksum = hashlib.sha256(profile_json.encode()).hexdigest()[:16]

        return profile

    def _build_tools(self, tools_data: Dict[str, Any]) -> List[ToolDefinition]:
        tools = []
        raw_list = tools_data.get("list", tools_data.get("tools", []))
        seen = set()
        for item in raw_list:
            name = item.get("name", "")
            if name in seen:
                continue
            seen.add(name)
            tools.append(ToolDefinition(
                name=name,
                description=item.get("description", ""),
                input_schema=item.get("inputSchema", item.get("input_schema", {})),
                output_schema=item.get("outputSchema", item.get("output_schema", {})),
                tags=item.get("tags", []),
                requires_approval=item.get("requiresApproval", item.get("requires_approval", False)),
                timeout_ms=item.get("timeoutMs", item.get("timeout_ms", 30000)),
            ))
        return tools

    def _build_prompts(self, prompts_data: Dict[str, Any]) -> List[PromptDefinition]:
        prompts = []
        raw_list = prompts_data.get("list", prompts_data.get("prompts", []))
        for item in raw_list:
            prompts.append(PromptDefinition(
                name=item.get("name", ""),
                description=item.get("description", ""),
                arguments=[
                    PromptArgument(name=a["name"], description=a.get("description", ""), required=a.get("required", False))
                    for a in item.get("arguments", [])
                ],
            ))
        return prompts

    def _build_resources(self, resources_data: Dict[str, Any]) -> List[ResourceDefinition]:
        resources = []
        raw_list = resources_data.get("list", resources_data.get("resources", []))
        for item in raw_list:
            resources.append(ResourceDefinition(
                uri=item.get("uri", ""),
                name=item.get("name", ""),
                description=item.get("description", ""),
                mime_type=item.get("mimeType", item.get("mime_type", "text/plain")),
            ))
        return resources

    def _build_templates(self, templates_data: Dict[str, Any]) -> List[TemplateDefinition]:
        templates = []
        raw_list = templates_data.get("list", templates_data.get("templates", []))
        for item in raw_list:
            templates.append(TemplateDefinition(
                uri_template=item.get("uriTemplate", item.get("uri_template", "")),
                name=item.get("name", ""),
                description=item.get("description", ""),
                mime_type=item.get("mimeType", item.get("mime_type", "text/plain")),
            ))
        return templates
