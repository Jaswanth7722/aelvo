"""PermissionModel — permission definitions for specialist access to MCP tools."""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger("aelvo.mcp.governance.permissions")


@dataclass
class MCPPermission:
    """A single permission rule for specialist MCP access."""
    specialist_id: str
    server_id: Optional[str] = None       # None = wildcard (all servers)
    tool_pattern: str = "*"               # Glob or regex pattern
    allowed: bool = True
    requires_approval: bool = False
    conditions: List[dict] = field(default_factory=list)
    expires_at: Optional[datetime] = None
    details: Dict[str, Any] = field(default_factory=dict)
    reason: str = ""


@dataclass
class PermissionCheckResult:
    """Result of a permission check."""
    allowed: bool
    reason: str = ""
    requires_approval: bool = False
    matching_rule: Optional[MCPPermission] = None
    details: Dict[str, Any] = field(default_factory=dict)


class PermissionModel:
    """Manages MCP tool permissions for all specialists.

    Permissions are checked in order: most specific rules take priority.
    Deny rules override allow rules for the same scope.
    """

    def __init__(self):
        self._permissions: List[MCPPermission] = []
        self._load_defaults()

    def _load_defaults(self) -> None:
        """Load default permission rules."""
        self._permissions = [
            # HERMES — user context servers, read-only
            MCPPermission(specialist_id="HERMES", server_id=None, tool_pattern="*read*", allowed=True,
                          reason="HERMES: read-only access to user context"),
            MCPPermission(specialist_id="HERMES", server_id=None, tool_pattern="*write*", allowed=True,
                          requires_approval=True,
                          reason="HERMES: write requires approval (trust_level >= TRUSTED)"),

            # ARCHITECT — planning resources, read-only
            MCPPermission(specialist_id="ARCHITECT", server_id=None, tool_pattern="*read*", allowed=True,
                          reason="ARCHITECT: read planning resources"),
            MCPPermission(specialist_id="ARCHITECT", server_id=None, tool_pattern="*query*", allowed=True,
                          reason="ARCHITECT: query capability graphs"),

            # ORACLE — full read access
            MCPPermission(specialist_id="ORACLE", server_id=None, tool_pattern="*read*", allowed=True,
                          reason="ORACLE: read documentation/research"),
            MCPPermission(specialist_id="ORACLE", server_id=None, tool_pattern="*search*", allowed=True,
                          reason="ORACLE: search knowledge servers"),

            # FORGE — repository tools, code generation
            MCPPermission(specialist_id="FORGE", server_id=None, tool_pattern="*read*", allowed=True,
                          reason="FORGE: read repository"),
            MCPPermission(specialist_id="FORGE", server_id=None, tool_pattern="*write*", allowed=True,
                          requires_approval=True,
                          reason="FORGE: write requires governance approval"),
            MCPPermission(specialist_id="FORGE", server_id=None, tool_pattern="*generate*", allowed=True,
                          requires_approval=True,
                          reason="FORGE: code generation requires governance"),

            # SENTINEL — trust inspection, read-only metadata
            MCPPermission(specialist_id="SENTINEL", server_id=None, tool_pattern="*inspect*", allowed=True,
                          reason="SENTINEL: inspect trust boundaries"),
            MCPPermission(specialist_id="SENTINEL", server_id=None, tool_pattern="*query*", allowed=True,
                          reason="SENTINEL: query permissions"),

            # TERMINUS — server lifecycle management
            MCPPermission(specialist_id="TERMINUS", server_id=None, tool_pattern="*lifecycle*", allowed=True,
                          reason="TERMINUS: server lifecycle management"),
            MCPPermission(specialist_id="TERMINUS", server_id=None, tool_pattern="*isolate*", allowed=True,
                          reason="TERMINUS: server isolation"),
            MCPPermission(specialist_id="TERMINUS", server_id=None, tool_pattern="*downgrade*", allowed=True,
                          reason="TERMINUS: trust downgrade"),

            # HERALD — read-only audit access
            MCPPermission(specialist_id="HERALD", server_id=None, tool_pattern="*read*", allowed=True,
                          reason="HERALD: read audit logs"),
            MCPPermission(specialist_id="HERALD", server_id=None, tool_pattern="*report*", allowed=True,
                          reason="HERALD: generate reports"),
        ]

    def check(self, specialist_id: str, server_id: str, tool_name: str) -> PermissionCheckResult:
        """Check if a specialist has permission for a tool on a server."""
        matching = []

        for perm in self._permissions:
            if perm.specialist_id != specialist_id:
                continue
            if perm.server_id is not None and perm.server_id != server_id:
                continue
            if not self._match_tool(tool_name, perm.tool_pattern):
                continue
            matching.append(perm)

        # Exact server matches take priority
        exact = [p for p in matching if p.server_id == server_id]
        if exact:
            matching = exact

        if not matching:
            return PermissionCheckResult(
                allowed=False,
                reason=f"No permission rule for specialist '{specialist_id}' on tool '{tool_name}'",
            )

        # First matching rule wins (deny can override allow for same scope)
        for perm in matching:
            if not perm.allowed:
                return PermissionCheckResult(
                    allowed=False,
                    reason=perm.reason or f"Denied by permission rule",
                    matching_rule=perm,
                )

        # First allow rule
        first = matching[0]
        return PermissionCheckResult(
            allowed=True,
            reason=first.reason or "Permission granted",
            requires_approval=first.requires_approval,
            matching_rule=first,
            details={"tool_pattern": first.tool_pattern, "server": first.server_id},
        )

    def add_permission(self, permission: MCPPermission) -> None:
        """Add a custom permission rule."""
        self._permissions.append(permission)

    def remove_permission(self, specialist_id: str, tool_pattern: str) -> bool:
        """Remove permission rules matching criteria."""
        before = len(self._permissions)
        self._permissions = [
            p for p in self._permissions
            if not (p.specialist_id == specialist_id and p.tool_pattern == tool_pattern)
        ]
        return len(self._permissions) < before

    @staticmethod
    def _match_tool(tool_name: str, pattern: str) -> bool:
        """Match a tool name against a pattern (supports * wildcards)."""
        if pattern == "*":
            return True
        if "*" in pattern:
            regex = "^" + re.escape(pattern).replace("\\*", ".*") + "$"
            return bool(re.match(regex, tool_name))
        return tool_name == pattern

    def list_permissions(self) -> List[dict]:
        return [
            {"specialist_id": p.specialist_id, "server_id": p.server_id,
             "tool_pattern": p.tool_pattern, "allowed": p.allowed,
             "requires_approval": p.requires_approval}
            for p in self._permissions
        ]
