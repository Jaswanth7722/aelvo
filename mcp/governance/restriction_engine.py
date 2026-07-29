"""RestrictionEngine — inspects MCP tool arguments for restricted patterns."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.governance.restrictions")


class RestrictionEngine:
    """Inspects MCP execution arguments for restricted patterns.

    Detects:
    - Dangerous file paths
    - Command injection patterns
    - Data exfiltration patterns
    - Other restricted argument values
    """

    def __init__(self):
        self._restricted_patterns: List[Dict[str, Any]] = []

    async def inspect(self, request: MCPExecutionRequest) -> Dict[str, Any]:
        """Inspect a request's arguments for restricted patterns."""
        issues = []

        for pattern in self._restricted_patterns:
            if not self._pattern_matches(request, pattern):
                continue
            issues.append(pattern.get("message", "Restricted pattern detected"))

        return {
            "blocked": len(issues) > 0 and any(p.get("action") == "block" for p in self._restricted_patterns),
            "issues": issues,
            "requires_approval": any(p.get("requires_approval") for p in self._restricted_patterns),
            "reason": "; ".join(issues) if issues else "",
        }

    def add_pattern(self, pattern: str, field: str = "arguments",
                    message: str = "Restricted pattern",
                    action: str = "block",
                    requires_approval: bool = False) -> None:
        """Add a restricted pattern to check.

        Args:
            pattern: Regex pattern to match.
            field: Argument field to check.
            message: Message to return on match.
            action: 'block' | 'warn'
            requires_approval: Whether match requires approval.
        """
        self._restricted_patterns.append({
            "pattern": pattern,
            "field": field,
            "message": message,
            "action": action,
            "requires_approval": requires_approval,
        })

    @staticmethod
    def _pattern_matches(request: MCPExecutionRequest, pattern: Dict[str, Any]) -> bool:
        """Check if any argument matches a restricted pattern."""
        field_path = pattern.get("field", "arguments")
        regex = pattern.get("pattern", "")
        if not regex:
            return False

        args = request.arguments
        for key, value in _resolve_field(args, field_path).items():
            if isinstance(value, str) and re.search(regex, value, re.IGNORECASE):
                return True
        return False


def _resolve_field(data: dict, path: str) -> dict:
    """Resolve a dot-separated field path into a dict."""
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return {}
    return current
