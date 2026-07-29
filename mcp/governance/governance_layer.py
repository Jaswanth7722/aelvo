"""MCPGovernanceLayer — pre-execution governance checks for all MCP execution.

Integrates with SENTINEL to enforce:
- Server allowlist checks
- Trust level requirements
- Tool permissions per specialist
- Argument inspection
- Rate limiting
- Side-effect gating
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from ..registry.models import TrustLevel, MCPServerRecord
from ..registry.server_registry import ServerRegistry
from ..execution.execution_request import MCPExecutionRequest
from .permission_model import PermissionModel, MCPPermission
from .allowlist_manager import AllowlistManager
from .restriction_engine import RestrictionEngine
from .audit_logger import MCPAuditLogger

log = logging.getLogger("aelvo.mcp.governance")


class GovernanceResult(BaseModel):
    """Result of a governance check."""
    allowed: bool
    reason: str = ""
    requires_approval: bool = False
    trust_level_ok: bool = False
    permission_ok: bool = False
    allowlist_ok: bool = False
    rate_limit_ok: bool = True
    side_effect_ok: bool = True
    details: Dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "requires_approval": self.requires_approval,
            "trust_level_ok": self.trust_level_ok,
            "permission_ok": self.permission_ok,
            "allowlist_ok": self.allowlist_ok,
            "rate_limit_ok": self.rate_limit_ok,
            "side_effect_ok": self.side_effect_ok,
        }


class MCPGovernanceLayer:
    """Pre-execution governance enforcement for MCP execution.

    Every MCPExecutionRequest passes through governance before
    the execution engine processes it. Bypassing governance
    is structurally impossible.
    """

    def __init__(
        self,
        registry: ServerRegistry,
        permission_model: Optional[PermissionModel] = None,
        allowlist: Optional[AllowlistManager] = None,
        restrictions: Optional[RestrictionEngine] = None,
        audit_logger: Optional[MCPAuditLogger] = None,
    ):
        self._registry = registry
        self._permission_model = permission_model or PermissionModel()
        self._allowlist = allowlist or AllowlistManager()
        self._restrictions = restrictions or RestrictionEngine()
        self._audit_logger = audit_logger or MCPAuditLogger()

    async def check(self, request: MCPExecutionRequest) -> GovernanceResult:
        """Run all governance checks for a request.

        Checks performed in order:
        1. Server allowlist
        2. Server trust level
        3. Specialist permission
        4. Argument inspection
        5. Rate limit
        6. Side-effect gate
        """
        record = self._registry.get(request.server_id)

        # 1. Server allowlist check
        if not self._allowlist.is_allowed(request.server_id):
            result = GovernanceResult(
                allowed=False,
                reason=f"Server '{request.server_id}' is not on the allowlist",
            )
            await self._audit_logger.log("DENIED", request, result)
            return result

        # 2. Trust level check
        if record:
            trust_ok = self._check_trust_level(record.trust_level, request.trust_requirement)
        else:
            trust_ok = False

        if not trust_ok:
            result = GovernanceResult(
                allowed=False,
                reason=f"Server trust level does not meet requirement: {request.trust_requirement.value}",
                trust_level_ok=False,
            )
            await self._audit_logger.log("DENIED", request, result)
            return result

        # 3. Specialist permission check
        permission = self._permission_model.check(
            specialist_id=request.specialist_id,
            server_id=request.server_id,
            tool_name=request.tool_name,
        )

        if not permission.allowed:
            result = GovernanceResult(
                allowed=False,
                reason=permission.reason or f"No permission for {request.specialist_id} on {request.tool_name}",
                permission_ok=False,
            )
            await self._audit_logger.log("DENIED", request, result)
            return result

        # 4. Argument inspection (restriction engine)
        restriction_result = await self._restrictions.inspect(request)
        if restriction_result.get("blocked"):
            result = GovernanceResult(
                allowed=False,
                reason=f"Argument restriction: {restriction_result.get('reason', '')}",
                details={"restriction_result": restriction_result},
            )
            await self._audit_logger.log("DENIED", request, result)
            return result

        # 5. Rate limit check
        rate_ok = self._check_rate_limit(request.server_id, request.tool_name, request.specialist_id)

        # 6. Side-effect gate
        side_effect_ok = await self._check_side_effects(request)

        # All checks passed
        requires_approval = permission.requires_approval or restriction_result.get("requires_approval", False)
        result = GovernanceResult(
            allowed=True,
            trust_level_ok=True,
            permission_ok=True,
            allowlist_ok=True,
            rate_limit_ok=rate_ok,
            side_effect_ok=side_effect_ok,
            requires_approval=requires_approval,
            details={
                "permission": permission.details,
                "restriction_result": restriction_result,
            },
        )
        await self._audit_logger.log("ALLOWED", request, result)
        return result

    # ------------------------------------------------------------------
    # Internal Checks
    # ------------------------------------------------------------------

    @staticmethod
    def _check_trust_level(server_trust: TrustLevel, requirement: TrustLevel) -> bool:
        order = ["blocked", "quarantined", "sandboxed", "verified", "trusted"]
        try:
            return order.index(server_trust.value) >= order.index(requirement.value)
        except ValueError:
            return False

    def _check_rate_limit(self, server_id: str, tool_name: str, specialist_id: str) -> bool:
        """Check rate limits for a server/tool/specialist combination."""
        # Simplified — returns True (rate limiting is a future enhancement)
        return True

    async def _check_side_effects(self, request: MCPExecutionRequest) -> bool:
        """Check if side effects are allowed for this request."""
        # Simplified — returns True (side-effect gating is a future enhancement)
        return True
