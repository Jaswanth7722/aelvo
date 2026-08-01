"""TrustDowngrade — downgrades MCP server trust level on trust violation."""

from __future__ import annotations

import logging
from typing import Optional

from ..registry.trust_manager import TrustManager, TrustChangeReason
from ..registry.server_registry import ServerRegistry
from ..registry.models import TrustLevel
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.recovery.trust_downgrade")


class TrustDowngrade:
    """Downgrades server trust level when a trust violation is detected."""

    def __init__(self, trust_manager: TrustManager,
                 event_publisher: Optional[MCPEventPublisher] = None,
                 registry: Optional[ServerRegistry] = None):
        self._trust_manager = trust_manager
        self._event_publisher = event_publisher
        self._registry = registry
        self.name = "TrustDowngrade"

    async def execute(self, request: MCPExecutionRequest, failure_type: FailureType, attempt: int) -> bool:
        # Resolve the server's actual current trust level instead of a hardcoded placeholder.
        current = TrustLevel.VERIFIED  # safe default
        if self._registry is not None:
            record = self._registry.get(request.server_id)
            if record is not None:
                current = record.trust_level

        target = TrustLevel.QUARANTINED
        if current == TrustLevel.BLOCKED:
            log.warning("TrustDowngrade: '%s' already BLOCKED — no further downgrade possible", request.server_id)
            return False
        if current == TrustLevel.QUARANTINED:
            log.info("TrustDowngrade: '%s' already QUARANTINED", request.server_id)
            return True

        success, new_level, msg = self._trust_manager.change_trust(
            server_id=request.server_id,
            current=current,
            target=target,
            reason=TrustChangeReason.TRUST_VIOLATION,
            details=f"Trust violation during {request.tool_name} execution",
        )

        if success:
            log.warning("TrustDowngrade: %s -> %s for '%s'", current, target, request.server_id)
            if self._event_publisher:
                await self._event_publisher.trust_changed(
                    request.server_id, current.value, target.value,
                    f"Trust violation: {failure_type.value}",
                )
            return True

        log.warning("TrustDowngrade: failed to downgrade '%s': %s", request.server_id, msg)
        return False
