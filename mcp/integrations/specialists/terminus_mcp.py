"""TERMINUS MCP specialist integration contract."""

from __future__ import annotations

import logging
from ...registry.server_registry import ServerRegistry
from ...client.connection_manager import ConnectionManager
from ...recovery.recovery_engine import MCPRecoveryEngine
from ...registry.models import TrustLevel, HealthState
from ...events.event_schemas import FailureType
from ...execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.specialist.terminus")


class TerminusMCPInterface:
    """Specialist integration contract for TERMINUS.

    Permitted MCP usage:
    - Server lifecycle management
    - Execution queue management

    Key behavior:
    TERMINUS is the only specialist permitted to trigger server_isolation and
    trust_downgrade recovery actions.
    """

    def __init__(
        self,
        registry: ServerRegistry,
        connection_manager: ConnectionManager,
        recovery_engine: MCPRecoveryEngine,
    ):
        self._registry = registry
        self._connection_manager = connection_manager
        self._recovery_engine = recovery_engine

    async def isolate_server(self, server_id: str) -> None:
        """Trigger immediate server isolation due to security or recovery failure."""
        log.warning("TERMINUS: isolating server '%s'", server_id)
        # Create a mock request to execute the isolation strategy
        request = MCPExecutionRequest(
            request_id="terminus_isolate",
            specialist_id="TERMINUS",
            server_id=server_id,
            tool_name="lifecycle",
        )
        strategy = self._recovery_engine._strategies.get(FailureType.RECOVERY_FAILED)
        if strategy:
            await strategy.execute(request, FailureType.RECOVERY_FAILED, 0)

    async def downgrade_trust(self, server_id: str, target_level: TrustLevel = TrustLevel.QUARANTINED) -> None:
        """Force a trust level downgrade for a server."""
        log.warning("TERMINUS: force downgrading trust for server '%s' to %s", server_id, target_level)
        record = self._registry.get(server_id)
        if record:
            self._registry.update_trust(server_id, target_level)
            if self._recovery_engine._event_publisher:
                await self._recovery_engine._event_publisher.trust_changed(
                    server_id, record.trust_level.value, target_level.value, "TERMINUS forced downgrade"
                )

    async def disable_server(self, server_id: str) -> bool:
        """Disable a server in the registry and disconnect it."""
        self._registry.disable(server_id)
        return await self._connection_manager.disconnect(server_id)

    async def enable_server(self, server_id: str) -> bool:
        """Enable a server in the registry."""
        return self._registry.enable(server_id)
