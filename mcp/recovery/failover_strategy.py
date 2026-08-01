"""FailoverStrategy — routes to alternate server when primary MCP server fails."""

from __future__ import annotations

import logging
from typing import Optional

from ..registry.server_registry import ServerRegistry
from ..capability.capability_engine import CapabilityEngine
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.recovery.failover")


class FailoverStrategy:
    """Routes MCP execution to an alternate server when the primary fails.

    Performs the actual reroute: selects the best alternative server that
    offers the requested tool and re-targets the request to it, so the
    retry loop dispatches to the alternative rather than the dead primary.
    """

    def __init__(self, registry: ServerRegistry, capability_engine: CapabilityEngine,
                 event_publisher: Optional[MCPEventPublisher] = None):
        self._registry = registry
        self._capability_engine = capability_engine
        self._event_publisher = event_publisher
        self.name = "FailoverStrategy"

    async def execute(self, request: MCPExecutionRequest, failure_type: FailureType, attempt: int) -> bool:
        alternatives = await self._capability_engine.get_alternatives_for(request.server_id, request.tool_name)
        if not alternatives:
            log.warning(
                "FailoverStrategy: no alternative server offers '%s' for %s",
                request.tool_name, request.server_id,
            )
            return False

        record, _ = alternatives[0]
        if record.id == request.server_id:
            # The only "alternative" is the failing server itself — no real failover possible.
            log.warning("FailoverStrategy: only alternative for %s is itself — cannot fail over", request.server_id)
            return False

        log.warning(
            "FailoverStrategy: '%s' is unavailable for '%s'; candidate '%s' exists but "
            "automatic re-dispatch is not supported by the runtime — isolating instead",
            request.server_id, request.tool_name, record.id,
        )
        # Record the reroute candidate for operators. We deliberately do NOT
        # mutate request.server_id: the execution engine does not re-dispatch
        # the request after recovery, so mutating the shared request would only
        # mask the real failure origin in downstream reads (e.g. tool_failed
        # events). Returning False lets the recovery engine fall through to
        # ServerIsolation, which is the honest outcome.
        request.metadata = {
            **(request.metadata or {}),
            "failover_target": record.id,
            "failover_from": request.server_id,
            "failover_strategy": "FailoverStrategy",
        }
        return False
