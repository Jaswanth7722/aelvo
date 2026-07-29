"""MCPRecoveryEngine — orchestrates MCP failure recovery without specialist involvement.

Recovery strategies are selected based on failure type and applied in
priority order. Recovery runs silently when possible and surfaces
partial failures to the EventBus and TUI.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType, VerificationAction
from ..registry.models import HealthState, TrustLevel
from ..registry.server_registry import ServerRegistry
from ..registry.health_tracker import HealthTracker
from ..registry.trust_manager import TrustManager, TrustChangeReason
from ..client.connection_manager import ConnectionManager
from ..client.reconnect_policy import ReconnectPolicy
from ..capability.capability_engine import CapabilityEngine
from ..execution.execution_request import MCPExecutionRequest
from .reconnect_strategy import ReconnectStrategy
from .retry_strategy import RetryStrategy
from .failover_strategy import FailoverStrategy
from .capability_refresh import CapabilityRefresh
from .server_isolation import ServerIsolation
from .trust_downgrade import TrustDowngrade

log = logging.getLogger("aelvo.mcp.recovery.engine")


class MCPRecoveryEngine:
    """Handles MCP failure recovery across all failure modes.

    Recovery strategies by failure type:
    - Connection lost: ReconnectStrategy → FailoverStrategy
    - Transient error: RetryStrategy → FailoverStrategy
    - Timeout: RetryStrategy (reduced timeout) → ServerIsolation
    - Schema violation: CapabilityRefresh → retry → ServerIsolation
    - Trust violation: TrustDowngrade → ServerIsolation
    - Repeated failure: ServerIsolation → manual recovery required
    """

    def __init__(
        self,
        registry: ServerRegistry,
        connection_manager: ConnectionManager,
        capability_engine: CapabilityEngine,
        health_tracker: HealthTracker,
        trust_manager: TrustManager,
        event_publisher: Optional[MCPEventPublisher] = None,
    ):
        self._registry = registry
        self._connection_manager = connection_manager
        self._capability_engine = capability_engine
        self._health_tracker = health_tracker
        self._trust_manager = trust_manager
        self._event_publisher = event_publisher

        self._strategies = {
            FailureType.CONNECTION_LOST: ReconnectStrategy(connection_manager, event_publisher),
            FailureType.TRANSIENT_ERROR: RetryStrategy(event_publisher),
            FailureType.TIMEOUT: RetryStrategy(event_publisher, reduced_timeout=True),
            FailureType.SCHEMA_VIOLATION: CapabilityRefresh(capability_engine, event_publisher),
            FailureType.TRUST_VIOLATION: TrustDowngrade(trust_manager, event_publisher),
            FailureType.CAPABILITY_MISMATCH: CapabilityRefresh(capability_engine, event_publisher),
            FailureType.RECOVERY_FAILED: ServerIsolation(registry, health_tracker, event_publisher),
        }

        self._fallbacks = {
            FailureType.CONNECTION_LOST: FailoverStrategy(registry, capability_engine, event_publisher),
            FailureType.TRANSIENT_ERROR: FailoverStrategy(registry, capability_engine, event_publisher),
            FailureType.TIMEOUT: ServerIsolation(registry, health_tracker, event_publisher),
        }

        self._recovery_history: List[Dict[str, Any]] = []
        self._max_attempts = 3

    async def attempt_recovery(
        self,
        request: MCPExecutionRequest,
        failure_type: FailureType,
        error_details: Optional[str] = None,
    ) -> bool:
        """Attempt to recover from a failure.

        Returns True if recovery succeeded (request can be retried).
        """
        await self._publish_recovery_started(request.server_id, failure_type)

        primary = self._strategies.get(failure_type)
        fallback = self._fallbacks.get(failure_type)

        for attempt in range(self._max_attempts):
            # Try primary strategy
            if primary:
                try:
                    success = await primary.execute(request, failure_type, attempt)
                    if success:
                        self._record_recovery(request.server_id, failure_type.value, primary.name, attempt, True)
                        await self._publish_recovery_succeeded(request.server_id, primary.name, attempt + 1)
                        return True
                except Exception as e:
                    log.warning("Recovery: primary strategy failed: %s", e)

            # Try fallback if primary failed
            if fallback:
                try:
                    success = await fallback.execute(request, failure_type, attempt)
                    if success:
                        self._record_recovery(request.server_id, failure_type.value, fallback.name, attempt, True)
                        await self._publish_recovery_succeeded(request.server_id, fallback.name, attempt + 1)
                        return True
                except Exception as e:
                    log.warning("Recovery: fallback strategy failed: %s", e)

        # All recovery attempts failed
        self._record_recovery(request.server_id, failure_type.value, "all", self._max_attempts, False)
        await self._publish_recovery_failed(request.server_id)

        # Isolate the server after repeated failures
        isolation = ServerIsolation(self._registry, self._health_tracker, self._event_publisher)
        await isolation.execute(request, FailureType.RECOVERY_FAILED, 0)

        return False

    def _record_recovery(self, server_id: str, failure_type: str, strategy: str, attempt: int, success: bool) -> None:
        self._recovery_history.append({
            "server_id": server_id,
            "failure_type": failure_type,
            "strategy": strategy,
            "attempt": attempt,
            "success": success,
        })

    async def _publish_recovery_started(self, server_id: str, failure_type: FailureType) -> None:
        if self._event_publisher:
            from ..events.mcp_events import MCPRecoveryStarted
            await self._event_publisher.publish(MCPRecoveryStarted(
                event_id=f"rec_start_{server_id}",
                server_id=server_id,
                strategy="primary",
                trigger=failure_type.value,
            ))

    async def _publish_recovery_succeeded(self, server_id: str, strategy: str, attempts: int) -> None:
        if self._event_publisher:
            from ..events.mcp_events import MCPRecoverySucceeded
            await self._event_publisher.publish(MCPRecoverySucceeded(
                event_id=f"rec_ok_{server_id}",
                server_id=server_id,
                strategy=strategy,
                attempts=attempts,
            ))

    async def _publish_recovery_failed(self, server_id: str) -> None:
        if self._event_publisher:
            from ..events.mcp_events import MCPRecoveryFailed
            await self._event_publisher.publish(MCPRecoveryFailed(
                event_id=f"rec_fail_{server_id}",
                server_id=server_id,
                strategy="all",
                server_isolated=True,
            ))

    @property
    def recovery_history(self) -> List[Dict[str, Any]]:
        return list(self._recovery_history)
