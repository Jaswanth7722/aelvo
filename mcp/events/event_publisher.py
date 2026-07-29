"""MCP Event Publisher â€” bridges MCP events to the AELVO EventBus."""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, Optional

from .mcp_events import (
    MCPEvent,
    MCPServerDiscovered,
    MCPServerRegistered,
    MCPServerEnabled,
    MCPServerDisabled,
    MCPConnected,
    MCPDisconnected,
    MCPConnectionFailed,
    MCPToolStarted,
    MCPToolCompleted,
    MCPToolFailed,
    MCPTrustChanged,
)
from .event_schemas import DiscoverySource, DisconnectReason

log = logging.getLogger("aelvo.mcp.events")

_MCP_EVENT_TYPE_MAP: Dict[str, str] = {
    "MCPServerDiscovered": "mcp_server_discovered",
    "MCPServerRegistered": "mcp_server_registered",
    "MCPServerEnabled": "mcp_server_enabled",
    "MCPServerDisabled": "mcp_server_disabled",
    "MCPConnected": "mcp_connected",
    "MCPDisconnected": "mcp_disconnected",
    "MCPConnectionFailed": "mcp_connection_failed",
    "MCPToolStarted": "mcp_tool_started",
    "MCPToolCompleted": "mcp_tool_completed",
    "MCPToolFailed": "mcp_tool_failed",
    "MCPVerificationPassed": "mcp_verification_passed",
    "MCPVerificationFailed": "mcp_verification_failed",
    "MCPRecoveryStarted": "mcp_recovery_started",
    "MCPRecoverySucceeded": "mcp_recovery_succeeded",
    "MCPRecoveryFailed": "mcp_recovery_failed",
    "MCPTrustChanged": "mcp_trust_changed",
    "MCPCapabilityNegotiated": "mcp_capability_negotiated",
    "MCPCapabilityDriftDetected": "mcp_capability_drift_detected",
}


class MCPEventPublisher:
    """Publishes MCP events to the AELVO EventBus.

    Translates typed MCPEvent subclasses to the EventBus format
    and publishes them with proper event types for TUI visibility.
    """

    def __init__(self, event_bus: Optional[Any] = None):
        self._event_bus = event_bus
        self._published_count = 0

    def set_event_bus(self, event_bus: Any) -> None:
        """Set or update the EventBus reference."""
        self._event_bus = event_bus

    async def publish(self, event: MCPEvent) -> None:
        """Publish an MCP event to the EventBus."""
        if self._event_bus is None:
            return

        event_type_name = type(event).__name__
        mcp_type = _MCP_EVENT_TYPE_MAP.get(event_type_name, "mcp_generic")

        try:
            from runtime_next.models.events import BaseEvent, EventType

            bus_event = BaseEvent(
                id=f"mcp_{event.event_id}_{int(time.time())}",
                type=EventType.LOG_MESSAGE,
                payload={
                    "mcp_event_type": mcp_type,
                    "mcp": event.model_dump() if hasattr(event, "model_dump") else {},
                    "timestamp": event.timestamp.isoformat() if hasattr(event, "timestamp") else "",
                },
            )
            await self._event_bus.publish(bus_event)
            self._published_count += 1

        except Exception as e:
            log.warning("Failed to publish MCP event to EventBus: %s", e)

    # ------------------------------------------------------------------
    # Convenience methods for common event types
    # ------------------------------------------------------------------

    async def server_discovered(self, server_id: str, source: DiscoverySource, name: str = "") -> None:
        event = MCPServerDiscovered(
            event_id=self._gen_id(),
            server_id=server_id,
            source=source,
            server_name=name,
        )
        await self.publish(event)

    async def server_registered(self, server_id: str, trust_level: str) -> None:
        event = MCPServerRegistered(
            event_id=self._gen_id(),
            server_id=server_id,
            trust_level=trust_level,
        )
        await self.publish(event)

    async def server_enabled(self, server_id: str) -> None:
        event = MCPServerEnabled(event_id=self._gen_id(), server_id=server_id)
        await self.publish(event)

    async def server_disabled(self, server_id: str, reason: str = "") -> None:
        event = MCPServerDisabled(event_id=self._gen_id(), server_id=server_id, reason=reason)
        await self.publish(event)

    async def connected(self, server_id: str, transport_type: str) -> None:
        event = MCPConnected(event_id=self._gen_id(), server_id=server_id, transport_type=transport_type)
        await self.publish(event)

    async def disconnected(self, server_id: str, reason: DisconnectReason) -> None:
        event = MCPDisconnected(event_id=self._gen_id(), server_id=server_id, reason=reason)
        await self.publish(event)

    async def connection_failed(self, server_id: str, error: str, attempt: int) -> None:
        event = MCPConnectionFailed(event_id=self._gen_id(), server_id=server_id, error=error, attempt=attempt)
        await self.publish(event)

    async def tool_started(self, request_id: str, server_id: str, tool_name: str, specialist_id: str, timeout_ms: int) -> None:
        event = MCPToolStarted(
            event_id=self._gen_id(),
            request_id=request_id,
            server_id=server_id,
            tool_name=tool_name,
            specialist_id=specialist_id,
            timeout_ms=timeout_ms,
        )
        await self.publish(event)

    async def tool_completed(self, request_id: str, server_id: str, tool_name: str, duration_ms: int, verified: bool) -> None:
        event = MCPToolCompleted(
            event_id=self._gen_id(),
            request_id=request_id,
            server_id=server_id,
            tool_name=tool_name,
            duration_ms=duration_ms,
            verification_passed=verified,
        )
        await self.publish(event)

    async def tool_failed(self, request_id: str, server_id: str, tool_name: str, failure_type: str, error: str = "", recovery: bool = False) -> None:
        event = MCPToolFailed(
            event_id=self._gen_id(),
            request_id=request_id,
            server_id=server_id,
            tool_name=tool_name,
            failure_type=failure_type,
            recovery_attempted=recovery,
            error=error,
        )
        await self.publish(event)

    async def trust_changed(self, server_id: str, old: str, new: str, reason: str) -> None:
        event = MCPTrustChanged(
            event_id=self._gen_id(),
            server_id=server_id,
            old_level=old,
            new_level=new,
            reason=reason,
        )
        await self.publish(event)

    @staticmethod
    def _gen_id() -> str:
        return hashlib.sha256(f"mcp_evt_{time.time()}".encode()).hexdigest()[:16]

    @property
    def published_count(self) -> int:
        return self._published_count
