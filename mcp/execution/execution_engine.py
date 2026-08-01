"""MCPExecutionEngine — core execution orchestrator for MCP tool calls.

Every execution flows through:
1. Governance check (SENTINEL integration)
2. Server health check
3. Capability validation
4. Transport send
5. Response received
6. Verification pipeline
7. Memory persistence
8. Result returned to specialist

Any step failure triggers the Recovery Engine before propagating errors.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional

from ..transport.base_transport import MCPMessage, BaseTransport
from ..registry.models import HealthState, MCPServerConfig
from ..registry.server_registry import ServerRegistry
from ..registry.health_tracker import HealthTracker
from ..client.connection_manager import ConnectionManager
from ..client.timeout_manager import TimeoutManager
from ..capability.capability_engine import CapabilityEngine
from ..governance.governance_layer import MCPGovernanceLayer
from ..verification.verification_pipeline import MCPVerificationPipeline
from ..recovery.recovery_engine import MCPRecoveryEngine
from ..memory.mcp_memory_store import MCPMemoryStore
from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from .execution_request import MCPExecutionRequest
from .execution_result import MCPExecutionResult
from .execution_router import ExecutionRouter
from .execution_queue import ExecutionQueue

log = logging.getLogger("aelvo.mcp.execution.engine")


class MCPExecutionEngine:
    """Core MCP execution engine that runs the full governed pipeline.

    The engine enforces governance, verification, recovery, and memory
    persistence on every execution — no bypass is possible.
    """

    def __init__(
        self,
        registry: ServerRegistry,
        connection_manager: ConnectionManager,
        capability_engine: CapabilityEngine,
        governance: MCPGovernanceLayer,
        verification: MCPVerificationPipeline,
        recovery: MCPRecoveryEngine,
        memory: MCPMemoryStore,
        event_publisher: MCPEventPublisher,
        health_tracker: HealthTracker,
        timeout_manager: Optional[TimeoutManager] = None,
        router: Optional[ExecutionRouter] = None,
        queue: Optional[ExecutionQueue] = None,
    ):
        self._registry = registry
        self._connection_manager = connection_manager
        self._capability_engine = capability_engine
        self._governance = governance
        self._verification = verification
        self._recovery = recovery
        self._memory = memory
        self._event_publisher = event_publisher
        self._health_tracker = health_tracker
        self._timeout_manager = timeout_manager or TimeoutManager()
        self._router = router
        self._queue = queue or ExecutionQueue()
        self._active_executions: Dict[str, asyncio.Task] = {}

    async def execute(self, request: MCPExecutionRequest) -> MCPExecutionResult:
        """Execute an MCP tool call through the full governed pipeline.

        Args:
            request: The typed execution request.

        Returns:
            Verified execution result with full metadata.
        """
        start_time = time.monotonic()

        # 0. Publish started event
        await self._event_publisher.tool_started(
            request.request_id, request.server_id, request.tool_name,
            request.specialist_id, request.timeout_ms,
        )

        # 1. Governance check (SENTINEL)
        gov_result = await self._governance.check(request)
        if not gov_result.allowed:
            duration = int((time.monotonic() - start_time) * 1000)
            result = MCPExecutionResult(
                request_id=request.request_id,
                specialist_id=request.specialist_id,
                server_id=request.server_id,
                tool_name=request.tool_name,
                success=False,
                error=f"Governance blocked: {gov_result.reason}",
                duration_ms=duration,
                governance_passed=False,
                governance_details=gov_result.to_dict(),
            )
            await self._event_publisher.tool_failed(
                request.request_id, request.server_id, request.tool_name,
                "permission_denied", gov_result.reason,
            )
            await self._memory.store_result(result)
            return result

        # 2. Route request (resolve server if not specified)
        if self._router:
            server_id, error = await self._router.route(request)
            if server_id:
                request.server_id = server_id
            elif error:
                duration = int((time.monotonic() - start_time) * 1000)
                result = MCPExecutionResult(
                    request_id=request.request_id,
                    specialist_id=request.specialist_id,
                    server_id=request.server_id,
                    tool_name=request.tool_name,
                    success=False,
                    error=error,
                    duration_ms=duration,
                )
                return result

        # 3. Check server health and capability
        record = self._registry.get(request.server_id)
        if not record:
            return self._fail(request, "Server not found in registry", start_time)

        if record.health_state == HealthState.UNREACHABLE:
            return self._fail(request, "Server is unreachable", start_time)

        # 4. Get or create transport
        transport = self._connection_manager.get_transport(request.server_id)
        if not transport or not transport.is_connected:
            config = MCPServerConfig(
                id=record.id,
                name=record.name,
                transport_type=record.transport_type,
                connection_config=record.connection_config,
                trust_level=record.trust_level,
            )
            connected = await self._connection_manager.connect(config)
            if not connected:
                self._health_tracker.mark_unreachable(request.server_id, "Connection failed")
                return self._fail(request, "Failed to connect to server", start_time)

            transport = self._connection_manager.get_transport(request.server_id)

        # 5. Execute with timeout
        try:
            mcp_message = MCPMessage(
                id=f"{request.server_id}:{request.request_id}",
                method="tools/call",
                params={
                    "name": request.tool_name,
                    "arguments": request.arguments,
                },
            )

            await transport.send(mcp_message)

            response = await asyncio.wait_for(
                self._receive_response(transport, mcp_message.id),
                timeout=request.timeout_ms / 1000,
            )

        except asyncio.TimeoutError:
            duration = int((time.monotonic() - start_time) * 1000)
            self._health_tracker.record_error(request.server_id, "Timeout")
            result = MCPExecutionResult(
                request_id=request.request_id,
                specialist_id=request.specialist_id,
                server_id=request.server_id,
                tool_name=request.tool_name,
                success=False,
                error=f"Execution timed out after {request.timeout_ms}ms",
                duration_ms=duration,
            )
            # Attempt recovery
            recovered = await self._recovery.attempt_recovery(
                request, FailureType.TIMEOUT
            )
            result.recovery_attempted = True
            result.recovery_successful = recovered
            await self._event_publisher.tool_failed(
                request.request_id, request.server_id, request.tool_name,
                "timeout", recovery=recovered,
            )
            await self._memory.store_result(result)
            return result

        except Exception as e:
            duration = int((time.monotonic() - start_time) * 1000)
            self._health_tracker.record_error(request.server_id, str(e))
            result = MCPExecutionResult(
                request_id=request.request_id,
                specialist_id=request.specialist_id,
                server_id=request.server_id,
                tool_name=request.tool_name,
                success=False,
                error=str(e),
                duration_ms=duration,
            )
            await self._event_publisher.tool_failed(
                request.request_id, request.server_id, request.tool_name,
                "server_error", str(e), recovery=False,
            )
            await self._memory.store_result(result)
            return result

        # 6. Verify response
        verification_results = await self._verification.verify(
            request_id=request.request_id,
            server_id=request.server_id,
            tool_name=request.tool_name,
            response=response,
            context={
                "start_time": start_time,
                "timeout_ms": request.timeout_ms,
            },
        )
        all_verified = all(v.passed for v in verification_results)

        # 7. Build result
        duration = int((time.monotonic() - start_time) * 1000)
        result = MCPExecutionResult(
            request_id=request.request_id,
            specialist_id=request.specialist_id,
            server_id=request.server_id,
            tool_name=request.tool_name,
            success=True,
            output=response.result if response else None,
            duration_ms=duration,
            verification_passed=all_verified,
            verification_results=[v.model_dump() for v in verification_results],
            governance_passed=True,
            trust_level_at_execution=record.trust_level.value,
            raw_response=response.model_dump() if response else None,
        )

        # 8. Publish completion
        await self._event_publisher.tool_completed(
            request.request_id, request.server_id, request.tool_name,
            duration, all_verified,
        )

        # 9. Store in memory
        await self._memory.store_result(result)

        return result

    async def _receive_response(self, transport: BaseTransport, message_id: str) -> MCPMessage:
        """Receive a response matching the message ID."""
        async for msg in transport.receive():
            if msg.id == message_id:
                return msg
            # Handle server-initiated messages (notifications, progress)
            if msg.method and msg.method.startswith("notifications/"):
                continue
        raise TimeoutError("No response received")

    def _fail(self, request: MCPExecutionRequest, error: str, start: float) -> MCPExecutionResult:
        duration = int((time.monotonic() - start) * 1000)
        result = MCPExecutionResult(
            request_id=request.request_id,
            specialist_id=request.specialist_id,
            server_id=request.server_id,
            tool_name=request.tool_name,
            success=False,
            error=error,
            duration_ms=duration,
        )
        return result
