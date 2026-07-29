"""RetryStrategy — retries MCP execution with jittered backoff on transient failures."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from ..events.event_publisher import MCPEventPublisher
from ..events.event_schemas import FailureType
from ..execution.execution_request import MCPExecutionRequest

log = logging.getLogger("aelvo.mcp.recovery.retry")


class RetryStrategy:
    """Retries MCP execution with jittered backoff on transient failures."""

    def __init__(self, event_publisher: Optional[MCPEventPublisher] = None, reduced_timeout: bool = False):
        self._event_publisher = event_publisher
        self._reduced_timeout = reduced_timeout
        self.name = "RetryStrategy"

    async def execute(self, request: MCPExecutionRequest, failure_type: FailureType, attempt: int) -> bool:
        delay = min(10.0, 1.0 * (2 ** attempt)) * 1.1  # Exponential with jitter
        await asyncio.sleep(delay)
        return True  # Signal that retry is possible; execution engine will re-send
