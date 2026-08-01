"""ExecutionQueue — prioritized queue for MCP execution requests with concurrency limits."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Dict, Optional, Set

from .execution_request import MCPExecutionRequest
from ..events.event_schemas import ExecutionPriority

log = logging.getLogger("aelvo.mcp.execution.queue")


class ExecutionQueue:
    """Manages queuing, prioritization, and concurrency limits for MCP execution requests.

    Per-server concurrency limits prevent overwhelming a single server.
    Priority ordering ensures critical requests are processed first.
    """

    def __init__(self, default_max_concurrent: int = 5):
        self._queues: Dict[str, asyncio.PriorityQueue] = {}  # server_id -> PriorityQueue
        self._semaphores: Dict[str, asyncio.Semaphore] = {}  # server_id -> Semaphore
        self._max_concurrent: Dict[str, int] = defaultdict(lambda: default_max_concurrent)
        self._active_workers: Dict[str, Set[str]] = defaultdict(set)  # server_id -> {request_ids}
        self._total_enqueued = 0
        self._seq = 0  # monotonic tiebreaker for priority ordering
        self._total_processed = 0
        self._total_failed = 0

    def configure_server(self, server_id: str, max_concurrent: int) -> None:
        """Set the maximum concurrent requests for a server."""
        self._max_concurrent[server_id] = max_concurrent
        self._ensure_queue(server_id)

    async def enqueue(self, request: MCPExecutionRequest) -> None:
        """Enqueue a request for processing."""
        self._ensure_queue(request.server_id)
        priority = self._priority_value(request.priority)
        # Put (priority, timestamp, request) — lower priority_value = higher priority
        self._seq += 1
        await self._queues[request.server_id].put((priority, self._seq, request))
        self._total_enqueued += 1

    async def dequeue(self, server_id: str) -> Optional[MCPExecutionRequest]:
        """Dequeue the next request for a server (blocking)."""
        queue = self._queues.get(server_id)
        if queue is None:
            return None

        try:
            _, _, request = await asyncio.wait_for(queue.get(), timeout=30.0)
            self._active_workers[server_id].add(request.request_id)
            return request
        except asyncio.TimeoutError:
            return None

    def complete(self, server_id: str, request_id: str, success: bool) -> None:
        """Mark a request as completed."""
        self._active_workers[server_id].discard(request_id)
        self._total_processed += 1
        if not success:
            self._total_failed += 1

    def get_queue_size(self, server_id: str) -> int:
        """Get the number of queued requests for a server."""
        queue = self._queues.get(server_id)
        return queue.qsize() if queue else 0

    def can_accept(self, server_id: str) -> bool:
        """Check if a server can accept a new request."""
        max_conc = self._max_concurrent.get(server_id, 5)
        active = len(self._active_workers.get(server_id, set()))
        return active < max_conc

    def clear_server(self, server_id: str) -> int:
        """Clear all queued requests for a server."""
        queue = self._queues.get(server_id)
        count = queue.qsize() if queue else 0
        if queue:
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
        self._active_workers[server_id].clear()
        return count

    @property
    def total_enqueued(self) -> int:
        return self._total_enqueued

    @property
    def total_processed(self) -> int:
        return self._total_processed

    @property
    def total_failed(self) -> int:
        return self._total_failed

    def _ensure_queue(self, server_id: str) -> None:
        if server_id not in self._queues:
            self._queues[server_id] = asyncio.PriorityQueue()

    @staticmethod
    def _priority_value(priority: ExecutionPriority) -> int:
        """Convert priority enum to numeric value (lower = higher priority)."""
        mapping = {
            ExecutionPriority.CRITICAL: 0,
            ExecutionPriority.HIGH: 1,
            ExecutionPriority.NORMAL: 2,
            ExecutionPriority.LOW: 3,
        }
        return mapping.get(priority, 2)
