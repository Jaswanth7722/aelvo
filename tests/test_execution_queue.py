"""Regression tests for the ExecutionQueue priority-ordering fix (report MED #32).

The queue stored tuples ``(priority, timestamp, request)`` in an
``asyncio.PriorityQueue``. If two requests shared the same priority AND the
same timestamp, Python fell through to comparing the ``MCPExecutionRequest``
pydantic models, which are not orderable, raising ``TypeError``. The fix
replaces the wall-clock timestamp with a monotonic sequence counter so tuple
comparison never reaches the request object.
"""

import asyncio

from mcp.events.event_schemas import ExecutionPriority
from mcp.execution.execution_queue import ExecutionQueue
from mcp.execution.execution_request import MCPExecutionRequest


def _make_request(request_id: str, priority: ExecutionPriority = ExecutionPriority.NORMAL) -> MCPExecutionRequest:
    return MCPExecutionRequest(
        request_id=request_id,
        specialist_id="test",
        server_id="srv",
        tool_name="read_file",
        arguments={"path": "x.txt"},
        priority=priority,
    )


def test_same_priority_enqueue_dequeue_roundtrip():
    """Same-priority requests must enqueue and dequeue without TypeError."""

    async def scenario():
        queue = ExecutionQueue(default_max_concurrent=10)
        queue.configure_server("srv", max_concurrent=10)
        reqs = [
            _make_request(f"r{i}", ExecutionPriority.CRITICAL)
            for i in range(5)
        ]
        for r in reqs:
            await queue.enqueue(r)

        dequeued = []
        for _ in reqs:
            r = await queue.dequeue("srv")
            assert r is not None
            dequeued.append(r.request_id)
        return dequeued

    ids = asyncio.run(scenario())
    assert sorted(ids) == ["r0", "r1", "r2", "r3", "r4"]


def test_priority_ordering_is_respected():
    """Higher-priority requests are dequeued first (lower value = higher priority)."""

    async def scenario():
        queue = ExecutionQueue(default_max_concurrent=10)
        queue.configure_server("srv", max_concurrent=10)
        await queue.enqueue(_make_request("low", ExecutionPriority.LOW))
        await queue.enqueue(_make_request("normal", ExecutionPriority.NORMAL))
        await queue.enqueue(_make_request("critical", ExecutionPriority.CRITICAL))

        first = await queue.dequeue("srv")
        second = await queue.dequeue("srv")
        return first.request_id, second.request_id

    first, second = asyncio.run(scenario())
    assert first == "critical"
    assert second == "normal"


def test_same_priority_stays_fifo():
    """Requests with equal priority keep insertion order (stable tiebreaker)."""

    async def scenario():
        queue = ExecutionQueue(default_max_concurrent=10)
        queue.configure_server("srv", max_concurrent=10)
        for i in range(4):
            await queue.enqueue(_make_request(f"q{i}", ExecutionPriority.NORMAL))

        order = []
        for _ in range(4):
            r = await queue.dequeue("srv")
            order.append(r.request_id)
        return order

    order = asyncio.run(scenario())
    assert order == ["q0", "q1", "q2", "q3"]
