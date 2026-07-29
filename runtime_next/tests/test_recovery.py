import asyncio
import logging
import shutil
import tempfile
from pathlib import Path

import pytest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test_recovery")


@pytest.fixture
def graph_and_recovery():
    from runtime_next.events.bus import EventBus
    from runtime_next.engine.file_mutex import FileMutex
    from runtime_next.engine.engine import ExecutionGraph
    from runtime_next.recovery.engine import RecoveryEngine
    bus = EventBus()
    mutex = FileMutex()
    graph = ExecutionGraph(bus, mutex)
    recovery = RecoveryEngine(graph)
    recovery.use_legacy_recovery(True)
    return graph, recovery, bus


@pytest.mark.asyncio
async def test_classify_syntax_error(graph_and_recovery):
    _, recovery, _ = graph_and_recovery
    cls = recovery._classify_failure("SyntaxError: invalid syntax at line 42")
    assert cls == "syntax_error"


@pytest.mark.asyncio
async def test_classify_missing_resource(graph_and_recovery):
    _, recovery, _ = graph_and_recovery
    cls = recovery._classify_failure("FileNotFoundError: No such file or directory: 'ruff'")
    assert cls == "missing_resource"


@pytest.mark.asyncio
async def test_classify_permission_denied(graph_and_recovery):
    _, recovery, _ = graph_and_recovery
    cls = recovery._classify_failure("Permission denied: Access is denied")
    assert cls == "permission_denied"


@pytest.mark.asyncio
async def test_classify_timeout(graph_and_recovery):
    _, recovery, _ = graph_and_recovery
    cls = recovery._classify_failure("Timeout: command timed out after 30 seconds")
    assert cls == "timeout"


@pytest.mark.asyncio
async def test_classify_lock_contention(graph_and_recovery):
    _, recovery, _ = graph_and_recovery
    cls = recovery._classify_failure("Lock contention: file is busy")
    assert cls == "lock_contention"


@pytest.mark.asyncio
async def test_classify_anchor_violation(graph_and_recovery):
    _, recovery, _ = graph_and_recovery
    cls = recovery._classify_failure("Constraint mismatch: anchor verification failed")
    assert cls == "anchor_violation"


@pytest.mark.asyncio
async def test_classify_unknown(graph_and_recovery):
    _, recovery, _ = graph_and_recovery
    cls = recovery._classify_failure("Something completely unexpected happened")
    assert cls == "unknown"


@pytest.mark.asyncio
async def test_recovery_retry_budget_exhausted(graph_and_recovery):
    from runtime_next.models.node import NodeDefinition, NodeState
    graph, recovery, _ = graph_and_recovery
    node = NodeDefinition(id="R1", description="Retry test", specialist="FORGE", retry_budget=1, retry_count=1)
    graph.add_node(node)
    await recovery.handle_failure("R1", "SyntaxError: bad code")
    assert graph.nodes["R1"].state == NodeState.FAILED


@pytest.mark.asyncio
async def test_recovery_injects_recovery_node(graph_and_recovery):
    from runtime_next.models.node import NodeDefinition, NodeState
    graph, recovery, _ = graph_and_recovery
    node = NodeDefinition(id="M1", description="Missing tool", specialist="FORGE")
    graph.add_node(node)
    await recovery.handle_failure("M1", "FileNotFoundError: 'ruff' not found")
    recovery_ids = [nid for nid in graph.nodes if "recover" in nid and "M1" in nid]
    assert len(recovery_ids) == 1


@pytest.mark.asyncio
async def test_recovery_permission_sets_blocked(graph_and_recovery):
    from runtime_next.models.node import NodeDefinition, NodeState
    graph, recovery, _ = graph_and_recovery
    node = NodeDefinition(id="P1", description="Permission test", specialist="FORGE")
    graph.add_node(node)
    await recovery.handle_failure("P1", "Permission denied: Access is denied")
    assert graph.nodes["P1"].state == NodeState.BLOCKED


@pytest.mark.asyncio
async def test_recovery_event_emitted(graph_and_recovery):
    from runtime_next.models.node import NodeDefinition
    from runtime_next.models.events import EventType
    graph, recovery, bus = graph_and_recovery
    events = []

    async def cb(event):
        if event.type == EventType.RECOVERY_INITIATED:
            events.append(event)

    bus.subscribe_all(cb)
    await bus.start()

    node = NodeDefinition(id="E1", description="Event test", specialist="FORGE")
    graph.add_node(node)
    await recovery.handle_failure("E1", "SyntaxError: bad")

    await asyncio.sleep(0.3)
    await bus.stop()

    assert len(events) >= 1
    assert events[0].classification == "syntax_error"
