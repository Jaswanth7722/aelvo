import asyncio
import logging
import shutil
import tempfile
from pathlib import Path

import pytest

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test_suite")


@pytest.fixture
def test_dir():
    d = Path(tempfile.mkdtemp(prefix="aelvo_val_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.mark.asyncio
async def test_full_integration_flow(test_dir):
    from runtime_next.events.bus import EventBus
    from runtime_next.engine.file_mutex import FileMutex
    from runtime_next.engine.engine import ExecutionGraph
    from runtime_next.recovery.engine import RecoveryEngine
    from runtime_next.models.node import NodeDefinition, NodeState

    log_path = test_dir / "events.log"
    bus = EventBus(log_path=str(log_path))
    mutex = FileMutex()
    graph = ExecutionGraph(bus, mutex)
    recovery = RecoveryEngine(graph)
    bus.subscribe_all(recovery.on_event)
    await bus.start()

    shared_file = str(test_dir / "shared.txt")

    a = NodeDefinition(id="A", description="Root", specialist="FORGE", files=[shared_file])
    b = NodeDefinition(id="B", description="Child B", specialist="SENTINEL", files=[shared_file])
    c = NodeDefinition(id="C", description="Child C", specialist="FORGE", files=[str(test_dir / "other.txt")])
    d = NodeDefinition(id="D", description="Merge", specialist="ARCHITECT", files=[shared_file])

    graph.add_node(a)
    graph.add_node(b)
    graph.add_node(c)
    graph.add_node(d)
    graph.add_edge("A", "B")
    graph.add_edge("A", "C")
    graph.add_edge("B", "D")
    graph.add_edge("C", "D")

    save_path = test_dir / "graph_pre.json"
    graph.serialize(str(save_path))
    assert save_path.exists()

    await graph.start()

    assert graph.nodes["A"].state == NodeState.COMPLETED
    assert graph.nodes["C"].state == NodeState.COMPLETED
    assert graph.nodes["D"].state == NodeState.COMPLETED

    all_terminal = all(
        n.state in (NodeState.COMPLETED, NodeState.SKIPPED)
        for n in graph.nodes.values()
    )
    assert all_terminal, f"Nodes not terminal: {[(nid, n.state.value) for nid, n in graph.nodes.items()]}"

    save_path2 = test_dir / "graph_post.json"
    graph.serialize(str(save_path2))
    assert save_path2.exists()

    await bus.stop()

    assert log_path.exists()
    replayed = []

    async def replay_cb(event):
        replayed.append(event)

    new_bus = EventBus()
    await new_bus.replay(str(log_path), replay_cb)
    assert len(replayed) > 0

    new_mutex = FileMutex()
    restored = ExecutionGraph.deserialize(str(save_path2), new_bus, new_mutex)
    assert "A" in restored.nodes
    assert restored.nodes["A"].state == NodeState.COMPLETED

    log.info("PASS: full integration: DAG -> serialize -> execute -> replay -> deserialize")


@pytest.mark.asyncio
async def test_recovery_with_serialization(test_dir):
    from runtime_next.events.bus import EventBus
    from runtime_next.engine.file_mutex import FileMutex
    from runtime_next.engine.engine import ExecutionGraph
    from runtime_next.recovery.engine import RecoveryEngine
    from runtime_next.models.node import NodeDefinition, NodeState

    bus = EventBus()
    mutex = FileMutex()
    graph = ExecutionGraph(bus, mutex)
    recovery = RecoveryEngine(graph)
    bus.subscribe_all(recovery.on_event)
    await bus.start()

    node = NodeDefinition(id="FAIL_NODE", description="Fail test", specialist="FORGE")
    graph.add_node(node)

    await graph.transition_node("FAIL_NODE", NodeState.FAILED, reason="SyntaxError: invalid syntax")
    await asyncio.sleep(0.3)

    save_path = test_dir / "recovery_graph.json"
    graph.serialize(str(save_path))

    restored = ExecutionGraph.deserialize(str(save_path), bus, mutex)
    assert "FAIL_NODE" in restored.nodes

    log.info(f"PASS: recovery + serialization, state={graph.nodes['FAIL_NODE'].state}")
    await bus.stop()
