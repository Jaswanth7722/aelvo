import asyncio
import logging
import shutil
import random
from pathlib import Path

from runtime_next.events.bus import EventBus
from runtime_next.capability.registry import CapabilityRegistry
from runtime_next.models.node import NodeDefinition, NodeState
from runtime_next.recovery.engine import RecoveryEngine
from runtime_next.engine.engine import ExecutionGraph
from runtime_next.engine.file_mutex import FileMutex

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')
log = logging.getLogger("validation")

class ValidationSuite:
    def __init__(self, test_dir: str = "validation_tmp"):
        self.test_dir = Path(test_dir)
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
        self.bus = EventBus(log_path=str(self.test_dir / "events.log"))
        self.mutex = FileMutex()
        self.registry = CapabilityRegistry(workspace_root=str(self.test_dir), event_bus=self.bus)

    async def setup(self):
        await self.bus.start()
        await self.registry.start_monitoring()

    async def teardown(self):
        await self.registry.stop_monitoring()
        await self.bus.stop()
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    async def test_serialization_integrity(self):
        """Phase 4: Serialization integrity and Resumability."""
        log.info("Testing Serialization & Resumability...")
        graph = ExecutionGraph(self.bus, self.mutex)
        
        n1 = NodeDefinition(id="N1", description="Task 1", specialist="FORGE", state=NodeState.COMPLETED)
        n2 = NodeDefinition(id="N2", description="Task 2", specialist="FORGE", state=NodeState.RUNNING)
        graph.add_node(n1)
        graph.add_node(n2)
        
        save_path = str(self.test_dir / "graph.json")
        graph.serialize(save_path)
        
        # Reload
        new_graph = ExecutionGraph.deserialize(save_path, self.bus, self.mutex)
        
        assert "N1" in new_graph.nodes
        assert "N2" in new_graph.nodes
        assert new_graph.nodes["N1"].state == NodeState.COMPLETED
        # RUNNING should be reset to PENDING for resumability
        assert new_graph.nodes["N2"].state == NodeState.PENDING
        log.info("✓ Serialization & Resumability Passed")

    async def test_file_mutex_stress(self):
        """Phase 4: Async concurrency and file mutex tests."""
        log.info("Testing File Mutex Stress (Concurrency)...")
        
        shared_file = str(self.test_dir / "shared.txt")
        counter = {"val": 0}
        
        async def worker(wid: int):
            for _ in range(20):
                await self.mutex.acquire([shared_file])
                try:
                    curr = counter["val"]
                    await asyncio.sleep(random.uniform(0.001, 0.005))
                    counter["val"] = curr + 1
                finally:
                    await self.mutex.release([shared_file])
        
        await asyncio.gather(*(worker(i) for i in range(10)))
        assert counter["val"] == 200
        log.info("✓ File Mutex Stress Passed")

    async def test_recovery_flow(self):
        """Phase 4: Failure recovery and classification."""
        log.info("Testing Recovery Engine (Classification & Injection)...")
        graph = ExecutionGraph(self.bus, self.mutex)
        recovery = RecoveryEngine(graph)
        self.bus.subscribe_all(recovery.on_event)
        
        node = NodeDefinition(id="FAIL_NODE", description="Test Fail", specialist="FORGE")
        graph.add_node(node)
        
        # Simulate Anchor Violation
        await graph.transition_node("FAIL_NODE", NodeState.FAILED, reason="Honesty Violation: Constraint mismatch")
        await asyncio.sleep(0.2)
        
        assert graph.nodes["FAIL_NODE"].state == NodeState.RETRYING
        assert graph.nodes["FAIL_NODE"].retry_count == 1
        
        # Simulate Missing Tool
        await graph.transition_node("FAIL_NODE", NodeState.FAILED, reason="FileNotFoundError: [Errno 2] No such file or directory: 'ruff'")
        await asyncio.sleep(0.2)
        
        # Should have injected a recovery node
        recovery_node_id = "recover_FAIL_NODE_tool"
        assert recovery_node_id in graph.nodes
        assert graph.nodes[recovery_node_id].specialist == "TERMINUS"
        log.info("✓ Recovery Engine Passed")

    async def test_event_replay(self):
        """Phase 3: Replay testing."""
        log.info("Testing Event Replay...")
        replayed_events = []
        async def replay_cb(event):
            replayed_events.append(event)
        
        log_path = str(self.test_dir / "events.log")
        # Ensure some events exist
        NodeDefinition(id="REPLAY_TEST", description="Replay", specialist="FORGE")
        # ... just publish some events
        from runtime_next.models.events import BaseEvent, EventType
        await self.bus.publish(BaseEvent(id="E1", type=EventType.LOG_MESSAGE, payload={"m": "hello"}))
        await asyncio.sleep(0.1)
        
        await self.bus.replay(log_path, replay_cb)
        assert len(replayed_events) > 0
        log.info(f"✓ Event Replay Passed (replayed {len(replayed_events)} events)")

    async def run_all(self):
        try:
            await self.setup()
            await self.test_serialization_integrity()
            await self.test_file_mutex_stress()
            await self.test_recovery_flow()
            await self.test_event_replay()
            log.info("==========================================")
            log.info("ALL VALIDATION TESTS PASSED 100%")
            log.info("==========================================")
        finally:
            await self.teardown()

if __name__ == "__main__":
    suite = ValidationSuite()
    asyncio.run(suite.run_all())
