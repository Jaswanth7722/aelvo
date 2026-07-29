import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("test_node")


class TestNodeModel:
    """Tests for NodeDefinition model and state machine."""

    def test_create_node(self):
        from runtime_next.models.node import NodeDefinition, NodeState, DangerClassification
        node = NodeDefinition(
            id="N1",
            description="Test node",
            specialist="FORGE",
            tools=["ruff", "mypy"],
            retry_budget=3,
            danger=DangerClassification.SAFE
        )
        assert node.id == "N1"
        assert node.state == NodeState.PENDING
        assert node.retry_budget == 3
        assert node.retry_count == 0
        assert node.danger == DangerClassification.SAFE
        log.info("PASS: create node")

    def test_add_history(self):
        from runtime_next.models.node import NodeDefinition, NodeState
        node = NodeDefinition(id="N1", description="Test", specialist="FORGE")
        node.add_history(NodeState.PENDING, NodeState.RUNNING, "started")
        assert len(node.history) == 1
        assert node.history[0]["from"] == "pending"
        assert node.history[0]["to"] == "running"
        log.info("PASS: add history")

    def test_can_retry(self):
        from runtime_next.models.node import NodeDefinition
        node = NodeDefinition(id="N1", description="Test", specialist="FORGE", retry_budget=3, retry_count=2)
        assert node.can_retry()
        node.retry_count = 3
        assert not node.can_retry()
        log.info("PASS: can_retry logic")

    def test_next_backoff_exponential(self):
        from runtime_next.models.node import NodeDefinition
        node = NodeDefinition(id="N1", description="Test", specialist="FORGE", backoff_strategy="exponential")
        node.retry_count = 0
        assert node.next_backoff() == 1.0
        node.retry_count = 1
        assert node.next_backoff() == 2.0
        node.retry_count = 2
        assert node.next_backoff() == 4.0
        log.info("PASS: exponential backoff")

    def test_next_backoff_linear(self):
        from runtime_next.models.node import NodeDefinition
        from runtime_next.models.plan import RetryPolicy, RetryDelayStrategy
        node = NodeDefinition(id="N1", description="Test", specialist="FORGE",
                              retry_policy=RetryPolicy(delay_strategy=RetryDelayStrategy.LINEAR, base_delay_seconds=2.0))
        node.retry_count = 0
        assert node.next_backoff() == 2.0
        node.retry_count = 1
        assert node.next_backoff() == 4.0
        node.retry_count = 2
        assert node.next_backoff() == 6.0
        log.info("PASS: linear backoff")

    def test_updated_at_on_history(self):
        from runtime_next.models.node import NodeDefinition, NodeState
        import time
        node = NodeDefinition(id="N1", description="Test", specialist="FORGE")
        old = node.updated_at
        time.sleep(0.01)
        node.add_history(NodeState.PENDING, NodeState.RUNNING, "test")
        assert node.updated_at > old
        log.info("PASS: updated_at changes on history add")


class TestCapabilityModels:
    """Tests for capability models."""

    def test_capability_snapshot(self):
        from runtime_next.models.capability import CapabilitySnapshot, EnvironmentHealth, GitState
        snap = CapabilitySnapshot(
            workspace_path="/test",
            health=EnvironmentHealth.FULLY_OPERATIONAL,
            memory_usage_mb=100.0,
            disk_free_gb=50.0
        )
        assert snap.workspace_path == "/test"
        assert snap.health == EnvironmentHealth.FULLY_OPERATIONAL
        log.info("PASS: capability snapshot")

    def test_git_state(self):
        from runtime_next.models.capability import GitState
        g = GitState(branch="main", is_dirty=True, uncommitted_count=3, has_conflicts=False, stash_count=1, remote_configured=True)
        assert g.branch == "main"
        assert g.is_dirty
        log.info("PASS: git state")

    def test_event_types(self):
        from runtime_next.models.events import EventType
        assert EventType.NODE_TRANSITION.value == "node_transition"
        assert EventType.CAPABILITY_CHANGED.value == "capability_changed"
        assert EventType.RECOVERY_INITIATED.value == "recovery_initiated"
        log.info("PASS: event types")


if __name__ == "__main__":
    for cls in [TestNodeModel, TestCapabilityModels]:
        t = cls()
        for name in dir(t):
            if name.startswith("test_"):
                try:
                    getattr(t, name)()
                except Exception as e:
                    log.error(f"FAIL: {name}: {e}")
                    raise
