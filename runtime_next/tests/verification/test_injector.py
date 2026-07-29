"""Tests for Layer 5 — Recovery Node Injection."""

from unittest.mock import MagicMock
from runtime_next.verification.types import (
    RecoveryAction, RecoveryStrategy, FailureClassification,
)


class _MockGraph:
    """Custom mock to avoid MagicMock dynamic attribute issues with hasattr."""
    def __init__(self):
        self.add_node = MagicMock(return_value=None)
        self.add_edge = MagicMock(return_value=None)


class TestRecoveryNodeInjector:
    """Tests for the RecoveryNodeInjector."""

    def _make_action(self):
        return RecoveryAction(
            id="action_001", strategy_id="strat_syntax", node_id="node_001",
            failure_classification=FailureClassification.SYNTAX_ERROR,
            action_type="retry", description="Retry with diagnostics",
        )

    def _make_strategy(self):
        return RecoveryStrategy(
            id="strat_syntax", name="Syntax retry",
            failure_type=FailureClassification.SYNTAX_ERROR,
            description="Reinvoke FORGE with diagnostics",
            danger_level="safe", max_retries=3,
        )

    def test_inject_recovery_node(self):
        from runtime_next.verification.injector import RecoveryNodeInjector

        injector = RecoveryNodeInjector()
        action = self._make_action()
        strategy = self._make_strategy()

        import asyncio

        async def run():
            graph = _MockGraph()

            node_id = await injector.inject_recovery_node(
                action, strategy, graph, {}
            )

            assert node_id is not None
            assert node_id.startswith("recover_node_001")
            graph.add_node.assert_called_once()
            graph.add_edge.assert_called_once()

        asyncio.run(run())

    def test_inject_rollback_node(self):
        from runtime_next.verification.injector import RecoveryNodeInjector

        injector = RecoveryNodeInjector()

        import asyncio

        async def run():
            graph = _MockGraph()

            node_id = await injector.inject_rollback_node(
                plan_id="plan_001",
                reason="Serialization failure",
                checkpoint_path="/tmp/checkpoint.json",
                nodes_affected=["n1", "n2"],
                graph=graph,
            )

            assert node_id is not None
            assert node_id.startswith("rollback_plan_001")
            graph.add_node.assert_called_once()

        asyncio.run(run())

    def test_injected_nodes_tracking(self):
        from runtime_next.verification.injector import RecoveryNodeInjector

        injector = RecoveryNodeInjector()
        action = self._make_action()
        strategy = self._make_strategy()

        import asyncio

        async def run():
            graph = _MockGraph()
            await injector.inject_recovery_node(action, strategy, graph, {})

            assert len(injector.injected_nodes) == 1

            injections = injector.get_injections_for_node("node_001")
            assert len(injections) == 1
            assert injections[0]["original_node_id"] == "node_001"

        asyncio.run(run())

    def test_graph_without_support(self):
        from runtime_next.verification.injector import RecoveryNodeInjector

        injector = RecoveryNodeInjector()
        action = self._make_action()
        strategy = self._make_strategy()

        import asyncio

        async def run():
            # Object without add_node or inject_node
            graph = object()

            node_id = await injector.inject_recovery_node(
                action, strategy, graph, {}
            )

            assert node_id is None  # Cannot inject

        asyncio.run(run())

    def test_clear(self):
        from runtime_next.verification.injector import RecoveryNodeInjector

        injector = RecoveryNodeInjector()
        action = self._make_action()
        strategy = self._make_strategy()

        import asyncio

        async def run():
            graph = _MockGraph()

            await injector.inject_recovery_node(action, strategy, graph, {})
            assert len(injector.injected_nodes) == 1

            injector.clear()
            assert len(injector.injected_nodes) == 0

        asyncio.run(run())
