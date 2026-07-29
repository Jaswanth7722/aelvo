"""Tests for Layer 8 — Learned Recovery Memory."""

from runtime_next.verification.types import (
    RecoveryAction, FailureClassification, RecoveryStrategy,
)
from runtime_next.verification.memory import LearnedRecoveryMemory


class TestLearnedRecoveryMemory:
    """Tests for the LearnedRecoveryMemory."""

    def _make_strategy(self, sid="strat_test", name="Test strategy"):
        return RecoveryStrategy(
            id=sid, name=name,
            failure_type=FailureClassification.TIMEOUT,
        )

    def _make_action(self, aid="a1", sid="strat_test", nid="n1",
                     failure=FailureClassification.TIMEOUT, success=True):
        return RecoveryAction(
            id=aid, strategy_id=sid, node_id=nid,
            failure_classification=failure,
            action_type="retry", success=success, duration_ms=100,
        )

    def test_record_entry(self):
        memory = LearnedRecoveryMemory()
        import asyncio

        async def run():
            action = self._make_action()
            strategy = self._make_strategy()
            entry = await memory.record(action, strategy, True)
            return entry

        entry = asyncio.run(run())
        assert entry.failure_type == FailureClassification.TIMEOUT
        assert entry.success is True
        assert entry.id is not None

    def test_record_and_query(self):
        memory = LearnedRecoveryMemory()
        import asyncio

        async def run():
            action1 = self._make_action("a1", "s1", "n1",
                                        FailureClassification.TIMEOUT, True)
            action2 = self._make_action("a2", "s2", "n2",
                                        FailureClassification.SYNTAX_ERROR, False)
            strategy1 = RecoveryStrategy(id="s1", name="Timeout handler",
                                          failure_type=FailureClassification.TIMEOUT)
            strategy2 = RecoveryStrategy(id="s2", name="Syntax handler",
                                          failure_type=FailureClassification.SYNTAX_ERROR)

            await memory.record(action1, strategy1, True, {"project_name": "aelvo"})
            await memory.record(action2, strategy2, False, {"project_name": "aelvo"})

            assert memory.total_entries == 2
            assert memory.overall_success_rate == 0.5

            similar = await memory.find_similar_failures(
                FailureClassification.TIMEOUT
            )
            assert len(similar) >= 1

        asyncio.run(run())

    def test_success_rate_filtered(self):
        memory = LearnedRecoveryMemory()
        strategy = RecoveryStrategy(id="s1", name="Timeout handler",
                                      failure_type=FailureClassification.TIMEOUT)
        import asyncio

        async def run():
            for i in range(3):
                action = self._make_action(f"a{i}", "s1", "n1",
                                            FailureClassification.TIMEOUT, True)
                await memory.record(action, strategy, True)

            action_fail = self._make_action("a_fail", "s2", "n2",
                                             FailureClassification.SYNTAX_ERROR, False)
            strategy_fail = RecoveryStrategy(id="s2", name="Syntax handler",
                                              failure_type=FailureClassification.SYNTAX_ERROR)
            await memory.record(action_fail, strategy_fail, False)

            rate = await memory.success_rate(
                failure_type=FailureClassification.TIMEOUT
            )
            assert rate == 1.0

            rate_all = await memory.success_rate()
            assert rate_all == 0.75

        asyncio.run(run())

    def test_strategy_ranking(self):
        memory = LearnedRecoveryMemory()
        import asyncio

        async def run():
            good_strat = RecoveryStrategy(id="strat_good", name="Good",
                                           failure_type=FailureClassification.TIMEOUT)
            bad_strat = RecoveryStrategy(id="strat_bad", name="Bad",
                                          failure_type=FailureClassification.TIMEOUT)

            for i in range(3):
                action = self._make_action(f"a{i}", "strat_good", "n1",
                                            FailureClassification.TIMEOUT, True)
                await memory.record(action, good_strat, True)

            action = self._make_action("a_bad", "strat_bad", "n2",
                                        FailureClassification.TIMEOUT, False)
            await memory.record(action, bad_strat, False)

            ranking = await memory.strategy_ranking(
                FailureClassification.TIMEOUT
            )
            assert len(ranking) >= 2

            good_rank = [r for r in ranking if r[0] == "strat_good"]
            bad_rank = [r for r in ranking if r[0] == "strat_bad"]
            assert good_rank[0][1] > bad_rank[0][1]

        asyncio.run(run())

    def test_persistence(self, tmp_path):
        import asyncio
        storage_file = tmp_path / "recovery_memory.json"

        async def run():
            memory = LearnedRecoveryMemory(str(storage_file))
            action = self._make_action("a_persist", "s1", "n1",
                                        FailureClassification.TIMEOUT, True)
            strategy = RecoveryStrategy(id="s1", name="Timeout",
                                         failure_type=FailureClassification.TIMEOUT)
            await memory.record(action, strategy, True)
            assert memory.total_entries == 1

        asyncio.run(run())

        async def verify():
            memory2 = LearnedRecoveryMemory(str(storage_file))
            assert memory2.total_entries == 1

        asyncio.run(verify())

    def test_best_recovery_for(self):
        memory = LearnedRecoveryMemory()
        import asyncio

        async def run():
            action = self._make_action("a1", "strat_best", "n1",
                                        FailureClassification.TIMEOUT, True)
            strategy = RecoveryStrategy(id="strat_best", name="Best",
                                         failure_type=FailureClassification.TIMEOUT)
            await memory.record(action, strategy, True)

            best = await memory.best_recovery_for(
                FailureClassification.TIMEOUT
            )
            assert best is not None
            entry, score = best
            assert entry.recovery_strategy_id == "strat_best"
            assert score > 0

        asyncio.run(run())

    def test_clear(self):
        memory = LearnedRecoveryMemory()
        import asyncio

        async def run():
            action = self._make_action()
            strategy = self._make_strategy()
            await memory.record(action, strategy, True)
            assert memory.total_entries == 1

            memory.clear()
            assert memory.total_entries == 0

        asyncio.run(run())
