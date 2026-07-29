# tests/test_autonomous_learning.py — Phase 9: Autonomous Learning & Strategy Memory
#
# Tests for:
#   1. StrategicMemory.auto_store_from_outcome() — automatic learning extraction
#   2. StrategicMemory.find_relevant_strategies() — strategy retrieval for planning
#   3. StrategicMemory.decay_stale_entries() — proactive memory maintenance
#   4. StrategicMemory.consolidate_similar_entries() — auto-consolidation
#   5. AutonomousLearningPipeline — full post-execution pipeline
#   6. CognitiveEngine integration — report_execution_outcome()
#   7. Strategy injection in plan_goal()
#   8. No direct messaging (Amendment 2)

from __future__ import annotations

import time
import pytest
from typing import Dict, List
from datetime import datetime, timezone, timedelta

from cognition.strategy_memory import StrategicMemory
from cognition.autonomous_learning import AutonomousLearningPipeline
from cognition.engine import CognitiveEngine, CognitiveEngineConfig
from cognition.types import MemoryType, StrategicMemoryEntry


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def strategic_memory():
    """Create a fresh StrategicMemory instance."""
    return StrategicMemory()


@pytest.fixture
def learning_pipeline(strategic_memory):
    """Create an AutonomousLearningPipeline connected to strategic memory."""
    return AutonomousLearningPipeline(strategic_memory=strategic_memory)


@pytest.fixture
def cognitive_engine():
    """Create a CognitiveEngine with auto_learning enabled."""
    config = CognitiveEngineConfig(
        auto_learning=True,
        strategy_injection=True,
    )
    return CognitiveEngine(config=config)


@pytest.fixture
def populated_memory(strategic_memory):
    """StrategicMemory pre-populated with test entries."""
    strategic_memory.store(
        memory_type=MemoryType.SUCCESS_PATTERN,
        content="Successfully refactored auth module using dependency injection pattern",
        importance=0.7,
        tags=["refactor", "auth", "dependency-injection"],
    )
    strategic_memory.store(
        memory_type=MemoryType.FAILURE_PATTERN,
        content="Failed deployment due to missing environment variables in production config",
        importance=0.6,
        tags=["deploy", "env-config", "failure"],
    )
    strategic_memory.store(
        memory_type=MemoryType.REUSABLE_STRATEGY,
        content="Use factory pattern when creating provider-specific implementations",
        importance=0.5,
        tags=["factory", "provider", "pattern"],
    )
    strategic_memory.store(
        memory_type=MemoryType.DOMAIN_KNOWLEDGE,
        content="The authentication system uses OAuth2 with JWT tokens",
        importance=0.4,
        tags=["auth", "oauth", "jwt"],
    )
    return strategic_memory


# =========================================================================
# 1. Auto-Store from Outcome
# =========================================================================


class TestAutoStoreFromOutcome:
    """StrategicMemory.auto_store_from_outcome() — automatic learning extraction."""

    def test_stores_success_pattern(self, strategic_memory):
        entry = strategic_memory.auto_store_from_outcome(
            goal_description="Refactor auth module",
            outcome="success",
            specialist="FORGE",
            execution_summary="Successfully refactored auth with DI",
        )
        assert entry is not None
        assert entry.memory_type == MemoryType.SUCCESS_PATTERN
        assert entry.importance > 0.3
        assert "auto-learned" in entry.tags
        assert "forge" in entry.tags

    def test_stores_failure_pattern(self, strategic_memory):
        entry = strategic_memory.auto_store_from_outcome(
            goal_description="Deploy to production",
            outcome="failure",
            specialist="TERMINUS",
            execution_summary="Failed due to missing env vars",
        )
        assert entry is not None
        assert entry.memory_type == MemoryType.FAILURE_PATTERN
        assert entry.importance > 0.4

    def test_stores_reusable_strategy_for_unknown_outcomes(self, strategic_memory):
        entry = strategic_memory.auto_store_from_outcome(
            goal_description="Research API",
            outcome="partial",
            specialist="ORACLE",
            execution_summary="Found partial information",
        )
        assert entry is not None
        assert entry.memory_type == MemoryType.REUSABLE_STRATEGY

    def test_returns_none_for_empty_input(self, strategic_memory):
        entry = strategic_memory.auto_store_from_outcome(
            goal_description="",
            outcome="success",
        )
        assert entry is None

    def test_auto_store_increases_entry_count(self, strategic_memory):
        initial_count = strategic_memory.snapshot()["total_entries"]
        strategic_memory.auto_store_from_outcome(
            goal_description="Test goal",
            outcome="success",
        )
        assert strategic_memory.snapshot()["total_entries"] == initial_count + 1


# =========================================================================
# 2. Find Relevant Strategies
# =========================================================================


class TestFindRelevantStrategies:
    """StrategicMemory.find_relevant_strategies() — strategy retrieval for planning."""

    def test_finds_relevant_strategies(self, populated_memory):
        strategies = populated_memory.find_relevant_strategies(
            goal_description="refactor the auth module",
            max_results=5,
        )
        assert len(strategies) >= 1
        # The "successfully refactored auth module" entry should match
        assert any("auth module" in s.content for s in strategies)

    def test_returns_empty_for_no_match(self, strategic_memory):
        strategies = strategic_memory.find_relevant_strategies(
            goal_description="something completely unrelated",
        )
        assert len(strategies) == 0

    def test_respects_min_importance(self, populated_memory):
        strategies = populated_memory.find_relevant_strategies(
            goal_description="refactor auth",
            min_importance=0.8,  # higher than all entries
        )
        assert len(strategies) == 0

    def test_respects_max_results(self, populated_memory):
        strategies = populated_memory.find_relevant_strategies(
            goal_description="auth",
            max_results=1,
        )
        assert len(strategies) <= 1

    def test_returns_ranked_by_relevance(self, populated_memory):
        strategies = populated_memory.find_relevant_strategies(
            goal_description="deploy production with environment configuration",
            max_results=5,
        )
        if len(strategies) >= 2:
            # First result should be more relevant than second
            assert strategies[0].importance >= strategies[-1].importance


# =========================================================================
# 3. Decay Stale Entries
# =========================================================================


class TestDecayStaleEntries:
    """StrategicMemory.decay_stale_entries() — proactive memory maintenance."""

    def test_decays_stale_entries(self, strategic_memory):
        # Store an old entry with a manually set last_accessed
        entry = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Old pattern",
            importance=0.5,
        )
        # Set last_accessed to 60 days ago
        entry.last_accessed = datetime.now(timezone.utc) - timedelta(days=60)

        result = strategic_memory.decay_stale_entries(
            stale_days=30,
            decay_amount=0.1,
        )
        # Should have decayed (importance reduced)
        assert entry.importance < 0.5
        assert result != 0

    def test_prunes_very_stale_entries(self, strategic_memory):
        entry = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Very old pattern to prune",
            importance=0.09,  # Already below threshold
        )
        result = strategic_memory.decay_stale_entries(
            stale_days=1, decay_amount=0.01,
        )
        # Should be pruned (importance < 0.1)
        assert strategic_memory.snapshot()["total_entries"] == 0

    def test_does_not_decay_recent_entries(self, strategic_memory):
        entry = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Recent pattern",
            importance=0.7,
        )
        # Set last_accessed to just now
        entry.last_accessed = datetime.now(timezone.utc)

        result = strategic_memory.decay_stale_entries(
            stale_days=30,
            decay_amount=0.1,
        )
        # Should NOT have decayed
        assert entry.importance == 0.7

    def test_decay_returns_correct_count(self, strategic_memory):
        # Two old entries
        e1 = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Old pattern 1",
            importance=0.5,
        )
        e1.last_accessed = datetime.now(timezone.utc) - timedelta(days=60)

        e2 = strategic_memory.store(
            memory_type=MemoryType.FAILURE_PATTERN,
            content="Old pattern 2",
            importance=0.5,
        )
        e2.last_accessed = datetime.now(timezone.utc) - timedelta(days=60)

        result = strategic_memory.decay_stale_entries(
            stale_days=30, decay_amount=0.1,
        )
        # Both should be decayed
        assert result >= 2 or result <= -2  # decayed or pruned


# =========================================================================
# 4. Consolidate Similar Entries
# =========================================================================


class TestConsolidateSimilarEntries:
    """StrategicMemory.consolidate_similar_entries() — auto-consolidation."""

    def test_consolidates_similar_entries(self, strategic_memory):
        # Must have Jaccard similarity >= 0.5 to trigger consolidation
        # Intersection: {Refactored, authentication, module, dependency, injection} = 5
        # Union: {Refactored, authentication, module, using, dependency, injection, with, pattern} = 8
        # Jaccard: 5/8 = 0.625 >= 0.5
        e1 = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Refactored authentication module using dependency injection",
            importance=0.6,
        )
        e2 = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Refactored authentication module with dependency injection pattern",
            importance=0.5,
        )

        count = strategic_memory.consolidate_similar_entries(
            similarity_threshold=0.5,
            max_consolidations=1,
        )
        assert count >= 1

    def test_does_not_consolidate_dissimilar_entries(self, strategic_memory):
        e1 = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Refactored authentication module",
            importance=0.6,
        )
        e2 = strategic_memory.store(
            memory_type=MemoryType.FAILURE_PATTERN,
            content="Deployment failed due to network timeout",
            importance=0.5,
        )

        count = strategic_memory.consolidate_similar_entries(
            similarity_threshold=0.9,  # Very high threshold
            max_consolidations=1,
        )
        assert count == 0

    def test_consolidation_reduces_entry_count(self, strategic_memory):
        e1 = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Refactored auth module using dependency injection pattern",
            importance=0.6,
        )
        e2 = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Refactored auth module with dependency injection",
            importance=0.5,
        )

        initial_count = strategic_memory.snapshot()["total_entries"]
        strategic_memory.consolidate_similar_entries(
            similarity_threshold=0.5,
            max_consolidations=1,
        )
        # Consolidation should replace 2 entries with 1
        assert strategic_memory.snapshot()["total_entries"] < initial_count


# =========================================================================
# 5. AutonomousLearningPipeline
# =========================================================================


class TestAutonomousLearningPipeline:
    """Full autonomous learning pipeline."""

    def test_process_success_outcome(self, learning_pipeline, strategic_memory):
        results = learning_pipeline.process_execution_outcome(
            goal_description="Refactor auth module",
            outcome="success",
            specialist="FORGE",
            execution_summary="Successfully completed refactoring",
        )
        assert results["stored"] == 1
        assert strategic_memory.snapshot()["total_entries"] >= 1

    def test_process_failure_outcome(self, learning_pipeline, strategic_memory):
        results = learning_pipeline.process_execution_outcome(
            goal_description="Deploy to production",
            outcome="failure",
            specialist="TERMINUS",
            execution_summary="Deployment failed due to missing env vars",
        )
        assert results["stored"] == 1

    def test_reinforces_successful_strategies(self, learning_pipeline, strategic_memory):
        entry = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="A useful strategy",
            importance=0.5,
        )
        original_importance = entry.importance

        results = learning_pipeline.process_execution_outcome(
            goal_description="Test goal",
            outcome="success",
            successful_strategy_ids=[entry.id],
        )
        assert results["reinforced"] == 1
        assert entry.importance > original_importance

    def test_flags_failed_strategies(self, learning_pipeline, strategic_memory):
        entry = strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="A failing strategy",
            importance=0.5,
        )
        original_importance = entry.importance

        results = learning_pipeline.process_execution_outcome(
            goal_description="Test goal",
            outcome="failure",
            failed_strategy_ids=[entry.id],
        )
        assert results["flagged"] == 1
        assert entry.importance < original_importance

    def test_snapshot_returns_metrics(self, learning_pipeline):
        snapshot = learning_pipeline.snapshot()
        assert "turn_count" in snapshot
        assert "total_learnings_stored" in snapshot
        assert "total_reinforcements" in snapshot
        assert "total_failures_flagged" in snapshot

    def test_get_strategies_for_planning(self, learning_pipeline, strategic_memory):
        strategic_memory.store(
            memory_type=MemoryType.SUCCESS_PATTERN,
            content="Successfully deployed using CI/CD pipeline with automated tests",
            importance=0.7,
            tags=["deploy", "ci-cd"],
        )
        strategies = learning_pipeline.get_strategies_for_planning(
            goal_description="deploy with CI/CD",
            max_results=3,
        )
        assert len(strategies) >= 1


# =========================================================================
# 6. CognitiveEngine Integration
# =========================================================================


class TestCognitiveEngineIntegration:
    """CognitiveEngine integration with autonomous learning."""

    def test_engine_initializes_learning_pipeline(self, cognitive_engine):
        assert hasattr(cognitive_engine, "learning")
        assert cognitive_engine.learning is not None

    def test_report_execution_outcome_success(self, cognitive_engine):
        goal = cognitive_engine.submit_goal("Refactor auth module")
        results = cognitive_engine.report_execution_outcome(
            goal_id=goal.id,
            outcome="success",
            specialist="FORGE",
            execution_summary="Successfully refactored",
        )
        assert results["stored"] == 1

    def test_report_execution_outcome_failure(self, cognitive_engine):
        goal = cognitive_engine.submit_goal("Deploy to production")
        results = cognitive_engine.report_execution_outcome(
            goal_id=goal.id,
            outcome="failure",
            specialist="TERMINUS",
            execution_summary="Deployment failed",
        )
        assert results["stored"] == 1

    def test_report_does_not_store_when_disabled(self):
        config = CognitiveEngineConfig(auto_learning=False)
        engine = CognitiveEngine(config=config)
        goal = engine.submit_goal("Test goal")
        results = engine.report_execution_outcome(
            goal_id=goal.id,
            outcome="success",
        )
        assert results["stored"] == 0
        assert results["reinforced"] == 0

    def test_snapshot_includes_learning(self, cognitive_engine):
        snapshot = cognitive_engine.snapshot()
        assert "learning" in snapshot.metadata
        assert "turn_count" in snapshot.metadata["learning"]

    def test_status_includes_learning(self, cognitive_engine):
        status = cognitive_engine.status()
        assert "learning" in status
        assert "turn_count" in status["learning"]

    def test_engine_config_has_learning_flags(self):
        config = CognitiveEngineConfig()
        assert config.auto_learning is True
        assert config.strategy_injection is True


# =========================================================================
# 7. Content Similarity Helper
# =========================================================================


class TestContentSimilarity:
    """StrategicMemory._compute_content_similarity() helper."""

    def test_identical_content(self, strategic_memory):
        sim = strategic_memory._compute_content_similarity(
            "Refactored auth module using DI",
            "Refactored auth module using DI",
        )
        assert sim == 1.0

    def test_partial_overlap(self, strategic_memory):
        sim = strategic_memory._compute_content_similarity(
            "Refactored auth module using dependency injection",
            "Refactored auth module with DI pattern",
        )
        assert 0.3 < sim < 1.0

    def test_no_overlap(self, strategic_memory):
        sim = strategic_memory._compute_content_similarity(
            "Refactored auth module",
            "Deployment failed due to network timeout",
        )
        assert sim == 0.0

    def test_empty_strings(self, strategic_memory):
        sim = strategic_memory._compute_content_similarity("", "")
        assert sim == 0.0

    def test_case_insensitive(self, strategic_memory):
        sim = strategic_memory._compute_content_similarity(
            "REFACTORED AUTH MODULE",
            "refactored auth module",
        )
        assert sim == 1.0


# =========================================================================
# 8. No Direct Messaging (Amendment 2)
# =========================================================================


class TestNoDirectMessaging:
    """Verify that autonomous learning uses no agent-to-agent messaging."""

    def test_no_communication_router_in_learning(self):
        import inspect
        from cognition.autonomous_learning import AutonomousLearningPipeline
        source = inspect.getsource(AutonomousLearningPipeline)
        assert "send_message" not in source
        assert "AgentCommunicationRouter" not in source

    def test_no_communication_router_in_strategy_memory(self):
        import inspect
        from cognition.strategy_memory import StrategicMemory
        source = inspect.getsource(StrategicMemory)
        assert "send_message" not in source
        assert "AgentCommunicationRouter" not in source
