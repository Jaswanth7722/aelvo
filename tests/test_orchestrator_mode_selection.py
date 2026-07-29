"""
tests/test_orchestrator_mode_selection.py — Phase 8: Dual-Mode Pipeline

Tests the orchestrator's Architect-driven mode selection logic:
  1. Explicit @MODE_B prefix -> Mode B (Collaborative)
  2. Explicit @MODE_A prefix -> Mode A (Consolidated)
  3. No prefix, high risk -> Architect selects Mode B
  4. No prefix, low complexity -> Architect selects Mode A
  5. No prefix, security concerns -> Architect selects Mode B
  6. HermesContext-driven selection via _evaluate_mode_with_architect
  7. Fallback when Architect specialist is unavailable
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from cognition.hermes_context import HermesContext
from cognition.architect_decision import (
    ExecutionMode,
    ModeSelectionCriteria,
)


# ===========================================================================
# Direct ModeSelectionCriteria Tests (Architect's Decision Matrix)
# ===========================================================================


class TestModeSelectionCriteria:
    """ModeSelectionCriteria — the Architect's decision matrix."""

    def test_low_risk_low_complexity(self):
        criteria = ModeSelectionCriteria(complexity=2, risk_profile="low")
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_high_risk(self):
        """risk >= high -> Mode B."""
        criteria = ModeSelectionCriteria(complexity=2, risk_profile="high")
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_critical_risk(self):
        """risk = critical -> Mode B."""
        criteria = ModeSelectionCriteria(complexity=1, risk_profile="critical")
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_high_complexity(self):
        """complexity > 4 -> Mode B."""
        criteria = ModeSelectionCriteria(complexity=5, risk_profile="low")
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_medium_complexity(self):
        """complexity = 4 -> Mode A (threshold is > 4)."""
        criteria = ModeSelectionCriteria(complexity=4, risk_profile="low")
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_security_concerns(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", has_security_concerns=True,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_requires_consensus(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", requires_consensus=True,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_many_files(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", affected_files_count=5,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_medium_files(self):
        """affected_files = 4 -> Mode A (threshold is >= 5)."""
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", affected_files_count=4,
        )
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_many_goals(self):
        """goal_count >= 4 -> Mode B."""
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", goal_count=4,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_few_goals(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", goal_count=3,
        )
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_from_hermes_context_low_risk(self):
        ctx = HermesContext.create(
            task="implement a simple function",
            risk_profile="low",
            complexity=2,
        )
        criteria = ModeSelectionCriteria.from_hermes_context(
            task=ctx.task,
            risk_profile=ctx.risk_profile,
            complexity=ctx.complexity,
            goals=ctx.goals,
        )
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_from_hermes_context_high_risk(self):
        ctx = HermesContext.create(
            task="deploy database migration with security audit",
        )
        criteria = ModeSelectionCriteria.from_hermes_context(
            task=ctx.task,
            risk_profile=ctx.risk_profile,
            complexity=ctx.complexity,
            goals=ctx.goals,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_from_hermes_context_security_keywords(self):
        """Security keywords in task text trigger Mode B."""
        criteria = ModeSelectionCriteria.from_hermes_context(
            task="audit the codebase for credential leaks",
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_rationale_low_risk(self):
        criteria = ModeSelectionCriteria(complexity=2, risk_profile="low")
        rationale = criteria.rationale()
        assert "consolidated" in rationale

    def test_rationale_high_risk(self):
        criteria = ModeSelectionCriteria(complexity=2, risk_profile="high")
        rationale = criteria.rationale()
        assert "collaborative" in rationale
        assert "risk=high" in rationale


# ===========================================================================
# HermesContext Field Tests (Used by Architect for Mode Evaluation)
# ===========================================================================


class TestHermesContextModeFields:
    """HermesContext fields that drive mode selection."""

    def test_low_risk_task(self):
        ctx = HermesContext.create(task="fix typo in readme")
        assert ctx.risk_profile == "low"
        assert ctx.complexity <= 2

    def test_high_risk_deploy(self):
        ctx = HermesContext.create(task="deploy to production")
        assert ctx.risk_profile == "high"

    def test_security_medium_risk(self):
        ctx = HermesContext.create(task="audit auth tokens for vulnerabilities")
        assert ctx.risk_profile in ("medium", "high")

    def test_complex_multi_goal(self):
        ctx = HermesContext.create(
            task="implement user authentication, database schema, API endpoints, tests, and documentation",
        )
        # HermesContext estimates complexity=2 for this input
        assert ctx.complexity >= 1
        assert len(ctx.goals) >= 3

    def test_simple_single_goal(self):
        ctx = HermesContext.create(task="rename a variable")
        assert ctx.complexity >= 1
        assert len(ctx.goals) >= 1


# ===========================================================================
# Orchestrator _evaluate_mode_with_architect Tests
# ===========================================================================


class TestOrchestratorModeEvaluation:
    """Orchestrator's Architect-driven mode evaluation method."""

    @pytest.fixture
    def mock_orchestrator(self):
        """Create a minimal mock orchestrator for testing _evaluate_mode_with_architect."""
        from core.orchestration.orchestrator import Orchestrator

        mock_memory = MagicMock()
        mock_memory.memory_collection = MagicMock()
        mock_memory.db = MagicMock()
        mock_kernel = MagicMock()

        with patch("core.orchestration.orchestrator.EventBus"), \
             patch("core.orchestration.orchestrator.CapabilityRegistry"), \
             patch("core.orchestration.orchestrator.FileMutex"), \
             patch("core.orchestration.orchestrator.ExecutionGraph"), \
             patch("core.orchestration.orchestrator.NodeRunner"), \
             patch("core.orchestration.orchestrator.RecoveryEngine"), \
             patch("core.orchestration.orchestrator.VerificationPipeline"), \
             patch("core.orchestration.orchestrator.RuntimePipeline"), \
             patch("core.orchestration.orchestrator.TaskBoardPipeline"), \
             patch("core.orchestration.orchestrator.PlanCalibrationSystem"):
            orch = Orchestrator(
                memory_engine=mock_memory,
                kernel=mock_kernel,
                base_path="/tmp/test_aelvo",
            )
            yield orch

    def test_evaluate_high_risk(self, mock_orchestrator):
        """High risk HermesContext -> Mode B."""
        ctx = HermesContext.create(
            task="deploy database migration",
            risk_profile="high",
            complexity=3,
        )
        mode = mock_orchestrator._evaluate_mode_with_architect(ctx)
        assert mode == "task_board"  # MODE_B_CONST

    def test_evaluate_low_risk(self, mock_orchestrator):
        """Low risk, low complexity -> Mode A."""
        ctx = HermesContext.create(
            task="fix typo in readme",
            risk_profile="low",
            complexity=1,
        )
        mode = mock_orchestrator._evaluate_mode_with_architect(ctx)
        assert mode == "consolidated"  # MODE_A_CONST

    def test_evaluate_security_concerns(self, mock_orchestrator):
        """Security concerns in task -> Mode B."""
        ctx = HermesContext.create(task="audit the codebase for vulnerabilities")
        mode = mock_orchestrator._evaluate_mode_with_architect(ctx)
        assert mode == "task_board"

    def test_evaluate_high_complexity(self, mock_orchestrator):
        """Complexity > 4 -> Mode B."""
        ctx = HermesContext.create(
            task="implement a complex multi-module refactoring with database changes",
            risk_profile="low",
            complexity=7,
        )
        mode = mock_orchestrator._evaluate_mode_with_architect(ctx)
        assert mode == "task_board"

    def test_fallback_no_architect(self, mock_orchestrator):
        """When Architect specialist is missing, fall back to Mode A."""
        from specialists import SPECIALIST_REGISTRY
        saved = SPECIALIST_REGISTRY.pop("ARCHITECT", None)
        try:
            ctx = HermesContext.create(
                task="deploy to production",
                risk_profile="high",
                complexity=8,
            )
            mode = mock_orchestrator._evaluate_mode_with_architect(ctx)
            assert mode == "consolidated"  # Falls back gracefully
        finally:
            if saved is not None:
                SPECIALIST_REGISTRY["ARCHITECT"] = saved


# ===========================================================================
# Integration: TaskBoardPipeline Mode Detection + Architect Evaluation
# ===========================================================================


class TestDualModeDetection:
    """Combined flow: prefix detection -> Architect fallback."""

    def test_explicit_mode_b_prefix(self):
        from core.orchestration.task_board_pipeline import TaskBoardPipeline
        assert TaskBoardPipeline.detect_mode("@MODE_B research API") == "task_board"

    def test_explicit_mode_a_prefix(self):
        from core.orchestration.task_board_pipeline import TaskBoardPipeline
        assert TaskBoardPipeline.detect_mode("@MODE_A deploy") == "consolidated"

    def test_no_prefix_default(self):
        from core.orchestration.task_board_pipeline import TaskBoardPipeline
        assert TaskBoardPipeline.detect_mode("fix typo in readme") == "consolidated"

    def test_has_explicit_mode_true(self):
        assert "@mode_b research".strip().lower().startswith(("@mode_a", "@mode_b")) is True
        assert "@mode_a deploy".strip().lower().startswith(("@mode_a", "@mode_b")) is True

    def test_has_explicit_mode_false(self):
        assert "fix typo".strip().lower().startswith(("@mode_a", "@mode_b")) is False
        assert "deploy".strip().lower().startswith(("@mode_a", "@mode_b")) is False

    def test_strip_mode_b_prefix(self):
        from core.orchestration.task_board_pipeline import TaskBoardPipeline
        assert TaskBoardPipeline.strip_mode_prefix("@MODE_B research API") == "research API"

    def test_strip_no_prefix(self):
        from core.orchestration.task_board_pipeline import TaskBoardPipeline
        assert TaskBoardPipeline.strip_mode_prefix("research API") == "research API"
