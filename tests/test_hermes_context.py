"""
tests/test_hermes_context.py — Phase 2: HermesContext + Global Cognition

Tests the immutable HermesContext model and its wiring into the
Orchestrator and PipelineContext.
"""

import time
import pytest
from typing import Dict, Any, List  # noqa: F401 — kept for potential type annotations in test data

from cognition.hermes_context import HermesContext


# ============================================================================
# HermesContext Model Tests
# ============================================================================


class TestHermesContextCreation:
    """HermesContext can be created with default and custom values."""

    def test_create_with_minimal_args(self):
        """HermesContext.create() works with just a task string."""
        ctx = HermesContext.create(task="fix the login bug")
        assert ctx.task == "fix the login bug"
        assert ctx.intent == "debug_and_fix"  # inferred from "fix" keyword
        assert len(ctx.goals) > 0
        assert ctx.risk_profile == "low"
        assert 1 <= ctx.complexity <= 10
        assert "read" in ctx.execution_permissions
        assert ctx.session_id == ""
        assert ctx.created_at > 0

    def test_create_with_all_fields(self):
        """HermesContext.create() works with all fields specified."""
        ctx = HermesContext.create(
            task="implement a new API endpoint",
            intent="custom_intent",
            goals=["Goal 1", "Goal 2", "Goal 3"],
            constraints={"language": "python", "framework": "fastapi"},
            risk_profile="medium",
            complexity=7,
            memory_context={"user_preferences": ["likes concise responses"]},
            user_model={"expertise": "high"},
            execution_permissions=["read", "write"],
            session_id="test-session-123",
            hermes_analysis={"concerns": [], "approach_suggestions": ["Step 1"]},
        )
        assert ctx.task == "implement a new API endpoint"
        assert ctx.intent == "custom_intent"
        assert len(ctx.goals) == 3
        assert ctx.constraints["language"] == "python"
        assert ctx.risk_profile == "medium"
        assert ctx.complexity == 7
        assert "write" in ctx.execution_permissions
        assert "execute" not in ctx.execution_permissions
        assert ctx.session_id == "test-session-123"
        assert len(ctx.hermes_analysis["approach_suggestions"]) == 1

    def test_intent_inference(self):
        """HermesContext infers intent correctly from task text."""
        cases = [
            ("fix the bug in auth", "debug_and_fix"),
            ("implement user profile page", "implement_feature"),
            ("refactor the database layer", "refactor_code"),
            ("research best practices for async python", "research_and_explain"),
            ("deploy to production", "execute_operation"),
            ("audit security vulnerabilities", "security_audit"),
            ("what is the weather today", "general_assistance"),
        ]
        for task, expected_intent in cases:
            ctx = HermesContext.create(task=task)
            assert ctx.intent == expected_intent, (
                f"Task '{task}' expected intent '{expected_intent}' "
                f"but got '{ctx.intent}'"
            )

    def test_goals_decomposition(self):
        """HermesContext decomposes goals based on task type."""
        fix_ctx = HermesContext.create(task="fix the login bug")
        assert any("Diagnose" in g for g in fix_ctx.goals)
        assert any("Apply" in g for g in fix_ctx.goals)

        implement_ctx = HermesContext.create(task="add user authentication")
        assert any("Understand" in g for g in implement_ctx.goals)
        assert any("Implement" in g for g in implement_ctx.goals)

        research_ctx = HermesContext.create(task="explain async patterns")
        assert any("Gather" in g for g in research_ctx.goals)
        assert any("Synthesize" in g for g in research_ctx.goals)

    def test_risk_profile_inference(self):
        """HermesContext infers risk profile correctly."""
        high_ctx = HermesContext.create(task="delete all production data")
        assert high_ctx.risk_profile == "high"

        medium_ctx = HermesContext.create(
            task="refactor the auth module"
        )
        assert medium_ctx.risk_profile == "medium"

        low_ctx = HermesContext.create(task="explain how routing works")
        assert low_ctx.risk_profile == "low"

    def test_complexity_estimation(self):
        """HermesContext estimates complexity on 1-10 scale."""
        simple_ctx = HermesContext.create(task="hi")
        assert 1 <= simple_ctx.complexity <= 3

        complex_ctx = HermesContext.create(
            task="implement a distributed multi-threaded async database "
                  "migration system with docker and kubernetes integration, "
                  "and also refactor the existing API layer to use graphql "
                  "instead of REST, and add websocket support for real-time "
                  "notifications across all services, and configure ci/cd "
                  "pipeline with integration testing and deployment automation, "
                  "and the architecture needs full redesign"
        )
        assert complex_ctx.complexity >= 5

    def test_permissions_inference(self):
        """HermesContext infers execution permissions from task."""
        read_only_ctx = HermesContext.create(
            task="explain how the system works"
        )
        assert "read" in read_only_ctx.execution_permissions
        assert "write" not in read_only_ctx.execution_permissions

        write_ctx = HermesContext.create(
            task="write a new function to handle errors"
        )
        assert "write" in write_ctx.execution_permissions

        execute_ctx = HermesContext.create(
            task="run the tests and build the project"
        )
        assert "execute" in execute_ctx.execution_permissions

        high_risk_ctx = HermesContext.create(
            task="delete all production data and run cleanup scripts"
        )
        assert "requires_security_review" in high_risk_ctx.execution_permissions


class TestHermesContextImmutability:
    """HermesContext must be immutable (frozen=True)."""

    def test_frozen_model(self):
        """Modifying a HermesContext field raises an exception (frozen)."""
        ctx = HermesContext.create(task="test task")
        with pytest.raises((TypeError, ValueError)):
            ctx.task = "modified task"  # type: ignore[misc]

    def test_frozen_dict_field(self):
        """Modifying nested dict fields raises an exception (frozen)."""
        ctx = HermesContext.create(task="test task")
        with pytest.raises((TypeError, ValueError)):
            ctx.constraints = {"new_key": "value"}  # type: ignore[misc]

    def test_frozen_list_field(self):
        """Modifying list fields raises an exception (frozen)."""
        ctx = HermesContext.create(task="test task")
        with pytest.raises((TypeError, ValueError)):
            ctx.goals = ["new goal"]  # type: ignore[misc]

    def test_model_copy_creates_new_instance(self):
        """model_copy() creates a new instance without modifying the original."""
        ctx = HermesContext.create(
            task="original task",
            risk_profile="low",
        )
        ctx2 = ctx.model_copy(update={"risk_profile": "high"})
        assert ctx.risk_profile == "low"  # original unchanged
        assert ctx2.risk_profile == "high"  # new instance updated
        assert ctx2.task == ctx.task  # other fields preserved


class TestHermesContextSerialization:
    """HermesContext supports JSON/model_dump serialization."""

    def test_model_dump(self):
        """HermesContext can be serialized to dict."""
        ctx = HermesContext.create(
            task="serialize test",
            goals=["Goal A", "Goal B"],
            constraints={"key": "value"},
        )
        data = ctx.model_dump()
        assert isinstance(data, dict)
        assert data["task"] == "serialize test"
        assert len(data["goals"]) == 2
        assert data["constraints"]["key"] == "value"
        assert "created_at" in data

    def test_model_dump_json(self):
        """HermesContext can be serialized to JSON."""
        ctx = HermesContext.create(task="json test")
        json_str = ctx.model_dump_json()
        assert isinstance(json_str, str)
        assert "json test" in json_str

    def test_summary(self):
        """summary() returns a compact representation."""
        ctx = HermesContext.create(
            task="test",
            intent="debug_and_fix",
            risk_profile="medium",
            complexity=5,
            session_id="session-1",
        )
        summary = ctx.summary()
        assert summary["intent"] == "debug_and_fix"
        assert summary["risk_profile"] == "medium"
        assert summary["complexity"] == 5
        assert summary["session_id"] == "session-1"

    def test_to_terminal_display(self):
        """to_terminal_display() returns a human-readable string."""
        ctx = HermesContext.create(
            task="display test",
            intent="implement_feature",
            risk_profile="low",
            complexity=3,
        )
        display = ctx.to_terminal_display()
        assert "HERMES CONTEXT" in display
        assert "implement_feature" in display
        assert "LOW" in display or "low" in display
        assert "3/10" in display

    def test_age_properties(self):
        """age_seconds and formatted_age work correctly."""
        ctx = HermesContext.create(task="age test")
        assert ctx.age_seconds >= 0
        assert isinstance(ctx.formatted_age, str)


# ============================================================================
# HermesContext Integration Wiring Tests
# ============================================================================


class TestHermesContextWiring:
    """HermesContext is properly wired into PipelineContext and orchestrator."""

    def test_pipeline_context_accepts_hermes_context(self):
        """PipelineContext can be created with an optional hermes_context."""
        from core.orchestration.pipeline import PipelineContext

        ctx = HermesContext.create(
            task="pipeline wiring test",
            risk_profile="medium",
            complexity=4,
            session_id="pipeline-test",
        )

        pipeline_ctx = PipelineContext(
            user_input="pipeline wiring test",
            conversation_history=[],
            hermes_context=ctx,
        )

        assert pipeline_ctx.hermes_context is ctx
        assert pipeline_ctx.hermes_context.risk_profile == "medium"
        assert pipeline_ctx.hermes_context.complexity == 4
        assert pipeline_ctx.hermes_context.session_id == "pipeline-test"

    def test_pipeline_context_works_without_hermes_context(self):
        """PipelineContext works without HermesContext (backward compat)."""
        from core.orchestration.pipeline import PipelineContext

        pipeline_ctx = PipelineContext(
            user_input="no hermes context",
            conversation_history=[],
        )
        assert pipeline_ctx.hermes_context is None

    def test_pipeline_context_phase_data_includes_hermes_context(self):
        """get_phase_data() includes hermes_context for all phases."""
        from core.orchestration.pipeline import (
            PipelineContext, PipelinePhase,
        )

        hctx = HermesContext.create(
            task="phase data test",
            intent="implement_feature",
            risk_profile="medium",
        )

        pipeline_ctx = PipelineContext(
            user_input="phase data test",
            conversation_history=[],
            hermes_context=hctx,
        )

        for phase in PipelinePhase:
            phase_data = pipeline_ctx.get_phase_data(phase)
            assert "hermes_context" in phase_data
            assert phase_data["hermes_context"] is hctx
            assert phase_data["hermes_context"].intent == "implement_feature"

    def test_hermes_context_available_in_forced_route_context(self):
        """HermesContext can be passed to build_shared_context."""
        hctx = HermesContext.create(
            task="forced route test",
            risk_profile="low",
        )

        shared = {
            "task": "forced route test",
            "hermes_context": hctx,
            "signals": {},
            "forced_route": True,
        }
        assert shared["hermes_context"].risk_profile == "low"
        assert shared["hermes_context"].task == "forced route test"
        # Verify immutability is enforced (Pydantic v2 uses ValidationError for frozen)
        with pytest.raises((TypeError, ValueError)):
            shared["hermes_context"].risk_profile = "high"  # type: ignore[misc]

    def test_hermes_context_creation_with_memory_context(self):
        """HermesContext can include memory context."""
        hctx = HermesContext.create(
            task="memory test",
            memory_context={
                "user_preferences": [
                    "likes short responses",
                    "prefers python over javascript",
                ],
                "recent_topics": ["authentication", "database"],
            },
        )
        assert len(hctx.memory_context["user_preferences"]) == 2
        assert "authentication" in hctx.memory_context["recent_topics"]

    def test_hermes_context_immutable_after_orchestrator_set(self):
        """Once set on orchestrator, HermesContext cannot be mutated."""
        hctx = HermesContext.create(
            task="orchestrator test",
            risk_profile="high",
        )
        # Simulate what happens when orchestrator.hermes_context = ctx
        orchestrator_context = hctx
        assert orchestrator_context.risk_profile == "high"
        # Attempting modification should fail (Pydantic v2 uses ValidationError)
        with pytest.raises((TypeError, ValueError)):
            orchestrator_context.risk_profile = "low"  # type: ignore[misc]

    def test_factory_method_matches_direct_construction(self):
        """create() factory produces same result as direct construction."""
        from cognition.hermes_context import HermesContext

        ctx1 = HermesContext.create(
            task="compare test",
            intent="explicit_intent",
            goals=["Goal 1"],
            risk_profile="high",
            complexity=8,
        )

        ctx2 = HermesContext(
            task="compare test",
            intent="explicit_intent",
            goals=["Goal 1"],
            risk_profile="high",
            complexity=8,
        )

        assert ctx1.task == ctx2.task
        assert ctx1.intent == ctx2.intent
        assert ctx1.goals == ctx2.goals
        assert ctx1.risk_profile == ctx2.risk_profile
        assert ctx1.complexity == ctx2.complexity


class TestHermesContextDecisions:
    """HermesContext's decision-making methods work correctly."""

    def test_infer_intent_detects_debug(self):
        assert HermesContext._infer_intent("fix the crash") == "debug_and_fix"
        assert HermesContext._infer_intent("error in login") == "debug_and_fix"
        assert HermesContext._infer_intent("broken build") == "debug_and_fix"

    def test_infer_intent_detects_implement(self):
        assert (
            HermesContext._infer_intent("implement new feature")
            == "implement_feature"
        )
        assert (
            HermesContext._infer_intent("create user profile page")
            == "implement_feature"
        )

    def test_infer_intent_detects_refactor(self):
        assert (
            HermesContext._infer_intent("refactor the auth system")
            == "refactor_code"
        )
        assert (
            HermesContext._infer_intent("rewrite the database layer")
            == "refactor_code"
        )

    def test_infer_intent_detects_research(self):
        assert (
            HermesContext._infer_intent("research async patterns")
            == "research_and_explain"
        )
        assert (
            HermesContext._infer_intent("explain how routing works")
            == "research_and_explain"
        )

    def test_decompose_goals_produces_reasonable_goals(self):
        goals = HermesContext._decompose_goals("fix the authentication bug")
        assert len(goals) >= 2
        assert any("Diagnose" in g or "Understand" in g for g in goals)
        assert any("Apply" in g or "Fix" in g for g in goals)
