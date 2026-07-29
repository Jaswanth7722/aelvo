"""tests/test_collaboration_orchestrator.py — Phase 12: Multi-Agent Collaboration & Task Board Routing

Tests the CollaborationOrchestrator, CollaborationSession, IntelligentRouter,
and their integration with SharedTaskBoard, CognitiveBlackboard, and consensus.
"""

import time
import pytest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, PropertyMock

from shared_task_board.task import Task, TaskStatus, TaskType, TaskPriority
from shared_task_board.board import SharedTaskBoard, TaskBoardConfig
from shared_task_board.collaboration_orchestrator import (
    CollaborationSession,
    CollaborationOrchestrator,
    IntelligentRouter,
    RoutingDecision,
    RoutingStrategy,
    SessionStatus,
    CollaborationPhase,
    SessionTaskRecord,
)

from cognition.blackboard import CognitiveBlackboard
from cognition.consensus import MultiAgentConsensusSystem
from cognition.coordination import SpecialistCoordinationRuntime, DelegationMode
from cognition.types import (
    ConflictRecord, ConflictSeverity, EntryType, Provenance, ProvenanceType,
)


# ============================================================================
# Fixtures
# ============================================================================


class MockSpecialist:
    """A minimal mock specialist for routing tests."""
    def __init__(self, name: str, trigger_patterns: Optional[List[str]] = None):
        self.name = name
        self.trigger_patterns = trigger_patterns or []

    def compute_activation_score(self, task: str, context: Dict[str, Any]) -> float:
        task_lower = task.lower()
        matches = sum(1 for p in self.trigger_patterns if p.lower() in task_lower)
        if matches > 0:
            return min(1.0, matches * 0.35)
        return 0.1


@pytest.fixture
def task_board() -> SharedTaskBoard:
    config = TaskBoardConfig(db_path="", auto_persist=False, enable_events=False)
    b = SharedTaskBoard(config)
    yield b
    b.clear()


@pytest.fixture
def blackboard() -> CognitiveBlackboard:
    return CognitiveBlackboard()


@pytest.fixture
def consensus() -> MultiAgentConsensusSystem:
    return MultiAgentConsensusSystem()


@pytest.fixture
def coordination() -> SpecialistCoordinationRuntime:
    registry = {
        "ORACLE": MockSpecialist("ORACLE", ["research", "find", "search"]),
        "FORGE": MockSpecialist("FORGE", ["implement", "refactor", "write", "code"]),
        "SENTINEL": MockSpecialist("SENTINEL", ["security", "review", "audit"]),
        "TERMINUS": MockSpecialist("TERMINUS", ["execute", "run", "deploy"]),
        "HERALD": MockSpecialist("HERALD", ["summarize", "report", "synthesize"]),
        "HERMES": MockSpecialist("HERMES", ["analyze", "understand", "context"]),
        "ARCHITECT": MockSpecialist("ARCHITECT", ["plan", "design", "architect"]),
    }
    return SpecialistCoordinationRuntime(specialist_registry=registry)


@pytest.fixture
def router(coordination) -> IntelligentRouter:
    registry = {
        "ORACLE": MockSpecialist("ORACLE", ["research", "find", "search"]),
        "FORGE": MockSpecialist("FORGE", ["implement", "refactor", "write", "code"]),
        "SENTINEL": MockSpecialist("SENTINEL", ["security", "review", "audit"]),
        "TERMINUS": MockSpecialist("TERMINUS", ["execute", "run", "deploy"]),
        "HERALD": MockSpecialist("HERALD", ["summarize", "report", "synthesize"]),
        "HERMES": MockSpecialist("HERMES", ["analyze", "understand", "context"]),
        "ARCHITECT": MockSpecialist("ARCHITECT", ["plan", "design", "architect"]),
    }
    return IntelligentRouter(
        specialist_registry=registry,
        coordination=coordination,
    )


@pytest.fixture
def orchestrator(task_board, blackboard, consensus, coordination, router) -> CollaborationOrchestrator:
    return CollaborationOrchestrator(
        task_board=task_board,
        blackboard=blackboard,
        consensus=consensus,
        coordination=coordination,
        router=router,
    )


# ============================================================================
# CollaborationSession Tests
# ============================================================================


class TestCollaborationSession:
    """CollaborationSession model creation and methods."""

    def test_create_session(self):
        """A session can be created with minimal args."""
        session = CollaborationSession(
            session_id="test_001",
            goal_description="Implement authentication system",
        )
        assert session.session_id == "test_001"
        assert session.goal_description == "Implement authentication system"
        assert session.status == SessionStatus.PENDING
        assert session.phase == CollaborationPhase.PLANNING
        assert session.tasks == []
        assert session.specialist_participants == set()

    def test_add_task(self):
        """add_task() records a task and tracks the specialist."""
        session = CollaborationSession(
            session_id="test_002",
            goal_description="Refactor codebase",
        )
        record = SessionTaskRecord(
            task_id="t1",
            specialist="FORGE",
            task_type="implement",
        )
        session.add_task(record)
        assert len(session.tasks) == 1
        assert "FORGE" in session.specialist_participants

    def test_get_task(self):
        """get_task() retrieves a task record by ID."""
        session = CollaborationSession(session_id="s1", goal_description="Test")
        session.add_task(SessionTaskRecord(task_id="t1", specialist="A", task_type="type1"))
        session.add_task(SessionTaskRecord(task_id="t2", specialist="B", task_type="type2"))

        t = session.get_task("t1")
        assert t is not None
        assert t.task_id == "t1"
        assert t.specialist == "A"

        assert session.get_task("nonexistent") is None

    def test_get_tasks_by_specialist(self):
        """get_tasks_by_specialist() filters by specialist."""
        session = CollaborationSession(session_id="s1", goal_description="Test")
        session.add_task(SessionTaskRecord(task_id="t1", specialist="FORGE", task_type="impl"))
        session.add_task(SessionTaskRecord(task_id="t2", specialist="ORACLE", task_type="research"))
        session.add_task(SessionTaskRecord(task_id="t3", specialist="FORGE", task_type="test"))

        forge_tasks = session.get_tasks_by_specialist("FORGE")
        assert len(forge_tasks) == 2

        oracle_tasks = session.get_tasks_by_specialist("ORACLE")
        assert len(oracle_tasks) == 1

    def test_get_tasks_by_status(self):
        """get_tasks_by_status() filters by status."""
        session = CollaborationSession(session_id="s1", goal_description="Test")
        session.add_task(SessionTaskRecord(task_id="t1", specialist="A", task_type="t", status="pending"))
        session.add_task(SessionTaskRecord(task_id="t2", specialist="B", task_type="t", status="completed"))
        session.add_task(SessionTaskRecord(task_id="t3", specialist="C", task_type="t", status="completed"))

        completed = session.get_tasks_by_status("completed")
        assert len(completed) == 2

        pending = session.get_tasks_by_status("pending")
        assert len(pending) == 1

    def test_progress_ratio(self):
        """progress_ratio reflects completed/total."""
        session = CollaborationSession(session_id="s1", goal_description="Test")
        assert session.progress_ratio == 0.0

        session.add_task(SessionTaskRecord(task_id="t1", specialist="A", task_type="t", status="completed"))
        assert session.progress_ratio == 1.0

        session.add_task(SessionTaskRecord(task_id="t2", specialist="B", task_type="t", status="pending"))
        assert session.progress_ratio == 0.5

    def test_duration(self):
        """duration_seconds is None for active sessions, set for completed."""
        session = CollaborationSession(session_id="s1", goal_description="Test")
        assert session.duration_seconds is None

        session.completed_at = session.created_at + 10.0
        assert session.duration_seconds == 10.0

    def test_summary(self):
        """summary() returns compact dict with key fields."""
        session = CollaborationSession(session_id="s1", goal_description="Implement auth")
        session.add_task(SessionTaskRecord(task_id="t1", specialist="FORGE", task_type="implement", status="completed"))
        session.add_task(SessionTaskRecord(task_id="t2", specialist="SENTINEL", task_type="security_review", status="pending"))

        summary = session.summary()
        assert summary["session_id"] == "s1"
        assert summary["tasks"] == 2
        assert summary["completed"] == 1
        assert summary["progress"] == 0.5
        assert "FORGE" in summary["specialists"]
        assert "SENTINEL" in summary["specialists"]

    def test_to_terminal_display(self):
        """to_terminal_display() returns human-readable output."""
        session = CollaborationSession(session_id="test_display", goal_description="Build feature")
        session.add_task(SessionTaskRecord(task_id="t1", specialist="FORGE", task_type="implement", status="in_progress"))
        display = session.to_terminal_display()
        assert "Collaboration Session" in display
        assert "Build feature" in display
        assert "FORGE" in display

    def test_session_participants_tracked_unique(self):
        """Specialist participants are tracked as a set (no duplicates)."""
        session = CollaborationSession(session_id="s1", goal_description="Test")
        session.add_task(SessionTaskRecord(task_id="t1", specialist="FORGE", task_type="impl"))
        session.add_task(SessionTaskRecord(task_id="t2", specialist="FORGE", task_type="test"))
        session.add_task(SessionTaskRecord(task_id="t3", specialist="ORACLE", task_type="research"))

        assert len(session.specialist_participants) == 2

    def test_serialize_round_trip(self):
        """CollaborationSession can be serialized and reconstructed."""
        session = CollaborationSession(session_id="s1", goal_description="Test")
        session.add_task(SessionTaskRecord(task_id="t1", specialist="FORGE", task_type="impl", status="completed"))
        data = session.model_dump()
        restored = CollaborationSession(**data)
        assert restored.session_id == "s1"
        assert len(restored.tasks) == 1
        assert restored.tasks[0].specialist == "FORGE"


# ============================================================================
# IntelligentRouter Tests
# ============================================================================


class TestIntelligentRouter:
    """IntelligentRouter routing logic."""

    def test_route_explicit_assignment(self, router):
        """Route with preferred_specialist uses explicit assignment."""
        task = Task.create(task_type=TaskType.IMPLEMENT, title="Write auth module")

        decision = router.route(task, preferred_specialist="FORGE")
        assert decision.selected_specialist == "FORGE"
        assert decision.strategy == RoutingStrategy.EXPLICIT_ASSIGNMENT

    def test_route_capability_match(self, router):
        """Route matches by capability when patterns align."""
        task = Task.create(task_type=TaskType.RESEARCH, title="Research async patterns")

        decision = router.route(task)
        assert decision.selected_specialist == "ORACLE"
        assert decision.capability_score > 0

    def test_route_fallback(self, router):
        """Route falls back when no specialists available."""
        empty_router = IntelligentRouter(specialist_registry={})
        task = Task.create(task_type=TaskType.GENERAL, title="Any task")

        decision = empty_router.route(task)
        assert decision.selected_specialist == "HERMES"
        assert decision.strategy == RoutingStrategy.FALLBACK

    def test_route_includes_rationale(self, router):
        """Routing decision includes rationale string."""
        task = Task.create(task_type=TaskType.IMPLEMENT, title="Implement feature")

        decision = router.route(task)
        assert len(decision.rationale) > 10

    def test_route_alternatives(self, router):
        """Routing decision includes alternative specialists."""
        task = Task.create(task_type=TaskType.RESEARCH, title="Research topic")

        decision = router.route(task)
        assert len(decision.alternatives) > 0

    def test_route_improves_with_performance(self, router, coordination):
        """Routing considers performance history."""
        # Give ORACLE a strong performance record
        for _ in range(5):
            coordination.score_delegation("node_1", success=True, score=0.9)

        task = Task.create(task_type=TaskType.RESEARCH, title="Research topic")

        decision = router.route(task)
        assert decision.selected_specialist == "ORACLE"
        assert decision.performance_score > 0.0

    def test_route_batch_distributes_workload(self, router):
        """route_batch handles multiple tasks with workload tracking."""
        tasks = [
            Task.create(task_type=TaskType.RESEARCH, title="Research topic A"),
            Task.create(task_type=TaskType.IMPLEMENT, title="Implement feature B"),
            Task.create(task_type=TaskType.SECURITY_REVIEW, title="Audit security C"),
        ]

        decisions = router.route_batch(tasks)
        assert len(decisions) == 3
        # Each task should be routed to a different specialist (different types)
        specialists = [d.selected_specialist for d in decisions]
        assert len(set(specialists)) >= 2

    def test_router_snapshot(self, router):
        """snapshot() returns route statistics."""
        task = Task.create(task_type=TaskType.RESEARCH, title="Research")
        router.route(task)

        snap = router.snapshot()
        assert snap["total_routes"] >= 1
        assert snap["specialists_registered"] == 7
        assert "routes_by_specialist" in snap
        assert "strategy_breakdown" in snap

    def test_route_history(self, router):
        """get_routing_history() returns recent decisions."""
        for i in range(3):
            task = Task.create(task_type=TaskType.RESEARCH, title=f"Research {i}")
            router.route(task)

        history = router.get_routing_history(limit=2)
        assert len(history) == 2

    def test_get_specialist_route_counts(self, router):
        """get_specialist_route_counts() returns per-specialist counts."""
        t1 = Task.create(task_type=TaskType.RESEARCH, title="Research")
        router.route(t1, preferred_specialist="ORACLE")

        t2 = Task.create(task_type=TaskType.IMPLEMENT, title="Implement")
        router.route(t2, preferred_specialist="FORGE")

        counts = router.get_specialist_route_counts()
        assert counts.get("ORACLE") == 1
        assert counts.get("FORGE") == 1

    def test_route_with_complexity_override(self, router):
        """Route accepts complexity_override."""
        task = Task.create(task_type=TaskType.RESEARCH, title="Simple task")
        decision = router.route(task, complexity_override=1)
        assert decision.complexity_estimate == 1

        decision2 = router.route(task, complexity_override=10)
        assert decision2.complexity_estimate == 10


# ============================================================================
# CollaborationOrchestrator Tests
# ============================================================================


class TestCollaborationOrchestrator:
    """CollaborationOrchestrator session management and integration."""

    def test_create_session(self, orchestrator):
        """create_session() returns a configured session."""
        session = orchestrator.create_session(
            goal_description="Implement JWT authentication",
            created_by="user",
        )
        assert session.session_id is not None
        assert session.goal_description == "Implement JWT authentication"
        assert session.status == SessionStatus.PENDING
        assert session.created_by == "user"
        assert session.blackboard_slot.startswith("session_")

    def test_get_session(self, orchestrator):
        """get_session() retrieves a session by ID."""
        created = orchestrator.create_session(goal_description="Test goal")
        retrieved = orchestrator.get_session(created.session_id)
        assert retrieved is not None
        assert retrieved.session_id == created.session_id

    def test_get_session_not_found(self, orchestrator):
        """get_session() returns None for unknown ID."""
        assert orchestrator.get_session("nonexistent") is None

    def test_get_active_sessions(self, orchestrator):
        """get_active_sessions() returns only active sessions."""
        s1 = orchestrator.create_session(goal_description="Active goal")
        s2 = orchestrator.create_session(goal_description="Another active goal")

        # Create a task in s1 to activate it
        orchestrator.create_routed_task(
            s1.session_id, TaskType.RESEARCH, "Research task",
        )

        active = orchestrator.get_active_sessions()
        assert len(active) >= 1

    def test_close_session_completed(self, orchestrator):
        """close_session() marks session as completed."""
        session = orchestrator.create_session(goal_description="Test completion")
        closed = orchestrator.close_session(session.session_id, SessionStatus.COMPLETED)
        assert closed is not None
        assert closed.status == SessionStatus.COMPLETED
        assert closed.completed_at is not None

    def test_close_session_not_found(self, orchestrator):
        """close_session() returns None for unknown ID."""
        result = orchestrator.close_session("nonexistent")
        assert result is None

    def test_create_routed_task(self, orchestrator):
        """create_routed_task() creates a task and routes it."""
        session = orchestrator.create_session(
            goal_description="Build authentication system",
        )

        task, decision, session_after = orchestrator.create_routed_task(
            session_id=session.session_id,
            task_type=TaskType.RESEARCH,
            title="Research auth libraries",
            description="Find best auth libraries for the project",
            priority="high",
        )

        assert task.id is not None
        assert task.title == "Research auth libraries"
        assert decision.selected_specialist == "ORACLE"
        assert len(session_after.tasks) == 1
        assert session_after.status == SessionStatus.ACTIVE

    def test_create_routed_task_invalid_session(self, orchestrator):
        """create_routed_task() raises ValueError for invalid session."""
        with pytest.raises(ValueError, match="Session nonexistent not found"):
            orchestrator.create_routed_task(
                session_id="nonexistent",
                task_type=TaskType.RESEARCH,
                title="Task",
            )

    def test_create_routed_task_with_preferred_specialist(self, orchestrator):
        """create_routed_task() accepts preferred_specialist override."""
        session = orchestrator.create_session(goal_description="Test")

        task, decision, _ = orchestrator.create_routed_task(
            session_id=session.session_id,
            task_type=TaskType.IMPLEMENT,
            title="Implement feature",
            preferred_specialist="ARCHITECT",
        )

        assert decision.selected_specialist == "ARCHITECT"
        assert decision.strategy == RoutingStrategy.EXPLICIT_ASSIGNMENT

    def test_on_task_completed(self, orchestrator):
        """on_task_completed() updates session state."""
        session = orchestrator.create_session(goal_description="Test")
        task, decision, _ = orchestrator.create_routed_task(
            session.session_id, TaskType.RESEARCH, "Research task",
        )

        # Start the task first so the board transition is valid
        orchestrator.on_task_started(task.id, session.session_id)

        result = orchestrator.on_task_completed(
            task.id, session.session_id,
            result={"findings": "Found libraries"},
        )

        assert result is not None
        record = result.get_task(task.id)
        assert record is not None
        assert record.status == "completed"
        assert record.completed_at is not None

        # Task should be completed on the board too
        board_task = orchestrator.task_board.get_task(task.id)
        assert board_task.status == TaskStatus.COMPLETED

    def test_on_task_failed(self, orchestrator):
        """on_task_failed() updates session state and logs error."""
        session = orchestrator.create_session(goal_description="Test")
        task, decision, _ = orchestrator.create_routed_task(
            session.session_id, TaskType.IMPLEMENT, "Implement feature",
        )

        # Start the task first so the board transition is valid
        orchestrator.on_task_started(task.id, session.session_id)

        result = orchestrator.on_task_failed(
            task.id, session.session_id,
            error="Build failed",
            failure_reason="Dependency not found",
        )

        assert result is not None
        record = result.get_task(task.id)
        assert record is not None
        assert record.status == "failed"
        assert record.error == "Build failed"

        # Task should be failed on the board too
        board_task = orchestrator.task_board.get_task(task.id)
        assert board_task.status == TaskStatus.FAILED

    def test_on_task_started(self, orchestrator):
        """on_task_started() updates session record."""
        session = orchestrator.create_session(goal_description="Test")
        task, decision, _ = orchestrator.create_routed_task(
            session.session_id, TaskType.RESEARCH, "Research task",
        )

        result = orchestrator.on_task_started(task.id, session.session_id)
        assert result is not None
        record = result.get_task(task.id)
        assert record is not None
        assert record.status == "in_progress"
        assert record.started_at is not None

    def test_on_task_completed_invalid_session(self, orchestrator):
        """on_task_completed() returns None for unknown session."""
        result = orchestrator.on_task_completed("task_1", "nonexistent")
        assert result is None

    def test_session_blocks_on_multiple_failures(self, orchestrator):
        """Session status changes to BLOCKED after 3+ failures."""
        session = orchestrator.create_session(goal_description="Test")

        for i in range(3):
            task = orchestrator.task_board.create_task(
                task_type=TaskType.IMPLEMENT if i % 2 == 0 else TaskType.RESEARCH,
                title=f"Task {i}",
                session_id=session.session_id,
            )
            session.add_task(SessionTaskRecord(
                task_id=task.id, specialist="FORGE",
                task_type="implement",
            ))
            session.status = SessionStatus.ACTIVE
            orchestrator.on_task_failed(task.id, session.session_id, error=f"Error {i}")

        assert session.status == SessionStatus.BLOCKED

    def test_request_consensus_on_conflict(self, orchestrator):
        """request_consensus_on_conflict() creates consensus event."""
        session = orchestrator.create_session(goal_description="Test")

        # Add participants
        session.add_task(SessionTaskRecord(task_id="t1", specialist="FORGE", task_type="impl"))
        session.add_task(SessionTaskRecord(task_id="t2", specialist="SENTINEL", task_type="security"))
        session.status = SessionStatus.ACTIVE

        conflict = ConflictRecord(
            id="conflict_001",
            description="Disagreement on implementation approach",
            severity=ConflictSeverity.MEDIUM,
        )

        event = orchestrator.request_consensus_on_conflict(
            session.session_id, conflict,
        )
        assert event is not None
        assert "session_conflict" in event.topic

    def test_learn_from_session(self, orchestrator):
        """learn_from_session() extracts learning summary."""
        session = orchestrator.create_session(goal_description="Test")
        session.status = SessionStatus.COMPLETED
        session.add_task(SessionTaskRecord(
            task_id="t1", specialist="FORGE", task_type="implement",
            status="completed",
        ))
        session.add_task(SessionTaskRecord(
            task_id="t2", specialist="SENTINEL", task_type="security",
            status="completed",
        ))

        result = orchestrator.learn_from_session(session.session_id)
        assert result is not None
        assert result["session_id"] == session.session_id
        assert result["outcome"] == "success"
        assert result["tasks_total"] == 2
        assert result["tasks_completed"] == 2

    def test_learn_from_session_incomplete(self, orchestrator):
        """learn_from_session() returns None for active sessions."""
        session = orchestrator.create_session(goal_description="Test")
        session.status = SessionStatus.ACTIVE

        result = orchestrator.learn_from_session(session.session_id)
        assert result is None

    def test_learn_from_session_nonexistent(self, orchestrator):
        """learn_from_session() returns None for unknown session."""
        result = orchestrator.learn_from_session("nonexistent")
        assert result is None

    def test_learn_from_session_with_learning_pipeline(self, orchestrator):
        """learn_from_session() with learning pipeline integrates correctly."""
        session = orchestrator.create_session(goal_description="Test")
        session.status = SessionStatus.COMPLETED
        session.add_task(SessionTaskRecord(
            task_id="t1", specialist="FORGE", task_type="implement",
            status="completed",
        ))

        learning_mock = MagicMock()
        learning_mock.process_execution_outcome.return_value = {
            "stored": 1, "reinforced": 1,
        }

        result = orchestrator.learn_from_session(
            session.session_id,
            autonomous_learning_pipeline=learning_mock,
        )
        assert result is not None
        assert result["learning_result"] == {"stored": 1, "reinforced": 1}
        learning_mock.process_execution_outcome.assert_called_once()

    def test_snapshot(self, orchestrator):
        """snapshot() returns comprehensive orchestrator state."""
        session = orchestrator.create_session(goal_description="Test goal")
        orchestrator.create_routed_task(
            session.session_id, TaskType.RESEARCH, "Research task",
        )

        snap = orchestrator.snapshot()
        assert snap["total_sessions"] >= 1
        assert "router" in snap
        assert "sessions" in snap

    def test_to_terminal_display(self, orchestrator):
        """to_terminal_display() returns human-readable output."""
        session = orchestrator.create_session(goal_description="Display test")
        orchestrator.create_routed_task(
            session.session_id, TaskType.RESEARCH, "Research task",
        )

        display = orchestrator.to_terminal_display()
        assert "COLLABORATION ORCHESTRATOR" in display
        assert "Sessions:" in display
        assert "Router:" in display


# ============================================================================
# Integration Tests
# ============================================================================


class TestCollaborationIntegration:
    """End-to-end integration of all collaboration components."""

    def test_full_collaboration_flow(self, orchestrator):
        """Complete collaboration lifecycle: create session → route → complete → learn."""
        # 1. Create session
        session = orchestrator.create_session(
            goal_description="Build user authentication",
            initial_context={"framework": "FastAPI"},
        )
        assert session.status == SessionStatus.PENDING
        assert session.session_context.get("framework") == "FastAPI"

        # 2. Create and route research task
        research_task, research_decision, session = orchestrator.create_routed_task(
            session.session_id, TaskType.RESEARCH,
            title="Research auth libraries",
            description="Find best JWT libraries",
        )
        assert research_decision.selected_specialist == "ORACLE"
        assert session.status == SessionStatus.ACTIVE

        # 3. Create and route implementation task (depends on research)
        impl_task, impl_decision, session = orchestrator.create_routed_task(
            session.session_id, TaskType.IMPLEMENT,
            title="Implement JWT auth",
            description="Add JWT middleware",
            depends_on=[research_task.id],
            priority="high",
        )

        # 4. Complete research task
        orchestrator.on_task_completed(
            research_task.id, session.session_id,
            result={"libraries": ["python-jose", "passlib"]},
        )

        # 5. Start and complete implementation task
        orchestrator.on_task_started(impl_task.id, session.session_id)
        orchestrator.on_task_completed(
            impl_task.id, session.session_id,
            result={"files": ["auth.py"], "tests_passing": True},
        )

        # 6. Verify session tracked everything
        assert session.completed_count == 2
        assert session.total_count == 2
        assert session.progress_ratio == 1.0

        # 7. Learn from the session
        learning = orchestrator.learn_from_session(session.session_id)
        assert learning is not None
        assert learning["outcome"] == "success"
        assert learning["tasks_completed"] == 2
        assert "FORGE" in learning["specialists_involved"]
        assert "ORACLE" in learning["specialists_involved"]

        # 8. Verify blackboard has session artifacts
        entries = orchestrator.blackboard.read("session_events")
        assert len(entries) >= 1

        # 9. Router tracked the decisions
        router_snap = orchestrator.router.snapshot()
        assert router_snap["total_routes"] >= 2

    def test_collaboration_with_failure_and_consensus(self, orchestrator):
        """Collaboration handles failures and triggers consensus."""
        session = orchestrator.create_session(
            goal_description="Deploy production",
        )

        # Create execution task
        exec_task, decision, session = orchestrator.create_routed_task(
            session.session_id, TaskType.EXECUTE,
            title="Deploy to production",
        )

        # Task fails
        orchestrator.on_task_failed(
            exec_task.id, session.session_id,
            error="Deployment failed: port conflict",
            failure_reason="Port 8080 already in use",
        )

        # Request consensus on the failure
        conflict = ConflictRecord(
            id=f"conflict_{session.session_id[:8]}",
            description="Deployment failure: how to resolve port conflict?",
            severity=ConflictSeverity.HIGH,
            involved_specialists=["TERMINUS", "SENTINEL"],
        )

        # Verify failure was recorded
        record = session.get_task(exec_task.id)
        assert record is not None
        assert record.status == "failed"

        # Consensus can be requested even after failure
        event = orchestrator.request_consensus_on_conflict(
            session.session_id, conflict,
        )
        assert event is not None

        # Verify blackboard has failure record
        slot_entries = orchestrator.blackboard.read(session.blackboard_slot)
        failure_entries = [
            e for e in slot_entries
            if "failed" in e.content or "failure" in e.content
        ]
        assert len(failure_entries) >= 1

    def test_multiple_sessions_isolation(self, orchestrator):
        """Multiple sessions don't interfere with each other."""
        # Session 1: Research project
        s1 = orchestrator.create_session(goal_description="Research AI frameworks")
        t1, _, _ = orchestrator.create_routed_task(
            s1.session_id, TaskType.RESEARCH, "Research TensorFlow",
        )

        # Session 2: Security audit
        s2 = orchestrator.create_session(goal_description="Audit authentication")
        t2, _, _ = orchestrator.create_routed_task(
            s2.session_id, TaskType.SECURITY_REVIEW, "Audit login flow",
        )

        # Complete tasks independently
        orchestrator.on_task_completed(t1.id, s1.session_id)
        orchestrator.on_task_completed(t2.id, s2.session_id)

        # Each session should have correct counts
        assert s1.completed_count == 1
        assert s2.completed_count == 1
        assert s1.total_count == 1
        assert s2.total_count == 1

        # Non-overlapping specialists
        assert "ORACLE" in s1.specialist_participants
        assert "SENTINEL" in s2.specialist_participants

    def test_collaboration_with_routing_learning_loop(self, orchestrator, coordination):
        """Routing improves with accumulated routing history."""
        session = orchestrator.create_session(goal_description="Build features")

        # Route multiple tasks to build history
        for i in range(5):
            orchestrator.create_routed_task(
                session.session_id,
                TaskType.IMPLEMENT,
                title=f"Feature {i}",
            )

        # Verify routing history grew
        router_history = orchestrator.router.get_routing_history()
        assert len(router_history) >= 5

        # The router should prefer FORGE for implement tasks
        latest_decisions = router_history[-3:]
        forge_count = sum(
            1 for d in latest_decisions
            if d.selected_specialist == "FORGE"
        )
        assert forge_count >= 1
