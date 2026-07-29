"""
tests/test_shared_task_board.py — Phase 3: SharedTaskBoard

Tests the SharedTaskBoard package including:
  - Task model creation and serialization
  - State machine valid/invalid transitions
  - SharedTaskBoard CRUD operations
  - SQLite persistence (load/save)
  - EventBus integration
"""

import json
import os
import tempfile
import time
import asyncio
import pytest

from shared_task_board.task import (
    Task, TaskStatus, TaskPriority, TaskType,
)
from shared_task_board.state_machine import (
    TaskStateMachine, InvalidTransitionError, TRANSITION_RULES,
)
from shared_task_board.board import (
    SharedTaskBoard, TaskNotFoundError, TaskBoardConfig,
)
from shared_task_board.context_schemas import (
    ResearchContext, ImplementContext, SecurityReviewContext,
    ExecuteContext, ConsensusContext, ReportContext,
)
from shared_task_board.result_schemas import (
    ResearchResult, ImplementResult, SecurityReviewResult,
    ExecuteResult, ConsensusResult, ReportResult,
)


# ==============================================================================
# Task Model Tests
# ==============================================================================


class TestTaskModel:
    """Task Pydantic model creation and serialization."""

    def test_create_with_minimal_args(self):
        """Task.create() works with minimal arguments."""
        task = Task.create(
            task_type=TaskType.RESEARCH,
            title="Test research task",
        )
        assert task.id and len(task.id) == 16
        assert task.type == TaskType.RESEARCH
        assert task.status == TaskStatus.PENDING
        assert task.title == "Test research task"
        assert task.created_at > 0
        assert len(task.history) == 1  # initial creation event
        assert task.history[0]["to"] == "pending"

    def test_create_with_all_fields(self):
        """Task.create() works with all fields specified."""
        task = Task.create(
            task_type=TaskType.IMPLEMENT,
            specialist="FORGE",
            title="Implement auth",
            description="Add JWT-based authentication",
            priority=TaskPriority.HIGH,
            context={"framework": "fastapi"},
            depends_on=["task_001"],
            assigned_by="ARCHITECT",
            session_id="session-1",
            tags=["auth", "security"],
        )
        assert task.type == TaskType.IMPLEMENT
        assert task.specialist == "FORGE"
        assert task.priority == TaskPriority.HIGH
        assert task.context["framework"] == "fastapi"
        assert task.depends_on == ["task_001"]
        assert task.assigned_by == "ARCHITECT"
        assert task.session_id == "session-1"
        assert "auth" in task.tags

    def test_summary(self):
        """Task.summary() returns compact representation."""
        task = Task.create(
            task_type=TaskType.RESEARCH,
            title="Research topic",
        )
        summary = task.summary()
        assert summary["type"] == "research"
        assert summary["status"] == "pending"
        assert "age_s" in summary

    def test_to_terminal_display(self):
        """Task.to_terminal_display() returns human-readable string."""
        task = Task.create(
            task_type=TaskType.IMPLEMENT,
            specialist="FORGE",
            title="Write tests",
        )
        display = task.to_terminal_display()
        assert "FORGE" in display
        assert "Write tests" in display
        assert "IMPLEMENT" in display or "implement" in display

    def test_model_dump(self):
        """Task can be serialized via model_dump()."""
        task = Task.create(
            task_type=TaskType.RESEARCH,
            title="Serialization test",
        )
        data = task.model_dump()
        assert data["type"] == "research"
        assert data["status"] == "pending"
        assert "history" in data

    def test_record_transition(self):
        """Task.record_transition() appends to history."""
        task = Task.create(task_type=TaskType.GENERAL, title="Test")
        task.record_transition(TaskStatus.PENDING, TaskStatus.ASSIGNED)
        assert len(task.history) == 2
        assert task.history[1]["from"] == "pending"
        assert task.history[1]["to"] == "assigned"

    def test_age_properties(self):
        """Task.age_seconds and duration_seconds work."""
        task = Task.create(task_type=TaskType.GENERAL, title="Test")
        assert task.age_seconds >= 0
        assert task.duration_seconds is None  # not completed yet


# ==============================================================================
# State Machine Tests
# ==============================================================================


class TestTaskStateMachine:
    """TaskStateMachine validates transitions correctly."""

    def test_valid_transitions(self):
        """Valid transitions are accepted by TaskStateMachine."""
        assert TaskStateMachine.can_transition(
            TaskStatus.PENDING, TaskStatus.ASSIGNED,
        )
        assert TaskStateMachine.can_transition(
            TaskStatus.ASSIGNED, TaskStatus.IN_PROGRESS,
        )
        assert TaskStateMachine.can_transition(
            TaskStatus.IN_PROGRESS, TaskStatus.REVIEWING,
        )
        assert TaskStateMachine.can_transition(
            TaskStatus.REVIEWING, TaskStatus.COMPLETED,
        )
        assert TaskStateMachine.can_transition(
            TaskStatus.REVIEWING, TaskStatus.IN_PROGRESS,
        )
        assert TaskStateMachine.can_transition(
            TaskStatus.FAILED, TaskStatus.ASSIGNED,
        )
        assert TaskStateMachine.can_transition(
            TaskStatus.BLOCKED, TaskStatus.IN_PROGRESS,
        )

    def test_invalid_transition_raises(self):
        """Invalid transitions raise InvalidTransitionError."""
        with pytest.raises(InvalidTransitionError):
            TaskStateMachine.transition(
                "test_id", TaskStatus.PENDING, TaskStatus.COMPLETED,
            )
        with pytest.raises(InvalidTransitionError):
            TaskStateMachine.transition(
                "test_id", TaskStatus.COMPLETED, TaskStatus.ASSIGNED,
            )
        with pytest.raises(InvalidTransitionError):
            TaskStateMachine.transition(
                "test_id", TaskStatus.CANCELLED, TaskStatus.PENDING,
            )

    def test_terminal_states(self):
        """COMPLETED and CANCELLED are terminal states."""
        assert TaskStateMachine.is_terminal(TaskStatus.COMPLETED)
        assert TaskStateMachine.is_terminal(TaskStatus.CANCELLED)
        assert not TaskStateMachine.is_terminal(TaskStatus.IN_PROGRESS)

    def test_allowed_transitions(self):
        """allowed_transitions() returns valid targets."""
        pending_targets = TaskStateMachine.allowed_transitions(
            TaskStatus.PENDING,
        )
        assert TaskStatus.ASSIGNED in pending_targets
        assert TaskStatus.CANCELLED in pending_targets
        assert TaskStatus.COMPLETED not in pending_targets

    def test_apply_task_invalid(self):
        """apply_task with invalid transition raises error."""
        task = Task.create(task_type=TaskType.GENERAL, title="Test")
        task.status = TaskStatus.COMPLETED
        with pytest.raises(InvalidTransitionError):
            TaskStateMachine.apply_task(task, TaskStatus.ASSIGNED)

    def test_apply_task_valid(self):
        """apply_task with valid transition updates task."""
        task = Task.create(task_type=TaskType.GENERAL, title="Test")
        # PENDING -> ASSIGNED
        TaskStateMachine.apply_task(task, TaskStatus.ASSIGNED)
        assert task.status == TaskStatus.ASSIGNED
        assert task.assigned_at is not None

        # ASSIGNED -> IN_PROGRESS
        TaskStateMachine.apply_task(task, TaskStatus.IN_PROGRESS)
        assert task.status == TaskStatus.IN_PROGRESS

        # IN_PROGRESS -> REVIEWING
        TaskStateMachine.apply_task(task, TaskStatus.REVIEWING)
        assert task.status == TaskStatus.REVIEWING

        # REVIEWING -> COMPLETED
        TaskStateMachine.apply_task(task, TaskStatus.COMPLETED)
        assert task.status == TaskStatus.COMPLETED
        assert task.completed_at is not None

    def test_transition_rules_completeness(self):
        """All TaskStatus values are represented in TRANSITION_RULES."""
        for status in TaskStatus:
            assert status in TRANSITION_RULES, (
                f"Missing transition rules for {status}"
            )


# ==============================================================================
# SharedTaskBoard CRUD Tests
# ==============================================================================


class TestSharedTaskBoard:
    """SharedTaskBoard CRUD operations."""

    @pytest.fixture
    def board(self):
        """Create a fresh in-memory board for each test."""
        config = TaskBoardConfig(
            db_path="",
            auto_persist=False,
            enable_events=False,
        )
        b = SharedTaskBoard(config)
        yield b
        b.clear()

    def test_create_and_get_task(self, board):
        """Create a task and retrieve it by ID."""
        task = board.create_task(
            task_type=TaskType.RESEARCH,
            specialist="ORACLE",
            title="Test research",
            description="Research async patterns",
        )
        assert task.id is not None

        retrieved = board.get_task(task.id)
        assert retrieved.id == task.id
        assert retrieved.title == "Test research"
        assert retrieved.status == TaskStatus.PENDING

    def test_get_task_not_found(self, board):
        """Getting a non-existent task raises TaskNotFoundError."""
        with pytest.raises(TaskNotFoundError):
            board.get_task("nonexistent_id")

    def test_get_tasks_with_filters(self, board):
        """get_tasks() supports filtering by status, type, specialist."""
        board.create_task(
            task_type=TaskType.RESEARCH, title="R1",
        )
        board.create_task(
            task_type=TaskType.IMPLEMENT, specialist="FORGE",
            title="I1",
        )
        board.create_task(
            task_type=TaskType.IMPLEMENT, specialist="FORGE",
            title="I2",
        )

        implement_tasks = board.get_tasks(task_type=TaskType.IMPLEMENT)
        assert len(implement_tasks) == 2

        forge_tasks = board.get_tasks(specialist="FORGE")
        assert len(forge_tasks) == 2

        research_tasks = board.get_tasks(task_type=TaskType.RESEARCH)
        assert len(research_tasks) == 1

    def test_get_active_tasks(self, board):
        """get_active_tasks() returns non-terminal tasks."""
        t1 = board.create_task(task_type=TaskType.RESEARCH, title="R1")
        t2 = board.create_task(task_type=TaskType.IMPLEMENT, title="I1")
        board.assign_task(t1.id, "ORACLE")
        board.start_task(t1.id)

        active = board.get_active_tasks()
        assert len(active) >= 1
        active_ids = [t.id for t in active]
        assert t1.id in active_ids

    def test_update_task(self, board):
        """update_task() modifies non-status fields."""
        task = board.create_task(
            task_type=TaskType.RESEARCH, title="Original",
        )
        board.update_task(task.id, {"title": "Updated", "priority": "high"})
        updated = board.get_task(task.id)
        assert updated.title == "Updated"
        assert updated.priority == TaskPriority.HIGH

    def test_delete_task(self, board):
        """delete_task() removes a task from the board."""
        task = board.create_task(
            task_type=TaskType.GENERAL, title="To delete",
        )
        assert board.delete_task(task.id) is True
        with pytest.raises(TaskNotFoundError):
            board.get_task(task.id)

    def test_assign_task(self, board):
        """assign_task() transitions PENDING -> ASSIGNED."""
        task = board.create_task(
            task_type=TaskType.RESEARCH,
            title="Assign test",
        )
        board.assign_task(task.id, "ORACLE", assigned_by="ARCHITECT")
        updated = board.get_task(task.id)
        assert updated.status == TaskStatus.ASSIGNED
        assert updated.specialist == "ORACLE"
        assert updated.assigned_by == "ARCHITECT"
        assert updated.assigned_at is not None

    def test_full_task_lifecycle(self, board):
        """A task can go through the full lifecycle."""
        task = board.create_task(
            task_type=TaskType.IMPLEMENT,
            title="Lifecycle test",
        )
        # PENDING -> ASSIGNED
        board.assign_task(task.id, "FORGE")
        assert board.get_task(task.id).status == TaskStatus.ASSIGNED

        # ASSIGNED -> IN_PROGRESS
        board.start_task(task.id)
        assert board.get_task(task.id).status == TaskStatus.IN_PROGRESS

        # IN_PROGRESS -> REVIEWING
        board.submit_for_review(task.id)
        assert board.get_task(task.id).status == TaskStatus.REVIEWING

        # REVIEWING -> COMPLETED
        board.complete_task(task.id, result={"message": "Done"})
        assert board.get_task(task.id).status == TaskStatus.COMPLETED
        assert board.get_task(task.id).result["message"] == "Done"
        assert board.get_task(task.id).completed_at is not None

    def test_fail_and_retry(self, board):
        """A task can fail and be retried."""
        task = board.create_task(
            task_type=TaskType.IMPLEMENT,
            title="Fail test",
        )
        board.assign_task(task.id, "FORGE")
        board.start_task(task.id)
        board.fail_task(task.id, error="Something broke",
                        failure_reason="Test failure")

        failed = board.get_task(task.id)
        assert failed.status == TaskStatus.FAILED
        assert "Something broke" in failed.error

        # Retry: FAILED -> ASSIGNED
        board.retry_task(task.id, TaskStatus.ASSIGNED)
        retried = board.get_task(task.id)
        assert retried.status == TaskStatus.ASSIGNED
        assert retried.error is None  # cleared on retry

    def test_block_and_unblock(self, board):
        """A task can be blocked and unblocked."""
        task = board.create_task(
            task_type=TaskType.IMPLEMENT,
            title="Block test",
        )
        board.assign_task(task.id, "FORGE")
        board.block_task(task.id, blocked_by=["task_001"],
                         reason="Waiting for research")

        blocked = board.get_task(task.id)
        assert blocked.status == TaskStatus.BLOCKED
        assert "task_001" in blocked.blocked_by

        # Unblock: BLOCKED -> ASSIGNED
        board.unblock_task(task.id, TaskStatus.ASSIGNED)
        unblocked = board.get_task(task.id)
        assert unblocked.status == TaskStatus.ASSIGNED

    def test_cancel_task(self, board):
        """A task can be cancelled (terminal)."""
        task = board.create_task(
            task_type=TaskType.GENERAL, title="Cancel me",
        )
        board.cancel_task(task.id, reason="No longer needed")
        cancelled = board.get_task(task.id)
        assert cancelled.status == TaskStatus.CANCELLED

    def test_snapshot(self, board):
        """snapshot() returns board statistics."""
        board.create_task(task_type=TaskType.RESEARCH, title="R1")
        board.create_task(task_type=TaskType.IMPLEMENT, title="I1")
        snap = board.snapshot()
        assert snap["total_tasks"] >= 2
        assert isinstance(snap["by_status"], dict)
        assert isinstance(snap["by_type"], dict)

    def test_to_terminal_display(self, board):
        """to_terminal_display() returns human-readable output."""
        board.create_task(
            task_type=TaskType.RESEARCH, specialist="ORACLE",
            title="Display test",
        )
        display = board.to_terminal_display()
        assert "TASK BOARD" in display
        assert "ORACLE" in display
        assert "research" in display


# ==============================================================================
# SQLite Persistence Tests
# ==============================================================================


class TestSharedTaskBoardPersistence:
    """SharedTaskBoard SQLite persistence."""

    @pytest.fixture
    def db_path(self):
        path = os.path.join(
            tempfile.gettempdir(),
            f"test_task_board_{int(time.time())}_{os.urandom(4).hex()}.db",
        )
        yield path
        # Retry deletion to handle Windows file locks
        for attempt in range(5):
            try:
                if os.path.exists(path):
                    os.unlink(path)
                break
            except PermissionError:
                import time as _t
                _t.sleep(0.2 * (attempt + 1))
            except FileNotFoundError:
                break

    def test_persistence_save_and_load(self, db_path):
        """Tasks are persisted to SQLite and can be reloaded."""
        config = TaskBoardConfig(
            db_path=db_path,
            auto_persist=True,
            enable_events=False,
        )
        board = SharedTaskBoard(config)

        board.create_task(
            task_type=TaskType.RESEARCH, title="Persist test",
        )
        board.create_task(
            task_type=TaskType.IMPLEMENT, specialist="FORGE",
            title="Implement feature",
        )

        # Verify tasks are in SQLite
        import sqlite3
        with sqlite3.connect(db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM task_board_tasks"
            ).fetchone()[0]
            assert count == 2

    def test_persistence_load_existing(self, db_path):
        """Existing SQLite data is loaded on board creation."""
        # Create board and add tasks
        config = TaskBoardConfig(
            db_path=db_path,
            auto_persist=True,
            enable_events=False,
        )
        board1 = SharedTaskBoard(config)
        t1 = board1.create_task(
            task_type=TaskType.RESEARCH, title="Load test",
        )
        board1.assign_task(t1.id, "ORACLE")

        # Create a new board instance pointing to the same DB
        board2 = SharedTaskBoard(config)
        assert len(board2._tasks) >= 1

        loaded = board2.get_task(t1.id)
        assert loaded.title == "Load test"
        assert loaded.status == TaskStatus.ASSIGNED
        assert loaded.specialist == "ORACLE"

    def test_persistence_transitions(self, db_path):
        """Task transitions are persisted."""
        config = TaskBoardConfig(
            db_path=db_path,
            auto_persist=True,
            enable_events=False,
        )
        board = SharedTaskBoard(config)

        task = board.create_task(
            task_type=TaskType.IMPLEMENT, title="Transition test",
        )
        board.assign_task(task.id, "FORGE")
        board.start_task(task.id)
        board.submit_for_review(task.id)
        board.complete_task(task.id, result={"ok": True})

        # Verify transitions in DB
        import sqlite3
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT from_status, to_status FROM task_board_transitions "
                "WHERE task_id = ? ORDER BY timestamp",
                (task.id,),
            ).fetchall()
            assert len(rows) >= 3  # at least 3 transitions

    def test_persistence_delete(self, db_path):
        """Deleted tasks are removed from SQLite."""
        config = TaskBoardConfig(
            db_path=db_path,
            auto_persist=True,
            enable_events=False,
        )
        board = SharedTaskBoard(config)

        task = board.create_task(
            task_type=TaskType.GENERAL, title="Delete me",
        )
        task_id = task.id
        board.delete_task(task_id)

        import sqlite3
        with sqlite3.connect(db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM task_board_tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()[0]
            assert count == 0


# ==============================================================================
# Context and Result Schemas Tests
# ==============================================================================


class TestTaskContextSchemas:
    """Typed context schemas work correctly."""

    def test_research_context(self):
        ctx = ResearchContext(
            task_id="test_001",
            query="How does async work in Python?",
            scope="web",
        )
        assert ctx.query == "How does async work in Python?"
        assert ctx.scope == "web"
        assert ctx.max_sources == 5

    def test_implement_context(self):
        ctx = ImplementContext(
            task_id="test_002",
            specification="Add JWT auth middleware",
            affected_files=["auth.py", "middleware.py"],
        )
        assert ctx.specification == "Add JWT auth middleware"
        assert len(ctx.affected_files) == 2
        assert ctx.test_required is True

    def test_security_review_context(self):
        ctx = SecurityReviewContext(
            task_id="test_003",
            files_to_review=["auth.py", "db.py"],
            changes_summary="Added user login flow",
        )
        assert "auth.py" in ctx.files_to_review
        assert ctx.risk_focus == "all"

    def test_execute_context(self):
        ctx = ExecuteContext(
            task_id="test_004",
            commands=["pytest tests/"],
        )
        assert len(ctx.commands) == 1
        assert ctx.timeout_seconds == 30

    def test_consensus_context(self):
        ctx = ConsensusContext(
            task_id="test_005",
            topic="Should we use JWT or sessions?",
            required_participants=["SENTINEL", "FORGE"],
        )
        assert len(ctx.required_participants) == 2
        assert ctx.resolution_strategy == "majority"


class TestTaskResultSchemas:
    """Typed result schemas work correctly."""

    def test_research_result(self):
        result = ResearchResult(
            task_id="test_001",
            findings=[{"topic": "async", "summary": "Use asyncio"}],
            sources=["docs.python.org"],
            confidence=0.85,
        )
        assert len(result.findings) == 1
        assert result.confidence == 0.85
        assert result.success is True

    def test_implement_result(self):
        result = ImplementResult(
            task_id="test_002",
            files_changed=["auth.py"],
            files_created=["jwt.py"],
            changes_summary="Added JWT authentication",
        )
        assert len(result.files_changed) == 1
        assert "jwt.py" in result.files_created

    def test_security_review_result(self):
        result = SecurityReviewResult(
            task_id="test_003",
            cleared=False,
            vulnerabilities=[{"type": "sql_injection", "severity": "high"}],
            risk_level="high",
        )
        assert result.cleared is False
        assert result.risk_level == "high"

    def test_execute_result(self):
        result = ExecuteResult(
            task_id="test_004",
            exit_code=0,
            stdout="All tests passed!",
        )
        assert result.exit_code == 0
        assert "All tests passed" in result.stdout

    def test_consensus_result(self):
        result = ConsensusResult(
            task_id="test_005",
            outcome="agreed",
            positions={"FORGE": "approve", "SENTINEL": "approve"},
            recommendation="Use JWT with refresh tokens",
        )
        assert result.outcome == "agreed"
        assert len(result.positions) == 2


# ==============================================================================
# EventBus Integration Test
# ==============================================================================


@pytest.mark.asyncio
class TestSharedTaskBoardEvents:
    """SharedTaskBoard emits events to the EventBus."""

    @pytest.fixture
    def event_bus(self):
        from runtime_next.events.bus import EventBus
        bus = EventBus()
        return bus

    @pytest.fixture
    def board_with_events(self, event_bus):
        config = TaskBoardConfig(
            db_path="",
            auto_persist=False,
            enable_events=True,
            event_bus=event_bus,
        )
        b = SharedTaskBoard(config)
        yield b
        b.clear()

    @pytest.mark.timeout(10)
    async def test_creates_publishes_event(self, board_with_events, event_bus):
        """Creating a task publishes an event."""
        collected = []

        async def collector(event):
            collected.append(event)

        event_bus.subscribe_all(collector)
        await event_bus.start()

        task = board_with_events.create_task(
            task_type=TaskType.RESEARCH,
            title="Event test",
        )

        # Give event bus time to process
        await asyncio.sleep(0.2)

        # Manually flush the queue if events weren't processed
        bus = event_bus
        while not bus._queue.empty():
            try:
                ev = bus._queue.get_nowait()
                if ev is not None:
                    for cb in bus._global_subscribers:
                        await cb(ev)
                bus._queue.task_done()
            except asyncio.QueueEmpty:
                break

        assert len(collected) >= 1
        # One of the events should relate to our task
        task_events = [
            e for e in collected
            if hasattr(e, 'payload') and isinstance(e.payload, dict)
            and e.payload.get("task_id") == task.id
        ]
        assert len(task_events) >= 1

        await event_bus.stop()

    @pytest.mark.timeout(10)
    async def test_transition_publishes_event(self, board_with_events, event_bus):
        """Task state transitions publish events."""
        collected = []

        async def collector(event):
            collected.append(event)

        event_bus.subscribe_all(collector)
        await event_bus.start()

        task = board_with_events.create_task(
            task_type=TaskType.IMPLEMENT,
            title="Transition event test",
        )
        board_with_events.assign_task(task.id, "FORGE")

        await asyncio.sleep(0.2)

        # Manually flush the queue
        bus = event_bus
        while not bus._queue.empty():
            try:
                ev = bus._queue.get_nowait()
                if ev is not None:
                    for cb in bus._global_subscribers:
                        await cb(ev)
                bus._queue.task_done()
            except asyncio.QueueEmpty:
                break

        # Should have at least the transition event (ASSIGNED)
        transition_events = [
            e for e in collected
            if hasattr(e, 'to_state') and e.to_state == "assigned"
        ]
        assert len(transition_events) >= 1

        await event_bus.stop()
