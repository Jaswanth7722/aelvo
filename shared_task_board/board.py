"""
board.py — SharedTaskBoard

The central coordination point for Mode B (Collaborative) execution.
Manages task lifecycle: create, assign, transition, persist, and
emit events for every state change.

All task transitions are:
  1. Validated by TaskStateMachine
  2. Persisted to SQLite
  3. Published to EventBus
  4. Visible in the TUI
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


from shared_task_board.task import Task, TaskStatus, TaskType
from shared_task_board.state_machine import (
    TaskStateMachine,
)

log = logging.getLogger("aelvo.shared_task_board")


class TaskNotFoundError(Exception):
    """Raised when a task is not found in the board."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task not found: {task_id[:12]}")


@dataclass
class TaskBoardConfig:
    """Configuration for the SharedTaskBoard."""
    db_path: str = ""
    max_active_tasks: int = 50
    event_bus: Optional[Any] = None
    auto_persist: bool = True
    enable_events: bool = True


class SharedTaskBoard:
    """Central task board for Mode B collaborative execution.

    The Task Board organizes work by tracking each task through
    its lifecycle. Every state change is:
    1. Validated by TaskStateMachine
    2. Persisted to SQLite
    3. Published to EventBus (if configured)
    4. Logged for observability
    """

    def __init__(self, config: Optional[TaskBoardConfig] = None):
        self.config = config or TaskBoardConfig()
        self._tasks: Dict[str, Task] = {}
        self._event_bus = self.config.event_bus

        if self.config.db_path:
            self._init_db()

    # ── Task CRUD ─────────────────────────────────────────────────

    def create_task(
        self,
        task_type: TaskType = TaskType.GENERAL,
        specialist: str = "",
        title: str = "",
        description: str = "",
        priority: str = "medium",
        context: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
        assigned_by: str = "",
        session_id: str = "",
        tags: Optional[List[str]] = None,
    ) -> Task:
        """Create a new task on the board."""
        from shared_task_board.task import TaskPriority

        task = Task.create(
            task_type=task_type,
            specialist=specialist,
            title=title,
            description=description,
            priority=TaskPriority(priority) if isinstance(priority, str) else priority,
            context=context or {},
            depends_on=depends_on or [],
            assigned_by=assigned_by,
            session_id=session_id,
            tags=tags or [],
        )

        self._tasks[task.id] = task
        self._persist_task(task)
        self._emit_task_event("task_created", task)

        log.info(
            "Task %s created: %s [%s] -> %s",
            task.id[:12], task.type.value,
            task.specialist or "unassigned",
            task.title[:60],
        )
        return task

    def get_task(self, task_id: str) -> Task:
        """Get a task by ID. Raises TaskNotFoundError if missing."""
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        return task

    def get_tasks(
        self,
        status: Optional[TaskStatus] = None,
        task_type: Optional[TaskType] = None,
        specialist: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Task]:
        """Get tasks with optional filters."""
        tasks = list(self._tasks.values())

        if status:
            tasks = [t for t in tasks if t.status == status]
        if task_type:
            tasks = [t for t in tasks if t.type == task_type]
        if specialist:
            tasks = [t for t in tasks if t.specialist == specialist]
        if session_id:
            tasks = [t for t in tasks if t.session_id == session_id]

        # Sort by priority (critical first), then by creation time
        priority_order = {
            "critical": 0, "high": 1, "medium": 2, "low": 3, "lowest": 4,
        }
        tasks.sort(key=lambda t: (
            priority_order.get(t.priority.value, 5),
            t.created_at,
        ))

        return tasks[:limit]

    def get_active_tasks(self) -> List[Task]:
        """Get all tasks that are in progress or pending."""
        active_statuses = {
            TaskStatus.PENDING,
            TaskStatus.ASSIGNED,
            TaskStatus.IN_PROGRESS,
            TaskStatus.REVIEWING,
            TaskStatus.BLOCKED,
        }
        return [t for t in self._tasks.values() if t.status in active_statuses]

    def update_task(
        self,
        task_id: str,
        updates: Dict[str, Any],
    ) -> Task:
        """Update task metadata (non-status fields)."""
        task = self.get_task(task_id)

        allowed_fields = {
            "title", "description", "context", "priority",
            "tags", "metadata", "depends_on",
        }
        for key, value in updates.items():
            if key in allowed_fields:
                setattr(task, key, value)

        task.updated_at = time.time()
        self._persist_task(task)
        return task

    def delete_task(self, task_id: str) -> bool:
        """Remove a task from the board entirely."""
        task = self._tasks.pop(task_id, None)
        if task is None:
            return False
        self._delete_from_db(task_id)
        self._emit_task_event("task_deleted", task)
        return True

    # ── State Transitions ─────────────────────────────────────────

    def assign_task(
        self,
        task_id: str,
        specialist: str,
        assigned_by: str = "architect",
    ) -> Task:
        """Assign a PENDING task to a specialist.

        This is a convenience method that combines updating the
        specialist field with transitioning to ASSIGNED status.
        """
        task = self.get_task(task_id)

        # Set specialist even before transition
        task.specialist = specialist
        task.assigned_by = assigned_by

        # Transition to ASSIGNED (validates PENDING -> ASSIGNED)
        self._transition_and_persist(
            task, TaskStatus.ASSIGNED,
            reason=f"Assigned to {specialist} by {assigned_by}",
        )
        return task

    def start_task(self, task_id: str) -> Task:
        """Mark an ASSIGNED task as IN_PROGRESS."""
        task = self.get_task(task_id)
        self._transition_and_persist(
            task, TaskStatus.IN_PROGRESS,
            reason="Specialist started work",
        )
        return task

    def submit_for_review(self, task_id: str) -> Task:
        """Submit an IN_PROGRESS task for review."""
        task = self.get_task(task_id)
        self._transition_and_persist(
            task, TaskStatus.REVIEWING,
            reason="Submitted for review",
        )
        return task

    def complete_task(
        self,
        task_id: str,
        result: Optional[Dict[str, Any]] = None,
    ) -> Task:
        """Mark a REVIEWING task as COMPLETED with optional result."""
        task = self.get_task(task_id)
        if result:
            task.result = result
        self._transition_and_persist(
            task, TaskStatus.COMPLETED,
            reason="Task completed successfully",
        )
        return task

    def fail_task(
        self,
        task_id: str,
        error: str,
        failure_reason: str = "",
    ) -> Task:
        """Mark a task as FAILED with an error message."""
        task = self.get_task(task_id)
        task.error = error
        task.failure_reason = failure_reason or error
        self._transition_and_persist(
            task, TaskStatus.FAILED,
            reason=failure_reason or error,
        )
        return task

    def block_task(
        self,
        task_id: str,
        blocked_by: Optional[List[str]] = None,
        reason: str = "",
    ) -> Task:
        """Mark a task as BLOCKED by other tasks or external factors."""
        task = self.get_task(task_id)
        if blocked_by:
            task.blocked_by = list(set(task.blocked_by + blocked_by))
        self._transition_and_persist(
            task, TaskStatus.BLOCKED,
            reason=reason or "Blocked by dependencies",
        )
        return task

    def unblock_task(
        self,
        task_id: str,
        target_status: TaskStatus = TaskStatus.ASSIGNED,
    ) -> Task:
        """Unblock a task, moving it to ASSIGNED or IN_PROGRESS."""
        task = self.get_task(task_id)
        self._transition_and_persist(
            task, target_status,
            reason="Dependencies resolved — unblocked",
        )
        return task

    def cancel_task(self, task_id: str, reason: str = "") -> Task:
        """Cancel a task (terminal state)."""
        task = self.get_task(task_id)
        self._transition_and_persist(
            task, TaskStatus.CANCELLED,
            reason=reason or "Cancelled by architect",
        )
        return task

    def retry_task(
        self,
        task_id: str,
        target_status: TaskStatus = TaskStatus.ASSIGNED,
    ) -> Task:
        """Retry a FAILED task — moves to ASSIGNED or IN_PROGRESS."""
        task = self.get_task(task_id)
        # Clear error so retry is clean
        task.error = None
        task.failure_reason = None
        self._transition_and_persist(
            task, target_status,
            reason=f"Retry requested — moving to {target_status.value}",
        )
        return task

    # ── Internal Transition Logic ─────────────────────────────────

    def _transition_and_persist(
        self,
        task: Task,
        to_status: TaskStatus,
        reason: str = "",
    ) -> None:
        """Validate, apply, persist, and emit a state transition."""
        from_status = task.status

        # 1. Validate via state machine
        TaskStateMachine.apply_task(task, to_status, reason=reason)

        # 2. Persist to SQLite
        self._persist_task(task)
        self._persist_transition(task.id, from_status, to_status, reason)

        # 3. Emit EventBus event
        self._emit_transition_event(task, from_status, to_status, reason)

        log.info(
            "Task %s: %s -> %s (%s)",
            task.id[:12], from_status.value, to_status.value,
            reason[:80] if reason else "no reason",
        )

    # ── EventBus Publishing ───────────────────────────────────────

    def _publish_event(self, event) -> None:
        """Publish an event to the EventBus if it's running."""
        if not self._event_bus or not self.config.enable_events:
            return
        try:
            import asyncio
            loop = asyncio.get_running_loop()
            if loop.is_running():
                asyncio.ensure_future(self._event_bus.publish(event))
        except (RuntimeError, Exception) as e:
            # RuntimeError means no running loop — fine, skip event
            log.debug("Failed to publish event: %s", e)

    def _emit_task_event(self, event_type: str, task: Task) -> None:
        """Publish a task lifecycle event to the EventBus."""
        from runtime_next.models.events import BaseEvent, EventType as ET
        event = BaseEvent(
            id=f"task_{event_type}_{task.id}_{int(time.time())}",
            type=ET.LOG_MESSAGE,
            payload={
                "event": event_type,
                "task_id": task.id,
                "task_type": task.type.value,
                "status": task.status.value,
                "specialist": task.specialist,
                "title": task.title[:100],
            },
        )
        self._publish_event(event)

    def _emit_transition_event(
        self,
        task: Task,
        from_status: TaskStatus,
        to_status: TaskStatus,
        reason: str,
    ) -> None:
        """Publish a transition event to the EventBus."""
        from runtime_next.models.events import NodeTransitionEvent
        event = NodeTransitionEvent(
            id=f"task_trans_{task.id}_{int(time.time())}",
            node_id=task.id,
            node_type=task.type.value,
            from_state=from_status.value,
            to_state=to_status.value,
            reason=reason[:200] if reason else "",
        )
        self._publish_event(event)

    # ── SQLite Persistence ────────────────────────────────────────

    def _init_db(self) -> None:
        """Initialize SQLite tables."""
        try:
            os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        except (OSError, ValueError) as _ex:
            # db_path might be a filename without a directory
            log.warning("Silenced exception: %s", _ex)
        with sqlite3.connect(self.config.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_board_tasks (
                    task_id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    specialist TEXT DEFAULT '',
                    assigned_by TEXT DEFAULT '',
                    title TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    context TEXT DEFAULT '{}',
                    result TEXT,
                    depends_on TEXT DEFAULT '[]',
                    blocked_by TEXT DEFAULT '[]',
                    error TEXT,
                    failure_reason TEXT,
                    session_id TEXT DEFAULT '',
                    tags TEXT DEFAULT '[]',
                    metadata TEXT DEFAULT '{}',
                    history TEXT DEFAULT '[]',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    assigned_at REAL,
                    completed_at REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_board_transitions (
                    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    from_status TEXT NOT NULL,
                    to_status TEXT NOT NULL,
                    reason TEXT DEFAULT '',
                    timestamp REAL NOT NULL,
                    FOREIGN KEY (task_id) REFERENCES task_board_tasks(task_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_transitions_task_id
                ON task_board_transitions(task_id)
            """)
        self._load_from_db()

    @property
    def _db_path(self) -> str:
        return self.config.db_path

    def _persist_task(self, task: Task) -> None:
        """Upsert a task into SQLite."""
        if not self.config.db_path or not self.config.auto_persist:
            return
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO task_board_tasks
                        (task_id, type, status, priority, specialist,
                         assigned_by, title, description, context, result,
                         depends_on, blocked_by, error, failure_reason,
                         session_id, tags, metadata, history,
                         created_at, updated_at, assigned_at, completed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?)
                    """,
                    (
                        task.id,
                        task.type.value,
                        task.status.value,
                        task.priority.value,
                        task.specialist,
                        task.assigned_by,
                        task.title,
                        task.description,
                        json.dumps(task.context),
                        json.dumps(task.result) if task.result else None,
                        json.dumps(task.depends_on),
                        json.dumps(task.blocked_by),
                        task.error,
                        task.failure_reason,
                        task.session_id,
                        json.dumps(task.tags),
                        json.dumps(task.metadata),
                        json.dumps(task.history),
                        task.created_at,
                        task.updated_at,
                        task.assigned_at,
                        task.completed_at,
                    ),
                )
        except Exception as e:
            log.warning("Failed to persist task %s: %s", task.id[:12], e)

    def _persist_transition(
        self,
        task_id: str,
        from_status: TaskStatus,
        to_status: TaskStatus,
        reason: str,
    ) -> None:
        """Record a transition in the transitions table."""
        if not self.config.db_path or not self.config.auto_persist:
            return
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO task_board_transitions
                        (task_id, from_status, to_status, reason, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (task_id, from_status.value, to_status.value,
                     reason, time.time()),
                )
        except Exception as e:
            log.debug("Failed to persist transition: %s", e)

    def _delete_from_db(self, task_id: str) -> None:
        """Delete a task and its transitions from SQLite."""
        if not self.config.db_path:
            return
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                conn.execute(
                    "DELETE FROM task_board_transitions WHERE task_id = ?",
                    (task_id,),
                )
                conn.execute(
                    "DELETE FROM task_board_tasks WHERE task_id = ?",
                    (task_id,),
                )
        except Exception as e:
            log.warning("Failed to delete task %s: %s", task_id[:12], e)

    def _load_from_db(self) -> None:
        """Load all tasks from SQLite into memory."""
        if not self.config.db_path:
            return
        try:
            with sqlite3.connect(self.config.db_path) as conn:
                rows = conn.execute(
                    "SELECT * FROM task_board_tasks"
                ).fetchall()
                # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
                # Column name is at index 1
                columns = [d[1] for d in conn.execute(
                    "PRAGMA table_info(task_board_tasks)"
                ).fetchall()]

                for row in rows:
                    data = dict(zip(columns, row))
                    try:
                        task = Task(
                            id=data["task_id"],
                            type=TaskType(data["type"]),
                            status=TaskStatus(data["status"]),
                            priority=self._parse_priority(data["priority"]),
                            specialist=data.get("specialist", ""),
                            assigned_by=data.get("assigned_by", ""),
                            title=data.get("title", ""),
                            description=data.get("description", ""),
                            context=json.loads(data.get("context", "{}")),
                            result=(
                                json.loads(data["result"])
                                if data.get("result") else None
                            ),
                            depends_on=json.loads(
                                data.get("depends_on", "[]")
                            ),
                            blocked_by=json.loads(
                                data.get("blocked_by", "[]")
                            ),
                            error=data.get("error"),
                            failure_reason=data.get("failure_reason"),
                            session_id=data.get("session_id", ""),
                            tags=json.loads(data.get("tags", "[]")),
                            metadata=json.loads(data.get("metadata", "{}")),
                            history=json.loads(data.get("history", "[]")),
                            created_at=data["created_at"],
                            updated_at=data["updated_at"],
                            assigned_at=data.get("assigned_at"),
                            completed_at=data.get("completed_at"),
                        )
                        self._tasks[task.id] = task
                    except Exception as e:
                        log.warning(
                            "Failed to restore task %s: %s",
                            data.get("task_id", "?")[:12], e,
                        )
            if self._tasks:
                log.info(
                    "Restored %d tasks from SQLite", len(self._tasks),
                )
        except Exception as e:
            log.debug("No existing task data to restore: %s", e)

    @staticmethod
    def _parse_priority(value: str):
        """Parse priority string to TaskPriority enum."""
        from shared_task_board.task import TaskPriority
        try:
            return TaskPriority(value)
        except ValueError:
            return TaskPriority.MEDIUM

    # ── Reporting ─────────────────────────────────────────────────

    def count_by_status(self) -> Dict[str, int]:
        """Count tasks grouped by status."""
        counts: Dict[str, int] = {}
        for task in self._tasks.values():
            s = task.status.value
            counts[s] = counts.get(s, 0) + 1
        return counts

    def count_by_type(self) -> Dict[str, int]:
        """Count tasks grouped by type."""
        counts: Dict[str, int] = {}
        for task in self._tasks.values():
            t = task.type.value
            counts[t] = counts.get(t, 0) + 1
        return counts

    def snapshot(self) -> Dict[str, Any]:
        """Get a snapshot of the board state."""
        return {
            "total_tasks": len(self._tasks),
            "active_tasks": len(self.get_active_tasks()),
            "by_status": self.count_by_status(),
            "by_type": self.count_by_type(),
            "db_path": self.config.db_path or "(in-memory)",
            "event_bus_enabled": self.config.enable_events,
        }

    def to_terminal_display(self) -> str:
        """Human-readable board display."""
        snapshot = self.snapshot()
        statuses = snapshot["by_status"]
        types = snapshot["by_type"]

        lines = [
            "── TASK BOARD ──",
            f"  Total: {snapshot['total_tasks']}  "
            f"Active: {snapshot['active_tasks']}",
            "",
            "  By Status:",
        ]
        for status in TaskStatus:
            count = statuses.get(status.value, 0)
            if count > 0:
                icon = {
                    TaskStatus.PENDING: "○",
                    TaskStatus.ASSIGNED: "◎",
                    TaskStatus.IN_PROGRESS: "◉",
                    TaskStatus.REVIEWING: "◐",
                    TaskStatus.COMPLETED: "✓",
                    TaskStatus.FAILED: "✗",
                    TaskStatus.BLOCKED: "⊘",
                    TaskStatus.CANCELLED: "−",
                }.get(status, "?")
                lines.append(f"    {icon} {status.value}: {count}")

        lines.append("")
        lines.append("  By Type:")
        for ttype in TaskType:
            count = types.get(ttype.value, 0)
            if count > 0:
                lines.append(f"    {ttype.value}: {count}")

        active = self.get_active_tasks()[:5]
        if active:
            lines.append("")
            lines.append("  Active Tasks (top 5):")
            for t in active:
                lines.append(t.to_terminal_display())

        lines.append("── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)

    def clear(self) -> None:
        """Clear all tasks (for testing)."""
        self._tasks.clear()
        if self.config.db_path:
            try:
                with sqlite3.connect(self.config.db_path) as conn:
                    conn.execute("DELETE FROM task_board_transitions")
                    conn.execute("DELETE FROM task_board_tasks")
            except Exception as e:
                log.warning("Failed to clear SQLite tasks: %s", e)
