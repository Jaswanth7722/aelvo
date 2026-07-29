"""shared_task_board/collaboration_orchestrator.py — Multi-Agent Collaboration & Task Board Routing

Phase 12: Provides intelligent task routing, collaboration session management,
workload-aware scheduling, and integration with the cognitive layer's blackboard,
consensus, and learning systems.

Key components:
  - CollaborationSession: Tracks multi-specialist work toward a shared goal
  - RoutingDecision: The result of a routing decision with rationale
  - IntelligentRouter: Routes tasks based on capability, workload, performance
  - CollaborationOrchestrator: Orchestrates full multi-agent collaboration
"""

from __future__ import annotations

import hashlib
import logging
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from shared_task_board.board import SharedTaskBoard
from shared_task_board.task import Task, TaskStatus, TaskType

from cognition.blackboard import CognitiveBlackboard
from cognition.consensus import MultiAgentConsensusSystem
from cognition.coordination import SpecialistCoordinationRuntime
from cognition.types import (
    EntryType, Provenance, ProvenanceType,
    ConflictRecord, MemoryType,
)

log = logging.getLogger("aelvo.shared_task_board.collaboration")


# ============================================================================
# Enums
# ============================================================================


class SessionStatus(str, Enum):
    """Lifecycle states for a collaboration session."""
    PENDING = "pending"
    ACTIVE = "active"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"


class RoutingStrategy(str, Enum):
    """Strategy used to route a task to a specialist."""
    CAPABILITY_MATCH = "capability_match"
    PERFORMANCE_BASED = "performance_based"
    WORKLOAD_BALANCE = "workload_balance"
    EXPLICIT_ASSIGNMENT = "explicit_assignment"
    CONSENSUS_REQUIRED = "consensus_required"
    FALLBACK = "fallback"


class CollaborationPhase(str, Enum):
    """Phases of a collaboration session's execution."""
    PLANNING = "planning"
    RESEARCH = "research"
    IMPLEMENTATION = "implementation"
    SECURITY_REVIEW = "security_review"
    EXECUTION = "execution"
    SYNTHESIS = "synthesis"
    COMPLETED = "completed"


# ============================================================================
# Pydantic Models
# ============================================================================


class RoutingDecision(BaseModel):
    """The result of a routing decision with full rationale."""

    task_id: str
    task_title: str
    selected_specialist: str
    strategy: RoutingStrategy = RoutingStrategy.CAPABILITY_MATCH
    capability_score: float = 0.0
    performance_score: float = 0.0
    workload_score: float = 0.0
    complexity_estimate: int = 1
    alternatives: List[str] = Field(default_factory=list)
    rationale: str = ""
    timestamp: float = Field(default_factory=time.time)


class SessionTaskRecord(BaseModel):
    """A record of a task within a collaboration session."""

    task_id: str
    specialist: str
    task_type: str
    status: str = "pending"
    routing: Optional[RoutingDecision] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None


class CollaborationSession(BaseModel):
    """Tracks a group of tasks worked on by multiple specialists toward a shared goal.

    Attributes:
        session_id: Unique session identifier.
        goal_description: The high-level goal being collaborated on.
        status: Current session status.
        phase: Current collaboration phase.
        tasks: Records of all tasks created within this session.
        specialist_participants: Specialists that have been assigned tasks.
        session_context: Shared context passed between specialists.
        blackboard_slot: The blackboard slot used for shared findings.
        created_by: The entity that initiated the session.
        created_at: Session creation timestamp.
        completed_at: Session completion timestamp.
        metadata: Additional session metadata.
    """

    session_id: str
    goal_description: str
    status: SessionStatus = SessionStatus.PENDING
    phase: CollaborationPhase = CollaborationPhase.PLANNING
    tasks: List[SessionTaskRecord] = Field(default_factory=list)
    specialist_participants: Set[str] = Field(default_factory=set)
    session_context: Dict[str, Any] = Field(default_factory=dict)
    blackboard_slot: str = ""
    created_by: str = "architect"
    created_at: float = Field(default_factory=time.time)
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def add_task(self, record: SessionTaskRecord) -> None:
        """Add a task record to the session."""
        self.tasks.append(record)
        self.specialist_participants.add(record.specialist)

    def get_task(self, task_id: str) -> Optional[SessionTaskRecord]:
        """Get a task record by task ID."""
        for t in self.tasks:
            if t.task_id == task_id:
                return t
        return None

    def get_tasks_by_specialist(self, specialist: str) -> List[SessionTaskRecord]:
        """Get all tasks assigned to a specialist in this session."""
        return [t for t in self.tasks if t.specialist == specialist]

    def get_tasks_by_status(self, status: str) -> List[SessionTaskRecord]:
        """Get all tasks with a given status."""
        return [t for t in self.tasks if t.status == status]

    @property
    def completed_count(self) -> int:
        """Number of completed tasks in the session."""
        return len(self.get_tasks_by_status("completed"))

    @property
    def total_count(self) -> int:
        """Total number of tasks in the session."""
        return len(self.tasks)

    @property
    def progress_ratio(self) -> float:
        """Ratio of completed tasks to total tasks."""
        if not self.tasks:
            return 0.0
        return self.completed_count / len(self.tasks)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Duration of the session if completed, else None."""
        if self.completed_at and self.created_at:
            return self.completed_at - self.created_at
        return None

    def summary(self) -> Dict[str, Any]:
        """Compact summary for logging / UI."""
        return {
            "session_id": self.session_id[:12],
            "goal": self.goal_description[:60],
            "status": self.status.value,
            "phase": self.phase.value,
            "tasks": self.total_count,
            "completed": self.completed_count,
            "progress": round(self.progress_ratio, 2),
            "specialists": sorted(self.specialist_participants),
            "duration_s": round(self.duration_seconds or 0, 1),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        lines = [
            f"  ── Collaboration Session [{self.session_id[:12]}] ──",
            f"  Goal: {self.goal_description[:70]}",
            f"  Status: {self.status.value}  |  Phase: {self.phase.value}",
            f"  Progress: {self.completed_count}/{self.total_count} tasks ({round(self.progress_ratio * 100)}%)",
            f"  Specialists: {', '.join(sorted(self.specialist_participants)) or 'none'}",
        ]
        if self.duration_seconds:
            lines.append(f"  Duration: {round(self.duration_seconds, 1)}s")
        lines.append("")
        # Show pending/active tasks
        active = self.get_tasks_by_status("in_progress") + self.get_tasks_by_status("assigned")
        if active:
            lines.append("  Active Tasks:")
            for t in active[:5]:
                lines.append(f"    ◉ [{t.task_id[:10]}] {t.specialist}: {t.task_type}")
        pending = self.get_tasks_by_status("pending")
        if pending:
            lines.append(f"  Pending Tasks: {len(pending)}")
        lines.append("  ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)


# ============================================================================
# Intelligent Router
# ============================================================================


class IntelligentRouter:
    """Routes tasks to specialists using capability, workload, and performance data.

    The router considers multiple signals:
    1. Capability matching — uses specialist.compute_activation_score()
    2. Workload awareness — checks active task counts per specialist
    3. Performance history — integrates with SpecialistCoordinationRuntime
    4. Task complexity — adjusts routing based on estimated complexity
    """

    def __init__(
        self,
        specialist_registry: Optional[Dict[str, Any]] = None,
        coordination: Optional[SpecialistCoordinationRuntime] = None,
    ):
        self._registry = specialist_registry or {}
        self._coordination = coordination
        self._routing_history: List[RoutingDecision] = []
        self._complexity_map: Dict[str, int] = {
            "research": 2,
            "implement": 4,
            "security_review": 3,
            "execute": 2,
            "report": 1,
            "consensus": 1,
            "general": 1,
        }

    # ── Specialist Registry ─────────────────────────────────────────

    def register_specialist(self, name: str, specialist: Any) -> None:
        """Register a specialist for routing."""
        self._registry[name.upper()] = specialist

    def get_available_specialists(self) -> List[str]:
        """Get all registered specialist names."""
        return list(self._registry.keys())

    # ── Routing ──────────────────────────────────────────────────────

    def route(
        self,
        task: Task,
        blackboard_context: Optional[Dict[str, Any]] = None,
        preferred_specialist: Optional[str] = None,
        complexity_override: Optional[int] = None,
    ) -> RoutingDecision:
        """Route a task to the best-fit specialist.

        Uses capability matching, workload awareness, and performance
        history to select the optimal specialist. Falls back gracefully
        when signals are unavailable.

        Args:
            task: The task to route.
            blackboard_context: Optional context from the blackboard.
            preferred_specialist: Explicit specialist preference.
            complexity_override: Override the default complexity estimate.

        Returns:
            A RoutingDecision with the selected specialist and rationale.
        """
        context = blackboard_context or {}

        # Explicit assignment
        if preferred_specialist and preferred_specialist.upper() in self._registry:
            decision = RoutingDecision(
                task_id=task.id,
                task_title=task.title,
                selected_specialist=preferred_specialist.upper(),
                strategy=RoutingStrategy.EXPLICIT_ASSIGNMENT,
                rationale=f"Explicitly assigned to {preferred_specialist.upper()}",
            )
            self._routing_history.append(decision)
            return decision

        # Score all available specialists
        task_type = task.type.value if hasattr(task.type, 'value') else str(task.type)
        complexity = complexity_override or self._complexity_map.get(task_type, 1)

        candidates: List[Tuple[str, float, float, float, float]] = []  # (name, total, capability, perf, workload)

        for name, specialist in self._registry.items():
            # 1. Capability score
            cap_score = self._score_capability(specialist, task, context)

            # 2. Performance score
            perf_score = self._score_performance(name)

            # 3. Workload score (lower active tasks = higher score)
            workload_score = self._score_workload(name)

            # Weighted composite
            total = (
                cap_score * 0.50 +
                perf_score * 0.25 +
                workload_score * 0.25
            )

            if cap_score > 0.0:  # Only consider specialists with some capability match
                candidates.append((name, total, cap_score, perf_score, workload_score))

        if not candidates:
            # Fallback: assign to the first available specialist
            fallback_name = list(self._registry.keys())[0] if self._registry else "HERMES"
            decision = RoutingDecision(
                task_id=task.id,
                task_title=task.title,
                selected_specialist=fallback_name,
                strategy=RoutingStrategy.FALLBACK,
                complexity_estimate=complexity,
                rationale=f"No capability-match found; fallback to {fallback_name}",
            )
            self._routing_history.append(decision)
            return decision

        # Sort by composite score descending
        candidates.sort(key=lambda c: c[1], reverse=True)
        best = candidates[0]

        alternatives = [c[0] for c in candidates[1:4]]

        strategy = RoutingStrategy.CAPABILITY_MATCH
        if best[3] > best[2]:  # performance score > capability score
            strategy = RoutingStrategy.PERFORMANCE_BASED
        elif best[4] > best[2]:  # workload score > capability score
            strategy = RoutingStrategy.WORKLOAD_BALANCE

        decision = RoutingDecision(
            task_id=task.id,
            task_title=task.title,
            selected_specialist=best[0],
            strategy=strategy,
            capability_score=round(best[2], 4),
            performance_score=round(best[3], 4),
            workload_score=round(best[4], 4),
            complexity_estimate=complexity,
            alternatives=alternatives,
            rationale=(
                f"Selected {best[0]} (composite={best[1]:.2f}, "
                f"cap={best[2]:.2f}, perf={best[3]:.2f}, workload={best[4]:.2f})"
            ),
        )
        self._routing_history.append(decision)
        return decision

    def route_batch(
        self,
        tasks: List[Task],
        blackboard_context: Optional[Dict[str, Any]] = None,
    ) -> List[RoutingDecision]:
        """Route multiple tasks, considering cumulative workload."""
        context = blackboard_context or {}
        decisions: List[RoutingDecision] = []
        workload_snapshot: Dict[str, int] = {}

        for task in tasks:
            decision = self.route(
                task,
                blackboard_context=context,
            )
            decisions.append(decision)

            # Track cumulative workload for this batch
            specialist = decision.selected_specialist
            workload_snapshot[specialist] = workload_snapshot.get(specialist, 0) + 1

        return decisions

    # ── Scoring Helpers ──────────────────────────────────────────────

    def _score_capability(self, specialist: Any, task: Task, context: Dict[str, Any]) -> float:
        """Score how capable a specialist is for a given task.

        Priority:
        1. TaskType mapping (strong, deterministic baseline)
        2. compute_activation_score (if available and above threshold, can boost)
        3. trigger_patterns (weak keyword signal)
        4. Neutral baseline (0.1)
        """
        task_type = task.type.value if hasattr(task.type, 'value') else str(task.type)

        # Phase 1: TaskType mapping — gives a strong baseline score
        type_map = {
            "research": ["ORACLE"],
            "implement": ["FORGE"],
            "security_review": ["SENTINEL"],
            "execute": ["TERMINUS"],
            "report": ["HERALD"],
            "consensus": ["ARCHITECT"],
        }
        specialist_name = getattr(specialist, "name", "").upper()
        matched_types = type_map.get(task_type, [])
        type_score = 0.6 if specialist_name in matched_types else 0.1

        # Phase 2: compute_activation_score — can boost above baseline
        if hasattr(specialist, "compute_activation_score"):
            try:
                act_score = specialist.compute_activation_score(
                    f"{task.title} {task.description}",
                    context,
                )
                if act_score > type_score:
                    return max(0.0, min(1.0, act_score))
            except Exception:
                pass

        # Phase 3: trigger_patterns — can boost above baseline but below TaskType
        if hasattr(specialist, "trigger_patterns"):
            task_text = f"{task.title} {task.description}".lower()
            matches = sum(
                1 for p in specialist.trigger_patterns
                if p.lower() in task_text
            )
            if matches > 0:
                pattern_score = min(1.0, matches * 0.25)
                if pattern_score > type_score:
                    return pattern_score

        return type_score

    def _score_performance(self, specialist_name: str) -> float:
        """Score based on historical performance."""
        if self._coordination is None:
            return 0.5  # neutral when no data

        perf = self._coordination.get_specialist_performance(specialist_name)
        if perf is None:
            return 0.5  # neutral for new specialists

        return max(0.1, min(1.0, perf.get("success_rate", 0.5)))

    def _score_workload(self, specialist_name: str) -> float:
        """Score based on current workload (lower active = higher score)."""
        if self._coordination is None:
            return 0.7  # slightly favor when no data

        # Count recent delegations as a proxy for workload
        history = self._coordination.get_delegation_history(specialist_name)
        recent = [d for d in history[-20:] if d.error is None]

        # Higher active count = lower score
        active_count = len(recent)
        if active_count == 0:
            return 1.0
        return max(0.2, 1.0 - (active_count * 0.08))

    # ── Reporting ────────────────────────────────────────────────────

    def get_routing_history(self, limit: int = 50) -> List[RoutingDecision]:
        """Get recent routing decisions."""
        return self._routing_history[-limit:]

    def get_specialist_route_counts(self) -> Dict[str, int]:
        """Get count of routes per specialist."""
        counts: Dict[str, int] = {}
        for d in self._routing_history:
            counts[d.selected_specialist] = counts.get(d.selected_specialist, 0) + 1
        return counts

    def snapshot(self) -> Dict[str, Any]:
        """Get a snapshot of the router state."""
        return {
            "total_routes": len(self._routing_history),
            "specialists_registered": len(self._registry),
            "routes_by_specialist": self.get_specialist_route_counts(),
            "strategy_breakdown": {
                s.value: sum(1 for d in self._routing_history if d.strategy == s)
                for s in RoutingStrategy
            },
        }


# ============================================================================
# Collaboration Orchestrator
# ============================================================================


class CollaborationOrchestrator:
    """Orchestrates multi-agent collaboration sessions with intelligent task routing.

    Integrates with:
    - SharedTaskBoard: task lifecycle management
    - CognitiveBlackboard: shared context across specialists
    - MultiAgentConsensusSystem: conflict resolution
    - SpecialistCoordinationRuntime: delegation tracking
    - IntelligentRouter: capability-aware routing
    - StrategicMemory: learning from collaboration outcomes
    """

    def __init__(
        self,
        task_board: SharedTaskBoard,
        blackboard: CognitiveBlackboard,
        consensus: MultiAgentConsensusSystem,
        coordination: SpecialistCoordinationRuntime,
        router: Optional[IntelligentRouter] = None,
        strategic_memory: Optional[Any] = None,
    ):
        self.task_board = task_board
        self.blackboard = blackboard
        self.consensus = consensus
        self.coordination = coordination
        self.router = router or IntelligentRouter(
            coordination=coordination,
        )
        self._strategic_memory = strategic_memory

        self._sessions: Dict[str, CollaborationSession] = {}
        self._session_counter: int = 0

    # ── Session Management ───────────────────────────────────────────

    def create_session(
        self,
        goal_description: str,
        created_by: str = "architect",
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> CollaborationSession:
        """Create a new collaboration session for a shared goal.

        Creates a dedicated blackboard slot for the session's shared
        findings and initializes the session tracking.
        """
        self._session_counter += 1
        raw = f"collab_{goal_description}_{time.time()}_{self._session_counter}"
        session_id = hashlib.sha256(raw.encode()).hexdigest()[:16]

        slot_name = f"session_{session_id[:8]}"
        self.blackboard.create_slot(slot_name, max_entries=200)

        session = CollaborationSession(
            session_id=session_id,
            goal_description=goal_description,
            created_by=created_by,
            blackboard_slot=slot_name,
            session_context=initial_context or {},
        )
        self._sessions[session_id] = session

        # Announce session creation on blackboard
        self.blackboard.publish(
            slot_name="session_events",
            content=f"Session {session_id[:12]} created: {goal_description[:60]}",
            entry_type=EntryType.COMMAND,
            provenance=Provenance(
                source_type=ProvenanceType.SYSTEM,
                source_id="collaboration_orchestrator",
            ),
            tags=["session", "created"],
        )

        log.info(
            "Collaboration session %s created: %s",
            session_id[:12], goal_description[:60],
        )
        return session

    def get_session(self, session_id: str) -> Optional[CollaborationSession]:
        """Get a session by ID."""
        return self._sessions.get(session_id)

    def get_active_sessions(self) -> List[CollaborationSession]:
        """Get all currently active sessions."""
        return [
            s for s in self._sessions.values()
            if s.status == SessionStatus.ACTIVE
        ]

    def close_session(
        self,
        session_id: str,
        status: SessionStatus = SessionStatus.COMPLETED,
    ) -> Optional[CollaborationSession]:
        """Close a collaboration session and publish final summary."""
        session = self._sessions.get(session_id)
        if session is None:
            return None

        session.status = status
        session.completed_at = time.time()

        # Publish session completion to blackboard
        summary = session.summary()
        self.blackboard.publish(
            slot_name="session_events",
            content=(
                f"Session {session_id[:12]} completed: "
                f"{session.goal_description[:40]} | "
                f"{summary['completed']}/{summary['tasks']} tasks | "
                f"{summary['specialists']}"
            ),
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SYSTEM,
                source_id="collaboration_orchestrator",
            ),
            tags=["session", "completed"],
        )

        log.info(
            "Session %s closed: %s — %d/%d tasks, %d specialists",
            session_id[:12], status.value,
            session.completed_count, session.total_count,
            len(session.specialist_participants),
        )
        return session

    # ── Task Creation & Routing ──────────────────────────────────────

    def create_routed_task(
        self,
        session_id: str,
        task_type: TaskType,
        title: str,
        description: str = "",
        priority: str = "medium",
        context: Optional[Dict[str, Any]] = None,
        preferred_specialist: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
    ) -> Tuple[Task, RoutingDecision, CollaborationSession]:
        """Create a task and route it to the best-fit specialist.

        Args:
            session_id: The session this task belongs to.
            task_type: Type of task to create.
            title: Task title.
            description: Task description.
            priority: Task priority string.
            context: Optional task context.
            preferred_specialist: Explicit specialist preference.
            depends_on: Task dependency IDs.

        Returns:
            Tuple of (Task, RoutingDecision, Session).
        """
        session = self._sessions.get(session_id)
        if session is None:
            raise ValueError(f"Session {session_id} not found")

        # Create the task on the board
        task = self.task_board.create_task(
            task_type=task_type,
            title=title,
            description=description,
            priority=priority,
            context=context or {},
            depends_on=depends_on or [],
            session_id=session_id,
            tags=["collaboration", session_id[:12]],
        )

        # Route to best-fit specialist
        blackboard_context = self._build_blackboard_context(session)
        decision = self.router.route(
            task,
            blackboard_context=blackboard_context,
            preferred_specialist=preferred_specialist,
        )

        # Assign via task board
        self.task_board.assign_task(
            task.id,
            decision.selected_specialist,
            assigned_by=session.created_by,
        )

        # Track in session
        session.add_task(SessionTaskRecord(
            task_id=task.id,
            specialist=decision.selected_specialist,
            task_type=task_type.value if hasattr(task_type, 'value') else str(task_type),
            routing=decision,
        ))

        # Set session active
        if session.status == SessionStatus.PENDING:
            session.status = SessionStatus.ACTIVE
            session.phase = self._infer_phase(task_type)

        # Publish routing decision to blackboard
        self.blackboard.publish(
            slot_name=session.blackboard_slot,
            content=(
                f"Routed task {task.id[:10]}: {title[:40]} → "
                f"{decision.selected_specialist} "
                f"({decision.strategy.value}, score={decision.capability_score:.2f})"
            ),
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SYSTEM,
                source_id="intelligent_router",
            ),
            tags=["routing", decision.selected_specialist.lower()],
        )

        log.info(
            "Task %s routed to %s (strategy=%s, capability=%.2f): %s",
            task.id[:10], decision.selected_specialist,
            decision.strategy.value, decision.capability_score,
            title[:50],
        )
        return task, decision, session

    # ── Task Lifecycle Tracking ──────────────────────────────────────

    def on_task_completed(
        self,
        task_id: str,
        session_id: str,
        result: Optional[Dict[str, Any]] = None,
    ) -> Optional[CollaborationSession]:
        """Notify the orchestrator that a task was completed.

        Updates the session record, publishes findings to the blackboard,
        and checks for session completion.
        """
        session = self._sessions.get(session_id)
        if session is None:
            return None

        # Update session task record
        record = session.get_task(task_id)
        if record:
            record.status = "completed"
            record.completed_at = time.time()

        # Complete the task on the board
        # Chain through intermediate states (ASSIGNED → IN_PROGRESS → REVIEWING → COMPLETED)
        try:
            bt = self.task_board.get_task(task_id)
            if bt.status == TaskStatus.ASSIGNED:
                self.task_board.start_task(task_id)
            if bt.status in (TaskStatus.ASSIGNED, TaskStatus.IN_PROGRESS):
                bt = self.task_board.get_task(task_id)
                self.task_board.submit_for_review(task_id)
            if bt.status in (TaskStatus.ASSIGNED, TaskStatus.IN_PROGRESS, TaskStatus.REVIEWING):
                bt = self.task_board.get_task(task_id)
                self.task_board.complete_task(task_id, result=result)
        except Exception as e:
            log.warning("Failed to update board for task %s: %s", task_id[:10], e)

        # Check if session is complete
        self._check_session_completion(session)

        return session

    def on_task_failed(
        self,
        task_id: str,
        session_id: str,
        error: str,
        failure_reason: str = "",
    ) -> Optional[CollaborationSession]:
        """Notify the orchestrator that a task failed.

        Updates the session record, logs the failure, and potentially
        triggers consensus for resolution.
        """
        session = self._sessions.get(session_id)
        if session is None:
            return None

        # Update session task record
        record = session.get_task(task_id)
        if record:
            record.status = "failed"
            record.error = error

        # Fail the task on the board
        # Chain through intermediate states (ASSIGNED → IN_PROGRESS → FAILED)
        try:
            bt = self.task_board.get_task(task_id)
            if bt.status == TaskStatus.ASSIGNED:
                self.task_board.start_task(task_id)
            if bt.status in (TaskStatus.ASSIGNED, TaskStatus.IN_PROGRESS, TaskStatus.REVIEWING):
                self.task_board.fail_task(task_id, error=error, failure_reason=failure_reason)
        except Exception as e:
            log.warning("Failed to mark task %s failed on board: %s", task_id[:10], e)

        # Record the failure on the blackboard
        self.blackboard.publish(
            slot_name=session.blackboard_slot,
            content=f"Task {task_id[:10]} failed: {error[:80]}",
            entry_type=EntryType.OBSERVATION,
            provenance=Provenance(
                source_type=ProvenanceType.SYSTEM,
                source_id="collaboration_orchestrator",
            ),
            tags=["failure", session_id[:12]],
        )

        # If session has too many failures, mark as blocked
        failed_count = len(session.get_tasks_by_status("failed"))
        if failed_count >= 3 and session.status == SessionStatus.ACTIVE:
            session.status = SessionStatus.BLOCKED
            log.warning(
                "Session %s blocked: %d failed tasks",
                session_id[:12], failed_count,
            )

        return session

    def on_task_started(self, task_id: str, session_id: str) -> Optional[CollaborationSession]:
        """Notify the orchestrator that a task has started execution."""
        session = self._sessions.get(session_id)
        if session is None:
            return None

        record = session.get_task(task_id)
        if record:
            record.status = "in_progress"
            record.started_at = time.time()

        try:
            self.task_board.start_task(task_id)
        except Exception as e:
            log.warning("Failed to start task %s on board: %s", task_id[:10], e)

        return session

    # ── Consensus Integration ────────────────────────────────────────

    def request_consensus_on_conflict(
        self,
        session_id: str,
        conflict: ConflictRecord,
    ) -> Optional[Any]:
        """Request consensus resolution for a conflict in a session.

        Creates a consensus event involving all specialists in the
        session and publishes the outcome to the blackboard.
        """
        session = self._sessions.get(session_id)
        if session is None:
            return None

        participants = list(session.specialist_participants) or ["SENTINEL", "FORGE", "HERMES"]

        event = self.consensus.propose_consensus(
            topic=f"session_conflict:{conflict.id}",
            participants=participants,
        )

        # Resolve via the consensus system
        resolved = self.consensus.resolve_conflict(conflict)
        if resolved:
            self.consensus.apply_governance(resolved.id)

        self.blackboard.publish(
            slot_name=session.blackboard_slot,
            content=(
                f"Consensus requested for conflict {conflict.id[:10]}: "
                f"{conflict.description[:50]}"
            ),
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.CONSENSUS,
                source_id=event.id,
            ),
            tags=["consensus", "conflict"],
        )

        return event

    # ── Learning Integration ─────────────────────────────────────────

    def learn_from_session(
        self,
        session_id: str,
        autonomous_learning_pipeline: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Extract learnings from a completed or failed session.

        Summarizes the session's routing decisions, task outcomes, and
        collaboration patterns. Passes findings to the autonomous learning
        pipeline for strategy reinforcement.
        """
        session = self._sessions.get(session_id)
        if session is None:
            return None

        if session.status not in (SessionStatus.COMPLETED, SessionStatus.FAILED):
            return None

        # Build learning summary
        successful_specialists = []
        failed_specialists = []

        for task_record in session.tasks:
            if task_record.status == "completed":
                successful_specialists.append(task_record.specialist)
            elif task_record.status == "failed":
                failed_specialists.append(task_record.specialist)

        outcome = "success" if session.status == SessionStatus.COMPLETED else "failure"
        specialist_list = list(session.specialist_participants)

        learning_result = None
        if autonomous_learning_pipeline is not None:
            learning_result = autonomous_learning_pipeline.process_execution_outcome(
                goal_description=session.goal_description,
                outcome=outcome,
                specialist=",".join(specialist_list),
                execution_summary=(
                    f"Collaboration session {session_id[:12]}: "
                    f"{session.completed_count}/{session.total_count} tasks "
                    f"by {', '.join(specialist_list)}"
                ),
                successful_strategy_ids=successful_specialists or None,
                failed_strategy_ids=failed_specialists or None,
            )

        # Store a memory entry about the collaboration
        if self._strategic_memory is not None:
            self._strategic_memory.store(
                memory_type=MemoryType.EXECUTION_TRACE,
                content=(
                    f"Collaboration session {session_id[:12]}: "
                    f"goal='{session.goal_description[:80]}' "
                    f"outcome={outcome} "
                    f"tasks={session.total_count} "
                    f"specialists={specialist_list}"
                ),
                importance=0.6 if outcome == "success" else 0.4,
                tags=["collaboration", "session", outcome],
            )

        return {
            "session_id": session_id,
            "outcome": outcome,
            "tasks_total": session.total_count,
            "tasks_completed": session.completed_count,
            "specialists_involved": specialist_list,
            "routing_decisions": len(self.router.get_routing_history()),
            "learning_result": learning_result,
        }

    # ── Internal Helpers ─────────────────────────────────────────────

    def _build_blackboard_context(self, session: CollaborationSession) -> Dict[str, Any]:
        """Build context from the session's blackboard slot."""
        entries = self.blackboard.read(session.blackboard_slot)
        return {
            "session_id": session.session_id,
            "goal": session.goal_description,
            "blackboard_entries": [
                {"content": e.content[:100], "type": e.entry_type.value}
                for e in entries[-10:]
            ],
            "specialists": list(session.specialist_participants),
        }

    def _check_session_completion(self, session: CollaborationSession) -> None:
        """Check if all tasks in a session are complete."""
        if session.status != SessionStatus.ACTIVE:
            return

        # Check if all non-failed tasks are completed
        all_done = all(
            t.status in ("completed", "failed", "cancelled")
            for t in session.tasks
        )

        if all_done:
            has_failures = any(t.status == "failed" for t in session.tasks)
            if has_failures:
                session.status = SessionStatus.FAILED

            self.close_session(
                session.session_id,
                status=SessionStatus.COMPLETED if not has_failures else SessionStatus.FAILED,
            )

    def _infer_phase(self, task_type: TaskType) -> CollaborationPhase:
        """Map a TaskType to a CollaborationPhase."""
        mapping = {
            TaskType.RESEARCH: CollaborationPhase.RESEARCH,
            TaskType.IMPLEMENT: CollaborationPhase.IMPLEMENTATION,
            TaskType.SECURITY_REVIEW: CollaborationPhase.SECURITY_REVIEW,
            TaskType.EXECUTE: CollaborationPhase.EXECUTION,
            TaskType.REPORT: CollaborationPhase.SYNTHESIS,
        }
        task_str = task_type.value if hasattr(task_type, 'value') else str(task_type)
        for key, phase in mapping.items():
            if key.value == task_str:
                return phase
        return CollaborationPhase.PLANNING

    # ── Reporting ────────────────────────────────────────────────────

    def get_all_sessions(self) -> List[CollaborationSession]:
        """Get all sessions."""
        return list(self._sessions.values())

    def snapshot(self) -> Dict[str, Any]:
        """Get a comprehensive snapshot of the orchestrator state."""
        active = self.get_active_sessions()
        return {
            "total_sessions": len(self._sessions),
            "active_sessions": len(active),
            "router": self.router.snapshot(),
            "sessions": [s.summary() for s in list(self._sessions.values())[-10:]],
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display of orchestrator state."""
        lines = [
            "  ── COLLABORATION ORCHESTRATOR ──",
            f"  Sessions: {len(self._sessions)} total, {len(self.get_active_sessions())} active",
            "",
        ]

        router_snap = self.router.snapshot()
        lines.append(f"  Router: {router_snap['total_routes']} routes, {router_snap['specialists_registered']} specialists")
        routes_str = ", ".join(
            f"{k}={v}" for k, v in router_snap['routes_by_specialist'].items()
        )
        if routes_str:
            lines.append(f"    Routes: {routes_str}")

        # Show active sessions
        active = self.get_active_sessions()
        if active:
            lines.append("")
            lines.append("  Active Sessions:")
            for s in active[:3]:
                lines.append(f"    ◉ [{s.session_id[:12]}] {s.goal_description[:60]}")
                lines.append(f"       Phase: {s.phase.value}  |  "
                             f"Tasks: {s.completed_count}/{s.total_count}")

        lines.append("  ── ── ── ── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)
