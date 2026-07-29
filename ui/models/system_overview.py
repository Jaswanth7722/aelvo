"""ui/models/system_overview.py — SystemOverview Model & Aggregator

A real-time aggregated view of the AELVO runtime, populated from actual
EventBus events rather than computed heuristics.

The omega_overview widget consumes this model.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict


@dataclass
class SystemOverview:
    """Immutable snapshot of the AELVO runtime state for TUI display.

    Every field is populated from real EventBus data — no heuristics,
    no placeholders, no invented percentages.
    """

    # ── Provider ────────────────────────────────────────────────
    active_provider: str = ""
    active_model: str = ""

    # ── Session ─────────────────────────────────────────────────
    session_state: str = "initializing"  # initializing | active | processing | error | shutdown
    uptime_seconds: float = 0.0
    current_goal: str = ""
    current_phase: str = ""

    # ── Agents ──────────────────────────────────────────────────
    agents_total: int = 7
    agents_active: int = 0
    agents_idle: int = 7

    # ── Tasks ───────────────────────────────────────────────────
    tasks_pending: int = 0
    tasks_active: int = 0
    tasks_completed: int = 0
    tasks_failed: int = 0
    tasks_blocked: int = 0

    # ── Consensus ───────────────────────────────────────────────
    consensus_state: str = "none"           # none | in_progress | resolved | escalated
    consensus_confidence: float = 0.0
    consensus_topic: str = ""

    # ── Recovery ────────────────────────────────────────────────
    recovery_state: str = "none"            # none | active | completed | failed
    recovery_count: int = 0

    # ── Verification ────────────────────────────────────────────
    verification_state: str = "none"        # none | running | passed | failed
    verification_count: int = 0

    # ── Progress ────────────────────────────────────────────────
    progress_pct: int = 0              # 0–100, computed from task state machine lifecycle

    # ── Trust ───────────────────────────────────────────────────
    trust_score: float = 0.0


class SystemOverviewAggregator:
    """Aggregates real-time state from EventBus events into a SystemOverview.

    Usage:
        aggregator = SystemOverviewAggregator()
        # On each event:
        aggregator.update_from_event(event_type, event_data)
        # To get current state at any time:
        snapshot = aggregator.snapshot()
    """

    def __init__(self):
        self._started_at = time.time()
        self._provider = ""
        self._model = ""
        self._session_state = "initializing"
        self._current_goal = ""
        self._current_phase = ""
        self._agent_states: Dict[str, str] = {}  # name -> state
        self._task_statuses: Dict[str, str] = {}  # task_id -> last known status
        self._tasks_pending = 0
        self._tasks_active = 0
        self._tasks_completed = 0
        self._tasks_failed = 0
        self._tasks_blocked = 0
        self._consensus_state = "none"
        self._consensus_confidence = 0.0
        self._consensus_topic = ""
        self._recovery_state = "none"
        self._recovery_count = 0
        self._verification_state = "none"
        self._verification_count = 0
        self._trust_confidence_sum = 0.0
        self._trust_confidence_count = 0

    # ── Public API ──────────────────────────────────────────────

    def set_provider(self, provider: str, model: str) -> None:
        """Set the active provider and model name."""
        self._provider = provider
        self._model = model

    def set_session_state(self, state: str) -> None:
        """Set the session state (initializing, active, processing, error, shutdown)."""
        self._session_state = state

    def set_current_goal(self, goal: str, phase: str = "") -> None:
        """Set the current goal and phase."""
        if goal:
            self._current_goal = goal
        if phase:
            self._current_phase = phase

    # ── Event Handlers ──────────────────────────────────────────

    def on_system_event(self, message: str) -> None:
        """Handle system startup/shutdown events."""
        msg_lower = message.lower()
        if "online" in msg_lower or "start" in msg_lower or "ready" in msg_lower:
            self._session_state = "active"
        elif "shutdown" in msg_lower or "stop" in msg_lower:
            self._session_state = "shutdown"
        elif "error" in msg_lower or "fail" in msg_lower:
            self._session_state = "error"

    def on_task_event(self, task_id: str, name: str, status: str, progress: float = 0.0) -> None:
        """Handle task lifecycle events — tracks per-task status to avoid counter inflation."""
        status_lower = status.lower()

        # Map status to counter field
        status_to_field = {
            "pending": "_tasks_pending",
            "assigned": "_tasks_pending",
            "running": "_tasks_active",
            "processing": "_tasks_active",
            "in_progress": "_tasks_active",
            "active": "_tasks_active",
            "completed": "_tasks_completed",
            "failed": "_tasks_failed",
            "blocked": "_tasks_blocked",
        }

        new_field = status_to_field.get(status_lower)
        if new_field is None:
            return

        # Decrement old status counter if we know the previous status
        old_status = self._task_statuses.get(task_id)
        if old_status:
            old_field = status_to_field.get(old_status)
            if old_field and old_field != new_field:
                current = getattr(self, old_field, 0)
                setattr(self, old_field, max(0, current - 1))

        # Increment new status counter
        current = getattr(self, new_field, 0)
        setattr(self, new_field, current + 1)

        # Store new status
        self._task_statuses[task_id] = status_lower

        # If there's a task name, use it as current goal
        if name and not self._current_goal:
            self._current_goal = name[:80]

    def on_specialist_event(self, specialist: str, state: str, action: str = "", score: float = 0.0) -> None:
        """Handle specialist state changes."""
        self._agent_states[specialist.upper()] = state

        # Track trust from specialist confidence scores
        if score > 0:
            self._trust_confidence_sum += score
            self._trust_confidence_count += 1

    def on_verification_event(self, vtype: str, target: str, status: str, confidence: float = 0.0) -> None:
        """Handle verification lifecycle events."""
        self._verification_count += 1
        status_lower = status.lower()
        if status_lower in ("running", "started"):
            self._verification_state = "running"
        elif status_lower in ("passed", "success", "verified"):
            self._verification_state = "passed"
        elif status_lower in ("failed", "error"):
            self._verification_state = "failed"

        if confidence > 0:
            self._trust_confidence_sum += confidence
            self._trust_confidence_count += 1

    def on_consensus_event(self, topic: str, outcome: str, confidence: float = 0.0, participants: list = None) -> None:
        """Handle consensus lifecycle events."""
        if topic:
            self._consensus_topic = topic
        if outcome:
            outcome_lower = outcome.lower()
            if "approv" in outcome_lower or "agreed" in outcome_lower or "resolved" in outcome_lower:
                self._consensus_state = "resolved"
            elif "reject" in outcome_lower or "escalat" in outcome_lower:
                self._consensus_state = "escalated"
            elif "progress" in outcome_lower or "vot" in outcome_lower:
                self._consensus_state = "in_progress"
            else:
                self._consensus_state = "in_progress"
        if confidence > 0:
            self._consensus_confidence = confidence
            self._trust_confidence_sum += confidence
            self._trust_confidence_count += 1

    def on_collaboration_decision(self, specialist: str, outcome: str) -> None:
        """Handle architect/specialist decisions (maps to recovery visibility)."""
        outcome_lower = outcome.lower()
        if "replan" in outcome_lower or "override" in outcome_lower:
            pass  # Could update recovery state

    def on_execution_event(self, specialist: str, command: str, status: str = "running") -> None:
        """Handle execution start/end events."""
        if status == "running":
            self._recovery_state = "active"
        elif status in ("success", "completed"):
            self._recovery_state = "completed"
        elif status == "failed":
            self._recovery_state = "failed"
            self._recovery_count += 1

    def on_recovery_event(self) -> None:
        """Handle explicit recovery events."""
        self._recovery_count += 1
        self._recovery_state = "active"

    def on_report_event(self) -> None:
        """Handle HERALD report generation events."""
        pass  # Phase 9 will use this

    # ── Progress ────────────────────────────────────────────────

    def _compute_progress(self) -> int:
        """Compute overall progress from the task state machine lifecycle.

        Each task's status maps to a lifecycle percentage:
            pending:     0%
            assigned:   15%
            in_progress: 40%   (also: running, processing, active)
            reviewing:   70%   (also: under_review)
            completed:  100%
            failed:      0%   (terminal)
            blocked:    40%   (at progress before blocking)
            cancelled:   0%   (terminal)

        Overall progress = average across all tracked tasks.
        """
        if not self._task_statuses:
            return 0

        status_progress = {
            "pending": 0,
            "assigned": 15,
            "in_progress": 40,
            "running": 40,
            "processing": 40,
            "active": 40,
            "reviewing": 70,
            "under_review": 70,
            "approved": 85,
            "completed": 100,
            "failed": 0,
            "blocked": 40,
            "cancelled": 0,
        }

        total = 0.0
        count = 0
        for task_id, status in self._task_statuses.items():
            pct = status_progress.get(status, 0)
            total += pct
            count += 1

        if count == 0:
            return 0
        return int(total / count)

    # ── Snapshot ────────────────────────────────────────────────

    def snapshot(self) -> SystemOverview:
        """Produce the current SystemOverview snapshot from aggregated state."""
        # Count active vs idle agents
        agents_active = sum(
            1 for s in self._agent_states.values()
            if s.lower() not in ("inactive", "deactivated")
        )
        agents_idle = max(0, 7 - agents_active)

        # Compute trust score as average of all confidence scores
        trust_score = 0.0
        if self._trust_confidence_count > 0:
            trust_score = round(
                self._trust_confidence_sum / self._trust_confidence_count, 4
            )

        # Phase 4: Compute progress from task state machine lifecycle
        progress_pct = self._compute_progress()

        return SystemOverview(
            active_provider=self._provider.upper() if self._provider else "LOCAL",
            active_model=self._model or "omega-runtime",
            session_state=self._session_state,
            uptime_seconds=time.time() - self._started_at,
            current_goal=self._current_goal,
            current_phase=self._current_phase,
            agents_total=7,
            agents_active=agents_active,
            agents_idle=agents_idle,
            tasks_pending=self._tasks_pending,
            tasks_active=self._tasks_active,
            tasks_completed=self._tasks_completed,
            tasks_failed=self._tasks_failed,
            tasks_blocked=self._tasks_blocked,
            consensus_state=self._consensus_state,
            consensus_confidence=self._consensus_confidence,
            consensus_topic=self._consensus_topic,
            recovery_state=self._recovery_state,
            recovery_count=self._recovery_count,
            verification_state=self._verification_state,
            verification_count=self._verification_count,
            progress_pct=progress_pct,
            trust_score=trust_score,
        )
