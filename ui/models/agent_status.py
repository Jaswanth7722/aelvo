"""ui/models/agent_status.py — AgentStatus Model & Tracker

Per-agent tracked profiles populated from real runtime events.
Every agent gets: current task, status, confidence, last action,
task count, success rate, and contribution score.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

AGENT_NAMES = ["HERMES", "ARCHITECT", "ORACLE", "FORGE", "SENTINEL", "TERMINUS", "HERALD"]


@dataclass
class AgentStatus:
    """Real-time profile for a single specialist agent.

    All fields are populated from actual EventBus data.
    """

    name: str
    status: str = "inactive"          # inactive | thinking | researching | coding | reviewing | acting | active
    current_task: str = ""            # Short description of the task they're working on
    last_action: str = ""             # Last recorded activity string
    confidence: float = 0.0           # Latest confidence score (0.0–1.0)
    task_count: int = 0               # Total tasks assigned
    success_count: int = 0            # Tasks completed successfully
    failure_count: int = 0            # Tasks that failed
    participation_count: int = 0      # Consensus participations
    last_updated: float = 0.0         # Timestamp of last activity

    @property
    def success_rate(self) -> float:
        """Fraction of completed tasks that succeeded (0.0–1.0)."""
        total = self.success_count + self.failure_count
        return round(self.success_count / total, 4) if total > 0 else 0.0

    @property
    def contribution_score(self) -> float:
        """Weighted contribution score as a percentage (0–100).

        Computed from:
        - Task activity (30%): saturates at 20 tasks
        - Success rate (40%): how reliable the agent is
        - Confidence (30%): self-reported confidence
        """
        if self.task_count == 0 and self.success_count == 0:
            return 0.0
        task_factor = min(1.0, self.task_count / 20.0)
        success_factor = self.success_rate
        confidence_factor = self.confidence
        raw = task_factor * 0.30 + success_factor * 0.40 + confidence_factor * 0.30
        return round(raw * 100, 1)

    @property
    def is_active(self) -> bool:
        return self.status.lower() not in ("inactive", "deactivated")


class AgentStatusTracker:
    """Tracks per-agent profiles in real time from EventBus data.

    Usage:
        tracker = AgentStatusTracker()
        tracker.on_specialist_event("ORACLE", "thinking", "Researching deps", 0.91)
        tracker.on_task_event("task_1", "Dependency Analysis", "running", "ORACLE")
        tracker.on_task_event("task_1", "Dependency Analysis", "completed", "ORACLE")
        profile = tracker.get("ORACLE")  # AgentStatus with all fields
    """

    def __init__(self):
        self._agents: Dict[str, AgentStatus] = {}
        self._task_owners: Dict[str, str] = {}  # task_id -> specialist
        self._seen_tasks: set = set()  # task_ids already counted for assignment

    def _ensure(self, name: str) -> AgentStatus:
        """Get or create an agent profile."""
        name = name.upper()
        if name not in self._agents:
            self._agents[name] = AgentStatus(name=name)
        return self._agents[name]

    # ── Event Handlers ──────────────────────────────────────────

    def on_specialist_event(self, specialist: str, state: str, action: str = "", score: float = 0.0) -> None:
        """Handle specialist state change events."""
        if not specialist:
            return
        name = specialist.upper()
        agent = self._ensure(name)
        agent.status = state
        agent.last_action = action[:64] if action else agent.last_action
        if score > 0:
            agent.confidence = score
        agent.last_updated = __import__("time").time()

    def on_task_event(self, task_id: str, task_name: str, status: str, specialist: str = "") -> None:
        """Handle task lifecycle events to track per-agent task counts."""
        status_lower = status.lower()
        specialist_upper = specialist.upper() if specialist else ""

        # Track task ownership — broad status set to match bridge event values
        if status_lower in ("pending", "assigned", "active", "running", "processing", "in_progress"):
            if specialist_upper:
                self._task_owners[task_id] = specialist_upper
                agent = self._ensure(specialist_upper)
                agent.current_task = task_name[:64]
                # Increment task count on first assignment (any active status)
                if task_id not in self._seen_tasks:
                    agent.task_count += 1
                    self._seen_tasks.add(task_id)

        # Track outcomes
        if status_lower == "completed":
            owner = self._task_owners.pop(task_id, specialist_upper)
            if owner:
                self._ensure(owner).success_count += 1
                self._ensure(owner).current_task = ""
        elif status_lower == "failed":
            owner = self._task_owners.pop(task_id, specialist_upper)
            if owner:
                self._ensure(owner).failure_count += 1
                self._ensure(owner).current_task = ""

    def on_consensus_event(self, topic: str, outcome: str, confidence: float = 0.0, participants: List[str] = None) -> None:
        """Handle consensus participation for tracking engagement."""
        if participants:
            for p in participants:
                p_upper = p.upper()
                if p_upper in AGENT_NAMES:
                    self._ensure(p_upper).participation_count += 1

    def on_collaboration_finding(self, specialist: str, summary: str, entry_type: str = "finding", confidence: float = 0.0) -> None:
        """Track findings published by specialists (contribution signal)."""
        if specialist:
            agent = self._ensure(specialist)
            agent.last_action = summary[:64]
            if confidence > 0:
                agent.confidence = confidence

    def on_collaboration_decision(self, specialist: str, outcome: str) -> None:
        """Track architect/specialist decisions."""
        if specialist:
            agent = self._ensure(specialist)
            agent.last_action = f"Decision: {outcome[:40]}"

    # ── Queries ─────────────────────────────────────────────────

    def get(self, name: str) -> Optional[AgentStatus]:
        """Get the profile for a specific agent."""
        agent = self._agents.get(name.upper())
        return agent

    def get_all(self) -> List[AgentStatus]:
        """Get all agent profiles."""
        return list(self._agents.values())

    def get_ordered(self) -> List[AgentStatus]:
        """Get all agent profiles in canonical order."""
        result = []
        for name in AGENT_NAMES:
            if name in self._agents:
                result.append(self._agents[name])
            else:
                result.append(AgentStatus(name=name))
        return result

    def get_active(self) -> List[AgentStatus]:
        """Get profiles for currently active agents."""
        return [a for a in self._agents.values() if a.is_active]

    def snapshot(self) -> Dict[str, AgentStatus]:
        """Return a snapshot dict of all tracked profiles."""
        return dict(self._agents)
