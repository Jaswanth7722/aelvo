"""ui/core/ui_event.py — UIEvent: The Single Abstraction for All Visible Actions

Phase 11: UI Event Standardization.

Every visible action in the AELVO TUI becomes a UIEvent instance.
The bridge emits UIEvents. Widgets subscribe to the UIEventType they
care about. No direct widget method calls from the bridge.

This is the canonical event type for display purposes.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict


class UIEventType(Enum):
    """The complete set of visible actions in the AELVO TUI.

    Every event the TUI displays maps to exactly one of these types.
    No other event types should flow to the display layer.
    """

    # ── Task lifecycle ────────────────────────────────────────────
    TASK_CREATED = "task_created"
    TASK_ASSIGNED = "task_assigned"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_BLOCKED = "task_blocked"
    TASK_CANCELLED = "task_cancelled"
    TASK_PROGRESS = "task_progress"

    # ── Specialist lifecycle ───────────────────────────────────────
    SPECIALIST_ACTIVATED = "specialist_activated"
    SPECIALIST_DEACTIVATED = "specialist_deactivated"
    SPECIALIST_THINKING = "specialist_thinking"
    SPECIALIST_ACTION = "specialist_action"

    # ── Tool execution ─────────────────────────────────────────────
    TOOL_STARTED = "tool_started"
    TOOL_COMPLETED = "tool_completed"
    TOOL_FAILED = "tool_failed"

    # ── Verification ───────────────────────────────────────────────
    VERIFICATION_PASSED = "verification_passed"
    VERIFICATION_FAILED = "verification_failed"
    VERIFICATION_RUNNING = "verification_running"

    # ── Collaboration findings ─────────────────────────────────────
    FINDING_PUBLISHED = "finding_published"
    EVIDENCE_CONSUMED = "evidence_consumed"
    CHALLENGE_RAISED = "challenge_raised"

    # ── Consensus ──────────────────────────────────────────────────
    CONSENSUS_STARTED = "consensus_started"
    CONSENSUS_POSITION = "consensus_position"
    CONSENSUS_OUTCOME = "consensus_outcome"

    # ── Decisions ──────────────────────────────────────────────────
    DECISION_APPROVED = "decision_approved"
    DECISION_REJECTED = "decision_rejected"
    DECISION_OVERRIDE = "decision_override"
    DECISION_REPLAN = "decision_replan"

    # ── Execution ──────────────────────────────────────────────────
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"

    # ── Recovery ───────────────────────────────────────────────────
    PROVIDER_FAILURE = "provider_failure"
    FALLBACK_ACTIVATED = "fallback_activated"
    RETRY_STARTED = "retry_started"
    RECOVERY_SUCCEEDED = "recovery_succeeded"
    RECOVERY_FAILED = "recovery_failed"
    SPECIALIST_REASSIGNED = "specialist_reassigned"

    # ── Reporting ──────────────────────────────────────────────────
    REPORT_GENERATED = "report_generated"
    HERALD_NARRATIVE = "herald_narrative"

    # ── System ─────────────────────────────────────────────────────
    SYSTEM_ONLINE = "system_online"
    SYSTEM_ERROR = "system_error"
    SYSTEM_WARNING = "system_warning"
    USER_MESSAGE = "user_message"
    RESPONSE_MESSAGE = "response_message"

    # ── Overview snapshot ──────────────────────────────────────────
    OVERVIEW_UPDATED = "overview_updated"
    WORK_QUEUE_UPDATED = "work_queue_updated"
    CONSENSUS_UPDATED = "consensus_updated"
    RECOVERY_UPDATED = "recovery_updated"
    AGENT_METRICS_UPDATED = "agent_metrics_updated"


# Display metadata for each UIEventType
UIEVENT_DISPLAY: Dict[UIEventType, Dict[str, Any]] = {
    # Task
    UIEventType.TASK_CREATED:     {"icon": "○", "color": "#52627f"},
    UIEventType.TASK_ASSIGNED:    {"icon": "→", "color": "#a565ff"},
    UIEventType.TASK_STARTED:     {"icon": "◉", "color": "#3b82f6"},
    UIEventType.TASK_COMPLETED:   {"icon": "✓", "color": "#00e38c"},
    UIEventType.TASK_FAILED:      {"icon": "✗", "color": "#ff5c7a"},
    UIEventType.TASK_BLOCKED:     {"icon": "⊘", "color": "#f7b731"},
    UIEventType.TASK_CANCELLED:   {"icon": "−", "color": "#52627f"},
    UIEventType.TASK_PROGRESS:    {"icon": "◔", "color": "#3b82f6"},
    # Specialist
    UIEventType.SPECIALIST_ACTIVATED:   {"icon": "●", "color": "#00e38c"},
    UIEventType.SPECIALIST_DEACTIVATED: {"icon": "○", "color": "#52627f"},
    UIEventType.SPECIALIST_THINKING:    {"icon": "◌", "color": "#3b82f6"},
    UIEventType.SPECIALIST_ACTION:      {"icon": "▶", "color": "#a565ff"},
    # Tool
    UIEventType.TOOL_STARTED:  {"icon": "⚙", "color": "#f7b731"},
    UIEventType.TOOL_COMPLETED: {"icon": "✓", "color": "#00e38c"},
    UIEventType.TOOL_FAILED:   {"icon": "✗", "color": "#ff5c7a"},
    # Verification
    UIEventType.VERIFICATION_PASSED:  {"icon": "✓", "color": "#00e38c"},
    UIEventType.VERIFICATION_FAILED:  {"icon": "✗", "color": "#ff5c7a"},
    UIEventType.VERIFICATION_RUNNING: {"icon": "◌", "color": "#3b82f6"},
    # Collaboration
    UIEventType.FINDING_PUBLISHED: {"icon": "◆", "color": "#8c5cff"},
    UIEventType.EVIDENCE_CONSUMED: {"icon": "▷", "color": "#00d889"},
    UIEventType.CHALLENGE_RAISED:  {"icon": "⚠", "color": "#ff5c7a"},
    # Consensus
    UIEventType.CONSENSUS_STARTED:  {"icon": "◉", "color": "#3b82f6"},
    UIEventType.CONSENSUS_POSITION: {"icon": "◔", "color": "#19f5a5"},
    UIEventType.CONSENSUS_OUTCOME:  {"icon": "↻", "color": "#19f5a5"},
    # Decisions
    UIEventType.DECISION_APPROVED:  {"icon": "✓", "color": "#00e38c"},
    UIEventType.DECISION_REJECTED:  {"icon": "✗", "color": "#ff5c7a"},
    UIEventType.DECISION_OVERRIDE:  {"icon": "⚠", "color": "#a565ff"},
    UIEventType.DECISION_REPLAN:    {"icon": "⟳", "color": "#f7b731"},
    # Execution
    UIEventType.EXECUTION_STARTED:  {"icon": "▶", "color": "#f7b731"},
    UIEventType.EXECUTION_COMPLETED: {"icon": "✓", "color": "#00e38c"},
    # Recovery
    UIEventType.PROVIDER_FAILURE:    {"icon": "⛔", "color": "#ff5c7a"},
    UIEventType.FALLBACK_ACTIVATED:  {"icon": "🔀", "color": "#f7b731"},
    UIEventType.RETRY_STARTED:       {"icon": "🔄", "color": "#3b82f6"},
    UIEventType.RECOVERY_SUCCEEDED:  {"icon": "✅", "color": "#00e38c"},
    UIEventType.RECOVERY_FAILED:     {"icon": "❌", "color": "#ff5c7a"},
    UIEventType.SPECIALIST_REASSIGNED: {"icon": "🔄", "color": "#a565ff"},
    # Report
    UIEventType.REPORT_GENERATED:  {"icon": "★", "color": "#39c8ff"},
    UIEventType.HERALD_NARRATIVE:  {"icon": "★", "color": "#39c8ff"},
    # System
    UIEventType.SYSTEM_ONLINE:    {"icon": "●", "color": "#00e38c"},
    UIEventType.SYSTEM_ERROR:     {"icon": "✗", "color": "#ff5c7a"},
    UIEventType.SYSTEM_WARNING:   {"icon": "⚠", "color": "#f7b731"},
    UIEventType.USER_MESSAGE:     {"icon": "◉", "color": "#1f8fff"},
    UIEventType.RESPONSE_MESSAGE: {"icon": "○", "color": "#8c5cff"},
    # Updates
    UIEventType.OVERVIEW_UPDATED:       {"icon": "", "color": "#52627f"},
    UIEventType.WORK_QUEUE_UPDATED:     {"icon": "", "color": "#52627f"},
    UIEventType.CONSENSUS_UPDATED:      {"icon": "", "color": "#52627f"},
    UIEventType.RECOVERY_UPDATED:       {"icon": "", "color": "#52627f"},
    UIEventType.AGENT_METRICS_UPDATED:  {"icon": "", "color": "#52627f"},
}


@dataclass
class UIEvent:
    """A single visible action in the AELVO TUI.

    This is the canonical display event. Every visible action in the
    TUI is represented by exactly one UIEvent. The bridge emits these.
    Widgets subscribe to specific UIEventType values.

    Attributes:
        type: The canonical event type (UIEventType enum).
        source: Subsystem that produced the event (e.g., "orchestrator", "consensus", "recovery").
        specialist: The agent involved (e.g., "ORACLE", "FORGE", "ARCHITECT") or empty.
        action: Short human-readable description of what happened (max 80 chars).
        data: Structured metadata dict with event-specific fields.
        timestamp: Unix timestamp of the event.
    """

    type: UIEventType
    source: str = ""
    specialist: str = ""
    action: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @property
    def icon(self) -> str:
        return UIEVENT_DISPLAY.get(self.type, {}).get("icon", "•")

    @property
    def color(self) -> str:
        return UIEVENT_DISPLAY.get(self.type, {}).get("color", "#52627f")

    def to_display_line(self) -> str:
        """Format as a single-line display string."""
        ts = time.localtime(self.timestamp)
        stamp = f"{ts.tm_hour:02d}:{ts.tm_min:02d}"
        parts = [f"[#52627f]{stamp}[/] [{self.color}]{self.icon}[/]"]
        if self.specialist:
            parts.append(f"[bold {self.color}]{self.specialist}[/]")
        parts.append(self.action[:70])
        return " ".join(parts)
