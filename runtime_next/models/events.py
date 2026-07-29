from __future__ import annotations
from enum import Enum
from typing import Any, Dict, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime
from .plan import NodeState, NodeType, Criticality


class EventType(str, Enum):
    CAPABILITY_CHANGED = "capability_changed"
    NODE_TRANSITION = "node_transition"
    NODE_FAILED = "node_failed"
    GRAPH_STARTED = "graph_started"
    GRAPH_COMPLETED = "graph_completed"
    PLAN_CREATED = "plan_created"
    RECOVERY_INITIATED = "recovery_initiated"
    RECOVERY_COMPLETED = "recovery_completed"
    BUDGET_WARNING = "budget_warning"
    HANDOFF_INITIATED = "handoff_initiated"
    LOG_MESSAGE = "log_message"
    TELEMETRY = "telemetry"
    # Verification + Self-Healing Runtime Events
    VERIFICATION_STARTED = "verification_started"
    VERIFICATION_COMPLETED = "verification_completed"
    VERIFICATION_FAILED = "verification_failed"
    FAILURE_CLASSIFIED = "failure_classified"
    RECOVERY_INJECTED = "recovery_injected"
    RETRY_BLOCKED = "retry_blocked"
    GRAPH_ROLLBACK = "graph_rollback"
    REPLAY_DIVERGENCE = "replay_divergence"
    CONSISTENCY_CHECK = "consistency_check"
    PLAN_VALIDATED = "plan_validated"
    PLAN_FAILED = "plan_failed"
    # Phase 9 — Collaborative / Mode B Events
    ARCHITECT_DECISION = "architect_decision"
    MODE_SELECTED = "mode_selected"
    TASK_BOARD_TRANSITION = "task_board_transition"
    CONSENSUS_FORMED = "consensus_formed"
    BLACKBOARD_PUBLICATION = "blackboard_publication"
    # Phase 8 — Event Visibility Events
    FINDING_CONSUMED = "finding_consumed"
    CHALLENGE_RAISED = "challenge_raised"
    REPORT_GENERATED = "report_generated"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"


class BaseEvent(BaseModel):
    id: str
    type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    payload: Dict[str, Any] = Field(default_factory=dict)


class NodeTransitionEvent(BaseEvent):
    type: EventType = EventType.NODE_TRANSITION
    node_id: str = ""
    node_type: str = ""
    criticality: str = ""
    from_state: str = ""
    to_state: str = ""
    reason: Optional[str] = None
    steps_consumed: int = 0


class CapabilityEvent(BaseEvent):
    type: EventType = EventType.CAPABILITY_CHANGED
    diff: Dict[str, Any] = Field(default_factory=dict)


class RecoveryEvent(BaseEvent):
    type: EventType = EventType.RECOVERY_INITIATED
    node_id: str = ""
    classification: str = ""
    action: str = ""
    retry_count: int = 0


class GraphEvent(BaseEvent):
    type: EventType
    graph_id: str = ""
    node_count: int = 0
    completed_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    total_steps: int = 0


class TelemetryEvent(BaseEvent):
    type: EventType = EventType.TELEMETRY
    plan_id: str = ""
    node_telemetry: List[Dict[str, Any]] = Field(default_factory=list)
    total_steps_consumed: int = 0
    critical_path_completed: bool = False


class ArchitectPlanEvent(BaseEvent):
    """Event emitted by ArchitectOrchestrator at key plan lifecycle points.

    Emitted for:
    - PLAN_CREATED: A new plan was created (draft)
    - PLAN_VALIDATED: A plan passed self-review and was finalized
    - PLAN_FAILED: A plan failed validation, self-review, or encountered an error
    """
    plan_id: str = Field(..., description="ID of the plan this event relates to")
    plan_title: str = Field(default="", description="Short title of the plan")
    objective: str = Field(default="", description="Primary goal of the plan")
    phase_count: int = Field(default=0, description="Number of execution phases")
    specialist_roles: List[str] = Field(default_factory=list, description="Assigned specialist roles")
    risk_level: str = Field(default="", description="Overall risk level (low/medium/high/critical)")
    verification_count: int = Field(default=0, description="Number of verification checks")
    self_review_score: float = Field(default=0.0, ge=0.0, le=1.0, description="Self-review score")
    failure_reason: Optional[str] = Field(default=None, description="Reason for plan failure (if PLAN_FAILED)")


# ===========================================================================
# Phase 9 — Collaborative Event Models
# ===========================================================================


class ArchitectDecisionEvent(BaseEvent):
    """Emitted when the Architect makes a decision on a consensus, plan, or task.

    Carries the full decision context including outcome, reason, and any
    conditions or override details. Used for TUI visibility and audit logging.

    Per Amendment 3: Consensus is advisory, Architect is authoritative.
    """
    type: EventType = EventType.ARCHITECT_DECISION
    decision_id: str = Field(default="", description="Unique decision identifier")
    outcome: str = Field(default="", description="Decision outcome: approve, reject, escalate, replan, override")
    target_type: str = Field(default="", description="What the decision applies to (plan, task, consensus)")
    target_id: str = Field(default="", description="ID of the target")
    reason: str = Field(default="", description="Human-readable justification")
    conditions: List[str] = Field(default_factory=list, description="Conditions that must be satisfied")
    assigned_to: str = Field(default="", description="Specialist assigned to act on this decision")
    overridden_recommendation: str = Field(default="", description="Original recommendation that was overridden")
    override_rationale: str = Field(default="", description="Why the Architect chose to override")
    replan_trigger: str = Field(default="", description="Replan trigger reason")
    replan_scope: str = Field(default="", description="'full' or 'partial' replan")

    def to_summary(self) -> Dict[str, Any]:
        """Compact summary for logging / TUI display."""
        return {
            "decision_id": self.decision_id[:12],
            "outcome": self.outcome.upper(),
            "target": f"{self.target_type}:{self.target_id[:12]}",
            "reason": self.reason[:80],
            "conditions": len(self.conditions),
            "assigned_to": self.assigned_to,
        }


class ModeSelectionEvent(BaseEvent):
    """Emitted when the execution mode is selected for a turn.

    Per Amendment 1: The Architect evaluates the task and selects
    Consolidated (Mode A) or Collaborative (Mode B) execution.
    """
    type: EventType = EventType.MODE_SELECTED
    mode: str = Field(default="", description="Selected mode: 'consolidated' (Mode A) or 'collaborative' (Mode B)")
    rationale: str = Field(default="", description="Why this mode was selected")
    task_preview: str = Field(default="", description="First 80 chars of the task")
    risk_profile: str = Field(default="", description="Risk level that influenced selection")
    complexity: int = Field(default=0, ge=0, le=10, description="Complexity score that influenced selection")
    has_explicit_prefix: bool = Field(default=False, description="Whether @MODE_A/@MODE_B prefix was used")
    triggers: List[str] = Field(default_factory=list, description="What triggered this mode selection")

    def to_summary(self) -> Dict[str, Any]:
        return {
            "mode": "Mode A (Consolidated)" if self.mode == "consolidated" else "Mode B (Collaborative)",
            "rationale": self.rationale[:80],
            "explicit": self.has_explicit_prefix,
            "risk": self.risk_profile,
            "complexity": self.complexity,
        }


class TaskBoardTransitionEvent(BaseEvent):
    """Emitted when a task on the SharedTaskBoard transitions state.

    Every task state change is: validated by TaskStateMachine, persisted
    to SQLite, and published to the EventBus for TUI visibility.
    """
    type: EventType = EventType.TASK_BOARD_TRANSITION
    task_id: str = Field(default="", description="Task identifier")
    task_type: str = Field(default="", description="Task type (research, implement, security_review, etc.)")
    from_status: str = Field(default="", description="Previous task status")
    to_status: str = Field(default="", description="New task status")
    specialist: str = Field(default="", description="Assigned specialist")
    reason: str = Field(default="", description="Why the transition occurred")
    session_id: str = Field(default="", description="Session this task belongs to")

    def to_summary(self) -> Dict[str, Any]:
        icon = {
            "completed": "done",
            "failed": "err",
            "in_progress": "running",
            "blocked": "wait",
        }.get(self.to_status, "next")
        return {
            "task": f"{self.task_id[:8]} ({self.task_type})",
            "transition": f"{self.from_status} -> {self.to_status}",
            "specialist": self.specialist,
            "reason": self.reason[:60],
            "icon": icon,
        }


class ConsensusEvent(BaseEvent):
    """Emitted when consensus is formed on a task or proposal.

    Per Amendment 3: Consensus is advisory, Architect is authoritative.
    This event carries the advisory positions for TUI visibility and
    audit logging.

    The consensus positions inform the Architect's decision but do not
    determine it — the Architect may APPROVE, REJECT, ESCALATE, REPLAN,
    or OVERRIDE.
    """
    type: EventType = EventType.CONSENSUS_FORMED
    consensus_id: str = Field(default="", description="Unique identifier for this consensus")
    target_id: str = Field(default="", description="What this consensus applies to")
    recommendation: str = Field(default="", description="Advisory recommendation from consensus")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0, description="Consensus confidence level")
    positions: Dict[str, str] = Field(default_factory=dict, description="Specialist -> position mapping")
    method: str = Field(default="", description="Resolution strategy used (majority, unanimous, weighted, etc.)")

    def to_summary(self) -> Dict[str, Any]:
        for_count = sum(1 for p in self.positions.values() if p.lower() in ("yes", "for", "approve"))
        against_count = sum(1 for p in self.positions.values() if p.lower() in ("no", "against", "reject"))
        return {
            "consensus_id": self.consensus_id[:12],
            "recommendation": self.recommendation[:80],
            "confidence": f"{self.confidence:.2f}",
            "for": for_count,
            "against": against_count,
            "method": self.method,
        }


class BlackboardPublicationEvent(BaseEvent):
    """Emitted when a specialist publishes an entry to the CognitiveBlackboard.

    Per Amendment 2: All specialist communication flows through the blackboard.
    No agent-to-agent messaging. This event enables TUI visibility of all
    blackboard activity.

    Carries trust metadata (confidence, source, verification_status,
    challenged, lifecycle_status) so the TUI can display trust indicators
    with real data instead of defaults.
    """
    type: EventType = EventType.BLACKBOARD_PUBLICATION
    specialist: str = Field(default="", description="Specialist that published")
    entry_type: str = Field(default="", description="Type of blackboard entry (finding, implementation, review, report, etc.)")
    summary: str = Field(default="", description="Short summary of the publication")
    tags: List[str] = Field(default_factory=list, description="Tags associated with the entry")
    session_id: str = Field(default="", description="Session this publication belongs to")

    # ── Trust metadata (Phase 6: Trust Visibility) ───────────────────
    confidence: float = Field(default=0.0, ge=0.0, le=1.0,
                              description="Confidence score from the producing specialist")
    source: str = Field(default="",
                        description="Provenance source (repository_analysis, code_review, etc.)")
    verification_status: str = Field(default="pending",
                                     description="Current verification state (pending, verified, challenged, etc.)")
    challenged: bool = Field(default=False,
                             description="Whether this entry has active challenges")
    challenge_count: int = Field(default=0, ge=0,
                                 description="Number of active challenges against this entry")
    lifecycle_status: str = Field(default="created",
                                  description="Lifecycle state (created, consumed, challenged, approved, etc.)")

    def to_summary(self) -> Dict[str, Any]:
        return {
            "specialist": self.specialist,
            "entry_type": self.entry_type,
            "summary": self.summary[:60],
            "tags": len(self.tags),
            "confidence": self.confidence,
            "verification_status": self.verification_status,
            "challenged": self.challenged,
        }


class FindingConsumedEvent(BaseEvent):
    """Emitted when a specialist consumes a blackboard entry.

    Enables TUI visibility of "who used what evidence" — shows the
    consumption trail in real time.
    """
    type: EventType = EventType.FINDING_CONSUMED
    entry_id: str = Field(default="", description="ID of the consumed entry")
    consumer: str = Field(default="", description="Specialist that consumed the entry")
    entry_owner: str = Field(default="", description="Original specialist that published the entry")
    entry_type: str = Field(default="", description="Type of entry consumed")
    slot_name: str = Field(default="", description="Blackboard slot containing the entry")

    def to_summary(self) -> Dict[str, Any]:
        return {
            "consumer": self.consumer,
            "entry": f"{self.entry_id[:8]} ({self.entry_type})",
            "owner": self.entry_owner,
            "slot": self.slot_name,
        }


class ChallengeRaisedEvent(BaseEvent):
    """Emitted when a specialist raises a challenge on a blackboard entry.

    Shows the challenger, challenged entry, and the claim being challenged.
    Used by TUI to display real-time challenge activity.
    """
    type: EventType = EventType.CHALLENGE_RAISED
    challenge_id: str = Field(default="", description="Unique challenge identifier")
    entry_id: str = Field(default="", description="ID of the challenged entry")
    challenger: str = Field(default="", description="Specialist that raised the challenge")
    challenged_claim: str = Field(default="", description="The claim being challenged")
    evidence: str = Field(default="", description="Evidence supporting the challenge")

    def to_summary(self) -> Dict[str, Any]:
        return {
            "challenger": self.challenger,
            "entry": self.entry_id[:8],
            "claim": self.challenged_claim[:60],
        }


class ReportGeneratedEvent(BaseEvent):
    """Emitted when HERALD generates a session report.

    Carries the report summary, session title, and key metrics for
    TUI display and audit logging.
    """
    type: EventType = EventType.REPORT_GENERATED
    report_id: str = Field(default="", description="Blackboard entry ID of the report")
    session_title: str = Field(default="", description="Title of the session")
    summary_length: int = Field(default=0, description="Length of the report narrative")
    evidence_count: int = Field(default=0, description="Number of evidence items in report")
    challenge_count: int = Field(default=0, description="Number of challenges in session")

    def to_summary(self) -> Dict[str, Any]:
        return {
            "report": self.report_id[:8],
            "title": self.session_title[:40],
            "size": f"{self.summary_length} chars",
            "evidence": self.evidence_count,
        }


class ExecutionStartedEvent(BaseEvent):
    """Emitted when TERMINUS begins execution.

    Carries the command being executed and the task context for
    TUI visibility.
    """
    type: EventType = EventType.EXECUTION_STARTED
    task_id: str = Field(default="", description="Task ID being executed")
    command: str = Field(default="", description="Command being executed")
    specialist: str = Field(default="TERMINUS", description="Executing specialist")

    def to_summary(self) -> Dict[str, Any]:
        return {
            "task": self.task_id[:8],
            "command": self.command[:60],
        }


class ExecutionCompletedEvent(BaseEvent):
    """Emitted when TERMINUS completes execution.

    Carries the exit code, stdout preview, and result entry ID for
    TUI visibility.
    """
    type: EventType = EventType.EXECUTION_COMPLETED
    task_id: str = Field(default="", description="Task ID that completed")
    entry_id: str = Field(default="", description="Blackboard entry ID with results")
    exit_code: int = Field(default=0, description="Process exit code")
    specialist: str = Field(default="TERMINUS", description="Executing specialist")

    def to_summary(self) -> Dict[str, Any]:
        return {
            "task": self.task_id[:8],
            "entry": self.entry_id[:8],
            "exit_code": self.exit_code,
        }
