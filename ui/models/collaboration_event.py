"""ui/models/collaboration_event.py — Typed Collaboration Event Model

Structured events that represent real collaboration activity.
Every event maps to a real runtime event — no fake chat.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from ui.models.trust_indicator import TrustIndicator


class CollaborationEventType(str, Enum):
    """The canonical set of collaboration event types.

    Every visible action in the TUI maps to one of these.
    """

    TASK_ASSIGNED = "task_assigned"                   # Architect assigned task to specialist
    FINDING_PUBLISHED = "finding_published"            # Oracle published a finding
    EVIDENCE_CONSUMED = "evidence_consumed"            # Forge consumed evidence
    CHALLENGE_RAISED = "challenge_raised"              # Sentinel raised a challenge
    CONSENSUS_OUTCOME = "consensus_outcome"            # Consensus completed (approved/revision/escalated)
    DECISION_APPROVED = "decision_approved"            # Architect approved decision
    EXECUTION_ACTION = "execution_action"              # Terminus executed action
    REPORT_GENERATED = "report_generated"              # Herald generated summary
    SYSTEM_EVENT = "system_event"                      # Generic system event
    USER_MESSAGE = "user_message"                      # User sent a message
    RESPONSE_MESSAGE = "response_message"              # AELVO responded


# Icons for each event type — displayed in the feed
EVENT_ICONS = {
    CollaborationEventType.TASK_ASSIGNED: "\u2192",       # →
    CollaborationEventType.FINDING_PUBLISHED: "\u25C6",   # ◆
    CollaborationEventType.EVIDENCE_CONSUMED: "\u25B7",   # ▷
    CollaborationEventType.CHALLENGE_RAISED: "\u26A0",    # ⚠
    CollaborationEventType.CONSENSUS_OUTCOME: "\u21BB",   # ↻
    CollaborationEventType.DECISION_APPROVED: "\u2713",   # ✓
    CollaborationEventType.EXECUTION_ACTION: "\u25B6",    # ▶
    CollaborationEventType.REPORT_GENERATED: "\u2605",    # ★
    CollaborationEventType.SYSTEM_EVENT: "\u2500",         # ─
    CollaborationEventType.USER_MESSAGE: "\u25C9",        # ◉
    CollaborationEventType.RESPONSE_MESSAGE: "\u25CB",    # ○
}

# Colors for each event type
EVENT_COLORS = {
    CollaborationEventType.TASK_ASSIGNED: "#a565ff",         # purple (Architect)
    CollaborationEventType.FINDING_PUBLISHED: "#8c5cff",     # purple (Oracle)
    CollaborationEventType.EVIDENCE_CONSUMED: "#00d889",     # green (Forge)
    CollaborationEventType.CHALLENGE_RAISED: "#ff5c7a",      # red (Sentinel)
    CollaborationEventType.CONSENSUS_OUTCOME: "#f7b731",     # amber (Consensus)
    CollaborationEventType.DECISION_APPROVED: "#19f5a5",     # teal (Approval)
    CollaborationEventType.EXECUTION_ACTION: "#f7b731",      # amber (Terminus)
    CollaborationEventType.REPORT_GENERATED: "#39c8ff",      # cyan (Herald)
    CollaborationEventType.SYSTEM_EVENT: "#52627f",           # muted
    CollaborationEventType.USER_MESSAGE: "#1f8fff",          # blue (user)
    CollaborationEventType.RESPONSE_MESSAGE: "#8c5cff",      # purple (AELVO)
}

# Specialist colors for agent name tags
SPECIALIST_COLORS = {
    "HERMES": "#1f8fff",
    "ARCHITECT": "#a565ff",
    "ORACLE": "#8c5cff",
    "FORGE": "#00d889",
    "SENTINEL": "#ff5c7a",
    "TERMINUS": "#f7b731",
    "HERALD": "#39c8ff",
    "CONSENSUS": "#19f5a5",
    "YOU": "#1f8fff",
    "AELVO": "#8c5cff",
}


@dataclass
class CollaborationEvent:
    """A single structured collaboration event for the TUI feed.

    Every instance maps to a real runtime event.
    """

    event_type: CollaborationEventType
    specialist: str = ""                          # Which agent (ORACLE, ARCHITECT, etc.)
    summary: str = ""                             # Brief description of what happened
    details: str = ""                             # Additional info (task name, file, confidence, etc.)
    confidence: float = 0.0                       # Confidence score if applicable
    metadata: Dict[str, Any] = field(default_factory=dict)  # Extra structured data
    timestamp: float = field(default_factory=time.time)
    trust: Optional[TrustIndicator] = None        # Phase 6: Trust metadata for finding display

    @classmethod
    def _with_trust(cls, event: CollaborationEvent, **trust_kwargs) -> CollaborationEvent:
        """Attach trust metadata to a CollaborationEvent."""
        if any(trust_kwargs.values()):
            event.trust = TrustIndicator(**{k: v for k, v in trust_kwargs.items() if v is not None})
        return event

    @classmethod
    def task_assigned(cls, specialist: str, task_name: str, assigned_by: str = "ARCHITECT") -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.TASK_ASSIGNED,
            specialist=assigned_by,
            summary=f"assigned {specialist}",
            details=task_name,
            metadata={"task": task_name, "assignee": specialist},
        )

    @classmethod
    def finding_published(
        cls,
        specialist: str,
        summary: str,
        confidence: float = 0.0,
        source: str = "",
        verification_status: str = "pending",
        challenged: bool = False,
        challenge_count: int = 0,
        affected_files: Optional[List[str]] = None,
        evidence_type: str = "finding",
        lifecycle_status: str = "created",
        timestamp: Optional[float] = None,
    ) -> CollaborationEvent:
        event = cls(
            event_type=CollaborationEventType.FINDING_PUBLISHED,
            specialist=specialist,
            summary=summary,
            confidence=confidence,
            metadata={
                "confidence": confidence,
                "source": source,
                "evidence_type": evidence_type,
                "challenged": challenged,
                "challenge_count": challenge_count,
            },
        )
        # Attach trust metadata
        return cls._with_trust(
            event,
            source=source,
            confidence=confidence,
            verification_status=verification_status,
            owner=specialist,
            timestamp=timestamp or time.time(),
            evidence_type=evidence_type,
            challenged=challenged,
            challenge_count=challenge_count,
            affected_files=affected_files or [],
            lifecycle_status=lifecycle_status,
        )

    @classmethod
    def evidence_consumed(cls, consumer: str, owner: str, entry_type: str = "finding") -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.EVIDENCE_CONSUMED,
            specialist=consumer,
            summary=f"consumed {entry_type} from {owner}",
            metadata={"owner": owner, "entry_type": entry_type},
        )

    @classmethod
    def challenge_raised(cls, specialist: str, reason: str) -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.CHALLENGE_RAISED,
            specialist=specialist,
            summary=reason,
        )

    @classmethod
    def consensus_outcome(cls, topic: str, outcome: str, confidence: float = 0.0, participants: list = None) -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.CONSENSUS_OUTCOME,
            specialist="CONSENSUS",
            summary=outcome,
            details=topic,
            confidence=confidence,
            metadata={"participants": participants or []},
        )

    @classmethod
    def decision_approved(cls, specialist: str, outcome: str, reason: str = "") -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.DECISION_APPROVED,
            specialist=specialist,
            summary=f"{outcome}",
            details=reason,
            metadata={"outcome": outcome},
        )

    @classmethod
    def execution_action(cls, specialist: str, command: str, status: str = "running") -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.EXECUTION_ACTION,
            specialist=specialist,
            summary=command,
            metadata={"status": status},
        )

    @classmethod
    def report_generated(cls, specialist: str, title: str, evidence_count: int = 0, challenge_count: int = 0) -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.REPORT_GENERATED,
            specialist=specialist,
            summary=title,
            metadata={"evidence_count": evidence_count, "challenge_count": challenge_count},
        )

    @classmethod
    def system(cls, message: str) -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.SYSTEM_EVENT,
            specialist="system",
            summary=message,
        )

    @classmethod
    def user_message(cls, content: str) -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.USER_MESSAGE,
            specialist="YOU",
            summary=content,
        )

    @classmethod
    def response_message(cls, content: str) -> CollaborationEvent:
        return cls(
            event_type=CollaborationEventType.RESPONSE_MESSAGE,
            specialist="AELVO",
            summary=content,
        )
