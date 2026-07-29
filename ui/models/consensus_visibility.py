"""
consensus_visibility.py — Consensus Visibility Model

Tracks the full consensus lifecycle for the TUI:
- Active consensus requests with all positions (who voted FOR/AGAINST/NEUTRAL)
- Vote breakdown with confidence per specialist
- Dissenting positions and conditions
- Challenge activity linked to consensus
- Decision history and architect outcomes
- Consensus timeline (proposed → positions submitted → resolved → architect reviewed)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Position labels ─────────────────────────────────────────────

POSITION_LABELS: Dict[str, str] = {
    "FOR": "FOR",
    "AGAINST": "AGAINST",
    "NEUTRAL": "NEUTRAL",
    "abstain": "ABSTAIN",
    "approve": "FOR",
    "reject": "AGAINST",
    "yes": "FOR",
    "no": "AGAINST",
}

POSITION_COLORS: Dict[str, str] = {
    "FOR": "#00e38c",
    "AGAINST": "#ff5c7a",
    "NEUTRAL": "#f7b731",
    "ABSTAIN": "#52627f",
}

OUTCOME_LABELS: Dict[str, str] = {
    "APPROVED": "APPROVED",
    "APPROVED_WITH_RISK": "APPROVED W/ RISK",
    "REQUIRES_REVISION": "NEEDS REVISION",
    "REJECTED": "REJECTED",
    "ESCALATED": "ESCALATED",
}

OUTCOME_COLORS: Dict[str, str] = {
    "APPROVED": "#00e38c",
    "APPROVED_WITH_RISK": "#f7b731",
    "REQUIRES_REVISION": "#f7b731",
    "REJECTED": "#ff5c7a",
    "ESCALATED": "#ff5c7a",
}

DECISION_COLORS: Dict[str, str] = {
    "approve": "#00e38c",
    "reject": "#ff5c7a",
    "escalate": "#ff5c7a",
    "replan": "#f7b731",
    "override": "#a565ff",
}

STRATEGY_LABELS: Dict[str, str] = {
    "MAJORITY": "majority",
    "SUPERMAJORITY": "supermajority",
    "UNANIMOUS": "unanimous",
    "WEIGHTED": "weighted",
    "ARCHITECT_DECIDES": "architect decides",
}


@dataclass
class ConsensusPosition:
    """A single specialist's position in a consensus vote."""

    specialist: str
    position: str  # FOR, AGAINST, NEUTRAL, ABSTAIN
    confidence: float = 0.5
    evidence: List[str] = field(default_factory=list)
    conditions: List[str] = field(default_factory=list)
    submitted_at: float = 0.0

    @property
    def position_label(self) -> str:
        return POSITION_LABELS.get(self.position, self.position.upper())

    @property
    def position_color(self) -> str:
        return POSITION_COLORS.get(self.position.upper(), "#52627f")

    @property
    def confidence_pct(self) -> int:
        return int(self.confidence * 100)


@dataclass
class ConsensusTopic:
    """A single consensus topic with full lifecycle state."""

    consensus_id: str
    topic: str
    status: str = "proposed"  # proposed → gathering → resolved → reviewed
    strategy: str = "MAJORITY"
    positions: List[ConsensusPosition] = field(default_factory=list)
    outcome: str = ""
    outcome_confidence: float = 0.0
    conditions: List[str] = field(default_factory=list)
    is_timeout: bool = False
    timeout_participants: List[str] = field(default_factory=list)
    dissenting_positions: List[ConsensusPosition] = field(default_factory=list)

    # Architect decision
    decision: str = ""        # approve, reject, escalate, replan, override
    decision_reason: str = ""
    decision_by: str = ""
    decision_conditions: List[str] = field(default_factory=list)

    # Challenge links
    challenge_ids: List[str] = field(default_factory=list)

    # Timing
    proposed_at: float = 0.0
    resolved_at: Optional[float] = None
    decided_at: Optional[float] = None

    @property
    def outcome_label(self) -> str:
        return OUTCOME_LABELS.get(self.outcome, self.outcome)

    @property
    def outcome_color(self) -> str:
        return OUTCOME_COLORS.get(self.outcome, "#52627f")

    @property
    def decision_color(self) -> str:
        return DECISION_COLORS.get(self.decision, "#52627f")

    @property
    def strategy_label(self) -> str:
        return STRATEGY_LABELS.get(self.strategy, self.strategy.lower())

    @property
    def participant_count(self) -> int:
        return len(self.positions)

    @property
    def for_count(self) -> int:
        return sum(1 for p in self.positions if p.position.upper() == "FOR")

    @property
    def against_count(self) -> int:
        return sum(1 for p in self.positions if p.position.upper() == "AGAINST")

    @property
    def neutral_count(self) -> int:
        return sum(1 for p in self.positions if p.position.upper() in ("NEUTRAL", "ABSTAIN"))

    @property
    def age_seconds(self) -> float:
        start = self.proposed_at
        end = self.decided_at or self.resolved_at or time.time()
        return max(0, end - start)

    @property
    def display_age(self) -> str:
        s = self.age_seconds
        if s < 60:
            return f"{int(s)}s"
        m = s / 60
        if m < 60:
            return f"{int(m)}m"
        return f"{int(m/60)}h"


@dataclass
class ChallengeLink:
    """A challenge event related to a consensus topic."""

    challenge_id: str
    challenger: str
    reason: str
    target_entry: str = ""
    status: str = "raised"  # raised → resolved → overridden
    resolved_by: str = ""
    resolved_at: Optional[float] = None
    raised_at: float = 0.0


class ConsensusVisibilityTracker:
    """Tracks the full consensus lifecycle for TUI display.

    Maintains active and resolved consensus topics with:
    - All positions (who voted what, with confidence)
    - Outcomes and confidence
    - Architect decisions
    - Linked challenge events
    """

    def __init__(self) -> None:
        self._active: Dict[str, ConsensusTopic] = {}
        self._resolved: Dict[str, ConsensusTopic] = {}
        self._challenges: Dict[str, ChallengeLink] = {}
        self._max_resolved = 20

    # ── Consensus Events ───────────────────────────────────────

    def on_consensus_started(
        self,
        consensus_id: str,
        topic: str,
        participants: Optional[List[str]] = None,
        strategy: str = "MAJORITY",
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Register a new consensus request."""
        now = time.time()
        self._active[consensus_id] = ConsensusTopic(
            consensus_id=consensus_id,
            topic=topic,
            status="gathering",
            strategy=strategy,
            proposed_at=now,
        )

    def on_consensus_position(
        self,
        consensus_id: str,
        specialist: str,
        position: str,
        confidence: float = 0.5,
        conditions: Optional[List[str]] = None,
    ) -> None:
        """Record a specialist's position on a consensus topic."""
        topic = self._active.get(consensus_id)
        if topic is None:
            return

        pos = ConsensusPosition(
            specialist=specialist,
            position=position,
            confidence=confidence,
            conditions=conditions or [],
            submitted_at=time.time(),
        )

        # Replace existing position from same specialist
        topic.positions = [
            p for p in topic.positions if p.specialist.upper() != specialist.upper()
        ]
        topic.positions.append(pos)

    def on_consensus_outcome(
        self,
        consensus_id: str,
        outcome: str,
        confidence: float = 0.0,
        conditions: Optional[List[str]] = None,
        dissenting: Optional[List[Dict[str, Any]]] = None,
        is_timeout: bool = False,
        timeout_participants: Optional[List[str]] = None,
        strategy: str = "MAJORITY",
    ) -> None:
        """Record a consensus outcome (resolved)."""
        topic = self._active.pop(consensus_id, None)
        if topic is None:
            # Might be a late outcome for a resolved topic
            topic = self._resolved.get(consensus_id)
            if topic is None:
                return

        topic.status = "resolved"
        topic.outcome = outcome
        topic.outcome_confidence = confidence
        topic.resolved_at = time.time()
        topic.is_timeout = is_timeout
        topic.timeout_participants = timeout_participants or []

        # Build dissenting positions from the data
        if dissenting:
            for d in dissenting:
                topic.dissenting_positions.append(ConsensusPosition(
                    specialist=d.get("specialist", ""),
                    position=d.get("position", "AGAINST"),
                    confidence=d.get("confidence", 0.0),
                    conditions=d.get("conditions", []),
                ))

        if conditions:
            topic.conditions = list(set(topic.conditions + conditions))

        # Move to resolved
        self._resolved[consensus_id] = topic
        self._prune_resolved()

    def on_consensus_decision(
        self,
        target_id: str,
        decision: str,
        reason: str = "",
        decided_by: str = "ARCHITECT",
        conditions: Optional[List[str]] = None,
    ) -> None:
        """Record an architect decision related to a consensus topic."""
        # Try to match by consensus_id or target_id
        topic = self._resolved.get(target_id) or self._active.get(target_id)
        if topic is None:
            # Try a fuzzy match on topic ID prefix
            for t in list(self._resolved.values()) + list(self._active.values()):
                if t.consensus_id.startswith(target_id[:8]):
                    topic = t
                    break
            if topic is None:
                return

        topic.status = "reviewed"
        topic.decision = decision
        topic.decision_reason = reason
        topic.decision_by = decided_by
        topic.decided_at = time.time()
        if conditions:
            topic.decision_conditions = conditions

        # If still active, move to resolved
        if target_id in self._active:
            self._resolved[target_id] = self._active.pop(target_id)
            self._prune_resolved()

    # ── Challenge Events ───────────────────────────────────────

    def on_challenge_raised(
        self,
        challenge_id: str,
        challenger: str,
        reason: str,
        entry_id: str = "",
        consensus_id: str = "",
    ) -> None:
        """Record a challenge event."""
        self._challenges[challenge_id] = ChallengeLink(
            challenge_id=challenge_id,
            challenger=challenger,
            reason=reason,
            target_entry=entry_id,
            raised_at=time.time(),
        )

        # Link to a consensus topic if specified
        if consensus_id:
            topic = self._active.get(consensus_id) or self._resolved.get(consensus_id)
            if topic:
                if challenge_id not in topic.challenge_ids:
                    topic.challenge_ids.append(challenge_id)

    def on_challenge_resolved(
        self,
        challenge_id: str,
        resolved_by: str = "",
        status: str = "resolved",
    ) -> None:
        """Mark a challenge as resolved."""
        challenge = self._challenges.get(challenge_id)
        if challenge:
            challenge.status = status
            challenge.resolved_by = resolved_by
            challenge.resolved_at = time.time()

    # ── Accessors ──────────────────────────────────────────────

    def get_active(self) -> List[ConsensusTopic]:
        """Get active consensus topics, newest first."""
        return sorted(
            self._active.values(),
            key=lambda t: t.proposed_at,
            reverse=True,
        )

    def get_resolved(self, limit: int = 10) -> List[ConsensusTopic]:
        """Get resolved consensus topics, newest first."""
        sorted_resolved = sorted(
            self._resolved.values(),
            key=lambda t: t.resolved_at or t.proposed_at,
            reverse=True,
        )
        return sorted_resolved[:limit]

    def get_challenges(self, limit: int = 20) -> List[ChallengeLink]:
        """Get recent challenges, newest first."""
        sorted_ch = sorted(
            self._challenges.values(),
            key=lambda c: c.raised_at,
            reverse=True,
        )
        return sorted_ch[:limit]

    def get_challenges_for_topic(self, consensus_id: str) -> List[ChallengeLink]:
        """Get challenges linked to a specific consensus topic."""
        topic = self._active.get(consensus_id) or self._resolved.get(consensus_id)
        if not topic:
            return []
        return [
            self._challenges[cid] for cid in topic.challenge_ids
            if cid in self._challenges
        ]

    def snapshot(self) -> Dict[str, Any]:
        """Get a complete snapshot for TUI display."""
        return {
            "active": [self._topic_to_dict(t) for t in self.get_active()],
            "resolved": [self._topic_to_dict(t) for t in self.get_resolved(limit=10)],
            "challenges": [self._challenge_to_dict(c) for c in self.get_challenges()],
            "active_count": len(self._active),
            "resolved_count": len(self._resolved),
        }

    def _prune_resolved(self) -> None:
        """Keep only the most recent resolved topics."""
        if len(self._resolved) > self._max_resolved:
            sorted_items = sorted(
                self._resolved.items(),
                key=lambda x: x[1].resolved_at or x[1].proposed_at,
                reverse=True,
            )
            self._resolved = dict(sorted_items[:self._max_resolved])

    # ── Serialization ──────────────────────────────────────────

    @staticmethod
    def _topic_to_dict(topic: ConsensusTopic) -> Dict[str, Any]:
        return {
            "consensus_id": topic.consensus_id,
            "topic": topic.topic,
            "status": topic.status,
            "strategy": topic.strategy,
            "strategy_label": topic.strategy_label,
            "positions": [
                {
                    "specialist": p.specialist,
                    "position": p.position_label,
                    "position_color": p.position_color,
                    "confidence": p.confidence,
                    "confidence_pct": p.confidence_pct,
                    "conditions": p.conditions,
                }
                for p in topic.positions
            ],
            "outcome": topic.outcome,
            "outcome_label": topic.outcome_label,
            "outcome_color": topic.outcome_color,
            "outcome_confidence": topic.outcome_confidence,
            "conditions": topic.conditions,
            "is_timeout": topic.is_timeout,
            "timeout_participants": topic.timeout_participants,
            "decision": topic.decision,
            "decision_color": topic.decision_color,
            "decision_reason": topic.decision_reason,
            "decision_by": topic.decision_by,
            "decision_conditions": topic.decision_conditions,
            "for_count": topic.for_count,
            "against_count": topic.against_count,
            "neutral_count": topic.neutral_count,
            "challenge_ids": topic.challenge_ids,
            "proposed_at": topic.proposed_at,
            "resolved_at": topic.resolved_at,
            "decided_at": topic.decided_at,
            "display_age": topic.display_age,
        }

    @staticmethod
    def _challenge_to_dict(ch: ChallengeLink) -> Dict[str, Any]:
        return {
            "challenge_id": ch.challenge_id,
            "challenger": ch.challenger,
            "reason": ch.reason,
            "status": ch.status,
            "resolved_by": ch.resolved_by,
            "raised_at": ch.raised_at,
        }
