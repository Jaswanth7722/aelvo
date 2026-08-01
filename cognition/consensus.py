from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from enum import Enum

from cognition.types import (
    ConsensusEvent, ConsensusResult, ConflictRecord, ConflictSeverity,
)

log = logging.getLogger("aelvo.cognition.consensus")


class GovernanceDecision(str, Enum):
    APPROVED = "approved"
    DENIED = "denied"
    REQUIRES_REVIEW = "requires_review"


class MultiAgentConsensusSystem:
    """Multi-Agent Consensus System.

    Manages agreement and disagreement between specialists (or plan
    branches). Supports voting, escalation, governance veto (SENTINEL),
    and conflict resolution.
    """

    def __init__(self, governance_kernel=None):
        self._events: Dict[str, ConsensusEvent] = {}
        self._resolved_conflicts: List[ConflictRecord] = []
        self._governance = governance_kernel

    def propose_consensus(
        self,
        topic: str,
        participants: List[str],
        initial_votes: Optional[Dict[str, str]] = None,
    ) -> ConsensusEvent:
        event_id = self._generate_id("consensus", topic)
        event = ConsensusEvent(
            id=event_id,
            topic=topic,
            participants=participants,
            votes=initial_votes or {},
        )
        self._events[event_id] = event
        log.info("Proposed consensus '%s' with %d participants", topic, len(participants))
        return event

    def vote(self, event_id: str, participant: str, vote: str) -> bool:
        event = self._events.get(event_id)
        if event is None:
            return False
        if participant not in event.participants:
            return False
        event.votes[participant] = vote
        log.debug("%s voted '%s' on '%s'", participant, vote, event_id)
        return self._check_consensus(event)

    def get_event(self, event_id: str) -> Optional[ConsensusEvent]:
        return self._events.get(event_id)

    def get_pending_events(self) -> List[ConsensusEvent]:
        return [e for e in self._events.values() if e.result == ConsensusResult.NOT_ATTEMPTED]

    def resolve_conflict(self, conflict: ConflictRecord) -> Optional[ConsensusEvent]:
        participants = conflict.involved_specialists or ["SENTINEL", "FORGE", "HERMES"]
        event = self.propose_consensus(
            topic=f"resolve_conflict:{conflict.id}",
            participants=participants,
        )
        event.result = ConsensusResult.PARTIAL
        event.confidence = self._compute_conflict_confidence(conflict)
        resolved = ConflictRecord(
            id=conflict.id,
            description=conflict.description,
            severity=conflict.severity,
            involved_entries=conflict.involved_entries,
            involved_specialists=conflict.involved_specialists,
            resolution_strategy=f"Auto-resolved via consensus {event.id}",
            resolved=True,
            resolved_at=datetime.now(timezone.utc),
            resolution_notes=f"Confidence: {event.confidence:.2f}",
        )
        self._resolved_conflicts.append(resolved)
        self._events[event.id] = event
        return event

    def apply_governance(self, event_id: str) -> GovernanceDecision:
        event = self._events.get(event_id)
        if event is None:
            return GovernanceDecision.REQUIRES_REVIEW
        if self._governance is None:
            event.governance_applied = True
            return GovernanceDecision.APPROVED
        try:
            veto_check = self._check_veto(event)
            if veto_check["veto"]:
                event.vetoed = True
                event.veto_reason = veto_check["reason"]
                log.warning("Governance veto on %s: %s", event_id, veto_check["reason"])
                return GovernanceDecision.DENIED
            event.governance_applied = True
            return GovernanceDecision.APPROVED
        except Exception as e:
            log.warning("Governance apply failed: %s", e)
            event.governance_applied = True
            return GovernanceDecision.APPROVED

    def get_resolved_conflicts(self) -> List[ConflictRecord]:
        return self._resolved_conflicts

    def summary(self) -> Dict[str, Any]:
        total = len(self._events)
        agreed = len([e for e in self._events.values() if e.result == ConsensusResult.AGREED])
        disagreed = len([e for e in self._events.values() if e.result == ConsensusResult.DISAGREED])
        pending = len(self.get_pending_events())
        return {
            "total_events": total,
            "agreed": agreed,
            "disagreed": disagreed,
            "pending": pending,
            "resolved_conflicts": len(self._resolved_conflicts),
            "vetoed": len([e for e in self._events.values() if e.vetoed]),
        }

    def snapshot(self) -> Dict[str, Any]:
        return self.summary()

    def _check_consensus(self, event: ConsensusEvent) -> bool:
        if len(event.votes) < len(event.participants):
            return False
        vote_values = list(event.votes.values())
        unique_votes = set(vote_values)
        if len(unique_votes) == 1:
            event.result = ConsensusResult.AGREED
            event.confidence = 1.0
            log.info("Consensus reached on %s: %s", event.id, unique_votes.pop())
            return True
        votes_for = max(unique_votes, key=lambda v: vote_values.count(v))
        count_for = vote_values.count(votes_for)
        total = len(vote_values)
        if count_for / total >= 0.6:
            event.result = ConsensusResult.PARTIAL
            event.confidence = round(count_for / total, 4)
            log.info("Partial consensus on %s: %s (%.0f%%)", event.id, votes_for, 100 * event.confidence)
            return True
        event.result = ConsensusResult.DISAGREED
        event.confidence = round(count_for / total, 4)
        log.info("No consensus on %s â€” %d unique positions", event.id, len(unique_votes))
        return False

    def _compute_conflict_confidence(self, conflict: ConflictRecord) -> float:
        base = {
            ConflictSeverity.LOW: 0.8,
            ConflictSeverity.MEDIUM: 0.6,
            ConflictSeverity.HIGH: 0.4,
            ConflictSeverity.CRITICAL: 0.2,
        }
        return base.get(conflict.severity, 0.5)

    def _check_veto(self, event: ConsensusEvent) -> Dict[str, Any]:
        try:
            for participant in event.participants:
                if "SENTINEL" in participant.upper():
                    if event.topic.startswith("resolve_conflict"):
                        for vote in event.votes.values():
                            if "security" in vote.lower() or "vulnerability" in vote.lower():
                                return {
                                    "veto": True,
                                    "reason": "Security concern detected in consensus vote",
                                }
        except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        if self._governance:
            try:
                state = None
                if hasattr(self._governance, 'session_state'):
                    state = self._governance.session_state
                if state and state.get("failures", 0) >= 3:
                    return {"veto": True, "reason": "Session failure threshold exceeded"}
            except Exception as _ex: log.warning("Silenced exception: %s", _ex)
        return {"veto": False, "reason": ""}

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
