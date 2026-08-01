# cognition/consensus_extended.py — Extended Consensus Engine
#
# Extends the existing MultiAgentConsensusSystem with:
# - Typed ConsensusRequest with topic, participants, deadline, resolution_strategy
# - 5 resolution strategies: MAJORITY, SUPERMAJORITY, UNANIMOUS, WEIGHTED, ARCHITECT_DECIDES
# - 5 outcome types: APPROVED, APPROVED_WITH_RISK, REQUIRES_REVISION, REJECTED, ESCALATED
# - Position submission with evidence, confidence, dissenting conditions
# - Consensus observability via EventBus and TUI

from __future__ import annotations

import hashlib
import json
import logging
import asyncio
from enum import Enum
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field

from cognition.consensus import MultiAgentConsensusSystem

log = logging.getLogger("aelvo.cognition.consensus_extended")

# Try EventBus imports for TUI visibility
try:
    from ui.events.event_bus import EventBus, Event, EventType as UIEventType
    HAS_UI_EVENTS = True
except ImportError:
    HAS_UI_EVENTS = False

# Try ChromaDB import
_HAS_CHROMADB = False
try:
    import chromadb
    _HAS_CHROMADB = True
except ImportError as _ex:
    log.warning("Silenced exception: %s", _ex)


# ============================================================================
# Resolution Strategies
# ============================================================================

class ResolutionStrategy(str, Enum):
    MAJORITY = "MAJORITY"                  # More than half must agree
    SUPERMAJORITY = "SUPERMAJORITY"        # Two-thirds must agree
    UNANIMOUS = "UNANIMOUS"                # All must agree
    WEIGHTED = "WEIGHTED"                  # Votes weighted by specialist confidence scores
    ARCHITECT_DECIDES = "ARCHITECT_DECIDES" # Consensus input informs but Architect makes the final call


# ============================================================================
# Consensus Outcome Types
# ============================================================================

class ConsensusOutcomeType(str, Enum):
    APPROVED = "APPROVED"
    APPROVED_WITH_RISK = "APPROVED_WITH_RISK"
    REQUIRES_REVISION = "REQUIRES_REVISION"
    REJECTED = "REJECTED"
    ESCALATED = "ESCALATED"


# ============================================================================
# Consensus Position — submitted by each participating specialist
# ============================================================================

class ConsensusPosition(BaseModel):
    """A specialist's position on a consensus topic."""
    specialist: str
    position: str                            # e.g., "FOR", "AGAINST", "NEUTRAL"
    evidence: List[Dict[str, Any]] = Field(default_factory=list)
    confidence: float = 0.5
    conditions: List[str] = Field(default_factory=list)  # Dissenting conditions
    submitted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ============================================================================
# Extended Consensus Request
# ============================================================================

class ConsensusRequest(BaseModel):
    """A typed request for consensus on a decision."""
    consensus_id: str
    topic: str
    options: List[str] = Field(default_factory=list)
    criteria: List[str] = Field(default_factory=list)
    participants: List[str] = Field(default_factory=list)
    deadline: Optional[datetime] = None
    resolution_strategy: ResolutionStrategy = ResolutionStrategy.MAJORITY
    context: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ============================================================================
# Extended Consensus Outcome
# ============================================================================

class ConsensusOutcome(BaseModel):
    """The complete outcome of a consensus process.

    Per Amendment 3: Consensus is ADVISORY.  The outcome informs the
    Architect, but the Architect makes the final decision.  The
    ``advisory`` flag is always ``True`` for consensus outcomes, and
    ``advisory_note`` explains this role.
    """
    consensus_id: str
    topic: str
    outcome: ConsensusOutcomeType
    confidence: float = 0.0
    positions: List[ConsensusPosition] = Field(default_factory=list)
    dissenting_positions: List[ConsensusPosition] = Field(default_factory=list)
    conditions: List[str] = Field(default_factory=list)
    resolution_strategy: ResolutionStrategy = ResolutionStrategy.MAJORITY
    is_timeout: bool = False
    timeout_participants: List[str] = Field(default_factory=list)
    final_decision: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: Optional[datetime] = None

    # ── Advisory (Amendment 3) ────────────────────────────────────
    advisory: bool = Field(
        default=True,
        description="Always True — consensus is advisory, not authoritative",
    )
    advisory_note: str = Field(
        default="This consensus outcome is ADVISORY. The Architect has final authority to APPROVE, REJECT, ESCALATE, REPLAN, or OVERRIDE.",
        description="Note explaining the advisory nature of consensus",
    )
    architect_reviewed: bool = Field(
        default=False,
        description="Whether the Architect has reviewed this outcome",
    )
    architect_decision_outcome: Optional[str] = Field(
        default=None,
        description="The Architect's final decision (approve, reject, escalate, replan, override)",
    )

    def recommendation(self) -> str:
        """Return the advisory recommendation text for the Architect."""
        return (
            f"Consensus outcome: {self.outcome.value} "
            f"(confidence={self.confidence:.2f}, strategy={self.resolution_strategy.value}). "
            f"Participants: {len(self.positions)}. "
            f"Dissenting: {len(self.dissenting_positions)}. "
            f"Conditions: {len(self.conditions)}. "
            f"This is ADVISORY. Architect has final authority."
        )

    def to_advisory_entry_content(self) -> str:
        """Serialize to a JSON string suitable for publication as a
        ``ConsensusEntry`` on the blackboard."""
        return json.dumps({
            "consensus_id": self.consensus_id,
            "topic": self.topic,
            "outcome": self.outcome.value,
            "positions": {
                p.specialist: p.position for p in self.positions
            },
            "confidence": self.confidence,
            "recommendation": self.recommendation(),
            "participants": [p.specialist for p in self.positions],
            "advisory": self.advisory,
            "conditions": self.conditions,
        })


# ============================================================================
# Extended Consensus Engine
# ============================================================================

class ExtendedConsensusEngine:
    """Extended consensus engine with resolution strategies and rich outcomes.

    Integrates with the existing MultiAgentConsensusSystem for governance
    and veto checks, while adding the full consensus process lifecycle.
    """

    def __init__(
        self,
        base_system: Optional[MultiAgentConsensusSystem] = None,
        specialist_confidence_scores: Optional[Dict[str, float]] = None,
    ):
        self._base = base_system or MultiAgentConsensusSystem()
        self._specialist_scores: Dict[str, float] = specialist_confidence_scores or {}
        self._active_requests: Dict[str, ConsensusRequest] = {}
        self._positions: Dict[str, List[ConsensusPosition]] = {}
        self._outcomes: Dict[str, ConsensusOutcome] = {}
        self._timer_tasks: Dict[str, asyncio.Task] = {}
        self._ui_event_bus: Optional[EventBus] = None

    # ======================================================================
    # Public API
    # ======================================================================

    def set_ui_event_bus(self, bus: EventBus) -> None:
        """Connect the UI event bus for consensus visibility."""
        self._ui_event_bus = bus

    def request_consensus(
        self,
        topic: str,
        participants: List[str],
        context: Optional[Dict[str, Any]] = None,
        deadline_seconds: int = 120,
        resolution_strategy: ResolutionStrategy = ResolutionStrategy.MAJORITY,
        options: Optional[List[str]] = None,
        criteria: Optional[List[str]] = None,
    ) -> ConsensusRequest:
        """Initiate a new consensus request.

        Args:
            topic: The decision to be made
            participants: Which specialists must participate
            context: Relevant context for the decision
            deadline_seconds: Maximum time to wait for all participants
            resolution_strategy: How to resolve (MAJORITY, SUPERMAJORITY, etc.)
            options: Options under consideration
            criteria: Criteria for evaluation

        Returns:
            The ConsensusRequest object
        """
        consensus_id = self._generate_id("consensus", topic)
        deadline = datetime.now(timezone.utc) + timedelta(seconds=deadline_seconds)

        request = ConsensusRequest(
            consensus_id=consensus_id,
            topic=topic,
            options=options or [],
            criteria=criteria or [],
            participants=participants,
            deadline=deadline,
            resolution_strategy=resolution_strategy,
            context=context or {},
        )

        self._active_requests[consensus_id] = request
        self._positions[consensus_id] = []

        # Propose to base system for governance integration
        self._base.propose_consensus(topic, participants)

        # Start deadline timer
        self._start_deadline_timer(consensus_id, deadline_seconds)

        # Publish event with full position-level data
        self._safe_publish_event("CONSENSUS_STARTED", {
            "consensus_id": consensus_id,
            "topic": topic,
            "summary": topic[:100],
            "participants": participants,
            "positions": [],  # Empty — no positions submitted yet
            "deadline": deadline.isoformat(),
            "strategy": resolution_strategy.value,
            "event_name": "CONSENSUS_STARTED",
        })

        log.info(
            "Consensus requested: %s (topic=%s, participants=%d, strategy=%s)",
            consensus_id[:8], topic[:40], len(participants), resolution_strategy.value,
        )
        return request

    def submit_position(
        self,
        consensus_id: str,
        specialist: str,
        position: str,
        evidence: Optional[List[Dict[str, Any]]] = None,
        confidence: float = 0.5,
        conditions: Optional[List[str]] = None,
    ) -> Optional[ConsensusOutcome]:
        """Submit a specialist's position on a consensus topic.

        When all participants have submitted, automatically resolves
        the consensus and returns the outcome.

        Args:
            consensus_id: The consensus request ID
            specialist: Which specialist is submitting
            position: Their position (e.g., "FOR", "AGAINST", "NEUTRAL")
            evidence: Supporting evidence
            confidence: Confidence in this position (0.0-1.0)
            conditions: Dissenting conditions or caveats

        Returns:
            ConsensusOutcome if consensus is reached, None if still waiting
        """
        request = self._active_requests.get(consensus_id)
        if request is None:
            log.warning("No active consensus found: %s", consensus_id[:8])
            return None

        pos = ConsensusPosition(
            specialist=specialist,
            position=position,
            evidence=evidence or [],
            confidence=confidence,
            conditions=conditions or [],
        )
        self._positions[consensus_id].append(pos)

        # Vote in the base system for governance
        self._base.vote(consensus_id, specialist, position)

        # Publish position event with full position-level data
        self._safe_publish_event("CONSENSUS_POSITION_SUBMITTED", {
            "consensus_id": consensus_id,
            "specialist": specialist,
            "position": position,
            "position_summary": position[:60],
            "confidence": confidence,
            "conditions": conditions or [],
            "event_name": "CONSENSUS_POSITION_SUBMITTED",
        })

        log.info(
            "Position submitted: %s by %s (position=%s, confidence=%.2f)",
            consensus_id[:8], specialist, position[:30], confidence,
        )

        # Check if all participants have submitted
        submitted_specialists = {p.specialist for p in self._positions[consensus_id]}
        if submitted_specialists.issuperset(set(request.participants)):
            return self._resolve(consensus_id)

        # Also check for timeout by participants who may never respond
        return None

    def get_active_consensus(self) -> List[ConsensusRequest]:
        """Get all active consensus requests."""
        return list(self._active_requests.values())

    def get_positions(self, consensus_id: str) -> List[ConsensusPosition]:
        """Get all positions submitted for a consensus."""
        return self._positions.get(consensus_id, [])

    def get_outcome(self, consensus_id: str) -> Optional[ConsensusOutcome]:
        """Get the outcome of a resolved consensus."""
        return self._outcomes.get(consensus_id)

    # ======================================================================
    # Resolution
    # ======================================================================

    def _resolve(self, consensus_id: str) -> ConsensusOutcome:
        """Resolve a consensus request and produce an outcome."""
        request = self._active_requests.get(consensus_id)
        if request is None:
            raise ValueError(f"Consensus {consensus_id} not found")

        positions = self._positions.get(consensus_id, [])
        resolved_at = datetime.now(timezone.utc)

        # Cancel deadline timer
        timer = self._timer_tasks.pop(consensus_id, None)
        if timer:
            timer.cancel()

        # Apply resolution strategy
        outcome_type, confidence, final_decision = self._apply_strategy(request, positions)

        # Identify dissenting positions
        dissenting = [
            p for p in positions
            if p.position.upper() != "FOR"
            and not (outcome_type in (ConsensusOutcomeType.APPROVED, ConsensusOutcomeType.APPROVED_WITH_RISK) and p.position.upper() == "FOR")
        ]

        # Collect conditions
        all_conditions: List[str] = []
        for p in positions:
            all_conditions.extend(p.conditions)

        outcome = ConsensusOutcome(
            consensus_id=consensus_id,
            topic=request.topic,
            outcome=outcome_type,
            confidence=confidence,
            positions=positions,
            dissenting_positions=dissenting,
            conditions=list(set(all_conditions)),
            resolution_strategy=request.resolution_strategy,
            final_decision=final_decision,
            created_at=request.created_at,
            resolved_at=resolved_at,
        )

        self._outcomes[consensus_id] = outcome
        self._active_requests.pop(consensus_id, None)

        # Publish outcome event with full position-level data
        outcome_data = {
            "consensus_id": consensus_id,
            "topic": request.topic,
            "outcome": outcome_type.value,
            "confidence": confidence,
            "participants": [p.specialist for p in positions],
            "positions": [
                {
                    "specialist": p.specialist,
                    "position": p.position,
                    "confidence": p.confidence,
                    "conditions": p.conditions,
                }
                for p in positions
            ],
            "dissenting": [
                {
                    "specialist": p.specialist,
                    "position": p.position,
                    "confidence": p.confidence,
                    "conditions": p.conditions,
                }
                for p in dissenting
            ],
            "conditions": list(set(all_conditions)),
            "strategy": request.resolution_strategy.value,
            "is_timeout": outcome.is_timeout if hasattr(outcome, 'is_timeout') else False,
            "timeout_participants": outcome.timeout_participants if hasattr(outcome, 'timeout_participants') else [],
            "event_name": "CONSENSUS_COMPLETED",
        }
        self._safe_publish_event("CONSENSUS_COMPLETED", outcome_data)

        log.info(
            "Consensus resolved: %s (outcome=%s, confidence=%.2f, strategy=%s)",
            consensus_id[:8], outcome_type.value, confidence, request.resolution_strategy.value,
        )
        return outcome

    def _apply_strategy(
        self,
        request: ConsensusRequest,
        positions: List[ConsensusPosition],
    ) -> Tuple[ConsensusOutcomeType, float, Optional[str]]:
        """Apply the resolution strategy to determine the outcome."""
        total = len(positions)
        if total == 0:
            return ConsensusOutcomeType.ESCALATED, 0.0, "No positions submitted"

        # Count FOR and AGAINST
        for_count = sum(1 for p in positions if p.position.upper() == "FOR")
        against_count = sum(1 for p in positions if p.position.upper() == "AGAINST")
        neutral_count = total - for_count - against_count

        # Weighted confidence
        sum(p.confidence for p in positions if p.position.upper() == "FOR")
        total_confidence = sum(p.confidence for p in positions)
        avg_confidence = round(total_confidence / total, 4) if total > 0 else 0.0

        # Check for critical dissenting conditions
        has_critical_conditions = any(
            any("security" in c.lower() or "vulnerability" in c.lower() for c in p.conditions)
            for p in positions
        )

        if request.resolution_strategy == ResolutionStrategy.ARCHITECT_DECIDES:
            # Architect makes the final call, but we provide the input
            for_input = {
                "for": for_count, "against": against_count, "neutral": neutral_count,
                "avg_confidence": avg_confidence,
            }
            decision = f"Architect decides. Input: {for_input}"
            return ConsensusOutcomeType.APPROVED_WITH_RISK, avg_confidence, decision

        if request.resolution_strategy == ResolutionStrategy.UNANIMOUS:
            if against_count == 0 and for_count == total:
                return ConsensusOutcomeType.APPROVED, avg_confidence, "Unanimous approval"
            else:
                return ConsensusOutcomeType.REJECTED, avg_confidence, "Not unanimous"

        if request.resolution_strategy == ResolutionStrategy.SUPERMAJORITY:
            threshold = total * 2 / 3
            if for_count >= threshold:
                if has_critical_conditions:
                    return ConsensusOutcomeType.APPROVED_WITH_RISK, avg_confidence, "Supermajority with conditions"
                return ConsensusOutcomeType.APPROVED, avg_confidence, "Supermajority approval"
            elif against_count > for_count:
                return ConsensusOutcomeType.REJECTED, avg_confidence, "Rejected — supermajority not met, majority against"
            elif for_count > 0:
                return ConsensusOutcomeType.REQUIRES_REVISION, avg_confidence, "Insufficient support for supermajority"
            else:
                return ConsensusOutcomeType.REJECTED, avg_confidence, "Rejected — no support"

        if request.resolution_strategy == ResolutionStrategy.WEIGHTED:
            weighted_for = sum(p.confidence for p in positions if p.position.upper() == "FOR")
            weighted_against = sum(p.confidence for p in positions if p.position.upper() == "AGAINST")
            weighted_total = weighted_for + weighted_against
            if weighted_total > 0 and weighted_for / weighted_total >= 0.5:
                if has_critical_conditions:
                    return ConsensusOutcomeType.APPROVED_WITH_RISK, round(weighted_for / weighted_total, 4), "Weighted approval with conditions"
                return ConsensusOutcomeType.APPROVED, round(weighted_for / weighted_total, 4), "Weighted majority"
            elif weighted_total > 0:
                return ConsensusOutcomeType.REJECTED, round(weighted_against / weighted_total, 4), "Weighted rejection"
            return ConsensusOutcomeType.ESCALATED, 0.0, "No weighted decision possible"

        # Default: MAJORITY
        valid_votes = for_count + against_count
        if valid_votes > 0 and for_count > valid_votes / 2:
            if has_critical_conditions:
                return ConsensusOutcomeType.APPROVED_WITH_RISK, round(for_count / total, 4), "Majority with conditions"
            return ConsensusOutcomeType.APPROVED, round(for_count / total, 4), "Majority approval"
        elif against_count > for_count:
            return ConsensusOutcomeType.REJECTED, round(against_count / total, 4), "Rejected by majority"
        elif for_count > 0:
            # Some FOR votes but not enough — split decision, revise
            return ConsensusOutcomeType.REQUIRES_REVISION, round(for_count / total, 4), "Insufficient majority, revision needed"
        else:
            return ConsensusOutcomeType.ESCALATED, 0.0, "No valid votes cast"

    # ======================================================================
    # Deadline Timer
    # ======================================================================

    def _start_deadline_timer(self, consensus_id: str, delay_seconds: int) -> None:
        """Start a background task that resolves on timeout.

        Only starts the timer if there is a running event loop.
        In synchronous contexts (e.g., unit tests), the timer is
        skipped and timeout is handled manually.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop — skip timer in sync context
            return

        async def _timer():
            try:
                await asyncio.sleep(delay_seconds)
                # Check if still active
                if consensus_id in self._active_requests:
                    request = self._active_requests[consensus_id]
                    positions = self._positions.get(consensus_id, [])
                    submitted = {p.specialist for p in positions}
                    timeout = [s for s in request.participants if s not in submitted]

                    if timeout:
                        outcome = ConsensusOutcome(
                            consensus_id=consensus_id,
                            topic=request.topic,
                            outcome=ConsensusOutcomeType.ESCALATED,
                            confidence=0.0,
                            positions=positions,
                            dissenting_positions=[],
                            resolution_strategy=request.resolution_strategy,
                            is_timeout=True,
                            timeout_participants=timeout,
                            final_decision=f"Timed out: {', '.join(timeout)} did not respond",
                            created_at=request.created_at,
                            resolved_at=datetime.now(timezone.utc),
                        )
                        self._outcomes[consensus_id] = outcome
                        self._active_requests.pop(consensus_id, None)
                        log.warning("Consensus %s timed out — %s did not respond", consensus_id[:8], timeout)
            except asyncio.CancelledError as _ex:
                log.warning("Silenced exception: %s", _ex)

        if loop.is_running():
            task = asyncio.create_task(_timer())
            self._timer_tasks[consensus_id] = task

    # ======================================================================
    # Participant Selection (Amendment 3)
    # ======================================================================

    @staticmethod
    def select_participants(topic: str) -> List[str]:
        """Automatically select relevant specialists based on topic content.

        Analyzes the topic string for keywords and returns a list of
        specialist names that should participate in the consensus.

        Args:
            topic: The consensus topic / question.

        Returns:
            List of specialist names (e.g., ["FORGE", "SENTINEL", "ORACLE"]).
        """
        lower = topic.lower()
        participants: List[str] = []

        # Always include ARCHITECT as observer
        participants.append("ARCHITECT")

        # Code/implementation keywords -> FORGE
        code_keywords = ["code", "implement", "refactor", "api", "function",
                         "class", "module", "library", "syntax", "type", "test"]
        if any(k in lower for k in code_keywords):
            participants.append("FORGE")

        # Security keywords -> SENTINEL
        security_keywords = ["security", "vulnerability", "secret", "credential",
                            "password", "token", "encrypt", "auth", "permission",
                            "firewall", "audit", "cve", "injection"]
        if any(k in lower for k in security_keywords):
            participants.append("SENTINEL")

        # Research keywords -> ORACLE
        research_keywords = ["research", "investigat", "find", "discover",
                            "learn", "what is", "how does", "best practice",
                            "compared to", "versus"]
        if any(k in lower for k in research_keywords):
            participants.append("ORACLE")

        # Execution/deployment keywords -> TERMINUS
        exec_keywords = ["deploy", "run", "execute", "command", "script",
                         "pipeline", "ci", "cd", "git", "docker", "shell"]
        if any(k in lower for k in exec_keywords):
            participants.append("TERMINUS")

        # Communication/report keywords -> HERALD
        comm_keywords = ["communicat", "report", "summar", "message",
                         "announce", "notify", "inform"]
        if any(k in lower for k in comm_keywords):
            participants.append("HERALD")

        # Architecture/planning keywords -> already ARCHITECT included
        # Default: if no specific specialist matched, include all core ones
        core = {"FORGE", "SENTINEL", "ORACLE", "TERMINUS", "HERALD"}
        if not any(s in participants for s in core):
            participants.extend(["FORGE", "SENTINEL"])

        # Deduplicate while preserving order
        seen: set = set()
        result = []
        for s in participants:
            if s not in seen:
                seen.add(s)
                result.append(s)

        return result

    # ======================================================================
    # Blackboard Publishing (Amendment 3 — advisory role)
    # ======================================================================

    def publish_to_blackboard(
        self,
        outcome: ConsensusOutcome,
        blackboard: Any,
    ) -> Optional[str]:
        """Publish the advisory consensus outcome to the blackboard.

        Publishes a ``ConsensusEntry`` to the ``consensus_outcomes``
        blackboard slot.  The entry is marked as advisory, informing
        the Architect (and all specialists) of the consensus position.

        The Architect reads this slot when making decisions.

        Args:
            outcome: The resolved consensus outcome.
            blackboard: A ``CognitiveBlackboard`` instance.

        Returns:
            The blackboard entry ID, or ``None`` if blackboard is None.
        """
        if blackboard is None:
            return None

        from cognition.types import EntryType, Provenance, ProvenanceType

        content = outcome.to_advisory_entry_content()
        entry = blackboard.publish(
            slot_name="consensus_outcomes",
            content=content,
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.CONSENSUS,
                source_id=f"consensus:{outcome.consensus_id}",
            ),
            confidence=outcome.confidence,
            tags=[
                "consensus",
                "advisory",
                outcome.outcome.value.lower(),
                outcome.resolution_strategy.value.lower(),
            ],
        )
        log.info(
            "Published advisory consensus outcome to blackboard (entry=%s, outcome=%s)",
            entry.id[:8], outcome.outcome.value,
        )
        return entry.id

    # ======================================================================
    # Persistence (SQLite + ChromaDB)
    # ======================================================================

    def persist_to_sqlite(
        self,
        outcome: ConsensusOutcome,
        db_path: str = "",
    ) -> bool:
        """Persist a consensus outcome to SQLite.

        Stores the outcome as a JSON record in a ``consensus_history``
        table.  Creates the table if it does not exist.

        Args:
            outcome: The resolved consensus outcome.
            db_path: Path to the SQLite database file.  If empty, uses
                     the default path ``(cwd)/consensus_history.db``.

        Returns:
            True if persisted successfully, False on error.
        """
        import sqlite3

        path = db_path or "consensus_history.db"
        try:
            conn = sqlite3.connect(path)
            conn.execute(
                """CREATE TABLE IF NOT EXISTS consensus_history (
                    id TEXT PRIMARY KEY,
                    topic TEXT,
                    outcome TEXT,
                    confidence REAL,
                    strategy TEXT,
                    is_timeout INTEGER,
                    positions_json TEXT,
                    created_at TEXT,
                    resolved_at TEXT
                )"""
            )
            conn.execute(
                """INSERT OR REPLACE INTO consensus_history
                   (id, topic, outcome, confidence, strategy, is_timeout,
                    positions_json, created_at, resolved_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    outcome.consensus_id,
                    outcome.topic[:500],
                    outcome.outcome.value,
                    outcome.confidence,
                    outcome.resolution_strategy.value,
                    1 if outcome.is_timeout else 0,
                    json.dumps([p.model_dump(mode='json') for p in outcome.positions]),
                    outcome.created_at.isoformat() if outcome.created_at else "",
                    outcome.resolved_at.isoformat() if outcome.resolved_at else "",
                ),
            )
            conn.commit()
            conn.close()
            log.debug("Persisted consensus %s to SQLite", outcome.consensus_id[:8])
            return True
        except Exception as e:
            log.warning("Failed to persist consensus to SQLite: %s", e)
            return False

    def persist_to_chromadb(
        self,
        outcome: ConsensusOutcome,
        collection=None,
    ) -> bool:
        """Persist a consensus outcome to ChromaDB for vector search.

        The outcome is embedded as a document that can be queried by
        topic, outcome type, or participants.

        Args:
            outcome: The resolved consensus outcome.
            collection: A ChromaDB collection.  If None, tries to create
                        one from the default client.

        Returns:
            True if persisted successfully, False on error or if ChromaDB
            is not available.
        """
        if not _HAS_CHROMADB:
            log.debug("ChromaDB not available — skipping vector persistence")
            return False

        if collection is None:
            try:
                client = chromadb.Client()
                collection = client.get_or_create_collection("consensus_history")
            except Exception as e:
                log.warning("Failed to create ChromaDB collection: %s", e)
                return False

        try:
            doc = (
                f"Consensus: {outcome.topic} | "
                f"Outcome: {outcome.outcome.value} | "
                f"Confidence: {outcome.confidence:.2f} | "
                f"Strategy: {outcome.resolution_strategy.value} | "
                f"Participants: {len(outcome.positions)} | "
                f"Conditions: {len(outcome.conditions)}"
            )
            meta = {
                "type": "consensus_outcome",
                "topic": outcome.topic[:200],
                "outcome": outcome.outcome.value,
                "confidence": outcome.confidence,
                "strategy": outcome.resolution_strategy.value,
                "participant_count": len(outcome.positions),
                "is_advisory": int(outcome.advisory),
            }
            collection.add(
                ids=[outcome.consensus_id],
                documents=[doc],
                metadatas=[meta],
            )
            log.debug("Persisted consensus %s to ChromaDB", outcome.consensus_id[:8])
            return True
        except Exception as e:
            log.warning("Failed to persist consensus to ChromaDB: %s", e)
            return False

    # ======================================================================
    # Event Publishing
    # ======================================================================

    def _safe_publish_event(self, event_name: str, data: Dict[str, Any]) -> None:
        """Safely schedule an event publish, no-op if no event loop."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return  # No running loop — skip in sync context
        try:
            asyncio.ensure_future(self._publish_event_async(event_name, data))
        except Exception as _ex:
            log.warning("Silenced exception: %s", _ex)

    async def _publish_event_async(self, event_name: str, data: Dict[str, Any]) -> None:
        """Publish a consensus event to the UI EventBus.

        Uses the dedicated COLLABORATION_CONSENSUS event type so the
        bridge's _on_collaboration_consensus handler receives it.
        The ``event_name`` field in data differentiates the event subtype
        (CONSENSUS_STARTED, CONSENSUS_POSITION_SUBMITTED, CONSENSUS_COMPLETED).
        """
        if HAS_UI_EVENTS and self._ui_event_bus:
            try:
                # Include the event_name in data for the bridge to route
                data["event_name"] = event_name
                event = Event(
                    event_type=UIEventType.COLLABORATION_CONSENSUS,
                    data=data,
                    source="consensus_engine",
                )
                await self._ui_event_bus.publish(event)
            except Exception as e:
                log.debug("UI event bus publish failed: %s", e)

    # ======================================================================
    # Helpers
    # ======================================================================

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def snapshot(self) -> Dict[str, Any]:
        return {
            "active_consensus": len(self._active_requests),
            "resolved_consensus": len(self._outcomes),
            "total_positions": sum(len(v) for v in self._positions.values()),
        }
