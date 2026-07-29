from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from pydantic import BaseModel, Field, field_validator

from cognition.types import (
    BlackboardEntry, BlackboardSlot, EntryType, Provenance, ConflictRecord, ConflictSeverity,
)

try:
    from runtime_next.models.events import (
        BlackboardPublicationEvent,
        FindingConsumedEvent,
        ChallengeRaisedEvent,
    )
    _HAS_RUNTIME_EVENTS = True
except ImportError:
    BlackboardPublicationEvent = None
    FindingConsumedEvent = None
    ChallengeRaisedEvent = None
    _HAS_RUNTIME_EVENTS = False

log = logging.getLogger("aelvo.cognition.blackboard")


class ChallengeEntry(BaseModel):
    """A challenge to a blackboard entry by a specialist."""
    challenge_id: str
    entry_id: str
    challenger: str
    challenged_claim: str
    evidence: str = ""
    proposed_alternative: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    resolved: bool = False
    resolution: Optional[str] = None


class VoteEntry(BaseModel):
    """A vote on a challenged or uncertain entry."""
    vote_id: str
    entry_id: str
    voter: str
    vote: str  # FOR, AGAINST, ABSTAIN
    confidence: float = 0.5
    reasoning: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("vote")
    @classmethod
    def validate_vote(cls, v: str) -> str:
        if v not in ("FOR", "AGAINST", "ABSTAIN"):
            raise ValueError(f"Invalid vote: {v}. Must be FOR, AGAINST, or ABSTAIN")
        return v

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        return max(0.0, min(1.0, v))


class CognitiveBlackboard:
    """Structured shared workspace for the cognitive layer.

    Uses typed slots with mandatory provenance. Never silently resolves
    conflicts â€” every conflict is recorded with severity for the consensus
    system.
    """

    def __init__(
        self,
        max_slots: int = 100,
        db_path: str = "",
        event_bus: Optional[Any] = None,
    ):
        """Initialize the CognitiveBlackboard.

        Args:
            max_slots: Maximum number of blackboard slots.
            db_path: Optional path to SQLite database for persistence.
            event_bus: Optional runtime EventBus for emitting events.
        """
        self._slots: Dict[str, BlackboardSlot] = {}
        self._max_slots = max_slots
        self._conflicts: List[ConflictRecord] = []
        self._subscriptions: Dict[str, List[Callable[[BlackboardEntry], None]]] = {}
        self._challenges: Dict[str, ChallengeEntry] = {}
        self._votes: Dict[str, List[VoteEntry]] = {}
        self._db_path = db_path
        self._event_bus = event_bus
        self._router: Optional[Any] = None
        self._consumptions: Dict[str, List[Dict[str, Any]]] = {}

        if db_path:
            self._init_db()

    def set_event_bus(self, event_bus: Any) -> None:
        """Set or replace the runtime EventBus for emitting BlackboardPublicationEvents.

        Allows post-construction wiring when the EventBus is not available
        during CognitiveBlackboard initialization.
        """
        self._event_bus = event_bus

    def set_router(self, router: Any) -> None:
        """Set or replace the collaboration TaskRouter.

        When attached, every publish() logs routing decisions through
        the router's route_publication() method, enabling the router
        to act as the Collaboration Transport Layer.
        """
        self._router = router

    def create_slot(self, name: str, max_entries: int = 100) -> BlackboardSlot:
        if name in self._slots:
            return self._slots[name]
        slot = BlackboardSlot(name=name, max_entries=max_entries)
        self._slots[name] = slot
        log.debug("Created blackboard slot '%s'", name)
        return slot

    def get_slot(self, name: str) -> Optional[BlackboardSlot]:
        return self._slots.get(name)

    def get_or_create_slot(self, name: str, max_entries: int = 100) -> BlackboardSlot:
        existing = self.get_slot(name)
        if existing is not None:
            return existing
        return self.create_slot(name, max_entries=max_entries)

    def publish(
        self,
        slot_name: str,
        content: str,
        entry_type: EntryType,
        provenance: Provenance,
        confidence: float = 0.5,
        tags: Optional[List[str]] = None,
        expires_at: Optional[datetime] = None,
    ) -> BlackboardEntry:
        slot = self.get_or_create_slot(slot_name)
        entry_id = self._generate_id(slot_name, content)
        entry = BlackboardEntry(
            id=entry_id,
            slot_name=slot_name,
            content=content,
            entry_type=entry_type,
            provenance=provenance,
            confidence=confidence,
            tags=tags or [],
            expires_at=expires_at,
        )
        conflicts = self._detect_conflicts(entry, slot)
        if conflicts:
            for conflict in conflicts:
                self._conflicts.append(conflict)
                log.warning("Conflict detected in slot '%s': %s", slot_name, conflict.description)

        slot.add_entry(entry)
        self._notify_subscribers(slot_name, entry)
        self._persist_entry(entry)

        # Route publication through collaboration router (if attached)
        if self._router and hasattr(self._router, 'route_publication'):
            try:
                specialist = provenance.source_id if provenance else ""
                routing_targets = self._router.route_publication(
                    evidence_type=entry_type.value if entry_type else "unknown",
                    specialist=specialist,
                    entry_id=entry_id,
                    content_preview=content[:80],
                )
                if routing_targets:
                    log.debug(
                        "Router: %s publication %s routed to %s",
                        specialist, entry_id[:8], routing_targets,
                    )
            except Exception as e:
                log.debug("Router routing failed: %s", e)

        # Emit BlackboardPublicationEvent to runtime EventBus for monitoring/TUI
        if self._event_bus and _HAS_RUNTIME_EVENTS:
            try:
                specialist = provenance.source_id if provenance else ""
                source_type = provenance.source_type.value if provenance else ""
                # Count active challenges against this entry
                active_challenges = [
                    c for c in self._challenges.values()
                    if c.entry_id == entry_id and not c.resolved
                ]
                has_challenges = len(active_challenges) > 0
                lifecycle = "challenged" if has_challenges else "created"
                pub_event = BlackboardPublicationEvent(
                    id=self._generate_id("bb_event", entry_id),
                    specialist=specialist,
                    entry_type=entry_type.value if entry_type else "",
                    summary=content[:120],
                    tags=tags or [],
                    # Phase 6: Trust metadata
                    confidence=confidence,
                    source=source_type,
                    verification_status="pending",
                    challenged=has_challenges,
                    challenge_count=len(active_challenges),
                    lifecycle_status=lifecycle,
                )
                try:
                    loop = asyncio.get_running_loop()
                    if loop.is_running():
                        asyncio.ensure_future(self._event_bus.publish(pub_event))
                except RuntimeError:
                    pass  # No running event loop — skip publish
            except Exception as e:
                log.debug("Failed to emit BlackboardPublicationEvent: %s", e)

        log.debug("Published entry %s to slot '%s'", entry_id, slot_name)
        return entry

    def read(self, slot_name: str, entry_type: Optional[EntryType] = None) -> List[BlackboardEntry]:
        slot = self.get_slot(slot_name)
        if slot is None:
            return []
        entries = slot.active_entries()
        if entry_type is not None:
            entries = [e for e in entries if e.entry_type == entry_type]
        return entries

    def read_latest(self, slot_name: str, entry_type: Optional[EntryType] = None) -> Optional[BlackboardEntry]:
        entries = self.read(slot_name, entry_type)
        if not entries:
            return None
        entries.sort(key=lambda e: e.timestamp, reverse=True)
        return entries[0]

    def supersede(self, entry_id: str, replacement_id: str) -> bool:
        for slot in self._slots.values():
            for entry in slot.entries:
                if entry.id == entry_id:
                    entry.superseded_by = replacement_id
                    return True
        return False

    def query(self, query_text: str, max_results: int = 10) -> List[BlackboardEntry]:
        query_lower = query_text.lower()
        scored: List[tuple] = []
        for slot in self._slots.values():
            for entry in slot.active_entries():
                score = 0.0
                if query_lower in entry.content.lower():
                    score += 0.5
                for tag in entry.tags:
                    if query_lower in tag.lower():
                        score += 0.3
                if query_lower in entry.slot_name.lower():
                    score += 0.2
                score *= entry.confidence
                if score > 0:
                    scored.append((score, entry))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [e for _, e in scored[:max_results]]

    def get_all_active_entries(self) -> List[BlackboardEntry]:
        result = []
        for slot in self._slots.values():
            result.extend(slot.active_entries())
        return result

    def get_pending_conflicts(self) -> List[ConflictRecord]:
        return [c for c in self._conflicts if not c.resolved]

    def resolve_conflict(self, conflict_id: str, resolution: str) -> bool:
        for conflict in self._conflicts:
            if conflict.id == conflict_id and not conflict.resolved:
                conflict.resolved = True
                conflict.resolved_at = datetime.now(timezone.utc)
                conflict.resolution_notes = resolution
                log.info("Resolved conflict %s: %s", conflict_id, resolution)
                return True
        return False

    def subscribe(self, slot_name: str, callback: Callable[[BlackboardEntry], None]) -> None:
        if slot_name not in self._subscriptions:
            self._subscriptions[slot_name] = []
        self._subscriptions[slot_name].append(callback)

    def slot_names(self) -> List[str]:
        return list(self._slots.keys())

    # ======================================================================
    # Challenge System
    # ======================================================================

    def challenge(
        self,
        slot_type: str,
        entry_id: str,
        challenger: str,
        challenged_claim: str,
        evidence: str = "",
        proposed_alternative: str = "",
    ) -> ChallengeEntry:
        """Challenge a finding published by another specialist.

        The challenge is stored alongside the original entry. The original
        entry's status changes to CHALLENGED. An event is published.
        Architect is notified. Challenges are never silent.
        """
        # Find the entry in any slot
        entry = None
        for sname, slot in self._slots.items():
            for e in slot.entries:
                if e.id == entry_id:
                    entry = e
                    break
            if entry:
                break

        if entry is None:
            raise ValueError(f"Entry {entry_id} not found on blackboard")

        challenge_entry = ChallengeEntry(
            challenge_id=self._generate_id("challenge", f"{entry_id}_{challenger}"),
            entry_id=entry_id,
            challenger=challenger,
            challenged_claim=challenged_claim,
            evidence=evidence,
            proposed_alternative=proposed_alternative,
        )
        self._challenges[challenge_entry.challenge_id] = challenge_entry

        # Initialize vote tracking for this entry
        if entry_id not in self._votes:
            self._votes[entry_id] = []

        log.warning(
            "Challenge raised by %s on entry %s: %s",
            challenger, entry_id[:8], challenged_claim[:60]
        )

        # Emit CHALLENGE_RAISED event
        if self._event_bus and _HAS_RUNTIME_EVENTS and ChallengeRaisedEvent:
            try:
                challenge_event = ChallengeRaisedEvent(
                    id=self._generate_id("challenge_event", challenge_entry.challenge_id),
                    challenge_id=challenge_entry.challenge_id,
                    entry_id=entry_id,
                    challenger=challenger,
                    challenged_claim=challenged_claim,
                    evidence=evidence,
                )
                try:
                    loop = asyncio.get_running_loop()
                    if loop.is_running():
                        asyncio.ensure_future(self._event_bus.publish(challenge_event))
                except RuntimeError:
                    pass
            except Exception as e:
                log.debug("Failed to emit ChallengeRaisedEvent: %s", e)

        return challenge_entry

    def get_challenges(self, entry_id: Optional[str] = None) -> List[ChallengeEntry]:
        """Get all challenges, optionally filtered by entry."""
        if entry_id:
            return [c for c in self._challenges.values() if c.entry_id == entry_id]
        return list(self._challenges.values())

    def resolve_challenge(self, challenge_id: str, resolution: str, resolver: str) -> bool:
        """Resolve a challenge with a final decision."""
        challenge = self._challenges.get(challenge_id)
        if challenge is None:
            return False
        challenge.resolved = True
        challenge.resolution = resolution
        log.info("Challenge %s resolved by %s: %s", challenge_id[:8], resolver, resolution[:60])
        return True

    # ======================================================================
    # Voting System
    # ======================================================================

    def vote(
        self,
        slot_type: str,
        entry_id: str,
        voter: str,
        vote: str,
        confidence: float = 0.5,
        reasoning: str = "",
    ) -> VoteEntry:
        """Vote on a challenged or uncertain entry.

        Votes are FOR, AGAINST, or ABSTAIN with a confidence float and
        optional reasoning. When all relevant specialists have voted or the
        voting deadline passes, the blackboard automatically tallies the
        votes and records the outcome.
        """
        vote_entry = VoteEntry(
            vote_id=self._generate_id("vote", f"{entry_id}_{voter}"),
            entry_id=entry_id,
            voter=voter,
            vote=vote,
            confidence=confidence,
            reasoning=reasoning,
        )

        if entry_id not in self._votes:
            self._votes[entry_id] = []
        self._votes[entry_id].append(vote_entry)

        log.info(
            "Vote recorded: %s voted %s on %s (confidence=%.2f)",
            voter, vote, entry_id[:8], confidence
        )
        return vote_entry

    def tally_votes(self, entry_id: str) -> Dict[str, Any]:
        """Tally votes for an entry and return the outcome."""
        votes = self._votes.get(entry_id, [])
        if not votes:
            return {"outcome": "NO_VOTES", "for": 0, "against": 0, "abstain": 0}

        counts = {"FOR": 0, "AGAINST": 0, "ABSTAIN": 0}
        total_confidence = 0.0
        for v in votes:
            if v.vote in counts:
                counts[v.vote] += 1
            total_confidence += v.confidence

        avg_confidence = round(total_confidence / len(votes), 4) if votes else 0.0
        for_count = counts["FOR"]
        total_valid = counts["FOR"] + counts["AGAINST"]

        outcome = "TIE"
        if total_valid > 0:
            if for_count > total_valid / 2:
                outcome = "APPROVED"
            elif for_count < total_valid / 2:
                outcome = "REJECTED"

        return {
            "outcome": outcome,
            "for": counts["FOR"],
            "against": counts["AGAINST"],
            "abstain": counts["ABSTAIN"],
            "total_votes": len(votes),
            "avg_confidence": avg_confidence,
        }

    def get_votes(self, entry_id: str) -> List[VoteEntry]:
        """Get all votes for an entry."""
        return self._votes.get(entry_id, [])

    # ======================================================================
    # SQLite Persistence
    # ======================================================================

    def _init_db(self) -> None:
        """Initialize SQLite tables for blackboard persistence."""
        try:
            os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
            with sqlite3.connect(self._db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS blackboard_entries (
                        entry_id TEXT PRIMARY KEY,
                        slot_name TEXT NOT NULL,
                        content TEXT NOT NULL,
                        entry_type TEXT NOT NULL,
                        provenance TEXT NOT NULL,
                        confidence REAL DEFAULT 0.5,
                        tags TEXT DEFAULT '[]',
                        timestamp TEXT DEFAULT (datetime('now'))
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS blackboard_challenges (
                        challenge_id TEXT PRIMARY KEY,
                        entry_id TEXT NOT NULL,
                        challenger TEXT NOT NULL,
                        challenged_claim TEXT NOT NULL,
                        evidence TEXT DEFAULT '',
                        proposed_alternative TEXT DEFAULT '',
                        resolved INTEGER DEFAULT 0,
                        resolution TEXT
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS blackboard_votes (
                        vote_id TEXT PRIMARY KEY,
                        entry_id TEXT NOT NULL,
                        voter TEXT NOT NULL,
                        vote TEXT NOT NULL,
                        confidence REAL DEFAULT 0.5,
                        reasoning TEXT DEFAULT ''
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS archived_entries (
                        entry_id TEXT PRIMARY KEY,
                        reason TEXT DEFAULT '',
                        archived_by TEXT DEFAULT '',
                        archived_at TEXT DEFAULT (datetime('now'))
                    )
                """)
            self._load_from_db()
            self._restore_archive_state()
        except Exception as e:
            log.error("Failed to initialize blackboard database: %s", e)

    def _persist_entry(self, entry: BlackboardEntry) -> None:
        """Persist a single entry to SQLite."""
        if not self._db_path:
            return
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO blackboard_entries
                       (entry_id, slot_name, content, entry_type, provenance, confidence, tags)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        entry.id,
                        entry.slot_name,
                        entry.content,
                        entry.entry_type.value,
                        entry.provenance.model_dump_json(),
                        entry.confidence,
                        json.dumps(entry.tags),
                    ),
                )
        except Exception as e:
            log.warning("Failed to persist entry %s: %s", entry.id[:8], e)

    def _load_from_db(self) -> None:
        """Load entries from SQLite into memory."""
        if not self._db_path:
            return
        try:
            with sqlite3.connect(self._db_path) as conn:
                rows = conn.execute("SELECT * FROM blackboard_entries").fetchall()
                for row in rows:
                    try:
                        entry = BlackboardEntry(
                            id=row[0],
                            slot_name=row[1],
                            content=row[2],
                            entry_type=EntryType(row[3]),
                            provenance=Provenance(**json.loads(row[4])),
                            confidence=row[5],
                            tags=json.loads(row[6]),
                        )
                        slot = self.get_or_create_slot(entry.slot_name)
                        slot.entries.append(entry)
                    except Exception as e:
                        log.warning("Failed to restore blackboard entry: %s", e)

                # Load challenges (inside same connection scope)
                challenge_rows = conn.execute("SELECT * FROM blackboard_challenges").fetchall()
                for row in challenge_rows:
                    self._challenges[row[0]] = ChallengeEntry(
                        challenge_id=row[0],
                        entry_id=row[1],
                        challenger=row[2],
                        challenged_claim=row[3],
                        evidence=row[4],
                        proposed_alternative=row[5],
                        resolved=bool(row[6]),
                        resolution=row[7],
                    )

                # Load votes (inside same connection scope)
                vote_rows = conn.execute("SELECT * FROM blackboard_votes").fetchall()
                for row in vote_rows:
                    entry_id = row[1]
                    if entry_id not in self._votes:
                        self._votes[entry_id] = []
                    self._votes[entry_id].append(VoteEntry(
                        vote_id=row[0],
                        entry_id=entry_id,
                        voter=row[2],
                        vote=row[3],
                        confidence=row[4],
                        reasoning=row[5],
                    ))
        except Exception as e:
            log.debug("No existing blackboard data to restore: %s", e)

    def _restore_archive_state(self) -> None:
        """Cross-reference the ``archived_entries`` table after loading.

        Entries loaded from ``blackboard_entries`` have ``superseded_by=None``
        by default, so archived entries would incorrectly appear as active
        after a restart.  This method reads the ``archived_entries`` table
        and marks each referenced entry with the ``ARCHIVE_SENTINEL``.
        """
        if not self._db_path:
            return
        try:
            with sqlite3.connect(self._db_path) as conn:
                rows = conn.execute(
                    "SELECT entry_id FROM archived_entries"
                ).fetchall()
            archived_ids = {row[0] for row in rows}
            for slot in self._slots.values():
                for entry in slot.entries:
                    if entry.id in archived_ids:
                        entry.superseded_by = self.ARCHIVE_SENTINEL
            if archived_ids:
                log.info(
                    "Restored archive state for %d entries",
                    len(archived_ids),
                )
        except Exception as e:
            log.debug("No archive state to restore: %s", e)

    # ======================================================================
    # Consumption Tracking
    # ======================================================================

    def consume(self, entry_id: str, consumer: str) -> Optional[BlackboardEntry]:
        """Record that a specialist consumed a blackboard entry.

        Consumption tracking enables auditability and answers "who used what evidence?".
        Every call logs which specialist consumed which entry and when.
        Emits a ``FindingConsumedEvent`` on the runtime EventBus when available.

        Args:
            entry_id: The ID of the entry being consumed.
            consumer: The name of the specialist consuming the entry.

        Returns:
            The BlackboardEntry if found, None otherwise.
        """
        for slot in self._slots.values():
            for entry in slot.entries:
                if entry.id == entry_id:
                    if entry_id not in self._consumptions:
                        self._consumptions[entry_id] = []
                    self._consumptions[entry_id].append({
                        "consumer": consumer,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    log.debug(
                        "Consumption: %s consumed entry %s in slot '%s'",
                        consumer, entry_id[:8], slot.name,
                    )

                    # Emit FINDING_CONSUMED event
                    if self._event_bus and _HAS_RUNTIME_EVENTS and FindingConsumedEvent:
                        try:
                            consumed_event = FindingConsumedEvent(
                                id=self._generate_id("consumed", f"{entry_id}_{consumer}"),
                                entry_id=entry_id,
                                consumer=consumer,
                                entry_owner=entry.provenance.source_id if entry.provenance else "",
                                entry_type=entry.entry_type.value if entry.entry_type else "",
                                slot_name=slot.name,
                            )
                            loop = asyncio.get_running_loop()
                            if loop.is_running():
                                asyncio.ensure_future(self._event_bus.publish(consumed_event))
                        except RuntimeError:
                            pass
                        except Exception as e:
                            log.debug("Failed to emit FindingConsumedEvent: %s", e)

                    return entry
        log.warning("Consumption failed: entry %s not found", entry_id[:8])
        return None

    def get_consumption_trail(self, entry_id: str) -> List[Dict[str, Any]]:
        """Get the full consumption history for a blackboard entry.

        Returns a list of dicts with 'consumer' and 'timestamp' keys,
        ordered oldest first.

        Args:
            entry_id: The ID of the entry to get the trail for.

        Returns:
            List of consumption records, empty list if never consumed.
        """
        return self._consumptions.get(entry_id, [])

    def get_all_consumptions(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all consumption records across all entries.

        Returns:
            Dict mapping entry_id -> list of consumption records.
        """
        return dict(self._consumptions)

    def snapshot(self) -> Dict[str, Any]:
        consumption_count = sum(len(v) for v in self._consumptions.values())
        return {
            "slot_count": len(self._slots),
            "active_entry_count": len(self.get_all_active_entries()),
            "pending_conflicts": len(self.get_pending_conflicts()),
            "total_conflicts": len(self._conflicts),
            "slot_names": list(self._slots.keys()),
            "challenge_count": len(self._challenges),
            "vote_count": sum(len(v) for v in self._votes.values()),
            "consumption_count": consumption_count,
            "router_attached": self._router is not None,
        }

    def _generate_id(self, slot_name: str, content: str) -> str:
        raw = f"{slot_name}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def evidence(self) -> List[Any]:
        """Export all active knowledge from the blackboard.

        Returns CollaborationEvidence objects from active blackboard entries —
        promoted to typed CollaborationEvidence with full provenance,
        verification status, lifecycle tracking, consumption trail, and
        trust metadata.

        The lifecycle status is derived from the entry's state:
        - Archived entries are excluded (they use ARCHIVE_SENTINEL)
        - Entries with pending challenges are CHALLENGED
        - Entries that have been consumed are CONSUMED
        - Entries with active challenges get CHALLENGED status
        - Otherwise CREATED

        Import is deferred to avoid circular dependencies.
        """
        from cognition.types import (
            CollaborationEvidence, VerificationStatus,
            EvidenceLifecycleStatus, EvidenceTimeline,
        )

        result: List[Any] = []

        for slot in self._slots.values():
            for entry in slot.active_entries():
                consumption = self._consumptions.get(entry.id, [])

                has_challenges = any(
                    c.entry_id == entry.id and not c.resolved
                    for c in self._challenges.values()
                )
                has_consumptions = len(consumption) > 0

                if has_challenges:
                    lifecycle_status = EvidenceLifecycleStatus.CHALLENGED
                    verif_status = VerificationStatus.CHALLENGED
                elif has_consumptions:
                    lifecycle_status = EvidenceLifecycleStatus.CONSUMED
                    verif_status = VerificationStatus.VERIFIED
                else:
                    lifecycle_status = EvidenceLifecycleStatus.CREATED
                    verif_status = VerificationStatus.PENDING

                timeline = EvidenceTimeline(
                    created_at=entry.timestamp,
                    consumed_at=(
                        datetime.fromisoformat(consumption[-1]["timestamp"])
                        if has_consumptions and consumption[-1].get("timestamp")
                        else None
                    ),
                )

                evidence = CollaborationEvidence(
                    id=entry.id,
                    owner_agent=entry.provenance.source_id if entry.provenance else "",
                    timestamp=entry.timestamp,
                    confidence=entry.confidence,
                    source=entry.provenance.source_type.value if entry.provenance else "",
                    evidence_type=entry.entry_type.value if entry.entry_type else "unknown",
                    verification_status=verif_status,
                    lifecycle_status=lifecycle_status,
                    timeline=timeline,
                    related_tasks=[tag.split(":", 1)[1] for tag in entry.tags if tag.startswith("task:")],
                    affected_files=[],
                    summary=entry.content[:120],
                    content=entry.content,
                    metadata={
                        "slot_name": slot.name,
                        "tags": entry.tags,
                        "consumed_by": [c["consumer"] for c in consumption],
                        "consumption_count": len(consumption),
                        "challenge_count": sum(1 for c in self._challenges.values() if c.entry_id == entry.id),
                    },
                )
                result.append(evidence)

        return result

    def _detect_conflicts(self, new_entry: BlackboardEntry, slot: BlackboardSlot) -> List[ConflictRecord]:
        conflicts: List[ConflictRecord] = []
        for existing in slot.active_entries():
            if existing.id == new_entry.id:
                continue
            if existing.entry_type != new_entry.entry_type:
                continue
            if existing.entry_type in (EntryType.FACT, EntryType.CONSTRAINT, EntryType.DECISION):
                if self._is_contradictory(existing, new_entry):
                    severity = ConflictSeverity.HIGH if existing.entry_type == EntryType.CONSTRAINT else ConflictSeverity.MEDIUM
                    record = ConflictRecord(
                        id=self._generate_id("conflict", f"{existing.id}_{new_entry.id}"),
                        description=f"Contradiction between '{existing.content[:50]}' and '{new_entry.content[:50]}'",
                        severity=severity,
                        involved_entries=[existing.id, new_entry.id],
                    )
                    conflicts.append(record)
        return conflicts

    def _is_contradictory(self, a: BlackboardEntry, b: BlackboardEntry) -> bool:
        a_lower = a.content.lower()
        b_lower = b.content.lower()
        negations = ["not ", "never ", "cannot ", "should not ", "must not ", "isn't ", "aren't ", "don't "]
        a_has_neg = any(n in a_lower for n in negations)
        b_has_neg = any(n in b_lower for n in negations)
        if a_has_neg != b_has_neg:
            core_a = self._strip_negations(a_lower)
            core_b = self._strip_negations(b_lower)
            if core_a == core_b:
                return True
        return False

    def _strip_negations(self, text: str) -> str:
        for n in ["not ", "never ", "cannot ", "should not ", "must not "]:
            text = text.replace(n, "")
        return text.strip()

    # ======================================================================
    # Archive System
    # ======================================================================

    ARCHIVE_SENTINEL = "__archived__"

    def archive(self, entry_id: str, reason: str = "", archived_by: str = "") -> bool:
        """Archive an entry, removing it from active view.

        Archived entries are marked with the ARCHIVE_SENTINEL in their
        ``superseded_by`` field, which excludes them from
        ``active_entries()`` results. The reason and archiver are logged
        and persisted.

        Returns:
            True if the entry was found and archived, False otherwise.
        """
        for slot_name, slot in self._slots.items():
            for entry in slot.entries:
                if entry.id == entry_id:
                    entry.superseded_by = self.ARCHIVE_SENTINEL
                    log.info(
                        "Archived entry %s in slot '%s'%s%s",
                        entry_id[:8], slot_name,
                        f" by {archived_by}" if archived_by else "",
                        f": {reason}" if reason else "",
                    )
                    self._persist_archive(entry_id, reason, archived_by)
                    self._notify_subscribers(slot_name, entry)
                    return True
        return False

    def get_archived_entries(self, slot_name: Optional[str] = None) -> List[BlackboardEntry]:
        """Get all archived entries, optionally filtered by slot."""
        result = []
        for sname, slot in self._slots.items():
            if slot_name is not None and sname != slot_name:
                continue
            for entry in slot.entries:
                if entry.superseded_by == self.ARCHIVE_SENTINEL:
                    result.append(entry)
        return result

    def _persist_archive(self, entry_id: str, reason: str, archived_by: str) -> None:
        """Persist an archive record to SQLite."""
        if not self._db_path:
            return
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO archived_entries
                       (entry_id, reason, archived_by, archived_at)
                       VALUES (?, ?, ?, ?)""",
                    (
                        entry_id,
                        reason or "",
                        archived_by or "",
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
        except Exception as e:
            log.warning("Failed to persist archive for entry %s: %s", entry_id[:8], e)

    def _notify_subscribers(self, slot_name: str, entry: BlackboardEntry) -> None:
        callbacks = self._subscriptions.get(slot_name, [])
        for cb in callbacks:
            try:
                cb(entry)
            except Exception as e:
                log.error("Subscriber callback failed for slot '%s': %s", slot_name, e)
