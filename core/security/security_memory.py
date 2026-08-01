"""SecurityMemory — Records violations, dangerous patterns, and recovery outcomes.

The security memory subsystem learns from past security events to improve
future policy decisions. It stores:

- Policy violations (blocked actions, classification details)
- Recurring dangerous patterns (commands, paths, sequences)
- Recovery outcomes (what worked, what didn't)
- Known hostile entities (repositories, commands, URLs)
- Security posture trends

Memory entries have importance scores that decay over time unless
reinforced by repeated events.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from .execution_governance import PolicyDecision, RiskLevel, TrustLevel

log = logging.getLogger("aelvo.security.memory")


# ============================================================================
# Memory Entry Types
# ============================================================================


class MemoryEntryType(str, Enum):
    """What kind of security event this entry records."""

    POLICY_VIOLATION = "policy_violation"
    """A blocked or denied action."""

    DANGEROUS_PATTERN = "dangerous_pattern"
    """A recurring risky command or path pattern."""

    RECOVERY_OUTCOME = "recovery_outcome"
    """How a recovery from a security event went."""

    HOSTILE_ENTITY = "hostile_entity"
    """A known hostile repository, command, or URL."""

    APPROVED_RISKY_ACTION = "approved_risky_action"
    """A high-risk action that was explicitly approved."""

    SECURITY_INSIGHT = "security_insight"
    """A learned insight for improving policy."""


@dataclass
class SecurityMemoryEntry:
    """A single entry in the security memory system."""

    id: str = ""
    """Unique identifier for this entry."""

    entry_type: MemoryEntryType = MemoryEntryType.POLICY_VIOLATION
    """What kind of event this records."""

    risk_level: RiskLevel = RiskLevel.SAFE
    """The risk level of the event."""

    trust_level: TrustLevel = TrustLevel.TRUSTED
    """The trust level of the event."""

    target: str = ""
    """The action target (command, path, URL, etc.)."""

    specialist: str = ""
    """The specialist that was involved, if any."""

    tool_name: str = ""
    """The tool that was involved."""

    reason: str = ""
    """Why this entry was created."""

    evidence: Dict[str, Any] = field(default_factory=dict)
    """Supporting evidence."""

    importance: float = 0.5
    """Importance score (0.0–1.0). Decays over time unless reinforced."""

    recurrence_count: int = 1
    """How many times this pattern has been seen."""

    timestamp: float = 0.0
    """When the event occurred."""

    last_seen: float = 0.0
    """When this pattern was last observed."""

    tags: List[str] = field(default_factory=list)
    """Tags for categorization and querying."""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def decay(self, factor: float = 0.98) -> None:
        """Decay importance over time unless recently observed."""
        days_since_last_seen = (time.time() - self.last_seen) / 86400
        if days_since_last_seen > 7:
            self.importance *= (factor ** (days_since_last_seen / 7))
        # Floor removed — pruning handles cleanup of unimportant entries

    def reinforce(self) -> None:
        """Reinforce this entry (increases importance and recurrence)."""
        self.recurrence_count += 1
        self.last_seen = time.time()
        # Each recurrence reinforces importance but with diminishing returns
        self.importance = min(1.0, self.importance + (1.0 - self.importance) * 0.3)


# ============================================================================
# SecurityMemory
# ============================================================================


class SecurityMemory:
    """Persistent security memory that learns from past events.

    This is an in-memory store with optional integration to the AELVO
    memory collection (ChromaDB) for persistent storage.

    Usage:
        mem = SecurityMemory()
        mem.record_violation(decision)
        mem.record_risky_action("rm -rf temp/", "FORGE")
        threats = mem.get_recurring_threats(min_recurrence=3)
    """

    def __init__(
        self,
        max_entries: int = 10000,
        memory_collection: Any = None,
        project_name: str = "",
        db_path: Optional[str] = None,
    ):
        self._entries: Dict[str, SecurityMemoryEntry] = {}
        self._max_entries = max_entries
        self._memory_collection = memory_collection
        self._project_name = project_name
        self._db_path = db_path

        # Indexes for fast lookups (must exist before loading persisted entries)
        self._by_type: Dict[str, Set[str]] = {t.value: set() for t in MemoryEntryType}
        self._by_risk: Dict[str, Set[str]] = {r.value: set() for r in RiskLevel}
        self._by_target: Dict[str, Set[str]] = {}
        self._by_specialist: Dict[str, Set[str]] = {}

        # Load persisted entries from SQLite if configured
        if self._db_path:
            self._init_db()
            self._load_entries()

        log.info(f"SecurityMemory initialized (max_entries={max_entries})")

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_violation(self, decision: PolicyDecision) -> str:
        """Record a policy violation from a PolicyDecision.

        Args:
            decision: The policy decision that resulted in a violation.

        Returns:
            The entry ID.
        """
        entry_id = f"sec_{uuid.uuid4().hex[:12]}"
        entry = SecurityMemoryEntry(
            id=entry_id,
            entry_type=MemoryEntryType.POLICY_VIOLATION,
            risk_level=decision.risk_level,
            trust_level=decision.trust_level,
            target=decision.action_target[:500],
            tool_name=decision.action_type,
            reason=decision.reason,
            evidence=decision.to_dict(),
            importance=0.7 if decision.risk_level == RiskLevel.BLOCKED else 0.5,
            timestamp=decision.timestamp,
            last_seen=time.time(),
            tags=["violation", decision.risk_level.value],
        )
        return self._add_entry(entry)

    def record_risky_action(
        self,
        target: str,
        specialist: str = "",
        tool_name: str = "",
        risk_level: RiskLevel = RiskLevel.APPROVAL_REQUIRED,
        reason: str = "",
        evidence: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Record an approved risky action.

        Args:
            target: The action target.
            specialist: The specialist that performed the action.
            tool_name: The tool used.
            risk_level: The risk level.
            reason: Why the action was risky.
            evidence: Optional supporting evidence.

        Returns:
            The entry ID.
        """
        entry_id = f"sec_{uuid.uuid4().hex[:12]}"
        entry = SecurityMemoryEntry(
            id=entry_id,
            entry_type=MemoryEntryType.APPROVED_RISKY_ACTION,
            risk_level=risk_level,
            target=target[:500],
            specialist=specialist,
            tool_name=tool_name,
            reason=reason or f"Risky action approved: {target[:200]}",
            evidence=evidence or {},
            importance=0.4,
            timestamp=time.time(),
            last_seen=time.time(),
            tags=["approved", risk_level.value, specialist] if specialist else ["approved", risk_level.value],
        )
        return self._add_entry(entry)

    def record_dangerous_pattern(
        self,
        pattern_type: str,
        target: str,
        reason: str,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Record a recurring dangerous pattern.

        Args:
            pattern_type: The type of pattern (e.g., 'shell_injection', 'path_traversal').
            target: The pattern string.
            reason: Why this is dangerous.
            evidence: Optional supporting evidence.

        Returns:
            The entry ID.
        """
        # Check for existing entry with similar target
        existing = self._find_existing(target, MemoryEntryType.DANGEROUS_PATTERN)
        if existing:
            existing.reinforce()
            return existing.id

        entry_id = f"sec_{uuid.uuid4().hex[:12]}"
        entry = SecurityMemoryEntry(
            id=entry_id,
            entry_type=MemoryEntryType.DANGEROUS_PATTERN,
            risk_level=RiskLevel.RESTRICTED,
            target=target[:500],
            reason=reason,
            evidence=evidence or {},
            importance=0.6,
            timestamp=time.time(),
            last_seen=time.time(),
            tags=["pattern", pattern_type],
        )
        return self._add_entry(entry)

    def record_recovery_outcome(
        self,
        violation_id: str,
        success: bool,
        strategy: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Record the outcome of a recovery action.

        Args:
            violation_id: The ID of the original violation entry.
            strategy: The recovery strategy used.
            success: Whether the recovery succeeded.
            details: Optional details about the recovery.

        Returns:
            The entry ID.
        """
        entry_id = f"sec_{uuid.uuid4().hex[:12]}"
        entry = SecurityMemoryEntry(
            id=entry_id,
            entry_type=MemoryEntryType.RECOVERY_OUTCOME,
            risk_level=RiskLevel.SAFE,
            target=violation_id,
            reason=f"Recovery {'succeeded' if success else 'failed'}: {strategy}",
            evidence={"strategy": strategy, "success": success, **(details or {})},
            importance=0.5 if success else 0.7,
            timestamp=time.time(),
            last_seen=time.time(),
            tags=["recovery", "success" if success else "failure", strategy],
        )
        return self._add_entry(entry)

    def record_hostile_entity(
        self,
        entity_type: str,
        identifier: str,
        reason: str,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Record a known hostile entity.

        Args:
            entity_type: What kind of entity (e.g., 'command', 'repository', 'url').
            identifier: The entity identifier (e.g., 'rm -rf /', 'github.com/malicious').
            reason: Why this is considered hostile.
            evidence: Optional supporting evidence.

        Returns:
            The entry ID.
        """
        entry_id = f"sec_{uuid.uuid4().hex[:12]}"
        entry = SecurityMemoryEntry(
            id=entry_id,
            entry_type=MemoryEntryType.HOSTILE_ENTITY,
            risk_level=RiskLevel.BLOCKED,
            trust_level=TrustLevel.HOSTILE,
            target=identifier[:500],
            reason=reason,
            evidence=evidence or {},
            importance=0.9,
            timestamp=time.time(),
            last_seen=time.time(),
            tags=["hostile", entity_type],
        )
        return self._add_entry(entry)

    def record_insight(
        self,
        insight: str,
        importance: float = 0.5,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Record a learned security insight for policy improvement.

        Args:
            insight: The insight text.
            importance: Importance score (0.0–1.0).
            evidence: Optional supporting evidence.

        Returns:
            The entry ID.
        """
        entry_id = f"sec_{uuid.uuid4().hex[:12]}"
        entry = SecurityMemoryEntry(
            id=entry_id,
            entry_type=MemoryEntryType.SECURITY_INSIGHT,
            target=insight[:500],
            reason="Learned security insight",
            evidence=evidence or {},
            importance=importance,
            timestamp=time.time(),
            last_seen=time.time(),
            tags=["insight"],
        )
        return self._add_entry(entry)

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def get_recurring_threats(
        self,
        min_recurrence: int = 3,
        min_importance: float = 0.3,
    ) -> List[SecurityMemoryEntry]:
        """Get patterns that have recurred frequently and are still relevant.

        Args:
            min_recurrence: Minimum recurrence count.
            min_importance: Minimum importance score.

        Returns:
            List of matching entries.
        """
        threats = []
        for entry in self._entries.values():
            if (entry.entry_type == MemoryEntryType.DANGEROUS_PATTERN
                    and entry.recurrence_count >= min_recurrence
                    and entry.importance >= min_importance):
                threats.append(entry)
        return sorted(threats, key=lambda e: e.recurrence_count, reverse=True)

    def get_hostile_entities(self) -> List[SecurityMemoryEntry]:
        """Get all known hostile entities."""
        return [
            e for e in self._entries.values()
            if e.entry_type == MemoryEntryType.HOSTILE_ENTITY
        ]

    def get_recent_violations(self, n: int = 20) -> List[SecurityMemoryEntry]:
        """Get the n most recent policy violations."""
        violations = [
            e for e in self._entries.values()
            if e.entry_type == MemoryEntryType.POLICY_VIOLATION
        ]
        violations.sort(key=lambda e: e.timestamp, reverse=True)
        return violations[:n]

    def query(
        self,
        entry_type: Optional[MemoryEntryType] = None,
        risk_level: Optional[RiskLevel] = None,
        specialist: Optional[str] = None,
        min_importance: float = 0.0,
        limit: int = 50,
    ) -> List[SecurityMemoryEntry]:
        """Query security memory entries with filters.

        Args:
            entry_type: Filter by entry type.
            risk_level: Filter by risk level.
            specialist: Filter by specialist name.
            min_importance: Minimum importance threshold.
            limit: Maximum results.

        Returns:
            List of matching entries.
        """
        results = list(self._entries.values())

        if entry_type:
            results = [e for e in results if e.entry_type == entry_type]
        if risk_level:
            results = [e for e in results if e.risk_level == risk_level]
        if specialist:
            results = [e for e in results if e.specialist == specialist]
        if min_importance > 0:
            results = [e for e in results if e.importance >= min_importance]

        results.sort(key=lambda e: e.importance, reverse=True)
        return results[:limit]

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of security memory state."""
        by_type = {}
        for e in self._entries.values():
            by_type[e.entry_type.value] = by_type.get(e.entry_type.value, 0) + 1

        threats = self.get_recurring_threats(min_recurrence=3)
        hostile = self.get_hostile_entities()

        return {
            "total_entries": len(self._entries),
            "by_type": by_type,
            "recurring_threats": len(threats),
            "hostile_entities": len(hostile),
            "max_entries": self._max_entries,
            "top_threats": [
                {"reason": t.reason, "recurrence": t.recurrence_count, "importance": t.importance}
                for t in threats[:5]
            ],
        }

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def decay_all(self, factor: float = 0.98) -> None:
        """Decay importance of all entries."""
        for entry in self._entries.values():
            entry.decay(factor)

        # Prune low-importance entries
        to_remove = [
            eid for eid, e in self._entries.items()
            if e.importance < 0.05 and e.recurrence_count < 2
        ]
        for eid in to_remove:
            self._remove_entry(eid)

    def clear(self) -> None:
        """Clear all security memory entries."""
        self._entries.clear()
        self._by_type = {t.value: set() for t in MemoryEntryType}
        self._by_risk = {r.value: set() for r in RiskLevel}
        self._by_target.clear()
        self._by_specialist.clear()
        self._db_clear()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _add_entry(self, entry: SecurityMemoryEntry) -> str:
        """Add an entry and update indexes."""
        self._entries[entry.id] = entry

        # Update type index
        self._by_type.setdefault(entry.entry_type.value, set()).add(entry.id)

        # Update risk index
        self._by_risk.setdefault(entry.risk_level.value, set()).add(entry.id)

        # Update target index (first few chars as key)
        target_key = entry.target[:50]
        self._by_target.setdefault(target_key, set()).add(entry.id)

        # Update specialist index
        if entry.specialist:
            self._by_specialist.setdefault(entry.specialist, set()).add(entry.id)

        # Prune if over limit
        if len(self._entries) > self._max_entries:
            self._prune_oldest()

        # Persist to ChromaDB if available
        self._persist_entry(entry)

        # Persist to SQLite if configured
        self._db_upsert(entry)

        return entry.id

    def _remove_entry(self, entry_id: str) -> None:
        """Remove an entry and update indexes."""
        entry = self._entries.pop(entry_id, None)
        if entry is None:
            return

        self._by_type.get(entry.entry_type.value, set()).discard(entry_id)
        self._by_risk.get(entry.risk_level.value, set()).discard(entry_id)
        target_key = entry.target[:50]
        self._by_target.get(target_key, set()).discard(entry_id)
        if entry.specialist:
            self._by_specialist.get(entry.specialist, set()).discard(entry_id)
        self._db_delete(entry_id)

    def _find_existing(self, target: str, entry_type: MemoryEntryType) -> Optional[SecurityMemoryEntry]:
        """Find an existing entry with a similar target and type."""
        target_key = target[:50]
        candidates = self._by_target.get(target_key, set())
        for eid in candidates:
            entry = self._entries.get(eid)
            if entry and entry.entry_type == entry_type:
                # Check for similarity (simple substring match)
                if target[:100] in entry.target or entry.target[:100] in target:
                    return entry
        return None

    def _prune_oldest(self) -> int:
        """Remove oldest entries when over limit."""
        sorted_entries = sorted(
            self._entries.values(),
            key=lambda e: (e.last_seen, e.importance),
        )
        # Remove oldest 10%
        pruned = max(1, len(sorted_entries) // 10)
        for entry in sorted_entries[:pruned]:
            self._remove_entry(entry.id)
        log.info(f"Pruned {pruned} oldest security memory entries")
        return pruned

    def _persist_entry(self, entry: SecurityMemoryEntry) -> None:
        """Persist entry to ChromaDB if available."""
        if self._memory_collection is None:
            return
        try:
            self._memory_collection.add(
                ids=[entry.id],
                documents=[f"[SECURITY] {entry.entry_type.value}: {entry.reason}"],
                metadatas=[{
                    "type": "security_memory",
                    "entry_type": entry.entry_type.value,
                    "risk_level": entry.risk_level.value,
                    "importance": entry.importance,
                    "recurrence": entry.recurrence_count,
                    "project": self._project_name,
                    "timestamp": entry.timestamp,
                    "specialist": entry.specialist,
                    "tags": ",".join(entry.tags),
                }],
            )
        except Exception as e:
            log.debug(f"Failed to persist security entry to ChromaDB: {e}")

    # ------------------------------------------------------------------
    # SQLite persistence
    # ------------------------------------------------------------------

    def _init_db(self) -> None:
        """Create the security memory table if it does not exist."""
        import os
        import sqlite3
        try:
            os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """CREATE TABLE IF NOT EXISTS security_memory (
                        id TEXT PRIMARY KEY,
                        data TEXT
                    )"""
                )
        except Exception as e:
            log.warning("Failed to initialize security memory DB: %s", e)

    def _load_entries(self) -> None:
        """Load persisted entries from SQLite into memory."""
        import json
        import sqlite3
        try:
            with sqlite3.connect(self._db_path) as conn:
                rows = conn.execute(
                    "SELECT id, data FROM security_memory"
                ).fetchall()
            for _eid, data in rows:
                try:
                    d = json.loads(data)
                    # Coerce string-serialized enums back to their enum types
                    d["entry_type"] = MemoryEntryType(d.get("entry_type", MemoryEntryType.POLICY_VIOLATION.value))
                    d["risk_level"] = RiskLevel(d.get("risk_level", RiskLevel.SAFE.value))
                    d["trust_level"] = TrustLevel(d.get("trust_level", TrustLevel.TRUSTED.value))
                    entry = SecurityMemoryEntry(**d)
                    self._entries[entry.id] = entry
                    self._index_entry(entry)
                except Exception as e:
                    log.warning("Failed to load security memory entry: %s", e)
        except Exception as e:
            log.warning("Failed to load security memory DB: %s", e)

    def _db_upsert(self, entry: SecurityMemoryEntry) -> None:
        """Insert or replace a single entry in SQLite."""
        if not self._db_path:
            return
        import json
        import sqlite3
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO security_memory (id, data) VALUES (?, ?)",
                    (entry.id, json.dumps(self._entry_to_json(entry))),
                )
        except Exception as e:
            log.warning("Failed to persist security memory entry: %s", e)

    def _db_delete(self, entry_id: str) -> None:
        """Delete a single entry from SQLite."""
        if not self._db_path:
            return
        import sqlite3
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    "DELETE FROM security_memory WHERE id = ?", (entry_id,)
                )
        except Exception as e:
            log.warning("Failed to delete security memory entry: %s", e)

    def _db_clear(self) -> None:
        """Delete all entries from SQLite."""
        if not self._db_path:
            return
        import sqlite3
        try:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute("DELETE FROM security_memory")
        except Exception as e:
            log.warning("Failed to clear security memory DB: %s", e)

    @staticmethod
    def _entry_to_json(entry: SecurityMemoryEntry) -> Dict[str, Any]:
        """Serialize an entry with enum values as plain strings."""
        return {
            "id": entry.id,
            "entry_type": entry.entry_type.value,
            "risk_level": entry.risk_level.value,
            "trust_level": entry.trust_level.value,
            "target": entry.target,
            "specialist": entry.specialist,
            "tool_name": entry.tool_name,
            "reason": entry.reason,
            "evidence": entry.evidence,
            "importance": entry.importance,
            "recurrence_count": entry.recurrence_count,
            "timestamp": entry.timestamp,
            "last_seen": entry.last_seen,
            "tags": entry.tags,
        }

    def _index_entry(self, entry: SecurityMemoryEntry) -> None:
        """Add an entry to the lookup indexes (shared by add + load)."""
        self._by_type.setdefault(entry.entry_type.value, set()).add(entry.id)
        self._by_risk.setdefault(entry.risk_level.value, set()).add(entry.id)
        target_key = entry.target[:50]
        self._by_target.setdefault(target_key, set()).add(entry.id)
        if entry.specialist:
            self._by_specialist.setdefault(entry.specialist, set()).add(entry.id)
