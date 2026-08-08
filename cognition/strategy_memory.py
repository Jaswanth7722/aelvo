from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set

from cognition.types import (
    MemoryType, StrategicMemoryEntry, ConsolidationRecord,
)

log = logging.getLogger("aelvo.cognition.strategy_memory")

# Default thresholds for autonomous learning
_AUTO_CONSOLIDATION_THRESHOLD = 0.7   # Similarity score above which entries auto-consolidate
_STALE_DAYS_THRESHOLD = 30             # Days after which unused entries decay
_DECAY_AMOUNT_PER_PASS = 0.05          # Importance reduction per decay pass
_MIN_AUTO_IMPORTANCE = 0.15            # Floor below which auto-stored entries won't go
_SUCCESS_BOOST = 0.08                  # Importance boost on successful outcome
_FAILURE_PENALTY = 0.06                # Importance penalty on failure outcome


class StrategicMemory:
    """Strategic Memory Layer with autonomous learning.

    Manages seven memory types:
    - SUCCESS_PATTERN: What worked before
    - FAILURE_PATTERN: What failed before
    - REUSABLE_STRATEGY: Generalized strategies
    - CONSTRAINT: Hard constraints learned from execution
    - DOMAIN_KNOWLEDGE: Learned domain knowledge
    - EXECUTION_TRACE: Full execution traces for replay
    - USER_PREFERENCE: Learned user preferences

    Phase 9 enhancements:
    - ``auto_store_from_outcome()`` — automatically extract and store learnings
      from execution results without explicit store() calls
    - ``find_relevant_strategies()`` — retrieve strategies relevant to a new goal
    - ``decay_stale_entries()`` — proactive decay of unused memories
    - ``consolidate_similar_entries()`` — auto-consolidation of related entries
    """

    def __init__(self, forge_memory=None):
        self._forge = forge_memory
        self._entries: Dict[str, StrategicMemoryEntry] = {}
        self._consolidations: List[ConsolidationRecord] = []
        self._type_index: Dict[MemoryType, List[str]] = {mt: [] for mt in MemoryType}

    # ======================================================================
    # Core CRUD (existing)
    # ======================================================================

    def store(
        self,
        memory_type: MemoryType,
        content: str,
        importance: float = 0.5,
        source_goal_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
        embedding: Optional[List[float]] = None,
    ) -> StrategicMemoryEntry:
        entry_id = self._generate_id(memory_type, content)
        entry = StrategicMemoryEntry(
            id=entry_id,
            memory_type=memory_type,
            content=content,
            importance=importance,
            source_goal_id=source_goal_id,
            tags=tags or [],
            embedding=embedding,
        )
        self._entries[entry_id] = entry
        self._type_index[memory_type].append(entry_id)
        self._persist_to_forge(entry)
        log.debug("Stored %s entry %s (importance=%.2f)", memory_type.value, entry_id, importance)
        return entry

    def recall(
        self,
        memory_type: Optional[MemoryType] = None,
        tags: Optional[List[str]] = None,
        min_importance: float = 0.0,
        max_results: int = 10,
    ) -> List[StrategicMemoryEntry]:
        results: List[StrategicMemoryEntry] = []
        for entry in self._entries.values():
            if memory_type is not None and entry.memory_type != memory_type:
                continue
            if entry.importance < min_importance:
                continue
            if tags:
                if not any(t in entry.tags for t in tags):
                    continue
            results.append(entry)
        results.sort(key=lambda e: e.importance, reverse=True)
        for r in results[:max_results]:
            r.last_accessed = datetime.now(timezone.utc)
        return results[:max_results]

    def search(
        self,
        query: str,
        memory_type: Optional[MemoryType] = None,
        max_results: int = 5,
    ) -> List[StrategicMemoryEntry]:
        query_lower = query.lower()
        scored: List[tuple] = []
        for entry in self._entries.values():
            if memory_type is not None and entry.memory_type != memory_type:
                continue
            score = 0.0
            if query_lower in entry.content.lower():
                score += 0.5 * entry.importance
            for tag in entry.tags:
                if query_lower in tag.lower():
                    score += 0.3 * entry.importance
            if score > 0:
                scored.append((score, entry))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [e for _, e in scored[:max_results]]

    def get_by_type(self, memory_type: MemoryType) -> List[StrategicMemoryEntry]:
        entry_ids = self._type_index.get(memory_type, [])
        return [self._entries[eid] for eid in entry_ids if eid in self._entries]

    def consolidate(self, entry_ids: List[str]) -> Optional[StrategicMemoryEntry]:
        source_entries = [e for eid in entry_ids if (e := self._entries.get(eid))]
        if len(source_entries) < 2:
            return None
        content_parts = [e.content for e in source_entries]
        consolidated_content = self._merge_contents(content_parts)
        avg_importance = sum(e.importance for e in source_entries) / len(source_entries)
        best_memory_type = self._infer_consolidated_type(source_entries)

        record = ConsolidationRecord(
            id="consolidation_" + hashlib.sha256(str(entry_ids).encode()).hexdigest()[:12],
            source_entry_ids=entry_ids,
            consolidated_content=consolidated_content,
            memory_type=best_memory_type,
            importance=min(1.0, avg_importance + 0.1),
        )
        self._consolidations.append(record)

        consolidated = self.store(
            memory_type=best_memory_type,
            content=consolidated_content,
            importance=record.importance,
            tags=list({t for e in source_entries for t in e.tags}),
        )
        consolidated.consolidation_count = sum(e.consolidation_count for e in source_entries) + 1
        log.info("Consolidated %d entries into %s (type=%s, importance=%.2f)",
                 len(entry_ids), consolidated.id, best_memory_type.value, record.importance)
        return consolidated

    def boost(self, entry_id: str, amount: float = 0.05) -> bool:
        entry = self._entries.get(entry_id)
        if entry is None:
            return False
        entry.importance = min(1.0, entry.importance + amount)
        entry.last_accessed = datetime.now(timezone.utc)
        return True

    def decay(self, entry_id: str, amount: float = 0.02) -> bool:
        entry = self._entries.get(entry_id)
        if entry is None:
            return False
        entry.importance = max(0.0, entry.importance - amount)
        entry.last_accessed = datetime.now(timezone.utc)
        return True

    def snapshot(self) -> Dict[str, Any]:
        return {
            "total_entries": len(self._entries),
            "by_type": {mt.value: len(ids) for mt, ids in self._type_index.items()},
            "consolidations": len(self._consolidations),
            "avg_importance": round(
                sum(e.importance for e in self._entries.values()) / max(1, len(self._entries)), 4
            ),
        }

    # ======================================================================
    # Phase 9: Autonomous Learning — Auto-Store from Outcomes
    # ======================================================================

    def auto_store_from_outcome(
        self,
        goal_description: str,
        outcome: str,
        specialist: str = "",
        execution_summary: str = "",
        importance: Optional[float] = None,
    ) -> Optional[StrategicMemoryEntry]:
        """Automatically extract and store a learning from an execution outcome.

        Called after every pipeline execution.  No explicit ``store()`` call
        required — the engine calls this autonomously.

        Args:
            goal_description: What the goal/task was.
            outcome: ``"success"`` or ``"failure"`` (or any outcome string).
            specialist: Which specialist handled the execution.
            execution_summary: Brief summary of what happened.
            importance: Override importance.  Auto-calculated if None.

        Returns:
            The stored StrategicMemoryEntry, or None if no learning signal.
        """
        if not goal_description or not outcome:
            return None

        # Determine memory type from outcome
        if outcome.lower() in ("success", "completed", "passed"):
            memory_type = MemoryType.SUCCESS_PATTERN
            auto_importance = importance or _SUCCESS_BOOST + 0.3
            content = (
                f"Successful execution: {goal_description[:200]}\n"
                f"Specialist: {specialist}\n"
                f"Summary: {execution_summary[:300]}"
            )
        elif outcome.lower() in ("failure", "failed", "error", "blocked"):
            memory_type = MemoryType.FAILURE_PATTERN
            auto_importance = importance or _FAILURE_PENALTY + 0.5
            content = (
                f"Failed execution: {goal_description[:200]}\n"
                f"Specialist: {specialist}\n"
                f"Summary of failure: {execution_summary[:300]}"
            )
        else:
            memory_type = MemoryType.REUSABLE_STRATEGY
            auto_importance = importance or 0.3
            content = (
                f"Execution result ({outcome}): {goal_description[:200]}\n"
                f"Specialist: {specialist}\n"
                f"Summary: {execution_summary[:300]}"
            )

        auto_importance = max(_MIN_AUTO_IMPORTANCE, min(1.0, auto_importance))
        tags = ["auto-learned", outcome.lower(), specialist.lower()] if specialist else ["auto-learned", outcome.lower()]

        entry = self.store(
            memory_type=memory_type,
            content=content,
            importance=auto_importance,
            tags=tags,
        )
        log.info(
            "Auto-stored %s from outcome '%s' (importance=%.2f, specialist=%s)",
            memory_type.value, outcome, auto_importance, specialist,
        )
        return entry

    def find_relevant_strategies(
        self,
        goal_description: str,
        max_results: int = 5,
        min_importance: float = 0.2,
    ) -> List[StrategicMemoryEntry]:
        """Retrieve strategies relevant to a new goal.

        Searches across SUCCESS_PATTERN, FAILURE_PATTERN, and
        REUSABLE_STRATEGY types for entries related to the goal
        description.  Designed to be called at planning time to
        inject learned strategies into the plan.

        Args:
            goal_description: The goal/plan description to find matches for.
            max_results: Maximum strategies to return.
            min_importance: Minimum importance threshold.

        Returns:
            List of relevant StrategicMemoryEntry instances, ranked by
            relevance score.
        """
        relevant_types = {
            MemoryType.SUCCESS_PATTERN,
            MemoryType.FAILURE_PATTERN,
            MemoryType.REUSABLE_STRATEGY,
            MemoryType.DOMAIN_KNOWLEDGE,
        }
        query_lower = goal_description.lower()

        scored: List[tuple] = []
        for entry in self._entries.values():
            if entry.memory_type not in relevant_types:
                continue
            if entry.importance < min_importance:
                continue

            score = 0.0
            content_lower = entry.content.lower()

            # Keyword overlap
            goal_words = set(query_lower.split())
            content_words = set(content_lower.split())
            overlap = goal_words & content_words
            if overlap:
                score += 0.3 * (len(overlap) / max(1, len(goal_words)))

            # Exact phrase match
            if query_lower in content_lower:
                score += 0.4 * entry.importance

            # Tag match
            for tag in entry.tags:
                if tag.lower() in query_lower:
                    score += 0.2 * entry.importance

            # Boost for high-importance entries
            score *= (0.5 + 0.5 * entry.importance)

            if score > 0:
                scored.append((score, entry))

        scored.sort(key=lambda x: x[0], reverse=True)
        results = [e for _, e in scored[:max_results]]

        # Update last_accessed
        for r in results:
            r.last_accessed = datetime.now(timezone.utc)

        if results:
            log.debug("Found %d relevant strategies for goal '%s'", len(results), goal_description[:60])

        return results

    def decay_stale_entries(
        self,
        stale_days: int = _STALE_DAYS_THRESHOLD,
        decay_amount: float = _DECAY_AMOUNT_PER_PASS,
    ) -> int:
        """Proactively decay unused or stale memory entries.

        Scans all entries and reduces importance for those not accessed
        within ``stale_days``.  Entries that fall below 0.1 importance
        are pruned entirely.

        Should be called periodically (e.g., at session end, or every N
        turns) to prevent memory bloat.

        Args:
            stale_days: Days of inactivity before decay applies.
            decay_amount: Importance reduction per decay pass.

        Returns:
            Number of entries decayed (negative = net removal).
        """
        # Use naive UTC datetime to match StrategicMemoryEntry defaults (datetime.now(timezone.utc))
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff = now - timedelta(days=stale_days)
        decayed = 0
        pruned: List[str] = []

        for entry_id, entry in list(self._entries.items()):
            # Normalize entry.last_accessed to naive if it's timezone-aware
            last_accessed = entry.last_accessed
            if last_accessed.tzinfo is not None:
                last_accessed = last_accessed.replace(tzinfo=None)
            if last_accessed < cutoff and entry.importance > 0.1:
                new_importance = max(0.0, entry.importance - decay_amount)
                entry.importance = new_importance
                decayed += 1
                log.debug("Decayed entry %s (importance: %.2f -> %.2f)", entry_id[:8], entry.importance + decay_amount, new_importance)

            if entry.importance < 0.1:
                pruned.append(entry_id)

        # Prune
        for entry_id in pruned:
            entry = self._entries.pop(entry_id, None)
            if entry:
                type_list = self._type_index.get(entry.memory_type, [])
                if entry_id in type_list:
                    type_list.remove(entry_id)
                log.info("Pruned stale entry %s (type=%s)", entry_id[:8], entry.memory_type.value)

        if pruned:
            log.info("Decay pass complete: %d entries decayed, %d pruned", decayed, len(pruned))
        elif decayed > 0:
            log.debug("Decay pass: %d entries decayed, none pruned", decayed)

        return decayed - len(pruned)  # negative = net removal

    def consolidate_similar_entries(
        self,
        similarity_threshold: float = _AUTO_CONSOLIDATION_THRESHOLD,
        max_consolidations: int = 3,
    ) -> int:
        """Auto-consolidate similar memory entries.

        Scans for entries with the same ``memory_type`` and overlapping
        content.  Related entries are consolidated into a single entry
        with merged content and averaged importance.

        Args:
            similarity_threshold: Content overlap ratio to trigger consolidation.
            max_consolidations: Maximum consolidations per pass.

        Returns:
            Number of consolidations performed.
        """
        consolidations_done = 0

        # Group entries by type
        for mem_type in MemoryType:
            entries = self.get_by_type(mem_type)
            if len(entries) < 2:
                continue

            # Find similar pairs
            for i in range(len(entries)):
                if consolidations_done >= max_consolidations:
                    break
                for j in range(i + 1, len(entries)):
                    if consolidations_done >= max_consolidations:
                        break

                    a, b = entries[i], entries[j]
                    similarity = self._compute_content_similarity(a.content, b.content)

                    if similarity >= similarity_threshold:
                        # Consolidate into the higher-importance entry
                        entry_ids = [a.id, b.id]
                        consolidated = self.consolidate(entry_ids)
                        if consolidated:
                            consolidations_done += 1
                            # Remove the two source entries
                            for eid in entry_ids:
                                entry = self._entries.pop(eid, None)
                                if entry:
                                    type_list = self._type_index.get(entry.memory_type, [])
                                    if eid in type_list:
                                        type_list.remove(eid)

        if consolidations_done:
            log.info("Auto-consolidation: %d consolidations performed", consolidations_done)

        return consolidations_done

    # ======================================================================
    # Internal Helpers
    # ======================================================================

    def _persist_to_forge(self, entry: StrategicMemoryEntry) -> None:
        if self._forge is None:
            return
        try:
            memory_type_map = {
                MemoryType.SUCCESS_PATTERN: "success_pattern",
                MemoryType.FAILURE_PATTERN: "failure_pattern",
                MemoryType.REUSABLE_STRATEGY: "reusable_strategy",
                MemoryType.CONSTRAINT: "constraint",
                MemoryType.DOMAIN_KNOWLEDGE: "domain_knowledge",
                MemoryType.EXECUTION_TRACE: "execution_trace",
                MemoryType.USER_PREFERENCE: "user_preference",
                MemoryType.STRATEGIC_PLAN: "strategic_plan",
                MemoryType.ROADMAP: "roadmap",
            }
            forge_type = memory_type_map.get(entry.memory_type, "cognitive_memory")
            self._forge.save_code_pattern(
                description=entry.content,
                pattern_type=forge_type,
                context=f"source_goal={entry.source_goal_id or 'none'},tags={','.join(entry.tags)}",
            )
        except Exception as e:
            log.warning("Forge memory persist failed: %s", e)

    def _generate_id(self, mem_type: MemoryType, content: str) -> str:
        raw = f"{mem_type.value}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def _merge_contents(parts: List[str]) -> str:
        seen: Set[str] = set()
        merged: List[str] = []
        for part in parts:
            normalized = " ".join(part.split())
            if normalized not in seen:
                seen.add(normalized)
                merged.append(normalized)
        return "\n---\n".join(merged)

    @staticmethod
    def _infer_consolidated_type(entries: List[StrategicMemoryEntry]) -> MemoryType:
        type_counts: Dict[MemoryType, int] = {}
        for e in entries:
            type_counts[e.memory_type] = type_counts.get(e.memory_type, 0) + 1
        return max(type_counts, key=type_counts.get)

    @staticmethod
    def _compute_content_similarity(a: str, b: str) -> float:
        """Compute simple content overlap similarity (0.0-1.0)."""
        words_a = set(a.lower().split())
        words_b = set(b.lower().split())
        if not words_a or not words_b:
            return 0.0
        intersection = words_a & words_b
        union = words_a | words_b
        return len(intersection) / max(1, len(union))
