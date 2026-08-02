# learning/consensus_memory.py - ConsensusMemory
# Phase 10: Persists consensus outcomes to ChromaDB + SQLite dual-sync

from __future__ import annotations

import hashlib
import logging
import time
import threading
from typing import Any, Dict, List, Optional

from config.settings import (
    IMPORTANCE_CONSENSUS_RECORD,
    CONFLICT_SIMILARITY_DUPLICATE,
    CONFLICT_SIMILARITY_OVERRIDE,
)
from memory import MEMORY_TYPE_CONSENSUS_RECORD
from learning.types import (
    ConsensusMemoryRecord,
    ConsensusOutcome,
)

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from learning.collaboration_accumulator import CollaborationAccumulator

log = logging.getLogger("aelvo.learning.consensus_memory")


class ConsensusMemory:
    """Persists consensus outcomes to the shared ChromaDB + SQLite memory substrate.

    Every write follows the discipline:
    1. resolve_conflict() first.
    2. >= 0.95 similarity: skip (boost existing).
    3. >= 0.85 similarity: delete stale, proceed with fresh.
    4. ChromaDB add with required metadata fields.
    5. SQLite dual-sync.
    6. SQLite failure: delete ChromaDB entry immediately to restore sync.

    This mirrors the ForgeMemory write discipline exactly, ensuring consistency
    across all memory types in the system.
    """

    def __init__(self, memory_engine, project_name: str = "default"):
        self.memory_engine = memory_engine
        self.project = project_name
        self.collection = memory_engine.memory_collection
        self._lock = threading.RLock()
        self._collaboration_accumulator: Optional[CollaborationAccumulator] = None

    def set_collaboration_accumulator(self, accumulator: Any) -> None:
        """Wire a CollaborationAccumulator to receive consensus data automatically.

        When set, every successful save_consensus() call will also feed the
        consensus record into the CollaborationAccumulator for pattern learning.
        """
        self._collaboration_accumulator = accumulator

    # ------------------------------------------------------------------
    # WRITE — Full discipline
    # ------------------------------------------------------------------

    def save_consensus(
        self,
        consensus: ConsensusMemoryRecord,
    ) -> bool:
        """Save a consensus outcome to ChromaDB + SQLite dual-sync.

        If a CollaborationAccumulator is wired via set_collaboration_accumulator(),
        the consensus record is automatically fed into it for pattern learning.

        Args:
            consensus: The ConsensusMemoryRecord to persist.

        Returns:
            True if a new entry was persisted, False if duplicate or skipped.
        """
        content = consensus.build_content()
        if not content.strip():
            return False

        with self._lock:
            # Step 1: resolve_conflict
            if self._resolve_conflict(content):
                return False

            # Step 2: Generate entry ID
            entry_id = consensus.id or hashlib.sha256(
                f"consensus_{self.project}_{time.time()}_{consensus.consensus_id}".encode("utf-8")
            ).hexdigest()

            # Step 3: Build metadata
            now = time.time()
            meta: Dict[str, Any] = {
                "type": MEMORY_TYPE_CONSENSUS_RECORD,
                "importance": IMPORTANCE_CONSENSUS_RECORD,
                "timestamp_unix": now,
                "usage_count": 1,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "project": self.project,
                "source_specialist": "consensus",
                "consensus_id": consensus.consensus_id,
                "topic": consensus.topic[:200],
                "outcome": consensus.outcome.value,
                "confidence": consensus.confidence,
                "participant_count": consensus.participant_count,
                "vetoed": int(consensus.vetoed),
                "session_id": consensus.session_id,
            }

            # Step 4: ChromaDB write
            try:
                self.collection.add(
                    ids=[entry_id],
                    documents=[content],
                    metadatas=[meta],
                )
            except Exception as exc:
                log.error("ChromaDB write failed (consensus_record): %s", exc)
                return False

            # Step 5: SQLite dual-sync
            try:
                with self.memory_engine.db:
                    self.memory_engine.db.execute(
                        "INSERT INTO retained_memory (content) VALUES (?)",
                        (f"[CONSENSUS:{consensus.outcome.value}|{self.project}] {content[:800]}",),
                    )
            except Exception as exc:
                # Step 6: SQLite failed — rollback ChromaDB
                log.error("SQLite dual-sync failed (consensus_record), rolling back ChromaDB: %s", exc)
                try:
                    self.collection.delete(ids=[entry_id])
                except Exception as _ex:
                    log.warning("Silenced exception: %s", _ex)
                return False

            # Step 7: Feed into CollaborationAccumulator if wired
            if self._collaboration_accumulator is not None:
                try:
                    self._collaboration_accumulator.ingest_consensus(consensus)
                except Exception as ca_err:
                    log.warning(
                        "Failed to feed consensus to CollaborationAccumulator: %s", ca_err
                    )

            log.info(
                "✓ ConsensusMemory saved %s (outcome=%s, topic=%s)",
                consensus.consensus_id[:8],
                consensus.outcome.value,
                consensus.topic[:40],
            )
            return True

    # ------------------------------------------------------------------
    # READ — Typed queries
    # ------------------------------------------------------------------

    def query_consensus(
        self,
        topic: str = "",
        n_results: int = 5,
        outcome: Optional[ConsensusOutcome] = None,
    ) -> List[Dict[str, Any]]:
        """Query consensus records from ChromaDB.

        Args:
            topic: Text query for semantic search.
            n_results: Max results to return.
            outcome: Optional filter by consensus outcome.

        Returns:
            List of dicts with 'content', 'meta', 'score' keys.
        """
        with self._lock:
            query_text = topic or "consensus"
            where: Dict[str, Any] = {
                "type": MEMORY_TYPE_CONSENSUS_RECORD,
                "project": self.project,
            }
            if outcome:
                where["outcome"] = outcome.value

            try:
                res = self.collection.query(
                    query_texts=[query_text],
                    n_results=n_results,
                    where=where,
                    include=["documents", "metadatas", "distances"],
                )
            except Exception as e:
                log.debug("ConsensusMemory query failed: %s", e)
                return []

            hits: List[Dict[str, Any]] = []
            if not (res.get("ids") and res["ids"][0]):
                return hits

            for entry_id, doc, meta, dist in zip(
                res["ids"][0], res["documents"][0],
                res["metadatas"][0], res["distances"][0],
            ):
                score = round(max(0.0, 1.0 - float(dist)), 4)
                hits.append({
                    "id": entry_id,
                    "content": doc,
                    "meta": dict(meta),
                    "score": score,
                })

            return hits

    # ------------------------------------------------------------------
    # List recent consensus records from SQLite
    # ------------------------------------------------------------------

    def list_recent(self, limit: int = 20) -> List[Dict[str, Any]]:
        """List recent consensus records from SQLite retained_memory.

        Args:
            limit: Max records to return.

        Returns:
            List of dicts with 'content' and 'timestamp' keys.
        """
        try:
            cursor = self.memory_engine.db.cursor()
            cursor.execute(
                "SELECT content, timestamp FROM retained_memory "
                "WHERE content LIKE '[CONSENSUS:%' "
                "ORDER BY timestamp DESC LIMIT ?",
                (limit,),
            )
            return [
                {"content": row[0], "timestamp": row[1]}
                for row in cursor.fetchall()
            ]
        except Exception as e:
            log.debug("ConsensusMemory list_recent failed: %s", e)
            return []

    # ------------------------------------------------------------------
    # Conflict Resolution
    # ------------------------------------------------------------------

    def _resolve_conflict(self, content: str) -> bool:
        """Returns True if write should be skipped (conflict handled)."""
        with self._lock:
            try:
                results = self.collection.query(
                    query_texts=[content],
                    n_results=1,
                    where={"$and": [{"type": MEMORY_TYPE_CONSENSUS_RECORD}, {"project": self.project}]},
                    include=["documents", "metadatas", "distances"],
                )
                if not (results.get("ids") and results["ids"][0]):
                    return False

                dist = results["distances"][0][0]
                similarity = max(0.0, 1.0 - float(dist))
                existing_id = results["ids"][0][0]

                if similarity >= CONFLICT_SIMILARITY_DUPLICATE:
                    # Boost and skip
                    try:
                        meta = dict(results["metadatas"][0][0])
                        meta["usage_count"] = int(meta.get("usage_count", 0)) + 1
                        meta["importance"] = min(1.0, float(meta.get("importance", 0.5)) + 0.05)
                        self.collection.update(ids=[existing_id], metadatas=[meta])
                    except Exception as _ex:
                        log.warning("Silenced exception: %s", _ex)
                    log.debug("resolve_conflict: duplicate consensus_record (sim=%.3f) — skipped", similarity)
                    return True

                if similarity >= CONFLICT_SIMILARITY_OVERRIDE:
                    # Stale: prune before fresh insert
                    try:
                        self.collection.delete(ids=[existing_id])
                    except Exception as _ex:
                        log.warning("Silenced exception: %s", _ex)
                    log.debug("resolve_conflict: pruned stale consensus_record (sim=%.3f)", similarity)
                    return False

            except Exception as e:
                log.debug("resolve_conflict query error: %s", e)

            return False
