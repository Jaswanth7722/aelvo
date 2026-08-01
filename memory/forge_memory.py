# forge_memory.py - FORGE-scoped memory read/write discipline for AELVO OMEGA
"""
ForgeMemory wraps the shared aelvo_memory_{project} ChromaDB collection with FORGE-typed
queries and strict resolve_conflict discipline on every write.
Uses the shared collection with type metadata filtering (robust over per-collection messy).
All writes: resolve_conflict â†’ ChromaDB â†’ SQLite dual-sync. SQLite failure rolls back ChromaDB.
"""

import hashlib
import logging
import time
import threading
from typing import Any, Dict, List, Optional

from config.settings import (
    FORGE_NOISE_FLOOR,
    IMPORTANCE_CODE_PATTERN,
    IMPORTANCE_CONVENTION,
    IMPORTANCE_ERROR_RECOVERY,
    IMPORTANCE_REVIEW_PATTERN,
    CONFLICT_SIMILARITY_DUPLICATE,
    CONFLICT_SIMILARITY_OVERRIDE,
)
from memory import MEMORY_TYPE_CODE_PATTERN, MEMORY_TYPE_ERROR_RECOVERY

log = logging.getLogger("aelvo.forge.memory")

MEMORY_TYPE_CONVENTION = "convention"
MEMORY_TYPE_REVIEW_PATTERN = "review_pattern"


class ForgeMemory:
    """FORGE memory operations over the shared project collection.
    
    Every write follows the discipline:
    1. resolve_conflict() first.
    2. >= 0.95 similarity: skip (boost existing).
    3. >= 0.85 similarity: delete stale, proceed with fresh.
    4. ChromaDB add with all 4 required fields (type, importance, timestamp_unix, usage_count).
    5. SQLite dual-sync.
    6. SQLite failure: delete ChromaDB entry immediately to restore sync.
    """

    def __init__(self, memory_engine, project_name: str):
        self.memory_engine = memory_engine
        self.project = project_name
        # Use the shared collection â€” robust, no per-specialist collection mess
        self.collection = memory_engine.memory_collection
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # READ â€” Typed queries above noise floor, with usage boost
    # ------------------------------------------------------------------

    def query_patterns(self, task: str, n_results: int = 5) -> List[Dict[str, Any]]:
        """Return code_pattern entries relevant to task."""
        return self._query_typed(task, MEMORY_TYPE_CODE_PATTERN, n_results)

    def query_by_type(self, task: str, memory_type: str, n_results: int = 5) -> List[Dict[str, Any]]:
        """Return entries of any given type relevant to task."""
        return self._query_typed(task, memory_type, n_results)

    def _query_typed(self, task: str, mem_type: str, n: int) -> List[Dict[str, Any]]:
        with self._lock:
            try:
                res = self.collection.query(
                    query_texts=[task],
                    n_results=n,
                    where={"type": mem_type, "project": self.project},
                    include=["documents", "metadatas", "distances"],
                )
            except Exception as e:
                log.debug("ForgeMemory query(%s) failed: %s", mem_type, e)
                return []

            hits: List[Dict[str, Any]] = []
            if not (res.get("ids") and res["ids"][0]):
                return hits

            ids = res["ids"][0]
            docs = res["documents"][0]
            metas = res["metadatas"][0]
            dists = res["distances"][0]

            for entry_id, doc, meta, dist in zip(ids, docs, metas, dists):
                score = round(max(0.0, 1.0 - float(dist)), 4)
                if score < FORGE_NOISE_FLOOR:
                    continue
                hits.append({"doc": doc, "score": score, "meta": meta})

            return hits

    # ------------------------------------------------------------------
    # WRITE â€” Full discipline on every write
    # ------------------------------------------------------------------

    def save_code_pattern(
        self,
        description: str,
        file_path: str = "",
        language: str = "",
        pattern_type: str = "function",
        signature: str = "",
        context: str = "",
    ) -> bool:
        content = (
            f"{description}\n"
            f"[{pattern_type}] {language} {file_path}\n"
            f"Signature: {signature}\n"
            f"Context: {context[:400]}"
        ).strip()
        return self._write_entry(
            content=content,
            entry_type=MEMORY_TYPE_CODE_PATTERN,
            importance=IMPORTANCE_CODE_PATTERN,
            metadata={
                "file_path": file_path,
                "language": language,
                "pattern_type": pattern_type,
                "signature": signature[:200],
            },
        )

    def save_error_recovery(
        self,
        error_signature: str,
        fix_description: str,
        file_path: str = "",
        language: str = "",
        error_output: str = "",
        fix_applied: str = "",
    ) -> bool:
        content = (
            f"{fix_description}\n"
            f"ERROR_RECOVERY {language} {file_path}\n"
            f"Signature: {error_signature}\n"
            f"Error: {error_output[:300]}\n"
            f"Applied: {fix_applied[:300]}"
        ).strip()
        return self._write_entry(
            content=content,
            entry_type=MEMORY_TYPE_ERROR_RECOVERY,
            importance=IMPORTANCE_ERROR_RECOVERY,
            metadata={
                "error_signature": error_signature[:200],
                "file_path": file_path,
                "language": language,
                "fix_applied": fix_applied[:200],
            },
        )

    def save_convention(
        self, convention_description: str, source_file: str = "", language: str = ""
    ) -> bool:
        content = (
            f"{convention_description}\n"
            f"CONVENTION {language} {source_file}"
        ).strip()
        return self._write_entry(
            content=content,
            entry_type=MEMORY_TYPE_CONVENTION,
            importance=IMPORTANCE_CONVENTION,
            metadata={"source_file": source_file, "language": language},
        )

    def save_review_pattern(
        self,
        description: str,
        reviewer: str = "",
        file_path: str = "",
        vulnerability_type: str = "",
        severity: str = "medium",
        finding: str = "",
        resolution: str = "",
        was_approved: bool = False,
    ) -> bool:
        """Save a security review pattern from SENTINEL or FORGE review activity.

        Captures common review findings, approval patterns, and security
        vulnerabilities discovered during code review. Used to predict
        review outcomes and pre-emptively fix common issues.

        Args:
            description: Summary of the review pattern.
            reviewer: Which specialist performed the review (e.g., "SENTINEL").
            file_path: The file(s) that were reviewed.
            vulnerability_type: Type of vulnerability found (if any).
            severity: "critical", "high", "medium", "low", or "none".
            finding: Detailed finding description.
            resolution: How the finding was resolved.
            was_approved: Whether the review was ultimately approved.

        Returns:
            True if a new entry was persisted.
        """
        content = (
            f"{description}\n"
            f"REVIEW_PATTERN {severity} {file_path}\n"
            f"Reviewer: {reviewer}\n"
            f"Vulnerability: {vulnerability_type}\n"
            f"Finding: {finding[:300]}\n"
            f"Resolution: {resolution[:300]}\n"
            f"Approved: {was_approved}"
        ).strip()
        return self._write_entry(
            content=content,
            entry_type=MEMORY_TYPE_REVIEW_PATTERN,
            importance=IMPORTANCE_REVIEW_PATTERN,
            metadata={
                "reviewer": reviewer,
                "file_path": file_path,
                "vulnerability_type": vulnerability_type,
                "severity": severity,
                "was_approved": int(was_approved),
            },
        )

    def _write_entry(
        self,
        content: str,
        entry_type: str,
        importance: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Core write discipline. Returns True if a new entry was persisted.
        """
        if not content or not content.strip():
            return False

        content = content.strip()

        with self._lock:
            # Step 1: resolve_conflict
            if self._resolve_conflict(content, entry_type):
                return False  # Duplicate or merged — no new write needed

            # Step 2: Generate entry ID
            entry_id = hashlib.sha256(
                f"{entry_type}_{self.project}_{time.time()}_{content[:60]}".encode("utf-8")
            ).hexdigest()

            # Step 3: Build metadata with all 4 required fields
            now = time.time()
            full_meta: Dict[str, Any] = {
                "type": entry_type,
                "importance": float(importance),
                "timestamp_unix": now,
                "usage_count": 1,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "project": self.project,
                "source_specialist": "forge",
            }
            if metadata:
                for k, v in metadata.items():
                    if isinstance(v, (str, int, float, bool)):
                        full_meta[k] = v
                    else:
                        full_meta[k] = str(v)

            # Step 4: ChromaDB write
            try:
                self.collection.add(
                    ids=[entry_id],
                    documents=[content],
                    metadatas=[full_meta],
                )
            except Exception as exc:
                log.error("ChromaDB write failed (%s): %s", entry_type, exc)
                return False

            # Step 5: SQLite dual-sync
            try:
                with self.memory_engine.db:
                    self.memory_engine.db.execute(
                        "INSERT INTO retained_memory (content) VALUES (?)",
                        (f"[FORGE:{entry_type}|{self.project}] {content[:800]}",),
                    )
            except Exception as exc:
                # Step 6: SQLite failed — rollback ChromaDB to restore sync
                log.error("SQLite dual-sync failed (%s), rolling back ChromaDB: %s", entry_type, exc)
                try:
                    self.collection.delete(ids=[entry_id])
                except Exception as _ex: log.warning("Silenced exception: %s", _ex)
                return False

            log.info("✓ ForgeMemory saved %s (project=%s)", entry_type, self.project)
            return True

    def _resolve_conflict(self, content: str, entry_type: str) -> bool:
        """
        Returns True if write should be skipped (conflict handled).
        Mirrors rag.py resolve_conflict logic exactly.
        """
        with self._lock:
            try:
                results = self.collection.query(
                    query_texts=[content],
                    n_results=1,
                    where={"type": entry_type, "project": self.project},
                    include=["documents", "metadatas", "distances"],
                )
                if not (results.get("ids") and results["ids"][0]):
                    return False

                dist = results["distances"][0][0]
                similarity = max(0.0, 1.0 - float(dist))
                existing_id = results["ids"][0][0]

                if similarity >= CONFLICT_SIMILARITY_DUPLICATE:
                    # Exact duplicate: boost and skip
                    try:
                        meta = dict(results["metadatas"][0][0])
                        meta["usage_count"] = int(meta.get("usage_count", 0)) + 1
                        meta["importance"] = min(1.0, float(meta.get("importance", 0.5)) + 0.05)
                        self.collection.update(ids=[existing_id], metadatas=[meta])
                    except Exception as _ex: log.warning("Silenced exception: %s", _ex)
                    log.debug("resolve_conflict: duplicate %s (sim=%.3f) — skipped", entry_type, similarity)
                    return True  # Skip

                if similarity >= CONFLICT_SIMILARITY_OVERRIDE:
                    # Stale: prune before fresh insert
                    try:
                        self.collection.delete(ids=[existing_id])
                    except Exception as _ex: log.warning("Silenced exception: %s", _ex)
                    log.debug("resolve_conflict: pruned stale %s (sim=%.3f)", entry_type, similarity)
                    return False  # Proceed with fresh insert

            except Exception as e:
                log.debug("resolve_conflict query error: %s", e)

            return False  # Proceed with insert
