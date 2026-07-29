# learning/specialist_effectiveness.py - SpecialistEffectivenessTracker
# Phase 10: Tracks per-specialist effectiveness metrics in SQLite

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from learning.types import SpecialistEffectivenessRecord

log = logging.getLogger("aelvo.learning.effectiveness")


class SpecialistEffectivenessTracker:
    """Tracks per-specialist effectiveness metrics in a dedicated SQLite table.

    Records quantitative metrics for each specialist per session:
    - Tasks attempted / succeeded
    - First-attempt success rate
    - Consensus participation and alignment
    - Patterns contributed
    - Blackboard publications

    Enables queries like:
    - "Which specialist has the highest first-attempt success rate?"
    - "How does SENTINEL's consensus alignment compare to FORGE's?"
    - "Is FORGE getting more effective over time?"
    """

    def __init__(self, db_path: str = ":memory:"):
        self._db_path = db_path
        self._local = threading.local()
        self._lock = threading.RLock()
        self._metrics: List[Dict] = []
        self._init_schema()

    # ── Connection Management ─────────────────────────────────────────────

    @property
    def _conn(self) -> sqlite3.Connection:
        """Get thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self._db_path)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def _init_schema(self) -> None:
        """Create the specialist_effectiveness table."""
        conn = self._conn
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS specialist_effectiveness (
                id TEXT PRIMARY KEY,
                specialist TEXT NOT NULL,
                session_id TEXT NOT NULL,
                tasks_attempted INTEGER DEFAULT 0,
                tasks_succeeded INTEGER DEFAULT 0,
                first_attempts INTEGER DEFAULT 0,
                first_attempt_successes INTEGER DEFAULT 0,
                total_duration_ms REAL DEFAULT 0.0,
                consensus_participations INTEGER DEFAULT 0,
                consensus_aligned INTEGER DEFAULT 0,
                patterns_contributed INTEGER DEFAULT 0,
                blackboard_publications INTEGER DEFAULT 0,
                last_updated TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_eff_specialist
                ON specialist_effectiveness(specialist);
            CREATE INDEX IF NOT EXISTS idx_eff_session
                ON specialist_effectiveness(session_id);
            CREATE INDEX IF NOT EXISTS idx_eff_specialist_session
                ON specialist_effectiveness(specialist, session_id);

            CREATE TABLE IF NOT EXISTS specialist_effectiveness_sessions (
                session_id TEXT PRIMARY KEY,
                specialist TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                summary_json TEXT DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_eff_sessions_spec
                ON specialist_effectiveness_sessions(specialist);
        """)
        conn.commit()

    # ── Record Management ─────────────────────────────────────────────────

    def record_task_outcome(
        self,
        specialist: str,
        session_id: str,
        succeeded: bool,
        duration_ms: float = 0.0,
        was_first_attempt: bool = False,
    ) -> SpecialistEffectivenessRecord:
        """Record a task outcome for a specialist.

        Args:
            specialist: The specialist name (e.g., "FORGE", "SENTINEL").
            session_id: The session ID.
            succeeded: Whether the task succeeded.
            duration_ms: Task execution duration.
            was_first_attempt: Whether this was the specialist's first attempt.

        Returns:
            The updated SpecialistEffectivenessRecord.
        """
        with self._lock:
            record = self._get_or_create(specialist, session_id)
            record.tasks_attempted += 1
            if succeeded:
                record.tasks_succeeded += 1
            if was_first_attempt:
                record.first_attempts += 1
                if succeeded:
                    record.first_attempt_successes += 1
            record.total_duration_ms += duration_ms
            record.last_updated = datetime.now(timezone.utc)
            self._persist(record)
            return record

    def record_first_attempt(
        self,
        specialist: str,
        session_id: str,
        succeeded: bool,
    ) -> SpecialistEffectivenessRecord:
        """Record a first-attempt outcome.

        This is a convenience wrapper that records a task outcome with
        was_first_attempt=True.

        Args:
            specialist: The specialist name.
            session_id: The session ID.
            succeeded: Whether the first attempt succeeded.

        Returns:
            The updated SpecialistEffectivenessRecord.
        """
        return self.record_task_outcome(
            specialist=specialist,
            session_id=session_id,
            succeeded=succeeded,
            was_first_attempt=True,
        )

    def record_consensus_participation(
        self,
        specialist: str,
        session_id: str,
        aligned: bool,
    ) -> SpecialistEffectivenessRecord:
        """Record a consensus participation for a specialist.

        Args:
            specialist: The specialist name.
            session_id: The session ID.
            aligned: Whether the specialist's position matched the final outcome.

        Returns:
            The updated SpecialistEffectivenessRecord.
        """
        with self._lock:
            record = self._get_or_create(specialist, session_id)
            record.consensus_participations += 1
            if aligned:
                record.consensus_aligned += 1
            record.last_updated = datetime.now(timezone.utc)
            self._persist(record)
            return record

    def record_pattern_contributed(
        self,
        specialist: str,
        session_id: str,
    ) -> SpecialistEffectivenessRecord:
        """Record that a specialist contributed a pattern.

        Args:
            specialist: The specialist name.
            session_id: The session ID.

        Returns:
            The updated SpecialistEffectivenessRecord.
        """
        with self._lock:
            record = self._get_or_create(specialist, session_id)
            record.patterns_contributed += 1
            record.last_updated = datetime.now(timezone.utc)
            self._persist(record)
            return record

    def record_blackboard_publication(
        self,
        specialist: str,
        session_id: str,
    ) -> SpecialistEffectivenessRecord:
        """Record a blackboard publication by a specialist.

        Args:
            specialist: The specialist name.
            session_id: The session ID.

        Returns:
            The updated SpecialistEffectivenessRecord.
        """
        with self._lock:
            record = self._get_or_create(specialist, session_id)
            record.blackboard_publications += 1
            record.last_updated = datetime.now(timezone.utc)
            self._persist(record)
            return record

    # ── Query Methods ─────────────────────────────────────────────────────

    def get_record(
        self,
        specialist: str,
        session_id: str,
    ) -> Optional[SpecialistEffectivenessRecord]:
        """Get the effectiveness record for a specialist in a specific session."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM specialist_effectiveness "
                "WHERE specialist = ? AND session_id = ?",
                (specialist, session_id),
            ).fetchone()
            return self._row_to_record(row) if row else None

    def get_specialist_records(
        self,
        specialist: str,
        limit: int = 20,
    ) -> List[SpecialistEffectivenessRecord]:
        """Get all effectiveness records for a specialist across sessions."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM specialist_effectiveness "
                "WHERE specialist = ? "
                "ORDER BY last_updated DESC LIMIT ?",
                (specialist, limit),
            ).fetchall()
            return [
                self._row_to_record(r) for r in rows
                if r is not None
            ]

    def get_aggregate(self, specialist: str) -> Dict[str, Any]:
        """Get aggregate metrics for a specialist across all sessions.

        Args:
            specialist: The specialist name.

        Returns:
            Dict with keys: specialist, total_tasks, total_successes,
            overall_success_rate, total_first_attempts,
            first_attempt_success_rate, total_consensus_participations,
            consensus_alignment_rate, total_patterns_contributed,
            total_blackboard_publications, session_count.
        """
        with self._lock:
            records = self.get_specialist_records(specialist, limit=1000)
            if not records:
                return {
                    "specialist": specialist,
                    "total_tasks": 0,
                    "total_successes": 0,
                    "overall_success_rate": 0.0,
                    "total_first_attempts": 0,
                    "first_attempt_success_rate": 0.0,
                    "total_consensus_participations": 0,
                    "consensus_alignment_rate": 0.0,
                    "total_patterns_contributed": 0,
                    "total_blackboard_publications": 0,
                    "session_count": 0,
                }

            total_tasks = sum(r.tasks_attempted for r in records)
            total_successes = sum(r.tasks_succeeded for r in records)
            total_first = sum(r.first_attempts for r in records)
            total_first_success = sum(r.first_attempt_successes for r in records)
            total_consensus = sum(r.consensus_participations for r in records)
            total_aligned = sum(r.consensus_aligned for r in records)
            total_patterns = sum(r.patterns_contributed for r in records)
            total_pubs = sum(r.blackboard_publications for r in records)

            return {
                "specialist": specialist,
                "total_tasks": total_tasks,
                "total_successes": total_successes,
                "overall_success_rate": round(
                    total_successes / total_tasks, 4
                ) if total_tasks > 0 else 0.0,
                "total_first_attempts": total_first,
                "first_attempt_success_rate": round(
                    total_first_success / total_first, 4
                ) if total_first > 0 else 0.0,
                "total_consensus_participations": total_consensus,
                "consensus_alignment_rate": round(
                    total_aligned / total_consensus, 4
                ) if total_consensus > 0 else 0.0,
                "total_patterns_contributed": total_patterns,
                "total_blackboard_publications": total_pubs,
                "session_count": len(records),
            }

    def get_all_specialist_aggregates(self) -> Dict[str, Dict[str, Any]]:
        """Get aggregate metrics for all specialists."""
        with self._lock:
            specialists = set()
            rows = self._conn.execute(
                "SELECT DISTINCT specialist FROM specialist_effectiveness"
            ).fetchall()
            specialists = {r["specialist"] for r in rows}

            return {
                sp: self.get_aggregate(sp)
                for sp in sorted(specialists)
            }

    def get_top_first_attempt_specialists(
        self, min_attempts: int = 3, limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Get specialists ranked by first-attempt success rate.

        Args:
            min_attempts: Minimum number of first attempts to qualify.
            limit: Max results.

        Returns:
            List of dicts with specialist, rate, attempts, successes.
        """
        with self._lock:
            all_aggs = self.get_all_specialist_aggregates()
            qualified = [
                agg for agg in all_aggs.values()
                if agg["total_first_attempts"] >= min_attempts
            ]
            qualified.sort(
                key=lambda a: a["first_attempt_success_rate"],
                reverse=True,
            )
            return [
                {
                    "specialist": agg["specialist"],
                    "first_attempt_success_rate": agg["first_attempt_success_rate"],
                    "total_first_attempts": agg["total_first_attempts"],
                    "total_first_attempt_successes": agg["total_first_attempts"]
                        * agg["first_attempt_success_rate"],
                }
                for agg in qualified[:limit]
            ]

    # ── Session Lifecycle ─────────────────────────────────────────────────

    def start_session(self, specialist: str, session_id: str) -> None:
        """Mark the start of a session for a specialist."""
        with self._lock:
            now = datetime.now(timezone.utc).isoformat()
            self._conn.execute(
                "INSERT OR IGNORE INTO specialist_effectiveness_sessions "
                "(session_id, specialist, started_at) VALUES (?, ?, ?)",
                (session_id, specialist, now),
            )
            self._conn.commit()

    def end_session(self, specialist: str, session_id: str) -> None:
        """Mark the end of a session for a specialist."""
        with self._lock:
            now = datetime.now(timezone.utc).isoformat()
            self._conn.execute(
                "UPDATE specialist_effectiveness_sessions SET "
                "ended_at = ? WHERE session_id = ? AND specialist = ?",
                (now, session_id, specialist),
            )
            self._conn.commit()

    # ── Internal ──────────────────────────────────────────────────────────

    def _get_or_create(
        self,
        specialist: str,
        session_id: str,
    ) -> SpecialistEffectivenessRecord:
        """Get an existing record or create a new one."""
        row = self._conn.execute(
            "SELECT * FROM specialist_effectiveness "
            "WHERE specialist = ? AND session_id = ?",
            (specialist, session_id),
        ).fetchone()

        if row:
            return self._row_to_record(row)

        record = SpecialistEffectivenessRecord(
            specialist=specialist,
            session_id=session_id,
        )
        record.to_id()
        return record

    def _persist(self, record: SpecialistEffectivenessRecord) -> None:
        """Insert or update a record in the database."""
        start = time.time()
        conn = self._conn

        existing = conn.execute(
            "SELECT id FROM specialist_effectiveness WHERE id = ?",
            (record.id,),
        ).fetchone()

        if existing:
            conn.execute("""
                UPDATE specialist_effectiveness SET
                    tasks_attempted = ?, tasks_succeeded = ?,
                    first_attempts = ?, first_attempt_successes = ?,
                    total_duration_ms = ?,
                    consensus_participations = ?, consensus_aligned = ?,
                    patterns_contributed = ?, blackboard_publications = ?,
                    last_updated = ?
                WHERE id = ?
            """, (
                record.tasks_attempted, record.tasks_succeeded,
                record.first_attempts, record.first_attempt_successes,
                record.total_duration_ms,
                record.consensus_participations, record.consensus_aligned,
                record.patterns_contributed, record.blackboard_publications,
                record.last_updated.isoformat(),
                record.id,
            ))
        else:
            conn.execute("""
                INSERT INTO specialist_effectiveness (
                    id, specialist, session_id,
                    tasks_attempted, tasks_succeeded,
                    first_attempts, first_attempt_successes,
                    total_duration_ms,
                    consensus_participations, consensus_aligned,
                    patterns_contributed, blackboard_publications,
                    last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.id, record.specialist, record.session_id,
                record.tasks_attempted, record.tasks_succeeded,
                record.first_attempts, record.first_attempt_successes,
                record.total_duration_ms,
                record.consensus_participations, record.consensus_aligned,
                record.patterns_contributed, record.blackboard_publications,
                record.last_updated.isoformat(),
            ))

        conn.commit()
        elapsed = (time.time() - start) * 1000
        self._record_metric("persist", elapsed)

    def _row_to_record(
        self, row: sqlite3.Row
    ) -> Optional[SpecialistEffectivenessRecord]:
        """Convert a SQLite row to a SpecialistEffectivenessRecord."""
        try:
            return SpecialistEffectivenessRecord(
                id=row["id"],
                specialist=row["specialist"],
                session_id=row["session_id"],
                tasks_attempted=row["tasks_attempted"],
                tasks_succeeded=row["tasks_succeeded"],
                first_attempts=row["first_attempts"],
                first_attempt_successes=row["first_attempt_successes"],
                total_duration_ms=row["total_duration_ms"],
                consensus_participations=row["consensus_participations"],
                consensus_aligned=row["consensus_aligned"],
                patterns_contributed=row["patterns_contributed"],
                blackboard_publications=row["blackboard_publications"],
                last_updated=datetime.fromisoformat(row["last_updated"]),
            )
        except (ValueError, KeyError) as e:
            log.warning(f"Failed to convert row to SpecialistEffectivenessRecord: {e}")
            return None

    # ── Maintenance ───────────────────────────────────────────────────────

    def get_metrics(self) -> List[Dict]:
        with self._lock:
            return self._metrics.copy()

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self._metrics.append({
            "operation": operation,
            "duration_ms": round(duration_ms, 2),
        })

    def get_statistics(self) -> Dict[str, Any]:
        """Get aggregate statistics about all tracked data."""
        with self._lock:
            total = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM specialist_effectiveness"
            ).fetchone()["cnt"]
            specialists = self._conn.execute(
                "SELECT COUNT(DISTINCT specialist) as cnt FROM specialist_effectiveness"
            ).fetchone()["cnt"]
            sessions = self._conn.execute(
                "SELECT COUNT(DISTINCT session_id) as cnt FROM specialist_effectiveness"
            ).fetchone()["cnt"]

            return {
                "total_records": total,
                "unique_specialists": specialists,
                "unique_sessions": sessions,
            }

    def close(self) -> None:
        """Close the database connection."""
        try:
            self._conn.close()
        except Exception:
            pass
