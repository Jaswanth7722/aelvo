# learning/knowledge_graph.py - Engineering Knowledge Graph
# SQLite-backed typed graph for pattern persistence, relationship management,
# and structured queries

from __future__ import annotations

import time
import json
import logging
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict
from datetime import datetime, timezone

from learning.types import (
    EngineeringPattern, EditCategory, PatternQuery, PatternQueryResult,
    ValidationState, ContradictionRecord, ConfidenceUpdate,
    SubgraphSpec, EditCategorySignature,
)

log = logging.getLogger("aelvo.learning.knowledge_graph")


class KnowledgeGraph:
    """SQLite-backed typed graph for persistent pattern storage.

    Stores:
    - Pattern nodes (patterns table)
    - Relationship edges (edges table): DERIVED_FROM, CONTRADICTS, RESOLVES,
      SUPERSEDES, APPLIES_TO, CONFIRMS, REFUTES, GENERALIZES
    - Contradiction records (contradictions table)
    - Confidence updates (confidence_updates table)
    - Session checkpoints (sessions table)

    Thread-safe via connection-per-thread pattern.
    """

    def __init__(self, db_path: str = ":memory:"):
        self._db_path = db_path
        self._local = threading.local()
        self._lock = threading.Lock()
        self._metrics: List[Dict] = []

        self._init_schema()

    def _acquire_lock(self) -> None:
        """Acquire the write lock for thread-safe database operations."""
        self._lock.acquire()

    def _release_lock(self) -> None:
        """Release the write lock."""
        self._lock.release()

    def _with_lock(self, func, *args, **kwargs):
        """Execute a function with the write lock held."""
        self._acquire_lock()
        try:
            return func(*args, **kwargs)
        finally:
            self._release_lock()

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
        """Create tables and indexes."""
        conn = self._conn
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS patterns (
                id TEXT PRIMARY KEY,
                category TEXT NOT NULL,
                category_signature TEXT NOT NULL,
                subgraph_json TEXT NOT NULL DEFAULT '{}',
                confidence REAL NOT NULL DEFAULT 0.3,
                observation_count INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                validation_state TEXT NOT NULL DEFAULT 'observed',
                freshness REAL NOT NULL DEFAULT 1.0,
                created_at TEXT NOT NULL,
                last_observed TEXT NOT NULL,
                last_used TEXT,
                provenance_json TEXT NOT NULL DEFAULT '[]',
                source_specialist TEXT,
                project_scope TEXT,
                related_ids_json TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                edge_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                metadata_json TEXT DEFAULT '{}',
                FOREIGN KEY (source_id) REFERENCES patterns(id),
                FOREIGN KEY (target_id) REFERENCES patterns(id)
            );

            CREATE INDEX IF NOT EXISTS idx_edges_source
                ON edges(source_id);
            CREATE INDEX IF NOT EXISTS idx_edges_target
                ON edges(target_id);
            CREATE INDEX IF NOT EXISTS idx_edges_type
                ON edges(edge_type);

            CREATE TABLE IF NOT EXISTS contradictions (
                id TEXT PRIMARY KEY,
                old_knowledge_id TEXT NOT NULL,
                new_knowledge_id TEXT NOT NULL,
                contradiction_type TEXT NOT NULL,
                resolution_strategy TEXT NOT NULL DEFAULT '',
                resolution_accepted TEXT,
                resolution_rejected TEXT,
                reasoning TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                resolved INTEGER NOT NULL DEFAULT 0,
                confidence_impact REAL NOT NULL DEFAULT 0.0,
                FOREIGN KEY (old_knowledge_id) REFERENCES patterns(id),
                FOREIGN KEY (new_knowledge_id) REFERENCES patterns(id)
            );

            CREATE TABLE IF NOT EXISTS confidence_updates (
                id TEXT PRIMARY KEY,
                knowledge_item_id TEXT NOT NULL,
                previous_confidence REAL NOT NULL,
                new_confidence REAL NOT NULL,
                update_formula TEXT NOT NULL DEFAULT '',
                evidence TEXT NOT NULL DEFAULT '',
                timestamp TEXT NOT NULL,
                FOREIGN KEY (knowledge_item_id) REFERENCES patterns(id)
            );

            CREATE INDEX IF NOT EXISTS idx_conf_updates_item
                ON confidence_updates(knowledge_item_id);

            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                stats_json TEXT DEFAULT '{}',
                patterns_count INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_patterns_category
                ON patterns(category);
            CREATE INDEX IF NOT EXISTS idx_patterns_confidence
                ON patterns(confidence);
            CREATE INDEX IF NOT EXISTS idx_patterns_validation
                ON patterns(validation_state);
            CREATE INDEX IF NOT EXISTS idx_patterns_project
                ON patterns(project_scope);
        """)
        conn.commit()

    # ── Pattern Persistence ───────────────────────────────────────────────

    def save_pattern(self, pattern: EngineeringPattern) -> None:
        """Insert or update a pattern in the database."""
        def _do_save():
            start = time.time()
            conn = self._conn

            existing = conn.execute(
                "SELECT id FROM patterns WHERE id = ?", (pattern.id,)
            ).fetchone()

            subgraph_json = json.dumps(pattern.subgraph.model_dump())
            provenance_json = json.dumps(pattern.provenance)
            related_json = json.dumps(pattern.related_pattern_ids)
            sig_json = json.dumps(pattern.category_signature.model_dump())

            if existing:
                conn.execute("""
                    UPDATE patterns SET
                        category = ?, category_signature = ?, subgraph_json = ?,
                        confidence = ?, observation_count = ?, success_count = ?,
                        failure_count = ?, validation_state = ?, freshness = ?,
                        last_observed = ?, last_used = ?, provenance_json = ?,
                        related_ids_json = ?
                    WHERE id = ?
                """, (
                    pattern.category.value, sig_json, subgraph_json,
                    pattern.confidence, pattern.observation_count,
                    pattern.success_count, pattern.failure_count,
                    pattern.validation_state.value, pattern.freshness,
                    pattern.last_observed.isoformat(),
                    pattern.last_used.isoformat() if pattern.last_used else None,
                    provenance_json, related_json, pattern.id,
                ))
            else:
                conn.execute("""
                    INSERT INTO patterns (
                        id, category, category_signature, subgraph_json,
                        confidence, observation_count, success_count,
                        failure_count, validation_state, freshness,
                        created_at, last_observed, last_used, provenance_json,
                        source_specialist, project_scope, related_ids_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pattern.id, pattern.category.value, sig_json, subgraph_json,
                    pattern.confidence, pattern.observation_count,
                    pattern.success_count, pattern.failure_count,
                    pattern.validation_state.value, pattern.freshness,
                    pattern.created_at.isoformat(),
                    pattern.last_observed.isoformat(),
                    pattern.last_used.isoformat() if pattern.last_used else None,
                    provenance_json, pattern.source_specialist, pattern.project_scope,
                    related_json,
                ))

            conn.commit()
            elapsed = (time.time() - start) * 1000
            self._record_metric("save_pattern", elapsed)

        self._with_lock(_do_save)

    def load_pattern(self, pattern_id: str) -> Optional[EngineeringPattern]:
        """Load a single pattern by ID."""
        row = self._conn.execute(
            "SELECT * FROM patterns WHERE id = ?", (pattern_id,)
        ).fetchone()
        return self._row_to_pattern(row) if row else None

    def load_patterns(
        self,
        min_confidence: float = 0.0,
        limit: int = 100,
        offset: int = 0,
        category: Optional[EditCategory] = None,
        validation_state: Optional[ValidationState] = None,
        project_scope: Optional[str] = None,
    ) -> List[EngineeringPattern]:
        """Load patterns matching criteria."""
        where_clauses = ["confidence >= ?"]
        params: List[Any] = [min_confidence]

        if category:
            where_clauses.append("category = ?")
            params.append(category.value)
        if validation_state:
            where_clauses.append("validation_state = ?")
            params.append(validation_state.value)
        if project_scope is not None:
            where_clauses.append("project_scope = ?")
            params.append(project_scope)

        where = " AND ".join(where_clauses)
        rows = self._conn.execute(
            f"SELECT * FROM patterns WHERE {where} "
            f"ORDER BY confidence DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

        return [self._row_to_pattern(r) for r in rows if r is not None]

    def delete_pattern(self, pattern_id: str) -> bool:
        """Delete a pattern and its edges. Returns True if deleted."""
        def _do_delete():
            conn = self._conn
            conn.execute("DELETE FROM edges WHERE source_id = ? OR target_id = ?",
                         (pattern_id, pattern_id))
            cursor = conn.execute("DELETE FROM patterns WHERE id = ?", (pattern_id,))
            conn.commit()
            return cursor.rowcount > 0

        return self._with_lock(_do_delete)

    def _row_to_pattern(self, row: sqlite3.Row) -> Optional[EngineeringPattern]:
        """Convert a SQLite row to an EngineeringPattern."""
        try:
            subgraph_data = json.loads(row["subgraph_json"] or "{}")
            sig_data = json.loads(row["category_signature"] or "{}")
            provenance = json.loads(row["provenance_json"] or "[]")
            related = json.loads(row["related_ids_json"] or "[]")

            subgraph = SubgraphSpec(**subgraph_data) if subgraph_data else SubgraphSpec()
            sig = EditCategorySignature(**sig_data) if sig_data else EditCategorySignature()

            pattern = EngineeringPattern(
                id=row["id"],
                category=EditCategory(row["category"]),
                category_signature=sig,
                subgraph=subgraph,
                confidence=row["confidence"],
                observation_count=row["observation_count"],
                success_count=row["success_count"],
                failure_count=row["failure_count"],
                validation_state=ValidationState(row["validation_state"]),
                freshness=row["freshness"],
                created_at=datetime.fromisoformat(row["created_at"]),
                last_observed=datetime.fromisoformat(row["last_observed"]),
                last_used=datetime.fromisoformat(row["last_used"]) if row["last_used"] else None,
                provenance=provenance,
                source_specialist=row["source_specialist"],
                project_scope=row["project_scope"],
                related_pattern_ids=related,
            )
            return pattern
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            log.warning(f"Failed to convert row to pattern (id={row.get('id', 'unknown')}): {e}")
            return None

    # ── Edge Management ───────────────────────────────────────────────────

    def add_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        metadata: Optional[Dict] = None,
    ) -> bool:
        """Add a typed relationship edge between two patterns.

        Returns True if the edge was created, False if it already exists.
        """
        def _do_add():
            conn = self._conn
            existing = conn.execute(
                "SELECT id FROM edges WHERE source_id = ? AND target_id = ? AND edge_type = ?",
                (source_id, target_id, edge_type),
            ).fetchone()

            if existing:
                return False

            conn.execute("""
                INSERT INTO edges (source_id, target_id, edge_type, created_at, metadata_json)
                VALUES (?, ?, ?, ?, ?)
            """, (
                source_id, target_id, edge_type,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(metadata or {}),
            ))
            conn.commit()
            return True

        return self._with_lock(_do_add)

    def get_edges(
        self,
        pattern_id: Optional[str] = None,
        edge_type: Optional[str] = None,
        direction: str = "both",
    ) -> List[Dict]:
        """Get edges connected to a pattern.

        Args:
            pattern_id: Optional filter by pattern ID.
            edge_type: Optional filter by edge type.
            direction: "outgoing", "incoming", or "both".

        Returns:
            List of edge dicts with source_id, target_id, edge_type, metadata.
        """
        clauses: List[str] = []
        params: List[Any] = []

        if pattern_id:
            if direction == "outgoing":
                clauses.append("source_id = ?")
                params.append(pattern_id)
            elif direction == "incoming":
                clauses.append("target_id = ?")
                params.append(pattern_id)
            else:
                clauses.append("(source_id = ? OR target_id = ?)")
                params.extend([pattern_id, pattern_id])

        if edge_type:
            clauses.append("edge_type = ?")
            params.append(edge_type)

        where = " AND ".join(clauses) if clauses else "1=1"

        rows = self._conn.execute(
            f"SELECT * FROM edges WHERE {where} ORDER BY created_at",
            params,
        ).fetchall()

        return [
            {
                "id": r["id"],
                "source_id": r["source_id"],
                "target_id": r["target_id"],
                "edge_type": r["edge_type"],
                "created_at": r["created_at"],
                "metadata": json.loads(r["metadata_json"] or "{}"),
            }
            for r in rows
        ]

    # ── Contradiction Management ──────────────────────────────────────────

    def save_contradiction(self, record: ContradictionRecord) -> None:
        """Save a contradiction record."""
        def _do_save():
            conn = self._conn
            conn.execute("""
                INSERT OR REPLACE INTO contradictions (
                    id, old_knowledge_id, new_knowledge_id,
                    contradiction_type, resolution_strategy,
                    resolution_accepted, resolution_rejected,
                    reasoning, created_at, resolved_at, resolved,
                    confidence_impact
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.id, record.old_knowledge_id, record.new_knowledge_id,
                record.contradiction_type, record.resolution_strategy,
                record.resolution_accepted, record.resolution_rejected,
                record.reasoning, record.created_at.isoformat(),
                record.resolved_at.isoformat() if record.resolved_at else None,
                1 if record.resolved else 0,
                record.confidence_impact,
            ))
            conn.commit()

        self._with_lock(_do_save)

    def get_contradictions(
        self,
        pattern_id: Optional[str] = None,
        resolved: Optional[bool] = None,
    ) -> List[ContradictionRecord]:
        """Get contradiction records."""
        clauses: List[str] = []
        params: List[Any] = []

        if pattern_id:
            clauses.append("(old_knowledge_id = ? OR new_knowledge_id = ?)")
            params.extend([pattern_id, pattern_id])
        if resolved is not None:
            clauses.append("resolved = ?")
            params.append(1 if resolved else 0)

        where = " AND ".join(clauses) if clauses else "1=1"

        rows = self._conn.execute(
            f"SELECT * FROM contradictions WHERE {where} ORDER BY created_at DESC",
            params,
        ).fetchall()

        result: List[ContradictionRecord] = []
        for r in rows:
            record = ContradictionRecord(
                id=r["id"],
                old_knowledge_id=r["old_knowledge_id"],
                new_knowledge_id=r["new_knowledge_id"],
                contradiction_type=r["contradiction_type"],
                resolution_strategy=r["resolution_strategy"],
                resolution_accepted=r["resolution_accepted"],
                resolution_rejected=r["resolution_rejected"],
                reasoning=r["reasoning"],
                created_at=datetime.fromisoformat(r["created_at"]),
                resolved_at=datetime.fromisoformat(r["resolved_at"]) if r["resolved_at"] else None,
                resolved=bool(r["resolved"]),
                confidence_impact=r["confidence_impact"],
            )
            result.append(record)
        return result

    # ── Confidence Update Management ──────────────────────────────────────

    def save_confidence_update(self, update: ConfidenceUpdate) -> None:
        """Save a confidence update record."""
        def _do_save():
            conn = self._conn
            conn.execute("""
                INSERT OR REPLACE INTO confidence_updates (
                    id, knowledge_item_id, previous_confidence,
                    new_confidence, update_formula, evidence, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                update.id, update.knowledge_item_id,
                update.previous_confidence, update.new_confidence,
                update.update_formula, update.evidence,
                update.timestamp.isoformat(),
            ))
            conn.commit()

        self._with_lock(_do_save)

    def get_confidence_updates(
        self, pattern_id: str
    ) -> List[ConfidenceUpdate]:
        """Get all confidence updates for a pattern."""
        rows = self._conn.execute(
            "SELECT * FROM confidence_updates WHERE knowledge_item_id = ? "
            "ORDER BY timestamp",
            (pattern_id,),
        ).fetchall()

        return [
            ConfidenceUpdate(
                id=r["id"],
                knowledge_item_id=r["knowledge_item_id"],
                previous_confidence=r["previous_confidence"],
                new_confidence=r["new_confidence"],
                update_formula=r["update_formula"],
                evidence=r["evidence"],
                timestamp=datetime.fromisoformat(r["timestamp"]),
            )
            for r in rows
        ]

    # ── Sessions ──────────────────────────────────────────────────────────

    def save_session_checkpoint(
        self, session_id: str, stats: Dict
    ) -> None:
        """Save or update a session checkpoint."""
        def _do_save():
            conn = self._conn
            existing = conn.execute(
                "SELECT session_id FROM sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()

            if existing:
                conn.execute("""
                    UPDATE sessions SET
                        ended_at = ?, stats_json = ?, patterns_count = ?
                    WHERE session_id = ?
                """, (
                    datetime.now(timezone.utc).isoformat(),
                    json.dumps(stats),
                    stats.get("total_patterns", 0),
                    session_id,
                ))
            else:
                conn.execute("""
                    INSERT INTO sessions (session_id, started_at, stats_json, patterns_count)
                    VALUES (?, ?, ?, ?)
                """, (
                    session_id,
                    datetime.now(timezone.utc).isoformat(),
                    json.dumps(stats),
                    stats.get("total_patterns", 0),
                ))
            conn.commit()

        self._with_lock(_do_save)

    def load_session_checkpoint(self, session_id: str) -> Optional[Dict]:
        """Load a session checkpoint."""
        row = self._conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if row:
            return dict(row)
        return None

    # ── Query ─────────────────────────────────────────────────────────────

    def query(self, query: PatternQuery) -> PatternQueryResult:
        """Query patterns from the knowledge graph."""
        start = time.time()

        where_clauses: List[str] = ["1=1"]
        params: List[Any] = []

        if query.category:
            where_clauses.append("category = ?")
            params.append(query.category.value)
        if query.min_confidence > 0:
            where_clauses.append("confidence >= ?")
            params.append(query.min_confidence)
        if query.min_freshness > 0:
            where_clauses.append("freshness >= ?")
            params.append(query.min_freshness)
        if query.validation_state:
            where_clauses.append("validation_state = ?")
            params.append(query.validation_state.value)
        if query.project_scope is not None:
            where_clauses.append("project_scope = ?")
            params.append(query.project_scope)
        if query.source_specialist:
            where_clauses.append("source_specialist = ?")
            params.append(query.source_specialist)

        # Count total
        count_row = self._conn.execute(
            f"SELECT COUNT(*) as cnt FROM patterns WHERE {' AND '.join(where_clauses)}",
            params,
        ).fetchone()
        total = count_row["cnt"] if count_row else 0

        # Fetch patterns
        rows = self._conn.execute(
            f"SELECT * FROM patterns WHERE {' AND '.join(where_clauses)} "
            f"ORDER BY confidence DESC LIMIT ?",
            params + [query.max_results],
        ).fetchall()

        patterns = [p for r in rows if (p := self._row_to_pattern(r)) is not None]

        elapsed = (time.time() - start) * 1000
        self._record_metric("query", elapsed)

        return PatternQueryResult(
            query=query,
            patterns=patterns,
            total_matched=total,
            query_duration_ms=elapsed,
        )

    # ── Analytics ─────────────────────────────────────────────────────────

    def get_statistics(self) -> Dict[str, Any]:
        """Get aggregate statistics about stored data."""
        conn = self._conn
        total = conn.execute("SELECT COUNT(*) as cnt FROM patterns").fetchone()["cnt"]
        by_category = conn.execute(
            "SELECT category, COUNT(*) as cnt FROM patterns GROUP BY category"
        ).fetchall()
        by_state = conn.execute(
            "SELECT validation_state, COUNT(*) as cnt FROM patterns GROUP BY validation_state"
        ).fetchall()
        total_edges = conn.execute("SELECT COUNT(*) as cnt FROM edges").fetchone()["cnt"]
        total_contradictions = conn.execute(
            "SELECT COUNT(*) as cnt FROM contradictions"
        ).fetchone()["cnt"]

        stats = {
            "total_patterns": total,
            "by_category": {r["category"]: r["cnt"] for r in by_category},
            "by_validation_state": {r["validation_state"]: r["cnt"] for r in by_state},
            "total_edges": total_edges,
            "total_contradictions": total_contradictions,
        }

        if total > 0:
            avg_conf = conn.execute(
                "SELECT AVG(confidence) as avg_c FROM patterns"
            ).fetchone()["avg_c"]
            stats["avg_confidence"] = round(avg_conf, 4)

        return stats

    def find_orphaned_patterns(self) -> List[str]:
        """Find patterns with no edges to other patterns."""
        rows = self._conn.execute("""
            SELECT p.id FROM patterns p
            WHERE p.id NOT IN (
                SELECT source_id FROM edges
                UNION
                SELECT target_id FROM edges
            )
        """).fetchall()
        return [r["id"] for r in rows]

    def detect_cycles_supersedes(self) -> List[List[str]]:
        """Detect cycles in the SUPERSEDES relationship chain."""
        edges = self._conn.execute(
            "SELECT source_id, target_id FROM edges WHERE edge_type = 'SUPERSEDES'"
        ).fetchall()

        # Build adjacency list
        adj: Dict[str, List[str]] = defaultdict(list)
        for e in edges:
            adj[e["source_id"]].append(e["target_id"])

        # DFS cycle detection
        cycles: List[List[str]] = []
        visited: Set[str] = set()
        path: List[str] = []
        path_set: Set[str] = set()

        def dfs(node: str):
            visited.add(node)
            path.append(node)
            path_set.add(node)
            for neighbor in adj.get(node, []):
                if neighbor in path_set:
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:])
                elif neighbor not in visited:
                    dfs(neighbor)
            path.pop()
            path_set.discard(node)

        for node in adj:
            if node not in visited:
                dfs(node)

        return cycles

    # ── Maintenance ───────────────────────────────────────────────────────

    def vacuum(self) -> None:
        """Reclaim space and optimize database."""
        self._conn.execute("VACUUM")
        log.info("Knowledge graph vacuum completed")

    def close(self) -> None:
        """Close all connections."""
        self._conn.close()

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self._metrics.append({
            "operation": operation,
            "duration_ms": round(duration_ms, 2),
        })

    def get_metrics(self) -> List[Dict]:
        return self._metrics.copy()
