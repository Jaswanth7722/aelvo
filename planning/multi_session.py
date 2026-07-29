# planning/multi_session.py - Multi-Session Planning Engine for AELVO OMEGA
"""
The Multi-Session Planning Engine bridges sessions. It has three responsibilities:

1. SESSION RESTORE â€” At session start, load the previous session boundary record
   and reconstruct the in-memory hierarchy. Resume exactly where the last session
   ended without asking the user to re-explain context.

2. SESSION SAVE â€” At session end (or on any KeyboardInterrupt), write the full
   strategic state to disk via temp-file atomic rename. Never lose plan state.

3. CONTINUITY INJECTION â€” At session start, inject the restored context into
   the orchestrator's shared_context so all specialists know what was in progress.

This engine does not have its own storage substrate. All persistent state is
written to the same SQLite + ChromaDB that the rest of AELVO uses.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from planning.memory_types import (
    SessionBoundaryRecord,
    MEMORY_TYPE_SESSION_BOUNDARY,
    IMPORTANCE_SESSION_BOUNDARY,
)
from planning.goal_hierarchy import GoalHierarchyEngine
from config.settings import MEMORY_NOISE_FLOOR

log = logging.getLogger("aelvo.planning.multi_session")


class MultiSessionPlanningEngine:
    """Bridges planning state across session boundaries.

    Injected into the Orchestrator at initialization. The Orchestrator calls:
    - restore_session_start() during boot (before the first user message)
    - save_session_end()    during shutdown (after "exit"/"quit" received)

    The boundary record is stored both in ChromaDB (for semantic recall) and
    in a dedicated JSON file in the workspace (for fast atomic load on restore).
    """

    BOUNDARY_FILENAME = ".planning_session_boundary.json"

    def __init__(
        self,
        memory_engine,
        hierarchy: GoalHierarchyEngine,
        workspace_path: str,
        project: str,
    ):
        self.memory_engine = memory_engine
        self.hierarchy = hierarchy
        self.workspace_path = workspace_path
        self.project = project
        self.collection = memory_engine.memory_collection
        self.db = memory_engine.db
        self._boundary_file = os.path.join(workspace_path, self.BOUNDARY_FILENAME)
        self._session_turn_count = 0
        self._session_start_time = time.time()
        self._current_boundary: Optional[SessionBoundaryRecord] = None
        self._restored = False

    # ------------------------------------------------------------------
    # Session Start (called by orchestrator on boot)
    # ------------------------------------------------------------------

    def restore_session_start(self) -> Optional[Dict[str, Any]]:
        """Restore strategic state from the previous session boundary.

        Returns the continuity_context dict for injection into shared_context.
        If no boundary exists (first session), returns None.

        The boundary file is the authoritative restore source. ChromaDB is
        the secondary source used when the boundary file is missing.
        """
        record = self._load_boundary_from_disk()
        if record is None:
            record = self._load_boundary_from_chroma()

        if record is None:
            log.info("No previous session boundary â€” starting fresh")
            self._session_start_time = time.time()
            self._session_turn_count = 0
            return None

        self._current_boundary = record
        self._session_start_time = time.time()
        self._session_turn_count = 0

        # Reload hierarchy nodes from ChromaDB
        nodes_loaded = self.hierarchy.load_hierarchy()
        log.info(
            "Session restored: %d hierarchy nodes, last boundary at %.0f",
            nodes_loaded, record.timestamp_unix,
        )

        self._restored = True
        return self._build_continuity_context(record)

    # ------------------------------------------------------------------
    # Session End (called by orchestrator on shutdown / KeyboardInterrupt)
    # ------------------------------------------------------------------

    def save_session_end(self, interrupted_details: Optional[Dict[str, Any]] = None) -> bool:
        """Write the strategic state boundary for the next session to restore.

        Two writes in order:
        1. Temp file + atomic rename (workspace JSON) â†’ ensures safe disk write
        2. ChromaDB entry â†’ enables semantic recall of past session context

        Returns True if both writes succeeded.
        """
        try:
            record = self._build_boundary_record(interrupted_details)
            self._current_boundary = record

            # Write 1: Atomic JSON file
            atomic_ok = self._atomic_write_boundary(record)

            # Write 2: ChromaDB semantic entry
            chroma_ok = self._write_boundary_to_chroma(record)

            if atomic_ok:
                log.info(
                    "Session boundary saved (chroma=%s): %d active objectives, "
                    "%d active milestones, next_step='%s'",
                    chroma_ok,
                    len(record.active_objective_ids),
                    len(record.active_milestone_ids),
                    record.next_concrete_step[:60],
                )
            else:
                log.warning("Session boundary atomic write failed â€” plan state may be lost")

            return atomic_ok

        except Exception as exc:
            log.error("Session boundary save error: %s", exc)
            return False

    def increment_turn_count(self) -> None:
        """Called by the orchestrator after each successful turn."""
        self._session_turn_count += 1

    # ------------------------------------------------------------------
    # Continuity Context Builder
    # ------------------------------------------------------------------

    def _build_continuity_context(self, record: SessionBoundaryRecord) -> Dict[str, Any]:
        """Build the continuity context dict injected into orchestrator shared context.

        This block is small enough to fit in every system prompt without
        bloating it. Specialists that need deeper context will query the
        hierarchy directly.
        """
        session_age_hours = (time.time() - record.timestamp_unix) / 3600.0

        context = {
            "continuity": {
                "restored": True,
                "session_age_hours": round(session_age_hours, 1),
                "last_session_turns": record.session_turn_count,
                "interrupted_milestone": record.interrupted_milestone_id,
                "interrupted_pct": record.interrupted_pct,
                "next_concrete_step": record.next_concrete_step,
                "last_active_specialist": record.last_active_specialist,
                "active_objective_count": len(record.active_objective_ids),
                "active_milestone_count": len(record.active_milestone_ids),
                "complete_milestone_count": len(record.complete_milestone_ids),
                "objectives_summary": record.objectives_summary,
                "high_priority_next_actions": record.high_priority_next_actions,
                "restoration_context": record.restoration_context,
            }
        }

        if session_age_hours < 1:
            context["continuity"]["resume_msg"] = (
                f"Resumed from {session_age_hours * 60:.0f} minutes ago. "
                f"Continue: {record.next_concrete_step}"
            )
        elif session_age_hours < 24:
            context["continuity"]["resume_msg"] = (
                f"Resumed from {session_age_hours:.1f} hours ago. "
                f"Next: {record.next_concrete_step}"
            )
        else:
            days = session_age_hours / 24.0
            context["continuity"]["resume_msg"] = (
                f"Resumed from {days:.1f} days ago. "
                f"Active milestones: {len(record.active_milestone_ids)}. "
                f"Next: {record.next_concrete_step}"
            )

        return context

    # ------------------------------------------------------------------
    # Boundary Record Construction
    # ------------------------------------------------------------------

    def _build_boundary_record(
        self, interrupted_details: Optional[Dict[str, Any]] = None,
    ) -> SessionBoundaryRecord:
        """Build the session boundary record from the current hierarchy state."""
        now = time.time()
        record_id = hashlib.sha256(
            f"boundary_{self.project}_{now}".encode("utf-8")
        ).hexdigest()[:16]

        mission = self.hierarchy.get_mission()
        active_objectives = self.hierarchy.get_active_objectives()
        active_milestones = self.hierarchy.get_active_milestones()

        from planning.memory_types import HierarchyLevel
        complete_milestones = [
            n for n in self.hierarchy.find_nodes_by_level(HierarchyLevel.MILESTONE)
            if n.state.value == "complete"
        ]

        # Build the high-priority next actions list
        next_actions = []
        for ms in sorted(active_milestones, key=lambda m: -m.confidence)[:3]:
            children = self.hierarchy.get_children(ms.node_id)
            active_tasks = [c for c in children if c.state.value in ("proposed", "active")]
            if active_tasks:
                task = active_tasks[0]
                next_actions.append(
                    f"[{ms.title}] â†’ {task.title}"
                )
            else:
                next_actions.append(f"[{ms.title}] â†’ Review progress and identify next task")

        # Detect interrupted work
        interrupted_milestone_id = None
        interrupted_pct = 0.0
        if interrupted_details:
            interrupted_milestone_id = interrupted_details.get("milestone_id")
            interrupted_pct = float(interrupted_details.get("progress_pct", 0.0))

        # Build confidence snapshot
        confidence_snapshot = {
            n.node_id: n.confidence
            for n in self.hierarchy._nodes.values()
            if n.state.value in ("active", "proposed")
        }

        # Build objectives summary for fast loading
        objectives_summary = [
            {
                "node_id": o.node_id,
                "title": o.title,
                "state": o.state.value,
                "progress_pct": o.progress_pct,
                "confidence": o.confidence,
                "capability_area": o.capability_area,
            }
            for o in active_objectives
        ]

        # Build restoration context string
        restoration_parts = []
        if mission:
            restoration_parts.append(f"Mission: {mission.title}")
        if active_objectives:
            obj_titles = ", ".join(o.title for o in active_objectives[:3])
            restoration_parts.append(f"Active Objectives: {obj_titles}")
        if active_milestones:
            ms_titles = ", ".join(
                f"{m.title} ({m.progress_pct:.0f}%)" for m in active_milestones[:3]
            )
            restoration_parts.append(f"Active Milestones: {ms_titles}")
        if next_actions:
            restoration_parts.append(f"Next Steps: {next_actions[0]}")

        restoration_context = " | ".join(restoration_parts)

        return SessionBoundaryRecord(
            record_id=record_id,
            timestamp_unix=now,
            project=self.project,
            session_turn_count=self._session_turn_count,
            mission_node_id=mission.node_id if mission else None,
            active_objective_ids=[o.node_id for o in active_objectives],
            active_milestone_ids=[m.node_id for m in active_milestones],
            complete_milestone_ids=[m.node_id for m in complete_milestones],
            interrupted_milestone_id=interrupted_milestone_id,
            interrupted_pct=interrupted_pct,
            next_concrete_step=next_actions[0] if next_actions else "",
            restoration_context=restoration_context,
            objectives_summary=objectives_summary,
            high_priority_next_actions=next_actions,
            confidence_snapshot=confidence_snapshot,
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _atomic_write_boundary(self, record: SessionBoundaryRecord) -> bool:
        """Write boundary record via temp-file atomic rename."""
        tmp_path = self._boundary_file + ".tmp"
        try:
            data = record.model_dump()
            # Convert any non-JSON-serializable values
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, default=str, indent=2)
            os.replace(tmp_path, self._boundary_file)
            return True
        except Exception as exc:
            log.error("Boundary atomic write failed: %s", exc)
            try:
                os.unlink(tmp_path)
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)
            return False

    def _write_boundary_to_chroma(self, record: SessionBoundaryRecord) -> bool:
        """Write boundary record to ChromaDB for semantic recall."""
        try:
            doc = (
                f"Session boundary for project {self.project} "
                f"at {record.timestamp_unix:.0f}. "
                f"Objectives: {len(record.active_objective_ids)} active. "
                f"Milestones: {len(record.active_milestone_ids)} active, "
                f"{len(record.complete_milestone_ids)} complete. "
                f"Next: {record.next_concrete_step}"
            )
            meta = {
                "type": MEMORY_TYPE_SESSION_BOUNDARY,
                "importance": IMPORTANCE_SESSION_BOUNDARY,
                "timestamp_unix": record.timestamp_unix,
                "usage_count": 1,
                "project": self.project,
                "source_specialist": "planning",
                "record_id": record.record_id,
                "session_turns": record.session_turn_count,
                "active_objectives": len(record.active_objective_ids),
                "active_milestones": len(record.active_milestone_ids),
                "complete_milestones": len(record.complete_milestone_ids),
                "next_step": record.next_concrete_step[:200],
                "restoration_context": record.restoration_context[:400],
            }
            entry_id = hashlib.sha256(
                f"boundary_{self.project}_{record.timestamp_unix}".encode("utf-8")
            ).hexdigest()

            self.collection.add(
                ids=[entry_id],
                documents=[doc],
                metadatas=[meta],
            )
            return True
        except Exception as exc:
            log.warning("Boundary ChromaDB write failed: %s", exc)
            return False

    def _load_boundary_from_disk(self) -> Optional[SessionBoundaryRecord]:
        """Load the most recent boundary record from the workspace JSON file."""
        if not os.path.exists(self._boundary_file):
            return None
        try:
            with open(self._boundary_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return SessionBoundaryRecord(**data)
        except Exception as exc:
            log.warning("Failed to load boundary from disk: %s", exc)
            return None

    def _load_boundary_from_chroma(self) -> Optional[SessionBoundaryRecord]:
        """Load the most recent boundary record from ChromaDB (fallback)."""
        try:
            results = self.collection.query(
                query_texts=[f"session boundary project {self.project}"],
                n_results=1,
                where={
                    "type": MEMORY_TYPE_SESSION_BOUNDARY,
                    "project": self.project,
                },
                include=["metadatas"],
            )
            if not (results.get("ids") and results["ids"][0]):
                return None

            meta = results["metadatas"][0][0]
            if not isinstance(meta, dict):
                return None

            # Build a minimal SessionBoundaryRecord from ChromaDB metadata
            return SessionBoundaryRecord(
                record_id=meta.get("record_id", ""),
                timestamp_unix=float(meta.get("timestamp_unix", 0)),
                project=self.project,
                session_turn_count=int(meta.get("session_turns", 0)),
                next_concrete_step=meta.get("next_step", ""),
                restoration_context=meta.get("restoration_context", ""),
            )
        except Exception as exc:
            log.debug("Failed to load boundary from ChromaDB: %s", exc)
            return None
