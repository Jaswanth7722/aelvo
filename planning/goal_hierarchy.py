# planning/goal_hierarchy.py - Goal Hierarchy Engine for AELVO OMEGA Long-Horizon Planning
"""
The Goal Hierarchy Engine is the data structure that gives Long-Horizon
Planning its memory of the future.

Six levels: Mission → Strategic Objective → Program → Initiative → Milestone → Task

Every node at every level:
- Knows its parent and children
- Carries current state, confidence, blocking dependencies, and last-updated timestamp
- Is stored in the existing ChromaDB + SQLite memory substrate
- Benefits from the same semantic retrieval and deduplication as all other memory

Work that cannot be connected to a Strategic Objective through the hierarchy
is either mis-scoped or signals a missing Strategic Objective that should
be created.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from planning.memory_types import (
    StrategicPlanEntry,
    HierarchyLevel,
    PlanNodeState,
    RiskLevel,
    RiskAssessment,
    VerificationStrategy,
    RevisionRecord,
    EvolutionTriggerType,
    MEMORY_TYPE_STRATEGIC_PLAN,
    IMPORTANCE_STRATEGIC_PLAN,
)
from config.settings import (
    CONFLICT_SIMILARITY_DUPLICATE,
    CONFLICT_SIMILARITY_OVERRIDE,
    MEMORY_NOISE_FLOOR,
)

log = logging.getLogger("aelvo.planning.hierarchy")


class GoalHierarchyEngine:
    """Full CRUD engine for the six-level goal hierarchy.

    All reads and writes go through the existing MemoryEngine's ChromaDB
    collection and SQLite database. This engine never creates its own
    storage substrate — it is a discipline layer on top of what already
    exists.

    The hierarchy is queryable by the orchestrator before every task
    classification so it knows whether the incoming user request aligns
    with active strategic priorities, is neutral to them, or contradicts them.
    """

    def __init__(self, memory_engine, project: str):
        self.memory_engine = memory_engine
        self.project = project
        self.collection = memory_engine.memory_collection
        self.db = memory_engine.db
        # In-memory index for fast traversal (rebuilt from ChromaDB on load)
        self._nodes: Dict[str, StrategicPlanEntry] = {}
        self._children_index: Dict[str, List[str]] = {}  # parent_id → [child_node_ids]
        self._loaded = False

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def load_hierarchy(self) -> int:
        """Load all strategic plan nodes from ChromaDB into the in-memory index.

        Called once at session start by the multi-session engine. Returns
        the count of nodes loaded.
        """
        try:
            results = self.collection.get(
                where={"type": MEMORY_TYPE_STRATEGIC_PLAN, "project": self.project},
                include=["documents", "metadatas"],
            )
        except Exception as e:
            log.warning("Failed to load hierarchy from ChromaDB: %s", e)
            return 0

        ids = results.get("ids", []) or []
        docs = results.get("documents", []) or []
        metas = results.get("metadatas", []) or []

        loaded = 0
        for entry_id, doc, meta in zip(ids, docs, metas):
            if not isinstance(meta, dict):
                continue
            try:
                # Reconstruct StrategicPlanEntry from stored metadata + document
                node = self._reconstruct_node(entry_id, doc, meta)
                if node:
                    self._nodes[node.node_id] = node
                    if node.parent_id:
                        self._children_index.setdefault(node.parent_id, [])
                        if node.node_id not in self._children_index[node.parent_id]:
                            self._children_index[node.parent_id].append(node.node_id)
                    loaded += 1
            except Exception as exc:
                log.debug("Failed to reconstruct node from %s: %s", entry_id, exc)

        self._loaded = True
        log.info("Loaded %d strategic plan nodes for project=%s", loaded, self.project)
        return loaded

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    def create_node(
        self,
        level: HierarchyLevel,
        title: str,
        content: str,
        parent_id: Optional[str] = None,
        success_criteria: Optional[List[str]] = None,
        blocking_dependencies: Optional[List[str]] = None,
        enabling_dependencies: Optional[List[str]] = None,
        target_sessions: Optional[int] = None,
        risk_assessment: Optional[RiskAssessment] = None,
        verification_strategy: Optional[VerificationStrategy] = None,
        mission_statement: str = "",
        capability_area: str = "",
    ) -> Optional[StrategicPlanEntry]:
        """Create a new node in the goal hierarchy and persist it to memory.

        Follows the same write discipline as ForgeMemory:
        1. resolve_conflict() first
        2. Build full metadata with all 4 required fields
        3. ChromaDB write
        4. SQLite dual-sync
        5. SQLite failure → rollback ChromaDB

        Returns the created node or None if a duplicate was detected.
        """
        # 1. Validate parent exists (if specified)
        if parent_id and parent_id not in self._nodes:
            log.warning(
                "Cannot create node '%s' — parent '%s' not in hierarchy",
                title, parent_id,
            )
            return None

        # 2. Validate hierarchy rules (child level must be one below parent)
        if parent_id:
            parent = self._nodes[parent_id]
            valid = self._validate_parent_child_level(parent.level, level)
            if not valid:
                log.warning(
                    "Hierarchy violation: cannot attach %s under %s",
                    level.value, parent.level.value,
                )
                return None

        # 3. Build entry
        now = time.time()
        entry = StrategicPlanEntry(
            type=MEMORY_TYPE_STRATEGIC_PLAN,
            content=content,
            importance=IMPORTANCE_STRATEGIC_PLAN,
            timestamp_unix=now,
            usage_count=1,
            project=self.project,
            source_specialist="planning",
            level=level,
            parent_id=parent_id,
            title=title,
            success_criteria=success_criteria or [],
            blocking_dependencies=blocking_dependencies or [],
            enabling_dependencies=enabling_dependencies or [],
            target_sessions=target_sessions,
            risk_assessment=risk_assessment,
            verification_strategy=verification_strategy,
            mission_statement=mission_statement,
            capability_area=capability_area,
        )

        # 4. Conflict resolution
        if self._resolve_conflict(content, level):
            log.debug("Duplicate strategic plan node detected for '%s' — skipping", title)
            return None

        # 5. Persist
        entry_id = entry.id
        meta = self._build_metadata(entry)

        try:
            self.collection.add(
                ids=[entry_id],
                documents=[content],
                metadatas=[meta],
            )
        except Exception as exc:
            log.error("ChromaDB write failed for node '%s': %s", title, exc)
            return None

        try:
            with self.db:
                self.db.execute(
                    "INSERT INTO retained_memory (content) VALUES (?)",
                    (f"[PLANNING:{level.value}|{self.project}] {title}: {content[:400]}",),
                )
        except Exception as exc:
            log.error("SQLite dual-sync failed for '%s', rolling back: %s", title, exc)
            try:
                self.collection.delete(ids=[entry_id])
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)
            return None

        # 6. Update in-memory index
        self._nodes[entry.node_id] = entry
        if parent_id:
            self._children_index.setdefault(parent_id, [])
            self._children_index[parent_id].append(entry.node_id)
            # Update parent's children_ids list
            parent_node = self._nodes[parent_id]
            if entry.node_id not in parent_node.children_ids:
                parent_node.children_ids.append(entry.node_id)
                self._persist_node_update(parent_node)

        log.info("✓ Created %s node '%s' (node_id=%s)", level.value, title, entry.node_id)
        return entry

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_node(self, node_id: str) -> Optional[StrategicPlanEntry]:
        """Retrieve a node by its planning node_id."""
        node = self._nodes.get(node_id)
        if node:
            node.usage_count += 1
            node.importance = min(1.0, node.importance + 0.01)
        return node

    def get_mission(self) -> Optional[StrategicPlanEntry]:
        """Get the single Mission node for this project."""
        for node in self._nodes.values():
            if node.level == HierarchyLevel.MISSION:
                return node
        return None

    def get_active_objectives(self) -> List[StrategicPlanEntry]:
        """Return all Strategic Objective nodes in ACTIVE state."""
        return [
            n for n in self._nodes.values()
            if n.level == HierarchyLevel.STRATEGIC_OBJECTIVE
            and n.state == PlanNodeState.ACTIVE
        ]

    def get_active_milestones(self) -> List[StrategicPlanEntry]:
        """Return all Milestone nodes currently in ACTIVE state."""
        return [
            n for n in self._nodes.values()
            if n.level == HierarchyLevel.MILESTONE
            and n.state == PlanNodeState.ACTIVE
        ]

    def get_children(self, node_id: str) -> List[StrategicPlanEntry]:
        """Return all direct children of a node."""
        child_ids = self._children_index.get(node_id, [])
        return [self._nodes[cid] for cid in child_ids if cid in self._nodes]

    def get_ancestors(self, node_id: str) -> List[StrategicPlanEntry]:
        """Return the full ancestor chain from this node up to Mission."""
        ancestors = []
        current_id = node_id
        visited: set = set()
        while current_id and current_id not in visited:
            visited.add(current_id)
            node = self._nodes.get(current_id)
            if not node or not node.parent_id:
                break
            parent = self._nodes.get(node.parent_id)
            if parent:
                ancestors.append(parent)
                current_id = parent.node_id
            else:
                break
        return ancestors

    def get_objective_for_task(self, task_node_id: str) -> Optional[StrategicPlanEntry]:
        """Trace a task node up through the hierarchy to its Strategic Objective."""
        ancestors = self.get_ancestors(task_node_id)
        for ancestor in ancestors:
            if ancestor.level == HierarchyLevel.STRATEGIC_OBJECTIVE:
                return ancestor
        return None

    def find_nodes_by_level(self, level: HierarchyLevel) -> List[StrategicPlanEntry]:
        """Return all nodes at a given hierarchy level."""
        return [n for n in self._nodes.values() if n.level == level]

    def find_floating_tasks(self) -> List[StrategicPlanEntry]:
        """Find task nodes not traceable to any Strategic Objective.

        A task is floating if the ancestor chain has no Strategic Objective.
        This is a planning defect detected by the self-critique engine.
        """
        floating = []
        for node in self._nodes.values():
            if node.level == HierarchyLevel.TASK:
                objective = self.get_objective_for_task(node.node_id)
                if objective is None:
                    floating.append(node)
        return floating

    def detect_circular_dependencies(self) -> List[Tuple[str, str]]:
        """Detect circular references in blocking_dependencies.

        Returns list of (node_id, dependency_node_id) pairs that form cycles.
        A circular dependency is a scheduling defect that will deadlock execution.
        """
        cycles = []
        for node_id, node in self._nodes.items():
            for dep_id in node.blocking_dependencies:
                if self._is_ancestor_of(node_id, dep_id):
                    cycles.append((node_id, dep_id))
        return cycles

    def build_strategic_context_summary(self) -> Dict[str, Any]:
        """Build the strategic context dict injected into the orchestrator's shared context.

        This is the primary output consumed by the orchestrator's build_shared_context()
        method. It provides every specialist with strategic awareness automatically.
        """
        active_objectives = self.get_active_objectives()
        active_milestones = self.get_active_milestones()

        # Determine highest priority next action
        highest_priority_action = ""
        if active_milestones:
            # Find milestone with highest confidence and least progress blocking
            best = max(
                active_milestones,
                key=lambda m: (m.confidence, -m.progress_pct),
            )
            highest_priority_action = (
                f"Continue milestone '{best.title}' "
                f"({best.progress_pct:.0f}% complete, confidence={best.confidence:.2f})"
            )

        return {
            "strategic_context": {
                "active_objectives": [
                    {
                        "node_id": o.node_id,
                        "title": o.title,
                        "state": o.state.value,
                        "progress_pct": o.progress_pct,
                        "confidence": o.confidence,
                    }
                    for o in active_objectives
                ],
                "active_milestones": [
                    {
                        "node_id": m.node_id,
                        "title": m.title,
                        "state": m.state.value,
                        "progress_pct": m.progress_pct,
                        "confidence": m.confidence,
                        "risk_level": m.risk_assessment.overall_risk.value if m.risk_assessment else "unknown",
                    }
                    for m in active_milestones
                ],
                "highest_priority_next_action": highest_priority_action,
                "total_nodes": len(self._nodes),
                "objectives_count": len(find_nodes_by_level_internal(self._nodes, HierarchyLevel.STRATEGIC_OBJECTIVE)),
                "milestones_complete": len([
                    n for n in self._nodes.values()
                    if n.level == HierarchyLevel.MILESTONE
                    and n.state == PlanNodeState.COMPLETE
                ]),
            }
        }

    def classify_request_alignment(self, task: str) -> str:
        """Determine if a user request aligns with, is neutral to, or contradicts active objectives.

        Returns: "aligned", "neutral", or "contradicts"

        This result influences which specialists are activated and in what order,
        not by overriding the existing activation logic but by providing context
        that specialists can use for strategic awareness.
        """
        if not self._nodes:
            return "neutral"

        task_lower = task.lower()
        active_objectives = self.get_active_objectives()
        active_milestones = self.get_active_milestones()

        # Check alignment with active milestones first (most specific)
        for milestone in active_milestones:
            milestone_lower = (milestone.title + " " + milestone.content).lower()
            # Simple keyword overlap as a proxy for alignment
            task_words = set(task_lower.split())
            milestone_words = set(milestone_lower.split())
            overlap = task_words & milestone_words
            if len(overlap) >= 2:
                return "aligned"

        # Check alignment with active objectives (broader)
        for obj in active_objectives:
            obj_lower = (obj.title + " " + obj.content + " " + obj.capability_area).lower()
            task_words = set(task_lower.split())
            obj_words = set(obj_lower.split())
            overlap = task_words & obj_words
            if len(overlap) >= 2:
                return "aligned"

        return "neutral"

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update_node_state(
        self,
        node_id: str,
        new_state: PlanNodeState,
        trigger_summary: str = "",
        trigger_type: str = EvolutionTriggerType.USER_DIRECTIVE.value,
    ) -> bool:
        """Update a node's state and propagate progress to parents."""
        node = self._nodes.get(node_id)
        if not node:
            return False

        old_state = node.state
        node.state = new_state
        node.last_revised_unix = time.time()

        # Record revision
        node.record_revision(
            trigger_type=trigger_type,
            trigger_summary=trigger_summary or f"State changed from {old_state.value} to {new_state.value}",
            changes_made=f"state: {old_state.value} → {new_state.value}",
            rationale=trigger_summary or "State update",
            previous_state_summary=f"state={old_state.value}",
        )

        # Propagate progress upward
        self._propagate_progress(node_id)
        self._persist_node_update(node)
        log.info("Node '%s' state: %s → %s", node.title, old_state.value, new_state.value)
        return True

    def update_confidence(
        self,
        node_id: str,
        new_confidence: float,
        rationale: str = "",
        trigger_type: str = "verification_outcome",
    ) -> bool:
        """Update a node's confidence score with evidence-backed rationale."""
        node = self._nodes.get(node_id)
        if not node:
            return False

        old_confidence = node.confidence
        node.confidence = max(0.0, min(1.0, new_confidence))
        node.last_revised_unix = time.time()

        node.record_revision(
            trigger_type=trigger_type,
            trigger_summary=f"Confidence updated from {old_confidence:.2f} to {node.confidence:.2f}",
            changes_made=f"confidence: {old_confidence:.2f} → {node.confidence:.2f}",
            rationale=rationale or "Confidence adjustment from verification outcome",
        )

        self._persist_node_update(node)
        log.info(
            "Confidence update for '%s': %.2f → %.2f (%s)",
            node.title, old_confidence, node.confidence, rationale[:60],
        )
        return True

    def attach_risk_assessment(
        self,
        node_id: str,
        risk: RiskAssessment,
    ) -> bool:
        """Attach an evidence-grounded risk assessment to a milestone node."""
        node = self._nodes.get(node_id)
        if not node:
            return False
        node.risk_assessment = risk
        node.last_revised_unix = time.time()
        self._persist_node_update(node)
        return True

    def attach_verification_strategy(
        self,
        node_id: str,
        strategy: VerificationStrategy,
    ) -> bool:
        """Attach a verification strategy to a milestone or task node."""
        node = self._nodes.get(node_id)
        if not node:
            return False
        node.verification_strategy = strategy
        node.last_revised_unix = time.time()
        self._persist_node_update(node)
        return True

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def cancel_node(
        self,
        node_id: str,
        reason: str = "",
    ) -> bool:
        """Cancel a node by setting its state to CANCELLED.

        We never hard-delete plan nodes because the revision history and
        evidence trail are valuable even for cancelled work. Cancellation
        is the mechanism that removes work from the active plan without
        losing its institutional memory.
        """
        return self.update_node_state(
            node_id=node_id,
            new_state=PlanNodeState.CANCELLED,
            trigger_summary=reason or "Node cancelled",
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_parent_child_level(
        self, parent_level: HierarchyLevel, child_level: HierarchyLevel,
    ) -> bool:
        """Enforce the six-level hierarchy order."""
        order = [
            HierarchyLevel.MISSION,
            HierarchyLevel.STRATEGIC_OBJECTIVE,
            HierarchyLevel.PROGRAM,
            HierarchyLevel.INITIATIVE,
            HierarchyLevel.MILESTONE,
            HierarchyLevel.TASK,
        ]
        try:
            parent_idx = order.index(parent_level)
            child_idx = order.index(child_level)
            return child_idx == parent_idx + 1
        except ValueError:
            return False

    def _resolve_conflict(self, content: str, level: HierarchyLevel) -> bool:
        """Return True if the content is a duplicate and write should be skipped.

        Mirrors the resolve_conflict logic used throughout AELVO — same
        thresholds, same boost behavior on duplicates.
        """
        try:
            results = self.collection.query(
                query_texts=[content],
                n_results=1,
                where={
                    "type": MEMORY_TYPE_STRATEGIC_PLAN,
                    "project": self.project,
                    "level": level.value,
                },
                include=["documents", "metadatas", "distances"],
            )
            if not (results.get("ids") and results["ids"][0]):
                return False

            dist = results["distances"][0][0]
            similarity = max(0.0, 1.0 - float(dist))
            existing_id = results["ids"][0][0]

            if similarity >= CONFLICT_SIMILARITY_DUPLICATE:
                # Exact duplicate — boost and skip
                try:
                    meta = dict(results["metadatas"][0][0])
                    meta["usage_count"] = int(meta.get("usage_count", 0)) + 1
                    meta["importance"] = min(1.0, float(meta.get("importance", 0.5)) + 0.05)
                    self.collection.update(ids=[existing_id], metadatas=[meta])
                except Exception as _ex: log.debug("Silenced exception: %s", _ex)
                return True

            if similarity >= CONFLICT_SIMILARITY_OVERRIDE:
                # Stale — prune before fresh insert
                try:
                    self.collection.delete(ids=[existing_id])
                except Exception as _ex: log.debug("Silenced exception: %s", _ex)
                return False

        except Exception as e:
            log.debug("Conflict resolution query error: %s", e)

        return False

    def _build_metadata(self, entry: StrategicPlanEntry) -> Dict[str, Any]:
        """Build ChromaDB metadata dict from a StrategicPlanEntry.

        ChromaDB metadata must contain only str/int/float/bool values.
        Complex fields (lists, nested objects) are JSON-serialized strings.
        The four required AELVO memory metadata fields are always present.
        """
        return {
            # Required AELVO memory fields
            "type": entry.type,
            "importance": float(entry.importance),
            "timestamp_unix": float(entry.timestamp_unix),
            "usage_count": int(entry.usage_count),
            "project": entry.project,
            "source_specialist": entry.source_specialist,
            # Strategic plan identity fields
            "level": entry.level.value,
            "node_id": entry.node_id,
            "parent_id": entry.parent_id or "",
            "state": entry.state.value,
            "title": entry.title[:200],
            "confidence": float(entry.confidence),
            "progress_pct": float(entry.progress_pct),
            "last_revised_unix": float(entry.last_revised_unix),
            # JSON-serialized complex fields
            "children_ids": json.dumps(entry.children_ids),
            "blocking_dependencies": json.dumps(entry.blocking_dependencies),
            "enabling_dependencies": json.dumps(entry.enabling_dependencies),
            "success_criteria": json.dumps(entry.success_criteria),
            "revision_count": len(entry.revision_history),
            "revision_history": json.dumps([
                {
                    "revision_id": r.revision_id,
                    "timestamp_unix": r.timestamp_unix,
                    "trigger_type": r.trigger_type,
                    "changes_made": r.changes_made[:200],
                    "rationale": r.rationale[:200],
                }
                for r in entry.revision_history[-5:]  # Store last 5 revisions in metadata
            ]),
            # Level-specific extras
            "mission_statement": entry.mission_statement[:400],
            "capability_area": entry.capability_area[:200],
            "target_sessions": entry.target_sessions or 0,
            # Risk assessment summary (if present)
            "overall_risk": (
                entry.risk_assessment.overall_risk.value
                if entry.risk_assessment else "unknown"
            ),
        }

    def _reconstruct_node(
        self,
        entry_id: str,
        document: str,
        meta: Dict[str, Any],
    ) -> Optional[StrategicPlanEntry]:
        """Reconstruct a StrategicPlanEntry from ChromaDB storage."""
        try:
            level_str = meta.get("level", "task")
            level = HierarchyLevel(level_str)

            # Parse JSON-serialized fields
            def _parse_list(key: str) -> List[str]:
                raw = meta.get(key, "[]")
                try:
                    return json.loads(raw) if isinstance(raw, str) else (raw or [])
                except Exception:
                    return []

            revision_history = []
            rev_raw = meta.get("revision_history", "[]")
            try:
                rev_data = json.loads(rev_raw) if isinstance(rev_raw, str) else []
                for r in rev_data:
                    revision_history.append(RevisionRecord(
                        revision_id=r.get("revision_id", ""),
                        timestamp_unix=r.get("timestamp_unix", time.time()),
                        trigger_type=r.get("trigger_type", ""),
                        changes_made=r.get("changes_made", ""),
                        rationale=r.get("rationale", ""),
                        trigger_summary=r.get("changes_made", ""),
                        previous_state_summary="",
                    ))
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)

            node = StrategicPlanEntry(
                id=entry_id,
                type=MEMORY_TYPE_STRATEGIC_PLAN,
                content=document,
                importance=float(meta.get("importance", IMPORTANCE_STRATEGIC_PLAN)),
                timestamp_unix=float(meta.get("timestamp_unix", time.time())),
                usage_count=int(meta.get("usage_count", 0)),
                project=meta.get("project", self.project),
                source_specialist="planning",
                level=level,
                node_id=meta.get("node_id", entry_id),
                parent_id=meta.get("parent_id") or None,
                state=PlanNodeState(meta.get("state", "proposed")),
                title=meta.get("title", ""),
                confidence=float(meta.get("confidence", 0.75)),
                progress_pct=float(meta.get("progress_pct", 0.0)),
                last_revised_unix=float(meta.get("last_revised_unix", time.time())),
                children_ids=_parse_list("children_ids"),
                blocking_dependencies=_parse_list("blocking_dependencies"),
                enabling_dependencies=_parse_list("enabling_dependencies"),
                success_criteria=_parse_list("success_criteria"),
                revision_history=revision_history,
                mission_statement=meta.get("mission_statement", ""),
                capability_area=meta.get("capability_area", ""),
                target_sessions=int(meta.get("target_sessions", 0)) or None,
            )
            return node
        except Exception as e:
            log.debug("Node reconstruction failed: %s", e)
            return None

    def _persist_node_update(self, node: StrategicPlanEntry) -> None:
        """Persist an updated node back to ChromaDB.

        On update, we use the same entry_id to overwrite the existing metadata.
        The document text (content) does not change on updates — only metadata fields.
        """
        try:
            meta = self._build_metadata(node)
            self.collection.update(
                ids=[node.id],
                metadatas=[meta],
            )
        except Exception as e:
            log.warning("Failed to persist node update for '%s': %s", node.title, e)

    def _propagate_progress(self, node_id: str) -> None:
        """Walk up the hierarchy updating progress percentages.

        Progress at each level is derived from the completion state of
        direct children — never estimated subjectively.
        """
        node = self._nodes.get(node_id)
        if not node or not node.parent_id:
            return

        parent = self._nodes.get(node.parent_id)
        if not parent:
            return

        children = self.get_children(parent.node_id)
        if children:
            parent.update_progress_from_children(children)
            self._persist_node_update(parent)
            # Recurse upward
            self._propagate_progress(parent.node_id)

    def _is_ancestor_of(self, node_id: str, potential_ancestor_id: str) -> bool:
        """Check if potential_ancestor_id is an ancestor of node_id (for cycle detection)."""
        ancestors = self.get_ancestors(node_id)
        return any(a.node_id == potential_ancestor_id for a in ancestors)


def find_nodes_by_level_internal(
    nodes: Dict[str, StrategicPlanEntry],
    level: HierarchyLevel,
) -> List[StrategicPlanEntry]:
    """Module-level helper for accessing level-filtered nodes without engine reference."""
    return [n for n in nodes.values() if n.level == level]
