# learning/types.py - Complete Type System for Pattern Extraction & Knowledge Learning
# Dependency-graph-level pattern types, edit categories, confidence schema
# Phase 10: Collaboration & Consensus types added

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional, Set
from pydantic import BaseModel, Field, computed_field
from datetime import datetime, timezone
import hashlib

# ── Re-export repo_intelligence types we depend on ────────────────────────────

from repo_intelligence.types import (
    EdgeType, SymbolKind, ConfidenceLevel,
)


# ── Edit Category Enum ────────────────────────────────────────────────────────

class EditCategory(str, Enum):
    """Structural classification of a dependency graph transformation.

    Each category captures a specific kind of change to the codebase
    structure. Categories are derived from graph delta analysis, not
    from tool call sequences.
    """
    ADD_IMPORT_DEPENDENCY = "add_import_dependency"
    """A new import edge was added between two files (A now imports B)."""

    REMOVE_IMPORT_DEPENDENCY = "remove_import_dependency"
    """An existing import edge was removed (A no longer imports B)."""

    ADD_CALL_DEPENDENCY = "add_call_dependency"
    """A new function call edge was added (function X now calls function Y)."""

    ADD_INHERITANCE = "add_inheritance"
    """A new inheritance relationship was established (class A now extends class B)."""

    ADD_IMPLEMENTS = "add_implements"
    """A new implementation relationship was established (class A now implements interface B)."""

    REFACTOR_INTERNAL = "refactor_internal"
    """Changes within a file's internal structure, no new cross-file edges."""

    ADD_FILE = "add_file"
    """An entirely new file was added to the codebase."""

    DELETE_FILE = "delete_file"
    """A file was removed from the codebase."""

    MODIFY_SYMBOL_SIGNATURE = "modify_symbol_signature"
    """A function or method signature was changed (parameters, return type)."""

    BREAK_CYCLE = "break_cycle"
    """A dependency cycle was resolved."""

    CREATE_CYCLE = "create_cycle"
    """A new dependency cycle was introduced (usually a mistake)."""

    ADD_LAYER = "add_layer"
    """A new module/layer was added at a specific position in the topological order."""

    CHANGE_EXPORT_STATUS = "change_export_status"
    """A symbol's export visibility was changed (public → private or vice versa)."""

    CHANGE_TYPE_ANNOTATION = "change_type_annotation"
    """A type annotation was added, removed, or changed."""

    MIXED = "mixed"
    """Multiple incompatible structural changes in a single edit."""


# ── Validation State Enum ─────────────────────────────────────────────────────

class ValidationState(str, Enum):
    """Confidence in a knowledge item's correctness.

    - OBSERVED: Derived from observation only, not yet confirmed
    - VALIDATED: Confirmed by a subsequent successful use
    - DEPRECATED: Superseded by newer/revised knowledge
    - CONTRADICTED: In conflict with other knowledge
    """
    OBSERVED = "observed"
    VALIDATED = "validated"
    DEPRECATED = "deprecated"
    CONTRADICTED = "contradicted"


# ── Freshness Grade Enum ──────────────────────────────────────────────────────

class FreshnessGrade(str, Enum):
    FRESH = "fresh"
        # Recently observed, within expected validity window
    AGING = "aging"
        # Past the freshness window but still potentially valid
    STALE = "stale"
        # Significantly past freshness, should be re-evaluated
    UNKNOWN = "unknown"
        # No freshness information available


# ── Graph Delta Types ─────────────────────────────────────────────────────────

class GraphDeltaEdge(BaseModel):
    """A single edge change in the dependency graph delta.

    Captures one added or removed edge with its full context.
    """
    edge_type: EdgeType
    source_file_id: str = ""
    source_file_path: str = ""
    source_symbol_name: str = ""
    target_file_id: str = ""
    target_file_path: str = ""
    target_symbol_name: str = ""
    confidence: ConfidenceLevel = ConfidenceLevel.INFERRED

    @property
    def edge_key(self) -> str:
        """Deterministic key for deduplication."""
        return f"{self.edge_type.value}:{self.source_file_id}:{self.target_file_id}"


class DependencyGraphDelta(BaseModel):
    """Structured difference between two dependency graph snapshots.

    Computed by DeltaComputer. Forms the raw material for pattern extraction.
    """
    before_version: int = 0
    after_version: int = 0
    new_edges: List[GraphDeltaEdge] = Field(default_factory=list)
    removed_edges: List[GraphDeltaEdge] = Field(default_factory=list)
    added_files: List[str] = Field(default_factory=list)
    removed_files: List[str] = Field(default_factory=list)
    modified_files: List[str] = Field(default_factory=list)
    new_cycles: List[Set[str]] = Field(default_factory=list)
    resolved_cycles: List[Set[str]] = Field(default_factory=list)
    topological_shift: int = 0
    edge_count_delta: int = 0
    file_count_delta: int = 0
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_empty(self) -> bool:
        """True if nothing changed between snapshots."""
        return (
            len(self.new_edges) == 0
            and len(self.removed_edges) == 0
            and len(self.added_files) == 0
            and len(self.removed_files) == 0
            and len(self.modified_files) == 0
        )

    @property
    def has_structural_change(self) -> bool:
        """True if there are cross-file structural changes beyond internal refactoring."""
        return (
            len(self.new_edges) > 0
            or len(self.removed_edges) > 0
            or len(self.added_files) > 0
            or len(self.removed_files) > 0
            or len(self.new_cycles) > 0
            or len(self.resolved_cycles) > 0
        )

    @property
    def dominant_edge_type(self) -> Optional[EdgeType]:
        """Most common edge type among new edges (if any)."""
        if not self.new_edges:
            return None
        counts: Dict[EdgeType, int] = {}
        for e in self.new_edges:
            counts[e.edge_type] = counts.get(e.edge_type, 0) + 1
        return max(counts, key=counts.get)  # type: ignore

    def to_digest(self) -> str:
        """Compact hash for deduplication of identical deltas.

        Includes the actual edge content (not just counts) so that
        two deltas with the same number of edges but different
        edges produce different digests.
        """
        edge_keys_new = ",".join(sorted(e.edge_key for e in self.new_edges))
        edge_keys_removed = ",".join(sorted(e.edge_key for e in self.removed_edges))
        raw = (
            f"new_edges=[{edge_keys_new}]:"
            f"removed_edges=[{edge_keys_removed}]:"
            f"added_files={sorted(self.added_files)}:"
            f"removed_files={sorted(self.removed_files)}:"
            f"new_cycles_count={len(self.new_cycles)}:"
            f"topo_shift={self.topological_shift}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ── Edit Category Signature ───────────────────────────────────────────────────

class EditCategorySignature(BaseModel):
    """Structural fingerprint of an edit. Identifies the category of change.

    This signature is what gets matched against existing patterns to
    determine if a new observation reinforces an existing pattern or
    constitutes a new pattern.
    """
    category: EditCategory = EditCategory.REFACTOR_INTERNAL
    dominant_edge_type: Optional[EdgeType] = None
    file_count_delta: int = 0
    edge_count_delta: int = 0
    cycle_introduced: bool = False
    cycle_resolved: bool = False
    added_symbol_kinds: List[SymbolKind] = Field(default_factory=list)
    topological_position: Optional[str] = None
        # "entry", "middle", "leaf" — where in the dependency order the change happened

    @property
    def signature_hash(self) -> str:
        """Deterministic hash for matching identical signatures."""
        kinds_str = ",".join(sorted(k.value for k in self.added_symbol_kinds))
        raw = (
            f"{self.category.value}:{self.dominant_edge_type}:"
            f"{self.file_count_delta}:{self.edge_count_delta}:"
            f"{self.cycle_introduced}:{self.cycle_resolved}:{kinds_str}:"
            f"{self.topological_position}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# ── Subgraph Types ────────────────────────────────────────────────────────────

class SubgraphNode(BaseModel):
    """A single node in a pattern's subgraph specification."""
    file_id: str = ""
    file_path: str = ""
    symbol_name: str = ""
    symbol_kind: SymbolKind = SymbolKind.MODULE
    is_new: bool = False
    is_anchor: bool = False
        # The anchor node is the "center" of the pattern

    @property
    def node_key(self) -> str:
        return self.file_id or self.file_path


class SubgraphEdge(BaseModel):
    """A single edge in a pattern's subgraph specification."""
    source_key: str
    target_key: str
    edge_type: EdgeType

    @property
    def edge_key(self) -> str:
        return f"{self.edge_type.value}:{self.source_key}:{self.target_key}"


class SubgraphSpec(BaseModel):
    """Minimal typed subgraph that captures the essence of a structural pattern.

    Uses symbolic node references (not absolute file paths) so that
    the same pattern can match different projects with different
    file names but the same structural relationships.
    """
    anchor_node_key: str = ""
        # The "center" node of the pattern — the file/symbol most affected
    nodes: List[SubgraphNode] = Field(default_factory=list)
    edges: List[SubgraphEdge] = Field(default_factory=list)
    node_count: int = 0
    edge_count: int = 0
    category: EditCategory = EditCategory.REFACTOR_INTERNAL
    is_isomorphic_to: Optional[str] = None
        # If this subgraph is isomorphic to another, link by ID

    def get_anchor(self) -> Optional[SubgraphNode]:
        for n in self.nodes:
            if n.is_anchor:
                return n
        return None


# ── Engineering Pattern ───────────────────────────────────────────────────────

class EngineeringPattern(BaseModel):
    """A generalized, reusable insight extracted from multiple observations.

    This is the core knowledge type. It represents a structural pattern
    that AELVO has learned from observing multiple dependency graph deltas
    that share the same structural fingerprint.
    """
    id: str = ""
    category: EditCategory
    category_signature: EditCategorySignature = Field(default_factory=EditCategorySignature)
    subgraph: SubgraphSpec = Field(default_factory=SubgraphSpec)

    # Confidence & evidence
    confidence: float = 0.3
        # Initial confidence for a single observation (weak heuristic)
    observation_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    validation_state: ValidationState = ValidationState.OBSERVED

    # Freshness
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_observed: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_used: Optional[datetime] = None
    freshness: float = 1.0
        # 1.0 = just observed, decays toward 0

    # Provenance
    provenance: List[str] = Field(default_factory=list)
        # Observation IDs that contributed to this pattern
    source_specialist: Optional[str] = None
    project_scope: Optional[str] = None

    # Graph relationships
    related_pattern_ids: List[str] = Field(default_factory=list)

    def to_digest(self) -> str:
        """Deterministic ID from category + signature hash."""
        if not self.id:
            raw = f"{self.category.value}:{self.category_signature.signature_hash}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.0
        return self.success_count / total


class PatternObservation(BaseModel):
    """A single observation that contributes evidence to a pattern.

    Each observation records one execution outcome that relates
    to a specific EngineeringPattern.
    """
    id: str = ""
    pattern_id: str
    delta_digest: str
        # Hash of the DependencyGraphDelta that produced this observation
    outcome: str
        # "success" or "failure"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    confidence_impact: float = 0.0
        # How much this observation changed the pattern's confidence
    task_description: str = ""

    def to_id(self) -> str:
        if not self.id:
            raw = f"{self.pattern_id}:{self.delta_digest}:{self.outcome}:{self.timestamp.isoformat()}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id


# ── Confidence System Types ───────────────────────────────────────────────────

class ConfidenceUpdate(BaseModel):
    """A record of a change to a knowledge item's confidence level."""
    id: str = ""
    knowledge_item_id: str
    previous_confidence: float
    new_confidence: float
    update_formula: str = ""
        # e.g., "bonus = 0.1 * (1.0 - 0.5)" for human-readable audit
    evidence: str = ""
        # What evidence triggered the update
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_id(self) -> str:
        if not self.id:
            raw = f"{self.knowledge_item_id}:{self.previous_confidence}:{self.new_confidence}:{self.timestamp.isoformat()}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id


# ── Contradiction Types ───────────────────────────────────────────────────────

class ContradictionRecord(BaseModel):
    """Records a detected contradiction between old knowledge and new evidence.

    Every contradiction is explicitly recorded so the resolution
    reasoning is transparent and auditable.
    """
    id: str = ""
    old_knowledge_id: str
    new_knowledge_id: str
    contradiction_type: str = ""
        # "factual", "pattern_invalidation", "recipe_failure", "scope_conflict"
    resolution_strategy: str = ""
        # "accept_new", "retain_old", "retain_both_with_scope", "schedule_reevaluation"
    resolution_accepted: Optional[str] = None
        # Which item ID survived (or None if both retained)
    resolution_rejected: Optional[str] = None
        # Which item ID was deprecated (or None if both retained)
    reasoning: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: Optional[datetime] = None
    resolved: bool = False
    confidence_impact: float = 0.0
        # How confidence changed on the surviving item

    def to_id(self) -> str:
        if not self.id:
            raw = f"{self.old_knowledge_id}:{self.new_knowledge_id}:{self.created_at.isoformat()}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id


# ── Freshness Decay Configuration ─────────────────────────────────────────────

class FreshnessConfig(BaseModel):
    """Configuration for freshness decay per knowledge type.

    Different knowledge types decay at different rates. For example:
    - Type annotations change rarely → slow decay (30 days)
    - Package imports change frequently → fast decay (7 days)
    - Internal refactoring patterns → medium decay (14 days)
    """
    max_age_days: int = 14
        # After this many days, freshness reaches 0
    initial_freshness: float = 1.0
    decay_function: str = "linear"
        # "linear", "exponential", or "step"


# ── Query Types ───────────────────────────────────────────────────────────────

class PatternQuery(BaseModel):
    """Structured query for finding relevant patterns."""
    category: Optional[EditCategory] = None
    min_confidence: float = 0.0
    min_freshness: float = 0.0
    min_observations: int = 0
    validation_state: Optional[ValidationState] = None
    project_scope: Optional[str] = None
    source_specialist: Optional[str] = None
    max_results: int = 10

    def match(self, pattern: EngineeringPattern) -> bool:
        """Check if a pattern matches this query."""
        if self.category and pattern.category != self.category:
            return False
        if pattern.confidence < self.min_confidence:
            return False
        if pattern.freshness < self.min_freshness:
            return False
        if pattern.observation_count < self.min_observations:
            return False
        if self.validation_state and pattern.validation_state != self.validation_state:
            return False
        if self.project_scope and pattern.project_scope != self.project_scope:
            return False
        if self.source_specialist and pattern.source_specialist != self.source_specialist:
            return False
        return True


class PatternQueryResult(BaseModel):
    """Result of a pattern query with ranked matches."""
    query: PatternQuery
    patterns: List[EngineeringPattern] = Field(default_factory=list)
    total_matched: int = 0
    query_duration_ms: float = 0.0
    from_cache: bool = False


# ── Delta source metadata ─────────────────────────────────────────────────────

class DeltaSource(BaseModel):
    """Metadata about where a delta came from."""
    task_id: str = ""
    agent_id: str = ""
    specialist: str = ""
    project: str = ""
    task_description: str = ""
    outcome: str = "success"
        # "success" or "failure"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    execution_duration_ms: float = 0.0


# ── Analytics Types ───────────────────────────────────────────────────────────

class SessionRecord(BaseModel):
    """Complete record of a learning session's metrics."""
    session_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    deltas_processed: int = 0
    patterns_created: int = 0
    patterns_updated: int = 0
    contradictions_detected: int = 0
    patterns_pruned: int = 0
    avg_confidence: float = 0.0
    total_patterns_end: int = 0
    specialist_activity: Dict[str, int] = Field(default_factory=dict)
        # Maps specialist name → number of deltas they contributed
    category_distribution: Dict[str, int] = Field(default_factory=dict)
    first_attempts: int = 0
    first_attempt_successes: int = 0

    @property
    def first_attempt_success_rate(self) -> float:
        if self.first_attempts == 0:
            return 0.0
        return self.first_attempt_successes / self.first_attempts


class CalibrationBin(BaseModel):
    """A single bin in the confidence calibration histogram.

    Divides [0, 1] into N equal-width bins and tracks the
    accuracy within each bin. Perfect calibration means
    accuracy == bin_center for every bin.
    """
    bin_lower: float
    bin_upper: float
    bin_center: float
    count: int = 0
        # Number of predictions in this bin
    accuracy: float = 0.0
        # Fraction of correct predictions in this bin
    confidence: float = 0.0
        # Average confidence in this bin


class TrendPoint(BaseModel):
    """A single data point in a trend series."""
    timestamp: datetime
    value: float
    label: str = ""


class TrendDirection(str, Enum):
    """Direction of a trend."""
    IMPROVING = "improving"
    DEGRADING = "degrading"
    STABLE = "stable"
    INSUFFICIENT_DATA = "insufficient_data"


class TrendSeries(BaseModel):
    """A time-ordered series of data points with computed trend."""
    name: str
    points: List[TrendPoint] = Field(default_factory=list)
    direction: TrendDirection = TrendDirection.INSUFFICIENT_DATA
    slope: float = 0.0
        # Linear regression slope (positive = improving)
    r_squared: float = 0.0
        # Goodness of fit (0.0-1.0)
    min_value: float = 0.0
    max_value: float = 0.0
    avg_value: float = 0.0

    @property
    def count(self) -> int:
        return len(self.points)


class FirstAttemptRecord(BaseModel):
    """Records whether a specialist succeeded on their first attempt at a task.

    Used to measure whether pattern learning improves first-try success rates
    across sessions.
    """
    id: str = ""
    specialist: str
    task_description: str
    succeeded: bool
    session_id: str
    pattern_id: Optional[str] = None
        # Which pattern was used (if any)
    confidence_at_time: float = 0.0
        # The pattern's confidence when this attempt was made
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_id(self) -> str:
        if not self.id:
            raw = f"{self.specialist}:{self.session_id}:{self.task_description}:{self.timestamp.isoformat()}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id


class SpecialistLearningCurve(BaseModel):
    """Learning progress for a single specialist across sessions."""
    specialist: str
    session_records: List[SessionRecord] = Field(default_factory=list)
    first_attempt_trend: TrendSeries = Field(default_factory=lambda: TrendSeries(name="first_attempt_success_rate"))
    confidence_trend: TrendSeries = Field(default_factory=lambda: TrendSeries(name="avg_confidence"))
    pattern_count_trend: TrendSeries = Field(default_factory=lambda: TrendSeries(name="pattern_count"))
    total_first_attempts: int = 0
    total_first_attempt_successes: int = 0

    @computed_field
    @property
    def overall_first_attempt_success_rate(self) -> float:
        if self.total_first_attempts == 0:
            return 0.0
        return self.total_first_attempt_successes / self.total_first_attempts

    @computed_field
    @property
    def session_count(self) -> int:
        return len(self.session_records)


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 10: Collaboration & Consensus Types
# ═══════════════════════════════════════════════════════════════════════════════


class ConsensusOutcome(str, Enum):
    """Outcome of a consensus process."""
    AGREED = "agreed"
    PARTIAL = "partial"
    DISAGREED = "disagreed"
    VETOED = "vetoed"
    ESCALATED = "escalated"


class CollaborationEventType(str, Enum):
    """Types of collaboration events that can be learned from."""
    CONSENSUS_REACHED = "consensus_reached"
    BLACKBOARD_PUBLISHED = "blackboard_published"
    CHALLENGE_RAISED = "challenge_raised"
    CHALLENGE_RESOLVED = "challenge_resolved"
    ARCHITECT_DECIDED = "architect_decided"
    TASK_COMPLETED = "task_completed"
    SPECIALIST_COLLABORATED = "specialist_collaborated"


class ConsensusMemoryRecord(BaseModel):
    """A persistence-ready snapshot of a consensus outcome for ChromaDB storage.

    This is the flat, serializable form of a ConsensusEvent that gets
    written to the vector store for semantic retrieval.
    """
    id: str = ""
    consensus_id: str
    topic: str
    outcome: ConsensusOutcome
    confidence: float = 0.0
    participant_count: int = 0
    specialists_involved: List[str] = Field(default_factory=list)
    vote_summary: str = ""
        # Human-readable summary of votes: "FORGE: yes, SENTINEL: no, ..."
    vetoed: bool = False
    veto_reason: str = ""
    governance_applied: bool = False
    architect_override: bool = False
    session_id: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    content: str = ""
        # Full content string for ChromaDB vector embedding

    def build_content(self) -> str:
        """Build the ChromaDB document content from structured fields.

        Returns the formatted content string without mutating the record.
        Callers should use this to get the serializable text for ChromaDB.
        """
        parts = [
            f"Consensus: {self.topic}",
            f"Outcome: {self.outcome.value}",
            f"Confidence: {self.confidence:.2f}",
            f"Participants: {', '.join(self.specialists_involved)}",
        ]
        if self.vote_summary:
            parts.append(f"Votes: {self.vote_summary}")
        if self.vetoed:
            parts.append(f"Vetoed: {self.veto_reason}")
        if self.architect_override:
            parts.append("Architect override applied")
        return "\n".join(parts)


class CollaborationObservation(BaseModel):
    """A single observation of a collaboration event for pattern learning.

    Similar to PatternObservation but for collaboration patterns.
    """
    id: str = ""
    collaboration_id: str
    event_type: CollaborationEventType
    specialists_involved: List[str] = Field(default_factory=list)
    outcome: str = ""
        # "success", "failure", "agreed", "disagreed", etc.
    confidence: float = 0.0
    duration_ms: float = 0.0
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    description: str = ""

    def to_id(self) -> str:
        if not self.id:
            raw = f"{self.event_type.value}:{self.collaboration_id}:{self.timestamp.isoformat()}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id


class CollaborationSignature(BaseModel):
    """A structural fingerprint of a collaboration pattern.

    Defines what makes one collaboration event similar to another
    for the purpose of pattern matching.
    """
    event_type: CollaborationEventType = CollaborationEventType.CONSENSUS_REACHED
    participant_count: int = 0
    specialist_roles: List[str] = Field(default_factory=list)
        # Ordered list of roles involved (e.g., ["FORGE", "SENTINEL", "ARCHITECT"])
    had_conflict: bool = False
    required_architect_override: bool = False

    @property
    def signature_hash(self) -> str:
        """Deterministic hash for matching identical signatures."""
        roles = ",".join(sorted(self.specialist_roles))
        raw = (
            f"{self.event_type.value}:{self.participant_count}:"
            f"{roles}:{self.had_conflict}:{self.required_architect_override}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class CollaborationPattern(BaseModel):
    """A learned pattern about how specialists collaborate effectively.

    Captures recurring collaboration structures, such as:
    - "FORGE → SENTINEL review → FORGE revision → consensus"
    - "ARCHITECT decides after FORGE/SENTINEL disagreement"
    - "Two specialists resolve quickly via blackboard"
    """
    id: str = ""
    signature: CollaborationSignature = Field(default_factory=CollaborationSignature)

    # Confidence & evidence
    observation_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    confidence: float = 0.3
    avg_duration_ms: float = 0.0

    # Provenance
    provenance: List[str] = Field(default_factory=list)
    source_session: Optional[str] = None

    # Timing
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_observed: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    freshness: float = 1.0

    def to_digest(self) -> str:
        if not self.id:
            raw = f"{self.signature.signature_hash}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id

    @property
    def success_rate(self) -> float:
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.0
        return self.success_count / total


class SpecialistEffectivenessRecord(BaseModel):
    """A record of a specialist's effectiveness in a session.

    Tracks quantitative metrics for measuring whether learning
    improves specialist performance over time.
    """
    id: str = ""
    specialist: str
    session_id: str
    tasks_attempted: int = 0
    tasks_succeeded: int = 0
    first_attempts: int = 0
    first_attempt_successes: int = 0
    total_duration_ms: float = 0.0
    consensus_participations: int = 0
    consensus_aligned: int = 0
        # Times the specialist's position matched the final consensus
    patterns_contributed: int = 0
    blackboard_publications: int = 0
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def success_rate(self) -> float:
        if self.tasks_attempted == 0:
            return 0.0
        return self.tasks_succeeded / self.tasks_attempted

    @property
    def first_attempt_success_rate(self) -> float:
        if self.first_attempts == 0:
            return 0.0
        return self.first_attempt_successes / self.first_attempts

    @property
    def consensus_alignment_rate(self) -> float:
        if self.consensus_participations == 0:
            return 0.0
        return self.consensus_aligned / self.consensus_participations

    def to_id(self) -> str:
        if not self.id:
            raw = f"{self.specialist}:{self.session_id}"
            self.id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        return self.id
