# planning/memory_types.py - Strategic Plan Memory Type Definitions for AELVO OMEGA
# NOTE: CONFLICT_SIMILARITY_DUPLICATE and CONFLICT_SIMILARITY_OVERRIDE are imported
# from config.settings wherever they are needed (goal_hierarchy.py, etc.).
"""
Pydantic memory schemas for the Long-Horizon Planning system.

All strategic plan entries follow the same discipline as existing AELVO memory types:
- importance field (float, clamped 0.0â€“1.0)
- timestamp_unix field (float, epoch seconds)
- usage_count field (int, tracks retrieval frequency for importance boosting)
- project field (str, project-scoped isolation)
- source_specialist field (str, always "planning" for LHP entries)

These schemas are stored in the same ChromaDB collection and SQLite database
as all other AELVO memory entries. They benefit from the same semantic
retrieval, recency scoring, and deduplication that all other memory types use.
"""

import time
import hashlib
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from memory.types import MemoryEntry


# ---------------------------------------------------------------------------
# Canonical memory type strings â€” added to the existing AELVO type system
# ---------------------------------------------------------------------------

MEMORY_TYPE_STRATEGIC_PLAN = "strategic_plan"       # Goal hierarchy nodes (all 6 levels)
MEMORY_TYPE_SESSION_BOUNDARY = "session_boundary"   # Cross-session state capture
MEMORY_TYPE_DEBT_FORECAST = "debt_forecast"         # Technical debt projections
MEMORY_TYPE_CRITIQUE_AUDIT = "critique_audit"       # Self-critique defect log

# Starting importance weights for new strategic plan memory types.
# These are higher than most memory types because strategic entries have
# longer useful lifetimes and higher retrieval value across many sessions.
IMPORTANCE_STRATEGIC_PLAN = 0.85     # Mission/Objective nodes start very high
IMPORTANCE_SESSION_BOUNDARY = 0.90  # Session boundary is critical for restoration
IMPORTANCE_DEBT_FORECAST = 0.75     # Debt forecasts inform planning decisions
IMPORTANCE_CRITIQUE_AUDIT = 0.65    # Audit entries have moderate lifetime value


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class HierarchyLevel(str, Enum):
    """The six levels of the goal hierarchy, from permanent to atomic."""
    MISSION = "mission"                   # Level 1: Permanent project purpose
    STRATEGIC_OBJECTIVE = "objective"     # Level 2: Major capability area (months)
    PROGRAM = "program"                   # Level 3: Related body of work
    INITIATIVE = "initiative"             # Level 4: Focused effort with measurable outcome
    MILESTONE = "milestone"               # Level 5: Concrete deliverable
    TASK = "task"                         # Level 6: Atomic execution unit


class PlanNodeState(str, Enum):
    """Lifecycle states of any node in the goal hierarchy."""
    PROPOSED = "proposed"           # Created but not yet approved
    ACTIVE = "active"               # Currently being worked
    BLOCKED = "blocked"             # Cannot proceed due to dependency
    COMPLETE = "complete"           # All work done and verified
    TENTATIVE_COMPLETE = "tentative_complete"  # Done but verification pending
    DEFERRED = "deferred"           # Explicitly postponed
    ASPIRATIONAL = "aspirational"   # Goal stated but no milestones yet
    CANCELLED = "cancelled"         # Explicitly abandoned


class RiskLevel(str, Enum):
    """Risk severity levels for milestone and initiative risk assessments."""
    CRITICAL = "critical"   # Blocking risk â€” must be resolved before proceeding
    HIGH = "high"           # Significant risk requiring mitigation plan
    MEDIUM = "medium"       # Notable risk with known mitigation available
    LOW = "low"             # Minor risk, acceptable without specific mitigation


class EvolutionTriggerType(str, Enum):
    """The four types of events that trigger plan evolution."""
    VERIFICATION_FAILURE = "verification_failure"   # A verification contradicted a planning assumption
    CAPABILITY_DISCOVERY = "capability_discovery"   # A new capability makes something easier/possible
    RESOURCE_CONSTRAINT = "resource_constraint"     # A planned approach is now infeasible
    USER_DIRECTIVE = "user_directive"               # User explicitly changed strategic priorities


class DefectType(str, Enum):
    """Self-critique defect categories for plan quality enforcement."""
    FLOATING_TASK = "floating_task"                         # Task not connected to any milestone
    ASPIRATIONAL_OBJECTIVE = "aspirational_objective"       # Objective with no milestones
    CIRCULAR_DEPENDENCY = "circular_dependency"             # Dependency graph has cycles
    CONFIDENCE_DRIFT = "confidence_drift"                   # Confidence declining without replanning
    UNVERIFIED_COMPLETION = "unverified_completion"         # Marked complete without verification record


# ---------------------------------------------------------------------------
# Component Models (embedded in StrategicPlanEntry)
# ---------------------------------------------------------------------------

class RevisionRecord(BaseModel):
    """Immutable record of a plan revision event.

    Every time a hierarchy node is modified, a RevisionRecord is appended.
    This creates an institutional memory of why the plan changed. The
    revision history is the system's ability to learn from its own planning
    mistakes.
    """
    revision_id: str
    timestamp_unix: float = Field(default_factory=time.time)
    trigger_type: str                        # EvolutionTriggerType value
    trigger_event_id: Optional[str] = None  # ID of the triggering memory entry
    trigger_summary: str                     # Human-readable description of what triggered this
    components_affected: List[str] = Field(default_factory=list)  # Node IDs changed
    changes_made: str                        # Description of what was revised
    rationale: str                           # Why this specific revision was chosen
    previous_state_summary: str             # Snapshot of what existed before
    revised_by: str = "planning"             # Always "planning" for autonomous revisions


class VerificationStrategy(BaseModel):
    """Verification requirements that must be met before a milestone is marked complete.

    Every milestone must specify its verification strategy before it can be
    approved. This is not optional â€” it is what distinguishes a plan from a
    wishlist.
    """
    required_checks: List[str] = Field(default_factory=list)   # VerificationType values
    blocking_checks: List[str] = Field(default_factory=list)   # Checks that block completion
    success_thresholds: Dict[str, Any] = Field(default_factory=dict)  # e.g., {"test_coverage": 0.90}
    on_failure_replan_nodes: List[str] = Field(default_factory=list)  # Node IDs to mark failed
    on_failure_confidence_penalty: float = 0.10   # How much to reduce confidence on failure
    on_failure_recovery_path: str = ""             # Which recovery path to attempt first


class RiskAssessment(BaseModel):
    """Evidence-grounded risk assessment for a milestone.

    Risk is not assigned by formula. It is a judgment made by querying
    existing memory for relevant error recovery entries, security rules,
    and system decisions that bear on the milestone's domain. Each risk
    dimension cites the memory entry IDs that support its rating.
    """
    architectural_risk: RiskLevel = RiskLevel.LOW
    architectural_evidence: List[str] = Field(default_factory=list)   # Memory entry IDs
    implementation_risk: RiskLevel = RiskLevel.LOW
    implementation_evidence: List[str] = Field(default_factory=list)  # Error recovery entry IDs
    security_risk: RiskLevel = RiskLevel.LOW
    security_evidence: List[str] = Field(default_factory=list)        # Security rule entry IDs
    dependency_risk: RiskLevel = RiskLevel.LOW
    dependency_evidence: List[str] = Field(default_factory=list)      # Dependency memory IDs
    timeline_risk: RiskLevel = RiskLevel.LOW
    timeline_evidence: List[str] = Field(default_factory=list)        # Historical effort evidence
    overall_risk: RiskLevel = RiskLevel.LOW
    assessment_timestamp: float = Field(default_factory=time.time)
    assessed_by_query: str = ""  # The memory query that drove this assessment


# ---------------------------------------------------------------------------
# Primary Schema â€” StrategicPlanEntry
# ---------------------------------------------------------------------------

class StrategicPlanEntry(MemoryEntry):
    """A node in the goal hierarchy at any of the six levels.

    Follows the same Pydantic discipline as all other AELVO memory types.
    Every field that all other memory types carry is inherited from MemoryEntry:
    - id (md5 of type+timestamp+content prefix)
    - type (always MEMORY_TYPE_STRATEGIC_PLAN)
    - content (human-readable description of this node)
    - importance (float, 0.0â€“1.0, starts at IMPORTANCE_STRATEGIC_PLAN)
    - timestamp_unix (float, epoch seconds at creation)
    - usage_count (int, incremented on retrieval)
    - project (str, project-scoped isolation)
    - source_specialist (str, always "planning")

    Every node at every level knows its parent, its children, its current
    state, its confidence score, its blocking dependencies, and its
    last-updated timestamp. Every task traces back through the hierarchy
    to a Strategic Objective.
    """
    # Required from MemoryEntry but set here to fixed values for planning entries
    type: str = MEMORY_TYPE_STRATEGIC_PLAN
    importance: float = IMPORTANCE_STRATEGIC_PLAN
    source_specialist: str = "planning"

    # --- Hierarchy Identity ---
    level: HierarchyLevel
    node_id: str = ""               # Stable planning node identifier (separate from memory entry id)
    parent_id: Optional[str] = None  # node_id of parent node (None only for Mission)
    children_ids: List[str] = Field(default_factory=list)   # node_ids of direct children

    # --- State and Progress ---
    state: PlanNodeState = PlanNodeState.PROPOSED
    progress_pct: float = 0.0      # 0.0â€“100.0, derived from milestone completion (not estimated)
    confidence: float = 0.75       # 0.0â€“1.0, probability of successful completion

    # --- Strategic Content ---
    title: str = ""                  # Short title for display
    success_criteria: List[str] = Field(default_factory=list)   # Measurable completion signals
    blocking_dependencies: List[str] = Field(default_factory=list)  # node_ids that must complete first
    enabling_dependencies: List[str] = Field(default_factory=list)  # node_ids this unlocks

    # --- Risk and Verification (milestone and task levels) ---
    risk_assessment: Optional[RiskAssessment] = None
    verification_strategy: Optional[VerificationStrategy] = None

    # --- Planning Metadata ---
    revision_history: List[RevisionRecord] = Field(default_factory=list)
    last_revised_unix: float = Field(default_factory=time.time)
    session_created: int = 0          # Turn counter when this node was created
    target_sessions: Optional[int] = None   # Estimated sessions to complete (if known)

    # --- Mission-level extras (only populated for level == MISSION) ---
    mission_statement: str = ""

    # --- Objective-level extras (only for level == STRATEGIC_OBJECTIVE) ---
    capability_area: str = ""          # What capability is being developed
    planned_completion_session: Optional[int] = None   # Target session for completion

    def __init__(self, **data):
        super().__init__(**data)
        # Generate stable node_id if not provided
        if not self.node_id:
            raw = f"{self.level.value}_{self.project}_{self.title}_{self.timestamp_unix}"
            self.node_id = "lhp_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def update_progress_from_children(self, children: List["StrategicPlanEntry"]) -> None:
        """Derive progress from actual milestone/task completion rather than estimation."""
        if not children:
            return
        complete_count = sum(
            1 for c in children
            if c.state in (PlanNodeState.COMPLETE, PlanNodeState.TENTATIVE_COMPLETE)
        )
        self.progress_pct = round((complete_count / len(children)) * 100.0, 1)

    def record_revision(
        self,
        trigger_type: str,
        trigger_summary: str,
        changes_made: str,
        rationale: str,
        trigger_event_id: Optional[str] = None,
        components_affected: Optional[List[str]] = None,
        previous_state_summary: str = "",
    ) -> RevisionRecord:
        """Append a revision record and update last_revised_unix."""
        rev_id = hashlib.sha256(
            f"rev_{self.node_id}_{time.time()}".encode("utf-8")
        ).hexdigest()[:12]
        record = RevisionRecord(
            revision_id=rev_id,
            timestamp_unix=time.time(),
            trigger_type=trigger_type,
            trigger_event_id=trigger_event_id,
            trigger_summary=trigger_summary,
            components_affected=components_affected or [self.node_id],
            changes_made=changes_made,
            rationale=rationale,
            previous_state_summary=previous_state_summary,
        )
        self.revision_history.append(record)
        self.last_revised_unix = time.time()
        return record


# ---------------------------------------------------------------------------
# Session Boundary Record
# ---------------------------------------------------------------------------

class SessionBoundaryRecord(BaseModel):
    """Atomic snapshot of strategic state at session end.

    Written at the end of every session via temp-file atomic rename.
    Distinct from the session summary that the orchestrator already writes.
    The session summary records what happened. This record captures what
    was in progress and what to do next.

    This is the only place where Long-Horizon Planning writes proactively
    to disk rather than reactively to memory.
    """
    record_id: str
    timestamp_unix: float = Field(default_factory=time.time)
    project: str
    session_turn_count: int

    # Active hierarchy state
    mission_node_id: Optional[str] = None
    active_objective_ids: List[str] = Field(default_factory=list)
    active_milestone_ids: List[str] = Field(default_factory=list)
    complete_milestone_ids: List[str] = Field(default_factory=list)

    # Interrupted work state
    interrupted_initiative_id: Optional[str] = None
    interrupted_milestone_id: Optional[str] = None
    interrupted_pct: float = 0.0
    last_active_specialist: str = ""
    next_concrete_step: str = ""          # What to do first in the next session
    restoration_context: str = ""         # Full context blob for session restore

    # Strategic summary for fast loading
    objectives_summary: List[Dict[str, Any]] = Field(default_factory=list)
    high_priority_next_actions: List[str] = Field(default_factory=list)

    # Audit
    confidence_snapshot: Dict[str, float] = Field(default_factory=dict)  # node_id â†’ confidence
    open_risks: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Debt Forecast Entry
# ---------------------------------------------------------------------------

class DebtForecastEntry(MemoryEntry):
    """Technical debt forecast for a subsystem or file path.

    Built entirely from evidence in existing memory â€” never invented.
    Each debt signal points to the memory entry IDs that detected it.
    """
    type: str = MEMORY_TYPE_DEBT_FORECAST
    importance: float = IMPORTANCE_DEBT_FORECAST
    source_specialist: str = "planning"

    # Debt location
    subsystem: str = ""              # Module path or subsystem name
    file_paths: List[str] = Field(default_factory=list)

    # Evidence from memory
    error_recovery_count: int = 0    # Error recovery entries in this subsystem
    error_recovery_ids: List[str] = Field(default_factory=list)   # Memory entry IDs
    security_violation_count: int = 0
    security_violation_ids: List[str] = Field(default_factory=list)
    decision_reversal_count: int = 0  # System decisions that were revised
    decision_reversal_ids: List[str] = Field(default_factory=list)
    lint_violation_growth: float = 0.0  # Rate of lint violations per session

    # Debt classification
    implementation_debt_score: float = 0.0   # 0.0â€“1.0
    security_debt_score: float = 0.0
    design_debt_score: float = 0.0
    quality_debt_score: float = 0.0
    overall_debt_score: float = 0.0   # Weighted composite

    # Forward projection
    planned_changes_complexity: str = "low"  # low/medium/high
    projected_milestone: Optional[str] = None  # node_id of the milestone affected
    risk_at_milestone: RiskLevel = RiskLevel.LOW
    remediation_initiative_id: Optional[str] = None  # If remediation has been created


# ---------------------------------------------------------------------------
# Self-Critique Defect
# ---------------------------------------------------------------------------

class SelfCritiqueDefect(MemoryEntry):
    """A planning defect detected by the self-critique engine.

    Recorded in the audit log with specific defect type, the plan element
    it applies to, and the recommended correction. Defects that appear in
    three or more consecutive runs without resolution are escalated.
    """
    type: str = MEMORY_TYPE_CRITIQUE_AUDIT
    importance: float = IMPORTANCE_CRITIQUE_AUDIT
    source_specialist: str = "planning"

    defect_type: DefectType
    affected_node_id: str             # The plan hierarchy node with the defect
    affected_node_title: str = ""
    defect_description: str           # What exactly is wrong
    recommended_correction: str       # How to fix it
    consecutive_run_count: int = 1    # How many self-critique runs have seen this defect
    escalated: bool = False           # True if escalated to user attention
    resolved: bool = False
    resolved_unix: Optional[float] = None
    critique_run_id: str = ""         # Which self-critique run detected this
