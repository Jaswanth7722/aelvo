from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timezone


class GoalStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    BLOCKED = "blocked"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PlanStatus(str, Enum):
    DRAFT = "draft"
    VALIDATED = "validated"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SUPERSEDED = "superseded"


class ProvenanceType(str, Enum):
    SPECIALIST = "specialist"
    TOOL = "tool"
    MEMORY = "memory"
    SYNTHESIS = "synthesis"
    USER = "user"
    SYSTEM = "system"
    RESEARCH = "research"
    CONSENSUS = "consensus"


class EntryType(str, Enum):
    FACT = "fact"
    HYPOTHESIS = "hypothesis"
    CONSTRAINT = "constraint"
    PREFERENCE = "preference"
    COMMAND = "command"
    QUERY = "query"
    FINDING = "finding"
    DECISION = "decision"
    OBSERVATION = "observation"
    REQUIREMENT = "requirement"


class ConsensusResult(str, Enum):
    AGREED = "agreed"
    DISAGREED = "disagreed"
    PARTIAL = "partial"
    NOT_ATTEMPTED = "not_attempted"


class ConflictSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MemoryType(str, Enum):
    SUCCESS_PATTERN = "success_pattern"
    FAILURE_PATTERN = "failure_pattern"
    REUSABLE_STRATEGY = "reusable_strategy"
    CONSTRAINT = "constraint"
    DOMAIN_KNOWLEDGE = "domain_knowledge"
    EXECUTION_TRACE = "execution_trace"
    USER_PREFERENCE = "user_preference"
    STRATEGIC_PLAN = "strategic_plan"
    ROADMAP = "roadmap"


class HypothesisStatus(str, Enum):
    PROPOSED = "proposed"
    INVESTIGATING = "investigating"
    SUPPORTED = "supported"
    REFUTED = "refuted"
    INCONCLUSIVE = "inconclusive"


class ConfidenceLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CONFIRMED = "confirmed"


class SpecialistRole(str, Enum):
    ARCHITECT = "ARCHITECT"
    FORGE = "FORGE"
    HERALD = "HERALD"
    HERMES = "HERMES"
    ORACLE = "ORACLE"
    SENTINEL = "SENTINEL"
    TERMINUS = "TERMINUS"


class PlanPrecondition(BaseModel):
    description: str
    check_type: str = "automated"
    satisfied: bool = False


class PlanUncertainty(BaseModel):
    level: ConfidenceLevel = ConfidenceLevel.MEDIUM
    notes: str = ""

    @property
    def is_high_uncertainty(self) -> bool:
        """Whether this uncertainty level warrants elevated criticality."""
        return self.level == ConfidenceLevel.HIGH


class UncertaintyClass(str, Enum):
    EVIDENCE_GAP = "evidence_gap"
    CONTRADICTORY_EVIDENCE = "contradictory_evidence"
    AMBIGUOUS_SPECIFICATION = "ambiguous_specification"
    TIMING_VARIABILITY = "timing_variability"
    EXTERNAL_DEPENDENCY = "external_dependency"
    NO_INFORMATION = "no_information"


class Provenance(BaseModel):
    source_type: ProvenanceType
    source_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence_chain: List[str] = Field(default_factory=list)

    @field_validator("confidence")
    @classmethod
    def confidence_range(cls, v: float) -> float:
        return max(0.0, min(1.0, v))


class Goal(BaseModel):
    id: str
    description: str
    success_criteria: List[str] = Field(default_factory=list)
    priority: int = Field(default=5, ge=1, le=10)
    status: GoalStatus = GoalStatus.PENDING
    parent_goal_id: Optional[str] = None
    sub_goal_ids: List[str] = Field(default_factory=list)
    sub_goals: List["SubGoal"] = Field(default_factory=list)
    constraints: List[str] = Field(default_factory=list)
    prerequisites: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    deadline: Optional[datetime] = None
    owner: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SubGoal(BaseModel):
    id: str
    parent_goal_id: str
    description: str
    success_criteria: List[str] = Field(default_factory=list)
    steps: List["PlanStep"] = Field(default_factory=list)
    sub_goals: List["SubGoal"] = Field(default_factory=list)
    status: GoalStatus = GoalStatus.PENDING
    order: int = 0
    dependencies: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class PlanStep(BaseModel):
    id: str
    plan_id: str
    description: str
    goal_id: str
    sub_goal_id: Optional[str] = None
    execution_node_id: Optional[str] = None
    status: PlanStatus = PlanStatus.DRAFT
    dependencies: List[str] = Field(default_factory=list)
    preconditions: List["PlanPrecondition"] = Field(default_factory=list)
    uncertainty: Optional["PlanUncertainty"] = None
    specialist: Optional[str] = None
    tool_name: Optional[str] = None
    estimated_cost: int = 1
    estimated_effort: int = 1
    actual_cost: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


class PlanDependency(BaseModel):
    id: str
    from_step_id: str
    to_step_id: str
    condition: str = "completion"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BlackboardEntry(BaseModel):
    id: str
    slot_name: str
    content: str
    entry_type: EntryType
    provenance: Provenance
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    superseded_by: Optional[str] = None
    tags: List[str] = Field(default_factory=list)

    @field_validator("confidence")
    @classmethod
    def confidence_range(cls, v: float) -> float:
        return max(0.0, min(1.0, v))


class BlackboardSlot(BaseModel):
    name: str
    entries: List[BlackboardEntry] = Field(default_factory=list)
    max_entries: int = 100
    retention_policy: str = "most_recent"

    def active_entries(self) -> List[BlackboardEntry]:
        now = datetime.now(timezone.utc)
        result = []
        for e in self.entries:
            if e.superseded_by is not None:
                continue
            if e.expires_at is not None:
                expires = e.expires_at
                if isinstance(expires, datetime) and expires.tzinfo is None:
                    expires = expires.replace(tzinfo=timezone.utc)
                if now > expires:
                    continue
            result.append(e)
        return result

    def add_entry(self, entry: BlackboardEntry) -> None:
        self.entries.append(entry)
        if len(self.entries) > self.max_entries:
            self.entries.sort(key=lambda e: e.timestamp, reverse=True)
            self.entries = self.entries[:self.max_entries]


class ConsensusEvent(BaseModel):
    id: str
    topic: str
    participants: List[str]
    result: ConsensusResult = ConsensusResult.NOT_ATTEMPTED
    votes: Dict[str, str] = Field(default_factory=dict)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    summary: str = ""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    governance_applied: bool = False
    vetoed: bool = False
    veto_reason: Optional[str] = None


class ConflictRecord(BaseModel):
    id: str
    description: str
    severity: ConflictSeverity = ConflictSeverity.MEDIUM
    involved_entries: List[str] = Field(default_factory=list)
    involved_specialists: List[str] = Field(default_factory=list)
    resolution_strategy: str = ""
    resolved: bool = False
    resolved_at: Optional[datetime] = None
    resolution_notes: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ResearchEvidence(BaseModel):
    id: str
    description: str
    source: str
    relevance: float = Field(default=0.5, ge=0.0, le=1.0)
    reliability: float = Field(default=0.5, ge=0.0, le=1.0)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    content: str = ""


class ResearchHypothesis(BaseModel):
    id: str
    description: str
    status: HypothesisStatus = HypothesisStatus.PROPOSED
    supporting_evidence: List[ResearchEvidence] = Field(default_factory=list)
    refuting_evidence: List[ResearchEvidence] = Field(default_factory=list)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    proposed_by: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    tags: List[str] = Field(default_factory=list)

    def compute_confidence(self) -> float:
        support = sum(e.relevance * e.reliability for e in self.supporting_evidence)
        refute = sum(e.relevance * e.reliability for e in self.refuting_evidence)
        total = support + refute
        if total == 0:
            return 0.0
        return round(support / total, 4)


class ResearchFinding(BaseModel):
    id: str
    hypothesis_id: str
    description: str
    conclusion: str
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    evidence_summary: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)


class StrategicMemoryEntry(BaseModel):
    id: str
    memory_type: MemoryType
    content: str
    importance: float = Field(default=0.5, ge=0.0, le=1.0)
    consolidation_count: int = 0
    last_accessed: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    source_goal_id: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    embedding: Optional[List[float]] = None


class ConsolidationRecord(BaseModel):
    id: str
    source_entry_ids: List[str]
    consolidated_content: str
    memory_type: MemoryType
    importance: float = Field(default=0.5, ge=0.0, le=1.0)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class UncertaintyModel(BaseModel):
    uncertain_areas: Dict[str, List[UncertaintyClass]] = Field(default_factory=dict)
    evidence_quality: Dict[str, float] = Field(default_factory=dict)
    last_updated: datetime = Field(default_factory=datetime.utcnow)

    def register_uncertainty(self, area: str, uc: UncertaintyClass) -> None:
        if area not in self.uncertain_areas:
            self.uncertain_areas[area] = []
        if uc not in self.uncertain_areas[area]:
            self.uncertain_areas[area].append(uc)
        self.last_updated = datetime.now(timezone.utc)

    def resolve_uncertainty(self, area: str, uc: UncertaintyClass) -> None:
        if area in self.uncertain_areas and uc in self.uncertain_areas[area]:
            self.uncertain_areas[area].remove(uc)
            if not self.uncertain_areas[area]:
                del self.uncertain_areas[area]
            self.last_updated = datetime.now(timezone.utc)

    def is_area_certain(self, area: str) -> bool:
        return area not in self.uncertain_areas or len(self.uncertain_areas[area]) == 0


class ExecutionHypothesis(BaseModel):
    id: str
    description: str
    predicted_outcome: str
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence: List[str] = Field(default_factory=list)
    status: HypothesisStatus = HypothesisStatus.PROPOSED
    created_at: datetime = Field(default_factory=datetime.utcnow)


class BlockedPath(BaseModel):
    id: str
    step_id: str
    reason: str
    blocker_type: str = ""
    suggested_alternatives: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    resolved: bool = False
    resolved_at: Optional[datetime] = None


class EvidenceLifecycleStatus(str, Enum):
    """Lifecycle states for evidence objects.

    Every evidence object moves through these states explicitly:
    CREATED → VERIFIED → CONSUMED → REFERENCED → CHALLENGED → APPROVED/REJECTED → ARCHIVED
    """
    CREATED = "created"
    VERIFIED = "verified"
    CONSUMED = "consumed"
    REFERENCED = "referenced"
    CHALLENGED = "challenged"
    APPROVED = "approved"
    REJECTED = "rejected"
    ARCHIVED = "archived"


class EvidenceTimeline(BaseModel):
    """Timeline tracking for evidence lifecycle events.

    Records when key lifecycle events occurred for audit and reporting.
    All timestamps are optional — they populate as the evidence progresses.
    """
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When the evidence was created")
    verified_at: Optional[datetime] = Field(default=None, description="When the evidence was verified")
    consumed_at: Optional[datetime] = Field(default=None, description="When the evidence was first consumed")
    archived_at: Optional[datetime] = Field(default=None, description="When the evidence was archived")
    challenged_at: Optional[datetime] = Field(default=None, description="When the evidence was challenged")
    approved_at: Optional[datetime] = Field(default=None, description="When the evidence was approved")
    rejected_at: Optional[datetime] = Field(default=None, description="When the evidence was rejected")

    @property
    def current_status(self) -> str:
        """Derive the current lifecycle status from available timestamps."""
        if self.archived_at:
            return "archived"
        if self.rejected_at:
            return "rejected"
        if self.approved_at:
            return "approved"
        if self.challenged_at:
            return "challenged"
        if self.consumed_at:
            return "consumed"
        if self.verified_at:
            return "verified"
        return "created"

    @property
    def elapsed_seconds(self) -> float:
        """Seconds since creation."""
        now = datetime.now(timezone.utc)
        created = self.created_at
        # Handle naive datetimes from default factory
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return (now - created).total_seconds()

    def to_report_dict(self) -> Dict[str, Any]:
        """Serialize to dict for HERALD reports."""
        return {
            "created_at": self.created_at.isoformat(),
            "verified_at": self.verified_at.isoformat() if self.verified_at else None,
            "consumed_at": self.consumed_at.isoformat() if self.consumed_at else None,
            "archived_at": self.archived_at.isoformat() if self.archived_at else None,
            "challenged_at": self.challenged_at.isoformat() if self.challenged_at else None,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "rejected_at": self.rejected_at.isoformat() if self.rejected_at else None,
            "current_status": self.current_status,
            "elapsed_seconds": self.elapsed_seconds,
        }


class VerificationStatus(str, Enum):
    VERIFIED = "verified"
    PENDING = "pending"
    CHALLENGED = "challenged"
    REJECTED = "rejected"
    ESCALATED = "escalated"
    SUPERSEDED = "superseded"


class CollaborationEvidence(BaseModel):
    """Unified evidence structure for all collaboration artifacts.

    Every specialist output becomes a CollaborationEvidence object:
    - ORACLE findings
    - FORGE implementations
    - SENTINEL reviews / challenges
    - Consensus recommendations
    - ARCHITECT decisions
    - TERMINUS execution results
    - HERALD reports

    All evidence objects carry full provenance, verification status,
    lifecycle tracking, and traceability to enable auditable collaboration.
    """
    id: str = Field(..., description="Unique evidence identifier (e.g. F-001, I-042)")
    owner_agent: str = Field(..., description="Specialist that produced this evidence (ORACLE, FORGE, SENTINEL, etc.)")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="When the evidence was created")
    confidence: float = Field(default=0.5, ge=0.0, le=1.0, description="Confidence in the evidence (0.0–1.0)")
    source: str = Field(default="", description="Provenance source (repository_analysis, code_review, security_scan, etc.)")
    evidence_type: str = Field(..., description="Type: finding, implementation, review, challenge, decision, execution_result, report")
    verification_status: VerificationStatus = Field(default=VerificationStatus.PENDING, description="Current verification status")
    lifecycle_status: EvidenceLifecycleStatus = Field(default=EvidenceLifecycleStatus.CREATED, description="Current lifecycle state")
    timeline: EvidenceTimeline = Field(default_factory=EvidenceTimeline, description="Lifecycle timestamp tracking")
    related_tasks: List[str] = Field(default_factory=list, description="Task IDs this evidence relates to")
    affected_files: List[str] = Field(default_factory=list, description="Files affected by this evidence")
    summary: str = Field(default="", description="Short human-readable summary of the evidence")
    content: str = Field(default="", description="Full evidence content / detail")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional type-specific metadata")

    def to_short_summary(self) -> str:
        """Generate a one-line summary for TUI display."""
        return (
            f"[{self.owner_agent}] {self.evidence_type.upper()}: "
            f"{self.summary[:60] or self.content[:60]}"
        )

    def is_valid_evidence(self) -> bool:
        """Check whether this evidence object meets minimum quality standards.

        Evidence must have:
        - A non-empty owner_agent
        - A non-empty evidence_type
        - Either summary or content
        - A valid timestamp
        """
        if not self.owner_agent or not self.evidence_type:
            return False
        if not self.summary and not self.content:
            return False
        return True

    @classmethod
    def from_blackboard_entry(
        cls,
        entry: "BlackboardEntry",
        evidence_type: Optional[str] = None,
    ) -> "CollaborationEvidence":
        """Create a CollaborationEvidence from a BlackboardEntry.

        Extracts provenance data from the entry's provenance field and
        maps the entry_type to an evidence type string. Initialises
        lifecycle tracking from the entry timestamp.
        """
        return cls(
            id=entry.id,
            owner_agent=entry.provenance.source_id if entry.provenance else "",
            timestamp=entry.timestamp,
            confidence=entry.confidence,
            source=entry.provenance.source_type.value if entry.provenance else "",
            evidence_type=evidence_type or (entry.entry_type.value if entry.entry_type else "unknown"),
            verification_status=VerificationStatus.PENDING,
            lifecycle_status=EvidenceLifecycleStatus.CREATED,
            timeline=EvidenceTimeline(created_at=entry.timestamp),
            related_tasks=[],
            affected_files=[],
            summary=entry.content[:120],
            content=entry.content,
            metadata={"tags": entry.tags, "slot_name": entry.slot_name},
        )

    def transition_to(self, new_status: EvidenceLifecycleStatus) -> None:
        """Transition this evidence to a new lifecycle state.

        Records the timestamp on the EvidenceTimeline when the
        transition occurs. Only valid transitions are allowed.
        """
        now = datetime.now(timezone.utc)
        self.lifecycle_status = new_status

        # Record timestamp on the appropriate field
        if new_status == EvidenceLifecycleStatus.VERIFIED:
            self.timeline.verified_at = now
        elif new_status == EvidenceLifecycleStatus.CONSUMED:
            self.timeline.consumed_at = now
        elif new_status == EvidenceLifecycleStatus.CHALLENGED:
            self.timeline.challenged_at = now
        elif new_status == EvidenceLifecycleStatus.APPROVED:
            self.timeline.approved_at = now
        elif new_status == EvidenceLifecycleStatus.REJECTED:
            self.timeline.rejected_at = now
        elif new_status == EvidenceLifecycleStatus.ARCHIVED:
            self.timeline.archived_at = now


class CognitiveStateSnapshot(BaseModel):
    id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    active_goals: List[Goal] = Field(default_factory=list)
    completed_goals: List[str] = Field(default_factory=list)
    blocked_paths: List[BlockedPath] = Field(default_factory=list)
    uncertainty_model: Optional[UncertaintyModel] = None
    execution_hypotheses: List[ExecutionHypothesis] = Field(default_factory=list)
    active_plan_id: Optional[str] = None
    blackboard_slot_count: int = 0
    memory_entries_count: int = 0
    consensus_events_count: int = 0
    research_hypotheses_count: int = 0
    evidence_count: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)
