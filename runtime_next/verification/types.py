"""Layer 1 — Verification Type System.

Every verification produces a rich result containing confidence, provenance,
severity, retryability, artifacts, affected graph scope, and runtime impact.
Verification results are immutable once emitted.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


# ============================================================================
# Verification Types
# ============================================================================


class VerificationType(str, Enum):
    """Supported verification types. The system supports extensible plugins."""

    LINT = "lint"
    TYPECHECK = "typecheck"
    UNIT_TEST = "unit_test"
    INTEGRATION_TEST = "integration_test"
    SECURITY_SCAN = "security_scan"
    RUNTIME_VALIDATION = "runtime_validation"
    SANDBOX_VALIDATION = "sandbox_validation"
    DEPENDENCY_VALIDATION = "dependency_validation"
    GRAPH_CONSISTENCY = "graph_consistency"
    SERIALIZATION_INTEGRITY = "serialization_integrity"
    CAPABILITY_VALIDATION = "capability_validation"
    ARCHITECTURE_VALIDATION = "architecture_validation"
    MUTEX_VALIDATION = "mutex_validation"
    REPLAY_CONSISTENCY = "replay_consistency"


# ============================================================================
# Failure Classifications
# ============================================================================


class FailureClassification(str, Enum):
    """Every failure is classified. Unknown failures are NEVER silently retried."""

    SYNTAX_ERROR = "syntax_error"
    DEPENDENCY_MISSING = "dependency_missing"
    PERMISSION_DENIED = "permission_denied"
    ENVIRONMENT_FAILURE = "environment_failure"
    TIMEOUT = "timeout"
    VERIFICATION_FAILURE = "verification_failure"
    GRAPH_INCONSISTENCY = "graph_inconsistency"
    SERIALIZATION_FAILURE = "serialization_failure"
    TOOL_FAILURE = "tool_failure"
    STALE_RUNTIME_STATE = "stale_runtime_state"
    MUTEX_VIOLATION = "mutex_violation"
    REPLAY_DIVERGENCE = "replay_divergence"
    CAPABILITY_MISMATCH = "capability_mismatch"
    ARCHITECTURE_VIOLATION = "architecture_violation"
    SANDBOX_ESCAPE = "sandbox_escape"
    UNKNOWN_FAILURE = "unknown_failure"


# ============================================================================
# Severity & Confidence
# ============================================================================


class Severity(str, Enum):
    """How severe is a verification failure or runtime condition."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Confidence(str, Enum):
    """Confidence level for classifications and decisions."""

    CERTAIN = "certain"        # 95%+
    HIGH = "high"              # 80-94%
    MEDIUM = "medium"          # 60-79%
    LOW = "low"                # 40-59%
    GUESS = "guess"            # <40%


class Retryability(str, Enum):
    """Is a failure retryable, and under what conditions."""

    SAFE = "safe"                      # Can retry immediately
    CONDITIONAL = "conditional"        # Can retry after conditions met
    DANGEROUS = "dangerous"            # Retry may cause side effects
    NEVER = "never"                    # Must never retry


# ============================================================================
# Verification Manifest
# ============================================================================


class VerificationManifest(BaseModel):
    """Declares verification requirements for an execution node."""

    required: List[VerificationType] = Field(
        default_factory=list,
        description="Verifications that MUST pass for node to complete",
    )
    optional: List[VerificationType] = Field(
        default_factory=list,
        description="Verifications that SHOULD pass but are not blocking",
    )
    blocking: List[VerificationType] = Field(
        default_factory=list,
        description="Verifications that block downstream nodes on failure",
    )
    scope_override: Optional[VerificationScope] = Field(
        default=None,
        description="Override automatic scope determination",
    )


# ============================================================================
# Verification Scope
# ============================================================================


class VerificationScope(BaseModel):
    """Scoped verification targets derived from repository intelligence."""

    affected_files: List[str] = Field(
        default_factory=list,
        description="Files affected by the change",
    )
    affected_symbols: List[str] = Field(
        default_factory=list,
        description="Symbols affected by the change",
    )
    affected_tests: List[str] = Field(
        default_factory=list,
        description="Test files that should be run",
    )
    affected_architectural_boundaries: List[str] = Field(
        default_factory=list,
        description="Architectural boundaries crossed",
    )
    dependency_chain: List[str] = Field(
        default_factory=list,
        description="Dependency chain for impact analysis",
    )
    is_minimal: bool = Field(
        default=True,
        description="Whether scope was minimized vs full project scan",
    )
    provenance: str = Field(
        default="auto",
        description="How this scope was determined (auto, manual, override)",
    )

    @classmethod
    def full_project(cls) -> VerificationScope:
        """Create a full-project scope (fallback when repo intelligence unavailable)."""
        return cls(
            affected_files=[],
            affected_symbols=[],
            affected_tests=[],
            is_minimal=False,
            provenance="full_project_fallback",
        )

    @classmethod
    def empty(cls) -> VerificationScope:
        """Create an empty scope (no verification needed)."""
        return cls(provenance="empty")

    def is_empty(self) -> bool:
        return (
            not self.affected_files
            and not self.affected_symbols
            and not self.affected_tests
        )


# ============================================================================
# Verification Result
# ============================================================================


class VerificationResult(BaseModel):
    """Immutable result of a single verification.

    Once emitted, this object MUST NOT be mutated. The runtime treats
    verification results as source truth.
    """

    verification_id: str = Field(..., description="Unique ID for this verification")
    node_id: str = Field(..., description="The execution node that was verified")
    verification_type: VerificationType = Field(
        ..., description="Type of verification performed"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="When verification was performed",
    )
    duration_ms: float = Field(
        default=0.0,
        description="How long verification took in milliseconds",
    )
    success: bool = Field(..., description="Did verification pass")
    confidence: Confidence = Field(
        default=Confidence.HIGH,
        description="Confidence in this result",
    )
    severity: Severity = Field(
        default=Severity.ERROR,
        description="Severity if verification failed",
    )
    retryability: Retryability = Field(
        default=Retryability.SAFE,
        description="Is this failure retryable",
    )
    artifacts: Dict[str, Any] = Field(
        default_factory=dict,
        description="Output artifacts from verification (logs, diffs, reports)",
    )
    diagnostics: List[str] = Field(
        default_factory=list,
        description="Human-readable diagnostic messages",
    )
    affected_files: List[str] = Field(
        default_factory=list,
        description="Files that caused or were identified by failure",
    )
    affected_symbols: List[str] = Field(
        default_factory=list,
        description="Symbols that caused or were identified by failure",
    )
    runtime_implications: List[str] = Field(
        default_factory=list,
        description="How this affects runtime state/behavior",
    )
    graph_implications: List[str] = Field(
        default_factory=list,
        description="How this affects graph state",
    )
    provenance: str = Field(
        default="verification_pipeline",
        description="How this result was produced",
    )
    stale_state_indicators: List[str] = Field(
        default_factory=list,
        description="Signals that runtime state may be stale",
    )

    model_config = {"frozen": True}  # Immutable once emitted


# ============================================================================
# Classification Result
# ============================================================================


class ClassificationResult(BaseModel):
    """Probabilistic classification of a failure.

    Classification is never binary. Every classification includes
    confidence, evidence, and alternative possibilities.
    """

    primary: FailureClassification = Field(
        ..., description="Most likely classification"
    )
    confidence: Confidence = Field(
        ..., description="Confidence in primary classification"
    )
    confidence_score: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Numeric confidence score (0.0 - 1.0)",
    )
    evidence: Dict[str, Any] = Field(
        default_factory=dict,
        description="Evidence that supports this classification",
    )
    alternatives: List[FailureClassification] = Field(
        default_factory=list,
        description="Other possible classifications",
    )
    alternative_scores: Dict[FailureClassification, float] = Field(
        default_factory=dict,
        description="Scores for alternative classifications",
    )
    raw_stderr: str = Field(default="", description="Raw stderr output")
    raw_stdout: str = Field(default="", description="Raw stdout output")
    exit_code: Optional[int] = Field(default=None, description="Process exit code")
    graph_state_snapshot: Dict[str, Any] = Field(
        default_factory=dict,
        description="Graph state at time of failure",
    )
    capability_snapshot: Dict[str, Any] = Field(
        default_factory=dict,
        description="Capability state at time of failure",
    )

    def is_unknown(self) -> bool:
        """Unknown failures are NEVER silently retried."""
        return self.primary == FailureClassification.UNKNOWN_FAILURE


# ============================================================================
# Recovery Strategy
# ============================================================================


class RecoveryStrategy(BaseModel):
    """A typed, observable, replayable recovery strategy."""

    id: str = Field(..., description="Unique strategy identifier")
    name: str = Field(..., description="Human-readable name")
    failure_type: FailureClassification = Field(
        ..., description="Which failure this strategy addresses"
    )
    description: str = Field(default="", description="What this strategy does")
    danger_level: str = Field(
        default="safe",
        description="safe | approval_required | abort",
    )
    max_retries: int = Field(default=2, ge=0, description="Maximum retry attempts")
    requires_user_approval: bool = Field(
        default=False,
        description="Does this need user confirmation",
    )
    handler: Optional[str] = Field(
        default=None,
        description="Handler function reference (set at runtime)",
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def requires_approval(self) -> bool:
        return self.requires_user_approval or self.danger_level == "approval_required"


# ============================================================================
# Recovery Action
# ============================================================================


class RecoveryAction(BaseModel):
    """An executed recovery action, recorded in graph history."""

    id: str = Field(..., description="Unique action identifier")
    strategy_id: str = Field(..., description="Which strategy was used")
    node_id: str = Field(..., description="Target execution node")
    failure_classification: FailureClassification = Field(
        ..., description="What was classified"
    )
    action_type: str = Field(
        ..., description="retry | inject_node | escalate | rollback | skip"
    )
    description: str = Field(default="", description="What was done")
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters used for recovery",
    )
    injected_node_id: Optional[str] = Field(
        default=None,
        description="If a recovery node was injected, its ID",
    )
    success: bool = Field(default=False, description="Did recovery succeed")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    duration_ms: float = Field(default=0.0)
    result: Dict[str, Any] = Field(default_factory=dict)


# ============================================================================
# Retry Decision
# ============================================================================


class RetryDecision(BaseModel):
    """Result of retry safety evaluation."""

    can_retry: bool = Field(..., description="Is retry allowed")
    reason: str = Field(default="", description="Why retry is or isn't allowed")
    suggested_backoff: float = Field(
        default=0.0,
        description="Suggested backoff in seconds",
    )
    graph_consistent: bool = Field(default=True)
    capability_valid: bool = Field(default=True)
    mutation_safe: bool = Field(default=True)
    dependency_fresh: bool = Field(default=True)
    replay_divergence_risk: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Risk of replay divergence (0-1)",
    )
    failure_stability: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="How stable/consistent this failure is across retries",
    )
    retry_count: int = Field(default=0)
    blocking_condition: Optional[str] = Field(
        default=None,
        description="If can_retry is False, what is blocking",
    )


# ============================================================================
# Governance Decision
# ============================================================================


class GovernanceDecision(BaseModel):
    """Decision from the governance layer about how to proceed."""

    verdict: str = Field(
        ..., description="auto_recover | require_approval | abort | notify_user"
    )
    reason: str = Field(default="", description="Why this decision was made")
    confidence: Confidence = Field(
        default=Confidence.HIGH,
        description="Confidence in this decision",
    )
    danger_assessment: str = Field(
        default="safe",
        description="safe | reversible | destructive",
    )
    requires_user_intervention: bool = Field(default=False)
    suggested_message: Optional[str] = Field(
        default=None,
        description="Message to show the user if interrupted",
    )

    def should_stop_autonomy(self) -> bool:
        """Should AELVO stop autonomous execution."""
        return self.verdict in ("abort", "require_approval")


# ============================================================================
# Consistency Result
# ============================================================================


class ConsistencyResult(BaseModel):
    """Result of a runtime consistency validation."""

    is_consistent: bool = Field(..., description="Is the runtime consistent")
    checks_performed: List[str] = Field(
        default_factory=list,
        description="Which consistency checks were run",
    )
    violations: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Consistency violations found",
    )
    graph_integrity: bool = Field(default=True)
    serialization_integrity: bool = Field(default=True)
    replay_consistency: bool = Field(default=True)
    mutex_correctness: bool = Field(default=True)
    capability_freshness: bool = Field(default=True)
    event_ordering: bool = Field(default=True)
    dependency_validity: bool = Field(default=True)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    duration_ms: float = Field(default=0.0)


# ============================================================================
# Exception Hierarchy
# ============================================================================


class VerificationError(Exception):
    """Base exception for verification system errors."""


class VerificationNotImplementedError(VerificationError):
    """Raised when a verification type has no registered handler.

    This replaces silent low-confidence passes for unhandled verification
    types. Any caller requesting a verification type without a registered
    handler must explicitly handle this error rather than receiving a
    silently-failed result that may go unchecked.
    """

    def __init__(self, vtype: VerificationType, node_id: str):
        self.vtype = vtype
        self.node_id = node_id
        super().__init__(
            f"Verification type '{vtype.value}' has no registered handler "
            f"for node '{node_id}'. Register a verifier via "
            f"VerificationPipeline.register_verifier() before requesting "
            f"this verification type."
        )


# ============================================================================
# Helper Utilities
# ============================================================================


# Shared exit code mapping to prevent duplication drift between classify_exit_code() and classifier weights
EXIT_CODE_CLASSIFICATION_MAP: Dict[int, Dict[str, Any]] = {
    127: {"classification": FailureClassification.DEPENDENCY_MISSING, "weight": 0.8, "label": "command_not_found"},
    126: {"classification": FailureClassification.PERMISSION_DENIED, "weight": 0.7, "label": "permission_denied"},
    137: {"classification": FailureClassification.TIMEOUT, "weight": 0.6, "label": "killed_oom"},
    139: {"classification": FailureClassification.ENVIRONMENT_FAILURE, "weight": 0.6, "label": "segfault"},
}


def classify_exit_code(code: Optional[int]) -> Optional[FailureClassification]:
    """Map an exit code to a probable failure classification. Uses shared mapping."""
    if code is None:
        return None
    if code == 0:
        return None
    if code == 1:
        return None  # Generic error — needs deeper analysis
    entry = EXIT_CODE_CLASSIFICATION_MAP.get(code)
    if entry:
        return entry["classification"]
    return FailureClassification.UNKNOWN_FAILURE


# Mapping of failure -> default recovery strategy
DEFAULT_RECOVERY_MAP: Dict[FailureClassification, str] = {
    FailureClassification.SYNTAX_ERROR: "reinvoke_with_diagnostics",
    FailureClassification.DEPENDENCY_MISSING: "safe_install",
    FailureClassification.PERMISSION_DENIED: "block_and_notify",
    FailureClassification.ENVIRONMENT_FAILURE: "refresh_capabilities",
    FailureClassification.TIMEOUT: "retry_with_adjusted_limits",
    FailureClassification.VERIFICATION_FAILURE: "reverify_with_context",
    FailureClassification.GRAPH_INCONSISTENCY: "rebuild_graph_segment",
    FailureClassification.SERIALIZATION_FAILURE: "rollback_graph_checkpoint",
    FailureClassification.TOOL_FAILURE: "retry_with_clean_state",
    FailureClassification.SANDBOX_ESCAPE: "abort_and_notify",
    FailureClassification.STALE_RUNTIME_STATE: "refresh_runtime_state",
    FailureClassification.MUTEX_VIOLATION: "reschedule_execution",
    FailureClassification.REPLAY_DIVERGENCE: "abort_and_notify",
    FailureClassification.CAPABILITY_MISMATCH: "refresh_capabilities",
    FailureClassification.ARCHITECTURE_VIOLATION: "block_and_notify",
    FailureClassification.UNKNOWN_FAILURE: "abort_and_notify",
}
