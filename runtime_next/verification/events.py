"""Layer 9 — Replayable Verification Events.

Every verification and recovery action emits typed, timestamped, replayable,
serializable, deterministic events.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

from .types import (
    VerificationType,
    FailureClassification,
    VerificationResult,
    ClassificationResult,
    RetryDecision,
)


class VerificationStartedEvent(BaseModel):
    """Emitted when a verification begins."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node_id: str = Field(..., description="Node being verified")
    verification_type: VerificationType = Field(
        ..., description="Type of verification"
    )
    scope: Dict[str, Any] = Field(
        default_factory=dict,
        description="Verification scope snapshot",
    )
    replay_id: str = Field(default="")


class VerificationCompletedEvent(BaseModel):
    """Emitted when a verification completes successfully."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node_id: str = Field(..., description="Node that was verified")
    verification_type: VerificationType = Field(
        ..., description="Type of verification"
    )
    result: VerificationResult = Field(..., description="Full verification result")
    duration_ms: float = Field(default=0.0)
    replay_id: str = Field(default="")


class VerificationFailedEvent(BaseModel):
    """Emitted when a verification fails."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node_id: str = Field(..., description="Node that failed verification")
    verification_type: VerificationType = Field(
        ..., description="Type of verification"
    )
    result: VerificationResult = Field(..., description="Full verification result")
    classification: Optional[ClassificationResult] = Field(
        default=None,
        description="Failure classification if available",
    )
    duration_ms: float = Field(default=0.0)
    replay_id: str = Field(default="")


class FailureClassifiedEvent(BaseModel):
    """Emitted when a failure has been classified."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node_id: str = Field(..., description="Node that failed")
    classification: ClassificationResult = Field(
        ..., description="Full classification result"
    )
    raw_error: str = Field(default="", description="Original error message")
    replay_id: str = Field(default="")


class RecoveryInjectedEvent(BaseModel):
    """Emitted when a recovery node is injected into the execution graph."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node_id: str = Field(..., description="Original failing node")
    injected_node_id: str = Field(
        ..., description="ID of the injected recovery node"
    )
    strategy_id: str = Field(..., description="Which strategy was used")
    failure_classification: FailureClassification = Field(
        ..., description="What was classified"
    )
    strategy_params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Parameters of the recovery strategy",
    )
    replay_id: str = Field(default="")


class RetryBlockedEvent(BaseModel):
    """Emitted when retry is blocked by the Retry Safety Engine."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    node_id: str = Field(..., description="Node whose retry was blocked")
    classification: FailureClassification = Field(
        ..., description="Failure classification"
    )
    decision: RetryDecision = Field(
        ..., description="Full retry safety decision"
    )
    retry_attempt: int = Field(0, description="How many retries were attempted")
    replay_id: str = Field(default="")


class GraphRollbackEvent(BaseModel):
    """Emitted when the graph state is rolled back to a checkpoint."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    plan_id: str = Field(..., description="Plan that was rolled back")
    checkpoint_path: str = Field(
        default="", description="Path to the checkpoint used"
    )
    reason: str = Field(default="", description="Why rollback occurred")
    nodes_affected: List[str] = Field(
        default_factory=list,
        description="Nodes whose state was reset",
    )
    replay_id: str = Field(default="")


class ReplayDivergenceEvent(BaseModel):
    """Emitted when replay consistency check detects divergence."""

    event_id: str = Field(..., description="Unique event identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    expected_hash: str = Field(
        default="", description="Expected state hash from previous run"
    )
    actual_hash: str = Field(
        default="", description="Actual state hash from replay"
    )
    divergent_nodes: List[str] = Field(
        default_factory=list,
        description="Nodes where divergence was detected",
    )
    replay_id: str = Field(default="")
