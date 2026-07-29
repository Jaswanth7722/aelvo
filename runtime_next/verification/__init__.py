"""Verification + Self-Healing Runtime.

A production-grade reliability substrate that transforms execution into
verification-driven autonomous engineering.

Ten layers:
  1. Verification Types       — Complete type system
  2. Verification Pipeline    — Scoped, plugin-based verification
  3. Failure Classifier       — Probabilistic failure analysis
  4. Recovery Strategies      — Typed, state-aware recovery
  5. Retry Safety Engine      — Pre-retry evaluation
  6. Recovery Node Injection  — Recovery as graph nodes
  7. Runtime Consistency      — Runtime self-validation
  8. Learned Recovery Memory  — Improvement from experience
  9. Verification Events      — Replayable typed events
 10. Recovery Governance      — Autonomy boundary enforcement
"""

from .types import (
    VerificationType,
    FailureClassification,
    Severity,
    Confidence,
    Retryability,
    VerificationResult,
    VerificationScope,
    ClassificationResult,
    RecoveryStrategy,
    RecoveryAction,
    RetryDecision,
    GovernanceDecision,
    ConsistencyResult,
    VerificationManifest,
)
from .events import (
    VerificationStartedEvent,
    VerificationCompletedEvent,
    VerificationFailedEvent,
    FailureClassifiedEvent,
    RecoveryInjectedEvent,
    RetryBlockedEvent,
    GraphRollbackEvent,
    ReplayDivergenceEvent,
)
from .pipeline import VerificationPipeline
from .classifier import FailureClassifier
from .recovery import RecoveryStrategyEngine
from .retry_safety import RetrySafetyEngine
from .injector import RecoveryNodeInjector
from .consistency import RuntimeConsistencyValidator
from .memory import LearnedRecoveryMemory
from .governance import RecoveryGovernance
from .driven_recovery import (
    VerificationDrivenRecoveryPipeline,
    RecoveryPipelineResult,
    RecoveryPipelinePhase,
    RecoveryPipelineConfig,
)

__all__ = [
    # Types
    "VerificationType",
    "FailureClassification",
    "Severity",
    "Confidence",
    "Retryability",
    "VerificationResult",
    "VerificationScope",
    "ClassificationResult",
    "RecoveryStrategy",
    "RecoveryAction",
    "RetryDecision",
    "GovernanceDecision",
    "ConsistencyResult",
    "VerificationManifest",
    # Events
    "VerificationStartedEvent",
    "VerificationCompletedEvent",
    "VerificationFailedEvent",
    "FailureClassifiedEvent",
    "RecoveryInjectedEvent",
    "RetryBlockedEvent",
    "GraphRollbackEvent",
    "ReplayDivergenceEvent",
    # Engines
    "VerificationPipeline",
    "FailureClassifier",
    "RecoveryStrategyEngine",
    "RetrySafetyEngine",
    "RecoveryNodeInjector",
    "RuntimeConsistencyValidator",
    "LearnedRecoveryMemory",
    "RecoveryGovernance",
    # Phase 11: Driven Recovery Pipeline
    "VerificationDrivenRecoveryPipeline",
    "RecoveryPipelineResult",
    "RecoveryPipelinePhase",
    "RecoveryPipelineConfig",
]
