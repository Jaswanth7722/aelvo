# core/security — Execution Security Platform
#
# This package provides the authoritative security boundary for all
# dangerous or untrusted execution in AELVO.
#
# Subsystems:
#   ExecutionGovernance  — risk classification, trust levels, policy enforcement
#   SecurityMemory       — violation tracking, dangerous pattern storage, learning
#   SecurityAnalytics    — event frequency, blocked/allowed ratios, threat trends
#   ApprovalManager      — high-risk action approval workflows, escalation

from __future__ import annotations

from .execution_governance import (
    ExecutionGovernance,
    RiskLevel,
    TrustLevel,
    PolicyDecision,
    SecurityClassification,
)
from .security_memory import SecurityMemory, SecurityMemoryEntry
from .security_analytics import SecurityAnalytics, SecurityAnalyticsReport
from .approval_manager import ApprovalManager, ApprovalRequest, ApprovalState, EscalationPath

from .execution_sandbox import (
    ExecutionSandbox,
    SandboxCapability,
    SandboxPolicyAction,
    SandboxAuditRecord,
    RollbackApprovalGate,
    RollbackApprovalRequest,
    RollbackRiskLevel,
    SecurityIntegration,
)

__all__ = [
    "ExecutionGovernance",
    "RiskLevel",
    "TrustLevel",
    "PolicyDecision",
    "SecurityClassification",
    "SecurityMemory",
    "SecurityMemoryEntry",
    "SecurityAnalytics",
    "SecurityAnalyticsReport",
    "ApprovalManager",
    "ApprovalRequest",
    "ApprovalState",
    "EscalationPath",
    "ExecutionSandbox",
    "SandboxCapability",
    "SandboxPolicyAction",
    "SandboxAuditRecord",
    "RollbackApprovalGate",
    "RollbackApprovalRequest",
    "RollbackRiskLevel",
    "SecurityIntegration",
]
