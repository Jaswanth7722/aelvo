"""
agent_metrics.py — Per-Agent Operational Metrics

Tracks operational metrics for every specialist in the system:

Oracle:      finding_count, consumption_rate, challenge_rate, approval_rate
Forge:       implementation_count, approval_rate, revision_rate, success_rate
Sentinel:    review_count, challenge_count, detection_rate, approval_rate
Architect:   decision_count, override_count, replan_count, approval_rate
Consensus:   agreement_rate, revision_rate, escalation_rate
Recovery:    recovery_attempts, recovery_success_rate, fallback_success_rate

Metrics persist in-memory for the session and can be serialised
to JSON for reporting.
"""

from __future__ import annotations

import time
import json
import threading
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from pydantic import BaseModel, Field


# ============================================================================
# Per-Agent Metric Models
# ============================================================================


class OracleMetrics(BaseModel):
    """Operational metrics for the ORACLE specialist."""
    finding_count: int = 0
    consumption_rate: float = 0.0
    challenge_rate: float = 0.0
    approval_rate: float = 0.0

    @property
    def summary(self) -> Dict[str, Any]:
        return {
            "finding_count": self.finding_count,
            "consumption_rate": round(self.consumption_rate, 4),
            "challenge_rate": round(self.challenge_rate, 4),
            "approval_rate": round(self.approval_rate, 4),
        }


class ForgeMetrics(BaseModel):
    """Operational metrics for the FORGE specialist."""
    implementation_count: int = 0
    approval_rate: float = 0.0
    revision_rate: float = 0.0
    success_rate: float = 0.0

    @property
    def summary(self) -> Dict[str, Any]:
        return {
            "implementation_count": self.implementation_count,
            "approval_rate": round(self.approval_rate, 4),
            "revision_rate": round(self.revision_rate, 4),
            "success_rate": round(self.success_rate, 4),
        }


class SentinelMetrics(BaseModel):
    """Operational metrics for the SENTINEL specialist."""
    review_count: int = 0
    challenge_count: int = 0
    detection_rate: float = 0.0
    approval_rate: float = 0.0

    @property
    def summary(self) -> Dict[str, Any]:
        return {
            "review_count": self.review_count,
            "challenge_count": self.challenge_count,
            "detection_rate": round(self.detection_rate, 4),
            "approval_rate": round(self.approval_rate, 4),
        }


class ArchitectMetrics(BaseModel):
    """Operational metrics for the ARCHITECT specialist."""
    decision_count: int = 0
    override_count: int = 0
    replan_count: int = 0
    approval_rate: float = 0.0

    @property
    def summary(self) -> Dict[str, Any]:
        return {
            "decision_count": self.decision_count,
            "override_count": self.override_count,
            "replan_count": self.replan_count,
            "approval_rate": round(self.approval_rate, 4),
        }


class ConsensusMetrics(BaseModel):
    """Operational metrics for the Consensus system."""
    agreement_rate: float = 0.0
    revision_rate: float = 0.0
    escalation_rate: float = 0.0

    @property
    def summary(self) -> Dict[str, Any]:
        return {
            "agreement_rate": round(self.agreement_rate, 4),
            "revision_rate": round(self.revision_rate, 4),
            "escalation_rate": round(self.escalation_rate, 4),
        }


class RecoveryMetrics(BaseModel):
    """Operational metrics for the Recovery system."""
    recovery_attempts: int = 0
    recovery_success_rate: float = 0.0
    fallback_success_rate: float = 0.0

    @property
    def summary(self) -> Dict[str, Any]:
        return {
            "recovery_attempts": self.recovery_attempts,
            "recovery_success_rate": round(self.recovery_success_rate, 4),
            "fallback_success_rate": round(self.fallback_success_rate, 4),
        }


# ============================================================================
# Main Agent Metrics Tracker
# ============================================================================


class AgentMetricsTracker:
    """Tracks operational metrics for all specialists.

    Thread-safe. Metrics persist in-memory and can be exported
    as JSON for HERALD reports and TUI display.

    Usage:
        tracker = AgentMetricsTracker()
        tracker.record_oracle_finding(consumed=True)
        tracker.record_forge_implementation(success=True, approved=True)
        report = tracker.generate_report()
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._oracle = OracleMetrics()
        self._forge = ForgeMetrics()
        self._sentinel = SentinelMetrics()
        self._architect = ArchitectMetrics()
        self._consensus = ConsensusMetrics()
        self._recovery = RecoveryMetrics()

        # Raw counters for rate computation
        self._oracle_consumed: int = 0
        self._oracle_challenged: int = 0
        self._oracle_approved: int = 0
        self._forge_approved: int = 0
        self._forge_revision: int = 0
        self._sentinel_approved: int = 0
        self._architect_approvals: int = 0
        self._architect_overrides: int = 0
        self._architect_replans: int = 0
        self._consensus_agreements: int = 0
        self._consensus_revisions: int = 0
        self._consensus_escalations: int = 0
        self._recovery_successes: int = 0
        self._recovery_fallbacks: int = 0
        self._total_events: int = 0

    # ── ORACLE ──────────────────────────────────────────────────

    def record_oracle_finding(
        self, consumed: bool = False, challenged: bool = False,
    ) -> None:
        with self._lock:
            self._oracle.finding_count += 1
            self._total_events += 1
            if consumed:
                self._oracle_consumed += 1
            if challenged:
                self._oracle_challenged += 1
            self._recompute_oracle_rates()

    def record_oracle_approved(self) -> None:
        with self._lock:
            self._oracle_approved += 1
            self._recompute_oracle_rates()

    def _recompute_oracle_rates(self) -> None:
        fc = self._oracle.finding_count
        self._oracle.consumption_rate = (
            self._oracle_consumed / fc if fc > 0 else 0.0
        )
        self._oracle.challenge_rate = (
            self._oracle_challenged / fc if fc > 0 else 0.0
        )
        self._oracle.approval_rate = (
            self._oracle_approved / fc if fc > 0 else 0.0
        )

    # ── FORGE ───────────────────────────────────────────────────

    def record_forge_implementation(
        self, success: bool = True, approved: bool = True,
        revision: bool = False,
    ) -> None:
        with self._lock:
            self._forge.implementation_count += 1
            self._total_events += 1
            if approved:
                self._forge_approved += 1
            if revision:
                self._forge_revision += 1
            if success:
                self._forge.success_rate = (
                    (self._forge.success_rate * (self._forge.implementation_count - 1) + 1)
                    / self._forge.implementation_count
                )
            self._recompute_forge_rates()

    def _recompute_forge_rates(self) -> None:
        ic = self._forge.implementation_count
        self._forge.approval_rate = (
            self._forge_approved / ic if ic > 0 else 0.0
        )
        self._forge.revision_rate = (
            self._forge_revision / ic if ic > 0 else 0.0
        )

    # ── SENTINEL ────────────────────────────────────────────────

    def record_sentinel_review(
        self, challenged: bool = False, approved: bool = True,
    ) -> None:
        with self._lock:
            self._sentinel.review_count += 1
            self._total_events += 1
            if challenged:
                self._sentinel.challenge_count += 1
            if approved:
                self._sentinel_approved += 1
            self._recompute_sentinel_rates()

    def _recompute_sentinel_rates(self) -> None:
        rc = self._sentinel.review_count
        self._sentinel.detection_rate = (
            self._sentinel.challenge_count / rc if rc > 0 else 0.0
        )
        self._sentinel.approval_rate = (
            self._sentinel_approved / rc if rc > 0 else 0.0
        )

    # ── ARCHITECT ───────────────────────────────────────────────

    def record_architect_decision(
        self, outcome: str = "approve",
    ) -> None:
        with self._lock:
            self._architect.decision_count += 1
            self._total_events += 1
            if outcome == "approve":
                self._architect_approvals += 1
            elif outcome == "override":
                self._architect.override_count += 1
                self._architect_overrides += 1
            elif outcome == "replan":
                self._architect.replan_count += 1
                self._architect_replans += 1
            self._recompute_architect_rates()

    def _recompute_architect_rates(self) -> None:
        dc = self._architect.decision_count
        self._architect.approval_rate = (
            self._architect_approvals / dc if dc > 0 else 0.0
        )

    # ── CONSENSUS ───────────────────────────────────────────────

    def record_consensus_outcome(
        self, outcome: str = "agreed",
    ) -> None:
        with self._lock:
            self._total_events += 1
            if outcome == "agreed":
                self._consensus_agreements += 1
            elif outcome == "requires_revision":
                self._consensus_revisions += 1
            elif outcome == "escalated":
                self._consensus_escalations += 1
            self._recompute_consensus_rates()

    def _recompute_consensus_rates(self) -> None:
        total = (
            self._consensus_agreements
            + self._consensus_revisions
            + self._consensus_escalations
        )
        self._consensus.agreement_rate = (
            self._consensus_agreements / total if total > 0 else 0.0
        )
        self._consensus.revision_rate = (
            self._consensus_revisions / total if total > 0 else 0.0
        )
        self._consensus.escalation_rate = (
            self._consensus_escalations / total if total > 0 else 0.0
        )

    # ── RECOVERY ────────────────────────────────────────────────

    def record_recovery_attempt(
        self, success: bool = False, fallback: bool = False,
    ) -> None:
        with self._lock:
            self._recovery.recovery_attempts += 1
            self._total_events += 1
            if success:
                self._recovery_successes += 1
            if fallback:
                self._recovery_fallbacks += 1
            self._recompute_recovery_rates()

    def _recompute_recovery_rates(self) -> None:
        ra = self._recovery.recovery_attempts
        self._recovery.recovery_success_rate = (
            self._recovery_successes / ra if ra > 0 else 0.0
        )
        self._recovery.fallback_success_rate = (
            self._recovery_fallbacks / ra if ra > 0 else 0.0
        )

    # ── Reporting ───────────────────────────────────────────────

    def generate_report(self) -> Dict[str, Any]:
        """Generate a comprehensive agent metrics report."""
        with self._lock:
            return {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_events": self._total_events,
                "oracle": self._oracle.summary,
                "forge": self._forge.summary,
                "sentinel": self._sentinel.summary,
                "architect": self._architect.summary,
                "consensus": self._consensus.summary,
                "recovery": self._recovery.summary,
            }

    def to_json(self, indent: int = 2) -> str:
        """Serialize the report as JSON."""
        return json.dumps(self.generate_report(), indent=indent)

    def reset(self) -> None:
        """Reset all metrics to zero."""
        with self._lock:
            self._oracle = OracleMetrics()
            self._forge = ForgeMetrics()
            self._sentinel = SentinelMetrics()
            self._architect = ArchitectMetrics()
            self._consensus = ConsensusMetrics()
            self._recovery = RecoveryMetrics()
            self._oracle_consumed = 0
            self._oracle_challenged = 0
            self._oracle_approved = 0
            self._forge_approved = 0
            self._forge_revision = 0
            self._sentinel_approved = 0
            self._architect_approvals = 0
            self._architect_overrides = 0
            self._architect_replans = 0
            self._consensus_agreements = 0
            self._consensus_revisions = 0
            self._consensus_escalations = 0
            self._recovery_successes = 0
            self._recovery_fallbacks = 0
            self._total_events = 0
