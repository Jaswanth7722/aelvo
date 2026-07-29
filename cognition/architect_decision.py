"""
architect_decision.py — Architect Decision Authority Types

Per Amendment 3: Consensus is advisory, Architect is authoritative.

Architect Decisions are the final authority in the system:
- APPROVE  — Accept the consensus recommendation or plan
- REJECT   — Deny execution, return task for revision
- ESCALATE — Escalate to user (rare)
- REPLAN   — Trigger replanning via cognition/replan.py
- OVERRIDE — Override consensus advice (with reason recorded)

Every decision carries a reason, optional conditions, and metadata
for auditing and visibility in the TUI.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ArchitectDecisionOutcome(str, Enum):
    """The five possible outcomes of an Architect review."""

    APPROVE = "approve"
    """Accept the consensus recommendation or plan as-is."""

    REJECT = "reject"
    """Deny execution. Task must be revised and resubmitted."""

    ESCALATE = "escalate"
    """Escalate the decision to the user (rare — used for unresolvable
    uncertainty, policy violations, or out-of-scope requests)."""

    REPLAN = "replan"
    """Trigger a full or partial replan via DynamicReplanningEngine.
    The task/plan is sent back for restructuring."""

    OVERRIDE = "override"
    """Override the consensus recommendation with a different decision.
    The override reason must be recorded in the blackboard."""


class ExecutionMode(str, Enum):
    """The two execution modes the Architect can select.

    Per Amendment 1: The Architect evaluates the task and selects the
    appropriate mode.
    """

    CONSOLIDATED = "consolidated"
    """Mode A — Fast path. Single consolidated LLM call covering all
    specialist perspectives. Low latency, low cost. Used for simple
    tasks, low-risk work, fast responses."""

    COLLABORATIVE = "collaborative"
    """Mode B — Thorough path. Multi-step execution with task board
    tracking, parallel specialist dispatch, full consensus, and
    architect review. Used for complex engineering, multi-stage
    implementation, security-sensitive tasks."""


class ArchitectDecision(BaseModel):
    """A final decision made by the Architect.

    Every decision records:
    - What was decided (outcome)
    - Why (reason)
    - What it applies to (target_type, target_id)
    - Optional conditions that must be met
    - Optional override details (when OVERRIDE)
    - Metadata for audit and visibility

    This model is immutable once created — decisions are never changed.
    """

    decision_id: str = Field(
        default="",
        description="Unique decision identifier",
    )
    outcome: ArchitectDecisionOutcome = Field(
        ..., description="The decision outcome",
    )
    target_type: str = Field(
        default="plan",
        description="What this decision applies to: 'plan', 'task', 'consensus', 'proposal'",
    )
    target_id: str = Field(
        default="",
        description="ID of the target (plan_id, task_id, consensus_event_id, etc.)",
    )
    reason: str = Field(
        default="",
        description="Human-readable justification for the decision",
    )

    # ── Optional enrichments ───────────────────────────────────────
    conditions: List[str] = Field(
        default_factory=list,
        description="Conditions that must be satisfied (e.g., 'tests must pass', 'security review required')",
    )
    assigned_to: str = Field(
        default="",
        description="Specialist assigned to act on this decision (REJECT/REPLAN targets)",
    )
    assigned_reason: str = Field(
        default="",
        description="Why this specialist was assigned",
    )

    # ── Override details (only for OVERRIDE outcome) ───────────────
    overridden_recommendation: str = Field(
        default="",
        description="The original consensus recommendation that was overridden",
    )
    override_rationale: str = Field(
        default="",
        description="Why the Architect chose to override",
    )

    # ── Replan details (only for REPLAN outcome) ───────────────────
    replan_trigger: str = Field(
        default="",
        description="Replan trigger reason passed to DynamicReplanningEngine",
    )
    replan_scope: str = Field(
        default="partial",
        description="'full' or 'partial' replan",
    )

    # ── Source ────────────────────────────────────────────────────
    decided_by: str = Field(
        default="ARCHITECT",
        description="Who made this decision (always ARCHITECT in production)",
    )
    created_at: float = Field(
        default_factory=time.time,
        description="Unix timestamp of the decision",
    )

    # ── Summary ────────────────────────────────────────────────────

    def summary(self) -> Dict[str, Any]:
        """Compact summary for logging / UI display."""
        return {
            "decision_id": self.decision_id[:12],
            "outcome": self.outcome.value,
            "target": f"{self.target_type}:{self.target_id[:12]}",
            "reason": self.reason[:80],
            "conditions": len(self.conditions),
            "assigned_to": self.assigned_to,
            "age_s": round(time.time() - self.created_at, 1),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        icon = {
            ArchitectDecisionOutcome.APPROVE: "✓",
            ArchitectDecisionOutcome.REJECT: "✗",
            ArchitectDecisionOutcome.ESCALATE: "▲",
            ArchitectDecisionOutcome.REPLAN: "⟳",
            ArchitectDecisionOutcome.OVERRIDE: "⚠",
        }.get(self.outcome, "?")

        lines = [
            f"  {icon} [{self.decision_id[:10]}] {self.outcome.value.upper()}",
            f"       Target: {self.target_type} ({self.target_id[:16]})",
            f"       Reason: {self.reason[:120]}",
        ]
        if self.conditions:
            lines.append(f"       Conditions ({len(self.conditions)}):")
            for c in self.conditions:
                lines.append(f"         - {c[:80]}")
        if self.assigned_to:
            lines.append(f"       Assigned to: {self.assigned_to}")
        if self.outcome == ArchitectDecisionOutcome.OVERRIDE and self.override_rationale:
            lines.append(f"       Override rationale: {self.override_rationale[:120]}")
        if self.outcome == ArchitectDecisionOutcome.REPLAN:
            lines.append(
                f"       Replan: {self.replan_scope} | {self.replan_trigger[:80]}"
            )
        return "\n".join(lines)


# ===========================================================================
# Mode Selection Logic
# ===========================================================================


class ModeSelectionCriteria(BaseModel):
    """Evaluated criteria used by the Architect to select an execution mode.

    These fields are populated by evaluating the HermesContext and any
    additional context (task board load, available specialists, etc.).
    """

    complexity: int = Field(default=1, ge=1, le=10, description="Task complexity (1-10)")
    risk_profile: str = Field(default="low", description="Risk: low, medium, high, critical")
    goal_count: int = Field(default=0, ge=0, description="Number of decomposed goals")
    has_security_concerns: bool = Field(default=False)
    has_multi_file_scope: bool = Field(default=False)
    requires_consensus: bool = Field(default=False)
    estimated_duration_s: float = Field(default=0.0, description="Estimated duration in seconds")
    affected_files_count: int = Field(default=0, description="Estimated files affected")

    def select_mode(self) -> ExecutionMode:
        """Select the execution mode based on evaluated criteria.

        Decision matrix (from risk register):
        - risk >= high → Mode B (Collaborative)
        - complexity > 4 → Mode B
        - has_security_concerns → Mode B
        - requires_consensus → Mode B
        - affected_files >= 5 → Mode B
        - goal_count >= 4 → Mode B
        - Otherwise → Mode A (Consolidated)
        """
        if self.risk_profile in ("high", "critical"):
            return ExecutionMode.COLLABORATIVE
        if self.complexity > 4:
            return ExecutionMode.COLLABORATIVE
        if self.has_security_concerns:
            return ExecutionMode.COLLABORATIVE
        if self.requires_consensus:
            return ExecutionMode.COLLABORATIVE
        if self.affected_files_count >= 5:
            return ExecutionMode.COLLABORATIVE
        if self.goal_count >= 4:
            return ExecutionMode.COLLABORATIVE
        return ExecutionMode.CONSOLIDATED

    def rationale(self) -> str:
        """Human-readable rationale for the mode selection."""
        mode = self.select_mode()
        triggers = []
        if self.risk_profile in ("high", "critical"):
            triggers.append(f"risk={self.risk_profile}")
        if self.complexity > 4:
            triggers.append(f"complexity={self.complexity}")
        if self.has_security_concerns:
            triggers.append("security_concerns")
        if self.requires_consensus:
            triggers.append("requires_consensus")
        if self.affected_files_count >= 5:
            triggers.append(f"affected_files={self.affected_files_count}")
        if self.goal_count >= 4:
            triggers.append(f"goals={self.goal_count}")
        if not triggers:
            return f"Mode {mode.value}: task fits consolidated profile (low risk, low complexity)"
        return f"Mode {mode.value}: triggered by {', '.join(triggers)}"

    @classmethod
    def from_hermes_context(
        cls,
        task: str = "",
        risk_profile: str = "low",
        complexity: int = 1,
        goals: Optional[List[str]] = None,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> "ModeSelectionCriteria":
        """Build criteria from a HermesContext (or equivalent fields)."""
        goals_list = goals or []
        goal_count = len(goals_list)
        constraints_dict = constraints or {}

        # Detect security concerns from task text or constraints
        lower = task.lower()
        security_keywords = [
            "security", "vulnerability", "secret", "credential",
            "password", "token", "auth", "encryption", "permission",
            "firewall", "audit",
        ]
        has_security = any(k in lower for k in security_keywords)

        # Detect multi-file scope
        scope_keywords = [
            "all files", "everywhere", "entire", "global", "multiple files",
            "many files", "whole", "full",
        ]
        multi_file = any(k in lower for k in scope_keywords)

        # Check if consensus is required by task/constraints
        requires_consensus = (
            "consensus" in lower
            or constraints_dict.get("requires_consensus", False)
        )

        return cls(
            complexity=max(1, min(10, complexity)),
            risk_profile=risk_profile,
            goal_count=goal_count,
            has_security_concerns=has_security,
            has_multi_file_scope=multi_file,
            requires_consensus=requires_consensus,
            affected_files_count=constraints_dict.get("affected_files_count", 0),
        )
