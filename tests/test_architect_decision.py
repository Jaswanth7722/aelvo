"""Tests for Phase 5 — ARCHITECT mode selection & decision authority.

Covers:
- ``ArchitectDecision`` — all 5 outcomes, creation, summary, terminal display
- ``ArchitectDecisionOutcome`` — enum values
- ``ExecutionMode`` — CONSOLIDATED / COLLABORATIVE
- ``ModeSelectionCriteria`` — from_hermes_context, select_mode, rationale
- ``ArchitectSpecialist.select_execution_mode()`` — mode selection logic
- ``ArchitectSpecialist.make_decision()`` — decision creation
- ``ArchitectSpecialist.review_consensus()`` — consensus review logic
- ``ArchitectSpecialist.apply_decision()`` — decision application
"""

import time


from cognition.architect_decision import (
    ArchitectDecision,
    ArchitectDecisionOutcome,
    ExecutionMode,
    ModeSelectionCriteria,
)
from specialists.architect import ArchitectSpecialist


# ===========================================================================
# ArchitectDecisionOutcome
# ===========================================================================


class TestArchitectDecisionOutcome:
    def test_all_outcomes_present(self):
        values = {e.value for e in ArchitectDecisionOutcome}
        assert values == {"approve", "reject", "escalate", "replan", "override"}

    def test_outcome_count(self):
        assert len(ArchitectDecisionOutcome) == 5


class TestExecutionMode:
    def test_both_modes_present(self):
        values = {e.value for e in ExecutionMode}
        assert values == {"consolidated", "collaborative"}

    def test_mode_count(self):
        assert len(ExecutionMode) == 2


# ===========================================================================
# ArchitectDecision
# ===========================================================================


class TestArchitectDecision:
    def test_create_approve(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="consensus",
            target_id="ce_123",
            reason="Strong agreement",
        )
        assert d.outcome == ArchitectDecisionOutcome.APPROVE
        assert d.target_id == "ce_123"
        assert d.decided_by == "ARCHITECT"

    def test_create_reject(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.REJECT,
            target_type="task",
            target_id="task_456",
            reason="Security concerns",
            assigned_to="FORGE",
            assigned_reason="Revise and resubmit",
        )
        assert d.outcome == ArchitectDecisionOutcome.REJECT
        assert d.assigned_to == "FORGE"

    def test_create_escalate(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.ESCALATE,
            target_type="consensus",
            target_id="ce_789",
            reason="Tied vote, user input needed",
        )
        assert d.outcome == ArchitectDecisionOutcome.ESCALATE

    def test_create_replan(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.REPLAN,
            target_type="plan",
            target_id="plan_101",
            reason="Blocked dependencies",
            replan_trigger="blocked_path",
            replan_scope="full",
        )
        assert d.outcome == ArchitectDecisionOutcome.REPLAN
        assert d.replan_trigger == "blocked_path"
        assert d.replan_scope == "full"

    def test_create_override(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.OVERRIDE,
            target_type="consensus",
            target_id="ce_202",
            reason="Architect disagrees with consensus",
            overridden_recommendation="Proceed with deployment",
            override_rationale="Deployment would break production",
        )
        assert d.outcome == ArchitectDecisionOutcome.OVERRIDE
        assert "Deployment" in d.override_rationale

    def test_create_with_conditions(self):
        conditions = ["All tests must pass", "Security review required"]
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="task",
            target_id="t1",
            reason="Conditional approval",
            conditions=conditions,
        )
        assert len(d.conditions) == 2
        assert "Security review required" in d.conditions

    def test_summary(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="p1",
            reason="Looks good",
        )
        s = d.summary()
        assert s["outcome"] == "approve"
        assert "p1" in s["target"]
        assert s["reason"] == "Looks good"

    def test_terminal_display_approve(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="p_123",
            reason="All criteria satisfied",
        )
        display = d.to_terminal_display()
        assert "APPROVE" in display
        assert "p_123" in display

    def test_terminal_display_override(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.OVERRIDE,
            target_type="consensus",
            target_id="ce_1",
            reason="Safety override",
            overridden_recommendation="Deploy now",
            override_rationale="Unsafe to deploy",
        )
        display = d.to_terminal_display()
        assert "OVERRIDE" in display
        assert "Unsafe" in display

    def test_terminal_display_replan(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.REPLAN,
            target_type="plan",
            target_id="p_2",
            reason="Too risky",
            replan_trigger="blocked",
            replan_scope="full",
        )
        display = d.to_terminal_display()
        assert "REPLAN" in display
        assert "full" in display

    def test_default_decided_by(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="test",
            target_id="t1",
            reason="test",
        )
        assert d.decided_by == "ARCHITECT"

    def test_created_at_timestamp(self):
        d = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="test",
            target_id="t1",
            reason="test",
        )
        now = time.time()
        assert abs(d.created_at - now) < 2.0


# ===========================================================================
# ModeSelectionCriteria
# ===========================================================================


class TestModeSelectionCriteria:
    def test_from_hermes_context_simple(self):
        criteria = ModeSelectionCriteria.from_hermes_context(
            task="Fix a typo in the README",
            risk_profile="low",
            complexity=1,
        )
        assert criteria.risk_profile == "low"
        assert criteria.complexity == 1
        assert criteria.has_security_concerns is False
        assert criteria.has_multi_file_scope is False

    def test_from_hermes_context_security(self):
        criteria = ModeSelectionCriteria.from_hermes_context(
            task="Audit authentication tokens for security vulnerabilities",
            risk_profile="medium",
            complexity=5,
        )
        assert criteria.has_security_concerns is True

    def test_from_hermes_context_multi_file(self):
        criteria = ModeSelectionCriteria.from_hermes_context(
            task="Refactor the entire authentication module across multiple files",
        )
        assert criteria.has_multi_file_scope is True

    def test_from_hermes_context_constraints(self):
        criteria = ModeSelectionCriteria.from_hermes_context(
            task="Implement feature",
            constraints={"requires_consensus": True, "affected_files_count": 8},
        )
        assert criteria.requires_consensus is True
        assert criteria.affected_files_count == 8

    def test_select_mode_low_risk(self):
        criteria = ModeSelectionCriteria(complexity=2, risk_profile="low")
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_select_mode_high_risk(self):
        criteria = ModeSelectionCriteria(complexity=3, risk_profile="high")
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_select_mode_high_complexity(self):
        criteria = ModeSelectionCriteria(complexity=5, risk_profile="low")
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_select_mode_security_concerns(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", has_security_concerns=True,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_select_mode_requires_consensus(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", requires_consensus=True,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_select_mode_many_files(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", affected_files_count=5,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_select_mode_many_goals(self):
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", goal_count=4,
        )
        assert criteria.select_mode() == ExecutionMode.COLLABORATIVE

    def test_select_mode_edge_complexity_4(self):
        """complexity=4 should still be CONSOLIDATED (threshold is > 4)."""
        criteria = ModeSelectionCriteria(
            complexity=4, risk_profile="low",
        )
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_select_mode_edge_files_4(self):
        """affected_files=4 should be CONSOLIDATED (threshold is >= 5)."""
        criteria = ModeSelectionCriteria(
            complexity=2, risk_profile="low", affected_files_count=4,
        )
        assert criteria.select_mode() == ExecutionMode.CONSOLIDATED

    def test_rationale_consolidated(self):
        criteria = ModeSelectionCriteria(complexity=1, risk_profile="low")
        rationale = criteria.rationale()
        assert "consolidated" in rationale
        assert "low risk" in rationale

    def test_rationale_collaborative(self):
        criteria = ModeSelectionCriteria(
            complexity=6, risk_profile="high",
        )
        rationale = criteria.rationale()
        assert "collaborative" in rationale
        assert "risk=high" in rationale or "complexity=6" in rationale


# ===========================================================================
# ArchitectSpecialist — Mode Selection
# ===========================================================================


class TestArchitectModeSelection:
    """``ArchitectSpecialist.select_execution_mode()`` integration."""

    def setup_method(self):
        self.architect = ArchitectSpecialist()

    def test_selects_consolidated_for_simple_task(self):
        result = self.architect.select_execution_mode(
            task="Fix a typo in the README",
            risk_profile="low",
            complexity=1,
        )
        assert result["mode"] == ExecutionMode.CONSOLIDATED
        assert "consolidated" in result["rationale"]
        assert isinstance(result["criteria"], ModeSelectionCriteria)

    def test_selects_collaborative_for_high_risk(self):
        result = self.architect.select_execution_mode(
            task="Deploy to production",
            risk_profile="high",
            complexity=3,
        )
        assert result["mode"] == ExecutionMode.COLLABORATIVE

    def test_selects_collaborative_for_high_complexity(self):
        result = self.architect.select_execution_mode(
            task="Build a distributed system",
            risk_profile="low",
            complexity=7,
        )
        assert result["mode"] == ExecutionMode.COLLABORATIVE

    def test_selects_collaborative_for_security(self):
        result = self.architect.select_execution_mode(
            task="Audit authentication tokens for security vulnerabilities",
            risk_profile="medium",
            complexity=3,
        )
        assert result["mode"] == ExecutionMode.COLLABORATIVE

    def test_select_with_goals(self):
        goals = ["step 1", "step 2", "step 3", "step 4", "step 5"]
        result = self.architect.select_execution_mode(
            task="Complex task",
            risk_profile="low",
            complexity=2,
            goals=goals,
        )
        # 5 goals >= 4 -> COLLABORATIVE
        assert result["mode"] == ExecutionMode.COLLABORATIVE

    def test_select_with_constraints(self):
        result = self.architect.select_execution_mode(
            task="Implement feature",
            risk_profile="low",
            complexity=2,
            constraints={"affected_files_count": 6},
        )
        assert result["mode"] == ExecutionMode.COLLABORATIVE

    def test_select_with_hermes_context_fields(self):
        goals = ["Analyze", "Implement", "Verify"]
        result = self.architect.select_execution_mode(
            task="Update login flow",
            risk_profile="low",
            complexity=2,
            goals=goals,
        )
        assert result["mode"] == ExecutionMode.CONSOLIDATED


# ===========================================================================
# ArchitectSpecialist — Decision Authority
# ===========================================================================


class TestArchitectMakeDecision:
    """``ArchitectSpecialist.make_decision()``."""

    def setup_method(self):
        self.architect = ArchitectSpecialist()

    def test_make_approve(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="consensus",
            target_id="ce_1",
            reason="Good consensus",
        )
        assert isinstance(d, ArchitectDecision)
        assert d.outcome == ArchitectDecisionOutcome.APPROVE
        assert d.decision_id is not None
        assert len(d.decision_id) == 16  # hexdigest[:16]

    def test_make_reject_with_assignment(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.REJECT,
            target_type="task",
            target_id="t_1",
            reason="Needs revision",
            assigned_to="FORGE",
            assigned_reason="Fix security issues",
        )
        assert d.assigned_to == "FORGE"
        assert d.assigned_reason == "Fix security issues"

    def test_make_override(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.OVERRIDE,
            target_type="consensus",
            target_id="ce_2",
            reason="Safety first",
            overridden_recommendation="Proceed",
            override_rationale="Too dangerous",
        )
        assert d.overridden_recommendation == "Proceed"
        assert "dangerous" in d.override_rationale


class TestArchitectReviewConsensus:
    """``ArchitectSpecialist.review_consensus()``."""

    def setup_method(self):
        self.architect = ArchitectSpecialist()

    def test_approves_strong_consensus(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed with implementation",
            consensus_confidence=0.9,
            consensus_id="ce_1",
            positions={"ORACLE": "yes", "FORGE": "yes", "SENTINEL": "yes"},
            task="Add login feature",
        )
        assert decision.outcome == ArchitectDecisionOutcome.APPROVE
        assert "strong consensus" in decision.reason.lower()

    def test_approves_majority_with_conditions(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed",
            consensus_confidence=0.6,
            consensus_id="ce_2",
            positions={"ORACLE": "yes", "FORGE": "yes", "SENTINEL": "no"},
            task="Refactor auth",
        )
        assert decision.outcome == ArchitectDecisionOutcome.APPROVE
        assert len(decision.conditions) >= 1

    def test_rejects_majority_against(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed",
            consensus_confidence=0.3,
            consensus_id="ce_3",
            positions={"ORACLE": "no", "FORGE": "no", "SENTINEL": "yes"},
            task="High risk change",
        )
        assert decision.outcome == ArchitectDecisionOutcome.REJECT

    def test_rejects_high_risk_low_confidence(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Deploy to production",
            consensus_confidence=0.4,
            consensus_id="ce_4",
            positions={"ORACLE": "yes", "FORGE": "no"},
            task="Production deployment",
            risk_profile="high",
            complexity=8,
        )
        assert decision.outcome == ArchitectDecisionOutcome.REJECT

    def test_approves_high_risk_high_confidence(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Deploy",
            consensus_confidence=0.9,
            consensus_id="ce_5",
            positions={"ORACLE": "yes", "FORGE": "yes", "SENTINEL": "yes"},
            task="Production deployment",
            risk_profile="high",
            complexity=8,
        )
        assert decision.outcome == ArchitectDecisionOutcome.APPROVE

    def test_replans_tied_high_complexity(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed",
            consensus_confidence=0.5,
            consensus_id="ce_6",
            positions={"ORACLE": "yes", "FORGE": "no"},
            task="Complex architecture change",
            complexity=7,
        )
        assert decision.outcome in (
            ArchitectDecisionOutcome.REPLAN,
        )

    def test_escalates_tied_low_complexity(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed",
            consensus_confidence=0.5,
            consensus_id="ce_7",
            positions={"ORACLE": "yes", "FORGE": "no"},
            task="Simple change",
            complexity=2,
        )
        assert decision.outcome in (
            ArchitectDecisionOutcome.ESCALATE,
        )

    def test_overrides_low_confidence_consensus(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed with risky change",
            consensus_confidence=0.3,
            consensus_id="ce_8",
            positions={"ORACLE": "yes", "FORGE": "yes"},
            task="Risky refactor",
        )
        assert decision.outcome == ArchitectDecisionOutcome.OVERRIDE
        assert decision.overridden_recommendation == "Proceed with risky change"

    def test_default_approve(self):
        """Fallback: approve when no specific conditions match."""
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed",
            consensus_confidence=0.5,
            consensus_id="ce_9",
            positions={},
            task="Simple task",
        )
        assert decision.outcome == ArchitectDecisionOutcome.APPROVE

    def test_review_with_conditions(self):
        decision = self.architect.review_consensus(
            consensus_recommendation="Proceed",
            consensus_confidence=0.85,
            consensus_id="ce_10",
            positions={"ORACLE": "yes"},
            task="Deploy",
            conditions=["Tests must pass", "Security reviewed"],
        )
        assert decision.outcome == ArchitectDecisionOutcome.APPROVE
        assert len(decision.conditions) == 2


class TestArchitectApplyDecision:
    """``ArchitectSpecialist.apply_decision()``."""

    def setup_method(self):
        self.architect = ArchitectSpecialist()

    def test_apply_approve(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="p1",
            reason="Good plan",
        )
        result = self.architect.apply_decision(d)
        assert result["applied"] is True
        assert "APPROVED" in result["result"]
        assert result["decision"] is d

    def test_apply_reject(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.REJECT,
            target_type="task",
            target_id="t1",
            reason="Needs work",
            assigned_to="FORGE",
        )
        result = self.architect.apply_decision(d)
        assert "REJECTED" in result["result"]
        assert "FORGE" in result["result"]

    def test_apply_escalate(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.ESCALATE,
            target_type="consensus",
            target_id="ce_1",
            reason="User input required",
        )
        result = self.architect.apply_decision(d)
        assert "ESCALATED" in result["result"]

    def test_apply_override(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.OVERRIDE,
            target_type="consensus",
            target_id="ce_2",
            reason="Override unsafe recommendation",
            overridden_recommendation="Deploy now",
            override_rationale="Not safe",
        )
        result = self.architect.apply_decision(d)
        assert "OVERRIDE" in result["result"]
        assert "Deploy now" in result["result"]

    def test_apply_replan_no_engine(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.REPLAN,
            target_type="plan",
            target_id="p1",
            reason="Too risky",
            replan_trigger="blocked",
        )
        result = self.architect.apply_decision(d, context={})
        assert "REPLAN" in result["result"]
        # No replan engine in context, so the replan is deferred
        assert result["applied"] is False

    def test_apply_with_empty_context(self):
        d = self.architect.make_decision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="p1",
            reason="ok",
        )
        result = self.architect.apply_decision(d, context={})
        assert result["applied"] is True

    def test_apply_many_decisions(self):
        """Multiple decisions should all be created independently."""
        decisions = []
        for i in range(5):
            d = self.architect.make_decision(
                outcome=ArchitectDecisionOutcome.APPROVE,
                target_type="plan",
                target_id=f"p_{i}",
                reason=f"Approval {i}",
            )
            decisions.append(d)
        assert len(set(d.decision_id for d in decisions)) == 5
