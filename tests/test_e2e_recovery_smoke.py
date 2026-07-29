"""End-to-End Recovery Integration Smoke Test.

Simulates a realistic failure scenario that cascades through all three
recovery levels in a single chain:

  1. Consensus deadlock → ConsensusRecoveryEngine adds Architect
  2. Architect specialist failure → SpecialistRecoveryEngine retries, then escalates
  3. Consensus escalation → TaskRecoveryEngine picks up as CONSENSUS_FAILURE

The chain verifies that recovery decisions at each level correctly feed
into the next level, and that the RecoveryEngine coordinates all three.

Run:  python -m pytest tests/test_e2e_recovery_smoke.py -v
"""

from __future__ import annotations

import pytest
from typing import Dict, List

from runtime_next.recovery.engine import RecoveryEngine
from runtime_next.recovery.consensus_recovery import (
    ConsensusFailureType,
    ConsensusRecoveryAction,
)
from runtime_next.recovery.specialist_recovery import (
    SpecialistRecoveryAction,
    SpecialistState,
)
from runtime_next.recovery.task_recovery import (
    TaskRecoveryAction,
    TaskRecoveryTrigger,
)


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Consensus deadlock cascades through specialist → task recovery
# ═══════════════════════════════════════════════════════════════════════════

class TestE2EThreeLevelRecoveryChain:
    """End-to-end smoke test for the three-level recovery chain.

    Scenario:
        A consensus vote on deploying to production deadlocks (2 FOR, 2 AGAINST).
        ConsensusRecovery attempts ADD_ARCHITECT to break the tie.
        But ARCHITECT is unresponsive — SpecialistRecovery kicks in.
        After exhausting ARCHITECT's retries (ARCHITECT has no failovers),
        SpecialistRecovery escalates.
        The escalating consensus failure is then handled by TaskRecovery
        as a CONSENSUS_FAILURE trigger, which escalates to Architect
        for a decision.

    Assertions at each level:
        1. Consensus: deadlocked → ADD_ARCHITECT recovered=True
        2. Specialist: ARCHITECT retry exhaustion → no failover → escalate
        3. Task: CONSENSUS_FAILURE → ESCALATE_TO_ARCHITECT recovered=True
    """

    def test_three_level_recovery_chain(self):
        """Full chain: consensus deadlock → specialist escalation → task recovery."""
        engine = RecoveryEngine()

        # ── Phase 1: Consensus deadlock ──────────────────────────────────
        # Consensus is deadlocked on a deployment decision.
        # FORGE and SENTINEL disagree with ORACLE and HERMES.
        cons_result = engine.consensus_recovery.handle_consensus_failure(
            consensus_id="e2e_deploy_deadlock",
            failure_type=ConsensusFailureType.DEADLOCKED,
            context={
                "participants": ["FORGE", "SENTINEL", "ORACLE", "HERMES"],
                "topic": "Deploy auth refactor to production",
                "votes": {"FORGE": "approve", "SENTINEL": "approve",
                          "ORACLE": "reject", "HERMES": "reject"},
            },
        )

        # Consensus recovery added ARCHITECT to break the tie
        assert cons_result["recovered"] is True, \
            f"Expected recovered=True, got {cons_result}"
        assert cons_result["action"] == ConsensusRecoveryAction.ADD_ARCHITECT.value, \
            f"Expected ADD_ARCHITECT, got {cons_result['action']}"
        assert "ARCHITECT" in cons_result.get("remaining_participants", []), \
            "ARCHITECT should be added to participants"
        assert cons_result["next_steps"] == ["retry_consensus"], \
            f"Expected retry_consensus next step, got {cons_result['next_steps']}"

        # ── Phase 2: Specialist-level escalation ────────────────────────
        # ARCHITECT is unresponsive (task deadline exceeded).
        # Start with a negative timeout to simulate already-past deadline.
        engine.specialist_recovery.start_task(
            task_id="e2e_architect_decision",
            specialist="ARCHITECT",
            description="Break consensus tie on deploy decision",
            timeout_seconds=-1.0,  # Already past — deadline exceeded
        )

        # Verify deadline is indeed exceeded before proceeding
        assert engine.specialist_recovery.check_deadline("e2e_architect_decision"), \
            "Deadline should be exceeded for -1.0s timeout"

        # ARCHITECT has no failovers in DEFAULT_FAILOVERS, so after
        # exhausting retries, it must escalate.
        sp_result = None
        for _ in range(5):  # Exhaust retries (max_retries=3) + extra
            sp_result = engine.specialist_recovery.handle_failure(
                task_id="e2e_architect_decision",
                specialist="ARCHITECT",
                context={"task": "break_consensus_tie", "consensus_id": "e2e_deploy_deadlock"},
            )

        # Specialist recovery should escalate because ARCHITECT has no failovers
        assert sp_result is not None, "Specialist recovery should produce a result"
        assert sp_result["action"] == SpecialistRecoveryAction.ESCALATE_TO_ARCHITECT.value, \
            f"Expected ESCALATE_TO_ARCHITECT, got {sp_result['action']}"
        assert sp_result["recovered"] is False, \
            "Escalation should set recovered=False since recovery couldn't complete"
        assert "architect_reassign" in sp_result.get("next_steps", []), \
            f"Expected architect_reassign in next_steps, got {sp_result.get('next_steps')}"

        # ── Phase 3: Task-level recovery ────────────────────────────────
        # The consensus-with-specialist escalation is now handled by
        # TaskRecovery as a CONSENSUS_FAILURE trigger.
        task_result = engine.task_recovery.handle_task_failure(
            task_id="e2e_consensus_task",
            trigger=TaskRecoveryTrigger.CONSENSUS_FAILURE,
            context={
                "consensus_id": "e2e_deploy_deadlock",
                "specialist_escalation": sp_result,
                "phase_id": "deploy_approval",
                "topic": "Deploy auth refactor to production",
            },
        )

        # Task recovery escalates to Architect for final decision
        assert task_result["recovered"] is True, \
            f"Expected recovered=True, got {task_result}"
        assert task_result["action"] == TaskRecoveryAction.ESCALATE_TO_ARCHITECT.value, \
            f"Expected ESCALATE_TO_ARCHITECT, got {task_result['action']}"
        assert "architect_decides" in task_result.get("next_steps", []), \
            f"Expected architect_decides in next_steps, got {task_result.get('next_steps')}"

    def test_all_levels_have_history(self):
        """Verify all three recovery levels recorded the chain in their histories."""
        engine = RecoveryEngine()

        # Trigger all three levels
        engine.consensus_recovery.handle_consensus_failure(
            "e2e_hist", ConsensusFailureType.DEADLOCKED,
            context={"participants": ["FORGE", "SENTINEL"]},
        )

        engine.specialist_recovery.start_task("e2e_hist_task", "ARCHITECT",
                                              timeout_seconds=-0.001)
        for _ in range(3):
            engine.specialist_recovery.handle_failure("e2e_hist_task", "ARCHITECT")

        engine.task_recovery.handle_task_failure(
            "e2e_hist_task", TaskRecoveryTrigger.CONSENSUS_FAILURE,
        )

        # Each level should have recorded at least one entry
        cons_history = engine.consensus_recovery.get_history()
        sp_history = engine.specialist_recovery.get_history()
        task_history = engine.task_recovery.get_history()

        assert len(cons_history) >= 1, "Consensus recovery should have history"
        assert len(sp_history) >= 1, "Specialist recovery should have history"
        assert len(task_history) >= 1, "Task recovery should have history"


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Plan phase fails → veto → specialist failover → replan
# ═══════════════════════════════════════════════════════════════════════════

class TestE2EPlanFailureRecoveryChain:
    """End-to-end chain starting from a plan phase failure.

    Scenario:
        A plan phase fails because SENTINEL vetoes a security-sensitive change.
        Consensus recovery tries REDUCE_PARTICIPANTS (remove SENTINEL).
        But removing SENTINEL triggers a security governance concern,
        which is elevated to specialist recovery.
        FORGE takes over but fails repeatedly.
        FORGE failsover to TERMINUS.
        TERMINUS succeeds.
        Task recovery skips the failed phase and continues.
    """

    def test_veto_to_failover_to_skip_chain(self):
        """Chain: veto → reduce participants → specialist failover → task skip."""
        engine = RecoveryEngine()

        # ── Phase 1: Consensus veto ──────────────────────────────────────
        cons_result = engine.consensus_recovery.handle_consensus_failure(
            consensus_id="e2e_security_veto",
            failure_type=ConsensusFailureType.VETOED,
            context={
                "participants": ["FORGE", "SENTINEL", "ORACLE"],
                "topic": "Add credential rotation job",
                "veto_reason": "SENTINEL: Missing encryption for at-rest credentials",
            },
        )

        # First veto strategy: REDUCE_PARTICIPANTS (remove SENTINEL)
        assert cons_result["recovered"] is True
        assert cons_result["action"] in (
            ConsensusRecoveryAction.REDUCE_PARTICIPANTS.value,
            ConsensusRecoveryAction.MODIFY_PROPOSAL.value,
        ), f"Expected REDUCE_PARTICIPANTS or MODIFY_PROPOSAL, got {cons_result['action']}"

        # ── Phase 2: Specialist failover chain ───────────────────────────
        # FORGE takes on the revised task (without SENTINEL) but fails repeatedly.
        engine.specialist_recovery.start_task(
            task_id="e2e_credential_job",
            specialist="FORGE",
            description="Implement credential rotation job",
            timeout_seconds=120.0,
        )

        # First failure → RETRY_SAME
        sp_r1 = engine.specialist_recovery.handle_failure(
            "e2e_credential_job", "FORGE",
            context={"error": "Timeout generating encryption key"},
        )
        assert sp_r1["action"] == SpecialistRecoveryAction.RETRY_SAME.value

        # Retry again → still RETRY_SAME
        sp_r2 = engine.specialist_recovery.handle_failure(
            "e2e_credential_job", "FORGE",
            context={"error": "Memory limit exceeded"},
        )
        assert sp_r2["action"] == SpecialistRecoveryAction.RETRY_SAME.value

        # Third failure → still within budget
        sp_r3 = engine.specialist_recovery.handle_failure(
            "e2e_credential_job", "FORGE",
            context={"error": "Library not found"},
        )
        assert sp_r3["action"] == SpecialistRecoveryAction.RETRY_SAME.value

        # Fourth failure → retries exhausted → FAILOVER to TERMINUS
        sp_r4 = engine.specialist_recovery.handle_failure(
            "e2e_credential_job", "FORGE",
            context={"error": "Library not found"},
        )
        assert sp_r4["action"] == SpecialistRecoveryAction.FAILOVER.value, \
            f"Expected FAILOVER, got {sp_r4['action']}"
        assert sp_r4["specialist"] == "TERMINUS", \
            f"Expected failover to TERMINUS, got {sp_r4['specialist']}"

        # ── Phase 3: Task-level skip ─────────────────────────────────────
        # After the specialist failover, the task phase can be recovered.
        # The original phase failed, but the task can skip it.
        task_result = engine.task_recovery.handle_task_failure(
            task_id="e2e_credential_job",
            trigger=TaskRecoveryTrigger.PHASE_FAILURE,
            context={
                "phase_id": "implement_credential_rotation",
                "specialist_failover": sp_r4,
                "error": "FORGE failed, failovers to TERMINUS",
            },
        )

        # PHASE_FAILURE first strategy: RETRY_PHASE
        assert task_result["recovered"] is True
        assert task_result["action"] == TaskRecoveryAction.RETRY_PHASE.value

    def test_savepoint_rollback_after_specialist_failure(self):
        """Chain: savepoint → specialist failure → rollback → retry."""
        engine = RecoveryEngine()

        # Create a savepoint at the analysis phase
        engine.task_recovery.create_savepoint(
            "e2e_savepoint_task",
            {
                "phase": "analysis_complete",
                "findings": ["auth module needs refactor", "db schema is sound"],
                "files_analyzed": ["auth.py", "schema.sql", "routes.py"],
            },
        )

        # Specialist starts on the implementation phase
        engine.specialist_recovery.start_task(
            "e2e_savepoint_task", "FORGE",
            description="Implement auth refactor",
            timeout_seconds=60.0,
        )

        # Specialist fails repeatedly (e.g., tool unavailable)
        for i in range(4):
            result = engine.specialist_recovery.handle_failure(
                "e2e_savepoint_task", "FORGE",
                context={"error": f"Tool timeout (attempt {i+1})"},
            )

        assert result["action"] == SpecialistRecoveryAction.FAILOVER.value
        assert result["specialist"] == "TERMINUS"

        # Task-level: rollback to the analysis savepoint
        restored = engine.task_recovery.rollback_to_savepoint("e2e_savepoint_task")
        assert restored is not None, "Savepoint should exist"
        assert restored["phase"] == "analysis_complete"
        assert "auth.py" in restored["files_analyzed"]

        # Now retry phase using task recovery
        task_result = engine.task_recovery.handle_task_failure(
            "e2e_savepoint_task",
            TaskRecoveryTrigger.PHASE_FAILURE,
            context={
                "phase_id": "implement_auth_refactor",
                "savepoint_restored": True,
                "restored_state": restored,
            },
        )
        assert task_result["recovered"] is True
        assert task_result["action"] == TaskRecoveryAction.RETRY_PHASE.value


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Plan failure → consensus failure → specialist exhaustion → abort
# ═══════════════════════════════════════════════════════════════════════════

class TestE2EUnrecoverableChain:
    """End-to-end chain where all recovery strategies are exhausted.

    Verifies the system correctly identifies when a situation is unrecoverable
    and produces the expected escalation/abort signals.
    """

    def test_exhaust_all_levels_ends_in_abort(self):
        """All three levels exhausted → task aborts."""
        engine = RecoveryEngine()

        # ── Phase 1: Consensus exhausted ─────────────────────────────────
        # VETOED has 3 strategies (REDUCE_PARTICIPANTS max=2, MODIFY_PROPOSAL max=2,
        # ESCALATE_TO_USER max=1). 5 attempts to exhaust.
        last_cons_result = None
        for _ in range(6):
            last_cons_result = engine.consensus_recovery.handle_consensus_failure(
                consensus_id="e2e_unrecoverable",
                failure_type=ConsensusFailureType.VETOED,
                context={
                    "participants": ["FORGE", "SENTINEL"],
                    "veto_reason": "Persistent veto",
                },
            )

        # Consensus exhausted → escalate to user
        assert last_cons_result["recovered"] is False
        assert last_cons_result["action"] == ConsensusRecoveryAction.ESCALATE_TO_USER.value

        # ── Phase 2: Specialist exhausted ────────────────────────────────
        # ARCHITECT has no failovers and max_retries=3. 4+ attempts exhaust it.
        engine.specialist_recovery.start_task(
            "e2e_unrecoverable_sp", "ARCHITECT",
            timeout_seconds=-1.0,
        )
        last_sp_result = None
        for _ in range(5):
            last_sp_result = engine.specialist_recovery.handle_failure(
                "e2e_unrecoverable_sp", "ARCHITECT",
            )

        # Specialist exhausted → escalate to architect (but none available)
        assert last_sp_result["recovered"] is False
        assert last_sp_result["action"] == SpecialistRecoveryAction.ESCALATE_TO_ARCHITECT.value

        # ── Phase 3: Task exhausted → abort ──────────────────────────────
        # RESOURCE_EXHAUSTION has BREAKDOWN_TASK (max=2) then ABORT_TASK (max=1)
        last_task_result = None
        for _ in range(4):
            last_task_result = engine.task_recovery.handle_task_failure(
                "e2e_unrecoverable_task",
                TaskRecoveryTrigger.RESOURCE_EXHAUSTION,
                context={"budget_exceeded": True, "used_steps": 45, "budget": 30},
            )

        assert last_task_result["recovered"] is False
        assert last_task_result["action"] == TaskRecoveryAction.ABORT_TASK.value
        # After all strategies exhausted, the engine returns abort_task in next_steps
        assert "abort_task" in last_task_result.get("next_steps", [])

    def test_chain_leaves_clean_history(self):
        """After a full chain, all three engines have traceable histories."""
        engine = RecoveryEngine()

        # Consensus
        engine.consensus_recovery.handle_consensus_failure(
            "e2e_clean_hist", ConsensusFailureType.DEADLOCKED,
            context={"participants": ["FORGE", "SENTINEL"]},
        )
        # Specialist
        engine.specialist_recovery.start_task("e2e_clean_sp", "ARCHITECT")
        engine.specialist_recovery.handle_failure("e2e_clean_sp", "ARCHITECT")
        # Task
        engine.task_recovery.handle_task_failure(
            "e2e_clean_task", TaskRecoveryTrigger.PHASE_FAILURE,
        )

        cons_hist = engine.consensus_recovery.get_history("e2e_clean_hist")
        sp_hist = engine.specialist_recovery.get_history("ARCHITECT")
        task_hist = engine.task_recovery.get_history("e2e_clean_task")

        assert len(cons_hist) >= 1
        assert len(sp_hist) >= 1
        assert len(task_hist) >= 1

        # Verify structure of history entries
        for entry in cons_hist:
            assert "consensus_id" in entry
            assert "failure_type" in entry
            assert "attempt" in entry
            assert "strategy" in entry
            assert "timestamp" in entry

        for entry in sp_hist:
            assert "task_id" in entry
            assert "specialist" in entry
            assert "result" in entry
            assert "timestamp" in entry

        for entry in task_hist:
            assert "task_id" in entry
            assert "trigger" in entry
            assert "attempt" in entry
            assert "strategy" in entry
            assert "timestamp" in entry

    def test_reset_clears_all_levels(self):
        """Reset on any engine doesn't affect the others."""
        engine = RecoveryEngine()

        # Record history across all levels
        engine.consensus_recovery.handle_consensus_failure(
            "e2e_reset_a", ConsensusFailureType.DEADLOCKED,
        )
        engine.specialist_recovery.start_task("e2e_reset_sp", "FORGE")
        # Need 3 calls to progress through HEALTHY→DEGRADED→UNRESPONSIVE→FAILED
        for _ in range(3):
            engine.specialist_recovery.handle_failure("e2e_reset_sp", "FORGE")
        engine.task_recovery.handle_task_failure(
            "e2e_reset_task", TaskRecoveryTrigger.GOVERNANCE_BLOCK,
        )

        assert engine.consensus_recovery.recovery_count >= 1
        assert engine.specialist_recovery.failure_count >= 1
        assert engine.task_recovery.recovery_count >= 1

        # Reset only consensus
        engine.consensus_recovery.reset()

        assert engine.consensus_recovery.recovery_count == 0
        assert engine.specialist_recovery.failure_count >= 1  # Unchanged
        assert engine.task_recovery.recovery_count >= 1  # Unchanged

        # Reset task
        engine.task_recovery.reset()
        assert engine.task_recovery.recovery_count == 0
        assert engine.specialist_recovery.failure_count >= 1  # Still unchanged


# ═══════════════════════════════════════════════════════════════════════════
# Scenario: Full integration with RecoveryEngine callbacks
# ═══════════════════════════════════════════════════════════════════════════

class TestE2ECallbackChain:
    """End-to-end chain exercising all three recovery levels through callbacks."""

    def test_escalate_callback_through_all_levels(self):
        """Consensus escalate callback + architect callback + reassign callback."""
        engine = RecoveryEngine()

        captured_events: List[str] = []

        # Register consensus escalation callback
        engine.consensus_recovery.set_escalate_callback(
            lambda cid, result: captured_events.append(f"consensus_escalate:{cid[:8]}")
        )

        # Register task architect callback
        engine.task_recovery.set_architect_callback(
            lambda tid, ctx: captured_events.append(f"task_architect:{tid[:8]}")
        )

        # Register specialist reassign callback
        engine.specialist_recovery.set_reassign_callback(
            lambda orig, repl, ctx: captured_events.append(f"reassign:{orig}->{repl}")
        )

        # ── Chain: consensus veto → specialist failover → task architect ──
        # 1. Exhaust consensus to trigger escalate callback
        for _ in range(6):
            engine.consensus_recovery.handle_consensus_failure(
                "e2e_cb_chain",
                ConsensusFailureType.VETOED,
                context={"participants": ["FORGE"], "veto_reason": "test"},
            )

        # 2. Trigger specialist failover callback
        engine.specialist_recovery.start_task("e2e_cb_sp", "FORGE")
        for _ in range(4):
            engine.specialist_recovery.handle_failure("e2e_cb_sp", "FORGE")

        # 3. Trigger task architect callback (GOVERNANCE_BLOCK requires it)
        engine.task_recovery.handle_task_failure(
            "e2e_cb_task", TaskRecoveryTrigger.GOVERNANCE_BLOCK,
        )

        # All three callbacks should have fired
        assert any("consensus_escalate:" in e for e in captured_events), \
            f"Expected consensus escalate callback, got {captured_events}"
        assert any("reassign:" in e for e in captured_events), \
            f"Expected reassign callback, got {captured_events}"
        assert any("task_architect:" in e for e in captured_events), \
            f"Expected task architect callback, got {captured_events}"
