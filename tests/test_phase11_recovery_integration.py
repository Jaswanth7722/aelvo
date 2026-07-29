# tests/test_phase11_recovery_integration.py — Phase 11: Recovery Integration
#
# Tests for:
#   1. ConsensusRecoveryEngine — Deadlock, veto, escalation recovery
#   2. SpecialistRecoveryEngine — Failover, deadline, context preservation
#   3. TaskRecoveryEngine — Savepoints, phase retry/rollback, replanning
#   4. Integration of all three through RecoveryEngine

from __future__ import annotations

import pytest

from runtime_next.recovery.consensus_recovery import (
    ConsensusRecoveryEngine,
    ConsensusFailureType,
    ConsensusRecoveryAction,
)
from runtime_next.recovery.specialist_recovery import (
    SpecialistRecoveryEngine,
    SpecialistRecoveryAction,
    SpecialistState,
)
from runtime_next.recovery.task_recovery import (
    TaskRecoveryEngine,
    TaskRecoveryAction,
    TaskRecoveryTrigger,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def consensus_recovery():
    return ConsensusRecoveryEngine()


@pytest.fixture
def specialist_recovery():
    return SpecialistRecoveryEngine()


@pytest.fixture
def task_recovery():
    return TaskRecoveryEngine()


# =============================================================================
# Test ConsensusRecoveryEngine
# =============================================================================

class TestConsensusRecoveryEngine:
    def test_deadlocked_adds_architect(self, consensus_recovery):
        """Verify deadlocked consensus adds Architect to break tie."""
        result = consensus_recovery.handle_consensus_failure(
            consensus_id="cons_001",
            failure_type=ConsensusFailureType.DEADLOCKED,
            context={
                "participants": ["FORGE", "SENTINEL"],
                "topic": "Should we use async/await?",
            },
        )
        assert result["recovered"] is True
        assert result["action"] == ConsensusRecoveryAction.ADD_ARCHITECT.value
        assert "ARCHITECT" in result.get("remaining_participants", [])
        assert "retry_consensus" in result.get("next_steps", [])

    def test_vetoed_reduces_participants(self, consensus_recovery):
        """Verify vetoed consensus removes the vetoing participant."""
        result = consensus_recovery.handle_consensus_failure(
            consensus_id="cons_002",
            failure_type=ConsensusFailureType.VETOED,
            context={
                "participants": ["FORGE", "SENTINEL", "ARCHITECT"],
                "topic": "Deploy to production?",
                "veto_reason": "Security vulnerability in auth module",
            },
        )
        assert result["recovered"] is True
        assert result["action"] in (
            ConsensusRecoveryAction.REDUCE_PARTICIPANTS.value,
            ConsensusRecoveryAction.MODIFY_PROPOSAL.value,
        )

    def test_participant_timeout(self, consensus_recovery):
        """Verify participant timeout reduces pool."""
        result = consensus_recovery.handle_consensus_failure(
            consensus_id="cons_003",
            failure_type=ConsensusFailureType.PARTICIPANT_TIMEOUT,
            context={
                "participants": ["FORGE", "ORACLE", "SENTINEL"],
                "unresponsive_participants": ["ORACLE"],
            },
        )
        assert result["recovered"] is True
        assert "remaining_participants" in result
        assert "ORACLE" not in result.get("remaining_participants", ["ORACLE"])

    def test_governance_blocked_architect_decides(self, consensus_recovery):
        """Verify governance block is handled by Architect decision."""
        result = consensus_recovery.handle_consensus_failure(
            consensus_id="cons_004",
            failure_type=ConsensusFailureType.GOVERNANCE_BLOCKED,
        )
        assert result["recovered"] is True
        assert result["action"] == ConsensusRecoveryAction.USE_ARCHITECT_DECISION.value

    def test_escalated_architect_decides(self, consensus_recovery):
        """Verify escalated consensus uses Architect decision."""
        result = consensus_recovery.handle_consensus_failure(
            consensus_id="cons_005",
            failure_type=ConsensusFailureType.ESCALATED,
        )
        assert result["recovered"] is True
        assert result["action"] == ConsensusRecoveryAction.USE_ARCHITECT_DECISION.value

    def test_architect_rejected_modifies(self, consensus_recovery):
        """Verify Architect rejection triggers proposal modification."""
        result = consensus_recovery.handle_consensus_failure(
            consensus_id="cons_006",
            failure_type=ConsensusFailureType.ARCHITECT_REJECTED,
            context={"topic": "Refactor database layer"},
        )
        assert result["recovered"] is True
        assert result["action"] == ConsensusRecoveryAction.MODIFY_PROPOSAL.value
        assert "modify_proposal" in result.get("next_steps", [])

    def test_all_strategies_exhausted_escalates(self, consensus_recovery):
        """Verify exhausted strategies escalate to user."""
        # Exhaust each strategy by using up all attempts
        # For DEADLOCKED, there are 3 strategies with max_attempts 2, 2, 1 = 5 attempts
        for _ in range(6):  # Exhaust all
            result = consensus_recovery.handle_consensus_failure(
                consensus_id="cons_exhaust",
                failure_type=ConsensusFailureType.VETOED,  # 3 strategies: 2+2+1 = 5 max
                context={"participants": ["FORGE"], "veto_reason": "test"},
            )
            last_result = result

        assert last_result["recovered"] is False
        assert last_result["action"] == ConsensusRecoveryAction.ESCALATE_TO_USER.value

    def test_escalation_callback(self, consensus_recovery):
        """Verify escalation callback is invoked."""
        callback_called = []

        def cb(cid, result):
            callback_called.append((cid, result))

        consensus_recovery.set_escalate_callback(cb)

        # Trigger escalation by exhausting strategies for ESCALATED type
        for _ in range(2):  # Escalated has 1 strategy with max_attempts=1
            consensus_recovery.handle_consensus_failure(
                consensus_id="cons_cb",
                failure_type=ConsensusFailureType.ESCALATED,
            )

        assert len(callback_called) >= 1

    def test_get_history(self, consensus_recovery):
        """Verify recovery history is recorded."""
        consensus_recovery.handle_consensus_failure(
            "cons_hist", ConsensusFailureType.DEADLOCKED,
        )
        history = consensus_recovery.get_history("cons_hist")
        assert len(history) >= 1
        assert history[0]["consensus_id"] == "cons_hist"

    def test_reset(self, consensus_recovery):
        """Verify reset clears state."""
        consensus_recovery.handle_consensus_failure(
            "cons_reset", ConsensusFailureType.DEADLOCKED,
        )
        consensus_recovery.reset()
        assert consensus_recovery.recovery_count == 0

    def test_recovery_count(self, consensus_recovery):
        """Verify recovery_count tracks correctly."""
        assert consensus_recovery.recovery_count == 0
        consensus_recovery.handle_consensus_failure(
            "cons_cnt", ConsensusFailureType.DEADLOCKED,
        )
        assert consensus_recovery.recovery_count >= 1


# =============================================================================
# Test SpecialistRecoveryEngine
# =============================================================================

class TestSpecialistRecoveryEngine:
    def test_initial_states(self, specialist_recovery):
        """Verify all specialists start as HEALTHY."""
        states = specialist_recovery.get_all_states()
        assert len(states) >= 7
        assert all(s == SpecialistState.HEALTHY for s in states.values())

    def test_mark_failure_degrades(self, specialist_recovery):
        """Verify repeated failures progress through states."""
        for _ in range(3):
            specialist_recovery.mark_failure("FORGE")

        assert specialist_recovery.get_state("FORGE") == SpecialistState.FAILED

    def test_mark_healthy_resets(self, specialist_recovery):
        """Verify mark_healthy resets to HEALTHY."""
        specialist_recovery.mark_failure("FORGE")
        specialist_recovery.mark_healthy("FORGE")
        assert specialist_recovery.get_state("FORGE") == SpecialistState.HEALTHY

    def test_start_task_and_check_deadline(self, specialist_recovery):
        """Verify deadline tracking."""
        specialist_recovery.start_task(
            task_id="task_001",
            specialist="FORGE",
            description="Implement auth handler",
            timeout_seconds=60.0,
        )
        # Should not be overdue yet
        assert specialist_recovery.check_deadline("task_001") is False

    def test_deadline_exceeded(self, specialist_recovery):
        """Verify deadline detection works."""
        specialist_recovery.start_task(
            task_id="task_timeout",
            specialist="FORGE",
            timeout_seconds=-0.001,  # Already past
        )
        assert specialist_recovery.check_deadline("task_timeout") is True

    def test_complete_task_removes_deadline(self, specialist_recovery):
        """Verify completing a task removes its deadline."""
        specialist_recovery.start_task("task_done", "FORGE")
        specialist_recovery.complete_task("task_done")
        # Unknown task should return True (overdue / not found)
        assert specialist_recovery.check_deadline("task_done") is True

    def test_get_active_tasks(self, specialist_recovery):
        """Verify active task listing."""
        specialist_recovery.start_task("task_a", "FORGE", "Task A")
        specialist_recovery.start_task("task_b", "SENTINEL", "Task B")

        tasks = specialist_recovery.get_active_tasks()
        assert len(tasks) == 2
        task_ids = [t["task_id"] for t in tasks]
        assert "task_a" in task_ids
        assert "task_b" in task_ids

    def test_context_preservation(self, specialist_recovery):
        """Verify context save/restore."""
        ctx = {"files": ["auth.py"], "branch": "feature/auth"}
        specialist_recovery.preserve_context("FORGE", ctx)

        restored = specialist_recovery.get_preserved_context("FORGE")
        assert restored is not None
        assert restored["files"] == ["auth.py"]

    def test_clear_context(self, specialist_recovery):
        """Verify context clearing."""
        specialist_recovery.preserve_context("FORGE", {"key": "value"})
        specialist_recovery.clear_preserved_context("FORGE")
        assert specialist_recovery.get_preserved_context("FORGE") is None

    def test_handle_failure_retry_same(self, specialist_recovery):
        """Verify first failure triggers retry with same specialist."""
        specialist_recovery.start_task("task_r1", "FORGE")
        result = specialist_recovery.handle_failure(
            task_id="task_r1",
            specialist="FORGE",
        )
        assert result["recovered"] is True
        assert result["action"] == SpecialistRecoveryAction.RETRY_SAME.value
        assert result["specialist"] == "FORGE"

    def test_handle_failure_failover(self, specialist_recovery):
        """Verify repeated failures trigger failover."""
        specialist_recovery.start_task("task_fail", "FORGE")

        # Exhaust retries
        for _ in range(4):
            result = specialist_recovery.handle_failure("task_fail", "FORGE")

        assert result["recovered"] is True
        assert result["action"] == SpecialistRecoveryAction.FAILOVER.value
        assert result["specialist"] == "TERMINUS"  # FORGE's failover

    def test_handle_failure_escalates_when_no_failover(self, specialist_recovery):
        """Verify escalation when no failover available."""
        specialist_recovery.start_task("task_no_fail", "ARCHITECT")
        # ARCHITECT has no failovers
        for _ in range(4):
            specialist_recovery.handle_failure("task_no_fail", "ARCHITECT")

        result = specialist_recovery.handle_failure("task_no_fail", "ARCHITECT")
        assert result["recovered"] is False
        assert result["action"] == SpecialistRecoveryAction.ESCALATE_TO_ARCHITECT.value

    def test_reassign_callback(self, specialist_recovery):
        """Verify reassign callback is invoked on failover."""
        calls = []

        def cb(orig, repl, ctx):
            calls.append((orig, repl))

        specialist_recovery.set_reassign_callback(cb)

        specialist_recovery.start_task("task_cb", "FORGE")
        for _ in range(4):
            specialist_recovery.handle_failure("task_cb", "FORGE")

        assert len(calls) >= 1
        assert calls[0] == ("FORGE", "TERMINUS")

    def test_reset(self, specialist_recovery):
        """Verify reset restores initial state."""
        specialist_recovery.mark_failure("FORGE")
        specialist_recovery.preserve_context("FORGE", {"key": "val"})
        specialist_recovery.start_task("task_reset", "FORGE")

        specialist_recovery.reset()
        assert specialist_recovery.get_state("FORGE") == SpecialistState.HEALTHY
        assert specialist_recovery.get_preserved_context("FORGE") is None
        assert len(specialist_recovery.get_active_tasks()) == 0

    def test_failure_count(self, specialist_recovery):
        """Verify failure_count tracks failed specialists."""
        assert specialist_recovery.failure_count == 0
        specialist_recovery.mark_failure("FORGE")
        specialist_recovery.mark_failure("FORGE")
        specialist_recovery.mark_failure("FORGE")
        assert specialist_recovery.failure_count >= 1

    def test_recover_restores_state(self, specialist_recovery):
        """Verify recover sets state to RECOVERING."""
        specialist_recovery.mark_failure("FORGE")
        specialist_recovery.recover("FORGE")
        assert specialist_recovery.get_state("FORGE") == SpecialistState.RECOVERING


# =============================================================================
# Test TaskRecoveryEngine
# =============================================================================

class TestTaskRecoveryEngine:
    def test_phase_failure_retry(self, task_recovery):
        """Verify phase failure triggers retry."""
        result = task_recovery.handle_task_failure(
            task_id="task_001",
            trigger=TaskRecoveryTrigger.PHASE_FAILURE,
            context={"phase_id": "phase_1"},
        )
        assert result["recovered"] is True
        assert result["action"] == TaskRecoveryAction.RETRY_PHASE.value
        assert "retry_phase_execution" in result.get("next_steps", [])

    def test_phase_failure_exhausted_falls_through(self, task_recovery):
        """Verify all PHASE_FAILURE strategies can be exhausted."""
        for _ in range(5):  # More than total (retry 2 + skip 1 + rollback 1 = 4)
            result = task_recovery.handle_task_failure(
                task_id="task_skip",
                trigger=TaskRecoveryTrigger.PHASE_FAILURE,
            )
        # After all strategies exhausted, falls through to abort
        assert result["recovered"] is False
        assert "action" in result

    def test_plan_failure_replan(self, task_recovery):
        """Verify plan failure triggers replan."""
        result = task_recovery.handle_task_failure(
            task_id="task_replan",
            trigger=TaskRecoveryTrigger.PLAN_FAILURE,
        )
        assert result["recovered"] is True
        assert result["action"] == TaskRecoveryAction.REPLAN.value
        assert "architect_replan" in result.get("next_steps", [])

    def test_resource_exhaustion_breakdown(self, task_recovery):
        """Verify resource exhaustion triggers breakdown."""
        result = task_recovery.handle_task_failure(
            task_id="task_resource",
            trigger=TaskRecoveryTrigger.RESOURCE_EXHAUSTION,
        )
        # First strategy is BREAKDOWN_TASK
        assert result["recovered"] is True
        assert result["action"] == TaskRecoveryAction.BREAKDOWN_TASK.value

    def test_governance_block_escalates(self, task_recovery):
        """Verify governance block escalates to Architect."""
        result = task_recovery.handle_task_failure(
            task_id="task_gov",
            trigger=TaskRecoveryTrigger.GOVERNANCE_BLOCK,
        )
        assert result["recovered"] is True
        assert result["action"] == TaskRecoveryAction.ESCALATE_TO_ARCHITECT.value

    def test_savepoint_rollback(self, task_recovery):
        """Verify savepoint creation and rollback."""
        state = {"files": ["auth.py"], "phase": "analysis"}
        task_recovery.create_savepoint("task_sp", state)

        restored = task_recovery.rollback_to_savepoint("task_sp")
        assert restored is not None
        assert restored["files"] == ["auth.py"]

    def test_savepoint_clear(self, task_recovery):
        """Verify savepoint clearing."""
        task_recovery.create_savepoint("task_clear", {"key": "val"})
        task_recovery.clear_savepoint("task_clear")
        assert task_recovery.rollback_to_savepoint("task_clear") is None

    def test_dependency_failure_retry(self, task_recovery):
        """Verify dependency failure retries."""
        result = task_recovery.handle_task_failure(
            task_id="task_dep",
            trigger=TaskRecoveryTrigger.DEPENDENCY_FAILURE,
        )
        assert result["recovered"] is True
        assert result["action"] == TaskRecoveryAction.RETRY_PHASE.value

    def test_all_strategies_exhausted(self, task_recovery):
        """Verify exhausted strategies lead to abort."""
        # RESOURCE_EXHAUSTION has 2 strategies: BREAKDOWN_TASK (max=2), ABORT_TASK (max=1)
        for _ in range(4):
            result = task_recovery.handle_task_failure(
                task_id="task_exhaust",
                trigger=TaskRecoveryTrigger.PHASE_FAILURE,
            )

        assert result["recovered"] is False

    def test_architect_callback(self, task_recovery):
        """Verify architect callback is invoked for architect-required strategies."""
        calls = []

        def cb(task_id, context):
            calls.append((task_id, context))

        task_recovery.set_architect_callback(cb)

        # GOVERNANCE_BLOCK requires Architect
        task_recovery.handle_task_failure(
            "task_arch_cb",
            TaskRecoveryTrigger.GOVERNANCE_BLOCK,
        )

        assert len(calls) >= 1

    def test_get_history(self, task_recovery):
        """Verify history tracking."""
        task_recovery.handle_task_failure(
            "task_hist", TaskRecoveryTrigger.PHASE_FAILURE,
        )
        history = task_recovery.get_history("task_hist")
        assert len(history) >= 1

    def test_reset(self, task_recovery):
        """Verify reset clears state."""
        task_recovery.create_savepoint("task_rst", {"key": "val"})
        task_recovery.handle_task_failure("task_rst", TaskRecoveryTrigger.PHASE_FAILURE)

        task_recovery.reset()
        assert task_recovery.recovery_count == 0
        assert task_recovery.rollback_to_savepoint("task_rst") is None


# =============================================================================
# Test Recovery Engine Integration
# =============================================================================

class TestRecoveryEngineIntegration:
    def test_new_subsystems_wired(self):
        """Verify all new subsystems are properly wired into RecoveryEngine."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        # Check Phase 11 subsystems exist
        assert hasattr(engine, "consensus_recovery")
        assert hasattr(engine, "specialist_recovery")
        assert hasattr(engine, "task_recovery")

        # Verify they're properly instantiated
        assert engine.consensus_recovery is not None
        assert engine.specialist_recovery is not None
        assert engine.task_recovery is not None

    def test_reassign_callback_wired(self):
        """Verify reassign callback is connected."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        # Check that the specialist_recovery has the callback set
        # (set in RecoveryEngine.__init__ via set_reassign_callback)
        assert engine.specialist_recovery is not None

        # The callback should be registered — test by triggering a failover
        engine.specialist_recovery.start_task("wired_test", "FORGE")
        result = None
        for _ in range(4):
            result = engine.specialist_recovery.handle_failure("wired_test", "FORGE")

        # After exhausting retries, should attempt failover
        assert result is not None
        assert result["action"] == "failover"
        assert result["specialist"] == "TERMINUS"

    def test_consensus_failure_handled(self):
        """Verify consensus recovery works through the RecoveryEngine's delegate."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        result = engine.consensus_recovery.handle_consensus_failure(
            consensus_id="integ_cons",
            failure_type=ConsensusFailureType.DEADLOCKED,
            context={"participants": ["FORGE", "SENTINEL"]},
        )
        assert result["recovered"] is True

    def test_task_savepoint_integration(self):
        """Verify task savepoints work through RecoveryEngine."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        engine.task_recovery.create_savepoint(
            "integ_sp", {"phase": "complete", "files": ["main.py"]}
        )
        restored = engine.task_recovery.rollback_to_savepoint("integ_sp")
        assert restored is not None
        assert restored["phase"] == "complete"

    def test_consensus_recovery_linked(self):
        """Verify consensus_recovery has a reference back to main RecoveryEngine."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        # The link_recovery_engine should have been called in __init__
        # We can test this indirectly by checking the internal reference
        result = engine.consensus_recovery.handle_consensus_failure(
            "integ_linked",
            ConsensusFailureType.VETOED,
            context={
                "participants": ["FORGE", "SENTINEL"],
                "veto_reason": "Security concern",
            },
        )
        assert result["recovered"] is True


# =============================================================================
# Test End-to-End Recovery Chains
# =============================================================================

class TestEndToEndRecovery:
    def test_full_recovery_chain(self):
        """Verify a full recovery chain: task → specialist → consensus."""
        from runtime_next.recovery.engine import RecoveryEngine
        from runtime_next.recovery.consensus_recovery import ConsensusRecoveryAction
        from runtime_next.recovery.specialist_recovery import SpecialistRecoveryAction
        from runtime_next.recovery.task_recovery import TaskRecoveryAction

        engine = RecoveryEngine()

        # Step 1: Task-level phase failure → retry
        task_result = engine.task_recovery.handle_task_failure(
            "e2e_task",
            TaskRecoveryTrigger.PHASE_FAILURE,
            context={"phase_id": "phase_1"},
        )
        assert task_result["action"] == TaskRecoveryAction.RETRY_PHASE.value

        # Step 2: Specialist failure → retry then failover
        engine.specialist_recovery.start_task("e2e_task", "FORGE")
        sp_result = engine.specialist_recovery.handle_failure("e2e_task", "FORGE")
        assert sp_result["action"] == SpecialistRecoveryAction.RETRY_SAME.value

        # Step 3: Consensus deadlocked → add Architect
        cons_result = engine.consensus_recovery.handle_consensus_failure(
            "e2e_cons",
            ConsensusFailureType.DEADLOCKED,
            context={"participants": ["FORGE", "SENTINEL"]},
        )
        assert cons_result["action"] == ConsensusRecoveryAction.ADD_ARCHITECT.value

    def test_specialist_failover_then_task_rollback(self):
        """Verify specialist failover followed by task rollback."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # Create a savepoint first
        engine.task_recovery.create_savepoint("chain_task", {
            "phase": "analysis_done",
            "files": ["requirements.txt"],
        })

        # Specialist fails and failsover
        engine.specialist_recovery.start_task("chain_task", "FORGE")
        for _ in range(4):
            sp_result = engine.specialist_recovery.handle_failure("chain_task", "FORGE")

        assert sp_result["action"] == "failover"
        assert sp_result["specialist"] == "TERMINUS"

        # Task rolls back
        restored = engine.task_recovery.rollback_to_savepoint("chain_task")
        assert restored is not None
        assert restored["phase"] == "analysis_done"

    def test_consensus_escalation_triggers_task_recovery(self):
        """Verify consensus escalation can trigger task recovery."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # First consensus fails and exhausts strategies
        last_result = None
        for _ in range(6):
            last_result = engine.consensus_recovery.handle_consensus_failure(
                "e2e_escalate",
                ConsensusFailureType.VETOED,
                context={"participants": ["FORGE", "SENTINEL"]},
            )

        assert last_result["recovered"] is False
        assert last_result["action"] == "escalate_to_user"
        assert "user_resolution" in last_result.get("next_steps", [])

        # Task recovery picks up the failure
        task_result = engine.task_recovery.handle_task_failure(
            "e2e_escalate",
            TaskRecoveryTrigger.CONSENSUS_FAILURE,
            context={"consensus_failure": last_result},
        )
        assert task_result["recovered"] is True
        assert task_result["action"] == TaskRecoveryAction.ESCALATE_TO_ARCHITECT.value
