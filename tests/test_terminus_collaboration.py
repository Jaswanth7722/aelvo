"""Tests for Phase 6 — TERMINUS blackboard-based collaboration + execution gate.

Per Amendment 2: No agent-to-agent messaging.
Per Amendment 3: TERMINUS is a hard gate that checks Architect Decision.

Covers:
- ``TerminusSpecialist.pickup_task()`` — pick up EXECUTION tasks from SharedTaskBoard
- ``TerminusSpecialist.check_architect_decision()`` — execution gate (APPROVE blocks/runs)
- ``TerminusSpecialist.publish_execution_result()`` — publish ExecutionResultEntry
- ``TerminusSpecialist.publish_failure_report()`` — publish failure info
- ``TerminusSpecialist.read_execution_results()`` — read ExecutionResultEntry
- ``ExecutionBlockedError`` — raised when no APPROVE decision
- No direct messaging — no Message, no send_message, no AgentCommunicationRouter
"""

import json
import pytest
from typing import Any, Dict, List

from specialists.terminus import TerminusSpecialist, ExecutionBlockedError
from cognition.blackboard import CognitiveBlackboard
from cognition.blackboard_schemas import ExecutionResultEntry
from cognition.architect_decision import (
    ArchitectDecision,
    ArchitectDecisionOutcome,
)
from cognition.types import (
    EntryType, Provenance, ProvenanceType,
)


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def terminus() -> TerminusSpecialist:
    return TerminusSpecialist()


@pytest.fixture
def blackboard() -> CognitiveBlackboard:
    return CognitiveBlackboard()


@pytest.fixture
def provenance() -> Provenance:
    return Provenance(source_type=ProvenanceType.SPECIALIST, source_id="test")


# ===========================================================================
# Fake TaskBoard for testing
# ===========================================================================


class FakeTaskBoard:
    """A lightweight task board mock for testing TERMINUS task pickup."""

    def __init__(self):
        self._tasks: Dict[str, dict] = {}
        self._transitions: List[str] = []

    def add_task(self, task_id: str, **kwargs):
        defaults = {
            "type": "execute",
            "status": "pending",
            "specialist": "",
            "title": "Test execution",
            "assigned_by": "",
        }
        defaults.update(kwargs)
        self._tasks[task_id] = defaults

    def get_tasks(self, status=None, task_type=None, limit=100) -> List[Any]:
        from shared_task_board.task import Task, TaskStatus, TaskType, TaskPriority
        results = []
        for tid, data in self._tasks.items():
            if status and data.get("status") != status.value:
                continue
            if task_type and data.get("type") != task_type.value:
                continue
            task = Task(
                id=tid,
                type=TaskType(data.get("type", "execution")),
                status=TaskStatus(data.get("status", "pending")),
                specialist=data.get("specialist", ""),
                title=data.get("title", "Test"),
                assigned_by=data.get("assigned_by", ""),
                priority=TaskPriority.MEDIUM,
            )
            results.append(task)
            if len(results) >= limit:
                break
        return results

    def assign_task(self, task_id: str, specialist: str, assigned_by: str = "architect"):
        if task_id in self._tasks:
            self._tasks[task_id]["specialist"] = specialist
            self._tasks[task_id]["assigned_by"] = assigned_by
            self._tasks[task_id]["status"] = "assigned"
            self._transitions.append(f"assigned:{task_id}")

    def start_task(self, task_id: str):
        if task_id in self._tasks:
            self._tasks[task_id]["status"] = "in_progress"
            self._transitions.append(f"started:{task_id}")

    @property
    def transition_log(self) -> List[str]:
        return list(self._transitions)


# ===========================================================================
# ExecutionBlockedError
# ===========================================================================


class TestExecutionBlockedError:
    def test_default_message(self):
        err = ExecutionBlockedError()
        assert "ExecutionBlocked" in str(err)

    def test_with_reason(self):
        err = ExecutionBlockedError(reason="No architect decision")
        assert "No architect decision" in str(err)

    def test_with_all_fields(self):
        err = ExecutionBlockedError(
            reason="Not APPROVE",
            target_id="plan_123",
            command="git push",
            decision_outcome="reject",
        )
        msg = str(err)
        assert "Not APPROVE" in msg
        assert "plan_123" in msg
        assert "git push" in msg
        assert "reject" in msg

    def test_is_exception(self):
        assert issubclass(ExecutionBlockedError, Exception)

    def test_raise_and_catch(self):
        try:
            raise ExecutionBlockedError(reason="test")
        except ExecutionBlockedError as e:
            assert e.reason == "test"


# ===========================================================================
# Task Pickup
# ===========================================================================


class TestPickupTask:
    def test_pickup_no_tasks(self, terminus: TerminusSpecialist):
        board = FakeTaskBoard()
        picked = terminus.pickup_task(board)
        assert picked == []

    def test_pickup_pending_execution_task(self, terminus: TerminusSpecialist):
        board = FakeTaskBoard()
        board.add_task("t1", type="execute", status="pending", title="Run tests")
        picked = terminus.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t1"
        assert "assigned:t1" in board.transition_log
        assert "started:t1" in board.transition_log

    def test_pickup_already_assigned_to_terminus(self, terminus: TerminusSpecialist):
        board = FakeTaskBoard()
        board.add_task("t2", type="execute", status="assigned", specialist="TERMINUS")
        picked = terminus.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t2"

    def test_pickup_ignores_other_specialist_tasks(self, terminus: TerminusSpecialist):
        board = FakeTaskBoard()
        board.add_task("t3", type="execute", status="pending", specialist="FORGE")
        picked = terminus.pickup_task(board)
        assert picked == []

    def test_pickup_non_execution_task(self, terminus: TerminusSpecialist):
        board = FakeTaskBoard()
        board.add_task("t4", type="implement", status="pending", title="Write code")
        picked = terminus.pickup_task(board)
        assert picked == []

    def test_pickup_max_tasks(self, terminus: TerminusSpecialist):
        board = FakeTaskBoard()
        for i in range(5):
            board.add_task(f"t{i}", type="execute", status="pending", title=f"Task {i}")
        picked = terminus.pickup_task(board, max_tasks=2)
        assert len(picked) == 2

    def test_pickup_with_none_board(self, terminus: TerminusSpecialist):
        picked = terminus.pickup_task(None)
        assert picked == []

    def test_pickup_only_terminus_tasks(self, terminus: TerminusSpecialist):
        board = FakeTaskBoard()
        board.add_task("t7", type="execute", status="pending", title="Terminus task")
        board.add_task("t8", type="implement", status="pending", title="Forge task")
        board.add_task("t9", type="research", status="pending", title="Oracle task")
        picked = terminus.pickup_task(board, max_tasks=10)
        assert len(picked) == 1
        assert picked[0].id == "t7"


# ===========================================================================
# Check Architect Decision (Execution Gate)
# ===========================================================================


class TestCheckArchitectDecision:
    def test_no_decision_raises_blocked(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        """With no decisions on the blackboard, the gate raises ExecutionBlockedError."""
        with pytest.raises(ExecutionBlockedError) as exc_info:
            terminus.check_architect_decision(blackboard, target_id="plan_1")
        assert "No Architect decision" in str(exc_info.value)

    def test_approve_decision_passes_gate(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        """An APPROVE decision allows execution through the gate."""
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="plan_1",
            reason="Looks good",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        result = terminus.check_architect_decision(blackboard, target_id="plan_1")
        assert result["approved"] is True
        assert result["decision"].outcome == ArchitectDecisionOutcome.APPROVE

    def test_reject_decision_raises_blocked(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        """A REJECT decision raises ExecutionBlockedError."""
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.REJECT,
            target_type="plan",
            target_id="plan_2",
            reason="Security concerns",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        with pytest.raises(ExecutionBlockedError) as exc_info:
            terminus.check_architect_decision(blackboard, target_id="plan_2")
        assert "reject" in str(exc_info.value).lower()

    def test_escalate_decision_raises_blocked(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.ESCALATE,
            target_type="plan",
            target_id="plan_3",
            reason="Need user input",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        with pytest.raises(ExecutionBlockedError) as exc_info:
            terminus.check_architect_decision(blackboard, target_id="plan_3")
        assert "escalate" in str(exc_info.value).lower()

    def test_replan_decision_raises_blocked(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.REPLAN,
            target_type="plan",
            target_id="plan_4",
            reason="Replan needed",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        with pytest.raises(ExecutionBlockedError) as exc_info:
            terminus.check_architect_decision(blackboard, target_id="plan_4")
        assert "replan" in str(exc_info.value).lower()

    def test_override_decision_raises_blocked(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.OVERRIDE,
            target_type="plan",
            target_id="plan_5",
            reason="Architect disagrees",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        with pytest.raises(ExecutionBlockedError) as exc_info:
            terminus.check_architect_decision(blackboard, target_id="plan_5")
        assert "override" in str(exc_info.value).lower()

    def test_wrong_target_id_raises_blocked(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        """A decision for a different target_id should not pass the gate."""
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="other_plan",
            reason="OK",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        with pytest.raises(ExecutionBlockedError):
            terminus.check_architect_decision(blackboard, target_id="my_plan")

    def test_approve_with_conditions(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        """An APPROVE decision with conditions includes them in the result."""
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="plan_cond",
            reason="Approved with conditions",
            conditions=["Run tests", "Security review passed"],
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        result = terminus.check_architect_decision(blackboard, target_id="plan_cond")
        assert result["approved"] is True
        assert len(result["conditions"]) == 2

    def test_approve_any_target_without_filter(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        """When no target_id is specified, any APPROVE decision passes the gate."""
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="some_plan",
            reason="OK",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        # No target_id filter — should find the decision
        result = terminus.check_architect_decision(blackboard)
        assert result["approved"] is True

    def test_none_blackboard_raises_blocked(self, terminus: TerminusSpecialist):
        """A None blackboard raises ExecutionBlockedError."""
        with pytest.raises(ExecutionBlockedError) as exc_info:
            terminus.check_architect_decision(None, target_id="plan_1")
        assert "No blackboard" in str(exc_info.value)

    def test_approve_decision_with_command_context(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        """Command context is included in the result when a command is specified."""
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="plan_cmd",
            reason="Go ahead",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        result = terminus.check_architect_decision(
            blackboard, target_id="plan_cmd", command="git push",
        )
        assert result["approved"] is True


# ===========================================================================
# Publish Execution Result
# ===========================================================================


class TestPublishExecutionResult:
    def test_publish_success(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        entry_id = terminus.publish_execution_result(
            blackboard=blackboard,
            command="pytest tests/",
            exit_code=0,
            stdout="All tests passed",
            stderr="",
            success=True,
            duration_ms=1250.0,
        )
        assert entry_id != ""

        entries = blackboard.read("execution_results")
        assert len(entries) == 1
        restored = ExecutionResultEntry.from_entry_content(entries[0].content)
        assert restored.command == "pytest tests/"
        assert restored.exit_code == 0
        assert restored.success is True
        assert restored.duration_ms == 1250.0

    def test_publish_failure(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        entry_id = terminus.publish_execution_result(
            blackboard=blackboard,
            command="deploy.sh",
            exit_code=1,
            stdout="",
            stderr="Connection refused",
            success=False,
            duration_ms=500.0,
            task_id="task_99",
        )
        assert entry_id != ""

        entries = blackboard.read("execution_results")
        assert len(entries) == 1
        restored = ExecutionResultEntry.from_entry_content(entries[0].content)
        assert restored.success is False
        assert restored.exit_code == 1

    def test_publish_with_none_blackboard(self, terminus: TerminusSpecialist):
        entry_id = terminus.publish_execution_result(None, command="test")
        assert entry_id == ""

    def test_publish_minimal(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        entry_id = terminus.publish_execution_result(
            blackboard=blackboard,
            command="echo hello",
        )
        assert entry_id != ""

    def test_publish_multiple_results(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        for i in range(3):
            terminus.publish_execution_result(
                blackboard, command=f"cmd_{i}", exit_code=i,
            )
        entries = blackboard.read("execution_results")
        assert len(entries) == 3


# ===========================================================================
# Publish Failure Report
# ===========================================================================


class TestPublishFailureReport:
    def test_publish_failure(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        entry_id = terminus.publish_failure_report(
            blackboard=blackboard,
            command="git push",
            error_message="Remote rejected: permission denied",
            exit_code=128,
            recovery_suggestion="Check SSH keys and permissions",
            task_id="task_42",
        )
        assert entry_id != ""

        # Read the failure from the blackboard
        entries = blackboard.read("execution_failures")
        assert len(entries) == 1
        payload = json.loads(entries[0].content)
        assert "git push" in payload["command"]
        assert payload["exit_code"] == 128
        assert "SSH" in payload["recovery_suggestion"]

    def test_publish_failure_with_none_blackboard(self, terminus: TerminusSpecialist):
        entry_id = terminus.publish_failure_report(None, command="test")
        assert entry_id == ""

    def test_publish_failure_minimal(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        entry_id = terminus.publish_failure_report(
            blackboard=blackboard,
            command="deploy.sh",
            error_message="Failed",
        )
        assert entry_id != ""

    def test_publish_multiple_failures(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        for i in range(2):
            terminus.publish_failure_report(
                blackboard, command=f"cmd_{i}", error_message=f"Error {i}",
            )
        entries = blackboard.read("execution_failures")
        assert len(entries) == 2


# ===========================================================================
# Read Execution Results
# ===========================================================================


class TestReadExecutionResults:
    def test_read_empty(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard):
        results = terminus.read_execution_results(blackboard)
        assert results == []

    def test_read_results(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        result = ExecutionResultEntry(
            specialist="TERMINUS",
            command="pytest",
            exit_code=0,
            success=True,
            duration_ms=100.0,
        )
        blackboard.publish(
            slot_name="execution_results",
            content=result.to_entry_content(),
            entry_type=EntryType.FACT,
            provenance=provenance,
        )
        results = terminus.read_execution_results(blackboard)
        assert len(results) == 1
        assert results[0].command == "pytest"
        assert results[0].success is True

    def test_read_with_none_blackboard(self, terminus: TerminusSpecialist):
        results = terminus.read_execution_results(None)
        assert results == []

    def test_read_max_results(self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        for i in range(5):
            r = ExecutionResultEntry(command=f"cmd_{i}", exit_code=i)
            blackboard.publish(
                slot_name="execution_results",
                content=r.to_entry_content(),
                entry_type=EntryType.FACT,
                provenance=provenance,
            )
        results = terminus.read_execution_results(blackboard, max_results=3)
        assert len(results) == 3


# ===========================================================================
# Integration — Full Execution Cycle with Gate
# ===========================================================================


class TestExecutionCycle:
    """End-to-end: Architect decides → TERMINUS checks gate → TERMINUS reports."""

    def test_full_approved_execution_cycle(
        self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance,
    ):
        """Architect APPROVES → TERMINUS gate passes → TERMINUS publishes result."""
        # Architect publishes decision
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan",
            target_id="plan_e2e",
            reason="All checks passed",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )

        # TERMINUS checks the gate — should pass
        gate = terminus.check_architect_decision(blackboard, target_id="plan_e2e")
        assert gate["approved"] is True

        # TERMINUS executes and publishes result
        terminus.publish_execution_result(
            blackboard=blackboard,
            command="deploy.sh",
            exit_code=0,
            stdout="Deployment successful",
            success=True,
            duration_ms=3000.0,
            task_id="task_e2e",
        )

        # Verify result is on the blackboard
        results = terminus.read_execution_results(blackboard)
        assert len(results) == 1
        assert results[0].success is True

    def test_blocked_execution_cycle(
        self, terminus: TerminusSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance,
    ):
        """Architect REJECTS → TERMINUS gate blocks → TERMINUS reports failure."""
        # Architect rejects
        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.REJECT,
            target_type="plan",
            target_id="plan_blocked",
            reason="Security review failed",
        )
        blackboard.publish(
            slot_name="architect_decisions",
            content=decision.model_dump_json(exclude_none=True),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )

        # TERMINUS checks the gate — should raise
        with pytest.raises(ExecutionBlockedError) as exc_info:
            terminus.check_architect_decision(
                blackboard, target_id="plan_blocked", command="rm -rf /tmp/data",
            )
        assert "reject" in str(exc_info.value).lower()

        # TERMINUS reports the failure (even though it didn't execute)
        terminus.publish_failure_report(
            blackboard=blackboard,
            command="rm -rf /tmp/data",
            error_message="Blocked by architect: security review failed",
            exit_code=-1,
            recovery_suggestion="Request re-review after fixing security issues",
            task_id="task_blocked",
        )

        failures = blackboard.read("execution_failures")
        assert len(failures) == 1


# ===========================================================================
# No Direct Messaging
# ===========================================================================


class TestNoDirectMessaging:
    """Verify that the refactored TERMINUS uses NO agent-to-agent messaging."""

    def test_no_message_imports(self):
        """TerminusSpecialist should not import any messaging module."""
        import specialists.terminus as terminus_mod
        import inspect
        source = inspect.getsource(terminus_mod)
        forbidden_imports = [
            "from agent_communication",
            "import AgentCommunicationRouter",
            "from specialists.coordination",
        ]
        for pattern in forbidden_imports:
            assert pattern not in source, (
                f"TERMINUS contains forbidden direct-messaging import: {pattern}"
            )
        import re
        message_calls = re.findall(r"^[^#]*\bMessage\(", source, re.MULTILINE)
        assert len(message_calls) == 0, (
            f"TERMINUS contains {len(message_calls)} direct Message() calls"
        )

    def test_no_router_in_methods(self, terminus: TerminusSpecialist):
        """None of the collaboration methods should take a router parameter."""
        import inspect
        methods = [
            terminus.pickup_task,
            terminus.check_architect_decision,
            terminus.publish_execution_result,
            terminus.publish_failure_report,
            terminus.read_execution_results,
        ]
        for method in methods:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            forbidden_params = {"router", "communication_router", "messenger"}
            for param_name in params:
                assert param_name not in forbidden_params, (
                    f"{method.__name__} has forbidden param: {param_name}"
                )

    def test_all_methods_use_blackboard_or_taskboard(self, terminus: TerminusSpecialist):
        """All collaboration methods use blackboard or task_board, not messaging."""
        import inspect
        methods = {
            "pickup_task": "task_board",
            "check_architect_decision": "blackboard",
            "publish_execution_result": "blackboard",
            "publish_failure_report": "blackboard",
            "read_execution_results": "blackboard",
        }
        for method_name, expected_param in methods.items():
            method = getattr(terminus, method_name)
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            assert expected_param in params, (
                f"{method_name} missing expected param: {expected_param}"
            )
