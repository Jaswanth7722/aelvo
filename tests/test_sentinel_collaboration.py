"""Tests for Phase 6 — SENTINEL blackboard-based collaboration.

Per Amendment 2: No agent-to-agent messaging.
All communication flows through the Shared Blackboard using typed schemas.

Covers:
- ``SentinelSpecialist.pickup_task()`` — pick up SECURITY_REVIEW tasks from SharedTaskBoard
- ``SentinelSpecialist.read_implementations()`` — read ImplementationEntry from blackboard
- ``SentinelSpecialist.approve_implementation()`` — publish ApprovalEntry to blackboard
- ``SentinelSpecialist.reject_implementation()`` — publish RejectionEntry to blackboard
- ``SentinelSpecialist.escalate_to_architect()`` — publish EscalationEntry to blackboard
- ``SentinelSpecialist.check_for_escalations()`` — read EscalationEntry from blackboard
- No direct messaging — no Message, no send_message, no AgentCommunicationRouter
"""

import pytest
from typing import Any, Dict, List

from specialists.sentinel import SentinelSpecialist
from cognition.blackboard import CognitiveBlackboard
from cognition.blackboard_schemas import (
    ImplementationEntry,
    ApprovalEntry,
    RejectionEntry,
    EscalationEntry,
)
from cognition.types import (
    EntryType, Provenance, ProvenanceType,
)


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def sentinel() -> SentinelSpecialist:
    return SentinelSpecialist()


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
    """A lightweight task board mock for testing SENTINEL task pickup.

    Mimics the SharedTaskBoard API surface that SentinelSpecialist uses.
    """

    def __init__(self):
        self._tasks: Dict[str, dict] = {}
        self._transitions: List[str] = []

    def add_task(self, task_id: str, **kwargs):
        defaults = {
            "type": "security_review",
            "status": "pending",
            "specialist": "",
            "title": "Test review",
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
                type=TaskType(data.get("type", "security_review")),
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
# Task Pickup
# ===========================================================================


class TestPickupTask:
    def test_pickup_no_tasks(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        picked = sentinel.pickup_task(board)
        assert picked == []

    def test_pickup_pending_security_review(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        board.add_task("t1", type="security_review", status="pending", title="Review auth module")
        picked = sentinel.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t1"
        assert "assigned:t1" in board.transition_log
        assert "started:t1" in board.transition_log

    def test_pickup_already_assigned_to_sentinel(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        board.add_task("t2", type="security_review", status="assigned", specialist="SENTINEL")
        picked = sentinel.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t2"

    def test_pickup_ignores_other_specialist_tasks(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        board.add_task("t3", type="security_review", status="pending", specialist="FORGE")
        picked = sentinel.pickup_task(board)
        assert picked == []

    def test_pickup_non_security_task(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        board.add_task("t4", type="implement", status="pending", title="Write code")
        picked = sentinel.pickup_task(board)
        assert picked == []

    def test_pickup_max_tasks(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        for i in range(5):
            board.add_task(f"t{i}", type="security_review", status="pending", title=f"Review {i}")
        picked = sentinel.pickup_task(board, max_tasks=2)
        assert len(picked) == 2

    def test_pickup_with_none_board(self, sentinel: SentinelSpecialist):
        picked = sentinel.pickup_task(None)
        assert picked == []

    def test_pickup_multiple_tasks(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        board.add_task("t5", type="security_review", status="pending", title="Review A")
        board.add_task("t6", type="security_review", status="pending", title="Review B")
        picked = sentinel.pickup_task(board, max_tasks=10)
        assert len(picked) == 2

    def test_pickup_only_sentinel_tasks(self, sentinel: SentinelSpecialist):
        board = FakeTaskBoard()
        board.add_task("t7", type="security_review", status="pending", title="Sentinel task")
        board.add_task("t8", type="implement", status="pending", title="Forge task")
        board.add_task("t9", type="research", status="pending", title="Oracle task")
        picked = sentinel.pickup_task(board, max_tasks=10)
        assert len(picked) == 1
        assert picked[0].id == "t7"


# ===========================================================================
# Read Implementations
# ===========================================================================


class TestReadImplementations:
    def test_read_implementations_empty(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        impls = sentinel.read_implementations(blackboard)
        assert impls == []

    def test_read_implementations(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        impl = ImplementationEntry(
            summary="Added JWT auth",
            files_changed=["auth.py"],
            changes_description="Added JWT-based authentication",
        )
        blackboard.publish(
            slot_name="implementations",
            content=impl.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        impls = sentinel.read_implementations(blackboard)
        assert len(impls) == 1
        assert impls[0].summary == "Added JWT auth"
        assert "auth.py" in impls[0].files_changed

    def test_read_implementations_multiple(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        for i in range(3):
            impl = ImplementationEntry(summary=f"Impl {i}")
            blackboard.publish(
                slot_name="implementations",
                content=impl.to_entry_content(),
                entry_type=EntryType.FINDING,
                provenance=provenance,
            )
        impls = sentinel.read_implementations(blackboard, max_results=2)
        assert len(impls) == 2

    def test_read_implementations_with_none_blackboard(self, sentinel: SentinelSpecialist):
        impls = sentinel.read_implementations(None)
        assert impls == []

    def test_read_implementation_includes_security_flag(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        impl = ImplementationEntry(
            summary="Test",
            security_review_requested=True,
        )
        blackboard.publish(
            slot_name="implementations",
            content=impl.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        impls = sentinel.read_implementations(blackboard)
        assert len(impls) == 1
        assert impls[0].security_review_requested is True


# ===========================================================================
# Approve Implementation
# ===========================================================================


class TestApproveImplementation:
    def test_approve(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.approve_implementation(
            blackboard=blackboard,
            summary="No security issues found",
            entry_id="impl_1",
            conditions=[],
            confidence=0.95,
        )
        assert entry_id != ""

        # Read the approval back from blackboard
        entries = blackboard.read("reviews")
        assert len(entries) == 1
        restored = ApprovalEntry.from_entry_content(entries[0].content)
        assert restored.approved_by == "SENTINEL"
        assert restored.entry_id == "impl_1"
        assert restored.confidence == 0.95

    def test_approve_with_none_blackboard(self, sentinel: SentinelSpecialist):
        entry_id = sentinel.approve_implementation(None, summary="test")
        assert entry_id == ""

    def test_approve_with_conditions(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.approve_implementation(
            blackboard=blackboard,
            summary="Approved with conditions",
            entry_id="impl_2",
            conditions=["Add input validation", "Use parameterized queries"],
            confidence=0.8,
        )
        assert entry_id != ""

        entries = blackboard.read("reviews")
        assert len(entries) == 1
        restored = ApprovalEntry.from_entry_content(entries[0].content)
        assert len(restored.conditions) == 2
        assert "input validation" in restored.conditions[0]

    def test_approve_minimal(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.approve_implementation(
            blackboard=blackboard,
            summary="Approved",
        )
        assert entry_id != ""

    def test_approve_multiple(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        sentinel.approve_implementation(blackboard, summary="First", entry_id="e1")
        sentinel.approve_implementation(blackboard, summary="Second", entry_id="e2")
        entries = blackboard.read("reviews")
        assert len(entries) == 2


# ===========================================================================
# Reject Implementation
# ===========================================================================


class TestRejectImplementation:
    def test_reject(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.reject_implementation(
            blackboard=blackboard,
            entry_id="impl_1",
            reason="Hardcoded API key found in config.py",
            findings=["line 42: API_KEY = 'sk-xxx'"],
            remediations=["Use environment variable instead"],
            severity="high",
        )
        assert entry_id != ""

        # Read the rejection back from blackboard
        entries = blackboard.read("reviews")
        assert len(entries) == 1
        restored = RejectionEntry.from_entry_content(entries[0].content)
        assert restored.rejected_by == "SENTINEL"
        assert restored.entry_id == "impl_1"
        assert restored.severity == "high"
        assert len(restored.findings) == 1
        assert len(restored.remediations) == 1

    def test_reject_with_none_blackboard(self, sentinel: SentinelSpecialist):
        entry_id = sentinel.reject_implementation(None, reason="test")
        assert entry_id == ""

    def test_reject_minimal(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.reject_implementation(
            blackboard=blackboard,
            entry_id="impl_2",
            reason="Security vulnerability",
        )
        assert entry_id != ""

    def test_reject_multiple(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        sentinel.reject_implementation(blackboard, entry_id="e1", reason="Issue A")
        sentinel.reject_implementation(blackboard, entry_id="e2", reason="Issue B")
        entries = blackboard.read("reviews")
        assert len(entries) == 2


# ===========================================================================
# Escalate to Architect
# ===========================================================================


class TestEscalate:
    def test_escalate(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.escalate_to_architect(
            blackboard=blackboard,
            reason="Architectural security issue: no authentication layer",
            context={"affected_modules": ["api/", "core/"]},
            suggested_action="Add authentication middleware at the gateway level",
            urgency="high",
        )
        assert entry_id != ""

        # Read the escalation back from blackboard
        entries = blackboard.read("security_escalations")
        assert len(entries) == 1
        restored = EscalationEntry.from_entry_content(entries[0].content)
        assert restored.escalated_by == "SENTINEL"
        assert "authentication" in restored.reason
        assert restored.urgency == "high"
        assert "affected_modules" in restored.context

    def test_escalate_with_none_blackboard(self, sentinel: SentinelSpecialist):
        entry_id = sentinel.escalate_to_architect(None, reason="test")
        assert entry_id == ""

    def test_escalate_minimal(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.escalate_to_architect(
            blackboard=blackboard,
            reason="Security concern requires architectural review",
        )
        assert entry_id != ""

    def test_escalate_critical_urgency(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        entry_id = sentinel.escalate_to_architect(
            blackboard=blackboard,
            reason="Critical SQL injection vector in ORM design",
            urgency="critical",
        )
        assert entry_id != ""

        entries = blackboard.read("security_escalations")
        restored = EscalationEntry.from_entry_content(entries[0].content)
        assert restored.urgency == "critical"

    def test_escalate_multiple(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        sentinel.escalate_to_architect(blackboard, reason="Issue 1")
        sentinel.escalate_to_architect(blackboard, reason="Issue 2")
        entries = blackboard.read("security_escalations")
        assert len(entries) == 2


# ===========================================================================
# Check for Escalations
# ===========================================================================


class TestCheckForEscalations:
    def test_check_escalations_empty(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard):
        escalations = sentinel.check_for_escalations(blackboard)
        assert escalations == []

    def test_check_escalations(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        esc = EscalationEntry(
            escalated_by="SENTINEL",
            reason="Test escalation",
            urgency="medium",
        )
        blackboard.publish(
            slot_name="security_escalations",
            content=esc.to_entry_content(),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        escalations = sentinel.check_for_escalations(blackboard)
        assert len(escalations) == 1
        assert escalations[0].reason == "Test escalation"

    def test_check_escalations_with_none_blackboard(self, sentinel: SentinelSpecialist):
        escalations = sentinel.check_for_escalations(None)
        assert escalations == []

    def test_check_escalations_max_results(self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        for i in range(5):
            esc = EscalationEntry(escalated_by="SENTINEL", reason=f"Esc {i}")
            blackboard.publish(
                slot_name="security_escalations",
                content=esc.to_entry_content(),
                entry_type=EntryType.DECISION,
                provenance=provenance,
            )
        escalations = sentinel.check_for_escalations(blackboard, max_results=3)
        assert len(escalations) == 3


# ===========================================================================
# Integration — Full Review Cycle
# ===========================================================================


class TestReviewCycle:
    """End-to-end: FORGE submits → SENTINEL reviews (approve/reject)."""

    def test_full_approval_cycle(
        self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance,
    ):
        """FORGE submits implementation → SENTINEL reads → SENTINEL approves."""
        # FORGE publishes implementation
        impl = ImplementationEntry(
            summary="Added login middleware",
            files_changed=["middleware.py"],
            security_review_requested=True,
        )
        impl_entry = blackboard.publish(
            slot_name="implementations",
            content=impl.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )

        # SENTINEL reads implementations
        impls = sentinel.read_implementations(blackboard)
        assert len(impls) == 1
        assert impls[0].summary == "Added login middleware"

        # SENTINEL approves
        approval_id = sentinel.approve_implementation(
            blackboard=blackboard,
            summary="No security issues",
            entry_id=impl_entry.id,
            confidence=0.95,
        )
        assert approval_id != ""

        # FORGE reads the review result
        reviews = blackboard.read("reviews")
        assert len(reviews) == 1
        restored = ApprovalEntry.from_entry_content(reviews[0].content)
        assert restored.approved_by == "SENTINEL"

    def test_full_rejection_cycle(
        self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance,
    ):
        """FORGE submits → SENTINEL reads → SENTINEL rejects."""
        # FORGE publishes implementation
        impl = ImplementationEntry(summary="Added login", files_changed=["auth.py"])
        impl_entry = blackboard.publish(
            slot_name="implementations",
            content=impl.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )

        # SENTINEL rejects
        rejection_id = sentinel.reject_implementation(
            blackboard=blackboard,
            entry_id=impl_entry.id,
            reason="Hardcoded credentials detected",
            findings=["line 15: password = 'admin123'"],
            remediations=["Use secret manager or environment variable"],
            severity="critical",
        )
        assert rejection_id != ""

        # FORGE reads the review result
        reviews = blackboard.read("reviews")
        assert len(reviews) == 1
        restored = RejectionEntry.from_entry_content(reviews[0].content)
        assert restored.severity == "critical"
        assert len(restored.remediations) == 1

    def test_escalation_then_check(
        self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard,
    ):
        """SENTINEL escalates to Architect, then checks for any response."""
        # SENTINEL escalates
        sentinel.escalate_to_architect(
            blackboard=blackboard,
            reason="Architecture lacks input validation layer",
            context={"severity": "all endpoints"},
            suggested_action="Add middleware validation pipeline",
            urgency="high",
        )

        # SENTINEL checks for escalations (self-monitoring)
        escalations = sentinel.check_for_escalations(blackboard)
        assert len(escalations) == 1
        assert "input validation" in escalations[0].reason
        assert escalations[0].urgency == "high"


# ===========================================================================
# No Direct Messaging
# ===========================================================================


class TestNoDirectMessaging:
    """Verify that the refactored SENTINEL uses NO agent-to-agent messaging.

    The SentinelSpecialist should only communicate through:
    - SharedTaskBoard (task lifecycle)
    - CognitiveBlackboard (typed entries)
    - No Message objects, no send_message, no AgentCommunicationRouter
    """

    def test_no_message_imports(self):
        """SentinelSpecialist should not import any messaging module."""
        import specialists.sentinel as sentinel_mod
        import inspect
        source = inspect.getsource(sentinel_mod)
        # These import/call patterns indicate direct messaging
        forbidden_imports = [
            "from agent_communication",
            "import AgentCommunicationRouter",
            "from specialists.coordination",
        ]
        for pattern in forbidden_imports:
            assert pattern not in source, (
                f"SENTINEL contains forbidden direct-messaging import: {pattern}"
            )
        # Check that Message is not called as a constructor
        import re
        message_calls = re.findall(r"^[^#]*\bMessage\(", source, re.MULTILINE)
        assert len(message_calls) == 0, (
            f"SENTINEL contains {len(message_calls)} direct Message() calls"
        )

    def test_no_router_in_methods(self, sentinel: SentinelSpecialist):
        """None of the collaboration methods should take a router parameter."""
        import inspect
        methods = [
            sentinel.pickup_task,
            sentinel.read_implementations,
            sentinel.approve_implementation,
            sentinel.reject_implementation,
            sentinel.escalate_to_architect,
            sentinel.check_for_escalations,
        ]
        for method in methods:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            forbidden_params = {"router", "communication_router", "messenger"}
            for param_name in params:
                assert param_name not in forbidden_params, (
                    f"{method.__name__} has forbidden param: {param_name}"
                )

    def test_all_methods_use_blackboard_or_taskboard(self, sentinel: SentinelSpecialist):
        """All collaboration methods use blackboard or task_board, not messaging."""
        import inspect
        methods = {
            "pickup_task": "task_board",
            "read_implementations": "blackboard",
            "approve_implementation": "blackboard",
            "reject_implementation": "blackboard",
            "escalate_to_architect": "blackboard",
            "check_for_escalations": "blackboard",
        }
        for method_name, expected_param in methods.items():
            method = getattr(sentinel, method_name)
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            assert expected_param in params, (
                f"{method_name} missing expected param: {expected_param}"
            )

    def test_integration_publish_and_read_approvals(
        self, sentinel: SentinelSpecialist, blackboard: CognitiveBlackboard,
    ):
        """End-to-end: publish approval, then read it via blackboard."""
        eid = sentinel.approve_implementation(
            blackboard, summary="E2E test", entry_id="e2e_1",
        )
        assert eid != ""
        entries = blackboard.read("reviews")
        assert len(entries) == 1
        approval = ApprovalEntry.from_entry_content(entries[0].content)
        assert approval.approved_by == "SENTINEL"
        assert approval.entry_id == "e2e_1"
