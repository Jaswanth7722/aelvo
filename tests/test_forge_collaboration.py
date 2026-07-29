"""Tests for Phase 6 — FORGE blackboard-based collaboration.

Per Amendment 2: No agent-to-agent messaging.
All communication flows through the Shared Blackboard using typed schemas.

Covers:
- ``ForgeSpecialist.pickup_task()`` — pick up IMPLEMENT tasks from SharedTaskBoard
- ``ForgeSpecialist.request_research()`` — publish QuestionEntry to blackboard
- ``ForgeSpecialist.submit_for_review()`` — publish ImplementationEntry to blackboard
- ``ForgeSpecialist.check_for_revisions()`` — read RejectionEntry/ApprovalEntry
- ``ForgeSpecialist.read_findings()`` — read FindingEntry from blackboard
- No direct messaging — no Message, no send_message, no AgentCommunicationRouter
"""

import pytest
from typing import Any, Dict, List

from specialists.forge import ForgeSpecialist
from cognition.blackboard import CognitiveBlackboard
from cognition.blackboard_schemas import (
    FindingEntry,
    QuestionEntry,
    ImplementationEntry,
    RejectionEntry,
    ApprovalEntry,
)
from cognition.types import (
    EntryType, Provenance, ProvenanceType,
)


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def forge() -> ForgeSpecialist:
    return ForgeSpecialist()


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
    """A lightweight task board mock for testing FORGE task pickup."""

    def __init__(self):
        self._tasks: Dict[str, dict] = {}
        self._transitions: List[str] = []

    def add_task(self, task_id: str, **kwargs):
        defaults = {
            "type": "implement",
            "status": "pending",
            "specialist": "",
            "title": "Test task",
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
                type=TaskType(data.get("type", "implement")),
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
    def test_pickup_no_tasks(self, forge: ForgeSpecialist):
        board = FakeTaskBoard()
        picked = forge.pickup_task(board)
        assert picked == []

    def test_pickup_pending_implement_task(self, forge: ForgeSpecialist):
        board = FakeTaskBoard()
        board.add_task("t1", type="implement", status="pending", title="Implement login")
        picked = forge.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t1"
        assert "assigned:t1" in board.transition_log
        assert "started:t1" in board.transition_log

    def test_pickup_already_assigned_to_forge(self, forge: ForgeSpecialist):
        board = FakeTaskBoard()
        board.add_task("t2", type="implement", status="assigned", specialist="FORGE")
        picked = forge.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t2"

    def test_pickup_ignores_other_specialist_tasks(self, forge: ForgeSpecialist):
        board = FakeTaskBoard()
        board.add_task("t3", type="implement", status="pending", specialist="ORACLE")
        picked = forge.pickup_task(board)
        assert picked == []

    def test_pickup_non_implement_task(self, forge: ForgeSpecialist):
        board = FakeTaskBoard()
        board.add_task("t4", type="research", status="pending", title="Research topic")
        picked = forge.pickup_task(board)
        assert picked == []

    def test_pickup_max_tasks(self, forge: ForgeSpecialist):
        board = FakeTaskBoard()
        for i in range(5):
            board.add_task(f"t{i}", type="implement", status="pending", title=f"Task {i}")
        picked = forge.pickup_task(board, max_tasks=2)
        assert len(picked) == 2

    def test_pickup_with_none_board(self, forge: ForgeSpecialist):
        picked = forge.pickup_task(None)
        assert picked == []

    def test_pickup_only_forge_tasks(self, forge: ForgeSpecialist):
        board = FakeTaskBoard()
        board.add_task("t7", type="implement", status="pending", title="Forge task")
        board.add_task("t8", type="research", status="pending", title="Oracle task")
        board.add_task("t9", type="security_review", status="pending", title="Sentinel task")
        picked = forge.pickup_task(board, max_tasks=10)
        assert len(picked) == 1
        assert picked[0].id == "t7"


# ===========================================================================
# Research Request
# ===========================================================================


class TestRequestResearch:
    def test_request_research(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        entry_id = forge.request_research(
            blackboard=blackboard,
            question="What is the best library for async HTTP in Python?",
            task_id="task_123",
            context={"blocking": True},
        )
        assert entry_id != ""

        # Read the question back from the blackboard
        entries = blackboard.read("questions", entry_type=EntryType.QUERY)
        assert len(entries) == 1
        restored = QuestionEntry.from_entry_content(entries[0].content)
        assert "async HTTP" in restored.question
        assert restored.asked_by == "FORGE"

    def test_request_research_with_none_blackboard(self, forge: ForgeSpecialist):
        entry_id = forge.request_research(None, question="test")
        assert entry_id == ""

    def test_request_research_minimal(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        entry_id = forge.request_research(blackboard, question="Quick question?")
        assert entry_id != ""

    def test_request_research_multiple(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        forge.request_research(blackboard, question="Q1?")
        forge.request_research(blackboard, question="Q2?")
        entries = blackboard.read("questions", entry_type=EntryType.QUERY)
        assert len(entries) == 2


# ===========================================================================
# Submit for Review
# ===========================================================================


class TestSubmitForReview:
    def test_submit_for_review(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        entry_id = forge.submit_for_review(
            blackboard=blackboard,
            summary="Implemented login flow",
            files_changed=["auth.py", "login.py"],
            files_created=["login.py"],
            changes_description="Added JWT-based login",
            test_summary="All tests pass",
        )
        assert entry_id != ""

        entries = blackboard.read("implementations")
        assert len(entries) == 1
        restored = ImplementationEntry.from_entry_content(entries[0].content)
        assert restored.summary == "Implemented login flow"
        assert "auth.py" in restored.files_changed
        assert restored.security_review_requested is True

    def test_submit_with_none_blackboard(self, forge: ForgeSpecialist):
        entry_id = forge.submit_for_review(None, summary="test")
        assert entry_id == ""

    def test_submit_minimal(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        entry_id = forge.submit_for_review(
            blackboard=blackboard,
            summary="Quick fix",
        )
        assert entry_id != ""

    def test_submit_no_security_review(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        entry_id = forge.submit_for_review(
            blackboard=blackboard,
            summary="Minor refactor",
            security_review_requested=False,
        )
        assert entry_id != ""
        entries = blackboard.read("implementations")
        assert len(entries) == 1


# ===========================================================================
# Check for Revisions
# ===========================================================================


class TestCheckForRevisions:
    def test_no_revisions(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        results = forge.check_for_revisions(blackboard)
        assert results == []

    def test_read_approval(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        approval = ApprovalEntry(
            approved_by="SENTINEL",
            entry_id="impl_1",
            reason="No security issues",
        )
        blackboard.publish(
            slot_name="reviews",
            content=approval.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        results = forge.check_for_revisions(blackboard)
        assert len(results) == 1
        assert results[0]["type"] == "approval"
        assert results[0]["data"].approved_by == "SENTINEL"

    def test_read_rejection(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        rejection = RejectionEntry(
            rejected_by="SENTINEL",
            entry_id="impl_2",
            reason="Hardcoded secret found",
            findings=["line 42: API_KEY hardcoded"],
            remediations=["Use environment variable"],
            severity="high",
        )
        blackboard.publish(
            slot_name="reviews",
            content=rejection.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        results = forge.check_for_revisions(blackboard)
        assert len(results) == 1
        assert results[0]["type"] == "rejection"
        assert results[0]["data"].severity == "high"
        assert len(results[0]["data"].remediations) == 1

    def test_read_mixed_reviews(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        rejection = RejectionEntry(rejected_by="SENTINEL", entry_id="e1", reason="Issue")
        approval = ApprovalEntry(approved_by="SENTINEL", entry_id="e2", reason="OK")
        blackboard.publish("reviews", rejection.to_entry_content(), EntryType.FINDING, provenance)
        blackboard.publish("reviews", approval.to_entry_content(), EntryType.FINDING, provenance)
        results = forge.check_for_revisions(blackboard)
        types = [r["type"] for r in results]
        assert "rejection" in types
        assert "approval" in types


# ===========================================================================
# Read Findings
# ===========================================================================


class TestReadFindings:
    def test_read_findings_empty(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard):
        findings = forge.read_findings(blackboard)
        assert findings == []

    def test_read_findings(self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        finding = FindingEntry(summary="httpx is best for async HTTP", detail="Benchmarks show...")
        blackboard.publish(
            slot_name="research_findings",
            content=finding.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        findings = forge.read_findings(blackboard)
        assert len(findings) == 1
        assert "httpx" in findings[0].summary

    def test_read_findings_with_none_blackboard(self, forge: ForgeSpecialist):
        findings = forge.read_findings(None)
        assert findings == []


# ===========================================================================
# No Direct Messaging
# ===========================================================================


class TestNoDirectMessaging:
    """Verify that the refactored FORGE uses NO agent-to-agent messaging."""

    def test_no_message_imports(self):
        """ForgeSpecialist should not import any messaging module."""
        import specialists.forge as forge_mod
        import inspect
        source = inspect.getsource(forge_mod)
        forbidden_imports = [
            "from agent_communication",
            "import AgentCommunicationRouter",
        ]
        for pattern in forbidden_imports:
            assert pattern not in source, (
                f"FORGE contains forbidden direct-messaging import: {pattern}"
            )
        import re
        message_calls = re.findall(r"^[^#]*\bMessage\(", source, re.MULTILINE)
        assert len(message_calls) == 0, (
            f"FORGE contains {len(message_calls)} direct Message() calls"
        )

    def test_all_methods_use_blackboard_or_taskboard(self, forge: ForgeSpecialist):
        """All collaboration methods use blackboard or task_board, not messaging."""
        import inspect
        methods = {
            "pickup_task": "task_board",
            "request_research": "blackboard",
            "submit_for_review": "blackboard",
            "check_for_revisions": "blackboard",
            "read_findings": "blackboard",
        }
        for method_name, expected_param in methods.items():
            method = getattr(forge, method_name)
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            assert expected_param in params, (
                f"{method_name} missing expected param: {expected_param}"
            )

    def test_integration_publish_and_read(
        self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard,
    ):
        """End-to-end: submit implementation, then read it back."""
        eid = forge.submit_for_review(
            blackboard, summary="E2E test", files_changed=["test.py"],
        )
        assert eid != ""
        entries = blackboard.read("implementations")
        assert len(entries) == 1

    def test_integration_research_cycle(
        self, forge: ForgeSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance,
    ):
        """End-to-end: FORGE requests research, ORACLE publishes finding."""
        # FORGE publishes a question
        forge.request_research(blackboard, question="Best Python async lib?")
        # ORACLE publishes a finding (simulated)
        finding = FindingEntry(summary="Use httpx", detail="httpx supports async/await")
        blackboard.publish(
            "research_findings", finding.to_entry_content(),
            EntryType.FINDING, provenance,
        )
        # FORGE reads the finding
        findings = forge.read_findings(blackboard)
        assert len(findings) == 1
        assert "httpx" in findings[0].summary
