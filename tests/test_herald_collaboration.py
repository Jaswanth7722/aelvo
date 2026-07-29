"""Tests for Phase 6 — HERALD blackboard-based collaboration.

Per Amendment 2: No agent-to-agent messaging.
All communication flows through the Shared Blackboard using typed schemas.

Covers:
- ``HeraldSpecialist.pickup_task()`` — pick up REPORT tasks from SharedTaskBoard
- ``HeraldSpecialist.generate_collaboration_summary()`` — read blackboard, produce narrative
- ``HeraldSpecialist.submit_summary_for_review()`` — publish summary for Architect review
- ``HeraldSpecialist.check_for_summary_review()`` — read Architect's feedback
- ``HeraldSpecialist.generate_session_report()`` — publish user-facing report
- No direct messaging — no Message, no send_message, no AgentCommunicationRouter
"""

import pytest
from typing import Any, Dict, List

from specialists.herald import HeraldSpecialist
from cognition.blackboard import CognitiveBlackboard
from cognition.blackboard_schemas import (
    FindingEntry,
    ImplementationEntry,
    ApprovalEntry,
    RejectionEntry,
    EscalationEntry,
    ExecutionResultEntry,
)
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
def herald() -> HeraldSpecialist:
    return HeraldSpecialist()


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
    """A lightweight task board mock for testing HERALD task pickup."""

    def __init__(self):
        self._tasks: Dict[str, dict] = {}
        self._transitions: List[str] = []

    def add_task(self, task_id: str, **kwargs):
        defaults = {
            "type": "report",
            "status": "pending",
            "specialist": "",
            "title": "Test report",
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
                type=TaskType(data.get("type", "report")),
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
    def test_pickup_no_tasks(self, herald: HeraldSpecialist):
        board = FakeTaskBoard()
        picked = herald.pickup_task(board)
        assert picked == []

    def test_pickup_pending_report_task(self, herald: HeraldSpecialist):
        board = FakeTaskBoard()
        board.add_task("t1", type="report", status="pending", title="Generate summary")
        picked = herald.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t1"
        assert "assigned:t1" in board.transition_log
        assert "started:t1" in board.transition_log

    def test_pickup_already_assigned_to_herald(self, herald: HeraldSpecialist):
        board = FakeTaskBoard()
        board.add_task("t2", type="report", status="assigned", specialist="HERALD")
        picked = herald.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t2"

    def test_pickup_ignores_other_specialist_tasks(self, herald: HeraldSpecialist):
        board = FakeTaskBoard()
        board.add_task("t3", type="report", status="pending", specialist="FORGE")
        picked = herald.pickup_task(board)
        assert picked == []

    def test_pickup_non_report_task(self, herald: HeraldSpecialist):
        board = FakeTaskBoard()
        board.add_task("t4", type="implement", status="pending", title="Write code")
        picked = herald.pickup_task(board)
        assert picked == []

    def test_pickup_max_tasks(self, herald: HeraldSpecialist):
        board = FakeTaskBoard()
        for i in range(5):
            board.add_task(f"t{i}", type="report", status="pending", title=f"Report {i}")
        picked = herald.pickup_task(board, max_tasks=3)
        assert len(picked) == 3

    def test_pickup_with_none_board(self, herald: HeraldSpecialist):
        picked = herald.pickup_task(None)
        assert picked == []

    def test_pickup_only_herald_tasks(self, herald: HeraldSpecialist):
        board = FakeTaskBoard()
        board.add_task("t7", type="report", status="pending", title="Herald task")
        board.add_task("t8", type="implement", status="pending", title="Forge task")
        board.add_task("t9", type="research", status="pending", title="Oracle task")
        picked = herald.pickup_task(board, max_tasks=10)
        assert len(picked) == 1
        assert picked[0].id == "t7"


# ===========================================================================
# Generate Collaboration Summary
# ===========================================================================


class TestGenerateCollaborationSummary:
    def test_empty_blackboard(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        summary = herald.generate_collaboration_summary(blackboard)
        assert "Session Summary" in summary["overview"]
        assert summary["metadata"]["finding_count"] == 0
        assert summary["metadata"]["implementation_count"] == 0
        assert summary["metadata"]["approval_count"] == 0
        assert summary["metadata"]["rejection_count"] == 0

    def test_with_none_blackboard(self, herald: HeraldSpecialist):
        summary = herald.generate_collaboration_summary(None)
        assert "No blackboard data" in summary["full_narrative"]
        assert "error" in summary["metadata"]

    def test_with_findings(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        finding = FindingEntry(summary="httpx is best for async HTTP", detail="Benchmarks")
        blackboard.publish(
            slot_name="research_findings",
            content=finding.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        summary = herald.generate_collaboration_summary(blackboard)
        assert summary["metadata"]["finding_count"] == 1
        assert "httpx" in summary["findings"]

    def test_with_implementations(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        impl = ImplementationEntry(summary="Added JWT auth", files_changed=["auth.py"])
        blackboard.publish(
            slot_name="implementations",
            content=impl.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        summary = herald.generate_collaboration_summary(blackboard)
        assert summary["metadata"]["implementation_count"] == 1
        assert "JWT" in summary["implementations"]

    def test_with_approvals_and_rejections(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        approval = ApprovalEntry(approved_by="SENTINEL", entry_id="e1", reason="OK")
        rejection = RejectionEntry(rejected_by="SENTINEL", entry_id="e2", reason="Issue")
        blackboard.publish("reviews", approval.to_entry_content(), EntryType.FINDING, provenance)
        blackboard.publish("reviews", rejection.to_entry_content(), EntryType.FINDING, provenance)
        summary = herald.generate_collaboration_summary(blackboard)
        assert summary["metadata"]["approval_count"] == 1
        assert summary["metadata"]["rejection_count"] == 1

    def test_with_escalations(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        esc = EscalationEntry(escalated_by="SENTINEL", reason="Architecture issue", urgency="high")
        blackboard.publish(
            slot_name="security_escalations",
            content=esc.to_entry_content(),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        summary = herald.generate_collaboration_summary(blackboard)
        assert summary["metadata"]["escalation_count"] == 1
        assert "Architecture" in summary["escalations"]

    def test_with_execution_results(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        success = ExecutionResultEntry(command="pytest", exit_code=0, success=True)
        failure = ExecutionResultEntry(command="deploy", exit_code=1, success=False)
        blackboard.publish("execution_results", success.to_entry_content(), EntryType.FACT, provenance)
        blackboard.publish("execution_results", failure.to_entry_content(), EntryType.FACT, provenance)
        summary = herald.generate_collaboration_summary(blackboard)
        assert summary["metadata"]["execution_success_count"] == 1
        assert summary["metadata"]["execution_failure_count"] == 1

    def test_with_task_board(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        board = FakeTaskBoard()
        board.add_task("t1", type="report", status="completed", title="Done")
        board.add_task("t2", type="report", status="in_progress", title="Active")
        summary = herald.generate_collaboration_summary(blackboard, task_board=board)
        assert summary["metadata"]["task_stats"]["total"] == 2
        assert summary["metadata"]["task_stats"]["completed"] == 1

    def test_recommendations_on_issues(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        rejection = RejectionEntry(rejected_by="SENTINEL", entry_id="e1", reason="Bug")
        blackboard.publish("reviews", rejection.to_entry_content(), EntryType.FINDING, provenance)
        esc = EscalationEntry(escalated_by="SENTINEL", reason="Security", urgency="high")
        blackboard.publish("security_escalations", esc.to_entry_content(), EntryType.DECISION, provenance)
        summary = herald.generate_collaboration_summary(blackboard)
        assert "rejection" in summary["recommendations"].lower()
        assert "security" in summary["recommendations"].lower()


# ===========================================================================
# Submit Summary for Review
# ===========================================================================


class TestSubmitSummaryForReview:
    def test_submit_summary(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        summary = herald.generate_collaboration_summary(blackboard)
        entry_id = herald.submit_summary_for_review(blackboard, summary)
        assert entry_id != ""

        entries = blackboard.read("collaboration_summaries")
        assert len(entries) == 1

    def test_submit_with_none_blackboard(self, herald: HeraldSpecialist):
        entry_id = herald.submit_summary_for_review(None, {})
        assert entry_id == ""

    def test_submit_multiple_summaries(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        s1 = herald.generate_collaboration_summary(blackboard)
        s2 = herald.generate_collaboration_summary(blackboard)
        herald.submit_summary_for_review(blackboard, s1)
        herald.submit_summary_for_review(blackboard, s2)
        entries = blackboard.read("collaboration_summaries")
        assert len(entries) == 2

    def test_submit_with_task_id(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        summary = herald.generate_collaboration_summary(blackboard)
        entry_id = herald.submit_summary_for_review(blackboard, summary, task_id="task_42")
        assert entry_id != ""
        entries = blackboard.read("collaboration_summaries")
        assert len(entries) == 1


# ===========================================================================
# Check for Summary Review
# ===========================================================================


class TestCheckForSummaryReview:
    def test_no_reviews(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        results = herald.check_for_summary_review(blackboard)
        assert results == []

    def test_read_approval(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        approval = ApprovalEntry(approved_by="ARCHITECT", entry_id="s1", reason="Accurate")
        blackboard.publish(
            slot_name="collaboration_summaries",
            content=approval.to_entry_content(),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        results = herald.check_for_summary_review(blackboard)
        assert len(results) == 1
        assert results[0]["type"] == "approval"
        assert results[0]["data"].approved_by == "ARCHITECT"

    def test_read_rejection(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        rejection = RejectionEntry(
            rejected_by="ARCHITECT", entry_id="s2",
            reason="Missing implementation details",
        )
        blackboard.publish(
            slot_name="collaboration_summaries",
            content=rejection.to_entry_content(),
            entry_type=EntryType.DECISION,
            provenance=provenance,
        )
        results = herald.check_for_summary_review(blackboard)
        assert len(results) == 1
        assert results[0]["type"] == "rejection"
        assert "implementation" in results[0]["data"].reason.lower()

    def test_with_none_blackboard(self, herald: HeraldSpecialist):
        results = herald.check_for_summary_review(None)
        assert results == []


# ===========================================================================
# Generate Session Report
# ===========================================================================


class TestGenerateSessionReport:
    def test_generate_report(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        entry_id = herald.generate_session_report(blackboard)
        assert entry_id != ""

        entries = blackboard.read("user_reports")
        assert len(entries) == 1
        assert "HERALD" in entries[0].content

    def test_generate_report_with_none_blackboard(self, herald: HeraldSpecialist):
        entry_id = herald.generate_session_report(None)
        assert entry_id == ""

    def test_generate_report_with_summary(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        summary = herald.generate_collaboration_summary(blackboard)
        entry_id = herald.generate_session_report(blackboard, summary=summary)
        assert entry_id != ""

    def test_generate_report_with_custom_title(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        entry_id = herald.generate_session_report(
            blackboard, session_title="Deployment Review",
        )
        assert entry_id != ""
        entries = blackboard.read("user_reports")
        assert "Deployment Review" in entries[0].content

    def test_generate_report_multiple(self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard):
        herald.generate_session_report(blackboard)
        herald.generate_session_report(blackboard)
        entries = blackboard.read("user_reports")
        assert len(entries) == 2


# ===========================================================================
# Integration — Full Summary Cycle
# ===========================================================================


class TestSummaryCycle:
    """End-to-end: populate blackboard → generate summary → submit → check."""

    def test_full_summary_cycle(
        self, herald: HeraldSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance,
    ):
        """Populate blackboard → HERALD generates summary → submits for review."""
        # Populate blackboard with various entries
        finding = FindingEntry(summary="Python 3.13 perf improved", detail="Benchmarks show 15% faster")
        blackboard.publish("research_findings", finding.to_entry_content(), EntryType.FINDING, provenance)

        impl = ImplementationEntry(summary="Added login", files_changed=["auth.py"])
        blackboard.publish("implementations", impl.to_entry_content(), EntryType.FINDING, provenance)

        approval = ApprovalEntry(approved_by="SENTINEL", entry_id="e1", reason="Secure")
        blackboard.publish("reviews", approval.to_entry_content(), EntryType.FINDING, provenance)

        decision = ArchitectDecision(
            outcome=ArchitectDecisionOutcome.APPROVE,
            target_type="plan", target_id="plan_1", reason="Good plan",
        )
        blackboard.publish(
            "architect_decisions", decision.model_dump_json(exclude_none=True),
            EntryType.DECISION, provenance,
        )

        # Generate summary
        summary = herald.generate_collaboration_summary(
            blackboard, session_title="Sprint Review",
        )
        assert summary["metadata"]["finding_count"] == 1
        assert summary["metadata"]["implementation_count"] == 1
        assert summary["metadata"]["approval_count"] == 1
        assert summary["metadata"]["architect_decision_count"] == 1
        assert "Sprint Review" in summary["overview"]
        assert "Python" in summary["findings"]

        # Submit for review
        entry_id = herald.submit_summary_for_review(blackboard, summary, task_id="task_sprint")
        assert entry_id != ""

        # Verify published
        entries = blackboard.read("collaboration_summaries")
        assert len(entries) == 1

        # Generate session report
        report_id = herald.generate_session_report(blackboard, summary=summary)
        assert report_id != ""
        reports = blackboard.read("user_reports")
        assert len(reports) == 1


# ===========================================================================
# No Direct Messaging
# ===========================================================================


class TestNoDirectMessaging:
    """Verify that the refactored HERALD uses NO agent-to-agent messaging."""

    def test_no_message_imports(self):
        import specialists.herald as herald_mod
        import inspect
        source = inspect.getsource(herald_mod)
        forbidden_imports = [
            "from agent_communication",
            "import AgentCommunicationRouter",
            "from specialists.coordination",
        ]
        for pattern in forbidden_imports:
            assert pattern not in source, (
                f"HERALD contains forbidden direct-messaging import: {pattern}"
            )
        import re
        message_calls = re.findall(r"^[^#]*\bMessage\(", source, re.MULTILINE)
        assert len(message_calls) == 0, (
            f"HERALD contains {len(message_calls)} direct Message() calls"
        )

    def test_no_router_in_methods(self, herald: HeraldSpecialist):
        import inspect
        methods = [
            herald.pickup_task,
            herald.generate_collaboration_summary,
            herald.submit_summary_for_review,
            herald.check_for_summary_review,
            herald.generate_session_report,
        ]
        for method in methods:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            forbidden_params = {"router", "communication_router", "messenger"}
            for param_name in params:
                assert param_name not in forbidden_params, (
                    f"{method.__name__} has forbidden param: {param_name}"
                )

    def test_all_methods_use_blackboard_or_taskboard(self, herald: HeraldSpecialist):
        import inspect
        methods = {
            "pickup_task": "task_board",
            "generate_collaboration_summary": "blackboard",
            "submit_summary_for_review": "blackboard",
            "check_for_summary_review": "blackboard",
            "generate_session_report": "blackboard",
        }
        for method_name, expected_param in methods.items():
            method = getattr(herald, method_name)
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            assert expected_param in params, (
                f"{method_name} missing expected param: {expected_param}"
            )
