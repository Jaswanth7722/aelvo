"""Tests for Phase 6 — ORACLE blackboard-based collaboration.

Per Amendment 2: No agent-to-agent messaging.
All communication flows through the Shared Blackboard using typed schemas.

Covers:
- ``OracleSpecialist.pickup_task()`` — pick up RESEARCH tasks from SharedTaskBoard
- ``OracleSpecialist.publish_finding()`` — publish FindingEntry to blackboard
- ``OracleSpecialist.respond_to_question()`` — answer via AnswerEntry on blackboard
- ``OracleSpecialist.check_for_questions()`` — read QuestionEntry from blackboard
- ``OracleSpecialist.read_findings()`` — read FindingEntry from blackboard
- No direct messaging — no Message, no send_message, no agent-to-agent chat
"""

import pytest
from typing import Any, Dict, List

from specialists.oracle import OracleSpecialist
from cognition.blackboard import CognitiveBlackboard
from cognition.blackboard_schemas import (
    FindingEntry,
    QuestionEntry,
    AnswerEntry,
)
from cognition.types import (
    EntryType, Provenance, ProvenanceType,
)


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def oracle() -> OracleSpecialist:
    return OracleSpecialist()


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
    """A lightweight task board mock for testing ORACLE task pickup.

    Mimics the SharedTaskBoard API surface that OracleSpecialist uses.
    """

    def __init__(self):
        self._tasks: Dict[str, dict] = {}
        self._transitions: List[str] = []

    def add_task(self, task_id: str, **kwargs):
        defaults = {
            "type": "research",
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
                type=TaskType(data.get("type", "research")),
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
    def test_pickup_no_tasks(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        picked = oracle.pickup_task(board)
        assert picked == []

    def test_pickup_pending_research_task(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        board.add_task("t1", type="research", status="pending", title="Research Python async")
        picked = oracle.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t1"
        # Task should have been assigned and started
        assert "assigned:t1" in board.transition_log
        assert "started:t1" in board.transition_log

    def test_pickup_already_assigned_to_oracle(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        board.add_task("t2", type="research", status="assigned", specialist="ORACLE")
        picked = oracle.pickup_task(board)
        assert len(picked) == 1
        assert picked[0].id == "t2"

    def test_pickup_ignores_other_specialist_tasks(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        board.add_task("t3", type="research", status="pending", specialist="FORGE")
        picked = oracle.pickup_task(board)
        assert picked == []

    def test_pickup_non_research_task(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        board.add_task("t4", type="implement", status="pending", title="Write code")
        picked = oracle.pickup_task(board)
        assert picked == []

    def test_pickup_max_tasks(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        for i in range(5):
            board.add_task(f"t{i}", type="research", status="pending", title=f"Task {i}")
        picked = oracle.pickup_task(board, max_tasks=2)
        assert len(picked) == 2

    def test_pickup_with_none_board(self, oracle: OracleSpecialist):
        picked = oracle.pickup_task(None)
        assert picked == []

    def test_pickup_multiple_tasks(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        board.add_task("t5", type="research", status="pending", title="Research A")
        board.add_task("t6", type="research", status="pending", title="Research B")
        picked = oracle.pickup_task(board, max_tasks=10)
        assert len(picked) == 2

    def test_pickup_only_oracle_tasks(self, oracle: OracleSpecialist):
        board = FakeTaskBoard()
        board.add_task("t7", type="research", status="pending", title="Oracle task")
        board.add_task("t8", type="implement", status="pending", title="Forge task")
        board.add_task("t9", type="security_review", status="pending", title="Sentinel task")
        picked = oracle.pickup_task(board, max_tasks=10)
        assert len(picked) == 1
        assert picked[0].id == "t7"


# ===========================================================================
# Publish Finding
# ===========================================================================


class TestPublishFinding:
    def test_publish_finding(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        entry_id = oracle.publish_finding(
            blackboard=blackboard,
            summary="Python 3.13 is 15%% faster than 3.12",
            detail="Benchmarks show significant improvement in async performance",
            sources=["https://python.org/downloads"],
            confidence=0.8,
            tags=["python", "performance"],
        )
        assert entry_id != ""

        # Read back from blackboard
        entries = blackboard.read("research_findings")
        assert len(entries) == 1
        restored = FindingEntry.from_entry_content(entries[0].content)
        assert restored.summary == "Python 3.13 is 15%% faster than 3.12"
        assert "python" in restored.tags

    def test_publish_with_none_blackboard(self, oracle: OracleSpecialist):
        entry_id = oracle.publish_finding(None, summary="test")
        assert entry_id == ""

    def test_publish_minimal_finding(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        entry_id = oracle.publish_finding(
            blackboard=blackboard,
            summary="Quick finding",
        )
        assert entry_id != ""
        entries = blackboard.read("research_findings")
        assert len(entries) == 1

    def test_publish_multiple_findings(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        for i in range(3):
            oracle.publish_finding(
                blackboard=blackboard,
                summary=f"Finding {i}",
                confidence=0.5 + i * 0.1,
            )
        entries = blackboard.read("research_findings")
        assert len(entries) == 3

    def test_publish_finding_has_correct_slot(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        oracle.publish_finding(blackboard, summary="Test finding")
        entries = blackboard.read("research_findings")
        assert len(entries) == 1
        assert entries[0].slot_name == "research_findings"


# ===========================================================================
# Respond to Question
# ===========================================================================


class TestRespondToQuestion:
    def test_respond_to_question(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        question = QuestionEntry(
            asked_by="FORGE",
            question="What API should I use for async HTTP requests?",
            directed_to="ORACLE",
        )
        entry_id = oracle.respond_to_question(
            blackboard=blackboard,
            question_entry=question,
            answer="Use httpx.AsyncClient for async HTTP requests.",
            evidence=["https://www.python-httpx.org/"],
            confidence=0.9,
        )
        assert entry_id != ""

        # Read the answer back from the blackboard
        entries = blackboard.read("answers")
        assert len(entries) == 1
        restored = AnswerEntry.from_entry_content(entries[0].content)
        assert restored.question_id == question.question_id
        assert "httpx" in restored.answer
        assert restored.answered_by == "ORACLE"

    def test_respond_with_none_blackboard(self, oracle: OracleSpecialist):
        question = QuestionEntry(asked_by="FORGE", question="?")
        entry_id = oracle.respond_to_question(None, question, answer="no")
        assert entry_id == ""

    def test_respond_multiple_questions(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        q1 = QuestionEntry(asked_by="FORGE", question="Q1?")
        q2 = QuestionEntry(asked_by="SENTINEL", question="Q2?")
        oracle.respond_to_question(blackboard, q1, "A1")
        oracle.respond_to_question(blackboard, q2, "A2")
        entries = blackboard.read("answers")
        assert len(entries) == 2


# ===========================================================================
# Check for Questions
# ===========================================================================


class TestCheckForQuestions:
    def test_no_questions(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        questions = oracle.check_for_questions(blackboard)
        assert questions == []

    def test_find_questions(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        # Publish a question entry to the blackboard
        q = QuestionEntry(asked_by="FORGE", question="What library for CSV parsing?")
        blackboard.publish(
            slot_name="questions",
            content=q.to_entry_content(),
            entry_type=EntryType.QUERY,
            provenance=provenance,
        )
        questions = oracle.check_for_questions(blackboard)
        assert len(questions) == 1
        assert questions[0].asked_by == "FORGE"
        assert "CSV" in questions[0].question

    def test_find_multiple_questions(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        for i in range(3):
            q = QuestionEntry(asked_by="FORGE", question=f"Question {i}?")
            blackboard.publish(
                slot_name="questions",
                content=q.to_entry_content(),
                entry_type=EntryType.QUERY,
                provenance=provenance,
            )
        questions = oracle.check_for_questions(blackboard)
        assert len(questions) == 3

    def test_find_with_max_limit(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        for i in range(10):
            q = QuestionEntry(asked_by="FORGE", question=f"Q{i}?")
            blackboard.publish(
                slot_name="questions",
                content=q.to_entry_content(),
                entry_type=EntryType.QUERY,
                provenance=provenance,
            )
        questions = oracle.check_for_questions(blackboard, max_questions=3)
        assert len(questions) == 3

    def test_ignores_non_question_entries(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance):
        q = QuestionEntry(asked_by="FORGE", question="Real question?")
        blackboard.publish(
            slot_name="questions",
            content=q.to_entry_content(),
            entry_type=EntryType.QUERY,
            provenance=provenance,
        )
        # Publish a non-question entry in the same slot
        blackboard.publish(
            slot_name="questions",
            content="just a note",
            entry_type=EntryType.FACT,
            provenance=provenance,
        )
        # Only the QUERY-type entry should be returned
        questions = oracle.check_for_questions(blackboard)
        assert len(questions) == 1

    def test_read_findings(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        # Publish two findings
        oracle.publish_finding(blackboard, summary="Finding 1", tags=["python"])
        oracle.publish_finding(blackboard, summary="Finding 2", tags=["rust"])
        findings = oracle.read_findings(blackboard)
        assert len(findings) == 2
        assert any(f.summary == "Finding 1" for f in findings)

    def test_read_findings_empty(self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard):
        findings = oracle.read_findings(blackboard)
        assert findings == []

    def test_read_findings_with_none_blackboard(self, oracle: OracleSpecialist):
        findings = oracle.read_findings(None)
        assert findings == []


# ===========================================================================
# No Direct Messaging
# ===========================================================================


class TestNoDirectMessaging:
    """Verify that the refactored ORACLE uses NO agent-to-agent messaging.

    The OracleSpecialist should only communicate through:
    - SharedTaskBoard (task lifecycle)
    - CognitiveBlackboard (typed entries)
    - No Message objects, no send_message, no AgentCommunicationRouter
    """

    def test_no_message_imports(self):
        """OracleSpecialist should not import any messaging module."""
        import specialists.oracle as oracle_mod
        import inspect
        source = inspect.getsource(oracle_mod)
        # These import/call patterns indicate direct messaging.
        # We check at the AST/line level to avoid matching docstring comments.
        forbidden_imports = [
            "from agent_communication",
            "import AgentCommunicationRouter",
            "from specialists.coordination",
        ]
        for pattern in forbidden_imports:
            assert pattern not in source, (
                f"ORACLE contains forbidden direct-messaging import: {pattern}"
            )
        # Check that Message is not called as a constructor (but allow comments)
        # by looking for 'Message(' preceded by a non-comment character
        import re
        message_calls = re.findall(r"^[^#]*\bMessage\(", source, re.MULTILINE)
        assert len(message_calls) == 0, (
            f"ORACLE contains {len(message_calls)} direct Message() calls"
        )

    def test_no_router_in_methods(self, oracle: OracleSpecialist):
        """None of the collaboration methods should take a router parameter."""
        import inspect
        methods = [
            oracle.pickup_task,
            oracle.publish_finding,
            oracle.respond_to_question,
            oracle.check_for_questions,
            oracle.read_findings,
        ]
        for method in methods:
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            # None should have a 'router' or 'communication_router' parameter
            forbidden_params = {"router", "communication_router", "messenger"}
            for param_name in params:
                assert param_name not in forbidden_params, (
                    f"{method.__name__} has forbidden param: {param_name}"
                )

    def test_all_methods_use_blackboard_or_taskboard(self, oracle: OracleSpecialist):
        """All collaboration methods use blackboard or task_board, not messaging."""
        import inspect
        methods = {
            "pickup_task": "task_board",
            "publish_finding": "blackboard",
            "respond_to_question": "blackboard",
            "check_for_questions": "blackboard",
            "read_findings": "blackboard",
        }
        for method_name, expected_param in methods.items():
            method = getattr(oracle, method_name)
            sig = inspect.signature(method)
            params = list(sig.parameters.keys())
            assert expected_param in params, (
                f"{method_name} missing expected param: {expected_param}"
            )

    def test_integration_publish_and_read_findings(
        self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard,
    ):
        """End-to-end: publish finding, then read it via blackboard."""
        eid = oracle.publish_finding(
            blackboard, summary="E2E test", detail="Works end-to-end",
        )
        assert eid != ""
        entries = blackboard.read("research_findings")
        assert len(entries) == 1
        finding = FindingEntry.from_entry_content(entries[0].content)
        assert finding.summary == "E2E test"

    def test_integration_question_answer_cycle(
        self, oracle: OracleSpecialist, blackboard: CognitiveBlackboard, provenance: Provenance,
    ):
        """End-to-end: FORGE asks a question, ORACLE answers it."""
        # FORGE publishes a question
        q = QuestionEntry(
            asked_by="FORGE",
            question="Best practice for error handling in Python?",
            directed_to="ORACLE",
            tags=["python", "error-handling"],
        )
        blackboard.publish(
            slot_name="questions",
            content=q.to_entry_content(),
            entry_type=EntryType.QUERY,
            provenance=provenance,
        )
        # ORACLE checks for questions
        questions = oracle.check_for_questions(blackboard)
        assert len(questions) == 1
        assert questions[0].asked_by == "FORGE"

        # ORACLE answers
        answer_id = oracle.respond_to_question(
            blackboard=blackboard,
            question_entry=questions[0],
            answer="Use custom exception classes and a global handler.",
            evidence=["docs.python.org/3/tutorial/errors.html"],
            confidence=0.85,
        )
        assert answer_id != ""

        # Verify answer is on the blackboard
        answers = blackboard.read("answers")
        assert len(answers) == 1
        restored = AnswerEntry.from_entry_content(answers[0].content)
        assert restored.answer.startswith("Use custom exception")
        assert restored.answered_by == "ORACLE"
