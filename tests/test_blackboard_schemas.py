"""Tests for Phase 4 — SharedBlackboard evolution.

Covers:
- ``CognitiveBlackboard.archive()`` — archive entries, get archived, idempotency
- ``CognitiveBlackboard.get_archived_entries()`` — filter by slot, exclude from active
- ``cognition/blackboard_schemas.py`` — all 9 typed schemas, serialization round-trips
- Schema registry — deserialize_entry_content, serialize_to_entry_content
"""

import json
import pytest

from cognition.types import (
    EntryType, Provenance, ProvenanceType,
)
from cognition.blackboard import CognitiveBlackboard
from cognition.blackboard_schemas import (
    FindingEntry,
    ImplementationEntry,
    RejectionEntry,
    ApprovalEntry,
    EscalationEntry,
    ConsensusEntry,
    ExecutionResultEntry,
    QuestionEntry,
    AnswerEntry,
    ENTRY_SCHEMA_REGISTRY,
    deserialize_entry_content,
    serialize_to_entry_content,
)

# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def bb() -> CognitiveBlackboard:
    """A fresh blackboard with no persistence."""
    return CognitiveBlackboard()


@pytest.fixture
def provenance() -> Provenance:
    return Provenance(source_type=ProvenanceType.SPECIALIST, source_id="test")


# ===========================================================================
# archive() — CognitiveBlackboard
# ===========================================================================


class TestArchive:
    """``archive()`` marks entries as archived and excludes them from active view."""

    def test_archive_entry_by_id(self, bb: CognitiveBlackboard, provenance: Provenance):
        entry = bb.publish("findings", "original finding", EntryType.FINDING, provenance)
        assert len(bb.read("findings")) == 1

        result = bb.archive(entry.id, reason="Outdated")
        assert result is True

        active = bb.read("findings")
        assert len(active) == 0
        assert bb.get_all_active_entries() == []

    def test_archive_nonexistent_entry(self, bb: CognitiveBlackboard):
        result = bb.archive("nonexistent-id", reason="no such entry")
        assert result is False

    def test_archive_is_idempotent(self, bb: CognitiveBlackboard, provenance: Provenance):
        entry = bb.publish("facts", "something", EntryType.FACT, provenance)
        assert bb.archive(entry.id) is True
        # Archiving again should succeed but the entry is already archived
        assert bb.archive(entry.id) is True

    def test_archive_with_archived_by(self, bb: CognitiveBlackboard, provenance: Provenance):
        entry = bb.publish("research", "test data", EntryType.FACT, provenance)
        bb.archive(entry.id, reason="Superseded", archived_by="ARCHITECT")
        # Verify the entry is archived (can't check fields directly, but
        # the entry should not appear in active entries)
        assert len(bb.read("research")) == 0

    def test_archived_entries_do_not_appear_in_read_by_type(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        e1 = bb.publish("slot", "active", EntryType.FACT, provenance)
        e2 = bb.publish("slot", "to_archive", EntryType.FACT, provenance)
        bb.archive(e2.id, reason="No longer relevant")
        results = bb.read("slot", entry_type=EntryType.FACT)
        assert len(results) == 1
        assert results[0].id == e1.id

    def test_archived_entries_excluded_from_query(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        bb.publish("data", "keep this result", EntryType.FACT, provenance, tags=["keep"])
        e2 = bb.publish("data", "remove this result", EntryType.FACT, provenance, tags=["remove"])
        bb.archive(e2.id)
        results = bb.query("result")
        assert len(results) == 1
        assert "keep" in results[0].tags


class TestGetArchivedEntries:
    """``get_archived_entries()`` returns only archived entries."""

    def test_get_archived_returns_archived(self, bb: CognitiveBlackboard, provenance: Provenance):
        bb.publish("slot", "active", EntryType.FACT, provenance)
        e2 = bb.publish("slot", "gone", EntryType.FACT, provenance)
        bb.archive(e2.id)
        archived = bb.get_archived_entries()
        assert len(archived) == 1
        assert archived[0].id == e2.id

    def test_get_archived_empty_when_none(self, bb: CognitiveBlackboard):
        assert bb.get_archived_entries() == []

    def test_get_archived_filtered_by_slot(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        e1 = bb.publish("alpha", "a", EntryType.FACT, provenance)
        e2 = bb.publish("beta", "b", EntryType.FACT, provenance)
        bb.archive(e1.id)
        bb.archive(e2.id)
        assert len(bb.get_archived_entries(slot_name="alpha")) == 1
        assert len(bb.get_archived_entries(slot_name="beta")) == 1
        assert len(bb.get_archived_entries(slot_name="gamma")) == 0

    def test_archive_does_not_affect_other_slots(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        bb.publish("keep_slot", "active data", EntryType.FACT, provenance)
        e2 = bb.publish("archive_slot", "to archive", EntryType.FACT, provenance)
        bb.archive(e2.id)
        assert len(bb.read("keep_slot")) == 1
        assert len(bb.read("archive_slot")) == 0

    def test_superseded_entries_not_in_archived(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        e1 = bb.publish("slot", "original", EntryType.FACT, provenance)
        e2 = bb.publish("slot", "replacement", EntryType.FACT, provenance)
        bb.supersede(e1.id, e2.id)
        # superseded_by set, but not to ARCHIVE_SENTINEL
        archived = bb.get_archived_entries()
        assert len(archived) == 0

    def test_archive_via_supersede_then_archive(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        e = bb.publish("slot", "entry", EntryType.FACT, provenance)
        bb.supersede(e.id, "replacement_id")
        # First superseded, then archived
        bb.archive(e.id)
        archived = bb.get_archived_entries()
        assert len(archived) == 1
        assert archived[0].id == e.id


# ===========================================================================
# Typed Schemas — Creation & Defaults
# ===========================================================================


class TestFindingEntry:
    def test_create(self):
        entry = FindingEntry(summary="Found a bug")
        assert entry.specialist == "ORACLE"
        assert entry.summary == "Found a bug"
        assert entry.confidence == 0.5

    def test_serialize_round_trip(self):
        original = FindingEntry(summary="Test", detail="Details", sources=["src1"])
        content = original.to_entry_content()
        restored = FindingEntry.from_entry_content(content)
        assert restored.summary == original.summary
        assert restored.detail == original.detail
        assert restored.sources == ["src1"]


class TestImplementationEntry:
    def test_create(self):
        entry = ImplementationEntry(summary="Refactored auth")
        assert entry.specialist == "FORGE"
        assert entry.security_review_requested is True

    def test_round_trip(self):
        original = ImplementationEntry(
            summary="Added login",
            files_changed=["auth.py"],
            files_created=["login.py"],
        )
        restored = ImplementationEntry.from_entry_content(original.to_entry_content())
        assert restored.files_changed == ["auth.py"]
        assert restored.files_created == ["login.py"]


class TestRejectionEntry:
    def test_create(self):
        entry = RejectionEntry(rejected_by="SENTINEL", reason="Hardcoded secret")
        assert entry.severity == "medium"

    def test_round_trip(self):
        original = RejectionEntry(
            rejected_by="SENTINEL",
            entry_id="abc123",
            reason="Secret found",
            findings=["line 42: API_KEY hardcoded"],
            remediations=["Use env var"],
            severity="high",
        )
        restored = RejectionEntry.from_entry_content(original.to_entry_content())
        assert restored.rejected_by == "SENTINEL"
        assert restored.severity == "high"
        assert len(restored.remediations) == 1


class TestApprovalEntry:
    def test_create(self):
        entry = ApprovalEntry(approved_by="ARCHITECT", entry_id="e1")
        assert entry.confidence == 0.8

    def test_round_trip(self):
        original = ApprovalEntry(
            approved_by="SENTINEL",
            entry_id="impl_1",
            reason="No security issues",
            conditions=["Run tests before merge"],
        )
        restored = ApprovalEntry.from_entry_content(original.to_entry_content())
        assert len(restored.conditions) == 1
        assert restored.approved_by == "SENTINEL"


class TestEscalationEntry:
    def test_create(self):
        entry = EscalationEntry(
            escalated_by="FORGE",
            reason="Need architectural decision",
        )
        assert entry.urgency == "medium"

    def test_round_trip(self):
        original = EscalationEntry(
            escalated_by="SENTINEL",
            reason="Potential RCE vulnerability",
            context={"file": "upload.py", "line": 88},
            suggested_action="Disable upload endpoint",
            urgency="critical",
        )
        restored = EscalationEntry.from_entry_content(original.to_entry_content())
        assert restored.urgency == "critical"
        assert restored.context["file"] == "upload.py"


class TestConsensusEntry:
    def test_create(self):
        entry = ConsensusEntry(topic="Should we refactor?")
        assert entry.outcome == "agreed"

    def test_round_trip(self):
        original = ConsensusEntry(
            topic="Deployment strategy",
            outcome="partial",
            positions={"ORACLE": "blue-green", "FORGE": "rolling"},
            confidence=0.7,
            recommendation="Proceed with blue-green",
            participants=["ORACLE", "FORGE", "SENTINEL"],
        )
        restored = ConsensusEntry.from_entry_content(original.to_entry_content())
        assert restored.outcome == "partial"
        assert "ORACLE" in restored.participants
        assert restored.recommendation == "Proceed with blue-green"


class TestExecutionResultEntry:
    def test_create(self):
        entry = ExecutionResultEntry()
        assert entry.specialist == "TERMINUS"
        assert entry.exit_code == 0
        assert entry.success is True

    def test_round_trip(self):
        original = ExecutionResultEntry(
            command="pytest tests/",
            exit_code=1,
            stdout="3 failed",
            stderr="error details",
            success=False,
            duration_ms=1500.0,
        )
        restored = ExecutionResultEntry.from_entry_content(original.to_entry_content())
        assert restored.exit_code == 1
        assert restored.success is False
        assert restored.duration_ms == 1500.0


class TestQuestionEntry:
    def test_create(self):
        entry = QuestionEntry(asked_by="FORGE", question="What API should I use?")
        assert entry.directed_to == ""

    def test_round_trip(self):
        original = QuestionEntry(
            asked_by="FORGE",
            question="Best practice for error handling?",
            context={"file": "api.py"},
            directed_to="ORACLE",
            tags=["error-handling"],
        )
        restored = QuestionEntry.from_entry_content(original.to_entry_content())
        assert restored.directed_to == "ORACLE"
        assert "error-handling" in restored.tags


class TestAnswerEntry:
    def test_create(self):
        entry = AnswerEntry(question_id="q1", answered_by="ORACLE", answer="Use try/except")
        assert entry.confidence == 0.5

    def test_round_trip(self):
        original = AnswerEntry(
            question_id="q1",
            answered_by="ORACLE",
            answer="Use structlog for structured logging",
            evidence=["docs.python.org/3/howto/logging"],
            confidence=0.9,
        )
        restored = AnswerEntry.from_entry_content(original.to_entry_content())
        assert restored.question_id == "q1"
        assert len(restored.evidence) == 1
        assert restored.confidence == 0.9


# ===========================================================================
# Schema Registry
# ===========================================================================


class TestSchemaRegistry:
    def test_registry_contains_all_types(self):
        expected_keys = {
            "finding", "implementation", "rejection", "approval",
            "escalation", "consensus", "execution_result",
            "question", "answer",
        }
        assert set(ENTRY_SCHEMA_REGISTRY.keys()) == expected_keys

    def test_deserialize_finding(self):
        content = FindingEntry(summary="found it").to_entry_content()
        result = deserialize_entry_content(content, "finding")
        assert isinstance(result, FindingEntry)
        assert result.summary == "found it"

    def test_deserialize_approval(self):
        content = ApprovalEntry(approved_by="ARCHITECT", entry_id="e1").to_entry_content()
        result = deserialize_entry_content(content, "approval")
        assert isinstance(result, ApprovalEntry)
        assert result.approved_by == "ARCHITECT"

    def test_deserialize_unknown_type(self):
        with pytest.raises(ValueError, match="Unknown schema type"):
            deserialize_entry_content("{}", "unknown_type")

    def test_serialize_round_trip_all_types(self):
        schemas = [
            FindingEntry(summary="s"),
            ImplementationEntry(summary="s"),
            RejectionEntry(rejected_by="SENTINEL", reason="r"),
            ApprovalEntry(approved_by="ARCH", entry_id="e"),
            EscalationEntry(escalated_by="FORGE", reason="r"),
            ConsensusEntry(topic="t"),
            ExecutionResultEntry(),
            QuestionEntry(asked_by="F", question="q"),
            AnswerEntry(question_id="q", answered_by="O", answer="a"),
        ]
        for schema in schemas:
            content = serialize_to_entry_content(schema)
            assert isinstance(content, str)
            parsed = json.loads(content)
            assert len(parsed) > 0

    def test_serialize_unknown_schema_type(self):
        """serialize_to_entry_content works with any BaseModel."""
        content = serialize_to_entry_content(FindingEntry(summary="test"))
        assert "test" in content


# ===========================================================================
# Integration: publish schema content to blackboard
# ===========================================================================


class TestIntegration:
    """Verify typed schemas can be used as blackboard entry content."""

    def test_publish_finding_to_blackboard(self, bb: CognitiveBlackboard, provenance: Provenance):
        finding = FindingEntry(summary="Python 3.12 is faster", detail="Benchmarks show 10% improvement")
        entry = bb.publish(
            slot_name="research_findings",
            content=finding.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
            tags=finding.tags,
        )
        assert entry.id is not None
        assert entry.entry_type == EntryType.FINDING

        # Read back and deserialize
        results = bb.read("research_findings")
        assert len(results) == 1
        restored = FindingEntry.from_entry_content(results[0].content)
        assert restored.summary == "Python 3.12 is faster"

    def test_publish_and_archive_schema_entry(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        impl = ImplementationEntry(
            summary="Added login flow",
            files_changed=["auth.py"],
        )
        entry = bb.publish(
            slot_name="implementations",
            content=impl.to_entry_content(),
            entry_type=EntryType.FINDING,
            provenance=provenance,
        )
        bb.archive(entry.id, reason="Superseded by new version", archived_by="ARCHITECT")
        assert len(bb.read("implementations")) == 0
        archived = bb.get_archived_entries(slot_name="implementations")
        assert len(archived) == 1

    def test_blackboard_snapshot_includes_archived_count(
        self, bb: CognitiveBlackboard, provenance: Provenance,
    ):
        e1 = bb.publish("s1", "data1", EntryType.FACT, provenance)
        bb.publish("s1", "data2", EntryType.FACT, provenance)
        bb.archive(e1.id)
        snap = bb.snapshot()
        # active_entry_count should be 1 (only e2 is active)
        assert snap["active_entry_count"] == 1
        # slot_count should be 1
        assert snap["slot_count"] == 1
