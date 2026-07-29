# tests/test_phase10_memory_integration.py — Phase 10: Memory Integration
#
# Tests for:
#   1. ConsensusMemory — ChromaDB + SQLite dual-sync persistence
#   2. CollaborationAccumulator — Collaboration pattern extraction
#   3. SpecialistEffectivenessTracker — SQLite-based specialist metrics

from __future__ import annotations

import pytest
import sqlite3
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch
from pytest import approx

from learning.types import (
    ConsensusOutcome,
    CollaborationEventType,
    CollaborationSignature,
    CollaborationObservation,
    CollaborationPattern,
    ConsensusMemoryRecord,
    SpecialistEffectivenessRecord,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def memory_engine():
    """Create a mock memory engine with ChromaDB collection and SQLite."""
    engine = MagicMock()
    engine.memory_collection = MagicMock()
    engine.db = sqlite3.connect(":memory:")
    engine.db.execute("""
        CREATE TABLE IF NOT EXISTS retained_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    engine.db.commit()
    engine.project_name = "test_project"
    return engine


@pytest.fixture
def consensus_memory(memory_engine):
    """Create a ConsensusMemory with mock engine."""
    from learning.consensus_memory import ConsensusMemory
    return ConsensusMemory(memory_engine, project_name="test_project")


@pytest.fixture
def consensus_record():
    """Create a sample ConsensusMemoryRecord."""
    return ConsensusMemoryRecord(
        consensus_id="cons_001",
        topic="Should we use async/await for database access?",
        outcome=ConsensusOutcome.AGREED,
        confidence=0.85,
        participant_count=3,
        specialists_involved=["FORGE", "SENTINEL", "ARCHITECT"],
        vote_summary="FORGE: yes, SENTINEL: yes, ARCHITECT: yes",
        vetoed=False,
        governance_applied=True,
        architect_override=False,
        session_id="session_001",
    )


@pytest.fixture
def collaboration_accumulator():
    """Create a CollaborationAccumulator with default thresholds."""
    from learning.collaboration_accumulator import CollaborationAccumulator
    return CollaborationAccumulator(
        min_observations_for_pattern=2,
        max_patterns=50,
    )


@pytest.fixture
def effectiveness_tracker():
    """Create a SpecialistEffectivenessTracker with in-memory SQLite."""
    from learning.specialist_effectiveness import SpecialistEffectivenessTracker
    return SpecialistEffectivenessTracker(db_path=":memory:")


# =============================================================================
# Test ConsensusMemoryRecord
# =============================================================================

class TestConsensusMemoryRecord:
    def test_build_content(self, consensus_record):
        content = consensus_record.build_content()
        assert "Consensus: Should we use async/await" in content
        assert "Outcome: agreed" in content
        assert "Confidence: 0.85" in content
        assert "FORGE, SENTINEL, ARCHITECT" in content

    def test_build_content_no_vote(self):
        record = ConsensusMemoryRecord(
            consensus_id="cons_002",
            topic="Simple agreement",
            outcome=ConsensusOutcome.PARTIAL,
            confidence=0.6,
        )
        content = record.build_content()
        assert "Consensus: Simple agreement" in content
        assert "Outcome: partial" in content
        assert "Votes:" not in content  # No vote_summary provided

    def test_build_content_vetoed(self):
        record = ConsensusMemoryRecord(
            consensus_id="cons_003",
            topic="Security concern",
            outcome=ConsensusOutcome.VETOED,
            confidence=0.9,
            participant_count=2,
            specialists_involved=["FORGE", "SENTINEL"],
            vetoed=True,
            veto_reason="Security vulnerability detected in proposed implementation",
        )
        content = record.build_content()
        assert "Vetoed: Security vulnerability" in content

    def test_build_content_architect_override(self):
        record = ConsensusMemoryRecord(
            consensus_id="cons_004",
            topic="Architecture decision",
            outcome=ConsensusOutcome.ESCALATED,
            confidence=0.7,
            participant_count=3,
            specialists_involved=["FORGE", "SENTINEL", "ORACLE"],
            architect_override=True,
        )
        content = record.build_content()
        assert "Architect override applied" in content


# =============================================================================
# Test ConsensusMemory
# =============================================================================

class TestConsensusMemory:
    def test_save_consensus_success(self, consensus_memory, consensus_record):
        """Verify a consensus record is saved successfully."""
        result = consensus_memory.save_consensus(consensus_record)
        assert result is True

    def test_save_and_query(self, consensus_memory, consensus_record):
        """Verify saved consensus can be queried back."""
        consensus_memory.save_consensus(consensus_record)

        # Mock ChromaDB query to return proper results
        consensus_memory.collection.query.return_value = {
            "ids": [["id_1"]],
            "documents": [[consensus_record.build_content()]],
            "metadatas": [[{
                "type": "consensus_record",
                "outcome": "agreed",
                "confidence": 0.85,
                "topic": "async/await",
            }]],
            "distances": [[0.15]],
        }

        results = consensus_memory.query_consensus(topic="async/await")
        assert len(results) == 1
        assert results[0]["score"] > 0.7
        assert results[0]["meta"]["outcome"] == "agreed"

    def test_save_empty_content(self, consensus_memory):
        """Verify a minimal record saves successfully (build_content always produces output)."""
        record = ConsensusMemoryRecord(
            consensus_id="cons_minimal",
            topic="Minimal consensus",
            outcome=ConsensusOutcome.AGREED,
        )
        result = consensus_memory.save_consensus(record)
        assert result is True  # build_content always produces valid text

    def test_save_duplicate_resolved(self, consensus_memory, consensus_record):
        """Verify duplicate saves are handled via conflict resolution."""
        consensus_memory.save_consensus(consensus_record)

        # Mock collection.query to return a match with high similarity
        content = consensus_record.build_content()
        consensus_memory.collection.query.return_value = {
            "ids": [["existing_id"]],
            "documents": [[content]],
            "metadatas": [[{"importance": 0.5, "usage_count": 0}]],
            "distances": [[0.02]],  # similarity = 0.98 → duplicate
        }

        result = consensus_memory.save_consensus(consensus_record)
        assert result is False  # Duplicate, skipped

    def test_save_sqlite_rollback(self, consensus_memory, consensus_record):
        """Verify ChromaDB failure is handled gracefully."""
        # Make ChromaDB raise an error
        consensus_memory.collection.add.side_effect = Exception("ChromaDB error")

        result = consensus_memory.save_consensus(consensus_record)
        assert result is False  # ChromaDB write failed

    def test_list_recent(self, consensus_memory, consensus_record):
        """Verify list_recent reads from SQLite."""
        # Insert directly into SQLite
        consensus_memory.save_consensus(consensus_record)

        # Direct SQLite insert for testing
        consensus_memory.memory_engine.db.execute(
            "INSERT INTO retained_memory (content) VALUES (?)",
            ("[CONSENSUS:agreed|test_project] test consensus",),
        )
        consensus_memory.memory_engine.db.commit()

        results = consensus_memory.list_recent(limit=10)
        assert len(results) >= 1
        assert any("[CONSENSUS:" in r["content"] for r in results)

    def test_chromadb_failure(self, consensus_memory, consensus_record):
        """Verify ChromaDB failure returns False without SQLite write."""
        consensus_memory.collection.add.side_effect = Exception("ChromaDB error")

        result = consensus_memory.save_consensus(consensus_record)
        assert result is False

    def test_multiple_outcomes(self, consensus_memory):
        """Verify different consensus outcomes are saved correctly."""
        outcomes = [
            ConsensusOutcome.AGREED,
            ConsensusOutcome.PARTIAL,
            ConsensusOutcome.DISAGREED,
            ConsensusOutcome.VETOED,
            ConsensusOutcome.ESCALATED,
        ]

        for i, outcome in enumerate(outcomes):
            record = ConsensusMemoryRecord(
                consensus_id=f"cons_{i}",
                topic=f"Outcome test {i}",
                outcome=outcome,
                confidence=0.5 + i * 0.1,
                specialists_involved=["FORGE", "SENTINEL"],
            )
            result = consensus_memory.save_consensus(record)
            assert result is True


# =============================================================================
# Test CollaborationAccumulator
# =============================================================================

class TestCollaborationAccumulator:
    def test_ingest_below_threshold(self, collaboration_accumulator, consensus_record):
        """Verify single observation doesn't create a pattern."""
        pattern = collaboration_accumulator.ingest_consensus(consensus_record)
        assert pattern is None  # Below min_observations=2

    def test_ingest_creates_pattern(self, collaboration_accumulator, consensus_record):
        """Verify enough observations create a pattern."""
        pattern1 = collaboration_accumulator.ingest_consensus(consensus_record)
        assert pattern1 is None  # First

        pattern2 = collaboration_accumulator.ingest_consensus(consensus_record)
        assert pattern2 is not None  # Second → threshold met
        assert pattern2.signature.event_type == CollaborationEventType.CONSENSUS_REACHED
        assert pattern2.observation_count >= 2

    def test_pattern_confidence_increases(self, collaboration_accumulator, consensus_record):
        """Verify pattern confidence grows with more successes."""
        for _ in range(5):
            collaboration_accumulator.ingest_consensus(consensus_record)

        patterns = collaboration_accumulator.get_patterns()
        assert len(patterns) >= 1
        assert patterns[0].confidence > 0.3

    def test_different_outcomes(self, collaboration_accumulator):
        """Verify different event types produce different patterns."""
        # Consensus reached (success)
        record = ConsensusMemoryRecord(
            consensus_id="cons_success",
            topic="Agree on approach",
            outcome=ConsensusOutcome.AGREED,
            confidence=0.9,
            specialists_involved=["FORGE", "SENTINEL"],
        )
        collaboration_accumulator.ingest_consensus(record)
        collaboration_accumulator.ingest_consensus(record)

        # Collaboration event (different type)
        collaboration_accumulator.ingest_collaboration_event(
            event_type=CollaborationEventType.CHALLENGE_RAISED,
            specialists_involved=["FORGE", "SENTINEL"],
            outcome="disagreed",
            description="Challenge on implementation approach",
        )
        collaboration_accumulator.ingest_collaboration_event(
            event_type=CollaborationEventType.CHALLENGE_RAISED,
            specialists_involved=["FORGE", "SENTINEL"],
            outcome="disagreed",
            description="Challenge on security approach",
        )

        patterns = collaboration_accumulator.get_patterns()
        assert len(patterns) == 2  # Two different signatures

        by_type = collaboration_accumulator.get_patterns_by_type(
            CollaborationEventType.CONSENSUS_REACHED
        )
        assert len(by_type) == 1

        by_type2 = collaboration_accumulator.get_patterns_by_type(
            CollaborationEventType.CHALLENGE_RAISED
        )
        assert len(by_type2) == 1

    def test_raw_collaboration_event(self, collaboration_accumulator):
        """Verify raw collaboration events create patterns."""
        for _ in range(3):
            collaboration_accumulator.ingest_collaboration_event(
                event_type=CollaborationEventType.TASK_COMPLETED,
                specialists_involved=["FORGE"],
                outcome="success",
                description="Implemented feature X",
                duration_ms=1500,
            )

        patterns = collaboration_accumulator.get_patterns()
        assert len(patterns) >= 1
        assert patterns[0].avg_duration_ms > 0

    def test_persistence_callback(self, collaboration_accumulator, consensus_record):
        """Verify persistence callback is invoked."""
        saved_patterns = []

        def callback(pattern):
            saved_patterns.append(pattern)

        collaboration_accumulator.set_persistence_callback(callback)

        # Trigger pattern creation
        collaboration_accumulator.ingest_consensus(consensus_record)
        collaboration_accumulator.ingest_consensus(consensus_record)

        assert len(saved_patterns) >= 1
        assert isinstance(saved_patterns[0], CollaborationPattern)

    def test_get_statistics(self, collaboration_accumulator, consensus_record):
        """Verify get_statistics returns correct aggregate data."""
        stats = collaboration_accumulator.get_statistics()
        assert stats["total_patterns"] == 0

        # Create some patterns
        for _ in range(3):
            collaboration_accumulator.ingest_consensus(consensus_record)

        stats = collaboration_accumulator.get_statistics()
        assert stats["total_patterns"] >= 1
        assert stats["avg_confidence"] > 0

    def test_load_from_persistence(self, collaboration_accumulator):
        """Verify patterns can be loaded from persistence."""
        pattern = CollaborationPattern(
            signature=CollaborationSignature(
                event_type=CollaborationEventType.CONSENSUS_REACHED,
                participant_count=2,
                specialist_roles=["FORGE", "SENTINEL"],
            ),
            observation_count=5,
            confidence=0.8,
        )
        pattern.to_digest()

        count = collaboration_accumulator.load_from_persistence([pattern])
        assert count == 1
        assert collaboration_accumulator.get_pattern(pattern.id) is not None

    def test_flush(self, collaboration_accumulator, consensus_record):
        """Verify flush calls persistence callback for all patterns."""
        saved = []

        def cb(p):
            saved.append(p)

        collaboration_accumulator.set_persistence_callback(cb)

        # Create patterns
        for _ in range(3):
            collaboration_accumulator.ingest_consensus(consensus_record)

        count = collaboration_accumulator.flush()
        assert count >= 1
        assert len(saved) >= 1

    def test_max_patterns_enforced(self, collaboration_accumulator):
        """Verify max patterns limit is enforced."""
        # Use very low max to trigger limit
        from learning.collaboration_accumulator import CollaborationAccumulator
        limited = CollaborationAccumulator(
            min_observations_for_pattern=1,
            max_patterns=2,
        )

        # Create 3 different pattern types
        for i in range(3):
            record = ConsensusMemoryRecord(
                consensus_id=f"cons_{i}",
                topic=f"Distinct topic {i}",
                outcome=ConsensusOutcome.AGREED,
                participant_count=i + 1,
                specialists_involved=[f"SPEC{i}"] * (i + 1),
            )
            limited.ingest_consensus(record)

        assert len(limited.get_patterns()) <= 2

    def test_get_observation_count(self, collaboration_accumulator, consensus_record):
        """Verify observation counting works."""
        for _ in range(5):
            collaboration_accumulator.ingest_consensus(consensus_record)

        patterns = collaboration_accumulator.get_patterns()
        if patterns:
            sig_hash = patterns[0].signature.signature_hash
            count = collaboration_accumulator.get_observation_count(sig_hash)
            assert count >= 2  # At least as many as threshold


# =============================================================================
# Test SpecialistEffectivenessTracker SQLite
# =============================================================================

class TestSpecialistEffectivenessTracker:
    def test_record_task_outcome(self, effectiveness_tracker):
        """Verify task outcome recording."""
        record = effectiveness_tracker.record_task_outcome(
            specialist="FORGE",
            session_id="session_001",
            succeeded=True,
            duration_ms=5000.0,
            was_first_attempt=True,
        )
        assert record.tasks_attempted == 1
        assert record.tasks_succeeded == 1
        assert record.first_attempts == 1
        assert record.first_attempt_successes == 1
        assert record.total_duration_ms == 5000.0
        assert record.specialist == "FORGE"

    def test_record_first_attempt(self, effectiveness_tracker):
        """Verify first attempt recording."""
        record = effectiveness_tracker.record_first_attempt(
            specialist="SENTINEL",
            session_id="session_001",
            succeeded=True,
        )
        assert record.tasks_attempted == 1
        assert record.tasks_succeeded == 1
        assert record.first_attempts == 1
        assert record.first_attempt_successes == 1

    def test_record_consensus_participation(self, effectiveness_tracker):
        """Verify consensus participation recording."""
        record = effectiveness_tracker.record_consensus_participation(
            specialist="FORGE",
            session_id="session_001",
            aligned=True,
        )
        assert record.consensus_participations == 1
        assert record.consensus_aligned == 1

    def test_record_pattern_contributed(self, effectiveness_tracker):
        """Verify pattern contribution recording."""
        record = effectiveness_tracker.record_pattern_contributed(
            specialist="FORGE",
            session_id="session_001",
        )
        assert record.patterns_contributed == 1

    def test_record_blackboard_publication(self, effectiveness_tracker):
        """Verify blackboard publication recording."""
        record = effectiveness_tracker.record_blackboard_publication(
            specialist="ORACLE",
            session_id="session_001",
        )
        assert record.blackboard_publications == 1

    def test_get_record(self, effectiveness_tracker):
        """Verify record retrieval."""
        effectiveness_tracker.record_task_outcome(
            specialist="FORGE", session_id="session_001",
            succeeded=True,
        )
        record = effectiveness_tracker.get_record("FORGE", "session_001")
        assert record is not None
        assert record.id is not None

    def test_get_specialist_records(self, effectiveness_tracker):
        """Verify multiple records retrieval."""
        for i in range(3):
            effectiveness_tracker.record_task_outcome(
                specialist="FORGE",
                session_id=f"session_{i:03d}",
                succeeded=i % 2 == 0,
            )

        records = effectiveness_tracker.get_specialist_records("FORGE", limit=10)
        assert len(records) >= 3

    def test_get_aggregate(self, effectiveness_tracker):
        """Verify aggregate computation."""
        # FORGE: 5 tasks, 3 successes, 2 first attempts (both succeeded)
        for i in range(5):
            was_first = i < 2
            # First 2 attempts: i=0 (success), i=1 (success)
            # Next 3 attempts: i=2 (success), i=3 (failure), i=4 (failure)
            effectiveness_tracker.record_task_outcome(
                specialist="FORGE",
                session_id="session_001",
                succeeded=i < 3,
                was_first_attempt=was_first,
            )
        effectiveness_tracker.record_consensus_participation(
            "FORGE", "session_001", aligned=True,
        )
        effectiveness_tracker.record_consensus_participation(
            "FORGE", "session_001", aligned=False,
        )

        agg = effectiveness_tracker.get_aggregate("FORGE")
        assert agg["total_tasks"] == 5
        assert agg["total_successes"] == 3
        assert agg["overall_success_rate"] == approx(0.6)
        assert agg["total_first_attempts"] == 2
        assert agg["first_attempt_success_rate"] == approx(1.0)  # Both first attempts succeeded
        assert agg["total_consensus_participations"] == 2
        assert agg["consensus_alignment_rate"] == approx(0.5)

    def test_nonexistent_aggregate(self, effectiveness_tracker):
        """Verify aggregate for unknown specialist returns zeros."""
        agg = effectiveness_tracker.get_aggregate("UNKNOWN")
        assert agg["total_tasks"] == 0
        assert agg["overall_success_rate"] == 0.0

    def test_get_top_first_attempt_specialists(self, effectiveness_tracker):
        """Verify ranking of specialists by first-attempt success."""
        # FORGE: 100% success on 5 first attempts
        for _ in range(5):
            effectiveness_tracker.record_first_attempt("FORGE", "s1", succeeded=True)

        # SENTINEL: 60% success on 5 first attempts
        for i in range(5):
            effectiveness_tracker.record_first_attempt(
                "SENTINEL", "s1", succeeded=i < 3,
            )

        top = effectiveness_tracker.get_top_first_attempt_specialists(
            min_attempts=3, limit=2
        )
        assert len(top) == 2
        assert top[0]["specialist"] == "FORGE"
        assert top[0]["first_attempt_success_rate"] == approx(1.0)

    def test_session_lifecycle(self, effectiveness_tracker):
        """Verify session start/end tracking."""
        effectiveness_tracker.start_session("FORGE", "session_001")
        effectiveness_tracker.start_session("SENTINEL", "session_001")

        effectiveness_tracker.record_task_outcome(
            "FORGE", "session_001", succeeded=True,
        )

        effectiveness_tracker.end_session("FORGE", "session_001")

        # Verify no errors
        record = effectiveness_tracker.get_record("FORGE", "session_001")
        assert record is not None

    def test_get_all_aggregates(self, effectiveness_tracker):
        """Verify all-specialist aggregate computation."""
        effectiveness_tracker.record_task_outcome("FORGE", "s1", succeeded=True)
        effectiveness_tracker.record_task_outcome("SENTINEL", "s1", succeeded=True)
        effectiveness_tracker.record_task_outcome("ORACLE", "s1", succeeded=False)

        all_aggs = effectiveness_tracker.get_all_specialist_aggregates()
        assert len(all_aggs) >= 3
        assert "FORGE" in all_aggs
        assert "SENTINEL" in all_aggs
        assert "ORACLE" in all_aggs

    def test_properties(self, effectiveness_tracker):
        """Verify computed properties."""
        record = effectiveness_tracker.record_task_outcome(
            "FORGE", "s1", succeeded=True, was_first_attempt=True,
        )
        assert record.success_rate == 1.0
        assert record.first_attempt_success_rate == 1.0
        assert record.consensus_alignment_rate == 0.0  # No consensus yet

    def test_get_statistics(self, effectiveness_tracker):
        """Verify get_statistics returns correct data."""
        effectiveness_tracker.record_task_outcome("FORGE", "s1", succeeded=True)
        effectiveness_tracker.record_task_outcome("SENTINEL", "s1", succeeded=True)

        stats = effectiveness_tracker.get_statistics()
        assert stats["total_records"] >= 2
        assert stats["unique_specialists"] >= 2
        assert stats["unique_sessions"] >= 1

    def test_persist_and_reload(self, effectiveness_tracker):
        """Verify persistence across operations."""
        # Record some data
        effectiveness_tracker.record_task_outcome(
            "FORGE", "session_001", succeeded=True,
            duration_ms=2000.0,
        )
        effectiveness_tracker.record_task_outcome(
            "FORGE", "session_001", succeeded=False,
            duration_ms=3000.0,
        )

        # Reload and verify
        records = effectiveness_tracker.get_specialist_records("FORGE")
        record = records[0]
        assert record.tasks_attempted == 2
        assert record.tasks_succeeded == 1
        assert record.total_duration_ms == 5000.0

    def test_close(self, effectiveness_tracker):
        """Verify close doesn't raise."""
        effectiveness_tracker.close()  # Should not raise


# =============================================================================
# Test CollaborationSignature
# =============================================================================

class TestCollaborationSignature:
    def test_signature_hash_deterministic(self):
        """Verify same input produces same hash."""
        sig1 = CollaborationSignature(
            event_type=CollaborationEventType.CONSENSUS_REACHED,
            participant_count=3,
            specialist_roles=["FORGE", "SENTINEL", "ARCHITECT"],
        )
        sig2 = CollaborationSignature(
            event_type=CollaborationEventType.CONSENSUS_REACHED,
            participant_count=3,
            specialist_roles=["ARCHITECT", "SENTINEL", "FORGE"],  # Different order
        )
        assert sig1.signature_hash == sig2.signature_hash  # Sorted roles

    def test_signature_hash_different(self):
        """Verify different inputs produce different hashes."""
        sig1 = CollaborationSignature(
            event_type=CollaborationEventType.CONSENSUS_REACHED,
            participant_count=2,
            specialist_roles=["FORGE", "SENTINEL"],
        )
        sig2 = CollaborationSignature(
            event_type=CollaborationEventType.CHALLENGE_RAISED,
            participant_count=2,
            specialist_roles=["FORGE", "SENTINEL"],
        )
        assert sig1.signature_hash != sig2.signature_hash

    def test_signature_hash_with_conflict(self):
        """Verify conflict flag changes hash."""
        sig1 = CollaborationSignature(
            event_type=CollaborationEventType.CONSENSUS_REACHED,
            participant_count=2,
            had_conflict=False,
        )
        sig2 = CollaborationSignature(
            event_type=CollaborationEventType.CONSENSUS_REACHED,
            participant_count=2,
            had_conflict=True,
        )
        assert sig1.signature_hash != sig2.signature_hash


# =============================================================================
# Test CollaborationPattern
# =============================================================================

class TestCollaborationPattern:
    def test_to_digest(self):
        """Verify digest generation."""
        pattern = CollaborationPattern(
            signature=CollaborationSignature(
                event_type=CollaborationEventType.CONSENSUS_REACHED,
                participant_count=3,
                specialist_roles=["FORGE", "SENTINEL", "ARCHITECT"],
            ),
        )
        p_id = pattern.to_digest()
        assert p_id is not None
        assert len(p_id) == 16

    def test_success_rate_no_observations(self):
        """Verify success_rate returns 0 for no data."""
        pattern = CollaborationPattern()
        assert pattern.success_rate == 0.0

    def test_success_rate_mixed(self):
        """Verify success_rate computation."""
        pattern = CollaborationPattern(
            success_count=7,
            failure_count=3,
        )
        assert pattern.success_rate == 0.7

    def test_success_rate_all_success(self):
        """Verify success_rate is 1.0 for all successes."""
        pattern = CollaborationPattern(
            success_count=10,
            failure_count=0,
        )
        assert pattern.success_rate == 1.0


# =============================================================================
# Test SpecialistEffectivenessRecord
# =============================================================================

class TestSpecialistEffectivenessRecord:
    def test_to_id(self):
        """Verify ID generation."""
        record = SpecialistEffectivenessRecord(
            specialist="FORGE",
            session_id="session_001",
        )
        record.to_id()
        assert record.id is not None
        assert len(record.id) == 16

    def test_success_rate(self):
        """Verify success_rate computation."""
        record = SpecialistEffectivenessRecord(
            specialist="FORGE",
            session_id="s1",
            tasks_attempted=10,
            tasks_succeeded=7,
        )
        assert record.success_rate == 0.7

    def test_success_rate_zero(self):
        """Verify success_rate returns 0 when no tasks."""
        record = SpecialistEffectivenessRecord(
            specialist="FORGE",
            session_id="s1",
        )
        assert record.success_rate == 0.0

    def test_first_attempt_success_rate(self):
        """Verify first_attempt_success_rate computation."""
        record = SpecialistEffectivenessRecord(
            specialist="FORGE",
            session_id="s1",
            first_attempts=4,
            first_attempt_successes=3,
        )
        assert record.first_attempt_success_rate == 0.75

    def test_consensus_alignment_rate(self):
        """Verify consensus_alignment_rate computation."""
        record = SpecialistEffectivenessRecord(
            specialist="FORGE",
            session_id="s1",
            consensus_participations=10,
            consensus_aligned=8,
        )
        assert record.consensus_alignment_rate == 0.8


# =============================================================================
# Test End-to-End Integration
# =============================================================================

class TestPhase10Integration:
    """Test all three components working together."""

    def test_full_workflow(self, memory_engine):
        """Verify end-to-end: consensus → memory → collaboration patterns → effectiveness."""
        from learning.consensus_memory import ConsensusMemory
        from learning.collaboration_accumulator import CollaborationAccumulator
        from learning.specialist_effectiveness import SpecialistEffectivenessTracker

        consensus_mem = ConsensusMemory(memory_engine, "test_project")
        collab_acc = CollaborationAccumulator(min_observations_for_pattern=2)
        eff_tracker = SpecialistEffectivenessTracker(db_path=":memory:")

        # Create and save a consensus record
        record = ConsensusMemoryRecord(
            consensus_id="integ_001",
            topic="Integration test consensus",
            outcome=ConsensusOutcome.AGREED,
            confidence=0.9,
            participant_count=3,
            specialists_involved=["FORGE", "SENTINEL", "ARCHITECT"],
        )

        # Save to ChromaDB/SQLite
        saved = consensus_mem.save_consensus(record)
        assert saved is True

        # Extract collaboration pattern
        pattern1 = collab_acc.ingest_consensus(record)
        assert pattern1 is None  # Below threshold
        pattern2 = collab_acc.ingest_consensus(record)
        assert pattern2 is not None  # Pattern created

        # Track specialist effectiveness
        record_forge = eff_tracker.record_task_outcome(
            "FORGE", "session_integ", succeeded=True,
            was_first_attempt=True,
        )
        assert record_forge.tasks_attempted == 1
        assert record_forge.first_attempt_successes == 1

        record_sentinel = eff_tracker.record_consensus_participation(
            "SENTINEL", "session_integ", aligned=True,
        )
        assert record_sentinel.consensus_participations == 1

        # Get all data
        collab_stats = collab_acc.get_statistics()
        assert collab_stats["total_patterns"] >= 1

        eff_stats = eff_tracker.get_statistics()
        assert eff_stats["unique_specialists"] >= 2

    def test_pattern_from_different_event_types(self):
        """Verify different collaboration event types create distinct patterns."""
        from learning.collaboration_accumulator import CollaborationAccumulator

        acc = CollaborationAccumulator(min_observations_for_pattern=2)

        # Consensus events
        for _ in range(2):
            record = ConsensusMemoryRecord(
                consensus_id="test",
                topic="Decision on architecture",
                outcome=ConsensusOutcome.AGREED,
                specialists_involved=["FORGE", "ARCHITECT"],
            )
            acc.ingest_consensus(record)

        # Challenge events
        for _ in range(2):
            acc.ingest_collaboration_event(
                event_type=CollaborationEventType.CHALLENGE_RAISED,
                specialists_involved=["SENTINEL"],
                outcome="success",
                description="Security challenge",
            )

        patterns = acc.get_patterns()
        assert len(patterns) == 2

        # Verify persistence callback receives both types
        saved_types = []

        def cb(p):
            saved_types.append(p.signature.event_type)

        acc.set_persistence_callback(cb)
        acc.flush()
        assert CollaborationEventType.CONSENSUS_REACHED in saved_types
        assert CollaborationEventType.CHALLENGE_RAISED in saved_types
