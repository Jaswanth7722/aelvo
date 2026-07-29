"""Tests for Phase 7 — Extended Consensus Engine (Advisory Role).

Per Amendment 3:
- Consensus is ADVISORY — positions inform, Architect decides
- Architect has final authority: APPROVE, REJECT, ESCALATE, REPLAN, OVERRIDE

Covers:
- ``ConsensusOutcome`` advisory flag and recommendation()
- ``ExtendedConsensusEngine.select_participants()`` — topic-based specialist selection
- All 5 resolution strategies: MAJORITY, SUPERMAJORITY, UNANIMOUS, WEIGHTED, ARCHITECT_DECIDES
- Position submission and auto-resolution
- ``publish_to_blackboard()`` — advisory ConsensusEntry publication
- ``persist_to_sqlite()`` — SQLite persistence
- ``persist_to_chromadb()`` — ChromaDB persistence (graceful if unavailable)
- Full advisory cycle: request → positions → resolve → publish → persist
"""

import json
import os
import pytest
import tempfile

from cognition.consensus_extended import (
    ExtendedConsensusEngine,
    ConsensusOutcome,
    ConsensusPosition,
    ConsensusOutcomeType,
    ResolutionStrategy,
)
from cognition.blackboard import CognitiveBlackboard


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def engine() -> ExtendedConsensusEngine:
    return ExtendedConsensusEngine()


@pytest.fixture
def blackboard() -> CognitiveBlackboard:
    return CognitiveBlackboard()


# ===========================================================================
# ConsensusOutcome — Advisory Role
# ===========================================================================


class TestConsensusOutcomeAdvisory:
    def test_advisory_default_true(self):
        """All ConsensusOutcomes are advisory by default."""
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="test",
            outcome=ConsensusOutcomeType.APPROVED,
        )
        assert outcome.advisory is True
        assert "ADVISORY" in outcome.advisory_note

    def test_recommendation_includes_advisory_note(self):
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="test",
            outcome=ConsensusOutcomeType.APPROVED,
            confidence=0.85,
        )
        rec = outcome.recommendation()
        assert "APPROVED" in rec
        assert "ADVISORY" in rec
        assert "Architect" in rec

    def test_to_advisory_entry_content(self):
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="Should we refactor?",
            outcome=ConsensusOutcomeType.APPROVED,
            positions=[
                ConsensusPosition(specialist="FORGE", position="FOR", confidence=0.9),
                ConsensusPosition(specialist="SENTINEL", position="FOR", confidence=0.8),
            ],
            confidence=0.85,
        )
        content = outcome.to_advisory_entry_content()
        parsed = json.loads(content)
        assert parsed["consensus_id"] == "c1"
        assert parsed["advisory"] is True
        assert "refactor" in parsed["topic"]
        assert parsed["positions"]["FORGE"] == "FOR"

    def test_architect_reviewed_default_false(self):
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="test",
            outcome=ConsensusOutcomeType.APPROVED,
        )
        assert outcome.architect_reviewed is False
        assert outcome.architect_decision_outcome is None

    def test_conditions_dissenting_positions(self):
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="test",
            outcome=ConsensusOutcomeType.APPROVED_WITH_RISK,
            conditions=["security review required"],
            dissenting_positions=[
                ConsensusPosition(specialist="SENTINEL", position="AGAINST",
                                  confidence=0.7, conditions=["security bug"]),
            ],
            confidence=0.6,
        )
        assert len(outcome.conditions) == 1
        assert len(outcome.dissenting_positions) == 1
        rec = outcome.recommendation()
        assert "APPROVED_WITH_RISK" in rec

    def test_to_advisory_entry_content_matches_consensus_entry_schema(self):
        """Verify the output can be parsed by ConsensusEntry schema."""
        from cognition.blackboard_schemas import ConsensusEntry
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="API design",
            outcome=ConsensusOutcomeType.APPROVED,
            positions=[
                ConsensusPosition(specialist="FORGE", position="FOR"),
            ],
            confidence=0.9,
        )
        entry_content = outcome.to_advisory_entry_content()
        # ConsensusEntry.from_entry_content should be able to parse it
        parsed = ConsensusEntry.from_entry_content(entry_content)
        assert parsed.topic == "API design"
        assert parsed.outcome == "APPROVED"
        assert parsed.advisory is True


# ===========================================================================
# Participant Selection
# ===========================================================================


class TestParticipantSelection:
    def test_code_topic_selects_forge(self):
        participants = ExtendedConsensusEngine.select_participants(
            "Should we refactor the authentication module?"
        )
        assert "ARCHITECT" in participants
        assert "FORGE" in participants

    def test_security_topic_selects_sentinel(self):
        participants = ExtendedConsensusEngine.select_participants(
            "Is this API vulnerable to SQL injection?"
        )
        assert "SENTINEL" in participants

    def test_research_topic_selects_oracle(self):
        participants = ExtendedConsensusEngine.select_participants(
            "What is the best library for async HTTP in Python?"
        )
        assert "ORACLE" in participants

    def test_execution_topic_selects_terminus(self):
        participants = ExtendedConsensusEngine.select_participants(
            "Should we deploy to production with Docker Compose?"
        )
        assert "TERMINUS" in participants

    def test_communication_topic_selects_herald(self):
        participants = ExtendedConsensusEngine.select_participants(
            "How should we communicate the delay to stakeholders?"
        )
        assert "HERALD" in participants

    def test_mixed_topic_selects_multiple(self):
        participants = ExtendedConsensusEngine.select_participants(
            "Research and implement a secure authentication API"
        )
        assert "ORACLE" in participants
        assert "FORGE" in participants
        assert "SENTINEL" in participants

    def test_generic_topic_defaults(self):
        participants = ExtendedConsensusEngine.select_participants(
            "Which approach is better?"
        )
        assert "ARCHITECT" in participants
        # Should have default specialists
        assert len(participants) >= 2

    def test_architect_always_included(self):
        participants = ExtendedConsensusEngine.select_participants("")
        assert "ARCHITECT" in participants

    def test_no_duplicates(self):
        participants = ExtendedConsensusEngine.select_participants(
            "Research and implement and deploy a secure API"
        )
        # Count occurrences of each specialist
        from collections import Counter
        counts = Counter(participants)
        for specialist, count in counts.items():
            assert count == 1, f"{specialist} appears {count} times"


# ===========================================================================
# Resolution Strategies
# ===========================================================================


class TestResolutionStrategies:
    def test_majority_approves(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Shall we deploy?",
            participants=["FORGE", "SENTINEL", "ORACLE"],
            resolution_strategy=ResolutionStrategy.MAJORITY,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.8)
        engine.submit_position(req.consensus_id, "SENTINEL", "FOR", confidence=0.9)
        outcome = engine.submit_position(req.consensus_id, "ORACLE", "AGAINST", confidence=0.6)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.APPROVED
        assert outcome.confidence >= 0.5

    def test_majority_rejects(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Deploy now?",
            participants=["FORGE", "SENTINEL", "ORACLE"],
            resolution_strategy=ResolutionStrategy.MAJORITY,
        )
        engine.submit_position(req.consensus_id, "FORGE", "AGAINST", confidence=0.8)
        engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.9)
        outcome = engine.submit_position(req.consensus_id, "ORACLE", "FOR", confidence=0.6)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.REJECTED

    def test_supermajority_approves(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Major API change?",
            participants=["FORGE", "SENTINEL", "ORACLE", "TERMINUS"],
            resolution_strategy=ResolutionStrategy.SUPERMAJORITY,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        engine.submit_position(req.consensus_id, "SENTINEL", "FOR", confidence=0.8)
        engine.submit_position(req.consensus_id, "ORACLE", "FOR", confidence=0.7)
        outcome = engine.submit_position(req.consensus_id, "TERMINUS", "AGAINST", confidence=0.5)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.APPROVED  # 3/4 = 75% > 66.7%

    def test_supermajority_fails(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Risky change?",
            participants=["FORGE", "SENTINEL", "ORACLE"],
            resolution_strategy=ResolutionStrategy.SUPERMAJORITY,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.8)
        outcome = engine.submit_position(req.consensus_id, "ORACLE", "AGAINST", confidence=0.7)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.REJECTED  # 1/3 = 33% < 66.7%

    def test_unanimous_approves(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Trivial change?",
            participants=["FORGE", "SENTINEL"],
            resolution_strategy=ResolutionStrategy.UNANIMOUS,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=1.0)
        outcome = engine.submit_position(req.consensus_id, "SENTINEL", "FOR", confidence=0.9)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.APPROVED

    def test_unanimous_rejects(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Controversial?",
            participants=["FORGE", "SENTINEL"],
            resolution_strategy=ResolutionStrategy.UNANIMOUS,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        outcome = engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.8)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.REJECTED

    def test_weighted_approves(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Should we proceed?",
            participants=["FORGE", "SENTINEL"],
            resolution_strategy=ResolutionStrategy.WEIGHTED,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        outcome = engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.4)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.APPROVED  # 0.9 > 0.4

    def test_weighted_rejects(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="High risk?",
            participants=["FORGE", "SENTINEL"],
            resolution_strategy=ResolutionStrategy.WEIGHTED,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.3)
        outcome = engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.9)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.REJECTED  # 0.9 > 0.3

    def test_architect_decides(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Strategic decision",
            participants=["FORGE", "SENTINEL"],
            resolution_strategy=ResolutionStrategy.ARCHITECT_DECIDES,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        outcome = engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.8)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.APPROVED_WITH_RISK
        assert "Architect decides" in (outcome.final_decision or "")

    def test_requires_revision(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Split decision",
            participants=["FORGE", "SENTINEL", "ORACLE"],
            resolution_strategy=ResolutionStrategy.MAJORITY,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.8)
        outcome = engine.submit_position(req.consensus_id, "ORACLE", "NEUTRAL", confidence=0.5)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.REQUIRES_REVISION
        # 1 FOR, 1 AGAINST, 1 NEUTRAL — no clear majority

    def test_security_conditions_trigger_risk(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Deploy with concerns",
            participants=["FORGE", "SENTINEL"],
            resolution_strategy=ResolutionStrategy.MAJORITY,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        outcome = engine.submit_position(
            req.consensus_id, "SENTINEL", "FOR", confidence=0.7,
            conditions=["security vulnerability in dependency"],
        )
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.APPROVED_WITH_RISK


# ===========================================================================
# Position Submission
# ===========================================================================


class TestPositionSubmission:
    def test_submit_and_auto_resolve(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Quick decision",
            participants=["FORGE", "SENTINEL"],
        )
        # First position — not resolved yet
        result = engine.submit_position(req.consensus_id, "FORGE", "FOR")
        assert result is None  # Still waiting

        # Second position — auto-resolves
        outcome = engine.submit_position(req.consensus_id, "SENTINEL", "FOR")
        assert outcome is not None

    def test_get_positions(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Get positions",
            participants=["FORGE", "SENTINEL"],
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR")
        positions = engine.get_positions(req.consensus_id)
        assert len(positions) == 1
        assert positions[0].specialist == "FORGE"

    def test_get_outcome(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus(
            topic="Get outcome",
            participants=["FORGE", "SENTINEL"],
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR")
        outcome = engine.submit_position(req.consensus_id, "SENTINEL", "FOR")
        assert outcome is not None
        retrieved = engine.get_outcome(req.consensus_id)
        assert retrieved is not None
        assert retrieved.consensus_id == outcome.consensus_id

    def test_active_consensus_list(self, engine: ExtendedConsensusEngine):
        engine.request_consensus("Active 1", ["FORGE"])
        engine.request_consensus("Active 2", ["SENTINEL"])
        active = engine.get_active_consensus()
        assert len(active) == 2

    def test_already_resolved_not_active(self, engine: ExtendedConsensusEngine):
        req = engine.request_consensus("Resolve me", ["FORGE", "SENTINEL"])
        engine.submit_position(req.consensus_id, "FORGE", "FOR")
        engine.submit_position(req.consensus_id, "SENTINEL", "FOR")
        active = engine.get_active_consensus()
        assert len(active) == 0

    def test_unknown_consensus_returns_none(self, engine: ExtendedConsensusEngine):
        result = engine.submit_position("nonexistent", "FORGE", "FOR")
        assert result is None


# ===========================================================================
# Blackboard Publishing
# ===========================================================================


class TestBlackboardPublishing:
    def test_publish_to_blackboard(self, engine: ExtendedConsensusEngine, blackboard: CognitiveBlackboard):
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="Should we refactor?",
            outcome=ConsensusOutcomeType.APPROVED,
            positions=[ConsensusPosition(specialist="FORGE", position="FOR")],
            confidence=0.85,
        )
        entry_id = engine.publish_to_blackboard(outcome, blackboard)
        assert entry_id is not None

        entries = blackboard.read("consensus_outcomes")
        assert len(entries) == 1
        parsed = json.loads(entries[0].content)
        assert parsed["advisory"] is True
        assert parsed["topic"] == "Should we refactor?"

    def test_publish_with_none_blackboard(self, engine: ExtendedConsensusEngine):
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="test",
            outcome=ConsensusOutcomeType.APPROVED,
        )
        entry_id = engine.publish_to_blackboard(outcome, None)
        assert entry_id is None

    def test_publish_multiple_outcomes(self, engine: ExtendedConsensusEngine, blackboard: CognitiveBlackboard):
        for i in range(3):
            outcome = ConsensusOutcome(
                consensus_id=f"c{i}", topic=f"Topic {i}",
                outcome=ConsensusOutcomeType.APPROVED,
            )
            engine.publish_to_blackboard(outcome, blackboard)
        entries = blackboard.read("consensus_outcomes")
        assert len(entries) == 3

    def test_published_entry_matches_advisory_schema(self, engine: ExtendedConsensusEngine, blackboard: CognitiveBlackboard):
        from cognition.blackboard_schemas import ConsensusEntry
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="API v2",
            outcome=ConsensusOutcomeType.APPROVED,
            positions=[
                ConsensusPosition(specialist="FORGE", position="FOR"),
                ConsensusPosition(specialist="SENTINEL", position="FOR"),
            ],
            confidence=0.9,
        )
        engine.publish_to_blackboard(outcome, blackboard)
        entries = blackboard.read("consensus_outcomes")
        assert len(entries) == 1
        restored = ConsensusEntry.from_entry_content(entries[0].content)
        assert restored.topic == "API v2"
        assert restored.advisory is True


# ===========================================================================
# SQLite Persistence
# ===========================================================================


class TestSqlitePersistence:
    def test_persist_to_sqlite(self, engine: ExtendedConsensusEngine):
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="SQLite test",
            outcome=ConsensusOutcomeType.APPROVED,
            confidence=0.85,
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            result = engine.persist_to_sqlite(outcome, db_path=db_path)
            assert result is True

            # Verify data was written
            import sqlite3
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT id, topic, outcome, confidence FROM consensus_history WHERE id=?",
                ("c1",),
            ).fetchone()
            conn.close()
            assert row is not None
            assert row[0] == "c1"
            assert row[1] == "SQLite test"
            assert row[2] == "APPROVED"
            assert row[3] == 0.85
        finally:
            os.unlink(db_path)

    def test_persist_empty_db_path_uses_default(self, engine: ExtendedConsensusEngine):
        outcome = ConsensusOutcome(
            consensus_id="c2", topic="Default path",
            outcome=ConsensusOutcomeType.REJECTED,
        )
        result = engine.persist_to_sqlite(outcome)
        assert result is True
        # Clean up the default db file
        if os.path.exists("consensus_history.db"):
            os.unlink("consensus_history.db")

    def test_persist_multiple_outcomes(self, engine: ExtendedConsensusEngine):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            for i in range(3):
                o = ConsensusOutcome(
                    consensus_id=f"c{i}", topic=f"T{i}",
                    outcome=ConsensusOutcomeType.APPROVED,
                )
                engine.persist_to_sqlite(o, db_path=db_path)

            import sqlite3
            conn = sqlite3.connect(db_path)
            rows = conn.execute("SELECT id FROM consensus_history ORDER BY id").fetchall()
            conn.close()
            assert len(rows) == 3
        finally:
            os.unlink(db_path)

    def test_persist_with_positions(self, engine: ExtendedConsensusEngine):
        outcome = ConsensusOutcome(
            consensus_id="c_pos", topic="With positions",
            outcome=ConsensusOutcomeType.APPROVED,
            positions=[
                ConsensusPosition(specialist="FORGE", position="FOR", confidence=0.9),
                ConsensusPosition(specialist="SENTINEL", position="FOR", confidence=0.8),
            ],
        )
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            result = engine.persist_to_sqlite(outcome, db_path=db_path)
            assert result is True

            import sqlite3
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT positions_json FROM consensus_history WHERE id='c_pos'"
            ).fetchone()
            conn.close()
            positions = json.loads(row[0])
            assert len(positions) == 2
        finally:
            os.unlink(db_path)

    def test_persist_error_returns_false(self, engine: ExtendedConsensusEngine):
        outcome = ConsensusOutcome(
            consensus_id="c_err", topic="Error",
            outcome=ConsensusOutcomeType.APPROVED,
        )
        # Bad path should cause failure
        result = engine.persist_to_sqlite(outcome, db_path="/nonexistent/db/test.db")
        assert result is False


# ===========================================================================
# ChromaDB Persistence
# ===========================================================================


class TestChromaDbPersistence:
    def test_persist_when_unavailable_returns_false(self, engine: ExtendedConsensusEngine):
        """When ChromaDB is not installed, returns False gracefully."""
        outcome = ConsensusOutcome(
            consensus_id="c1", topic="ChromaDB test",
            outcome=ConsensusOutcomeType.APPROVED,
        )
        result = engine.persist_to_chromadb(outcome)
        # Either True (if ChromaDB available) or False (if not)
        assert isinstance(result, bool)

    def test_persist_with_collection(self, engine: ExtendedConsensusEngine):
        """Test with an explicit collection when ChromaDB is available."""
        outcome = ConsensusOutcome(
            consensus_id="c2", topic="Vector test",
            outcome=ConsensusOutcomeType.APPROVED,
            confidence=0.9,
            advisory=True,
        )
        # Try with client — only works if ChromaDB is installed
        try:
            import chromadb
            client = chromadb.Client()
            collection = client.get_or_create_collection("test_consensus")
            result = engine.persist_to_chromadb(outcome, collection=collection)
            assert result is True
            # Verify
            results = collection.get(ids=["c2"])
            assert len(results["ids"]) == 1
            assert "Vector test" in results["documents"][0]
            # Clean up
            client.delete_collection("test_consensus")
        except ImportError:
            assert engine.persist_to_chromadb(outcome) is False


# ===========================================================================
# Full Advisory Cycle
# ===========================================================================


class TestFullAdvisoryCycle:
    def test_full_cycle(self, engine: ExtendedConsensusEngine, blackboard: CognitiveBlackboard):
        """Full advisory cycle: request → submit → resolve → publish → persist."""
        # 1. Specify participants explicitly (ARCHITECT is always included by
        #    select_participants but doesn't vote — it decides)
        topic = "Should we implement OAuth2 authentication with security review?"
        participants = ["FORGE", "SENTINEL", "ORACLE"]

        # 2. Request consensus
        req = engine.request_consensus(
            topic=topic,
            participants=participants,
            resolution_strategy=ResolutionStrategy.MAJORITY,
        )
        assert req.consensus_id is not None
        assert req.topic == topic

        # 3. Submit positions
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        engine.submit_position(req.consensus_id, "SENTINEL", "FOR", confidence=0.8,
                               conditions=["Must use encryption"])
        outcome = engine.submit_position(req.consensus_id, "ORACLE", "FOR", confidence=0.7)
        assert outcome is not None

        # 4. Verify advisory nature
        assert outcome.advisory is True
        assert "ADVISORY" in outcome.advisory_note

        # 5. Publish to blackboard
        entry_id = engine.publish_to_blackboard(outcome, blackboard)
        assert entry_id is not None

        # 6. Verify blackboard entry
        entries = blackboard.read("consensus_outcomes")
        assert len(entries) == 1
        parsed = json.loads(entries[0].content)
        assert parsed["advisory"] is True
        assert "OAuth2" in parsed["topic"]
        assert "ADVISORY" in parsed["recommendation"]
        assert parsed["outcome"] == "APPROVED"

        # 7. Verify outcome is a valid ConsensusEntry
        from cognition.blackboard_schemas import ConsensusEntry
        restored = ConsensusEntry.from_entry_content(entries[0].content)
        assert restored.topic == topic
        assert restored.outcome == "APPROVED"

        # 8. Persist to SQLite
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            persisted = engine.persist_to_sqlite(outcome, db_path=db_path)
            assert persisted is True

            import sqlite3
            conn = sqlite3.connect(db_path)
            row = conn.execute(
                "SELECT outcome, confidence FROM consensus_history WHERE id=?",
                (outcome.consensus_id,),
            ).fetchone()
            conn.close()
            assert row is not None
            assert row[0] == "APPROVED"
        finally:
            os.unlink(db_path)

    def test_advisory_cycle_with_rejection(self, engine: ExtendedConsensusEngine, blackboard: CognitiveBlackboard):
        """Full cycle where consensus rejects, but it's still advisory."""
        req = engine.request_consensus(
            topic="Deploy to production",
            participants=["FORGE", "SENTINEL", "TERMINUS"],
            resolution_strategy=ResolutionStrategy.MAJORITY,
        )
        engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
        engine.submit_position(req.consensus_id, "SENTINEL", "AGAINST", confidence=0.9,
                               conditions=["Critical vulnerability found"])
        outcome = engine.submit_position(req.consensus_id, "TERMINUS", "AGAINST", confidence=0.8)
        assert outcome is not None
        assert outcome.outcome == ConsensusOutcomeType.REJECTED
        assert outcome.advisory is True

        # Publish rejection as advisory
        engine.publish_to_blackboard(outcome, blackboard)
        entries = blackboard.read("consensus_outcomes")
        assert len(entries) == 1
        parsed = json.loads(entries[0].content)
        assert parsed["outcome"] == "REJECTED"
        assert parsed["advisory"] is True

    def test_advisory_flag_on_all_strategies(self, engine: ExtendedConsensusEngine):
        """All resolution strategies produce advisory outcomes."""
        for strategy in ResolutionStrategy:
            req = engine.request_consensus(
                topic=f"Test {strategy.value}",
                participants=["FORGE", "SENTINEL"],
                resolution_strategy=strategy,
            )
            engine.submit_position(req.consensus_id, "FORGE", "FOR", confidence=0.9)
            outcome = engine.submit_position(req.consensus_id, "SENTINEL", "FOR", confidence=0.8)
            assert outcome is not None
            assert outcome.advisory is True, f"{strategy.value} outcome not advisory"
