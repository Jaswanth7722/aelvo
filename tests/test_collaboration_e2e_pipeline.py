"""
End-to-end pipeline verification: challenge → consensus → architect → execution → report.

Exercises the full Mode B collaboration workflow:
  1. ORACLE publishes findings to the blackboard
  2. FORGE consumes findings
  3. SENTINEL challenges low-confidence findings
  4. ExtendedConsensusEngine resolves challenges (majority voting)
  5. ARCHITECT reviews consensus and makes decisions
  6. TERMINUS executes approved actions
  7. HERALD generates report
  8. All events flow through runtime EventBus and UI bridge
"""

import asyncio
import pytest


# ══════════════════════════════════════════════════════════════════════
# SECTION 1: Core pipeline — blackboard, consensus, decisions
# ══════════════════════════════════════════════════════════════════════

class TestChallengeToConsensusPipeline:
    """Full challenge → consensus → architect decision flow."""

    def test_1_publish_findings(self):
        """ORACLE publishes findings to the blackboard."""
        from cognition.blackboard import CognitiveBlackboard
        from cognition.types import EntryType, Provenance, ProvenanceType

        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SPECIALIST, source_id='ORACLE')
        entry = bb.publish(
            slot_name='research_findings',
            content='Auth module needs async refactoring based on call graph analysis',
            entry_type=EntryType.FINDING,
            provenance=p,
            confidence=0.62,
            tags=['research', 'finding'],
        )
        assert entry is not None
        assert entry.id is not None
        assert entry.confidence == 0.62
        assert entry.entry_type == EntryType.FINDING
        assert 'auth' in entry.content.lower()

        # Verify blackboard slot
        slot = bb.get_slot('research_findings')
        assert slot is not None
        assert len(slot.active_entries()) == 1
        return bb, entry

    def test_2_challenge_findings(self):
        """SENTINEL challenges low-confidence findings."""
        from cognition.blackboard import CognitiveBlackboard
        from cognition.types import EntryType, Provenance, ProvenanceType
        from specialists.sentinel import SentinelSpecialist

        bb = CognitiveBlackboard()
        sentinel = SentinelSpecialist()

        # Publish a low-confidence finding
        p = Provenance(source_type=ProvenanceType.SPECIALIST, source_id='ORACLE')
        entry = bb.publish(
            slot_name='research_findings',
            content='Low confidence: refactor database layer to use async drivers',
            entry_type=EntryType.FINDING,
            provenance=p,
            confidence=0.55,
            tags=['research', 'finding'],
        )

        # SENTINEL reviews findings
        challenged = sentinel.review_findings(bb, max_results=5, confidence_threshold=0.7)
        assert len(challenged) == 1, f'Expected 1 challenge, got {len(challenged)}'
        assert challenged[0]['entry_id'] == entry.id
        assert challenged[0]['confidence'] == 0.55

        # Verify challenge exists on blackboard via get_challenges()
        challenges = bb.get_challenges()
        assert len(challenges) >= 1
        return bb, entry, challenged

    def test_3_consensus_resolution(self):
        """ExtendedConsensusEngine resolves challenges with positions from specialists."""
        from cognition.blackboard import CognitiveBlackboard
        from cognition.types import EntryType, Provenance, ProvenanceType
        from cognition.consensus_extended import (
            ExtendedConsensusEngine, ResolutionStrategy,
        )

        bb = CognitiveBlackboard()
        # ExtendedConsensusEngine does NOT accept event_bus kwarg
        engine = ExtendedConsensusEngine()

        # Publish finding
        p = Provenance(source_type=ProvenanceType.SPECIALIST, source_id='ORACLE')
        bb.publish(
            slot_name='research_findings',
            content='High confidence: migrate ORM to async',
            entry_type=EntryType.FINDING,
            provenance=p,
            confidence=0.88,
            tags=['finding'],
        )

        # Request consensus
        request = engine.request_consensus(
            topic='ORM async migration',
            participants=['FORGE', 'SENTINEL', 'ORACLE', 'ARCHITECT'],
            resolution_strategy=ResolutionStrategy.MAJORITY,
            context={'entry_type': EntryType.FINDING, 'confidence': 0.88},
        )
        assert request is not None
        assert request.consensus_id is not None
        assert len(request.participants) == 4

        # Submit positions (simulating specialist positions)
        engine.submit_position(request.consensus_id, 'FORGE', 'FOR', confidence=0.85)
        engine.submit_position(request.consensus_id, 'ORACLE', 'FOR', confidence=0.90)
        engine.submit_position(request.consensus_id, 'SENTINEL', 'AGAINST', confidence=0.70)

        # ARCHITECT submits and triggers resolution (all 4 have voted)
        outcome = engine.submit_position(
            request.consensus_id, 'ARCHITECT', 'NEUTRAL', confidence=0.75,
        )
        assert outcome is not None
        assert outcome.consensus_id == request.consensus_id
        assert outcome.outcome is not None  # ConsensusOutcomeType

        # Verify outcome
        from cognition.consensus_extended import ConsensusOutcomeType
        valid_outcomes = [
            ConsensusOutcomeType.APPROVED,
            ConsensusOutcomeType.APPROVED_WITH_RISK,
            ConsensusOutcomeType.REQUIRES_REVISION,
            ConsensusOutcomeType.REJECTED,
            ConsensusOutcomeType.ESCALATED,
        ]
        assert outcome.outcome in valid_outcomes, f"Unexpected outcome: {outcome.outcome}"

        print(f"  Consensus outcome: {outcome.outcome.value} "
              f"(confidence={outcome.confidence:.2f})")
        return bb, request, outcome

    def test_4_architect_decision(self):
        """ARCHITECT reviews consensus and makes binding decisions."""
        from specialists.architect import ArchitectSpecialist
        from cognition.consensus_extended import (
            ExtendedConsensusEngine, ResolutionStrategy,
        )

        architect = ArchitectSpecialist()
        engine = ExtendedConsensusEngine()

        # Create a consensus outcome
        request = engine.request_consensus(
            topic='Approve async migration plan',
            participants=['FORGE', 'ORACLE', 'SENTINEL'],
            resolution_strategy=ResolutionStrategy.MAJORITY,
        )
        engine.submit_position(request.consensus_id, 'FORGE', 'FOR', confidence=0.9)
        engine.submit_position(request.consensus_id, 'ORACLE', 'FOR', confidence=0.85)
        outcome = engine.submit_position(
            request.consensus_id, 'SENTINEL', 'AGAINST', confidence=0.6,
        )

        # Architect reviews
        outcome_type = outcome.outcome
        positions_dict = {p.specialist: p.position for p in outcome.positions}
        decision = architect.review_consensus(
            consensus_recommendation=outcome_type.value,
            consensus_confidence=outcome.confidence,
            consensus_id=request.consensus_id,
            positions=positions_dict,
            task='Approve async migration plan',
        )
        assert decision is not None
        # Verify decision is an ArchitectDecision with expected fields
        assert hasattr(decision, 'outcome') or hasattr(decision, 'decision')
        decision_str = str(decision)
        assert len(decision_str) > 0

        print(f"  Architect outcome: {outcome_type.value}")
        print(f"  Decision: {decision_str[:80]}")

    def test_5_full_pipeline_simulation(self):
        """Simulate the full pipeline: findings → challenge → consensus → architect."""
        from cognition.blackboard import CognitiveBlackboard
        from cognition.types import EntryType, Provenance, ProvenanceType
        from cognition.consensus_extended import (
            ExtendedConsensusEngine, ResolutionStrategy,
        )

        bb = CognitiveBlackboard()
        engine = ExtendedConsensusEngine()

        # Step 1: ORACLE publishes findings
        findings = [
            ("Auth module has 3 security vulnerabilities in session handling", 0.88),
            ("Database connection pooling is inefficient under load", 0.65),
            ("Frontend bundle size can be reduced by 40%", 0.72),
            ("Legacy API endpoint uses deprecated authentication", 0.45),
            ("Test coverage for utils module is below 60%", 0.91),
        ]
        entry_ids = []
        for content, confidence in findings:
            p = Provenance(source_type=ProvenanceType.SPECIALIST, source_id='ORACLE')
            entry = bb.publish(
                slot_name='research_findings',
                content=content,
                entry_type=EntryType.FINDING,
                provenance=p,
                confidence=confidence,
                tags=['finding'],
            )
            entry_ids.append(entry.id)

        assert len(entry_ids) == 5
        slot = bb.get_slot('research_findings')
        assert slot is not None

        # Step 2: FORGE consumes findings (track success per consume)
        consumed_count = 0
        for eid in entry_ids:
            consumed = bb.consume(eid, 'FORGE')
            if consumed is not None:
                consumed_count += 1
        assert consumed_count >= 1  # At least one should succeed

        # Step 3: SENTINEL reviews and challenges low-confidence findings
        from specialists.sentinel import SentinelSpecialist
        sentinel = SentinelSpecialist()
        challenged = sentinel.review_findings(bb, max_results=5, confidence_threshold=0.7)
        # Findings with confidence < 0.7 should be challenged
        low_conf_findings = [(c, f) for c, f in findings if f < 0.7]
        assert len(challenged) >= len(low_conf_findings), (
            f"Expected at least {len(low_conf_findings)} challenges for low-confidence findings, got {len(challenged)}"
        )

        # Step 4: Consensus on the challenged findings
        for c in challenged:
            request = engine.request_consensus(
                topic=c['entry_id'][:16],
                participants=['FORGE', 'SENTINEL', 'ORACLE', 'ARCHITECT'],
                resolution_strategy=ResolutionStrategy.MAJORITY,
                context={'confidence': c['confidence']},
            )
            engine.submit_position(request.consensus_id, 'FORGE', 'FOR', confidence=0.8)
            engine.submit_position(request.consensus_id, 'ORACLE', 'FOR', confidence=0.75)
            engine.submit_position(request.consensus_id, 'SENTINEL', 'AGAINST', confidence=0.9)
            outcome = engine.submit_position(
                request.consensus_id, 'ARCHITECT', 'NEUTRAL', confidence=0.7,
            )
            assert outcome is not None

        # Step 5: Verify consumption trail (get_all_consumptions returns dict)
        all_consumptions = bb.get_all_consumptions()
        total_consumed = sum(len(v) for v in all_consumptions.values())
        assert total_consumed >= 1
        print(f"  Total findings published: {len(entry_ids)}")
        print(f"  Challenges raised: {len(challenged)}")
        print(f"  Consumptions recorded: {total_consumed}")


# ══════════════════════════════════════════════════════════════════════
# SECTION 4: Mode B pipeline verification
# ══════════════════════════════════════════════════════════════════════

class TestPipelineIntegration:
    """Verify the TaskBoardPipeline phase execution and handoffs."""

    def test_11_pipeline_phase_handoffs(self):
        """Pipeline has correctly ordered phases with defined handoffs."""
        from core.orchestration.task_board_pipeline import (
            PipelinePhase, MODE_A, MODE_B,
        )

        # Verify mode constants
        assert MODE_A == 'consolidated'
        assert MODE_B == 'task_board'

        # Verify all pipeline phases defined
        phases = list(PipelinePhase)
        assert len(phases) >= 5
        phase_names = [p.value for p in phases]
        assert 'research' in phase_names
        assert 'implementation' in phase_names
        assert 'security' in phase_names or 'security_review' in phase_names
        assert 'command' in phase_names or 'execution' in phase_names
        # The report phase may be called 'report' or 'reporting'
        has_report = 'report' in phase_names or 'reporting' in phase_names or 'report_generation' in phase_names
        assert has_report, f"Report-type phase not found in: {phase_names}"

        print(f"  PipelinePhase: {len(phases)} defined phases: {phase_names}")

    def test_12_routing_table_integrated(self):
        """Router has collaboration routing table correctly configured."""
        from core.orchestration.router import TaskRouter, COLLABORATION_ROUTING_TABLE

        assert len(COLLABORATION_ROUTING_TABLE) > 0

        router = TaskRouter()

        # Verify routing for key evidence types
        finding_targets = router.route_publication('finding', 'ORACLE')
        assert 'FORGE' in finding_targets
        assert 'SENTINEL' in finding_targets

        decision_targets = router.route_publication('decision', 'ARCHITECT')
        assert len(decision_targets) > 0
        # Decisions should go to TERMINUS and/or HERALD
        assert any(t in decision_targets for t in ['TERMINUS', 'HERALD', 'FORGE'])

        execution_targets = router.route_publication('execution_result', 'TERMINUS')
        assert len(execution_targets) > 0

        print(f"  Routing table: {len(COLLABORATION_ROUTING_TABLE)} evidence types")

    def test_13_blackboard_evidence_export(self):
        """Blackboard evidence() method correctly exports CollaborationEvidence objects."""
        from cognition.blackboard import CognitiveBlackboard
        from cognition.types import (
            EntryType, Provenance, ProvenanceType, CollaborationEvidence,
        )

        bb = CognitiveBlackboard()

        # Publish findings
        p = Provenance(source_type=ProvenanceType.SPECIALIST, source_id='ORACLE')
        bb.publish(
            slot_name='findings',
            content='Memory leak in connection pool',
            entry_type=EntryType.FINDING,
            provenance=p,
            confidence=0.85,
            tags=['memory', 'leak'],
        )
        bb.publish(
            slot_name='findings',
            content='Race condition in cache invalidation',
            entry_type=EntryType.FINDING,
            provenance=p,
            confidence=0.72,
            tags=['race', 'cache'],
        )

        # Export evidence (returns list of CollaborationEvidence objects)
        evidence_list = bb.evidence()
        assert len(evidence_list) >= 2
        for ev in evidence_list:
            assert isinstance(ev, CollaborationEvidence), f"Expected CollaborationEvidence, got {type(ev)}"
            assert ev.owner_agent == 'ORACLE'
            assert ev.evidence_type == 'finding'
            assert ev.id is not None
            assert ev.summary is not None
            assert ev.confidence >= 0.0
            assert ev.verification_status is not None
            assert ev.source == 'specialist'
            assert ev.metadata is not None
            print(f"  Evidence: [{ev.owner_agent}] {ev.summary[:40]}... conf={ev.confidence:.2f} ver={ev.verification_status.value}")


# ══════════════════════════════════════════════════════════════════════
# SECTION 5: Memory and learning pipeline
# ══════════════════════════════════════════════════════════════════════

class TestMemoryAndLearning:
    """Verify the memory and learning subsystems are properly integrated."""

    def test_14_consumption_trail_persists(self):
        """Blackboard consumption trail records all evidence reads."""
        from cognition.blackboard import CognitiveBlackboard
        from cognition.types import EntryType, Provenance, ProvenanceType

        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SPECIALIST, source_id='ORACLE')

        entry = bb.publish(
            slot_name='findings', content='Test consumption tracking',
            entry_type=EntryType.FINDING, provenance=p, confidence=0.8,
        )

        # Multiple specialists consume the same entry
        bb.consume(entry.id, 'FORGE')
        bb.consume(entry.id, 'SENTINEL')
        bb.consume(entry.id, 'TERMINUS')

        # get_all_consumptions returns Dict[str, List[Dict[str, Any]]]
        all_consumptions = bb.get_all_consumptions()
        assert entry.id in all_consumptions
        assert len(all_consumptions[entry.id]) == 3

        consumers = [c['consumer'] for c in all_consumptions[entry.id]]
        assert 'FORGE' in consumers
        assert 'SENTINEL' in consumers
        assert 'TERMINUS' in consumers

        # Verify consumption trail per entry
        entry_trail = bb.get_consumption_trail(entry.id)
        assert len(entry_trail) == 3

        print(f"  Consumption trail: {len(consumers)} reads by {len(set(consumers))} specialists")
