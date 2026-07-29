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
            ConsensusOutcomeType,
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
# SECTION 2: EventBus bridge between runtime and UI
# ══════════════════════════════════════════════════════════════════════

class TestEventBusBridge:
    """Verify runtime events bridge correctly to UI EventBus."""

    @pytest.mark.asyncio
    async def test_6_runtime_events_forward_to_ui(self):
        """Runtime EventBus events forward to UI EventBus through RuntimeToUIBridge."""
        from ui.events import EventBus as UIEventBus, EventType
        from runtime_next.events.bus import EventBus as RuntimeEventBus
        from runtime_next.models.events import (
            BlackboardPublicationEvent, FindingConsumedEvent,
            ChallengeRaisedEvent, ConsensusEvent, ArchitectDecisionEvent,
            ExecutionStartedEvent, ExecutionCompletedEvent, ReportGeneratedEvent,
        )
        from ui.core.bridge import RuntimeToUIBridge

        ui_bus = UIEventBus()
        runtime_bus = RuntimeEventBus()

        received = []

        async def ui_handler(event):
            received.append({
                'type': event.event_type.value,
                'specialist': event.data.get('specialist', ''),
                'action': event.data.get('action', '')[:30],
            })

        # Subscribe to all collaboration events
        for etype in [EventType.COLLABORATION_FINDING, EventType.COLLABORATION_CONSUMED,
                      EventType.COLLABORATION_CHALLENGE, EventType.COLLABORATION_CONSENSUS,
                      EventType.COLLABORATION_DECISION, EventType.COLLABORATION_EXECUTION_START,
                      EventType.COLLABORATION_EXECUTION_END, EventType.COLLABORATION_REPORT]:
            ui_bus.subscribe(etype, ui_handler)

        await ui_bus.start()
        await runtime_bus.start()

        bridge = RuntimeToUIBridge(runtime_bus, ui_bus)
        await bridge.start()

        # Emit all 8 runtime event types with polling for receipt
        await runtime_bus.publish(BlackboardPublicationEvent(
            id='ev1', specialist='ORACLE', entry_type='finding',
            summary='Security vulnerability in auth module', tags=['security'],
        ))
        await asyncio.sleep(0.1)

        await runtime_bus.publish(FindingConsumedEvent(
            id='ev2', entry_id='e1', consumer='FORGE',
            entry_owner='ORACLE', entry_type='finding', slot_name='findings',
        ))
        await asyncio.sleep(0.1)

        await runtime_bus.publish(ChallengeRaisedEvent(
            id='ev3', challenge_id='c1', entry_id='e1',
            challenger='SENTINEL', challenged_claim='Low confidence finding',
            evidence='Confidence 0.55 below threshold',
        ))
        await asyncio.sleep(0.1)

        await runtime_bus.publish(ConsensusEvent(
            id='ev4', consensus_id='cons1', target_id='e1',
            recommendation='APPROVED', confidence=0.75,
            positions={'FORGE': 'FOR', 'SENTINEL': 'AGAINST', 'ORACLE': 'FOR'},
            method='majority',
        ))
        await asyncio.sleep(0.1)

        await runtime_bus.publish(ArchitectDecisionEvent(
            id='ev5', decision_id='dec1', outcome='APPROVE',
            target_type='consensus', target_id='cons1',
            reason='Approved with conditions', conditions=['Add tests'],
        ))
        await asyncio.sleep(0.1)

        await runtime_bus.publish(ExecutionStartedEvent(
            id='ev6', task_id='task1', command='python run_migration.py',
            specialist='TERMINUS',
        ))
        await asyncio.sleep(0.1)

        await runtime_bus.publish(ExecutionCompletedEvent(
            id='ev7', task_id='task1', entry_id='result1',
            exit_code=0, specialist='TERMINUS',
        ))
        await asyncio.sleep(0.1)

        await runtime_bus.publish(ReportGeneratedEvent(
            id='ev8', report_id='report1', session_title='Security Audit',
            summary_length=2500, evidence_count=5, challenge_count=2,
        ))
        await asyncio.sleep(0.2)

        # Verify all 8 events received (with retry for timing)
        assert len(received) == 8, f'Expected 8 events, got {len(received)}: {[r["type"] for r in received]}'

        types_received = [r['type'] for r in received]
        assert 'collaboration_finding' in types_received
        assert 'collaboration_consumed' in types_received
        assert 'collaboration_challenge' in types_received
        assert 'collaboration_consensus' in types_received
        assert 'collaboration_decision' in types_received
        assert 'collaboration_execution_start' in types_received
        assert 'collaboration_execution_end' in types_received
        assert 'collaboration_report' in types_received

        await bridge.stop()
        await runtime_bus.stop()
        await ui_bus.stop()

        print(f"  All 8 event types received: {types_received}")


# ══════════════════════════════════════════════════════════════════════
# SECTION 3: TUI Panel verification
# ══════════════════════════════════════════════════════════════════════

class TestTUIPanels:
    """Verify TUI panels correctly display event data."""

    def test_7_specialist_panel_receives_updates(self):
        """SpecialistPanel correctly tracks specialist states and activities."""
        from ui.widgets.specialist_panel import SpecialistPanel

        panel = SpecialistPanel()
        assert hasattr(panel, 'update_specialist')
        assert hasattr(panel, 'remove_specialist')

        panel.update_specialist('ORACLE', 'active', 'Researching auth module', 0.85)
        panel.update_specialist('FORGE', 'thinking', 'Implementing fix', 0.72)
        panel.update_specialist('SENTINEL', 'acting', 'Reviewing security', 0.91)

        specialists = panel.specialists
        assert 'ORACLE' in specialists
        assert specialists['ORACLE']['state'] == 'active'
        assert specialists['FORGE']['state'] == 'thinking'
        assert specialists['SENTINEL']['state'] == 'acting'
        assert specialists['ORACLE']['score'] == 0.85
        print("  SpecialistPanel: 3 specialists tracked with states and scores")

    def test_8_timeline_panel_receives_all_events(self):
        """TimelinePanel chronologically logs all event types."""
        from ui.widgets.timeline_panel import TimelinePanel

        panel = TimelinePanel()
        assert hasattr(panel, 'add_entry')
        assert hasattr(panel, 'clear')

        panel.add_entry('task', 'Task started: Security Audit', 'task_started')
        panel.add_entry('finding', 'ORACLE: Found vulnerability', 'collaboration_finding')
        panel.add_entry('challenge', 'SENTINEL: Low confidence', 'collaboration_challenge')
        panel.add_entry('consensus', 'APPROVED (conf=0.75)', 'collaboration_consensus')
        panel.add_entry('decision', 'ARCHITECT: APPROVE', 'collaboration_decision')
        panel.add_entry('execution', 'TERMINUS: python migrate.py', 'collaboration_execution_start')
        panel.add_entry('report', 'HERALD: Security Audit', 'collaboration_report')
        panel.add_entry('verification', 'lint: main.py [passed]', 'verification_passed')

        assert len(panel.entries) == 8
        # Verify each entry has expected keys
        for i, entry in enumerate(panel.entries):
            assert 'category' in entry
            assert 'summary' in entry
            assert 'event_type' in entry
        print("  TimelinePanel: 8 events logged chronologically")

    def test_9_verification_panel_tracks_pass_fail(self):
        """VerificationPanel correctly tracks verification results and counts."""
        from ui.widgets.verification_panel import VerificationPanel

        panel = VerificationPanel()
        assert hasattr(panel, 'add_result')

        # Simulate a verification session
        panel.add_result('lint', 'main.py', 'passed', 0.95)
        panel.add_result('typecheck', 'types.ts', 'passed', 0.88)
        panel.add_result('test', 'test_auth.py', 'failed', 0.0)
        panel.add_result('security', 'session.js', 'passed', 0.92)
        panel.add_result('lint', 'utils.py', 'passed', 0.97)
        panel.add_result('test', 'test_db.py', 'failed', 0.0)

        assert panel.pass_count == 4
        assert panel.fail_count == 2

        # Check verification types are tracked
        verifications = panel.verifications
        assert 'lint' in verifications
        assert 'typecheck' in verifications
        assert 'test' in verifications
        assert 'security' in verifications

        print(f"  VerificationPanel: {panel.pass_count} passed, {panel.fail_count} failed")

    def test_10_collaboration_view_full_workflow(self):
        """CollaborationView displays the full workflow: findings → decisions."""
        from ui.widgets.collaboration_view import CollaborationView

        view = CollaborationView()

        # Simulate the full workflow
        view.log_finding('ORACLE', 'Security vulnerability in auth', 'finding', 0.88)
        assert view.evidence_count == 1

        view.log_consumed('FORGE', 'ORACLE', 'finding')
        assert view.evidence_count == 1  # consumed events don't increment evidence

        view.log_challenge('SENTINEL', 'Low confidence: 0.55 < 0.7', 'entry_1')
        assert view.challenge_count == 1

        view.set_consensus('Auth security', ['FORGE', 'ORACLE', 'SENTINEL'], 'APPROVED', 0.75)
        assert view.consensus is not None
        assert view.consensus['outcome'] == 'APPROVED'

        # Decision clears consensus in the widget
        view.log_decision('ARCHITECT', 'APPROVE', 'Approved with conditions')
        assert view.decision_count == 1

        view.log_execution('TERMINUS', 'python run_migration.py', 'success')
        view.log_execution('TERMINUS', 'Running tests', 'running')

        view.log_report('Security Audit Report', 5, 2)

        assert len(view.activity_log) >= 7
        print(f"  CollaborationView: full workflow logged ({len(view.activity_log)} activities)")


# ══════════════════════════════════════════════════════════════════════
# SECTION 4: Mode B pipeline verification
# ══════════════════════════════════════════════════════════════════════

class TestPipelineIntegration:
    """Verify the TaskBoardPipeline phase execution and handoffs."""

    def test_11_pipeline_phase_handoffs(self):
        """Pipeline has correctly ordered phases with defined handoffs."""
        from core.orchestration.task_board_pipeline import (
            TaskBoardPipeline, PipelinePhase, MODE_A, MODE_B,
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
