"""Tests for AELVO Cognitive Layer — all 10 phases."""

from datetime import datetime, timedelta, timezone

from cognition.types import (
    Goal, SubGoal, GoalStatus, PlanStep, PlanStatus,
    Provenance, ProvenanceType, EntryType,
    BlackboardEntry, BlackboardSlot,
    ConsensusEvent, ConsensusResult, ConflictRecord, ConflictSeverity,
    ResearchHypothesis, ResearchEvidence, HypothesisStatus,
    StrategicMemoryEntry, MemoryType,
    UncertaintyModel, UncertaintyClass, ExecutionHypothesis, BlockedPath,
    CognitiveStateSnapshot,
)
from cognition.blackboard import CognitiveBlackboard
from cognition.state import CognitiveStateEngine
from cognition.planner import LongHorizonPlanner, DecompositionStrategy
from cognition.strategy_memory import StrategicMemory
from cognition.research import AutonomousResearchRuntime
from cognition.replan import DynamicReplanningEngine, ReplanTrigger, ReplanAction
from cognition.coordination import SpecialistCoordinationRuntime, DelegationMode
from cognition.consensus import MultiAgentConsensusSystem, GovernanceDecision
from cognition.engine import CognitiveEngine

from runtime_next.models.plan import ExecutionPlan, ExecutionNode, NodeType


# =============================================================================
# Phase 1: Foundation Types
# =============================================================================

class TestFoundationTypes:
    def test_goal_creation(self):
        g = Goal(id="g1", description="Test goal", priority=7)
        assert g.id == "g1"
        assert g.priority == 7
        assert g.status == GoalStatus.PENDING
        assert g.created_at is not None

    def test_sub_goal_creation(self):
        sg = SubGoal(id="sg1", parent_goal_id="g1", description="Sub task", order=1)
        assert sg.parent_goal_id == "g1"
        assert sg.order == 1

    def test_plan_step_creation(self):
        ps = PlanStep(id="ps1", plan_id="p1", description="Step", goal_id="g1")
        assert ps.status == PlanStatus.DRAFT

    def test_provenance_defaults(self):
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="test")
        assert p.confidence == 0.5
        assert p.evidence_chain == []

    def test_blackboard_entry(self):
        p = Provenance(source_type=ProvenanceType.TOOL, source_id="tool1")
        e = BlackboardEntry(id="e1", slot_name="test", content="hello",
                            entry_type=EntryType.FACT, provenance=p)
        assert e.confidence == 0.5
        assert e.superseded_by is None

    def test_blackboard_slot_active_entries(self):
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        e1 = BlackboardEntry(id="e1", slot_name="s", content="a",
                             entry_type=EntryType.FACT, provenance=p)
        e2 = BlackboardEntry(id="e2", slot_name="s", content="b",
                             entry_type=EntryType.FACT, provenance=p,
                             expires_at=datetime.now(timezone.utc) - timedelta(hours=1))
        e3 = BlackboardEntry(id="e3", slot_name="s", content="c",
                             entry_type=EntryType.FACT, provenance=p,
                             superseded_by="e4")
        slot = BlackboardSlot(name="s")
        slot.add_entry(e1)
        slot.add_entry(e2)
        slot.add_entry(e3)
        active = slot.active_entries()
        assert len(active) == 1
        assert active[0].id == "e1"

    def test_consensus_event_defaults(self):
        e = ConsensusEvent(id="ce1", topic="test", participants=["A", "B"])
        assert e.result == ConsensusResult.NOT_ATTEMPTED
        assert e.vetoed is False

    def test_conflict_record(self):
        c = ConflictRecord(id="cr1", description="conflict")
        assert c.resolved is False
        assert c.severity == ConflictSeverity.MEDIUM

    def test_research_hypothesis_confidence(self):
        h = ResearchHypothesis(id="rh1", description="test hypothesis")
        assert h.confidence == 0.0
        e1 = ResearchEvidence(id="ev1", description="evidence 1", source="src1",
                              relevance=0.8, reliability=0.9)
        e2 = ResearchEvidence(id="ev2", description="evidence 2", source="src2",
                              relevance=0.6, reliability=0.7)
        h.supporting_evidence.append(e1)
        h.refuting_evidence.append(e2)
        conf = h.compute_confidence()
        assert 0.5 < conf < 0.7

    def test_uncertainty_model(self):
        um = UncertaintyModel()
        um.register_uncertainty("api_design", UncertaintyClass.EVIDENCE_GAP)
        assert "api_design" in um.uncertain_areas
        assert um.is_area_certain("api_design") is False
        um.resolve_uncertainty("api_design", UncertaintyClass.EVIDENCE_GAP)
        assert um.is_area_certain("api_design") is True

    def test_execution_hypothesis(self):
        h = ExecutionHypothesis(id="eh1", description="will work",
                                predicted_outcome="success")
        assert h.status == HypothesisStatus.PROPOSED

    def test_blocked_path(self):
        bp = BlockedPath(id="bp1", step_id="step1", reason="blocked")
        assert bp.resolved is False
        bp.resolved = True
        assert bp.resolved is True

    def test_strategic_memory_entry(self):
        e = StrategicMemoryEntry(id="me1", memory_type=MemoryType.SUCCESS_PATTERN,
                                 content="worked before")
        assert e.consolidation_count == 0
        assert e.importance == 0.5

    def test_cognitive_state_snapshot(self):
        s = CognitiveStateSnapshot(id="snap1")
        assert s.active_goals == []
        assert s.completed_goals == []


# =============================================================================
# Phase 2: Cognitive Blackboard
# =============================================================================

class TestCognitiveBlackboard:
    def test_create_slot(self):
        bb = CognitiveBlackboard()
        slot = bb.create_slot("test_slot")
        assert slot.name == "test_slot"

    def test_get_or_create_slot(self):
        bb = CognitiveBlackboard()
        slot1 = bb.get_or_create_slot("my_slot")
        slot2 = bb.get_or_create_slot("my_slot")
        assert slot1 is slot2

    def test_publish_and_read(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.USER, source_id="user1")
        entry = bb.publish(
            slot_name="facts",
            content="The sky is blue",
            entry_type=EntryType.FACT,
            provenance=p,
            confidence=0.9,
        )
        assert entry.id is not None
        assert entry.slot_name == "facts"

        results = bb.read("facts")
        assert len(results) == 1
        assert results[0].content == "The sky is blue"

    def test_publish_multiple_and_read_latest(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        bb.publish("test", "first", EntryType.FACT, p)
        import time; time.sleep(0.01)
        bb.publish("test", "second", EntryType.FACT, p)
        latest = bb.read_latest("test")
        assert latest is not None
        assert latest.content == "second"

    def test_read_by_type(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        bb.publish("mix", "fact1", EntryType.FACT, p)
        bb.publish("mix", "hyp1", EntryType.HYPOTHESIS, p)
        facts = bb.read("mix", entry_type=EntryType.FACT)
        assert len(facts) == 1
        hyps = bb.read("mix", entry_type=EntryType.HYPOTHESIS)
        assert len(hyps) == 1

    def test_supersede(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        e1 = bb.publish("s", "old", EntryType.FACT, p)
        e2 = bb.publish("s", "new", EntryType.FACT, p)
        bb.supersede(e1.id, e2.id)
        active = bb.read("s")
        assert len(active) == 1
        assert active[0].id == e2.id

    def test_query(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        bb.publish("s1", "python is great", EntryType.FACT, p, tags=["python"])
        bb.publish("s2", "javascript is ok", EntryType.FACT, p, tags=["js"])
        results = bb.query("python")
        assert len(results) >= 1

    def test_conflict_detection(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        bb.publish("constraints", "must not use python 2", EntryType.CONSTRAINT, p, confidence=1.0)
        bb.publish("constraints", "must use python 2", EntryType.CONSTRAINT, p, confidence=1.0)
        conflicts = bb.get_pending_conflicts()
        assert len(conflicts) >= 1

    def test_resolve_conflict(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        bb.publish("c", "must not use X", EntryType.CONSTRAINT, p, confidence=1.0)
        bb.publish("c", "must use X", EntryType.CONSTRAINT, p, confidence=1.0)
        conflicts = bb.get_pending_conflicts()
        assert len(conflicts) > 0
        bb.resolve_conflict(conflicts[0].id, "User confirmed")
        assert bb.get_pending_conflicts() == []

    def test_snapshot(self):
        bb = CognitiveBlackboard()
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        bb.publish("s1", "data", EntryType.FACT, p)
        snap = bb.snapshot()
        assert snap["slot_count"] == 1
        assert snap["active_entry_count"] == 1

    def test_subscribe(self):
        bb = CognitiveBlackboard()
        received = []
        def cb(entry):
            received.append(entry)
        bb.subscribe("watch", cb)
        p = Provenance(source_type=ProvenanceType.SYSTEM, source_id="sys")
        bb.publish("watch", "notify me", EntryType.FACT, p)
        assert len(received) == 1
        assert received[0].content == "notify me"


# =============================================================================
# Phase 3: Cognitive State Engine
# =============================================================================

class TestCognitiveStateEngine:
    def test_register_goal(self):
        se = CognitiveStateEngine()
        g = Goal(id="g1", description="test")
        se.register_goal(g)
        assert se.get_goal("g1") is g

    def test_update_goal_status(self):
        se = CognitiveStateEngine()
        g = Goal(id="g1", description="test")
        se.register_goal(g)
        se.update_goal_status("g1", GoalStatus.IN_PROGRESS)
        assert se.get_goal("g1").status == GoalStatus.IN_PROGRESS

    def test_active_goals(self):
        se = CognitiveStateEngine()
        se.register_goal(Goal(id="g1", description="active", status=GoalStatus.IN_PROGRESS))
        se.register_goal(Goal(id="g2", description="pending"))
        se.register_goal(Goal(id="g3", description="completed", status=GoalStatus.COMPLETED))
        assert len(se.get_active_goals()) == 1
        assert len(se.get_pending_goals()) == 1
        assert len(se.get_completed_goal_ids()) == 1

    def test_register_sub_goal(self):
        se = CognitiveStateEngine()
        g = Goal(id="g1", description="parent")
        se.register_goal(g)
        sg = SubGoal(id="sg1", parent_goal_id="g1", description="child", order=0)
        se.register_sub_goal(sg)
        subs = se.get_sub_goals("g1")
        assert len(subs) == 1
        assert g.sub_goal_ids == ["sg1"]

    def test_blocked_paths(self):
        se = CognitiveStateEngine()
        bp = BlockedPath(id="bp1", step_id="step1", reason="blocked")
        se.add_blocked_path(bp)
        assert len(se.get_active_blocked_paths()) == 1
        se.resolve_blocked_path("bp1")
        assert se.get_active_blocked_paths() == []

    def test_uncertainty(self):
        se = CognitiveStateEngine()
        se.register_uncertainty("design", UncertaintyClass.EVIDENCE_GAP)
        summary = se.get_uncertainty_summary()
        assert "design" in summary
        se.resolve_uncertainty("design", UncertaintyClass.EVIDENCE_GAP)
        assert se.get_uncertainty_summary() == {}

    def test_hypotheses(self):
        se = CognitiveStateEngine()
        h = ExecutionHypothesis(id="eh1", description="test", predicted_outcome="ok")
        se.add_hypothesis(h)
        assert len(se.get_active_hypotheses()) == 1
        se.update_hypothesis_status("eh1", HypothesisStatus.SUPPORTED)
        assert se.get_active_hypotheses() == []

    def test_snapshot(self):
        se = CognitiveStateEngine()
        g = Goal(id="g1", description="test", status=GoalStatus.IN_PROGRESS)
        se.register_goal(g)
        snap = se.snapshot()
        assert len(snap.active_goals) == 1
        assert snap.active_goals[0].id == "g1"

    def test_terminal_display(self):
        se = CognitiveStateEngine()
        g = Goal(id="g1", description="display test", status=GoalStatus.IN_PROGRESS)
        se.register_goal(g)
        display = se.to_terminal_display()
        assert "COGNITIVE STATE" in display
        assert "display test" in display

    def test_check_invariants(self):
        se = CognitiveStateEngine()
        g = Goal(id="g1", description="parent", status=GoalStatus.COMPLETED)
        se.register_goal(g)
        sg = SubGoal(id="sg1", parent_goal_id="g1", description="child", status=GoalStatus.IN_PROGRESS, order=0)
        se.register_sub_goal(sg)
        bp = BlockedPath(id="bp1", step_id="sg1", reason="blocked")
        se.add_blocked_path(bp)
        violations = se.check_invariants()
        assert len(violations) >= 1


# =============================================================================
# Phase 4: Long-Horizon Planning Engine
# =============================================================================

class TestLongHorizonPlanner:
    def test_create_plan(self):
        planner = LongHorizonPlanner()
        goal = Goal(id="g1", description="implement a new feature")
        plan = planner.create_plan(goal, strategy=DecompositionStrategy.TOP_DOWN)
        assert plan.id is not None
        assert len(plan.nodes) > 0
        assert len(plan.edges) >= 0

    def test_create_plan_with_sub_goals(self):
        planner = LongHorizonPlanner()
        goal = Goal(id="g2", description="refactor module")
        sub_goals = [
            SubGoal(id="sg1", parent_goal_id="g2", description="read files", order=0),
            SubGoal(id="sg2", parent_goal_id="g2", description="analyze", order=1),
            SubGoal(id="sg3", parent_goal_id="g2", description="apply changes", order=2),
        ]
        plan = planner.create_plan(goal, sub_goals=sub_goals, strategy=DecompositionStrategy.BOTTOM_UP)
        assert len(plan.nodes) >= 3

    def test_get_plan(self):
        planner = LongHorizonPlanner()
        goal = Goal(id="g3", description="test plan")
        plan = planner.create_plan(goal)
        assert planner.get_plan(plan.id) is plan

    def test_get_plan_steps(self):
        planner = LongHorizonPlanner()
        goal = Goal(id="g4", description="fix bug")
        plan = planner.create_plan(goal)
        steps = planner.get_plan_steps(plan.id)
        assert isinstance(steps, list)

    def test_update_plan(self):
        planner = LongHorizonPlanner()
        goal = Goal(id="g5", description="update test")
        plan = planner.create_plan(goal)
        plan.total_budget = 50
        planner.update_plan(plan.id, plan)
        assert planner.get_plan(plan.id).total_budget == 50

    def test_list_plans(self):
        planner = LongHorizonPlanner()
        g1 = Goal(id="ga", description="plan A")
        g2 = Goal(id="gb", description="plan B")
        planner.create_plan(g1)
        planner.create_plan(g2)
        assert len(planner.list_plans()) == 2

    def test_plan_topological_order(self):
        planner = LongHorizonPlanner()
        goal = Goal(id="g6", description="implement feature")
        plan = planner.create_plan(goal)
        topo = plan.topological_sort()
        assert len(topo) == len(plan.nodes)

    def test_plan_critical_path(self):
        planner = LongHorizonPlanner()
        goal = Goal(id="g7", description="fix critical bug")
        plan = planner.create_plan(goal)
        assert plan.critical_path is not None

    def test_decomposition_strategies(self):
        planner = LongHorizonPlanner()
        for strategy in DecompositionStrategy:
            if strategy == DecompositionStrategy.PATTERN_MATCHED:
                continue
            goal = Goal(id=f"g_{strategy.value}", description=f"test {strategy.value}")
            plan = planner.create_plan(goal, strategy=strategy)
            assert len(plan.nodes) > 0


# =============================================================================
# Phase 5: Strategic Memory Layer
# =============================================================================

class TestStrategicMemory:
    def test_store_and_recall(self):
        sm = StrategicMemory()
        entry = sm.store(MemoryType.SUCCESS_PATTERN, "worked before", importance=0.8)
        results = sm.recall(memory_type=MemoryType.SUCCESS_PATTERN)
        assert len(results) == 1
        assert results[0].id == entry.id

    def test_recall_by_tags(self):
        sm = StrategicMemory()
        sm.store(MemoryType.DOMAIN_KNOWLEDGE, "python knowledge", tags=["python"])
        sm.store(MemoryType.DOMAIN_KNOWLEDGE, "rust knowledge", tags=["rust"])
        results = sm.recall(tags=["python"])
        assert len(results) == 1

    def test_recall_min_importance(self):
        sm = StrategicMemory()
        sm.store(MemoryType.SUCCESS_PATTERN, "low importance", importance=0.2)
        sm.store(MemoryType.SUCCESS_PATTERN, "high importance", importance=0.9)
        results = sm.recall(memory_type=MemoryType.SUCCESS_PATTERN, min_importance=0.5)
        assert len(results) == 1
        assert results[0].importance == 0.9

    def test_search(self):
        sm = StrategicMemory()
        sm.store(MemoryType.DOMAIN_KNOWLEDGE, "python async patterns", tags=["python"])
        sm.store(MemoryType.DOMAIN_KNOWLEDGE, "javascript callbacks", tags=["js"])
        results = sm.search("python")
        assert len(results) >= 1

    def test_get_by_type(self):
        sm = StrategicMemory()
        sm.store(MemoryType.SUCCESS_PATTERN, "sp1")
        sm.store(MemoryType.FAILURE_PATTERN, "fp1")
        sm.store(MemoryType.SUCCESS_PATTERN, "sp2")
        assert len(sm.get_by_type(MemoryType.SUCCESS_PATTERN)) == 2
        assert len(sm.get_by_type(MemoryType.FAILURE_PATTERN)) == 1

    def test_consolidate(self):
        sm = StrategicMemory()
        e1 = sm.store(MemoryType.SUCCESS_PATTERN, "strategy A works", importance=0.7)
        e2 = sm.store(MemoryType.SUCCESS_PATTERN, "strategy B works", importance=0.8)
        consolidated = sm.consolidate([e1.id, e2.id])
        assert consolidated is not None
        assert consolidated.memory_type == MemoryType.SUCCESS_PATTERN
        assert consolidated.consolidation_count >= 1
        assert consolidated.importance >= 0.7

    def test_boost_and_decay(self):
        sm = StrategicMemory()
        entry = sm.store(MemoryType.SUCCESS_PATTERN, "boost test", importance=0.5)
        sm.boost(entry.id, 0.3)
        assert abs(sm._entries[entry.id].importance - 0.8) < 1e-9
        sm.decay(entry.id, 0.2)
        assert abs(sm._entries[entry.id].importance - 0.6) < 1e-9

    def test_snapshot(self):
        sm = StrategicMemory()
        sm.store(MemoryType.SUCCESS_PATTERN, "sp1")
        sm.store(MemoryType.FAILURE_PATTERN, "fp1")
        snap = sm.snapshot()
        assert snap["total_entries"] == 2
        assert snap["by_type"]["success_pattern"] == 1
        assert snap["by_type"]["failure_pattern"] == 1


# =============================================================================
# Phase 6: Autonomous Research Runtime
# =============================================================================

class TestAutonomousResearchRuntime:
    def test_propose_hypothesis(self):
        ar = AutonomousResearchRuntime()
        h = ar.propose_hypothesis("test hypothesis", proposed_by="tester")
        assert h.status == HypothesisStatus.PROPOSED
        assert h.proposed_by == "tester"

    def test_add_supporting_evidence(self):
        ar = AutonomousResearchRuntime()
        h = ar.propose_hypothesis("hypothesis X")
        ar.add_evidence(h.id, "evidence for", "source1", relevance=0.9, reliability=0.8, supports=True)
        assert len(ar.get_hypothesis(h.id).supporting_evidence) == 1

    def test_add_refuting_evidence(self):
        ar = AutonomousResearchRuntime()
        h = ar.propose_hypothesis("hypothesis Y")
        ar.add_evidence(h.id, "evidence against", "source2", relevance=0.8, reliability=0.7, supports=False)
        assert len(ar.get_hypothesis(h.id).refuting_evidence) == 1

    def test_confidence_computation(self):
        ar = AutonomousResearchRuntime()
        h = ar.propose_hypothesis("confidence test")
        ar.add_evidence(h.id, "strong support", "src1", relevance=1.0, reliability=1.0, supports=True)
        ar.add_evidence(h.id, "weak refute", "src2", relevance=0.1, reliability=0.1, supports=False)
        hyp = ar.get_hypothesis(h.id)
        assert hyp.confidence > 0.8

    def test_conclude_supported(self):
        ar = AutonomousResearchRuntime()
        h = ar.propose_hypothesis("conclusion test")
        ar.add_evidence(h.id, "strong support", "src1", relevance=1.0, reliability=1.0, supports=True)
        finding = ar.conclude_hypothesis(h.id)
        assert finding is not None
        assert finding.confidence >= 0.5
        assert "supported" in finding.conclusion.lower()

    def test_find_findings(self):
        ar = AutonomousResearchRuntime()
        h1 = ar.propose_hypothesis("python performance")
        ar.add_evidence(h1.id, "fast", "src", relevance=1.0, reliability=1.0, supports=True)
        ar.conclude_hypothesis(h1.id)
        h2 = ar.propose_hypothesis("rust safety")
        ar.add_evidence(h2.id, "safe", "src", relevance=1.0, reliability=1.0, supports=True)
        ar.conclude_hypothesis(h2.id)
        results = ar.find_findings("python")
        assert len(results) >= 1

    def test_investigate(self):
        ar = AutonomousResearchRuntime()
        h = ar.propose_hypothesis("investigate me")
        ar.investigate(h.id)
        assert ar.get_hypothesis(h.id).status == HypothesisStatus.INVESTIGATING

    def test_snapshot(self):
        ar = AutonomousResearchRuntime()
        ar.propose_hypothesis("h1")
        ar.propose_hypothesis("h2")
        snap = ar.snapshot()
        assert snap["total_hypotheses"] == 2

    def test_get_active_hypotheses(self):
        ar = AutonomousResearchRuntime()
        ar.propose_hypothesis("active h")
        assert len(ar.get_active_hypotheses()) == 1


# =============================================================================
# Phase 7: Dynamic Replanning Engine
# =============================================================================

class TestDynamicReplanningEngine:
    def test_evaluate_node_failure_retry(self):
        engine = DynamicReplanningEngine()
        plan = ExecutionPlan(id="p1", task_description="test plan")
        node = ExecutionNode(id="n1", description="test node",
                             node_type=NodeType.TOOL_CALL,
                             step_consumed=0, estimated_steps=2)
        plan.add_node(node)
        result = engine.evaluate(plan, ReplanTrigger.NODE_FAILURE,
                                 context={"failed_node_id": "n1", "failure_reason": "timeout"})
        assert result is not None
        assert result.action == ReplanAction.RETRY

    def test_try_retry(self):
        engine = DynamicReplanningEngine()
        plan = ExecutionPlan(id="p2", task_description="retry test")
        node = ExecutionNode(id="n1", description="retry node",
                             step_consumed=0)
        plan.add_node(node)
        result = engine.try_retry(plan, "n1")
        assert result is not None
        assert result.action == ReplanAction.RETRY

    def test_try_substitute(self):
        engine = DynamicReplanningEngine()
        plan = ExecutionPlan(id="p3", task_description="substitute test")
        node = ExecutionNode(id="n1", description="old node")
        plan.add_node(node)
        new_node = ExecutionNode(id="n2", description="new node")
        result = engine.try_substitute(plan, "n1", new_node)
        assert result is not None
        assert "n1" in result.removed_node_ids
        assert "n2" in plan.nodes

    def test_try_abort(self):
        engine = DynamicReplanningEngine()
        plan = ExecutionPlan(id="p4", task_description="abort test")
        result = engine.try_abort(plan, "catastrophic failure")
        assert result.action == ReplanAction.ABORT
        assert "catastrophic" in result.description

    def test_try_restructure(self):
        engine = DynamicReplanningEngine()
        plan = ExecutionPlan(id="p5", task_description="restructure test")
        old = ExecutionNode(id="n1", description="old")
        plan.add_node(old)
        new_nodes = [ExecutionNode(id="n2", description="new1"),
                     ExecutionNode(id="n3", description="new2")]
        new_edges = []
        result = engine.try_restructure(plan, new_nodes, new_edges, remove_node_ids=["n1"])
        assert "n1" not in plan.nodes
        assert "n2" in plan.nodes
        assert result.action == ReplanAction.RESTRUCTURE

    def test_get_history(self):
        engine = DynamicReplanningEngine()
        plan = ExecutionPlan(id="p6", task_description="history test")
        engine.try_abort(plan, "reason1")
        engine.try_abort(plan, "reason2")
        assert len(engine.get_history("p6")) == 2

    def test_snapshot(self):
        engine = DynamicReplanningEngine()
        plan = ExecutionPlan(id="p7", task_description="snapshot test")
        engine.try_abort(plan, "fail")
        snap = engine.snapshot()
        assert snap["total_replans"] == 1


# =============================================================================
# Phase 8: Specialist Coordination Runtime
# =============================================================================

class TestSpecialistCoordinationRuntime:
    def test_available_specialists(self):
        coord = SpecialistCoordinationRuntime()
        avail = coord.available_specialists()
        assert len(avail) > 0
        assert "HERMES" in avail

    def test_delegate(self):
        coord = SpecialistCoordinationRuntime()
        node = ExecutionNode(id="n1", description="test task")
        record = coord.delegate(node, "analyze code", mode=DelegationMode.GRAPH_AWARE)
        assert record.node_id == "n1"
        assert record.specialist_name is not None
        assert record.mode == DelegationMode.GRAPH_AWARE

    def test_score_delegation(self):
        coord = SpecialistCoordinationRuntime()
        node = ExecutionNode(id="n1", description="test")
        coord.delegate(node, "task")
        coord.score_delegation("n1", True, 0.9)
        perf = coord.get_specialist_performance(list(coord._registry.keys())[0])
        if perf:
            assert perf["success_rate"] > 0

    def test_list_performance(self):
        coord = SpecialistCoordinationRuntime()
        perfs = coord.list_performance()
        assert isinstance(perfs, list)

    def test_get_specialist(self):
        coord = SpecialistCoordinationRuntime()
        spec = coord.get_specialist("FORGE")
        assert spec is not None

    def test_snapshot(self):
        coord = SpecialistCoordinationRuntime()
        snap = coord.snapshot()
        assert snap["total_delegations"] >= 0
        assert snap["registered_specialists"] > 0

    def test_delegation_confidence_aware(self):
        coord = SpecialistCoordinationRuntime()
        node = ExecutionNode(id="n2", description="confidence test")
        record = coord.delegate(node, "test task", mode=DelegationMode.CONFIDENCE_AWARE)
        assert record.mode == DelegationMode.CONFIDENCE_AWARE


# =============================================================================
# Phase 9: Multi-Agent Consensus System
# =============================================================================

class TestMultiAgentConsensusSystem:
    def test_propose_consensus(self):
        cs = MultiAgentConsensusSystem()
        event = cs.propose_consensus("topic1", ["A", "B", "C"])
        assert event.topic == "topic1"
        assert event.result == ConsensusResult.NOT_ATTEMPTED

    def test_vote_agreed(self):
        cs = MultiAgentConsensusSystem()
        event = cs.propose_consensus("vote test", ["A", "B", "C"])
        cs.vote(event.id, "A", "yes")
        cs.vote(event.id, "B", "yes")
        cs.vote(event.id, "C", "yes")
        assert cs.get_event(event.id).result == ConsensusResult.AGREED

    def test_vote_disagreed(self):
        cs = MultiAgentConsensusSystem()
        event = cs.propose_consensus("split vote", ["A", "B", "C"])
        cs.vote(event.id, "A", "yes")
        cs.vote(event.id, "B", "no")
        cs.vote(event.id, "C", "maybe")
        assert cs.get_event(event.id).result == ConsensusResult.DISAGREED

    def test_resolve_conflict(self):
        cs = MultiAgentConsensusSystem()
        conflict = ConflictRecord(
            id="cr1", description="test conflict",
            severity=ConflictSeverity.MEDIUM,
            involved_specialists=["SENTINEL", "FORGE"],
        )
        event = cs.resolve_conflict(conflict)
        assert event is not None
        resolved = cs.get_resolved_conflicts()
        assert len(resolved) >= 1

    def test_apply_governance(self):
        cs = MultiAgentConsensusSystem()
        event = cs.propose_consensus("gov test", ["A"])
        decision = cs.apply_governance(event.id)
        assert decision in (GovernanceDecision.APPROVED, GovernanceDecision.REQUIRES_REVIEW)

    def test_get_pending_events(self):
        cs = MultiAgentConsensusSystem()
        cs.propose_consensus("pending1", ["A"])
        cs.propose_consensus("pending2", ["B"])
        cs.propose_consensus("pending3", ["C"])
        assert len(cs.get_pending_events()) == 3

    def test_summary(self):
        cs = MultiAgentConsensusSystem()
        cs.propose_consensus("summary test", ["X", "Y"])
        summary = cs.summary()
        assert summary["total_events"] == 1
        assert summary["pending"] == 1


# =============================================================================
# Phase 10: Full Integration
# =============================================================================

class TestCognitiveEngine:
    def test_engine_init(self):
        engine = CognitiveEngine()
        assert engine.blackboard is not None
        assert engine.state is not None
        assert engine.planner is not None
        assert engine.strategic_memory is not None
        assert engine.research is not None
        assert engine.replan is not None
        assert engine.coordination is not None
        assert engine.consensus is not None

    def test_submit_goal(self):
        engine = CognitiveEngine()
        goal = engine.submit_goal("implement login feature", priority=8)
        assert goal.id is not None
        assert goal.description == "implement login feature"
        assert goal.priority == 8

    def test_decompose_goal(self):
        engine = CognitiveEngine()
        goal = engine.submit_goal("build auth system")
        sub_goals = engine.decompose_goal(goal.id, [
            "design database schema",
            "implement JWT tokens",
            "create login endpoint",
        ])
        assert len(sub_goals) == 3
        assert all(sg.parent_goal_id == goal.id for sg in sub_goals)

    def test_plan_goal(self):
        engine = CognitiveEngine()
        goal = engine.submit_goal("add new API endpoint")
        plan = engine.plan_goal(goal.id)
        assert plan.id is not None
        assert len(plan.nodes) > 0

    def test_execute_plan(self):
        engine = CognitiveEngine()
        goal = engine.submit_goal("execute test")
        plan = engine.plan_goal(goal.id)
        order = engine.execute_plan(plan.id)
        assert isinstance(order, list)

    def test_research_topic(self):
        engine = CognitiveEngine()
        hyp = engine.research_topic("Is Python suited for microservices?",
                                    proposed_by="tester", tags=["python", "architecture"])
        assert hyp.id is not None
        assert hyp.status in (HypothesisStatus.PROPOSED, HypothesisStatus.INVESTIGATING)

    def test_conclude_research(self):
        engine = CognitiveEngine()
        hyp = engine.research_topic("test conclusion")
        hyp.supporting_evidence.append(
            ResearchEvidence(id="ev1", description="evidence", source="src",
                             relevance=0.9, reliability=0.8)
        )
        finding = engine.conclude_research(hyp.id)
        if finding:
            assert finding.hypothesis_id == hyp.id

    def test_store_and_recall_memories(self):
        engine = CognitiveEngine()
        entry = engine.store_memory(MemoryType.SUCCESS_PATTERN,
                                    "successfully used dependency injection",
                                    importance=0.8,
                                    tags=["python", "design-patterns"])
        assert entry.id is not None
        results = engine.recall_memories("dependency injection",
                                         memory_type=MemoryType.SUCCESS_PATTERN)
        assert len(results) >= 1

    def test_handle_failure(self):
        engine = CognitiveEngine()
        goal = engine.submit_goal("failure test")
        plan = engine.plan_goal(goal.id)
        plan.add_node(ExecutionNode(
            id="n_fail", description="failing node",
            node_type=NodeType.TOOL_CALL,
            step_consumed=0,
        ))
        result = engine.handle_failure(plan, "n_fail", "timeout")
        if result:
            assert result.action in (ReplanAction.RETRY, ReplanAction.SKIP)

    def test_apply_governance(self):
        engine = CognitiveEngine()
        event = engine.consensus.propose_consensus("governance test", ["A"])
        decision = engine.apply_governance(event.id)
        assert decision in (GovernanceDecision.APPROVED, GovernanceDecision.REQUIRES_REVIEW)

    def test_snapshot(self):
        engine = CognitiveEngine()
        goal = engine.submit_goal("snapshot test")
        engine.decompose_goal(goal.id, ["step1", "step2"])
        engine.plan_goal(goal.id)
        engine.store_memory(MemoryType.DOMAIN_KNOWLEDGE, "test knowledge")
        snap = engine.snapshot()
        assert snap.active_goals is not None
        assert snap.memory_entries_count >= 1
        assert snap.blackboard_slot_count >= 1

    def test_terminal_display(self):
        engine = CognitiveEngine()
        engine.submit_goal("display test")
        display = engine.status()
        assert "state" in display
        assert "blackboard" in display
        assert "planner" in display
        assert "memory" in display
        assert "research" in display
        assert "replan" in display
        assert "coordination" in display
        assert "consensus" in display

    def test_full_workflow(self):
        engine = CognitiveEngine()

        goal = engine.submit_goal("refactor authentication", priority=9)
        engine.decompose_goal(goal.id, [
            "extract auth logic",
            "add unit tests",
            "update imports",
        ])
        plan = engine.plan_goal(goal.id)
        assert len(plan.nodes) >= 3

        engine.research_topic("best practices for auth refactoring",
                                    tags=["auth", "refactoring"])
        engine.store_memory(
            MemoryType.REUSABLE_STRATEGY,
            "Extract interface before refactoring implementation",
            importance=0.9,
            source_goal_id=goal.id,
            tags=["refactoring", "strategy"],
        )

        snap = engine.snapshot()
        assert snap.active_goals is not None
        assert len(snap.active_goals) > 0 or goal.status in (GoalStatus.PENDING, GoalStatus.IN_PROGRESS)
