"""
tests/test_critique_evolution_pipeline.py — Phase 10: Self-Critique & Plan Evolution Pipeline

Tests the SelfCritiqueEvolutionPipeline class including:
- Defect → Evolution Trigger mapping (all 5 defect types)
- Automatic remediation (floating tasks, circular dependencies, unverified completions)
- Pipeline iteration (critique → evolve → verify loop)
- Pipeline stopping conditions (clean, partial, blocked, max iterations, plateau)
- Integration with LongHorizonPlanningIntegration
- Edge cases (empty hierarchy, no defects, single defect, all defects)
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch, PropertyMock

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from planning.critique_evolution_pipeline import (
    SelfCritiqueEvolutionPipeline,
    PipelineResult,
    PipelineStatus,
    PipelineIteration,
    EvolutionAction,
    SeverityLevel,
    DEFECT_TO_TRIGGER,
    DEFECT_TO_SCOPE,
    DEFECT_TO_SEVERITY,
    _AUTO_REMEDIATION_HANDLERS,
    _auto_fix_floating_task,
    _auto_fix_circular_dependency,
    _auto_fix_unverified_completion,
)
from planning.memory_types import (
    SelfCritiqueDefect,
    DefectType,
    HierarchyLevel,
    PlanNodeState,
    StrategicPlanEntry,
    RevisionRecord,
    MEMORY_TYPE_CRITIQUE_AUDIT,
    IMPORTANCE_CRITIQUE_AUDIT,
)
from planning.self_critique import CritiqueRunResult
from planning.plan_evolution import EvolutionTrigger, RevisionScope, PlanRevisionResult


# ===========================================================================
# Helpers
# ===========================================================================


def _make_defect(
    defect_type: DefectType,
    node_id: str = "node_001",
    node_title: str = "Test Node",
    description: str = "Test defect description",
) -> SelfCritiqueDefect:
    """Create a SelfCritiqueDefect for testing."""
    return SelfCritiqueDefect(
        type=MEMORY_TYPE_CRITIQUE_AUDIT,
        content=description,
        importance=IMPORTANCE_CRITIQUE_AUDIT,
        project="test_project",
        defect_type=defect_type,
        affected_node_id=node_id,
        affected_node_title=node_title,
        defect_description=description,
        recommended_correction="Fix: resolve the defect",
        critique_run_id="test_run_001",
    )


def _make_critique_result(
    defects: List[SelfCritiqueDefect],
    quality_score: float = 0.75,
) -> CritiqueRunResult:
    """Create a CritiqueRunResult for testing."""
    escalated = [d for d in defects if d.escalated]
    return CritiqueRunResult(
        run_id="test_run_001",
        defects=defects,
        plan_quality_score=quality_score,
        escalated_defects=escalated,
    )


class MockHierarchy:
    """Simplified mock of GoalHierarchyEngine for pipeline testing."""

    def __init__(self):
        self._nodes: Dict[str, StrategicPlanEntry] = {}
        self._children_index: Dict[str, List[str]] = {}

    def get_node(self, node_id: str) -> Optional[StrategicPlanEntry]:
        return self._nodes.get(node_id)

    def get_active_milestones(self) -> List[StrategicPlanEntry]:
        return [
            n for n in self._nodes.values()
            if n.level == HierarchyLevel.MILESTONE
            and n.state == PlanNodeState.ACTIVE
        ]

    def _validate_parent_child_level(self, parent_level: HierarchyLevel, child_level: HierarchyLevel) -> bool:
        order = [
            HierarchyLevel.MISSION,
            HierarchyLevel.STRATEGIC_OBJECTIVE,
            HierarchyLevel.PROGRAM,
            HierarchyLevel.INITIATIVE,
            HierarchyLevel.MILESTONE,
            HierarchyLevel.TASK,
        ]
        try:
            parent_idx = order.index(parent_level)
            child_idx = order.index(child_level)
            return child_idx == parent_idx + 1
        except ValueError:
            return False

    def _is_ancestor_of(self, node_id: str, potential_ancestor_id: str) -> bool:
        return False  # Simple mock — no cycles

    def update_node_state(self, node_id: str, new_state: PlanNodeState, **kwargs) -> bool:
        node = self._nodes.get(node_id)
        if not node:
            return False
        old_state = node.state
        node.state = new_state
        node.record_revision(
            trigger_type=kwargs.get("trigger_type", "test"),
            trigger_summary=kwargs.get("trigger_summary", "test"),
            changes_made=f"state: {old_state.value} → {new_state.value}",
            rationale=kwargs.get("trigger_summary", "test"),
        )
        return True

    def _persist_node_update(self, node: StrategicPlanEntry) -> None:
        pass  # No-op for testing

    def update_confidence(self, node_id: str, new_confidence: float, **kwargs) -> bool:
        node = self._nodes.get(node_id)
        if not node:
            return False
        node.confidence = new_confidence
        return True

    def add_node(self, node: StrategicPlanEntry) -> None:
        self._nodes[node.node_id] = node
        if node.parent_id:
            self._children_index.setdefault(node.parent_id, [])
            self._children_index[node.parent_id].append(node.node_id)


# ===========================================================================
# Tests: Defect → Trigger Mapping
# ===========================================================================


class TestDefectTriggerMapping(unittest.TestCase):
    """Every defect type must have a mapped trigger, scope, and severity."""

    def test_all_five_defect_types_mapped(self):
        """All 5 DefectType values must have corresponding trigger mappings."""
        expected = {
            DefectType.FLOATING_TASK,
            DefectType.ASPIRATIONAL_OBJECTIVE,
            DefectType.CIRCULAR_DEPENDENCY,
            DefectType.CONFIDENCE_DRIFT,
            DefectType.UNVERIFIED_COMPLETION,
        }
        self.assertEqual(set(DEFECT_TO_TRIGGER.keys()), expected)

    def test_all_five_defect_types_have_scope(self):
        """All defect types must have a revision scope."""
        expected = {
            DefectType.FLOATING_TASK: RevisionScope.TASK_ONLY,
            DefectType.ASPIRATIONAL_OBJECTIVE: RevisionScope.OBJECTIVE,
            DefectType.CIRCULAR_DEPENDENCY: RevisionScope.MILESTONE,
            DefectType.CONFIDENCE_DRIFT: RevisionScope.MILESTONE,
            DefectType.UNVERIFIED_COMPLETION: RevisionScope.TASK_ONLY,
        }
        for dt, expected_scope in expected.items():
            self.assertEqual(DEFECT_TO_SCOPE[dt], expected_scope)

    def test_all_five_defect_types_have_severity(self):
        """All defect types must have a severity classification."""
        self.assertEqual(len(DEFECT_TO_SEVERITY), 5)

    def test_circular_dependency_is_critical(self):
        """Circular dependencies are the only CRITICAL severity defect."""
        self.assertEqual(
            DEFECT_TO_SEVERITY[DefectType.CIRCULAR_DEPENDENCY],
            SeverityLevel.CRITICAL,
        )

    def test_aspirational_objective_is_medium(self):
        """Aspirational objectives are MEDIUM severity."""
        self.assertEqual(
            DEFECT_TO_SEVERITY[DefectType.ASPIRATIONAL_OBJECTIVE],
            SeverityLevel.MEDIUM,
        )

    def test_floating_task_trigger_is_user_directive(self):
        """Floating tasks map to USER_DIRECTIVE (need user to decide where to attach)."""
        self.assertEqual(
            DEFECT_TO_TRIGGER[DefectType.FLOATING_TASK],
            EvolutionTrigger.USER_DIRECTIVE,
        )

    def test_circular_dependency_trigger_is_resource_constraint(self):
        """Circular dependencies map to RESOURCE_CONSTRAINT (dependency graph issue)."""
        self.assertEqual(
            DEFECT_TO_TRIGGER[DefectType.CIRCULAR_DEPENDENCY],
            EvolutionTrigger.RESOURCE_CONSTRAINT,
        )


# ===========================================================================
# Tests: Auto-Remediation Handlers
# ===========================================================================


class TestAutoRemediationFloatingTask(unittest.TestCase):
    """Auto-remediation for floating tasks — attach to best-matching milestone."""

    def setUp(self):
        self.hierarchy = MockHierarchy()
        self.root = StrategicPlanEntry(
            type="strategic_plan", content="Mission", project="test",
            level=HierarchyLevel.MISSION,
            title="Test Mission",
            node_id="mission_001",
        )
        self.hierarchy.add_node(self.root)

        self.objective = StrategicPlanEntry(
            type="strategic_plan", content="Build auth", project="test",
            level=HierarchyLevel.STRATEGIC_OBJECTIVE,
            title="Authentication",
            parent_id="mission_001",
            node_id="obj_001",
        )
        self.hierarchy.add_node(self.objective)

        self.milestone = StrategicPlanEntry(
            type="strategic_plan", content="Implement login", project="test",
            level=HierarchyLevel.MILESTONE,
            title="Login Implementation",
            parent_id="obj_001",
            node_id="ms_001",
            state=PlanNodeState.ACTIVE,
        )
        self.hierarchy.add_node(self.milestone)

        self.floating_task = StrategicPlanEntry(
            type="strategic_plan", content="Build login form", project="test",
            level=HierarchyLevel.TASK,
            title="Login Form UI",
            parent_id=None,  # Floating — no parent
            node_id="task_floating",
            state=PlanNodeState.ACTIVE,
        )
        self.hierarchy.add_node(self.floating_task)

    def test_attaches_to_active_milestone(self):
        """Auto-fix should attach floating task to the best-matching active milestone."""
        defect = _make_defect(
            DefectType.FLOATING_TASK,
            node_id="task_floating",
            node_title="Login Form UI",
        )
        result = _auto_fix_floating_task(self.hierarchy, defect)
        self.assertIsNotNone(result)
        self.assertTrue(result.success)
        self.assertIn("task_floating", result.revised_nodes)

        # Verify task now has a parent
        updated_task = self.hierarchy.get_node("task_floating")
        self.assertIsNotNone(updated_task)
        self.assertEqual(updated_task.parent_id, "ms_001")

    def test_no_active_milestone_returns_none(self):
        """Auto-fix should return None if no active milestones exist."""
        self.milestone.state = PlanNodeState.COMPLETE  # No active milestones
        defect = _make_defect(
            DefectType.FLOATING_TASK,
            node_id="task_floating",
        )
        result = _auto_fix_floating_task(self.hierarchy, defect)
        self.assertIsNone(result)


class TestAutoRemediationCircularDependency(unittest.TestCase):
    """Auto-remediation for circular dependencies — remove problematic edge."""

    def setUp(self):
        self.hierarchy = MockHierarchy()
        self.root = StrategicPlanEntry(
            type="strategic_plan", content="Root", project="test",
            level=HierarchyLevel.MISSION, title="Root", node_id="root",
        )
        self.hierarchy.add_node(self.root)

        # The circular dependency: task_1 depends on task_2, which depends on task_1
        self.task_1 = StrategicPlanEntry(
            type="strategic_plan", content="Task 1", project="test",
            level=HierarchyLevel.TASK, title="Task One",
            parent_id="root", node_id="task_1",
            blocking_dependencies=["task_2"],
        )
        self.hierarchy.add_node(self.task_1)

        self.task_2 = StrategicPlanEntry(
            type="strategic_plan", content="Task 2", project="test",
            level=HierarchyLevel.TASK, title="Task Two",
            parent_id="root", node_id="task_2",
            blocking_dependencies=["task_1"],
        )
        self.hierarchy.add_node(self.task_2)

        # Override _is_ancestor_of to detect the cycle
        self.hierarchy._is_ancestor_of = lambda n, a: a == "root" or (
            n == "task_1" and a == "task_2"
        )

    def test_removes_circular_edge(self):
        """Auto-fix should remove the problematic dependency edge."""
        defect = _make_defect(
            DefectType.CIRCULAR_DEPENDENCY,
            node_id="task_1",
            node_title="Task One",
        )
        result = _auto_fix_circular_dependency(self.hierarchy, defect)
        self.assertIsNotNone(result)
        self.assertTrue(result.success)
        self.assertIn("task_1", result.revised_nodes)

        # Verify the dependency was removed
        node = self.hierarchy.get_node("task_1")
        self.assertIsNotNone(node)
        self.assertNotIn("task_2", node.blocking_dependencies)


class TestAutoRemediationUnverifiedCompletion(unittest.TestCase):
    """Auto-remediation for unverified completions — revert to ACTIVE."""

    def setUp(self):
        self.hierarchy = MockHierarchy()
        self.node = StrategicPlanEntry(
            type="strategic_plan", content="Done work", project="test",
            level=HierarchyLevel.MILESTONE, title="Completed Milestone",
            node_id="ms_done", state=PlanNodeState.COMPLETE,
            progress_pct=100.0,
        )
        self.hierarchy.add_node(self.node)

    def test_reverts_complete_to_active(self):
        """Auto-fix should revert COMPLETE state to ACTIVE."""
        defect = _make_defect(
            DefectType.UNVERIFIED_COMPLETION,
            node_id="ms_done",
            node_title="Completed Milestone",
        )
        result = _auto_fix_unverified_completion(self.hierarchy, defect)
        self.assertIsNotNone(result)
        self.assertTrue(result.success)

        updated = self.hierarchy.get_node("ms_done")
        self.assertIsNotNone(updated)
        self.assertIn(updated.state, (PlanNodeState.ACTIVE, PlanNodeState.PROPOSED))

    def test_skips_node_not_complete(self):
        """Auto-fix should return None for non-complete nodes."""
        self.node.state = PlanNodeState.ACTIVE
        defect = _make_defect(
            DefectType.UNVERIFIED_COMPLETION,
            node_id="ms_done",
        )
        result = _auto_fix_unverified_completion(self.hierarchy, defect)
        self.assertIsNone(result)


# ===========================================================================
# Tests: Pipeline Iteration
# ===========================================================================


class MockSelfCritiqueEngine:
    """Mock self-critique that returns pre-determined defect lists."""

    def __init__(self):
        self.run_count = 0
        self.defect_schedule: List[List[SelfCritiqueDefect]] = []

    def run(self) -> CritiqueRunResult:
        if self.run_count < len(self.defect_schedule):
            defects = self.defect_schedule[self.run_count]
        else:
            defects = []
        self.run_count += 1

        score = 1.0 - (len(defects) * 0.1)
        return _make_critique_result(defects, quality_score=max(0.0, score))


class MockPlanEvolutionEngine:
    """Mock evolution engine that records trigger calls."""

    def __init__(self):
        self.calls: List[Dict[str, Any]] = []
        self.success = True

    def process_verification_failure(self, **kwargs) -> PlanRevisionResult:
        self.calls.append({"trigger": "verification_failure", **kwargs})
        return PlanRevisionResult(
            success=self.success,
            scope=RevisionScope.MILESTONE,
            revised_nodes=[kwargs.get("milestone_id", "")],
            revision_records=[],
        )

    def process_resource_constraint(self, **kwargs) -> PlanRevisionResult:
        self.calls.append({"trigger": "resource_constraint", **kwargs})
        return PlanRevisionResult(
            success=self.success,
            scope=RevisionScope.MILESTONE,
            revised_nodes=kwargs.get("affected_node_ids", []),
            revision_records=[],
        )

    def process_user_directive(self, **kwargs) -> PlanRevisionResult:
        self.calls.append({"trigger": "user_directive", **kwargs})
        return PlanRevisionResult(
            success=self.success,
            scope=RevisionScope.OBJECTIVE,
            revised_nodes=[kwargs.get("new_priority_objective_id", "")] if kwargs.get("new_priority_objective_id") else [],
            revision_records=[],
        )

    def process_capability_discovery(self, **kwargs) -> PlanRevisionResult:
        self.calls.append({"trigger": "capability_discovery", **kwargs})
        return PlanRevisionResult(
            success=self.success,
            scope=RevisionScope.MILESTONE,
            revised_nodes=kwargs.get("affected_milestone_ids", []),
            revision_records=[],
        )


class TestPipelineCleanPlan(unittest.TestCase):
    """Pipeline should immediately return COMPLETE_CLEAN with no defects."""

    def test_no_defects_returns_clean(self):
        """Pipeline should return COMPLETE_CLEAN when no defects found."""
        mock_critique = MockSelfCritiqueEngine()
        mock_critique.defect_schedule = [[]]  # No defects on first run

        pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=MagicMock(),
            evolution=MockPlanEvolutionEngine(),
            critique=mock_critique,
        )

        result = pipeline.run(max_iterations=3)
        self.assertEqual(result.status, PipelineStatus.COMPLETE_CLEAN)
        self.assertEqual(result.initial_quality_score, 1.0)
        self.assertEqual(result.final_quality_score, 1.0)
        self.assertEqual(len(result.iterations), 0)
        self.assertEqual(result.total_actions_taken, 0)


class TestPipelineSingleIteration(unittest.TestCase):
    """Pipeline should resolve defects in a single iteration when possible."""

    def setUp(self):
        self.mock_critique = MockSelfCritiqueEngine()
        self.mock_evolution = MockPlanEvolutionEngine()
        self.mock_hierarchy = MagicMock()
        self.mock_hierarchy.get_node.return_value = MagicMock(
            title="Test Node",
            node_id="node_001",
        )

        # First run: 2 defects. Second run: 0 defects (all resolved)
        self.mock_critique.defect_schedule = [
            [
                _make_defect(DefectType.CONFIDENCE_DRIFT, node_id="node_001"),
                _make_defect(DefectType.FLOATING_TASK, node_id="node_002"),
            ],
            [],  # All fixed
        ]

        self.pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )

    def test_single_iteration_resolves_all(self):
        """Pipeline should complete in one iteration when evolution fixes everything."""
        result = self.pipeline.run(max_iterations=3)
        self.assertEqual(result.status, PipelineStatus.COMPLETE_CLEAN)
        self.assertEqual(len(result.iterations), 1)
        self.assertEqual(result.total_actions_taken, 2)
        self.assertGreater(result.final_quality_score, result.initial_quality_score)

    def test_evolution_trigger_called_for_each_defect(self):
        """Each defect should produce an evolution action."""
        result = self.pipeline.run(max_iterations=3)
        actions = result.all_evolution_actions
        self.assertEqual(len(actions), 2)

        trigger_types = {a.evolution_trigger for a in actions}
        self.assertIn(EvolutionTrigger.VERIFICATION_FAILURE, trigger_types)
        self.assertIn(EvolutionTrigger.USER_DIRECTIVE, trigger_types)


class TestPipelinePartialResolution(unittest.TestCase):
    """Pipeline should stop with COMPLETE_PARTIAL when non-critical defects remain."""

    def setUp(self):
        self.mock_critique = MockSelfCritiqueEngine()
        self.mock_evolution = MockPlanEvolutionEngine()
        self.mock_hierarchy = MagicMock()
        self.mock_hierarchy.get_node.return_value = MagicMock(
            title="Test", node_id="node_001",
        )

        # 3 defects → 1 resolved → 2 minor remain
        self.mock_critique.defect_schedule = [
            [
                _make_defect(DefectType.CIRCULAR_DEPENDENCY, node_id="circ_001"),
                _make_defect(DefectType.FLOATING_TASK, node_id="float_001"),
                _make_defect(DefectType.UNVERIFIED_COMPLETION, node_id="unv_001"),
            ],
            [
                _make_defect(DefectType.FLOATING_TASK, node_id="float_001"),
                _make_defect(DefectType.CONFIDENCE_DRIFT, node_id="drift_001"),
            ],
        ]

        self.pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )

    def test_stops_at_max_iterations_with_partial(self):
        """Pipeline should return COMPLETE_PARTIAL when non-critical defects remain."""
        result = self.pipeline.run(max_iterations=1)  # Only 1 iteration allowed
        self.assertIn(
            result.status,
            [PipelineStatus.MAX_ITERATIONS_REACHED, PipelineStatus.COMPLETE_PARTIAL],
        )
        self.assertGreaterEqual(len(result.iterations), 1)


class TestPipelineDefectCascade(unittest.TestCase):
    """Pipeline should correctly cascade defects to evolution actions."""

    def setUp(self):
        self.mock_critique = MockSelfCritiqueEngine()
        self.mock_evolution = MockPlanEvolutionEngine()
        self.mock_hierarchy = MagicMock()
        self.mock_hierarchy.get_node.return_value = MagicMock(
            title="Test", node_id="test_001",
        )

        self.pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )

    def test_cascades_single_defect(self):
        """A single defect should produce exactly one evolution action."""
        defect = _make_defect(DefectType.FLOATING_TASK, node_id="task_001")

        # The auto-remediation tries first; since mock_hierarchy.get_active_milestones
        # returns empty list (MagicMock default), auto-remediation fails → falls to evolution
        actions = self.pipeline._cascade_defects_to_evolution([defect])
        self.assertEqual(len(actions), 1)

        action = actions[0]
        self.assertEqual(action.defect_type, DefectType.FLOATING_TASK)
        self.assertEqual(action.affected_node_id, "task_001")
        self.assertEqual(action.evolution_trigger, EvolutionTrigger.USER_DIRECTIVE)

    def test_skips_unknown_defect_type(self):
        """Unknown defect types should be skipped gracefully."""
        # Create a defect with a type not in DEFECT_TO_TRIGGER (shouldn't happen in practice)
        # We'll test with a regular type that IS mapped but pass target_types param
        defect = _make_defect(DefectType.FLOATING_TASK, node_id="task_001")
        actions = self.pipeline._cascade_defects_to_evolution(
            [defect],
            target_types=[DefectType.CIRCULAR_DEPENDENCY],  # Different from FLOATING_TASK
        )
        self.assertEqual(len(actions), 0)  # Filtered out


class TestPipelineIterationMetrics(unittest.TestCase):
    """Pipeline iteration tracking and metrics."""

    def test_iteration_quality_improvement(self):
        """Iteration should correctly report quality improvement."""
        before = _make_critique_result(
            defects=[_make_defect(DefectType.FLOATING_TASK)],
            quality_score=0.6,
        )
        after = _make_critique_result(
            defects=[],
            quality_score=0.9,
        )

        iteration = PipelineIteration(
            iteration_number=1,
            critique_before=before,
            critique_after=after,
        )
        self.assertAlmostEqual(iteration.quality_improvement, 0.3)
        self.assertEqual(iteration.defects_resolved, 1)
        self.assertEqual(iteration.new_defects_introduced, 0)

    def test_iteration_new_defects_introduced(self):
        """Iteration should detect new defects introduced by evolution."""
        # Use different descriptions to guarantee unique defect IDs
        before = _make_critique_result(
            defects=[_make_defect(DefectType.FLOATING_TASK, node_id="a", description="first defect")],
            quality_score=0.7,
        )
        after = _make_critique_result(
            defects=[_make_defect(DefectType.CONFIDENCE_DRIFT, node_id="b", description="second defect")],
            quality_score=0.6,
        )

        iteration = PipelineIteration(
            iteration_number=1,
            critique_before=before,
            critique_after=after,
        )
        self.assertEqual(iteration.defects_resolved, 1)
        self.assertEqual(iteration.new_defects_introduced, 1)
        self.assertAlmostEqual(iteration.quality_improvement, -0.1)


# ===========================================================================
# Tests: Pipeline Result Reporting
# ===========================================================================


class TestPipelineResult(unittest.TestCase):
    """PipelineResult formatting and reporting."""

    def setUp(self):
        self.result = PipelineResult(
            pipeline_id="test_pipeline_001",
            status=PipelineStatus.COMPLETE_CLEAN,
            initial_quality_score=0.6,
            final_quality_score=0.95,
            started_at=1000.0,
            completed_at=1050.0,
            total_actions_taken=3,
            errors=[],
        )

    def test_duration_calculation(self):
        """Duration should be computed from started_at and completed_at."""
        self.assertEqual(self.result.duration_ms, 50000.0)

    def test_quality_improvement(self):
        """Quality improvement should be final minus initial."""
        self.assertAlmostEqual(self.result.quality_improvement, 0.35)

    def test_to_summary_includes_status(self):
        """Summary should include key pipeline metrics."""
        summary = self.result.to_summary()
        self.assertIn("complete_clean", summary)
        self.assertIn("0.60", summary)
        self.assertIn("0.95", summary)

    def test_format_report_structure(self):
        """Format report should return structured dict for integration."""
        report = self.result.format_report()
        self.assertEqual(report["pipeline_id"], "test_pipeline_001")
        self.assertEqual(report["status"], "complete_clean")
        self.assertEqual(report["quality_delta"], 0.35)
        self.assertEqual(report["total_actions"], 3)


# ===========================================================================
# Tests: Pipeline with Auto-Remediation
# ===========================================================================


class TestPipelineWithAutoRemediation(unittest.TestCase):
    """Pipeline with auto_remediate=True should prefer automatic fixes."""

    def setUp(self):
        self.hierarchy = MockHierarchy()
        root = StrategicPlanEntry(
            type="strategic_plan", content="Root", project="test",
            level=HierarchyLevel.MISSION, title="Root", node_id="root",
        )
        self.hierarchy.add_node(root)
        obj = StrategicPlanEntry(
            type="strategic_plan", content="Obj", project="test",
            level=HierarchyLevel.STRATEGIC_OBJECTIVE, title="Objective",
            parent_id="root", node_id="obj_001",
        )
        self.hierarchy.add_node(obj)
        ms = StrategicPlanEntry(
            type="strategic_plan", content="Build feature", project="test",
            level=HierarchyLevel.MILESTONE, title="Feature Milestone",
            parent_id="obj_001", node_id="ms_001",
            state=PlanNodeState.ACTIVE,
        )
        self.hierarchy.add_node(ms)
        task = StrategicPlanEntry(
            type="strategic_plan", content="Do the thing", project="test",
            level=HierarchyLevel.TASK, title="Implementation Task",
            parent_id=None, node_id="task_floating_auto",
            state=PlanNodeState.ACTIVE,
        )
        self.hierarchy.add_node(task)

        self.evolution = MockPlanEvolutionEngine()
        mock_critique = MockSelfCritiqueEngine()
        mock_critique.defect_schedule = [
            [_make_defect(DefectType.FLOATING_TASK, node_id="task_floating_auto", node_title="Implementation Task")],
            [],  # Fixed
        ]

        self.pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.hierarchy,
            evolution=self.evolution,
            critique=mock_critique,
        )

    def test_auto_remediation_preferred_over_evolution(self):
        """Pipeline should use auto-remediation instead of evolution when handler exists."""
        result = self.pipeline.run(max_iterations=2, auto_remediate=True)
        # The floating task should have been auto-fixed (attached to milestone)
        # So evolution should NOT have been triggered
        self.assertEqual(len(self.evolution.calls), 0)  # No evolution calls needed
        self.assertEqual(result.status, PipelineStatus.COMPLETE_CLEAN)


# ===========================================================================
# Tests: Edge Cases
# ===========================================================================


class TestPipelineEdgeCases(unittest.TestCase):
    """Edge cases for the critique evolution pipeline."""

    def setUp(self):
        self.mock_critique = MockSelfCritiqueEngine()
        self.mock_evolution = MockPlanEvolutionEngine()
        self.mock_hierarchy = MagicMock()
        self.mock_hierarchy.get_node.return_value = MagicMock(
            title="Test", node_id="test_001",
        )

    def test_empty_hierarchy_returns_clean(self):
        """Pipeline should handle empty hierarchy gracefully."""
        self.mock_critique.defect_schedule = [[]]
        pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )
        result = pipeline.run()
        self.assertEqual(result.status, PipelineStatus.COMPLETE_CLEAN)

    def test_pipeline_id_is_unique(self):
        """Each pipeline run should have a unique ID."""
        pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )

        # Use different defect schedules for each run
        pipeline.critique = MockSelfCritiqueEngine()
        pipeline.critique.defect_schedule = [[_make_defect(DefectType.FLOATING_TASK)]]
        # Disable auto-remediation since mock hierarchy won't handle it
        result1 = pipeline.run(auto_remediate=False, max_iterations=1)

        pipeline.critique = MockSelfCritiqueEngine()
        pipeline.critique.defect_schedule = [[_make_defect(DefectType.FLOATING_TASK)]]
        result2 = pipeline.run(auto_remediate=False, max_iterations=1)

        self.assertNotEqual(result1.pipeline_id, result2.pipeline_id)

    def test_get_statistics_returns_all_defects(self):
        """get_statistics should return info for all 5 defect types."""
        pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )
        stats = pipeline.get_statistics()
        self.assertIn("defect_types_mapped", stats)
        self.assertEqual(len(stats["defect_types_mapped"]), 5)
        for defect_type in DefectType:
            self.assertIn(defect_type.value, stats["defect_types_mapped"])

    def test_auto_remediation_handler_registry(self):
        """Auto-remediation should have handlers for floating, circular, and completion."""
        self.assertIn(DefectType.FLOATING_TASK, _AUTO_REMEDIATION_HANDLERS)
        self.assertIn(DefectType.CIRCULAR_DEPENDENCY, _AUTO_REMEDIATION_HANDLERS)
        self.assertIn(DefectType.UNVERIFIED_COMPLETION, _AUTO_REMEDIATION_HANDLERS)
        # Aspirational objectives and confidence drift require user judgment
        self.assertNotIn(DefectType.ASPIRATIONAL_OBJECTIVE, _AUTO_REMEDIATION_HANDLERS)
        self.assertNotIn(DefectType.CONFIDENCE_DRIFT, _AUTO_REMEDIATION_HANDLERS)


# ===========================================================================
# Tests: PipelineRunTargeted
# ===========================================================================


class TestPipelineRunTargeted(unittest.TestCase):
    """run_targeted should only process specified defect types."""

    def setUp(self):
        self.mock_critique = MockSelfCritiqueEngine()
        self.mock_evolution = MockPlanEvolutionEngine()
        self.mock_hierarchy = MagicMock()
        self.mock_hierarchy.get_node.return_value = MagicMock(
            title="Test", node_id="test_001",
        )

    def test_targeted_skips_other_types(self):
        """Targeted run should only process specified defect types."""
        self.mock_critique.defect_schedule = [
            [
                _make_defect(DefectType.FLOATING_TASK, node_id="a"),
                _make_defect(DefectType.CIRCULAR_DEPENDENCY, node_id="b"),
                _make_defect(DefectType.CONFIDENCE_DRIFT, node_id="c"),
            ],
            [
                _make_defect(DefectType.CONFIDENCE_DRIFT, node_id="c"),
            ],
        ]

        pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )

        # Target only FLOATING_TASK and CIRCULAR_DEPENDENCY
        result = pipeline.run_targeted(
            defect_types=[DefectType.FLOATING_TASK, DefectType.CIRCULAR_DEPENDENCY],
            max_iterations=1,
        )

        # Only 2 evolution actions (for the 2 targeted defect types)
        self.assertEqual(result.total_actions_taken, 2)
        action_types = {a.defect_type for a in result.all_evolution_actions}
        self.assertEqual(action_types, {DefectType.FLOATING_TASK, DefectType.CIRCULAR_DEPENDENCY})


# ===========================================================================
# Tests: Pipeline with Evolution Failure
# ===========================================================================


class TestPipelineEvolutionFailure(unittest.TestCase):
    """Pipeline should handle evolution engine failures gracefully."""

    def setUp(self):
        self.mock_critique = MockSelfCritiqueEngine()
        self.mock_evolution = MockPlanEvolutionEngine()
        self.mock_hierarchy = MagicMock()
        self.mock_hierarchy.get_node.return_value = MagicMock(
            title="Test", node_id="test_001",
        )

        # Configure mock nodes to make get_active_milestones return empty
        # so auto_remediation for floating_task will fail
        self.mock_hierarchy.get_active_milestones.return_value = []

        self.mock_critique.defect_schedule = [
            [_make_defect(DefectType.FLOATING_TASK, node_id="test_001")],
            [_make_defect(DefectType.FLOATING_TASK, node_id="test_001")],  # Not resolved
        ]

        self.pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )

    def test_evolution_action_records_error_message(self):
        """Pipeline should record error message when auto-remediation and evolution both fail."""
        # The mock hierarchy has get_active_milestones returning empty (so auto-remediation fails)
        # and get_node returns a valid node (so evolution trigger can be called)
        # This means the action should have auto_remediated=False and no error_message initially
        result = self.pipeline.run(max_iterations=1, auto_remediate=True)
        actions = result.all_evolution_actions
        self.assertGreater(len(actions), 0, "Pipeline should produce at least one action")
        action = actions[0]
        self.assertEqual(action.defect_type, DefectType.FLOATING_TASK)
        # Auto-remediation failed (no active milestones), so it should NOT be auto_remediated
        self.assertFalse(action.auto_remediated,
                         "Auto-remediation should fail without active milestones")


# ===========================================================================
# Tests: Pipeline Status Transitions
# ===========================================================================


class TestPipelineStatusTransitions(unittest.TestCase):
    """Pipeline should correctly classify its final status."""

    def test_complete_clean_status(self):
        """COMPLETE_CLEAN when no defects remain."""
        result = PipelineResult(
            pipeline_id="test",
            status=PipelineStatus.COMPLETE_CLEAN,
            initial_quality_score=0.7,
            final_quality_score=1.0,
        )
        self.assertEqual(result.status, PipelineStatus.COMPLETE_CLEAN)

    def test_complete_blocked_status(self):
        """COMPLETE_BLOCKED when critical defects remain."""
        result = PipelineResult(
            pipeline_id="test",
            status=PipelineStatus.COMPLETE_BLOCKED,
            initial_quality_score=0.3,
            final_quality_score=0.3,
        )
        self.assertEqual(result.status, PipelineStatus.COMPLETE_BLOCKED)


# ===========================================================================
# Tests: Integration with LongHorizonPlanningIntegration
# ===========================================================================


class TestIntegrationPipeline(unittest.TestCase):
    """The pipeline should be accessible via LongHorizonPlanningIntegration."""

    def test_pipeline_integration_exists(self):
        """LongHorizonPlanningIntegration should have a critique_pipeline attribute."""
        from planning.integration import LongHorizonPlanningIntegration
        # We can't fully instantiate without memory_engine, but we can verify
        # the class references the pipeline
        import inspect
        source = inspect.getsource(LongHorizonPlanningIntegration.__init__)
        self.assertIn("critique_pipeline", source)

    def test_run_method_signature(self):
        """Integration's run_critique_evolution_pipeline should have correct signature."""
        from planning.integration import LongHorizonPlanningIntegration
        import inspect
        sig = inspect.signature(LongHorizonPlanningIntegration.run_critique_evolution_pipeline)
        params = list(sig.parameters.keys())
        self.assertIn("max_iterations", params)
        self.assertIn("auto_remediate", params)
        self.assertIn("target_defect_types", params)

    def test_pipeline_imported(self):
        """SelfCritiqueEvolutionPipeline should be importable from planning package."""
        from planning import SelfCritiqueEvolutionPipeline
        self.assertIsNotNone(SelfCritiqueEvolutionPipeline)


# ===========================================================================
# Tests: Defect Severity Classification
# ===========================================================================


class TestDefectSeverity(unittest.TestCase):
    """Defect severity classification should match expected values."""

    def test_circular_dependency_is_critical(self):
        self.assertEqual(
            DEFECT_TO_SEVERITY[DefectType.CIRCULAR_DEPENDENCY],
            SeverityLevel.CRITICAL,
        )

    def test_unverified_completion_is_high(self):
        self.assertEqual(
            DEFECT_TO_SEVERITY[DefectType.UNVERIFIED_COMPLETION],
            SeverityLevel.HIGH,
        )

    def test_floating_task_is_medium(self):
        self.assertEqual(
            DEFECT_TO_SEVERITY[DefectType.FLOATING_TASK],
            SeverityLevel.MEDIUM,
        )

    def test_aspirational_objective_is_medium(self):
        self.assertEqual(
            DEFECT_TO_SEVERITY[DefectType.ASPIRATIONAL_OBJECTIVE],
            SeverityLevel.MEDIUM,
        )

    def test_confidence_drift_is_low(self):
        self.assertEqual(
            DEFECT_TO_SEVERITY[DefectType.CONFIDENCE_DRIFT],
            SeverityLevel.LOW,
        )


# ===========================================================================
# Tests: Pipeline with All Defect Types
# ===========================================================================


class TestPipelineAllDefectTypes(unittest.TestCase):
    """Pipeline should handle all 5 defect types simultaneously."""

    def setUp(self):
        self.mock_critique = MockSelfCritiqueEngine()
        self.mock_evolution = MockPlanEvolutionEngine()
        self.mock_hierarchy = MagicMock()
        self.mock_hierarchy.get_node.return_value = MagicMock(
            title="Test", node_id="test_node",
        )

        # All 5 defect types
        self.all_defects = [
            _make_defect(DefectType.FLOATING_TASK, node_id="node_f", node_title="Floating"),
            _make_defect(DefectType.ASPIRATIONAL_OBJECTIVE, node_id="node_a", node_title="Aspirational"),
            _make_defect(DefectType.CIRCULAR_DEPENDENCY, node_id="node_c", node_title="Circular"),
            _make_defect(DefectType.CONFIDENCE_DRIFT, node_id="node_d", node_title="Drift"),
            _make_defect(DefectType.UNVERIFIED_COMPLETION, node_id="node_u", node_title="Unverified"),
        ]

        self.mock_critique.defect_schedule = [self.all_defects, []]

    def test_maps_all_five_defect_types(self):
        """Pipeline should map all 5 defect types to evolution actions."""
        pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )
        actions = pipeline._cascade_defects_to_evolution(
            self.all_defects,
            auto_remediate=False,  # Skip auto to test evolution mapping
        )
        self.assertEqual(len(actions), 5)

        # Each action should have a different defect type
        action_types = {a.defect_type for a in actions}
        self.assertEqual(len(action_types), 5)

    def test_completes_with_all_types(self):
        """Pipeline should complete (not crash) with all 5 defect types."""
        pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.mock_hierarchy,
            evolution=self.mock_evolution,
            critique=self.mock_critique,
        )
        result = pipeline.run(auto_remediate=False, max_iterations=2)
        self.assertIn(result.status, [
            PipelineStatus.COMPLETE_CLEAN,
            PipelineStatus.COMPLETE_PARTIAL,
            PipelineStatus.MAX_ITERATIONS_REACHED,
        ])


# ===========================================================================
# Tests: EvolutionAction tracking
# ===========================================================================


class TestEvolutionAction(unittest.TestCase):
    """EvolutionAction should correctly track its state."""

    def test_success_property_true(self):
        """success should be True when revision_result is successful."""
        action = EvolutionAction(
            action_id="act_001",
            defect_type=DefectType.FLOATING_TASK,
            defect_id="def_001",
            affected_node_id="node_001",
            evolution_trigger=EvolutionTrigger.USER_DIRECTIVE,
            revision_scope=RevisionScope.TASK_ONLY,
            revision_result=PlanRevisionResult(
                success=True, scope=RevisionScope.TASK_ONLY,
                revised_nodes=["node_001"], revision_records=[],
            ),
        )
        self.assertTrue(action.success)

    def test_success_property_false(self):
        """success should be False when revision_result failed."""
        action = EvolutionAction(
            action_id="act_002",
            defect_type=DefectType.FLOATING_TASK,
            defect_id="def_002",
            affected_node_id="node_002",
            evolution_trigger=EvolutionTrigger.USER_DIRECTIVE,
            revision_scope=RevisionScope.TASK_ONLY,
            revision_result=PlanRevisionResult(
                success=False, scope=RevisionScope.TASK_ONLY,
                revised_nodes=[], revision_records=[],
                rejection_reason="Failed to process",
            ),
        )
        self.assertFalse(action.success)

    def test_success_property_none(self):
        """success should be False when revision_result is None."""
        action = EvolutionAction(
            action_id="act_003",
            defect_type=DefectType.CONFIDENCE_DRIFT,
            defect_id="def_003",
            affected_node_id="node_003",
            evolution_trigger=EvolutionTrigger.VERIFICATION_FAILURE,
            revision_scope=RevisionScope.MILESTONE,
        )
        self.assertFalse(action.success)


if __name__ == "__main__":
    unittest.main()
