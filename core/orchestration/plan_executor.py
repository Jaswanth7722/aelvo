"""plan_executor.py — Architect plan integration for AELVO OMEGA."""
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from runtime_next.models.node import NodeDefinition
from runtime_next.engine.engine import ExecutionGraph
from runtime_next.verification.types import (
    VerificationType, VerificationManifest, VerificationScope,
    VerificationResult, Confidence, Severity, Retryability,
)
from runtime_next.plan.architect import ArchitectOrchestrator
from runtime_next.plan.calibration import PlanCalibrationSystem

log = logging.getLogger("aelvo.plan_executor")

_VERIFICATION_METHOD_MAP = {
    "unit_test": VerificationType.UNIT_TEST,
    "integration_test": VerificationType.INTEGRATION_TEST,
    "typecheck": VerificationType.TYPECHECK,
    "lint": VerificationType.LINT,
    "security_scan": VerificationType.SECURITY_SCAN,
    "architecture_check": VerificationType.ARCHITECTURE_VALIDATION,
    "comparison": VerificationType.RUNTIME_VALIDATION,
    "manual_review": None,
}

_STANDARD_VERIFIER_TYPES: set = {
    "lint", "typecheck", "sandbox_validation", "graph_consistency",
}


class PlanExecutor:
    """Manages architect plan creation, execution graph construction, and outcome recording.

    Responsibilities:
    - Lazy-init and cache the ArchitectOrchestrator
    - Create strategic plans with self-critique
    - Build execution graphs from plans
    - Record plan outcomes and verification calibration
    """

    def __init__(self, memory_engine=None, runtime_bus=None,
                 cognitive_engine=None, plan_calibration=None):
        self.memory_engine = memory_engine
        self.runtime_bus = runtime_bus
        self.cognitive_engine = cognitive_engine
        self._plan_calibration = plan_calibration or PlanCalibrationSystem(
            storage_path=None,
        )
        self._architect_orchestrator: Optional[ArchitectOrchestrator] = None

    def get_architect_orchestrator(self) -> Optional[ArchitectOrchestrator]:
        """Lazy-init and return the ArchitectOrchestrator."""
        if self._architect_orchestrator is not None:
            return self._architect_orchestrator

        try:
            forge_memory = None
            if self.cognitive_engine:
                forge_memory = getattr(self.cognitive_engine, '_forge_memory', None)
            if forge_memory is None:
                forge_memory = self.memory_engine

            repo_intel = None
            if self.cognitive_engine:
                repo_intel = getattr(self.cognitive_engine, '_repo_intel', None)

            self._architect_orchestrator = ArchitectOrchestrator(
                repo_intelligence=repo_intel,
                forge_memory=forge_memory,
                event_bus=self.runtime_bus,
            )

            if self.cognitive_engine:
                strategic_memory = getattr(self.cognitive_engine, 'strategic_memory', None)
                if strategic_memory:
                    self._architect_orchestrator.link_strategic_memory(strategic_memory)
                    log.info("Architect linked to CognitiveEngine StrategicMemory")

            log.info("Architect Intelligence orchestrator online")
        except Exception as e:
            log.warning("Architect orchestrator unavailable: %s", e)
            self._architect_orchestrator = None

        return self._architect_orchestrator

    def create_plan(self, task: str, base_path: str = "",
                    get_workspace_tree=None) -> Optional[Any]:
        """Create an Architect Intelligence plan for the task."""
        orchestrator = self.get_architect_orchestrator()
        if orchestrator is None:
            return None

        try:
            project = getattr(self.memory_engine, 'project_name', '') if self.memory_engine else ''

            constraints = {}
            if self.memory_engine:
                try:
                    constraints = self.memory_engine.parse_anchor() or {}
                except Exception:
                    pass

            affected_files = []
            if self.cognitive_engine:
                try:
                    bb = getattr(self.cognitive_engine, 'blackboard', None)
                    if bb and hasattr(bb, 'read'):
                        slots = bb.read('repo_intel_results')
                        if slots:
                            for slot in slots:
                                if hasattr(slot, 'content') and isinstance(slot.content, dict):
                                    affected_files.extend(slot.content.get('files', []))
                except Exception:
                    pass

            tree = get_workspace_tree() if get_workspace_tree else ""

            planning_context = {
                "task": task,
                "constraints": constraints,
                "project": project,
                "tree_snapshot": tree,
                "affected_files": affected_files,
                "relevant_modules": [],
            }

            plan = orchestrator.create_plan(task, planning_context)

            issues = orchestrator.self_critique(plan)
            if issues:
                log.info("Plan %s: %d issues found during critique",
                         plan.id[:12], len(issues))

            log.info(
                "Plan %s: %d phases, %d specialists, score=%.2f",
                plan.id[:12],
                len(plan.execution_strategy.phases),
                len(plan.specialist_assignments.assignments),
                plan.self_review.score,
            )
            return plan
        except Exception as e:
            log.warning("Plan creation failed: %s", e)
            return None

    def extract_specialists_from_plan(self, plan, resolve_order_fn) -> List[str]:
        """Extract ordered specialist names from plan assignments."""
        seen: List[str] = []
        for assignment in plan.specialist_assignments.assignments:
            name = assignment.specialist.value
            if name not in seen:
                seen.append(name)
        return resolve_order_fn(seen)

    def build_graph_from_plan(
        self, plan, task_id: str, effective_input: str,
        runtime_bus, runtime_mutex, runtime_runner,
    ) -> Tuple[ExecutionGraph, Optional[str], Dict[str, str]]:
        """Build an execution graph from plan phases."""
        graph = ExecutionGraph(runtime_bus, runtime_mutex, runner=runtime_runner)
        phase_node_map: Dict[str, str] = {}
        last_node_id: Optional[str] = None

        if not plan.execution_strategy.phases:
            return graph, None, {}

        phase_specialists: Dict[str, str] = {}
        for assignment in plan.specialist_assignments.assignments:
            if assignment.phase_id and assignment.specialist:
                phase_specialists[assignment.phase_id] = assignment.specialist.value

        for phase in plan.execution_strategy.phases:
            specialist_name = phase_specialists.get(phase.id, "FORGE")
            node_id = f"{task_id}_{phase.id}_{specialist_name}"
            node = NodeDefinition(
                id=node_id,
                description=phase.description or effective_input,
                specialist=specialist_name,
            )
            graph.add_node(node)
            phase_node_map[phase.id] = node_id
            last_node_id = node_id

        for edge in plan.execution_strategy.dependency_edges:
            src = phase_node_map.get(edge.source)
            tgt = phase_node_map.get(edge.target)
            if src and tgt:
                graph.add_edge(src, tgt)

        log.info("Graph from plan: %d nodes, %d edges", len(graph.nodes), len(graph.edges))
        return graph, last_node_id, phase_node_map

    def record_plan_outcome(
        self, plan, success: bool, ordered_names: List[str],
        failed_nodes: list, total_duration_ms: float,
        strategy_class: str = "general",
        plan_verification_results: Optional[list] = None,
    ):
        """Record execution outcome for plan calibration and learning."""
        if plan is None:
            return

        lower = (plan.objective.goal or "").lower()
        if any(w in lower for w in ("refactor", "rewrite", "restructure")):
            task_type = "refactor"
        elif any(w in lower for w in ("fix", "bug", "error", "issue")):
            task_type = "fix"
        elif any(w in lower for w in ("add", "create", "implement", "build", "new")):
            task_type = "feature"
        elif any(w in lower for w in ("security", "auth", "vulnerability")):
            task_type = "security"
        else:
            task_type = "general"

        planned_specialists = [a.specialist.value for a in plan.specialist_assignments.assignments]
        actual_specialists = list(set(ordered_names))
        materialized_risks = len(failed_nodes)

        verification_checks_run = len(plan.verification_plan.checks)
        verification_failures_caught = 0
        verification_type_failures: Dict[str, int] = {}

        if plan_verification_results:
            plan_results = [
                r for r in plan_verification_results
                if getattr(r, 'provenance', '').startswith('plan_verification')
            ]
            for vr in plan_results:
                if not vr.success:
                    verification_failures_caught += 1
                    vtype_name = vr.verification_type.value
                    verification_type_failures[vtype_name] = (
                        verification_type_failures.get(vtype_name, 0) + 1
                    )

        unplanned_failures = sum(
            1 for nid, _ in failed_nodes
            if not any(fs.failure_mode in str(nid) for fs in plan.recovery_plan.failure_strategies)
        )

        try:
            self._plan_calibration.record_outcome(
                plan_id=plan.id,
                objective=plan.objective.goal[:200],
                task_type=task_type,
                strategy_class=strategy_class,
                planned_phases=len(plan.execution_strategy.phases),
                completed_phases=len([
                    p for p in plan.execution_strategy.phases
                    if p.id not in [n.split("_")[1] for nid, _ in failed_nodes]
                ]),
                planned_specialists=planned_specialists,
                actual_specialists=actual_specialists,
                planned_risks=len(plan.risks.risks),
                materialized_risks=materialized_risks,
                verification_checks_run=verification_checks_run,
                verification_failures_caught=verification_failures_caught,
                verification_type_failures=verification_type_failures,
                unplanned_failures=unplanned_failures,
                total_duration_ms=total_duration_ms,
                success=success,
            )
            log.info("Calibration recorded for %s: success=%s", plan.id[:12], success)
        except Exception as e:
            log.warning("Failed to record plan outcome: %s", e)

    def record_verification_calibration(
        self, plan: Any,
        all_verification_results: List[Any],
        plan_verification_results: List[Any],
    ):
        """Record verification results as calibration deviations."""
        if not all_verification_results:
            return

        try:
            type_failures: Dict[str, int] = {}
            type_total: Dict[str, int] = {}
            sandbox_failures = 0
            plan_check_failures = 0
            plan_check_total = 0

            for vr in all_verification_results:
                vtype = getattr(vr, 'verification_type', None)
                if vtype is None:
                    continue
                vtype_name = vtype.value if hasattr(vtype, 'value') else str(vtype)
                type_total[vtype_name] = type_total.get(vtype_name, 0) + 1

                if not getattr(vr, 'success', True):
                    type_failures[vtype_name] = type_failures.get(vtype_name, 0) + 1
                    provenance = getattr(vr, 'provenance', '')
                    if 'plan_verification' in provenance:
                        plan_check_failures += 1
                    else:
                        sandbox_failures += 1
                if getattr(vr, 'provenance', '').startswith('plan_verification'):
                    plan_check_total += 1

            if not type_failures:
                return

            calibration = self._plan_calibration
            if not hasattr(calibration, 'record_outcome') or not hasattr(calibration, '_outcomes'):
                return

            vtype_failures_for_outcome: Dict[str, int] = {
                vt: fc for vt, fc in type_failures.items() if fc > 0
            }

            try:
                calibration.record_outcome(
                    plan_id=plan.id + "_vcheck",
                    objective=f"Verification check for {plan.id[:12]}",
                    task_type="verification_analytics",
                    strategy_class="verification",
                    planned_phases=len(type_total),
                    completed_phases=len([
                        t for t, c in type_total.items()
                        if type_failures.get(t, 0) == 0
                    ]),
                    planned_specialists=[],
                    actual_specialists=[],
                    planned_risks=0,
                    materialized_risks=0,
                    verification_checks_run=len(all_verification_results),
                    verification_failures_caught=len(type_failures),
                    verification_type_failures=vtype_failures_for_outcome,
                    unplanned_failures=0,
                    total_duration_ms=0.0,
                    success=len(type_failures) == 0,
                )
                log.info("Verification calibration recorded for %s: %d type failures",
                         plan.id[:12], len(type_failures))
            except Exception:
                pass
        except Exception as e:
            log.warning("Verification calibration failed: %s", e)
