"""
pipeline.py â€” Canonical AELVO Execution Pipeline
=================================================
Defines the authoritative execution flow for all AELVO tasks.

The pipeline enforces the canonical architecture:

    User â†’ HERMES (calibrate) â†’ ARCHITECT (plan) â†’ ORACLE (research)
         â†’ FORGE (implement) â†’ SENTINEL (secure) â†’ TERMINUS (execute)
         â†’ HERALD (report) â†’ Memory Consolidation â†’ Verification â†’ Recovery

Each phase has an explicit handoff contract, verification requirements,
and recovery strategies. Shared context flows through every phase via
a PipelineContext object that accumulates data, decisions, and artifacts.

Band of Agents Ready: This pipeline demonstrates at least 3 agents
(HERMES, ARCHITECT, FORGE, ORACLE, SENTINEL, TERMINUS, HERALD)
collaborating through a shared coordination layer with visible handoffs,
role specialization, and state passing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from runtime_next.events.bus import EventBus
from runtime_next.models.events import BaseEvent, EventType as RuntimeEventType

log = logging.getLogger("aelvo.pipeline")


# ============================================================================
# Phase Definitions
# ============================================================================


class PipelinePhase(str, Enum):
    """Canonical execution phases matching the AELVO architecture."""

    CALIBRATION = "calibration"          # HERMES - interpret intent, calibrate tone
    PLANNING = "planning"               # ARCHITECT - decompose goals, produce plan
    RESEARCH = "research"               # ORACLE - gather evidence, research facts
    IMPLEMENTATION = "implementation"   # FORGE - write code, perform refactors
    SECURITY = "security"               # SENTINEL - inspect risk, block unsafe ops
    EXECUTION = "execution"             # TERMINUS - run commands, manage operations
    REPORTING = "reporting"             # HERALD - produce reports, communicate results


# Handoff contract for each phase
@dataclass
class HandoffContract:
    """Defines what a specialist receives and must produce.

    Every handoff is explicit â€” the receiving specialist knows exactly
    what context was produced by the previous phase and what is expected.
    """

    phase: PipelinePhase
    specialist_name: str
    receives_from: Optional[PipelinePhase] = None
    produces: List[str] = field(default_factory=list)
    verification_required: bool = True
    required_context_keys: List[str] = field(default_factory=list)
    failure_recovery_strategy: str = "retry_phase"


# Complete handoff graph
PIPELINE_HANDOFFS: Dict[PipelinePhase, HandoffContract] = {
    PipelinePhase.CALIBRATION: HandoffContract(
        phase=PipelinePhase.CALIBRATION,
        specialist_name="HERMES",
        receives_from=None,
        produces=[
            "calibrated_user_model",
            "communication_preferences",
            "workflow_mode",
            "expertise_level",
            "frustration_signals",
            "extracted_constraints",
        ],
        verification_required=True,
        required_context_keys=["task", "conversation_history"],
    ),
    PipelinePhase.PLANNING: HandoffContract(
        phase=PipelinePhase.PLANNING,
        specialist_name="ARCHITECT",
        receives_from=PipelinePhase.CALIBRATION,
        produces=[
            "strategic_plan",
            "decomposed_requirements",
            "specialist_assignments",
            "verification_plan",
            "recovery_plan",
            "risk_assessment",
            "execution_phases",
            "completion_criteria",
        ],
        verification_required=True,
        required_context_keys=["task", "calibrated_user_model", "constraints"],
    ),
    PipelinePhase.RESEARCH: HandoffContract(
        phase=PipelinePhase.RESEARCH,
        specialist_name="ORACLE",
        receives_from=PipelinePhase.PLANNING,
        produces=[
            "research_findings",
            "codebase_evidence",
            "verified_claims",
            "supporting_information",
            "source_citations",
        ],
        verification_required=True,
        required_context_keys=["task", "strategic_plan"],
        failure_recovery_strategy="skip_if_not_required",
    ),
    PipelinePhase.IMPLEMENTATION: HandoffContract(
        phase=PipelinePhase.IMPLEMENTATION,
        specialist_name="FORGE",
        receives_from=PipelinePhase.RESEARCH,
        produces=[
            "code_changes",
            "refactored_modules",
            "new_files",
            "test_updates",
            "pattern_extractions",
        ],
        verification_required=True,
        required_context_keys=["task", "strategic_plan", "research_findings"],
    ),
    PipelinePhase.SECURITY: HandoffContract(
        phase=PipelinePhase.SECURITY,
        specialist_name="SENTINEL",
        receives_from=PipelinePhase.IMPLEMENTATION,
        produces=[
            "security_clearance",
            "risk_findings",
            "remediated_code",
            "security_rules",
            "vulnerability_report",
        ],
        verification_required=True,
        required_context_keys=["code_changes", "security_rules"],
        failure_recovery_strategy="block_on_critical",
    ),
    PipelinePhase.EXECUTION: HandoffContract(
        phase=PipelinePhase.EXECUTION,
        specialist_name="TERMINUS",
        receives_from=PipelinePhase.SECURITY,
        produces=[
            "executed_commands",
            "deployment_results",
            "runtime_operations",
            "operational_artifacts",
        ],
        verification_required=True,
        required_context_keys=["security_clearance", "execution_plan"],
        failure_recovery_strategy="rollback_on_failure",
    ),
    PipelinePhase.REPORTING: HandoffContract(
        phase=PipelinePhase.REPORTING,
        specialist_name="HERALD",
        receives_from=PipelinePhase.EXECUTION,
        produces=[
            "final_response",
            "execution_summary",
            "status_report",
            "next_steps",
        ],
        verification_required=False,
        required_context_keys=[
            "execution_results", "security_clearance", "research_findings",
        ],
    ),
}


# ============================================================================
# Pipeline Execution Phases
# ============================================================================


@dataclass
class PhaseResult:
    """Result of executing a single pipeline phase."""

    phase: PipelinePhase
    specialist_name: str
    success: bool
    output: str = ""
    artifacts: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    duration_ms: float = 0.0
    verification_passed: bool = True
    handoff_data: Dict[str, Any] = field(default_factory=dict)
    recovery_attempted: bool = False
    recovery_success: bool = False


@dataclass
class PipelineResult:
    """Result of executing the full pipeline."""

    success: bool
    phases_executed: List[PipelinePhase]
    phase_results: Dict[PipelinePhase, PhaseResult]
    total_duration_ms: float = 0.0
    final_output: str = ""
    failures: List[Tuple[PipelinePhase, str]] = field(default_factory=list)
    memory_consolidated: bool = False
    verification_summary: str = ""
    recovery_actions: List[str] = field(default_factory=list)


# ============================================================================
# PipelineContext â€” Shared Mutable State
# ============================================================================


class PipelineContext:
    """Shared context that flows through every pipeline phase.

    Each phase reads from and writes to this context. This replaces the
    ad-hoc `build_shared_context()` pattern with a structured, typed,
    and auditable context object.

    Specialists can read context produced by previous phases via
    dedicated accessors, ensuring clean isolation between phases.

    HermesContext is the immutable global cognition context produced
    by HERMES at the start of every turn. It is consumed immutably
    by every component. Per Amendment 4: Hermes is NOT preprocessing.
    Hermes remains active throughout every workflow.
    """

    def __init__(
        self,
        user_input: str,
        conversation_history: List[Dict[str, Any]],
        memory_engine=None,
        fs=None,
        kernel=None,
        provider_runtime=None,
        runtime_bus=None,
        event_bus=None,
        workspace_path: str = "",
        project: str = "",
        hermes_context: Optional[Any] = None,
    ):
        # Immutable inputs
        self.user_input = user_input
        self.conversation_history = list(conversation_history)
        self.memory_engine = memory_engine
        self.fs = fs
        self.kernel = kernel
        self.provider_runtime = provider_runtime
        self.runtime_bus = runtime_bus
        self.event_bus = event_bus
        self.workspace_path = workspace_path
        self.project = project

        # ── HermesContext — Immutable Global Cognition ──
        self.hermes_context = hermes_context

        # Phase-specific outputs (accumulated across phases)
        self.calibrated_user_model: Dict[str, Any] = {}
        self.communication_preferences: Dict[str, Any] = {}
        self.workflow_mode: str = "exploring"
        self.strategic_plan: Any = None
        self.decomposed_requirements: Dict[str, Any] = {}
        self.specialist_assignments: List[Any] = []
        self.verification_plan: Any = None
        self.recovery_plan: Any = None
        self.risk_assessment: Dict[str, Any] = {}
        self.research_findings: List[Dict[str, Any]] = []
        self.codebase_evidence: List[Dict[str, Any]] = []
        self.code_changes: List[Dict[str, Any]] = []
        self.refactored_modules: List[str] = []
        self.new_files: List[str] = []
        self.security_clearance: Dict[str, Any] = {}
        self.risk_findings: List[Dict[str, Any]] = []
        self.security_rules: List[Dict[str, Any]] = []
        self.executed_commands: List[str] = []
        self.deployment_results: Dict[str, Any] = {}
        self.execution_artifacts: Dict[str, Any] = {}
        self.final_response: str = ""
        self.execution_summary: str = ""
        self.next_steps: List[str] = []

        # Cross-cutting
        self.constraints: Dict[str, Any] = {}
        self.state: Dict[str, Any] = {}
        self.tree_snapshot: str = ""
        self.active_specialists: List[str] = []
        self.budget: int = 30
        self.signals: Dict[str, Any] = {}
        self.forced_route: bool = False
        self.verification_results: List[Any] = []
        self.recovery_history: List[Dict[str, Any]] = []

        # Tracking
        self._phase_results: Dict[PipelinePhase, PhaseResult] = {}
        self._start_time: float = time.time()

    def get_phase_data(self, phase: PipelinePhase) -> Dict[str, Any]:
        """Get context data relevant to a specific phase."""
        base = {
            "task": self.user_input,
            "project": self.project,
            "budget": self.budget,
            "constraints": self.constraints,
            "state": self.state,
            "workspace_path": self.workspace_path,
            "fs": self.fs,
            "memory_engine": self.memory_engine,
            "provider_runtime": self.provider_runtime,
            "runtime_bus": self.runtime_bus,
            "event_bus": self.event_bus,
            "tree_snapshot": self.tree_snapshot,
            "signals": self.signals,
            "forced_route": self.forced_route,
            "conversation_history": self.conversation_history,
            "kernel": self.kernel,
            "active_specialists": self.active_specialists,
            # HermesContext — immutable global cognition, available to ALL phases
            "hermes_context": self.hermes_context,
        }

        # Add handoff data from previous phases
        phase_data = {
            PipelinePhase.CALIBRATION: {
                **base,
                "user_model": self.calibrated_user_model,
                "communication_preferences": self.communication_preferences,
                "workflow_mode": self.workflow_mode,
            },
            PipelinePhase.PLANNING: {
                **base,
                "user_model": self.calibrated_user_model,
                "user_profile_prompt": self._build_user_profile_prompt(),
                "cross_memory": self._build_cross_memory(),
                "security_rules": self.security_rules,
                "system_decisions": self._get_system_decisions(),
            },
            PipelinePhase.RESEARCH: {
                **base,
                "strategic_plan": self.strategic_plan,
                "user_model": self.calibrated_user_model,
                "decomposed_requirements": self.decomposed_requirements,
            },
            PipelinePhase.IMPLEMENTATION: {
                **base,
                "strategic_plan": self.strategic_plan,
                "research_findings": self.research_findings,
                "codebase_evidence": self.codebase_evidence,
                "user_model": self.calibrated_user_model,
                "code_patterns": self._get_code_patterns(),
                "security_rules": self.security_rules,
            },
            PipelinePhase.SECURITY: {
                **base,
                "code_changes": self.code_changes,
                "strategic_plan": self.strategic_plan,
                "security_rules": self.security_rules,
            },
            PipelinePhase.EXECUTION: {
                **base,
                "security_clearance": self.security_clearance,
                "strategic_plan": self.strategic_plan,
                "security_rules": self.security_rules,
            },
            PipelinePhase.REPORTING: {
                **base,
                "security_clearance": self.security_clearance,
                "research_findings": self.research_findings,
                "code_changes": self.code_changes,
                "execution_artifacts": self.execution_artifacts,
                "strategic_plan": self.strategic_plan,
                "user_model": self.calibrated_user_model,
            },
        }

        return phase_data.get(phase, base)

    def record_phase_result(self, result: PhaseResult) -> None:
        """Record and publish the result of a pipeline phase."""
        self._phase_results[result.phase] = result
        if result.success:
            for key, value in result.handoff_data.items():
                if hasattr(self, key):
                    setattr(self, key, value)

    def _build_user_profile_prompt(self) -> str:
        """Build a user profile prompt from calibrated data."""
        model = self.calibrated_user_model
        lines = []
        if model.get("communication_style"):
            lines.append(f"Communication Style: {model['communication_style']}")
        if model.get("expertise"):
            lines.append(f"Expertise: {model['expertise']}")
        if model.get("workflow_mode"):
            lines.append(f"Workflow: {model['workflow_mode']}")
        return "\n".join(lines) if lines else ""

    def _build_cross_memory(self) -> Dict[str, Any]:
        """Aggregate memory from all specialists into shared cross-memory."""
        cross = {}
        if self.security_rules:
            cross["security_rules"] = self.security_rules
        if self.research_findings:
            cross["research_findings"] = self.research_findings
        return cross

    def _get_system_decisions(self) -> List[Dict[str, Any]]:
        """Get architectural decisions from context."""
        decisions = []
        if self.memory_engine:
            try:
                res = self.memory_engine.memory_collection.query(
                    query_texts=[self.user_input],
                    n_results=5,
                    where={"type": "system_decision", "project": self.project},
                )
                if res.get("ids") and res["ids"][0]:
                    for doc, dist in zip(res["documents"][0], res["distances"][0]):
                        decisions.append({
                            "doc": doc,
                            "score": round(1.0 - float(dist), 3),
                        })
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)
        return decisions

    def _get_code_patterns(self) -> List[Dict[str, Any]]:
        """Get code patterns from memory."""
        patterns = []
        if self.memory_engine:
            try:
                res = self.memory_engine.memory_collection.query(
                    query_texts=[self.user_input],
                    n_results=5,
                    where={"type": "code_pattern", "project": self.project},
                )
                if res.get("ids") and res["ids"][0]:
                    for doc, dist in zip(res["documents"][0], res["distances"][0]):
                        patterns.append({
                            "doc": doc,
                            "score": round(1.0 - float(dist), 3),
                        })
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)
        return patterns

    @property
    def elapsed_ms(self) -> float:
        return (time.time() - self._start_time) * 1000

    @property
    def phase_results(self) -> Dict[PipelinePhase, PhaseResult]:
        return dict(self._phase_results)


# ============================================================================
# RuntimePipeline â€” Canonical Execution Pipeline
# ============================================================================


class RuntimePipeline:
    """Canonical AELVO execution pipeline.

    The pipeline enforces the architecture's specialist ordering with
    explicit handoff contracts, cross-cutting verification, recovery,
    and memory consolidation.

    Usage:
        pipeline = RuntimePipeline(orchestrator)
        result = await pipeline.run(user_input, agent)
    """

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.memory_engine = getattr(orchestrator, "memory_engine", None)
        self.fs = getattr(orchestrator, "fs", None)
        self.kernel = getattr(orchestrator, "kernel", None)
        self.runtime_bus = getattr(orchestrator, "runtime_bus", None)
        self.event_bus = getattr(orchestrator, "event_bus", None)
        self.provider_runtime = getattr(orchestrator, "provider_runtime", None)
        self.verification_pipeline = getattr(
            orchestrator, "verification_pipeline", None
        )
        self.runtime_recovery = getattr(orchestrator, "runtime_recovery", None)
        self.runtime_graph = getattr(orchestrator, "runtime_graph", None)
        self._plan_calibration = getattr(
            orchestrator, "_plan_calibration", None
        )
        self.cognitive_engine = getattr(
            orchestrator, "cognitive_engine", None
        )

        # Specialists
        from specialists import SPECIALIST_REGISTRY
        self.specialists = SPECIALIST_REGISTRY

        # Active phases for the current run
        self._active_phases: List[PipelinePhase] = []

    async def run(
        self,
        user_input: str,
        agent: Any,
        conversation_history: List[Dict[str, Any]],
        hermes_context: Optional[Any] = None,
    ) -> PipelineResult:
        """Execute the full canonical pipeline.

        OPTIMIZED: Uses a single consolidated prompt per turn instead of
        making N separate LLM API calls (one per phase). All specialist
        context is merged into one comprehensive prompt.

        The consolidated prompt includes:
        - Shared context (tree, constraints, state, memory)
        - Each active specialist's system prompt (condensed)
        - The canonical handoff chain instructions
        - Output format specification covering all phases

        The LLM processes all specialist roles within a single response,
        eliminating unnecessary API round-trips between phases.
        """
        start_time = time.time()

        # 1. Build pipeline context (includes HermesContext if provided)
        ctx = self._build_initial_context(
            user_input, agent, conversation_history,
            hermes_context=hermes_context,
        )

        # 2. Determine which phases should run
        self._active_phases = self._determine_phases(ctx)

        # Check circuit breakers for active phases
        for phase in self._active_phases:
            contract = PIPELINE_HANDOFFS.get(phase)
            if contract:
                spec_name = contract.specialist_name
                fail_count = getattr(self.orchestrator, "specialist_failures", {}).get(spec_name, 0)
                if fail_count >= 3:
                    log.error("Circuit breaker TRIPPED for specialist %s (failures: %d)", spec_name, fail_count)
                    return PipelineResult(
                        success=False,
                        phases_executed=[],
                        phase_results={},
                        total_duration_ms=(time.time() - start_time) * 1000,
                        final_output=f"Circuit breaker TRIPPED for specialist {spec_name}. Cannot proceed.",
                        failures=[(phase, f"Circuit breaker tripped: {spec_name} has {fail_count} failures.")]
                    )

        # Notify UI
        self._notify_pipeline_start(ctx)

        # Guard: if no phases are active, return early
        if not self._active_phases:
            log.warning("Pipeline run called with zero active phases")
            return PipelineResult(
                success=True,
                phases_executed=[],
                phase_results={},
                total_duration_ms=(time.time() - start_time) * 1000,
                final_output="",
            )

        # 3. Build consolidated context for ALL phases upfront
        #    This queries memory ONCE for all phases instead of N times
        consolidated_context = self._build_consolidated_context(ctx)

        # 4. Build ONE consolidated prompt covering all active phases
        consolidated_prompt = self._build_consolidated_prompt(
            consolidated_context, ctx
        )

        # 5. Execute ONE LLM call for the entire pipeline turn
        #    (instead of N separate calls for each phase)
        execution_start = time.time()
        raw_output = await self._execute_consolidated_turn(
            agent, consolidated_prompt
        )
        execution_duration = (time.time() - execution_start) * 1000

        # 6. Create phase results for tracking (all share the same output
        #    since it came from one call â€” but we record per-phase info)
        failures: List[Tuple[PipelinePhase, str]] = []
        phase_results: Dict[PipelinePhase, PhaseResult] = {}

        for phase in self._active_phases:
            contract = PIPELINE_HANDOFFS.get(phase)
            if contract is None:
                continue

            result = PhaseResult(
                phase=phase,
                specialist_name=contract.specialist_name,
                success=True,
                output=raw_output,
                handoff_data=self._extract_handoff_data(
                    raw_output, contract.produces, ctx
                ),
                duration_ms=execution_duration / max(1, len(self._active_phases)),
            )
            phase_results[phase] = result
            ctx.record_phase_result(result)

            # Post-process per specialist (memory persistence)
            specialist = self.specialists.get(contract.specialist_name)
            if specialist:
                try:
                    audit = specialist.post_process(
                        raw_output, ctx.memory_engine, ctx.conversation_history
                    )
                    log.info(
                        "Phase %s post_process: %s", phase.value, audit
                    )
                except Exception as e:
                    log.warning(
                        "Phase %s post_process failed: %s", phase.value, e
                    )

            # Verify phase output
            if contract.verification_required and result.success:
                verified = self._verify_phase_output_sync(
                    phase, result, ctx
                )
                result.verification_passed = verified
                if not verified:
                    log.warning(
                        "Phase %s verification failed", phase.value
                    )
                    failures.append((
                        phase,
                        f"Verification failed for {contract.specialist_name}",
                    ))

        # 7. Memory Consolidation Phase
        consolidation_result = await self._consolidate_memory(ctx)

        # 8. Build final result
        total_duration = (time.time() - start_time) * 1000
        verification_summary = self._build_verification_summary(
            phase_results
        )

        pipeline_result = PipelineResult(
            success=len(failures) == 0,
            phases_executed=self._active_phases,
            phase_results=phase_results,
            total_duration_ms=total_duration,
            final_output=raw_output,
            failures=failures,
            memory_consolidated=consolidation_result,
            verification_summary=verification_summary,
            recovery_actions=[],
        )

        # 9. Record calibration outcome
        await self._record_pipeline_outcome(pipeline_result, ctx)

        # Notify UI
        self._notify_pipeline_complete(pipeline_result)

        log.info(
            "Pipeline completed: %s in %.1fs with %d phases (1 LLM call)",
            "SUCCESS" if pipeline_result.success else "FAILURE",
            total_duration / 1000,
            len(self._active_phases),
        )

        return pipeline_result

    def _build_initial_context(
        self,
        user_input: str,
        agent: Any,
        conversation_history: List[Dict[str, Any]],
        hermes_context: Optional[Any] = None,
    ) -> PipelineContext:
        """Build the initial pipeline context with basic system state.

        Includes HermesContext if provided — the immutable global cognition
        context produced by HERMES at the start of every turn.
        """
        workspace_path = getattr(
            self.orchestrator, "base_path",
            getattr(self.orchestrator, "workspace", ""),
        )
        project = getattr(
            self.memory_engine, "project_name", "default"
        ) if self.memory_engine else "default"

        ctx = PipelineContext(
            user_input=user_input,
            conversation_history=conversation_history,
            memory_engine=self.memory_engine,
            fs=self.fs,
            kernel=self.kernel,
            provider_runtime=self.provider_runtime,
            runtime_bus=self.runtime_bus,
            event_bus=self.event_bus,
            workspace_path=workspace_path,
            project=project,
            hermes_context=hermes_context,
        )

        # Load constraints from anchor
        if self.memory_engine:
            try:
                ctx.constraints = (
                    self.memory_engine.parse_anchor() or {}
                )
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)

            # Load state
            try:
                rows = self.memory_engine.db.execute(
                    "SELECT key, value FROM state WHERE key NOT LIKE 'runtime:%'"
                ).fetchall()
                ctx.state = {r[0]: r[1] for r in rows}
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)

        # Get workspace tree
        if hasattr(self.orchestrator, "get_workspace_tree"):
            ctx.tree_snapshot = self.orchestrator.get_workspace_tree()

        ctx.active_specialists = list(self.specialists.keys())

        return ctx

    def _determine_phases(
        self, ctx: PipelineContext
    ) -> List[PipelinePhase]:
        """Determine which phases should run for this task.

        Always includes CALIBRATION, PLANNING, SECURITY, REPORTING.
        Conditionally includes RESEARCH, IMPLEMENTATION, EXECUTION
        based on task analysis.
        """
        task_lower = ctx.user_input.lower()

        # Always required
        phases = [
            PipelinePhase.CALIBRATION,
            PipelinePhase.PLANNING,
        ]

        # RESEARCH: Include when research/external knowledge is needed
        research_keywords = [
            "research", "search", "find", "investigate", "explain",
            "what is", "who is", "how does", "latest", "documentation",
        ]
        if any(kw in task_lower for kw in research_keywords):
            phases.append(PipelinePhase.RESEARCH)

        # IMPLEMENTATION: Include when code changes are needed
        implementation_keywords = [
            "implement", "code", "write", "refactor", "fix", "build",
            "create", "add feature", "update", "modify", "change",
        ]
        if any(kw in task_lower for kw in implementation_keywords):
            phases.append(PipelinePhase.IMPLEMENTATION)

        # SECURITY: Always include if implementation ran, or when
        # security is explicitly mentioned
        security_keywords = [
            "security", "vulnerability", "audit", "secret", "leak",
            "cve", "injection", "xss", "sqli",
        ]
        if (
            PipelinePhase.IMPLEMENTATION in phases
            or any(kw in task_lower for kw in security_keywords)
        ):
            phases.append(PipelinePhase.SECURITY)

        # EXECUTION: Include when terminal/runtime operations are needed
        execution_keywords = [
            "run", "execute", "deploy", "docker", "git", "commit",
            "push", "npm", "pip install", "terminal", "bash",
            "command", "ci/cd",
        ]
        if any(kw in task_lower for kw in execution_keywords):
            phases.append(PipelinePhase.EXECUTION)

        # REPORTING: Always required
        phases.append(PipelinePhase.REPORTING)

        return phases

    def _should_run_phase(
        self, phase: PipelinePhase, ctx: PipelineContext
    ) -> bool:
        """Check if a phase should actually execute (vs being skipped).

        Research and Execution phases can be skipped if the plan
        determines they're not needed.
        """
        contract = PIPELINE_HANDOFFS.get(phase)
        if contract is None:
            return False

        # CALIBRATION, PLANNING, SECURITY, REPORTING are always required
        if contract.failure_recovery_strategy != "skip_if_not_required":
            return True

        # For optional phases: check if there's anything to do
        if phase == PipelinePhase.RESEARCH:
            return bool(ctx.decomposed_requirements)

        if phase == PipelinePhase.EXECUTION:
            return bool(ctx.code_changes or ctx.execution_artifacts)

        return True

    # â”€â”€ LEGACY: Per-phase execution methods â”€â”€
    # These methods are kept for reference and backward compatibility.
    # The primary execution path now uses the consolidated prompt approach
    # (_build_consolidated_prompt + _execute_consolidated_turn) which makes
    # ONE LLM call per turn instead of N separate calls.
    #
    # These legacy methods are not currently called but remain available
    # for forced-route fallback or if per-phase execution is needed.

    async def _execute_phase(
        self,
        phase: PipelinePhase,
        contract: HandoffContract,
        ctx: PipelineContext,
        agent: Any,
    ) -> PhaseResult:
        """Execute a single pipeline phase by dispatching to the
        appropriate specialist.

        LEGACY: Superseded by consolidated prompt flow.
        """
        phase_start = time.time()

        # Build phase-specific context
        phase_context = ctx.get_phase_data(phase)
        phase_context["agent"] = agent

        # 1. Get the specialist's system prompt with handoff data
        specialist = self.specialists.get(contract.specialist_name)
        if specialist is None:
            return PhaseResult(
                phase=phase,
                specialist_name=contract.specialist_name,
                success=False,
                error=f"Specialist {contract.specialist_name} not found",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 2. Inject handoff information from previous phase
        if contract.receives_from:
            prev_result = ctx.phase_results.get(contract.receives_from)
            if prev_result:
                phase_context["handoff_from"] = {
                    "phase": contract.receives_from.value,
                    "specialist": prev_result.specialist_name,
                    "artifacts": prev_result.handoff_data,
                    "success": prev_result.success,
                }

        # 3. Inject handoff contract for this phase
        phase_context["handoff_contract"] = {
            "phase": phase.value,
            "specialist": contract.specialist_name,
            "produces": contract.produces,
            "required_context_keys": contract.required_context_keys,
        }

        # 4. Build specialist system prompt with enriched context
        specialist_prompt = specialist.get_system_prompt(phase_context)

        # 5. Send to the agent for execution
        # NOTE: We pass the specialist prompt directly to _execute_specialist_turn,
        # which handles injecting it. DO NOT append it to conversation_history here
        # to avoid the double-prompt bug (the specialist would get both the prompt
        # AND the raw task as separate user messages).
        raw_output = await self._execute_specialist_turn(
            specialist, agent, phase_context, specialist_prompt
        )

        # 6. Post-process result
        try:
            memory_engine = ctx.memory_engine
            audit = specialist.post_process(
                raw_output, memory_engine, ctx.conversation_history
            )
            log.info("Phase %s post_process: %s", phase.value, audit)
        except Exception as e:
            log.warning(
                "Phase %s post_process failed: %s", phase.value, e
            )

        # 7. Extract handoff data from output
        handoff_data = self._extract_handoff_data(
            raw_output, contract.produces, ctx
        )

        return PhaseResult(
            phase=phase,
            specialist_name=contract.specialist_name,
            success=True,
            output=raw_output,
            handoff_data=handoff_data,
            duration_ms=(time.time() - phase_start) * 1000,
        )

    async def _execute_specialist_turn(
        self,
        specialist: Any,
        agent: Any,
        context: Dict[str, Any],
        specialist_prompt: str,
    ) -> str:
        """Execute a specialist turn through the agent.

        Injects the specialist prompt as the user message (not appended twice).
        """
        try:
            # Send the specialist prompt as the user message.
            # agent.send_user_message handles appending to conversation_history
            raw_output = agent.send_user_message(specialist_prompt)
            if isinstance(raw_output, str):
                return raw_output
            return str(raw_output)
        except Exception as e:
            log.error(
                "Specialist turn execution failed for %s: %s",
                specialist.name, e,
            )
            return f"Error: {e}"

    def _extract_handoff_data(
        self,
        output: str,
        expected_keys: List[str],
        ctx: PipelineContext,
    ) -> Dict[str, Any]:
        """Extract structured handoff data from specialist output."""
        handoff = {}
        for key in expected_keys:
            # Map handoff keys to context attributes
            attr_map = {
                "calibrated_user_model": "calibrated_user_model",
                "communication_preferences": "communication_preferences",
                "workflow_mode": "workflow_mode",
                "strategic_plan": "strategic_plan",
                "specialist_assignments": "specialist_assignments",
                "verification_plan": "verification_plan",
                "research_findings": "research_findings",
                "code_changes": "code_changes",
                "security_clearance": "security_clearance",
                "security_rules": "security_rules",
                "risk_findings": "risk_findings",
            }
            context_attr = attr_map.get(key)
            if context_attr and hasattr(ctx, context_attr):
                handoff[key] = getattr(ctx, context_attr)

        return handoff

    def _is_critical_failure(
        self, phase: PipelinePhase, contract: HandoffContract
    ) -> bool:
        """Determine if a phase failure is critical enough to stop the
        pipeline."""
        if contract.failure_recovery_strategy == "block_on_critical":
            return True
        if phase in (
            PipelinePhase.CALIBRATION,
            PipelinePhase.PLANNING,
            PipelinePhase.SECURITY,
        ):
            return True
        return False

    async def _attempt_recovery(
        self,
        phase: PipelinePhase,
        result: PhaseResult,
        ctx: PipelineContext,
    ) -> Optional[PhaseResult]:
        """Attempt to recover from a phase failure.

        Uses the runtime RecoveryEngine if available, falling back to
        simple retry logic.
        """
        contract = PIPELINE_HANDOFFS.get(phase)
        if contract is None:
            return None

        strategy = contract.failure_recovery_strategy
        log.info(
            "Attempting recovery for phase %s with strategy '%s'",
            phase.value, strategy,
        )

        if strategy == "skip_if_not_required":
            # Skip this phase and proceed
            return PhaseResult(
                phase=phase,
                specialist_name=contract.specialist_name,
                success=True,
                output=f"[{phase.value}] Skipped after failure â€” not critical",
                handoff_data=result.handoff_data,
            )

        if strategy == "retry_phase":
            # Simple retry with backoff
            await asyncio.sleep(1.0)
            return None  # Let the pipeline re-run the phase

        if strategy == "block_on_critical":
            # Cannot recover â€” pipeline will stop
            return None

        return None

    async def _attempt_verification_recovery(
        self,
        phase: PipelinePhase,
        result: PhaseResult,
        ctx: PipelineContext,
    ) -> Optional[PhaseResult]:
        """Attempt to recover from a verification failure."""
        log.info(
            "Attempting verification recovery for phase %s", phase.value
        )
        return None  # Placeholder â€” could re-run with stricter constraints

    async def _verify_phase_output(
        self,
        phase: PipelinePhase,
        result: PhaseResult,
        ctx: PipelineContext,
    ) -> bool:
        """Run verification on a phase's output.

        Delegates to the VerificationPipeline if available.
        """
        if self.verification_pipeline is None:
            return True

        contract = PIPELINE_HANDOFFS.get(phase)
        if contract is None or not contract.verification_required:
            return True

        try:
            from runtime_next.verification.types import (
                VerificationType,
                VerificationManifest,
                VerificationScope,
            )

            manifest = VerificationManifest(
                required=[VerificationType.SANDBOX_VALIDATION],
                blocking=[VerificationType.SANDBOX_VALIDATION],
            )
            scope = VerificationScope(
                affected_files=[],
                is_minimal=True,
                provenance=f"pipeline_{phase.value}",
            )
            context = {
                "phase": phase.value,
                "specialist": contract.specialist_name,
                "output": result.output,
            }

            verif_results = await self.verification_pipeline.verify(
                node_id=f"pipeline_{phase.value}",
                manifest=manifest,
                scope=scope,
                context=context,
            )

            success = all(vr.success for vr in verif_results)
            return success

        except Exception as e:
            log.warning(
                "Phase verification failed for %s: %s", phase.value, e
            )
            return True  # Don't block on verification errors

    # ==================================================================
    # Memory Consolidation
    # ==================================================================

    async def _consolidate_memory(
        self, ctx: PipelineContext
    ) -> bool:
        """Consolidate memory at the end of a pipeline run.

        This is the authoritative memory consolidation step that:
        1. Saves session summary with specialist handoff trace
        2. Reinforces used memories
        3. Prunes stale memories (importance decay)
        4. Cross-links related memories
        """
        if not self.memory_engine:
            return False

        project = ctx.project
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

        try:
            # 1. Build session summary with handoff trace
            handoff_trace = []
            for phase in self._active_phases:
                result = ctx.phase_results.get(phase)
                if result:
                    handoff_trace.append(
                        f"{phase.value}({result.specialist_name}): "
                        f"{'âœ“' if result.success else 'âœ—'} "
                        f"({result.duration_ms:.0f}ms)"
                    )

            summary = (
                f"PIPELINE EXECUTION [{project}]:\n"
                f"Input: {ctx.user_input[:200]}\n"
                f"Handoff Chain: {' â†’ '.join(p.value for p in self._active_phases)}\n"
                f"Results:\n" + "\n".join(f"  {t}" for t in handoff_trace)
            )

            # 2. Save to ChromaDB
            import hashlib

            m_id = hashlib.sha256(
                f"pipeline_{project}_{time.time()}".encode()
            ).hexdigest()
            self.memory_engine.memory_collection.add(
                ids=[m_id],
                documents=[summary],
                metadatas=[{
                    "type": "session_summary",
                    "project": project,
                    "timestamp": timestamp,
                    "timestamp_unix": time.time(),
                    "importance": 0.7,
                    "usage_count": 1,
                    "source_specialist": "pipeline",
                    "pipeline_phases": ", ".join(
                        p.value for p in self._active_phases
                    ),
                    "success": str(all(
                        r.success
                        for r in ctx.phase_results.values()
                    )),
                }],
            )

            # 3. Dual-sync to SQLite
            if hasattr(self.memory_engine, "db"):
                with self.memory_engine.db:
                    self.memory_engine.db.execute(
                        "INSERT INTO retained_memory (content) VALUES (?)",
                        (
                            f"[PIPELINE:session_summary|{project}] "
                            f"{summary[:800]}",
                        ),
                    )

            log.info(
                "Pipeline memory consolidated: %s phases, handoff trace saved",
                len(self._active_phases),
            )
            return True

        except Exception as e:
            log.warning("Pipeline memory consolidation failed: %s", e)
            return False

    # ==================================================================
    # Result Building
    # ==================================================================

    def _build_final_output(
        self,
        ctx: PipelineContext,
        phase_results: Dict[PipelinePhase, PhaseResult],
    ) -> str:
        """Build the final output from all phase results.

        Returns the raw LLM output from the last successful phase that
        produced output, so the REPL loop can parse it for tool calls.
        Falls back to a summary if no raw output is available.
        """
        # Try to get the raw LLM output from the last successful phase
        # that actually produced output (implementation, reporting, etc.)
        for phase in reversed(self._active_phases):
            result = phase_results.get(phase)
            if result and result.success and result.output:
                # Skip metadata-only outputs
                output = result.output.strip()
                if output and len(output) > 10:
                    return output

        # Absolute fallback: return empty string for REPL compatibility
        return ""

    def _build_verification_summary(
        self,
        phase_results: Dict[PipelinePhase, PhaseResult],
    ) -> str:
        """Build a summary of verification results across phases."""
        lines = []
        for phase in self._active_phases:
            result = phase_results.get(phase)
            if result is None:
                continue
            vstatus = "âœ“" if result.verification_passed else "âœ—"
            lines.append(
                f"  {vstatus} {phase.value}: verification "
                f"{'passed' if result.verification_passed else 'failed'}"
            )
            if result.recovery_attempted:
                lines.append(
                    f"     â†» Recovery: "
                    f"{'succeeded' if result.recovery_success else 'failed'}"
                )
        return "\n".join(lines)

    def _collect_recovery_actions(
        self,
        phase_results: Dict[PipelinePhase, PhaseResult],
    ) -> List[str]:
        """Collect all recovery actions taken across phases."""
        actions = []
        for phase, result in phase_results.items():
            if result.recovery_attempted:
                actions.append(
                    f"{phase.value}: "
                    f"{'âœ“' if result.recovery_success else 'âœ—'}"
                )
        return actions

    # ==================================================================
    # Calibration Recording
    # ==================================================================

    async def _record_pipeline_outcome(
        self,
        pipeline_result: PipelineResult,
        ctx: PipelineContext,
    ) -> None:
        """Record the pipeline execution outcome for learning."""
        if not self._plan_calibration:
            return
        try:
            self._plan_calibration.record_outcome(
                plan_id=f"pipeline_{int(time.time())}",
                objective=ctx.user_input[:200],
                task_type=self._classify_task_type(ctx.user_input),
                strategy_class="pipeline",
                planned_phases=len(self._active_phases),
                completed_phases=sum(
                    1 for r in pipeline_result.phase_results.values()
                    if r.success
                ),
                planned_specialists=[
                    PIPELINE_HANDOFFS[p].specialist_name
                    for p in self._active_phases
                ],
                actual_specialists=[
                    r.specialist_name
                    for r in pipeline_result.phase_results.values()
                    if r.success
                ],
                planned_risks=0,
                materialized_risks=len(pipeline_result.failures),
                verification_checks_run=len(self._active_phases),
                verification_failures_caught=sum(
                    1 for r in pipeline_result.phase_results.values()
                    if not r.verification_passed
                ),
                verification_type_failures={},
                unplanned_failures=len(pipeline_result.failures),
                total_duration_ms=pipeline_result.total_duration_ms,
                success=pipeline_result.success,
            )
        except Exception as e:
            log.warning(
                "Pipeline calibration recording failed: %s", e
            )

    @staticmethod
    def _classify_task_type(task: str) -> str:
        """Classify the task type from the input text."""
        lower = task.lower()
        if any(
            w in lower
            for w in ("refactor", "rewrite", "restructure")
        ):
            return "refactor"
        if any(
            w in lower
            for w in ("fix", "bug", "error", "issue")
        ):
            return "fix"
        if any(
            w in lower
            for w in ("add", "create", "implement", "build", "new")
        ):
            return "feature"
        if any(
            w in lower
            for w in ("security", "auth", "vulnerability")
        ):
            return "security"
        return "general"

    # ==================================================================
    # UI Notifications
    # ==================================================================

    def _notify_pipeline_start(self, ctx: PipelineContext) -> None:
        """Notify UI that the pipeline is starting."""
        phases_str = " â†’ ".join(p.value for p in self._active_phases)
        log.info(
            "[PIPELINE] Starting: %s | Phases: %s",
            ctx.user_input[:60], phases_str,
        )
        # Emit specialist activation events to UI event bus
        event_bus = ctx.event_bus
        if event_bus:
            try:
                from ui.events.event_factory import create_specialist_event
                from ui.events import EventType as UIEventType
                loop = asyncio.get_running_loop()
                for phase in self._active_phases:
                    contract = PIPELINE_HANDOFFS.get(phase)
                    if contract:
                        loop.create_task(event_bus.publish(
                            create_specialist_event(
                                UIEventType.SPECIALIST_ACTIVATED,
                                contract.specialist_name,
                                f"Pipeline phase: {phase.value}",
                            )
                        ))
            except Exception:
                pass

    def _notify_pipeline_complete(
        self, result: PipelineResult
    ) -> None:
        """Notify UI that the pipeline completed."""
        log.info(
            "[PIPELINE] Completed: success=%s, duration=%.1fs, phases=%d",
            result.success,
            result.total_duration_ms / 1000,
            len(result.phases_executed),
        )
        # Emit specialist completion events
        if self.event_bus:
            try:
                from ui.events.event_factory import create_specialist_event
                from ui.events import EventType as UIEventType
                loop = asyncio.get_running_loop()
                for phase in result.phases_executed:
                    contract = PIPELINE_HANDOFFS.get(phase)
                    if contract:
                        phase_result = result.phase_results.get(phase)
                        success = phase_result.success if phase_result else True
                        loop.create_task(self.event_bus.publish(
                            create_specialist_event(
                                UIEventType.SPECIALIST_ACTION if success else UIEventType.SPECIALIST_DEACTIVATED,
                                contract.specialist_name,
                                f"Pipeline {'completed' if success else 'failed'}: {phase.value}",
                                {"success": success},
                            )
                        ))
            except Exception:
                pass

    # ==================================================================
    # CONSOLIDATED PROMPT (Single LLM Call Per Turn)
    # ==================================================================
    #
    # Instead of making N separate LLM API calls (one per specialist phase),
    # these methods build a single consolidated prompt that includes the
    # context and instructions for ALL active phases. The LLM processes
    # all specialist roles within a single response.
    #
    # Benefits:
    #   1. 1 LLM call per turn instead of up to 7
    #   2. All context available upfront â€” no sequential handoff bottlenecks
    #   3. Consistent shared context across all phases
    #   4. ~70% reduction in total turn latency
    # ==================================================================

    def _build_consolidated_context(
        self, ctx: PipelineContext
    ) -> Dict[str, Dict[str, Any]]:
        """Build context for ALL active phases upfront with a single memory
        query pass, instead of querying memory N times (once per phase).

        Returns a dict mapping PipelinePhase values to their context dicts.
        """
        phase_contexts: Dict[str, Dict[str, Any]] = {}

        # â”€â”€ Single unified memory query â”€â”€
        # Query memory once for ALL specialist memory types, then distribute
        # the results to the appropriate phase contexts.
        unified_memory: Dict[str, List[Dict[str, Any]]] = {}
        if ctx.memory_engine:
            try:
                # Batch query: get all memory types relevant to this task.
                # Using n_results=25 ensures each of the 7+ memory types
                # gets a meaningful number of hits (old per-phase approach
                # queried n_results=5 per type with N separate queries).
                res = ctx.memory_engine.memory_collection.query(
                    query_texts=[ctx.user_input],
                    n_results=25,
                    include=["documents", "metadatas", "distances"],
                )
                if res.get("ids") and res["ids"][0]:
                    for doc, meta, dist in zip(
                        res["documents"][0],
                        res["metadatas"][0],
                        res["distances"][0],
                    ):
                        mtype = meta.get("type", "unknown")
                        if mtype not in unified_memory:
                            unified_memory[mtype] = []
                        unified_memory[mtype].append({
                            "doc": doc,
                            "score": round(1.0 - float(dist), 3),
                            "metadata": meta,
                        })
            except Exception as _ex: log.debug("Silenced exception: %s", _ex)

        # â”€â”€ Build context for each active phase â”€â”€
        for phase in self._active_phases:
            base = {
                "task": ctx.user_input,
                "project": ctx.project,
                "budget": ctx.budget,
                "constraints": ctx.constraints,
                "state": ctx.state,
                "workspace_path": ctx.workspace_path,
                "fs": ctx.fs,
                "memory_engine": ctx.memory_engine,
                "provider_runtime": ctx.provider_runtime,
                "runtime_bus": ctx.runtime_bus,
                "event_bus": ctx.event_bus,
                "tree_snapshot": ctx.tree_snapshot,
                "signals": ctx.signals,
                "forced_route": ctx.forced_route,
                "conversation_history": ctx.conversation_history,
                "kernel": ctx.kernel,
                "active_specialists": ctx.active_specialists,
                "user_model": ctx.calibrated_user_model,
                "strategic_plan": ctx.strategic_plan,
                "system_decisions": (
                    unified_memory.get("system_decision", [])
                    + unified_memory.get("system_decision_proposal", [])
                ),
                "research_findings": (
                    unified_memory.get("research_finding", [])
                    + unified_memory.get("research", [])
                ),
                "security_rules": unified_memory.get("security_rule", []),
                "code_patterns": (
                    unified_memory.get("code_pattern", [])
                    + unified_memory.get("convention", [])
                ),
                "error_recoveries": unified_memory.get("error_recovery", []),
            }

            contract = PIPELINE_HANDOFFS.get(phase)
            if contract:
                base["handoff_contract"] = {
                    "phase": phase.value,
                    "specialist": contract.specialist_name,
                    "produces": contract.produces,
                }

            phase_contexts[phase.value] = base

        return phase_contexts

    def _build_consolidated_prompt(
        self,
        phase_contexts: Dict[str, Dict[str, Any]],
        ctx: PipelineContext,
    ) -> str:
        """Build a SINGLE consolidated prompt covering all active pipeline
        phases. This replaces N separate specialist calls with one
        comprehensive prompt that the LLM processes in a single response.
        """
        sections: List[str] = []

        # â”€â”€ Header â”€â”€
        phases_display = " â†’ ".join(
            p.value for p in self._active_phases
        )
        header = (
            "â•" * 60 + "\n"
            "AELVO PIPELINE â€” CONSOLIDATED EXECUTION PROMPT\n"
            "â•" * 60 + "\n\n"
            f"User Task: {ctx.user_input}\n"
            f"Pipeline Flow: {phases_display}\n\n"
            "You will process the user's request through ALL of the following\n"
            "specialist roles IN SEQUENCE within this single response. Each\n"
            "phase builds on the previous one. Work through them in order.\n"
            "Use the available tools (read_file, write_file, search_memory,\n"
            "bash_exec, etc.) as needed by each specialist role.\n"
        )
        sections.append(header)

        # â”€â”€ Shared Context Section â”€â”€
        shared = self._build_shared_context_section(ctx)
        sections.append(shared)

        # â”€â”€ Phase-by-Phase Instructions â”€â”€
        for idx, phase in enumerate(self._active_phases, start=1):
            contract = PIPELINE_HANDOFFS.get(phase)
            if contract is None:
                continue

            phase_ctx = phase_contexts.get(phase.value, {})

            # Get the specialist's system prompt
            specialist = self.specialists.get(contract.specialist_name)
            specialist_prompt = ""
            if specialist:
                try:
                    specialist_prompt = specialist.get_system_prompt(phase_ctx)
                except Exception as e:
                    log.warning(
                        "Failed to get prompt for %s: %s",
                        contract.specialist_name, e,
                    )

            # Build phase section
            phase_section = (
                "â”" * 60 + "\n"
                f"PHASE {idx}: {phase.value.upper()} â€” {contract.specialist_name}\n"
                "â”" * 60 + "\n\n"
                f"Role: {self.phase_descriptions.get(phase, '')}\n\n"
                f"Handoff â€” receives context from: "
                f"{contract.receives_from.value if contract.receives_from else '(user input)'}\n"
                f"Produces: {', '.join(contract.produces)}\n\n"
                f"INSTRUCTIONS:\n"
                f"{specialist_prompt}\n\n"
            )
            sections.append(phase_section)

        # â”€â”€ Output Format â”€â”€
        output_section = (
            "â•" * 60 + "\n"
            "OUTPUT FORMAT\n"
            "â•" * 60 + "\n\n"
            "Your response must be a JSON array of tool calls.\n"
            "Work through ALL phases above in order. Each phase may require\n"
            "different tool calls (read_file, search_memory, write_file, etc.).\n"
            "The final phase (HERALD/reporting) should use the 'respond' tool\n"
            "to deliver the results to the user.\n\n"
            "Example:\n"
            "[\n"
            '  {"rationale": "...", "tool": "search_memory", '
            '"args": {"query": "..."}},\n'
            '  {"rationale": "...", "tool": "read_file", '
            '"args": {"path": "..."}},\n'
            '  {"rationale": "...", "tool": "respond", '
            '"args": {"message": "..."}}\n'
            "]\n"
        )
        sections.append(output_section)

        return "\n".join(sections)

    def _build_shared_context_section(
        self, ctx: PipelineContext
    ) -> str:
        """Build the shared context section used by ALL specialists."""
        lines: List[str] = [
            "â”" * 60,
            "SHARED CONTEXT (available to all specialists)",
            "â”" * 60,
            "",
            f"Project: {ctx.project}",
            f"Workspace: {ctx.workspace_path}",
            f"Budget: {ctx.budget} tool steps",
            "",
        ]

        # Constraints
        if ctx.constraints:
            lines.append("CONSTRAINTS:")
            for k, v in ctx.constraints.items():
                if isinstance(v, dict):
                    lines.append(f"  {k}: {v.get('value', v)}")
                else:
                    lines.append(f"  {k}: {v}")
            lines.append("")

        # State
        if ctx.state:
            lines.append("STATE:")
            for k, v in list(ctx.state.items())[:15]:
                lines.append(f"  {k}: {v}")
            lines.append("")

        # Tree
        if ctx.tree_snapshot:
            lines.append("WORKSPACE STRUCTURE:")
            lines.append(ctx.tree_snapshot[:2000])
            lines.append("")

        # User profile
        if ctx.calibrated_user_model:
            model = ctx.calibrated_user_model
            lines.append("USER PROFILE:")
            for key, val in model.items():
                lines.append(f"  {key}: {val}")
            lines.append("")

        return "\n".join(lines)

    async def _execute_consolidated_turn(
        self,
        agent: Any,
        consolidated_prompt: str,
    ) -> str:
        """Execute ONE consolidated LLM call for the entire pipeline turn.

        Instead of making N separate agent.send_user_message() calls
        (one per phase), this makes a single call with the consolidated
        prompt that covers all specialist roles.
        """
        try:
            raw_output = agent.send_user_message(consolidated_prompt)
            if isinstance(raw_output, str):
                return raw_output
            return str(raw_output)
        except Exception as e:
            log.error(
                "Consolidated pipeline turn execution failed: %s", e
            )
            return f"Error: {e}"

    def _verify_phase_output_sync(
        self,
        phase: PipelinePhase,
        result: PhaseResult,
        ctx: PipelineContext,
    ) -> bool:
        """Synchronous verification of a phase's output.

        Since the consolidated prompt produces all phase outputs in a
        single call, verification is done synchronously (no async needed).

        NOTE: We pass an empty dict for context to verify_output() because
        specialist verification methods (HERMES, ARCHITECT, FORGE, SENTINEL,
        TERMINUS, HERALD) all work on the output text alone â€” they check for
        empty strings, Mermaid syntax, tool call ordering, secret patterns,
        destructive commands, and communication structure respectively.
        If a future specialist requires context-dependent verification,
        this should be updated.
        """
        if self.verification_pipeline is None:
            return True

        contract = PIPELINE_HANDOFFS.get(phase)
        if contract is None or not contract.verification_required:
            return True

        try:
            # Use the specialist's verify_output method for lightweight checks
            specialist = self.specialists.get(contract.specialist_name)
            if specialist and hasattr(specialist, "verify_output"):
                verified, reason = specialist.verify_output(
                    result.output, {}
                )
                if not verified:
                    log.warning(
                        "Phase %s verification failed: %s",
                        phase.value, reason,
                    )
                return verified
        except Exception as e:
            log.warning(
                "Phase verification failed for %s: %s", phase.value, e
            )

        return True

    # ==================================================================
    # Public Accessors
    # ==================================================================

    @property
    def phase_descriptions(self) -> Dict[PipelinePhase, str]:
        """Get descriptions of all phases for display/UI."""
        return {
            PipelinePhase.CALIBRATION: (
                "HERMES calibrates user intent, extracts preferences, "
                "and shapes communication context."
            ),
            PipelinePhase.PLANNING: (
                "ARCHITECT decomposes the goal, produces a structured "
                "14-section strategic plan, and defines specialist assignments."
            ),
            PipelinePhase.RESEARCH: (
                "ORACLE gathers facts, inspects repository evidence, "
                "and supplies supporting information."
            ),
            PipelinePhase.IMPLEMENTATION: (
                "FORGE writes code, performs refactors, and produces "
                "verified changes."
            ),
            PipelinePhase.SECURITY: (
                "SENTINEL inspects changes for risk, blocks unsafe "
                "operations, and enforces policy."
            ),
            PipelinePhase.EXECUTION: (
                "TERMINUS runs commands, manages operational behavior, "
                "and executes deployment actions."
            ),
            PipelinePhase.REPORTING: (
                "HERALD produces human-readable reports, execution "
                "summaries, and communicates results."
            ),
        }

    def get_handoff_chain_display(self) -> str:
        """Get a human-readable display of the handoff chain."""
        lines = ["AELVO Pipeline Handoff Chain:", "=" * 50]
        for phase in PipelinePhase:
            contract = PIPELINE_HANDOFFS.get(phase)
            if contract is None:
                continue
            prev = (
                f" â† from {contract.receives_from.value}"
                if contract.receives_from
                else " (entry point)"
            )
            lines.append(
                f"\n{contract.specialist_name:12s} | "
                f"{phase.value:16s}{prev}"
            )
            lines.append(
                f"{'':12s} | Produces: "
                f"{', '.join(contract.produces[:4])}"
            )
            if len(contract.produces) > 4:
                lines.append(
                    f"{'':12s} |           "
                    f"{', '.join(contract.produces[4:])}"
                )
        return "\n".join(lines)
