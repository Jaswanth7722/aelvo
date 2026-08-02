# orchestrator.py - Central Coordinator & Task Router for AELVO OMEGA
#
# REFACTORED: Now delegates to the canonical RuntimePipeline for task execution.
# The pipeline enforces the architecture's specialist ordering:
#   HERMES → ARCHITECT → ORACLE → FORGE → SENTINEL → TERMINUS → HERALD
#
# The Orchestrator remains the point of integration for UI, memory, and
# subsystem wiring, while the pipeline owns the execution flow.

import time
import os
import re
import json
import hashlib
import asyncio
import logging

log = logging.getLogger("aelvo.orchestrator")
from typing import List, Dict, Tuple, Any, Optional
from config.settings import CACHE_TREE_EXPIRY_SECONDS
from specialists import get_specialist
from memory.user_model import UserModelManager
from cognition.hermes_context import HermesContext
from cognition.architect_decision import ExecutionMode
from learning.agent_metrics import AgentMetricsTracker

# New Runtime Integration
from runtime_next.events.bus import EventBus
from runtime_next.capability.registry import CapabilityRegistry
from runtime_next.models.events import BaseEvent, EventType as RuntimeEventType
from runtime_next.recovery.engine import RecoveryEngine
from runtime_next.verification.pipeline import VerificationPipeline
from runtime_next.verification.sandbox_verifier import register_sandbox_verifier
from runtime_next.verification.types import (
    VerificationType, VerificationManifest, VerificationScope, VerificationResult,
)
from runtime_next.verification.events import VerificationFailedEvent as VerifFailedEvent
from runtime_next.engine.file_mutex import FileMutex
from runtime_next.engine.engine import ExecutionGraph
from runtime_next.engine.runner import NodeRunner
from runtime_next.models.node import NodeDefinition, NodeState, NodeType

# Canonical Pipeline Integration
from core.orchestration.pipeline import (
    RuntimePipeline,
    PipelineResult,
    PIPELINE_HANDOFFS,
)

# Mode B — Task Board Pipeline
from core.orchestration.task_board_pipeline import (
    TaskBoardPipeline,
    MODE_A as MODE_A_CONST,
    MODE_B as MODE_B_CONST,
)

# Focused orchestration sub-classes
from core.orchestration.router import TaskRouter
from core.orchestration.context_builder import ContextBuilder
from core.orchestration.plan_executor import PlanExecutor
from core.orchestration.verification_coordinator import VerificationCoordinator
from core.orchestration.session_manager import SessionManager
from core.orchestration.ui_notifier import UINotifier

# Architect Intelligence Integration
from runtime_next.plan.architect import ArchitectOrchestrator
from runtime_next.plan.calibration import PlanCalibrationSystem

# UI Integration — the Python TUI was removed; events stream to the web via
# the runtime EventBus (orchestrator.runtime_bus) instead of ui.events.

# Default cadence for compacting conversation history into a session summary
SESSION_SUMMARY_INTERVAL = 50

# Force-route prefix syntax users can use to manually pin specialists, e.g. `@FORGE @SENTINEL fix this`
_FORCE_ROUTE_RE = re.compile(r"^\s*((?:@[A-Z]+\s+)+)", re.IGNORECASE)


class Orchestrator:
    """The central coordinator of AELVO OMEGA.

    Delegates to focused sub-classes:
    - TaskRouter: classification, force-route parsing, calibration
    - ContextBuilder: shared context, workspace tree
    - PlanExecutor: architect plans, execution graphs, outcome recording
    - VerificationCoordinator: plan checks, sandbox verification, summaries
    - SessionManager: periodic session summarization
    """

    def __init__(self, memory_engine, kernel, base_path: str, provider_runtime=None):
        self.memory_engine = memory_engine
        self.kernel = kernel
        self.base_path = base_path
        self.provider_runtime = provider_runtime

        self.user_manager = UserModelManager(self.memory_engine.memory_collection, self.memory_engine.db)
        from core.filesystem import AelvoFileSystem
        self.fs = AelvoFileSystem(self.base_path, self.kernel)

        self.tree_cache = ""
        self.tree_cache_time = 0.0

        # Track turns for periodic session summaries
        self._turn_counter: int = 0

        # ── Focused orchestration sub-classes ──
        self.router = TaskRouter(memory_engine=self.memory_engine)
        self.context_builder = ContextBuilder(
            memory_engine=self.memory_engine,
            base_path=self.base_path,
            kernel=self.kernel,
        )

        # Track the active workspace root so the web UI can point the agent
        # at any folder (CLI/web/desktop-agent style "open folder").
        self._workspace_root = self.base_path
        self.session_manager = SessionManager(memory_engine=self.memory_engine)
        self.ui_notifier = UINotifier(self)

        # New Runtime Substrate (Phase 5, Layer 1 Integration)
        self.runtime_bus = EventBus(log_path=os.path.join(self.base_path, "runtime_events.log"))
        self.runtime_mutex = FileMutex()
        self.runtime_registry = CapabilityRegistry(workspace_root=self.base_path, event_bus=self.runtime_bus)
        
        self.env_runtime = None
        
        # Specialist Execution Bridge
        self.runtime_runner = NodeRunner(None) # agent will be set per turn
        self.runtime_graph = ExecutionGraph(
            self.runtime_bus, self.runtime_mutex,
            runner=self.runtime_runner,
        )
        self.runtime_recovery = RecoveryEngine(self.runtime_graph)
        
        # Connect Recovery Engine
        self.runtime_bus.subscribe_all(self.runtime_recovery.on_event)

        # Specialist Failure Breakers
        self.specialist_failures: Dict[str, int] = {}

        # Subscribe Orchestrator to event bus for graph checkpointing
        self.runtime_bus.subscribe_all(self.on_bus_event)

        # ── Live Agent Metrics ──
        # AgentMetricsTracker records per-specialist operational metrics from
        # runtime events and publishes agent_metrics_updated snapshots to the
        # bus (streamed to the web dashboard).
        self.agent_metrics = AgentMetricsTracker()
        self._last_metrics_publish = 0.0
        self.runtime_bus.subscribe_all(self._on_metrics_event)

        # Check for serialized graph checkpoint in SQLite and restore it
        try:
            with self.memory_engine.db as db:
                row = db.execute("SELECT value FROM state WHERE key = ?", ("runtime_graph_checkpoint",)).fetchone()
            if row:
                log.info("Found serialized graph checkpoint in SQLite. Restoring...")
                checkpoint_data = json.loads(row[0])
                from runtime_next.models.node import NodeDefinition
                for nid, node_data in checkpoint_data.get("nodes", {}).items():
                    if isinstance(node_data, dict):
                        try:
                            node = NodeDefinition(**node_data)
                            self.runtime_graph.nodes[node.id] = node
                        except Exception as e:
                            log.warning("Failed to restore node %s from checkpoint: %s", nid, e)
                self.runtime_graph.edges = [(e[0], e[1]) if isinstance(e, list) else (e[0], e[1]) if isinstance(e, tuple) else (e.get("source"), e.get("target")) for e in checkpoint_data.get("edges", [])]
                log.info("Execution graph restored from checkpoint (nodes: %d, edges: %d).", len(self.runtime_graph.nodes), len(self.runtime_graph.edges))
        except Exception as e:
            log.warning("Failed to restore graph from checkpoint: %s", e)

        # Verification Pipeline
        self.verification_pipeline = VerificationPipeline()
        self._sandbox_verifier = register_sandbox_verifier(self.verification_pipeline)
        from runtime_next.verification.code_verifier import CodeVerifier, TypeCheckVerifier
        from runtime_next.verification.graph_verifier import GraphConsistencyVerifier
        from runtime_next.verification.additional_verifiers import AdditionalVerifier
        self.verification_pipeline.register_verifier(VerificationType.LINT, CodeVerifier().create_handler())
        self.verification_pipeline.register_verifier(VerificationType.TYPECHECK, TypeCheckVerifier().create_handler())
        self.verification_pipeline.register_verifier(VerificationType.GRAPH_CONSISTENCY, GraphConsistencyVerifier().create_handler())
        
        # Register the 10 previously unhandled types
        additional_types = [
            VerificationType.UNIT_TEST,
            VerificationType.INTEGRATION_TEST,
            VerificationType.SECURITY_SCAN,
            VerificationType.RUNTIME_VALIDATION,
            VerificationType.DEPENDENCY_VALIDATION,
            VerificationType.SERIALIZATION_INTEGRITY,
            VerificationType.CAPABILITY_VALIDATION,
            VerificationType.ARCHITECTURE_VALIDATION,
            VerificationType.MUTEX_VALIDATION,
            VerificationType.REPLAY_CONSISTENCY,
        ]
        for vtype in additional_types:
            self.verification_pipeline.register_verifier(vtype, AdditionalVerifier(vtype).create_handler())
        log.info("Registered code correctness, graph consistency and all 10 additional verifiers")

        # Verification coordinator
        self.verification_coordinator = VerificationCoordinator(
            verification_pipeline=self.verification_pipeline,
            runtime_graph=self.runtime_graph,
            runtime_bus=self.runtime_bus,
        )

        # Connect verification pipeline events to the runtime event bus
        async def _verification_event_to_bus(event):
            try:
                node_id = getattr(event, "node_id", "")
                rtype = RuntimeEventType.LOG_MESSAGE
                if isinstance(event, VerifFailedEvent):
                    rtype = RuntimeEventType.VERIFICATION_FAILED
                bus_event = BaseEvent(
                    id=getattr(event, "event_id", ""),
                    type=rtype,
                    payload={
                        "verification_source": "sandbox",
                        "node_id": node_id,
                    },
                )
                await self.runtime_bus.publish(bus_event)
            except Exception as e:
                log.error(f"Verification→bus publish error: {e}")
        self.verification_pipeline.on_event(_verification_event_to_bus)

        # Architect Intelligence
        self._architect_orchestrator = None
        self._plan_calibration = PlanCalibrationSystem(
            storage_path=os.path.join(self.base_path, ".aelvo_runtime")
        )

        # Plan executor
        self.plan_executor = PlanExecutor(
            memory_engine=self.memory_engine,
            runtime_bus=self.runtime_bus,
            plan_calibration=self._plan_calibration,
        )

        # Wire calibration → recovery link
        self.runtime_recovery.link_calibration_system(self._plan_calibration)

        # Start substrate (background tasks)
        try:
            loop = asyncio.get_running_loop()
            self._bus_task = loop.create_task(self._safe_start_bus())
        except RuntimeError:
            self._bus_task = None
            log.warning("No event loop running during Orchestrator init - bus task deferred")
        try:
            loop = asyncio.get_running_loop()
            self._registry_task = loop.create_task(self._safe_start_registry())
        except RuntimeError:
            self._registry_task = None
            log.warning("No event loop running during Orchestrator init - registry task deferred")

        # ── Canonical RuntimePipeline (Mode A) ──
        self.pipeline: RuntimePipeline = RuntimePipeline(self)

        # ── Task Board Pipeline (Mode B) ──
        self.task_board_pipeline: TaskBoardPipeline = TaskBoardPipeline(self)

        # UI Integration — no Python TUI; the web reads runtime_bus directly.
        self.ui_panel = None
        self.event_bus = None

        # Link context_builder back to runtime_registry and provider_runtime
        self.context_builder.runtime_registry = self.runtime_registry
        self.context_builder.provider_runtime = self.provider_runtime
        self.context_builder.user_manager = self.user_manager

    async def _safe_start_bus(self):
        try:
            await self.runtime_bus.start()
        except Exception as e:
            log.error(f"Runtime event bus failed to start: {e}")

    async def _safe_start_registry(self):
        try:
            await self.runtime_registry.start_monitoring()
        except Exception as e:
            log.error(f"Capability registry monitoring failed to start: {e}")

    # ------------------------------------------------------------------
    # Workspace tree
    # ------------------------------------------------------------------
    def get_workspace_tree(self) -> str:
        """Delegate to ContextBuilder for workspace tree."""
        # Keep local cache for backward compatibility
        now = time.time()
        if now - self.tree_cache_time < CACHE_TREE_EXPIRY_SECONDS and self.tree_cache:
            return self.tree_cache
        tree = self.context_builder.get_workspace_tree()
        self.tree_cache = tree
        self.tree_cache_time = now
        return tree

    def set_workspace_root(self, new_root: str) -> str:
        """Point the agent at a new workspace folder.

        Re-jails the filesystem (all file tools resolve against the new root),
        updates the shared-context builder, invalidates caches, and refreshes
        the capability registry so the agent operates directly on the chosen
        folder — matching how CLI/web/desktop coding agents open a folder.

        Returns the resolved absolute path.
        """
        resolved = self.fs.set_base_path(new_root)
        self.base_path = resolved
        self._workspace_root = resolved
        self.context_builder.base_path = resolved
        self.tree_cache = ""
        self.tree_cache_time = 0.0
        if hasattr(self, "runtime_registry") and self.runtime_registry is not None:
            try:
                self.runtime_registry.workspace_root = resolved
            except Exception as exc:
                log.debug("Failed to update registry workspace root: %s", exc)
        log.info("Workspace switched to %s", resolved)
        return resolved

    @property
    def workspace_root(self) -> str:
        """The currently active workspace folder for the agent."""
        return self._workspace_root

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------
    def _parse_force_route(self, task: str) -> Tuple[List[str], str]:
        """Delegate to TaskRouter for force-route parsing."""
        return self.router.parse_force_route(task)

    def classify_task(self, task: str) -> List[str]:
        """Delegate to TaskRouter for activation scoring."""
        return self.router.classify_task(task)

    def resolve_execution_order(self, active_specialists: List[str]) -> List[str]:
        """Delegate to TaskRouter for execution ordering."""
        return self.router.resolve_execution_order(active_specialists)

    # ------------------------------------------------------------------
    # Calibration
    # ------------------------------------------------------------------
    def _calibrate_raw_output(self, raw_output: str, context: Dict[str, Any]) -> str:
        """Delegate to TaskRouter for output calibration."""
        signals = context.get("signals", {})
        return self.router.calibrate_raw_output(raw_output, signals)

    # ------------------------------------------------------------------
    # Shared context
    # ------------------------------------------------------------------
    def build_shared_context(self, task: str, active_specialists: List[str]) -> Dict[str, Any]:
        """Delegate to ContextBuilder for shared context assembly."""
        ctx = self.context_builder.build_shared_context(task, active_specialists)
        # Add orchestrator-specific keys
        ctx["fs"] = self.fs
        ctx["runtime_bus"] = self.runtime_bus
        ctx["event_bus"] = self.event_bus
        ctx["user_manager"] = self.user_manager
        ctx["provider_runtime"] = self.provider_runtime
        return ctx


    # ------------------------------------------------------------------
    # Session summarization
    # ------------------------------------------------------------------
    def _maybe_summarize_session(self, agent) -> Optional[str]:
        """Delegate to SessionManager for session summarization."""
        return self.session_manager.maybe_summarize_session(agent)


    # ==================================================================
    # Architect Intelligence Integration
    # ==================================================================

    def _get_architect_orchestrator(self) -> Optional[ArchitectOrchestrator]:
        """Delegate to PlanExecutor for architect orchestrator."""
        # Sync cognitive_engine to plan_executor if not yet set
        if hasattr(self, 'cognitive_engine'):
            self.plan_executor.cognitive_engine = self.cognitive_engine
        return self.plan_executor.get_architect_orchestrator()

    def _create_architect_plan(self, task: str) -> Optional[Any]:
        """Delegate to PlanExecutor for architect plan creation."""
        if hasattr(self, 'cognitive_engine'):
            self.plan_executor.cognitive_engine = self.cognitive_engine
        return self.plan_executor.create_plan(
            task, self.base_path, self.get_workspace_tree
        )

    def _extract_specialists_from_plan(self, plan) -> List[str]:
        """Delegate to PlanExecutor."""
        return self.plan_executor.extract_specialists_from_plan(
            plan, self.resolve_execution_order
        )

    def _build_graph_from_plan(
        self, plan, task_id: str, effective_input: str,
    ) -> Tuple[ExecutionGraph, Optional[str], Dict[str, str]]:
        """Delegate to PlanExecutor."""
        return self.plan_executor.build_graph_from_plan(
            plan, task_id, effective_input,
            self.runtime_bus, self.runtime_mutex, self.runtime_runner,
        )

    def _record_plan_outcome(
        self, plan, success: bool, ordered_names: List[str],
        failed_nodes: list, total_duration_ms: float,
        strategy_class: str = "general",
        plan_verification_results: Optional[List[VerificationResult]] = None,
    ):
        """Delegate to PlanExecutor."""
        self.plan_executor.record_plan_outcome(
            plan, success, ordered_names, failed_nodes,
            total_duration_ms, strategy_class, plan_verification_results,
        )

    def _record_verification_calibration(
        self, plan: Any,
        all_verification_results: List[Any],
        plan_verification_results: List[Any],
    ):
        """Delegate to PlanExecutor."""
        self.plan_executor.record_verification_calibration(
            plan, all_verification_results, plan_verification_results,
        )

    # ==================================================================
    # Plan Verification Integration — Delegated to VerificationCoordinator
    # ==================================================================

    def _register_plan_checks(self, plan) -> set:
        """Delegate to VerificationCoordinator."""
        return self.verification_coordinator.register_plan_checks(plan)

    async def _run_plan_verification(
        self, plan, phase_node_map: Dict[str, str],
    ) -> List[VerificationResult]:
        """Delegate to VerificationCoordinator."""
        return await self.verification_coordinator.run_plan_verification(
            plan, phase_node_map,
        )

    def _build_verification_summary(
        self, plan, results: List[VerificationResult],
    ) -> str:
        """Delegate to VerificationCoordinator."""
        phase_names = {
            p.id: p.name for p in plan.execution_strategy.phases
        } if plan else {}
        return self.verification_coordinator.build_verification_summary(
            plan, results, phase_names,
        )

    # ── Architect Mode Evaluation ────────────────────────────────

    def _evaluate_mode_with_architect(
        self,
        hermes_ctx: HermesContext,
    ) -> str:
        """Use the Architect specialist to evaluate the ideal execution mode.

        Called when no explicit @MODE_A/@MODE_B prefix is present.
        The Architect evaluates the HermesContext and returns
        MODE_A_CONST or MODE_B_CONST.

        Falls back to MODE_A_CONST if the Architect specialist is
        not available or the evaluation fails.
        """
        architect = get_specialist("ARCHITECT")
        if architect is None or not hasattr(architect, "select_execution_mode"):
            return MODE_A_CONST

        try:
            result = architect.select_execution_mode(
                task=hermes_ctx.task,
                risk_profile=hermes_ctx.risk_profile,
                complexity=hermes_ctx.complexity,
                goals=hermes_ctx.goals,
                constraints=dict(hermes_ctx.constraints),
                hermes_context=hermes_ctx,
            )
            mode = result.get("mode")
            if mode is not None:
                # Map ExecutionMode enum to MODE_A/MODE_B constants
                if mode == ExecutionMode.COLLABORATIVE:
                    return MODE_B_CONST  # "task_board"
                return MODE_A_CONST  # "consolidated"
            return MODE_A_CONST
        except Exception as e:
            log.warning("Architect mode evaluation failed: %s", e)
            return MODE_A_CONST

    # ── HermesContext — Global Cognition ────────────────────────────
    _hermes_context: Optional[HermesContext] = None

    @property
    def hermes_context(self) -> Optional[HermesContext]:
        """The immutable HermesContext for the current turn.

        Set at the start of execute_turn(), consumed immutably
        by every component throughout the turn.
        """
        return self._hermes_context

    @hermes_context.setter
    def hermes_context(self, ctx: HermesContext) -> None:
        """Set the HermesContext for the current turn.

        Once set, this context is immutable — no component should
        modify it (the frozen=True model_config enforces this).
        """
        self._hermes_context = ctx

    async def _create_hermes_context(
        self,
        user_input: str,
        agent: Any,
    ) -> HermesContext:
        """Create the immutable HermesContext for this turn.

        Uses HERMES (the global cognition specialist) to analyze
        the user input, build the user model, assess risk, and
        produce the global cognition context consumed by all components.
        """
        hermes = get_specialist("HERMES")
        if hermes is None:
            log.warning("HERMES specialist not found — creating default HermesContext")
            return HermesContext.create(
                task=user_input,
                session_id=getattr(agent, "session_id", ""),
            )

        conversation_history = getattr(agent, "conversation_history", [])
        session_id = getattr(agent, "session_id", "")

        context = await hermes.create_hermes_context(
            task=user_input,
            conversation_history=conversation_history,
            memory_engine=self.memory_engine,
            session_id=session_id,
        )

        log.info(
            "HermesContext: intent=%s | risk=%s | complexity=%d/10 | goals=%d | perms=%s",
            context.intent[:40], context.risk_profile,
            context.complexity, len(context.goals),
            context.execution_permissions,
        )

        # Notify UI
        self._notify_ui_specialist_completed(
            "HERMES",
            f"Global cognition: {context.intent[:40]} | "
            f"risk={context.risk_profile} | complexity={context.complexity}/10",
        )

        return context

    # ------------------------------------------------------------------
    # Turn execution
    # ------------------------------------------------------------------
    async def execute_turn(self, agent, user_input: str, session_tracker=None, tui_session=None, stream_callback=None, mcp_cli=None, db_path=None) -> Dict[str, Any]:
        """Main orchestrator turn runner.

        Now delegates to the canonical RuntimePipeline which enforces:
            HERMES → ARCHITECT → ORACLE → FORGE → SENTINEL → TERMINUS → HERALD

        Falls back to force-route dispatch for manual specialist pinning.
        """
        self._turn_counter += 1
        self.session_manager.increment_turn()
        self.runtime_runner.agent = agent
        time.time()

        # 1. Preparation (UI, Force-Route, Mode Detection)
        task_id = f"turn_{self._turn_counter}"
        self._notify_ui_task_created(task_id, user_input[:50], "PIPELINE")
        self._notify_ui_specialist_activated("HERMES", 0.78, "Calibrating user intent")

        # Mode detection BEFORE force-route check (to prevent @MODE_B
        # from being consumed by the @SPECIALIST force-route regex)
        #
        # Phase 8 — Dual-Mode Pipeline:
        #   - @MODE_B prefix → explicit Mode B (Collaborative, task-board-based)
        #   - @MODE_A prefix → explicit Mode A (Consolidated, single-prompt)
        #   - No prefix     → Architect evaluates HermesContext to decide
        #                     (risk>=high, complexity>4, security concerns,
        #                      consensus required, 5+ files, 4+ goals all
        #                      trigger Mode B; otherwise Mode A)
        has_explicit_mode = user_input.strip().lower().startswith(("@mode_a", "@mode_b"))
        detected_mode = TaskBoardPipeline.detect_mode(user_input)
        input_for_routing = TaskBoardPipeline.strip_mode_prefix(user_input)

        forced, stripped_input = self._parse_force_route(input_for_routing)
        effective_input = stripped_input if forced else input_for_routing

        # 2. Create HermesContext — Immutable Global Cognition
        #    Created BEFORE the force-route check so that even
        #    @SPECIALIST dispatches carry global cognition context.
        #    Per Amendment 4: Hermes is NOT preprocessing.
        conversation_history = getattr(agent, "conversation_history", [])
        self.hermes_context = await self._create_hermes_context(
            user_input=effective_input,
            agent=agent,
        )

        # 3. Handle forced routes — use legacy specialist dispatch
        #    HermesContext is available via self.hermes_context for
        #    the forced route handler to pass to graph context.
        if forced:
            log.info(
                "Force route detected: %s — using legacy specialist dispatch",
                forced,
            )
            # Inject HermesContext into the forced-route context
            return await self._execute_forced_route(
                agent, forced, effective_input, task_id,
                session_tracker=session_tracker,
                tui_session=tui_session,
                stream_callback=stream_callback,
                mcp_cli=mcp_cli,
                db_path=db_path,
            )

        # 4. HermesContext is created and stored on self.hermes_context

        # ── Dual-Mode Selection ────────────────────────────────────
        # If no explicit @MODE_A/@MODE_B prefix, let the Architect
        # evaluate the HermesContext and decide the best mode.
        if not has_explicit_mode:
            architect_mode = self._evaluate_mode_with_architect(
                self.hermes_context,
            )
            if architect_mode != MODE_A_CONST:
                detected_mode = architect_mode
                log.info(
                    "Architect selected mode=%s (risk=%s, complexity=%d, goals=%d)",
                    detected_mode,
                    self.hermes_context.risk_profile,
                    self.hermes_context.complexity,
                    len(self.hermes_context.goals),
                )

        if detected_mode == MODE_B_CONST:
            # ── Mode B: Task-Board-Based Collaboration ──
            log.info("Mode B detected — using task-board pipeline")
            self._notify_ui_specialist_thinking(
                "ARCHITECT",
                "Decomposing into task-board tasks (Mode B)",
            )
            self._notify_ui_specialist_thinking(
                "HERMES",
                "Preparing global cognition for Mode B",
            )

            pipeline_result = await self.task_board_pipeline.run(
                user_input=input_for_routing,
                agent=agent,
                conversation_history=conversation_history,
                hermes_context=self.hermes_context,
            )

            log.info(
                "Mode B pipeline completed: success=%s, phases=%d",
                pipeline_result.success,
                len(pipeline_result.phases_executed),
            )
        else:
            # ── Mode A: Consolidated Prompt Pipeline (default) ──
            log.info("Mode A (default) — using consolidated prompt pipeline")

            # Notify specialist activation for all phases that may run
            self._notify_ui_specialist_activated(
                "ARCHITECT", 0.91, "Generating strategic plan",
            )
            self._notify_ui_specialist_thinking(
                "ARCHITECT",
                "Decomposing goals and planning execution",
            )
            self._notify_ui_specialist_thinking(
                "HERMES",
                "Processing consolidated pipeline prompt",
            )

            pipeline_result = await self.pipeline.run(
                user_input=effective_input,
                agent=agent,
                conversation_history=conversation_history,
                hermes_context=self.hermes_context,
            )

        # 4. Submit goal to CognitiveEngine for learning
        if hasattr(self, "cognitive_engine") and self.cognitive_engine:
            try:
                g = self.cognitive_engine.submit_goal(
                    effective_input, owner="user"
                )
                self.cognitive_goal_id = g.id
                sub_descriptions = [
                    "Understand and analyze the request",
                    "Gather relevant context and research",
                    "Execute solution via specialists",
                    "Verify and summarize results",
                ]
                self.cognitive_engine.decompose_goal(g.id, sub_descriptions)
                plan = self.cognitive_engine.plan_goal(g.id)
                self.cognitive_plan = plan
                log.info(
                    "CognitiveEngine: goal %s planned (%d nodes)",
                    g.id, len(plan.nodes) if plan and plan.nodes else 0,
                )
            except Exception as ce_err:
                log.warning(
                    "CognitiveEngine submission/planning failed: %s", ce_err
                )

        # 5. Notify CognitiveEngine of failures
        if not pipeline_result.success:
            for phase, error in pipeline_result.failures:
                log.warning(
                    "Pipeline phase %s failed: %s", phase.value, error,
                )
            if (
                getattr(self, "cognitive_plan", None)
                and hasattr(self, "cognitive_engine")
                and self.cognitive_engine
            ):
                for phase, err in pipeline_result.failures:
                    try:
                        self.cognitive_engine.handle_failure(
                            self.cognitive_plan, phase.value, err,
                        )
                    except Exception as _ex: log.warning("Silenced exception: %s", _ex)

        # 6. Store execution trace in cognitive memory on success
        if pipeline_result.success:
            if (
                getattr(self, "cognitive_goal_id", None)
                and hasattr(self, "cognitive_engine")
                and self.cognitive_engine
            ):
                try:
                    from cognition.types import MemoryType
                    self.cognitive_engine.store_memory(
                        memory_type=MemoryType.EXECUTION_TRACE,
                        content=(
                            f"Pipeline completed: "
                            f"{effective_input[:200]}\n"
                            f"Output: "
                            f"{pipeline_result.final_output[:500]}"
                        ),
                        importance=0.6,
                        source_goal_id=self.cognitive_goal_id,
                    )
                except Exception as ce_err:
                    log.warning(
                        "CognitiveEngine memory storage failed: %s", ce_err
                    )

        # 7. Periodic session summary
        session_summary = self._maybe_summarize_session(agent)
        audit_traces = []
        if session_summary:
            audit_traces.append(
                "[ORCHESTRATOR] Persisted periodic session summary to memory."
            )

        # Add pipeline audit traces
        if pipeline_result.recovery_actions:
            audit_traces.append(
                f"[PIPELINE] Recovery actions: "
                f"{', '.join(pipeline_result.recovery_actions)}"
            )
        if pipeline_result.verification_summary:
            audit_traces.append(
                f"[PIPELINE] Verification:\n"
                f"{pipeline_result.verification_summary}"
            )

        # Notify all specialists that completed based on phases executed
        for p in pipeline_result.phases_executed:
            contract = PIPELINE_HANDOFFS.get(p)
            if contract:
                success = p not in [f[0] for f in pipeline_result.failures]
                self._notify_ui_specialist_completed(contract.specialist_name, f"{p.value} phase completed", success)

        # Update specialist failure tracking / circuit breakers
        if not pipeline_result.success:
            for phase, error in pipeline_result.failures:
                contract = PIPELINE_HANDOFFS.get(phase)
                if contract:
                    spec_name = contract.specialist_name
                    self.specialist_failures[spec_name] = self.specialist_failures.get(spec_name, 0) + 1
                    log.warning("Specialist %s failure logged. Total failures: %d", spec_name, self.specialist_failures[spec_name])
        else:
            # On success, reset failures for all executed phases
            for p in pipeline_result.phases_executed:
                contract = PIPELINE_HANDOFFS.get(p)
                if contract:
                    spec_name = contract.specialist_name
                    self.specialist_failures[spec_name] = 0

        # Wire the graph-based tool executor into NodeRunner
        # This routes tool execution through the ExecutionGraph infrastructure
        # instead of the legacy memory_engine.execute_turn() path.
        self.runtime_runner._tool_executor = self._graph_tool_executor

        # Call tool loop to handle any tools emitted in the raw output
        final_answer = await self._execute_tool_loop(
            agent,
            pipeline_result.final_output,
            session_tracker=session_tracker,
            tui_session=tui_session,
            stream_callback=stream_callback,
            mcp_cli=mcp_cli,
            db_path=db_path,
        )

        # 8. UI Integration
        self._notify_ui_task_completed(task_id, pipeline_result.success)

        # 9. Return result in legacy format for backward compatibility
        specialist_names = []
        for p in pipeline_result.phases_executed:
            contract = PIPELINE_HANDOFFS.get(p)
            if contract:
                specialist_names.append(contract.specialist_name)
            else:
                specialist_names.append(p.value)

        return {
            "status": "success" if pipeline_result.success else "partial_failure",
            "output": final_answer,
            "specialists_active": specialist_names,
            "audit_traces": audit_traces,
            "turn": self._turn_counter,
            "forced_route": bool(forced),
            "session_summary": session_summary,
            "architect_plan_used": True,
            "plan_display": self._build_pipeline_display(pipeline_result),
            "pipeline_result": pipeline_result,
        }

    async def _safe_verify_tool_output(
        self,
        node_id: str,
        manifest: VerificationManifest,
        scope: VerificationScope,
    ) -> None:
        """Run verification in background. Results publish to EventBus;
        recovery engine handles VERIFICATION_FAILED events."""
        try:
            await self.verification_pipeline.verify(node_id, manifest, scope, {})
        except Exception as e:
            log.debug("Background tool verification failed: %s", e)

    async def _graph_tool_executor(self, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool call directly through the tool registry.

        This replaces the legacy `TurnAgent + memory_engine.execute_turn()` path
        by routing tool execution through the NodeRunner, which is wired into
        the ExecutionGraph, VerificationPipeline, and RecoveryEngine.

        This enables:
        - Graph-traced tool execution for full observability
        - Verification pipeline checks on tool outputs
        - Recovery engine integration on tool failures
        - EventBus events for all tool lifecycle events
        """
        tool_def = self.memory_engine.tools.get(tool_name)
        if tool_def is None:
            return {"status": "error", "logs": f"Unknown tool: {tool_name}", "executed": {}}

        fn = tool_def.get("fn")
        if fn is None:
            return {"status": "error", "logs": f"Tool {tool_name} has no handler in registry", "executed": {}}

        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(**args)
            else:
                return fn(**args)
        except Exception as e:
            log.error("Tool %s execution failed: %s", tool_name, e)
            return {"status": "error", "logs": str(e), "executed": {}}

    async def _execute_forced_route(
        self, agent, forced_names: List[str], task: str, task_id: str,
        session_tracker=None, tui_session=None, stream_callback=None,
        mcp_cli=None, db_path=None
    ) -> Dict[str, Any]:
        """Execute a forced-route task using legacy graph dispatch.

        This is the fallback path for @SPECIALIST prefixes.
        It uses the original graph building logic with the specified
        specialists only.
        """
        # Check circuit breakers for the forced specialists
        for name in forced_names:
            fail_count = self.specialist_failures.get(name, 0)
            if fail_count >= 3:
                log.error("Circuit breaker TRIPPED for specialist %s (failures: %d)", name, fail_count)
                return {
                    "status": "failure",
                    "output": f"Circuit breaker TRIPPED for specialist {name}. Cannot proceed.",
                    "specialists_active": forced_names,
                    "audit_traces": [f"[ORCHESTRATOR] Circuit breaker TRIPPED for specialist {name}."],
                    "turn": self._turn_counter,
                    "forced_route": True,
                    "session_summary": None,
                    "architect_plan_used": False,
                    "plan_display": None,
                }

        # Wire graph tool executor for forced-route tool execution
        self.runtime_runner._tool_executor = self._graph_tool_executor

        # Build graph nodes in priority order
        ordered = self.resolve_execution_order(forced_names)
        prev_node_id = None
        for name in ordered:
            node_id = f"{task_id}_{name}"
            node = NodeDefinition(
                id=node_id,
                description=task,
                specialist=name,
            )
            self.runtime_graph.add_node(node)
            if prev_node_id:
                self.runtime_graph.add_edge(prev_node_id, node_id)
            prev_node_id = node_id

        # Build shared context
        context = self.build_shared_context(task, ordered)
        context["signals"] = self.user_manager.extract_signals_from_history(
            getattr(agent, "conversation_history", [])
        )
        context["forced_route"] = True

        # Notify each specialist activation and thinking
        for name in ordered:
            self._notify_ui_specialist_activated(name, 1.0, f"Force-routed: {task[:50]}")
            self._notify_ui_specialist_thinking(name, "Executing via forced-route graph")

        # Execute
        log.info(
            "Force-route: Starting graph execution with %s", ordered
        )
        await self.runtime_graph.start(context)

        # Notify completion for each specialist
        for name in ordered:
            self._notify_ui_specialist_completed(name, "Force-route execution completed")

        # Harvest results
        last_node = self.runtime_graph.nodes.get(prev_node_id) if prev_node_id else None
        raw_output = ""
        if last_node and last_node.result:
            raw_output = last_node.result.get("output", "")

        self._notify_ui_task_completed(task_id, True)

        final_answer = await self._execute_tool_loop(
            agent,
            raw_output,
            session_tracker=session_tracker,
            tui_session=tui_session,
            stream_callback=stream_callback,
            mcp_cli=mcp_cli,
            db_path=db_path,
        )

        return {
            "status": "success",
            "output": final_answer,
            "specialists_active": ordered,
            "audit_traces": ["[ORCHESTRATOR] Force-route execution completed."],
            "turn": self._turn_counter,
            "forced_route": True,
            "session_summary": None,
            "architect_plan_used": False,
            "plan_display": None,
        }

    def _build_pipeline_display(
        self, pipeline_result: PipelineResult
    ) -> str:
        """Build a human-readable display of the pipeline execution."""
        lines = [
            "",
            "  ── AELVO PIPELINE EXECUTION ──",
            f"  Phases: {' → '.join(p.value for p in pipeline_result.phases_executed)}",
            f"  Result: {'✅ SUCCESS' if pipeline_result.success else '❌ FAILURE'}",
            f"  Duration: {pipeline_result.total_duration_ms:.0f}ms",
            f"  Memory: {'Consolidated' if pipeline_result.memory_consolidated else 'Not consolidated'}",
        ]

        if pipeline_result.failures:
            lines.append("  Failures:")
            for phase, error in pipeline_result.failures:
                lines.append(f"    ✗ {phase.value}: {error[:100]}")

        if pipeline_result.verification_summary:
            lines.append("")
            lines.append("  Verification:")
            lines.append(pipeline_result.verification_summary)

        if pipeline_result.recovery_actions:
            lines.append("")
            lines.append("  Recovery Actions:")
            for action in pipeline_result.recovery_actions:
                lines.append(f"    ↻ {action}")

        lines.append("")
        return "\n".join(lines)

    def get_pipeline_handoff_chain(self) -> str:
        """Get a human-readable display of the canonical pipeline handoff chain."""
        return self.pipeline.get_handoff_chain_display() if self.pipeline else "Pipeline not initialized"
    
    # ------------------------------------------------------------------
    # UI Integration Methods (Direct, delegated to self.ui_notifier)
    # ------------------------------------------------------------------
    def _notify_ui_task_created(self, task_id: str, task_name: str, specialist: str):
        """Notify UI of task creation."""
        self.ui_notifier.notify_task_created(task_id, task_name, specialist)
    
    def _notify_ui_task_completed(self, task_id: str, success: bool):
        """Notify UI of task completion."""
        self.ui_notifier.notify_task_completed(task_id, success)

    def _notify_ui_specialist_activated(self, specialist: str, score: float = 0.0, action: str = ""):
        """Notify UI that a specialist has been activated."""
        self.ui_notifier.notify_specialist_activated(specialist, score, action)

    def _notify_ui_specialist_thinking(self, specialist: str, action: str = ""):
        """Notify UI that a specialist is processing."""
        self.ui_notifier.notify_specialist_thinking(specialist, action)

    def _notify_ui_specialist_completed(self, specialist: str, summary: str = "", success: bool = True):
        """Notify UI that a specialist has completed its work."""
        self.ui_notifier.notify_specialist_completed(specialist, summary, success)

    # ------------------------------------------------------------------
    # Sandbox Verification Integration
    # ------------------------------------------------------------------

    async def _verify_sandbox_results(self) -> list:
        """Delegate to VerificationCoordinator."""
        return await self.verification_coordinator.verify_sandbox_results()

    def get_ui_status(self) -> Dict[str, Any]:
        """Get current UI status."""
        if self.ui_panel:
            return self.ui_panel.get_status()
        return {"ui_available": False}

    async def _on_metrics_event(self, event):
        """Record operational metrics from runtime events and publish snapshots.

        Subscribes to the runtime EventBus. Maps runtime event types to
        AgentMetricsTracker counters, then publishes an
        ``agent_metrics_updated`` snapshot (rate-limited to ~1/sec) so the
        web dashboard can render live per-agent metrics.
        """
        etype = getattr(event, "type", None)
        payload = getattr(event, "payload", {}) or {}

        try:
            if etype == RuntimeEventType.BLACKBOARD_PUBLICATION:
                self.agent_metrics.record_oracle_finding()
            elif etype == RuntimeEventType.FINDING_CONSUMED:
                self.agent_metrics.record_oracle_finding(consumed=True)
            elif etype == RuntimeEventType.CHALLENGE_RAISED:
                self.agent_metrics.record_sentinel_review(challenged=True)
            elif etype == RuntimeEventType.CONSENSUS_FORMED:
                raw = str(payload.get("outcome") or payload.get("event_name") or "agreed").upper()
                outcome = {
                    "APPROVED": "agreed",
                    "APPROVED_WITH_RISK": "agreed",
                    "REQUIRES_REVISION": "requires_revision",
                    "REJECTED": "escalated",
                    "ESCALATED": "escalated",
                }.get(raw, "agreed")
                self.agent_metrics.record_consensus_outcome(outcome=outcome)
            elif etype == RuntimeEventType.ARCHITECT_DECISION:
                outcome = str(payload.get("outcome") or "approve").lower()
                self.agent_metrics.record_architect_decision(outcome=outcome)
            elif etype in (
                RuntimeEventType.RECOVERY_INITIATED,
                RuntimeEventType.RECOVERY_COMPLETED,
            ):
                self.agent_metrics.record_recovery_attempt(
                    success=etype == RuntimeEventType.RECOVERY_COMPLETED,
                )
            elif etype == RuntimeEventType.EXECUTION_COMPLETED:
                exit_code = payload.get("exit_code", 0)
                self.agent_metrics.record_forge_implementation(
                    success=(exit_code == 0),
                )
            else:
                return
        except Exception as exc:
            log.debug("Agent metrics recording failed: %s", exc)
            return

        await self._publish_agent_metrics()

    async def _publish_agent_metrics(self) -> None:
        """Publish the current metrics report to the runtime bus.

        Rate-limited to one snapshot per second to keep the event stream
        clean while still streaming live metrics to the web dashboard.
        """
        now = time.time()
        if now - self._last_metrics_publish < 1.0:
            return
        self._last_metrics_publish = now
        try:
            report = self.agent_metrics.generate_report()
            await self.runtime_bus.publish(BaseEvent(
                id=f"metrics_{int(now * 1000)}",
                type=RuntimeEventType.AGENT_METRICS_UPDATED,
                payload=report,
            ))
        except Exception as exc:
            log.debug("Agent metrics publish failed: %s", exc)

    async def on_bus_event(self, event):
        """Listen to event bus for NODE_TRANSITION events to auto-checkpoint graph."""
        if getattr(event, "type", None) == RuntimeEventType.NODE_TRANSITION:
            log.info("NODE_TRANSITION event detected. Checkpointing execution graph...")
            try:
                # Serialize graph dict
                nodes_data = {nid: node_def.model_dump() if hasattr(node_def, "model_dump") else str(node_def) for nid, node_def in self.runtime_graph.nodes.items()}
                data = {
                    "nodes": nodes_data,
                    "edges": list(self.runtime_graph.edges),
                }
                # Store in SQLite state table
                with self.memory_engine.db as db:
                    db.execute(
                        "INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)",
                        ("runtime_graph_checkpoint", json.dumps(data, default=str))
                    )
                log.info("Successfully checkpointed execution graph to SQLite.")
            except Exception as e:
                log.error("Failed to auto-checkpoint execution graph: %s", e)

    def get_health_status(self) -> Dict[str, Any]:
        """Expose system health checking databases, filesystem, bus, and specialists."""
        status = {
            "status": "healthy",
            "timestamp": time.time(),
            "components": {}
        }
        
        # 1. Check databases
        try:
            if self.memory_engine and self.memory_engine.db:
                self.memory_engine.db.execute("SELECT 1").fetchone()
                status["components"]["memory_db"] = {"status": "healthy"}
            else:
                status["components"]["memory_db"] = {"status": "unhealthy", "error": "Not initialized"}
                status["status"] = "degraded"
        except Exception as e:
            status["components"]["memory_db"] = {"status": "unhealthy", "error": str(e)}
            status["status"] = "degraded"

        # 2. Check filesystem access
        try:
            test_file = os.path.join(self.base_path, ".health_check_temp")
            with open(test_file, "w") as f:
                f.write("health_ok")
            os.remove(test_file)
            status["components"]["filesystem"] = {"status": "healthy"}
        except Exception as e:
            status["components"]["filesystem"] = {"status": "unhealthy", "error": str(e)}
            status["status"] = "unhealthy"

        # 3. Check Event Bus health
        try:
            if self.runtime_bus and not self.runtime_bus._stopped:
                status["components"]["event_bus"] = {"status": "healthy"}
            else:
                status["components"]["event_bus"] = {"status": "unhealthy", "error": "Stopped or missing"}
                status["status"] = "degraded"
        except Exception as e:
            status["components"]["event_bus"] = {"status": "unhealthy", "error": str(e)}
            status["status"] = "degraded"

        # 4. Check specialist breaker states
        breakers = {}
        for spec, count in self.specialist_failures.items():
            breakers[spec] = {
                "failures": count,
                "tripped": count >= 3
            }
            if count >= 3:
                status["status"] = "degraded"
        status["components"]["specialists"] = {
            "status": "healthy" if status["status"] != "degraded" else "degraded",
            "details": breakers
        }
        
        return status

    async def _execute_tool_loop(
        self,
        agent,
        raw_output: str,
        session_tracker=None,
        tui_session=None,
        stream_callback=None,
        mcp_cli=None,
        db_path=None,
    ) -> str:
        """Unified, async-safe execution loop that runs LLM-generated tool calls or kernel commands."""
        from core.orchestration.parser import parse_llm_output
        from core.rag import MemorySearcher
        import sqlite3
        import datetime

        if db_path is None:
            db_path = os.path.join(self.base_path, "memory.db")

        output_type, payload = parse_llm_output(raw_output)
        final_answer = raw_output

        if output_type == "kernel_command":
            for cmd in payload:
                log.info("Executing kernel command from loop: %s...", cmd[:50])
                if tui_session:
                    await tui_session.emit_system(f"Kernel command: {cmd[:50]}")
                
                if cmd.lower().startswith("#mcp"):
                    if mcp_cli:
                        result = await mcp_cli.execute(cmd)
                    else:
                        result = {"status": "REJECTED", "error": "MCP Subsystem not initialized"}
                else:
                    result = self.kernel.parse_and_execute(cmd)
                
                if not tui_session:
                    print(f"\n[KERNEL] {json.dumps(result, indent=2)}\n")
                
                agent.feed_result({"type": "kernel_command", "result": result})
                if session_tracker:
                    session_tracker.record_tool("kernel", {"command": cmd[:80]}, result.get("status", "SUCCESS").lower())
                    session_tracker.record_answer(json.dumps(result)[:300])
                    session_tracker.save(db_path)
            
            final_answer = json.dumps({"status": "completed", "type": "kernel_command"})

        elif output_type == "tool_calls":
            MAX_STEPS = 30
            current_batch = payload
            batch_complete = False

            for step in range(MAX_STEPS):
                if batch_complete:
                    break

                for call in current_batch:
                    tool_name = call.get("tool", "")
                    tool_args = call.get("args", {})

                    if tool_name == "respond":
                        msg = tool_args.get("message", "")
                        final_answer = msg
                        if tui_session:
                            await tui_session.emit_system(f"AELVO: {msg[:80]}")
                        else:
                            print(f"\n[AELVO] {msg}\n")
                        
                        if stream_callback:
                            stream_callback(msg)

                        retain = tool_args.get("retain_memory")
                        if retain:
                            with self.memory_engine.collection_guard() as coll:
                                searcher = MemorySearcher(coll)
                                deduplicated = searcher.resolve_conflict(retain, meta_type="fact")
                            if not deduplicated:
                                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                m_id = hashlib.sha256(f"voluntary_{ts}_{str(retain or "")[:30]}".encode()).hexdigest()
                                
                                # SQLite sync
                                try:
                                    conn = sqlite3.connect(db_path)
                                    conn.execute("INSERT INTO retained_memory (content) VALUES (?)", (retain,))
                                    conn.commit()
                                    conn.close()
                                except Exception as db_err:
                                    log.error("Failed to insert voluntary memory: %s", db_err)
                                
                                # ChromaDB sync (defer to avoid blocking read-path writes)
                                async def add_chroma_async():
                                    try:
                                        with self.memory_engine.collection_guard() as coll:
                                            coll.add(
                                                ids=[m_id],
                                                documents=[retain],
                                                metadatas=[{
                                                    "type": "voluntary",
                                                    "timestamp": ts,
                                                    "timestamp_unix": time.time(),
                                                    "importance": 0.6,
                                                    "usage_count": 0,
                                                    "source": "respond"
                                                }]
                                            )
                                    except Exception as chroma_err:
                                        log.error("ChromaDB voluntary memory store failed: %s", chroma_err)

                                asyncio.create_task(add_chroma_async())

                                if tui_session:
                                    from runtime_next.models.events import EventType
                                    await tui_session.emit_memory(EventType.MEMORY_STORED, "voluntary", str(retain or "")[:40], 1, 0.6)

                        if session_tracker:
                            session_tracker.record_answer(msg)
                            session_tracker.save(db_path)

                        batch_complete = True
                        break

                    log.info("[Step %d/%d] Executing '%s'...", step + 1, MAX_STEPS, tool_name)
                    if tui_session:
                        from runtime_next.models.events import EventType
                        await tui_session.emit_tool(EventType.TOOL_STARTED, tool_name, str(tool_args)[:60], "running")

                    try:
                        # ── Graph-based tool execution ──
                        # Create a NodeDefinition for this tool call and execute it
                        # through the NodeRunner, which routes through the
                        # ExecutionGraph infrastructure. This replaces the legacy
                        # TurnAgent + memory_engine.execute_turn() path.
                        tool_node_id = f"tool_{step}_{tool_name}_{time.time_ns()}"
                        tool_node = NodeDefinition(
                            id=tool_node_id,
                            description=f"Execute {tool_name}",
                            tool_name=tool_name,
                            args=call.get("args", {}),
                            node_type=NodeType.TOOL_CALL,
                        )
                        self.runtime_graph.add_node(tool_node)

                        # Execute via NodeRunner (which calls _tool_executor)
                        outcome = await self.runtime_runner.run_node(tool_node, {})
                        tool_node.result = outcome

                        # Transition node state based on outcome
                        if outcome.get("status") == "error":
                            await self.runtime_graph.transition_node(
                                tool_node_id, NodeState.FAILED,
                                reason=str(outcome.get("logs", "Tool execution failed"))[:120],
                            )
                            # Recovery engine handles the failure via event bus
                        else:
                            await self.runtime_graph.transition_node(
                                tool_node_id, NodeState.COMPLETED,
                                reason="Tool executed successfully",
                            )

                            # Fire background verification for write/edit/bash tools
                            # Results are published to the event bus; recovery engine
                            # picks up VERIFICATION_FAILED events autonomously.
                            if tool_name in ("write_file", "edit_file", "bash_exec"):
                                try:
                                    vmanifest = VerificationManifest(
                                        required=[VerificationType.LINT],
                                        blocking=[VerificationType.LINT],
                                    )
                                    vscope = VerificationScope(
                                        affected_files=[tool_args.get("path", "")] if tool_args.get("path") else [],
                                        is_minimal=True,
                                        provenance="tool_output",
                                    )
                                    asyncio.ensure_future(
                                        self._safe_verify_tool_output(
                                            tool_node_id, vmanifest, vscope,
                                        )
                                    )
                                except Exception as verr:
                                    log.debug("Tool output verification init failed: %s", verr)

                        if tool_name == "search_memory" and outcome.get("executed", {}).get("retrieved_ids"):
                            self.memory_engine.last_retrieved_ids = outcome["executed"]["retrieved_ids"]
                            if tui_session:
                                from runtime_next.models.events import EventType
                                await tui_session.emit_memory(
                                    EventType.MEMORY_RETRIEVED,
                                    "semantic",
                                    str(tool_args).get("query", "")[:40] if isinstance(tool_args, dict) else str(tool_args)[:40],
                                    len(outcome["executed"]["retrieved_ids"]),
                                    0.0
                                )
                        elif tool_name == "search_memory":
                            # Also handle case where retrieved_ids may be under different key
                            retrieved = outcome.get("executed", {}).get("retrieved_ids") or outcome.get("data", {}).get("ids", [])
                            if retrieved:
                                self.memory_engine.last_retrieved_ids = retrieved

                        status = "completed"
                        exit_code = 0
                        if outcome.get("status") == "error":
                            status = "failed"
                            exit_code = 1

                        if tui_session:
                            from runtime_next.models.events import EventType
                            await tui_session.emit_tool(EventType.TOOL_COMPLETED, tool_name, str(tool_args)[:60], status, exit_code)

                    except Exception as e:
                        outcome = {"status": "error", "logs": str(e), "executed": {}}
                        if tui_session:
                            from runtime_next.models.events import EventType
                            await tui_session.emit_tool(EventType.TOOL_FAILED, tool_name, str(tool_args)[:60], "failed")
                        log.error("Tool %s failed: %s", tool_name, e)

                    agent.feed_result(outcome)
                    if session_tracker:
                        session_tracker.record_tool(tool_name, tool_args, outcome.get("status", "error").lower())
                        session_tracker.save(db_path)

                    if outcome.get("status") == "error":
                        break

                if batch_complete:
                    break

                next_output = await asyncio.to_thread(
                    agent.send_user_message,
                    "Batch execution complete. If you need further tools, BATCH them into a JSON array for efficiency. "
                    "If you are finished, use the 'respond' tool with your final answer."
                )

                n_type, n_payload = parse_llm_output(next_output)
                if n_type == "tool_calls":
                    current_batch = n_payload
                    continue
                else:
                    final_answer = next_output
                    if not tui_session:
                        print(f"\n[AELVO] {next_output}\n")
                    if stream_callback:
                        stream_callback(next_output)
                    if session_tracker:
                        session_tracker.record_answer(next_output)
                        session_tracker.save(db_path)
                    break
        else:
            if not tui_session:
                print(f"\n[AELVO] {raw_output}\n")
            if stream_callback:
                stream_callback(raw_output)
            if session_tracker:
                session_tracker.record_answer(raw_output)
                session_tracker.save(db_path)

        return final_answer
