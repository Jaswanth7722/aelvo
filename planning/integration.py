# planning/integration.py - Orchestrator Integration for AELVO OMEGA Long-Horizon Planning
"""
The LongHorizonPlanningIntegration class is the single seam point between
the Long-Horizon Planning system and the rest of AELVO OMEGA.

It attaches to three precise locations in the existing system, as identified
in the pre-build system analysis:

SEAM 1 — build_shared_context():
    The orchestrator calls build_shared_context() before every turn to assemble
    the context dict passed to all specialists. We inject:
    - strategic_context: active objectives, active milestones, highest priority next action
    - continuity: session restoration state (if session was restored)
    - request_alignment: "aligned", "neutral", or "contradicts"

SEAM 2 — post_process() (via EventBus, post-process pathway):
    After each specialist responds, we subscribe to specialist_complete events
    on the EventBus. When we receive one, we:
    - Check if the result contains verification outcomes (for plan evolution)
    - Update active milestone progress
    - Write the outcome back to the appropriate hierarchy node

SEAM 3 — session boundary:
    The orchestrator's shutdown sequence calls save_session_end() via the
    LongHorizonPlanningIntegration.on_session_end() method, which we register
    as a shutdown callback in main_async().

These three seam points are attached without modifying any existing specialist
or orchestrator code — we extend, never modify.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from planning.goal_hierarchy import GoalHierarchyEngine
from planning.multi_session import MultiSessionPlanningEngine
from planning.plan_evolution import PlanEvolutionEngine
from planning.debt_forecasting import TechnicalDebtForecaster
from planning.self_critique import SelfCritiqueEngine, CritiqueRunResult
from planning.critique_evolution_pipeline import (
    SelfCritiqueEvolutionPipeline,
    PipelineResult,
)
from planning.memory_types import HierarchyLevel, PlanNodeState

log = logging.getLogger("aelvo.planning.integration")


class LongHorizonPlanningIntegration:
    """Single seam point for Long-Horizon Planning integration with AELVO OMEGA.

    Instantiated in main_async() after the Orchestrator is created. The
    Orchestrator does not need to be modified — the integration attaches
    via the Orchestrator's existing extension points.

    Usage in main_async():
        lhp = LongHorizonPlanningIntegration(
            memory_engine=memory_engine,
            orchestrator=orchestrator,
            workspace_path=WORKSPACE_PATH,
            project=_ws_name,
        )
        await lhp.start()
        # ... (REPL loop)
        await lhp.shutdown()
    """

    def __init__(
        self,
        memory_engine,
        orchestrator,
        workspace_path: str,
        project: str,
    ):
        self.memory_engine = memory_engine
        self.orchestrator = orchestrator
        self.workspace_path = workspace_path
        self.project = project

        # Initialize all sub-engines
        self.hierarchy = GoalHierarchyEngine(
            memory_engine=memory_engine,
            project=project,
        )
        self.multi_session = MultiSessionPlanningEngine(
            memory_engine=memory_engine,
            hierarchy=self.hierarchy,
            workspace_path=workspace_path,
            project=project,
        )
        self.evolution = PlanEvolutionEngine(hierarchy=self.hierarchy)
        self.debt = TechnicalDebtForecaster(
            memory_engine=memory_engine,
            hierarchy=self.hierarchy,
            project=project,
        )
        self.critique = SelfCritiqueEngine(
            hierarchy=self.hierarchy,
            memory_engine=memory_engine,
            project=project,
        )
        self.critique_pipeline = SelfCritiqueEvolutionPipeline(
            hierarchy=self.hierarchy,
            evolution=self.evolution,
            critique=self.critique,
        )

        self._started = False
        self._last_critique_result: Optional[CritiqueRunResult] = None
        self._last_pipeline_result: Optional[PipelineResult] = None
        self._continuity_context: Optional[Dict[str, Any]] = None
        self._event_bus_subscribed = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> Optional[Dict[str, Any]]:
        """Initialize the planning system at session start.

        1. Load hierarchy from ChromaDB
        2. Restore session state from boundary record
        3. Run initial self-critique
        4. Attach to Orchestrator shared context
        5. Subscribe to EventBus for post-process events

        Returns the continuity context if session was restored, else None.
        """
        log.info("Starting Long-Horizon Planning system for project=%s", self.project)

        # 1. Restore session state
        self._continuity_context = self.multi_session.restore_session_start()

        # 2. Run initial self-critique if hierarchy has any nodes
        if self.hierarchy._nodes:
            self._last_critique_result = self.critique.run()
            if self._last_critique_result.escalated_defects:
                log.warning(
                    "LHP start: %d escalated plan defects — %s",
                    len(self._last_critique_result.escalated_defects),
                    self._last_critique_result.to_summary(),
                )

        # 3. Attach to Orchestrator shared context (SEAM 1)
        self._attach_to_orchestrator_context()

        # 4. Subscribe to EventBus (SEAM 2) — non-blocking
        self._subscribe_to_event_bus()

        self._started = True
        log.info(
            "LHP started: %d hierarchy nodes, restored=%s",
            len(self.hierarchy._nodes),
            self._continuity_context is not None,
        )

        return self._continuity_context

    async def shutdown(self, interrupted_details: Optional[Dict[str, Any]] = None) -> None:
        """Persist session state at session end (SEAM 3).

        Called by the REPL loop cleanup or KeyboardInterrupt handler.
        This write is never skipped — it is the mechanism that enables
        cross-session continuity.
        """
        if not self._started:
            return

        # Run final self-critique before saving
        if self.hierarchy._nodes:
            self._last_critique_result = self.critique.run()

        # Write boundary record
        success = self.multi_session.save_session_end(interrupted_details)
        if success:
            log.info("LHP session boundary saved successfully")
        else:
            log.warning("LHP session boundary save failed — plan state may be incomplete")

    def on_turn_complete(self) -> None:
        """Called by the REPL loop after each successful user turn."""
        self.multi_session.increment_turn_count()

    # ------------------------------------------------------------------
    # SEAM 1: Orchestrator Shared Context Injection
    # ------------------------------------------------------------------

    def _attach_to_orchestrator_context(self) -> None:
        """Monkey-patch build_shared_context to inject strategic planning context.

        This approach extends the orchestrator without modifying its source file.
        We wrap the existing build_shared_context method and inject our context
        after the original method runs.
        """
        if not hasattr(self.orchestrator, "build_shared_context"):
            log.debug("Orchestrator has no build_shared_context — skipping context attachment")
            return

        original_build_context = self.orchestrator.build_shared_context

        def enhanced_build_shared_context(*args, **kwargs):
            # Call original method
            context = original_build_context(*args, **kwargs)

            # Inject strategic planning context
            planning_context = self._build_planning_context_injection()
            if planning_context:
                context.update(planning_context)

            return context

        self.orchestrator.build_shared_context = enhanced_build_shared_context
        log.info("✓ LHP: build_shared_context extended with strategic planning context")

    def _build_planning_context_injection(self) -> Dict[str, Any]:
        """Build the context dict to inject into the orchestrator's shared context."""
        result = {}

        # Strategic hierarchy context
        if self.hierarchy._nodes:
            hierarchy_context = self.hierarchy.build_strategic_context_summary()
            result.update(hierarchy_context)

        # Session continuity context (if restored)
        if self._continuity_context:
            result.update(self._continuity_context)

        # Plan quality signal (if critique has run)
        if self._last_critique_result:
            result["plan_quality"] = {
                "score": self._last_critique_result.plan_quality_score,
                "defect_count": len(self._last_critique_result.defects),
                "escalated_count": len(self._last_critique_result.escalated_defects),
                "has_blocking_defects": self._last_critique_result.has_blocking_defects,
                "summary": self._last_critique_result.to_summary()[:300],
            }

        # Pipeline execution signal (if pipeline has run)
        if self._last_pipeline_result:
            result["pipeline_report"] = self._last_pipeline_result.format_report()

        return result

    # ------------------------------------------------------------------
    # SEAM 2: EventBus Subscription
    # ------------------------------------------------------------------

    def _subscribe_to_event_bus(self) -> None:
        """Subscribe to specialist and verification events for plan evolution triggers."""
        try:
            from ui.events import get_event_bus, EventType
            bus = get_event_bus()

            async def on_verification_failed(event):
                await self._handle_verification_event(event, success=False)

            async def on_verification_passed(event):
                await self._handle_verification_event(event, success=True)

            async def on_specialist_action(event):
                await self._handle_specialist_action_event(event)

            bus.subscribe(EventType.VERIFICATION_FAILED, on_verification_failed)
            bus.subscribe(EventType.VERIFICATION_PASSED, on_verification_passed)
            bus.subscribe(EventType.SPECIALIST_ACTION, on_specialist_action)
            self._event_bus_subscribed = True
            log.info("✓ LHP: Subscribed to EventBus (VERIFICATION_FAILED/PASSED, SPECIALIST_ACTION)")
        except Exception as exc:
            log.debug("EventBus subscription skipped (not available): %s", exc)

    async def _handle_verification_event(self, event, success: bool) -> None:
        """Handle VERIFICATION_FAILED and VERIFICATION_PASSED events."""
        if not self.hierarchy._nodes:
            return
        try:
            payload = getattr(event, "data", {}) or {}
            check_name = payload.get("check_name", "verification check")
            summary = payload.get("summary", "") or payload.get("message", "")

            active_milestones = self.hierarchy.get_active_milestones()
            if not active_milestones:
                return

            target = max(active_milestones, key=lambda m: m.progress_pct)
            if success:
                new_conf = min(1.0, target.confidence + 0.02)
                self.hierarchy.update_confidence(
                    node_id=target.node_id,
                    new_confidence=new_conf,
                    rationale=f"Verification passed: {check_name}",
                    trigger_type="capability_discovery",
                )
            else:
                self.evolution.process_verification_failure(
                    milestone_id=target.node_id,
                    check_name=check_name,
                    failure_summary=summary[:200],
                )
        except Exception as exc:
            log.debug("Verification event handling error: %s", exc)

    async def _handle_specialist_action_event(self, event) -> None:
        """Handle SPECIALIST_ACTION events — modest confidence nudges on success."""
        if not self.hierarchy._nodes:
            return
        try:
            payload = getattr(event, "data", {}) or {}
            specialist = payload.get("specialist", "") or getattr(event, "specialist", "")
            status = payload.get("status", "")

            if specialist in ("FORGE", "SENTINEL") and status == "success":
                active_milestones = self.hierarchy.get_active_milestones()
                for ms in active_milestones[:1]:
                    await asyncio.sleep(0)  # yield to event loop
                    new_conf = min(1.0, ms.confidence + 0.01)
                    self.hierarchy.update_confidence(
                        node_id=ms.node_id,
                        new_confidence=new_conf,
                        rationale=f"{specialist} action succeeded",
                        trigger_type="capability_discovery",
                    )
        except Exception as exc:
            log.debug("Specialist action event handling error: %s", exc)

    # ------------------------------------------------------------------
    # Public plan creation API
    # ------------------------------------------------------------------

    def declare_mission(
        self,
        mission_statement: str,
        title: str = "",
        content: str = "",
    ) -> Optional[str]:
        """Declare the project mission. Should be called once per project.

        Returns the mission node_id or None if already exists.
        """
        existing = self.hierarchy.get_mission()
        if existing:
            log.info("Mission already declared: %s", existing.title)
            return existing.node_id

        node = self.hierarchy.create_node(
            level=HierarchyLevel.MISSION,
            title=title or "Project Mission",
            content=content or mission_statement,
            mission_statement=mission_statement,
        )
        if node:
            log.info("Mission declared: '%s'", node.title)
            return node.node_id
        return None

    def add_strategic_objective(
        self,
        title: str,
        description: str,
        capability_area: str = "",
        target_sessions: Optional[int] = None,
    ) -> Optional[str]:
        """Add a Strategic Objective under the Mission.

        Returns the node_id or None on failure.
        """
        mission = self.hierarchy.get_mission()
        if not mission:
            log.warning("Cannot add objective — no Mission declared. Call declare_mission() first.")
            return None

        node = self.hierarchy.create_node(
            level=HierarchyLevel.STRATEGIC_OBJECTIVE,
            title=title,
            content=description,
            parent_id=mission.node_id,
            capability_area=capability_area,
            target_sessions=target_sessions,
        )
        if node:
            # Activate the objective
            self.hierarchy.update_node_state(
                node_id=node.node_id,
                new_state=PlanNodeState.ACTIVE,
                trigger_summary="Objective added and activated",
            )
            log.info("Strategic Objective added: '%s'", title)
            return node.node_id
        return None

    def add_milestone(
        self,
        title: str,
        description: str,
        parent_id: str,  # Must be Initiative or Program node_id
        success_criteria: Optional[List[str]] = None,
        target_sessions: Optional[int] = None,
    ) -> Optional[str]:
        """Add a Milestone under a Program or Initiative.

        Returns the node_id or None on failure.
        """
        from planning.memory_types import VerificationStrategy

        node = self.hierarchy.create_node(
            level=HierarchyLevel.MILESTONE,
            title=title,
            content=description,
            parent_id=parent_id,
            success_criteria=success_criteria or [],
            target_sessions=target_sessions,
            verification_strategy=VerificationStrategy(
                required_checks=["functional_test", "lint_pass"],
                blocking_checks=["functional_test"],
                success_thresholds={"test_pass_rate": 1.0},
            ),
        )
        if node:
            self.hierarchy.update_node_state(
                node_id=node.node_id,
                new_state=PlanNodeState.ACTIVE,
                trigger_summary="Milestone created and activated",
            )
            log.info("Milestone added: '%s'", title)
            return node.node_id
        return None

    def get_strategic_context_for_prompt(self) -> str:
        """Return a compact strategic context string for specialist prompt injection.

        This is used by specialists that want to include strategic awareness
        in their system prompts directly, without going through shared_context.
        """
        if not self.hierarchy._nodes:
            return ""

        lines = ["STRATEGIC PLANNING CONTEXT:"]
        mission = self.hierarchy.get_mission()
        if mission:
            lines.append(f"  Mission: {mission.title}")

        active_objectives = self.hierarchy.get_active_objectives()
        if active_objectives:
            obj_strs = [f"{o.title} ({o.progress_pct:.0f}%)" for o in active_objectives[:3]]
            lines.append(f"  Active Objectives: {', '.join(obj_strs)}")

        active_milestones = self.hierarchy.get_active_milestones()
        if active_milestones:
            best = max(active_milestones, key=lambda m: m.confidence)
            lines.append(
                f"  Priority Milestone: {best.title} "
                f"({best.progress_pct:.0f}% complete, confidence={best.confidence:.2f})"
            )

        if self._continuity_context:
            resume_msg = (
                self._continuity_context.get("continuity", {}).get("resume_msg", "")
            )
            if resume_msg:
                lines.append(f"  Session Resume: {resume_msg}")

        return "\n".join(lines)

    def run_debt_scan(
        self,
        target_subsystems: Optional[List[str]] = None,
    ) -> List[str]:
        """Run a technical debt scan and return a list of summary strings."""
        forecasts = self.debt.run_scan(target_subsystems)
        summaries = []
        for f in forecasts:
            summaries.append(
                f"{f.subsystem}: debt={f.overall_debt_score:.2f} "
                f"(impl={f.implementation_debt_score:.2f}, "
                f"sec={f.security_debt_score:.2f}, "
                f"design={f.design_debt_score:.2f}), "
                f"risk={f.risk_at_milestone.value}"
            )
        return summaries

    def run_self_critique(self) -> CritiqueRunResult:
        """Run a self-critique pass and return the result."""
        result = self.critique.run()
        self._last_critique_result = result
        return result

    def run_critique_evolution_pipeline(
        self,
        max_iterations: int = 3,
        auto_remediate: bool = True,
        target_defect_types: Optional[List[str]] = None,
    ) -> PipelineResult:
        """Run the full self-critique → plan evolution pipeline.

        This is the primary integration point for the pipeline.
        It cascades self-critique defects into plan evolution triggers,
        attempts automatic remediation, and iterates until defects are
        resolved or max_iterations is reached.

        Args:
            max_iterations: Max critique→evolve cycles.
            auto_remediate: Attempt automatic fixes for known defect types.
            target_defect_types: If provided, only process these defect types
                                 by name (e.g., ["floating_task", "circular_dependency"]).

        Returns:
            PipelineResult with full iteration traceability.
        """
        defect_type_enum = None
        if target_defect_types is not None:
            defect_type_enum = []
            from planning.memory_types import DefectType as DT
            for name in target_defect_types:
                try:
                    defect_type_enum.append(DT(name))
                except ValueError:
                    log.warning("Unknown defect type '%s' — skipping", name)

        result = self.critique_pipeline.run(
            max_iterations=max_iterations,
            auto_remediate=auto_remediate,
            target_defect_types=defect_type_enum or None,
        )
        self._last_pipeline_result = result
        return result

    def handle_verification_failure(
        self,
        check_name: str,
        failure_summary: str,
        failed_task_ids: Optional[List[str]] = None,
    ) -> None:
        """Called by external code (e.g., VerificationPipeline) when a check fails.

        Finds the most relevant active milestone and applies the four-trigger
        VERIFICATION_FAILURE evolution.
        """
        active_milestones = self.hierarchy.get_active_milestones()
        if not active_milestones:
            return

        # Target the milestone with highest progress (most likely to be affected)
        target = max(active_milestones, key=lambda m: m.progress_pct)
        self.evolution.process_verification_failure(
            milestone_id=target.node_id,
            check_name=check_name,
            failure_summary=failure_summary,
            failed_task_ids=failed_task_ids,
        )

    def handle_user_directive(
        self,
        directive_text: str,
        new_priority_objective_title: Optional[str] = None,
        deactivate_objective_titles: Optional[List[str]] = None,
    ) -> None:
        """Called when a user message contains explicit strategic direction change.

        Resolves objective titles to node_ids before calling process_user_directive.
        """
        from planning.memory_types import HierarchyLevel as HL

        new_priority_id = None
        if new_priority_objective_title:
            objectives = self.hierarchy.find_nodes_by_level(HL.STRATEGIC_OBJECTIVE)
            for obj in objectives:
                if new_priority_objective_title.lower() in obj.title.lower():
                    new_priority_id = obj.node_id
                    break

        deactivate_ids = []
        if deactivate_objective_titles:
            objectives = self.hierarchy.find_nodes_by_level(HL.STRATEGIC_OBJECTIVE)
            for title in deactivate_objective_titles:
                for obj in objectives:
                    if title.lower() in obj.title.lower():
                        deactivate_ids.append(obj.node_id)

        self.evolution.process_user_directive(
            directive_text=directive_text,
            target_level=HL.STRATEGIC_OBJECTIVE,
            new_priority_objective_id=new_priority_id,
            deactivate_objective_ids=deactivate_ids,
        )
