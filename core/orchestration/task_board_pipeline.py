# core/orchestration/task_board_pipeline.py — Mode B: Task-Board-Based Execution
#
# Phase 8: Dual-Mode Pipeline
#   Mode A (preserved): Consolidated prompt approach in RuntimePipeline.run()
#   Mode B (new):       Task-board-based collaborative execution via SharedTaskBoard
#                       and CognitiveBlackboard
#
# Mode B replaces the single-LLM-call pipeline with specialist-driven collaboration:
#   1. ARCHITECT decomposes the request into typed tasks
#   2. Tasks are published to the SharedTaskBoard
#   3. Specialists pick up tasks and execute via blackboard communication
#   4. HERALD aggregates results into a final report
#   5. Results are packaged into the PipelineResult format

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

from core.orchestration.pipeline import (
    PipelinePhase,
    PipelineResult,
    PhaseResult,
    PIPELINE_HANDOFFS,
)

log = logging.getLogger("aelvo.task_board_pipeline")

# Mode constants
MODE_A = "consolidated"
MODE_B = "task_board"


class TaskBoardPipeline:
    """Mode B execution pipeline — task-board and blackboard based.

    Instead of the consolidated prompt approach (Mode A), this path:
    1. Creates a SharedTaskBoard and CognitiveBlackboard for the session
    2. Classifies the user request to determine the task graph
    3. Publishes typed tasks to the SharedTaskBoard
    4. Routes each task to the appropriate specialist via their
       blackboard collaboration methods (pickup_task, publish_finding, etc.)
    5. Collects results from the blackboard
    6. Aggregates via HERALD into a final report
    7. Returns a PipelineResult-compatible result

    All inter-specialist communication flows through the blackboard.
    No direct messaging (Amendment 2).
    """

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.memory_engine = getattr(orchestrator, "memory_engine", None)
        self.fs = getattr(orchestrator, "fs", None)
        self.kernel = getattr(orchestrator, "kernel", None)
        self.runtime_bus = getattr(orchestrator, "runtime_bus", None)
        self.event_bus = getattr(orchestrator, "event_bus", None)
        self.provider_runtime = getattr(orchestrator, "provider_runtime", None)

        # Specialists
        from specialists import SPECIALIST_REGISTRY
        self.specialists = SPECIALIST_REGISTRY

    async def run(
        self,
        user_input: str,
        agent: Any,
        conversation_history: List[Dict[str, Any]],
        hermes_context: Optional[Any] = None,
        task_board: Optional[Any] = None,
        blackboard: Optional[Any] = None,
    ) -> PipelineResult:
        """Execute the task-board-based pipeline for this turn.

        Args:
            user_input: The user's request text.
            agent: The LLM agent instance.
            conversation_history: Previous conversation messages.
            hermes_context: Optional HermesContext for global cognition.
            task_board: Optional pre-existing SharedTaskBoard (creates one if None).
            blackboard: Optional pre-existing CognitiveBlackboard (creates one if None).

        Returns:
            A PipelineResult compatible with Mode A's output format.
        """
        start_time = time.time()
        phases_executed: List[PipelinePhase] = []
        phase_results: Dict[PipelinePhase, PhaseResult] = {}
        failures: List[Tuple[PipelinePhase, str]] = []

        # ── 1. Create resources ──────────────────────────────────
        if task_board is None:
            task_board = self._create_task_board()
        if blackboard is None:
            blackboard = self._create_blackboard()

        # ── 1b. Wire collaboration infrastructure ────────────────
        # Attach orchestrator's router as collaboration transport layer
        orchestrator_router = getattr(self.orchestrator, 'router', None)
        if orchestrator_router:
            blackboard.set_router(orchestrator_router)

        # Wire runtime_bus to blackboard for EventBus visibility
        if self.runtime_bus:
            blackboard.set_event_bus(self.runtime_bus)

        # ── 1c. Wire blackboard subscriptions ────────────────────
        # Specialists automatically accumulate data via subscribe() callbacks
        # instead of polling the blackboard directly.
        for specialist in self.specialists.values():
            if hasattr(specialist, 'setup_subscriptions'):
                specialist.clear_session()
                specialist.setup_subscriptions(blackboard)

        # ── 2. Classify request ──────────────────────────────────
        task_classification = self._classify_request(
            user_input, hermes_context,
        )
        log.info(
            "Mode B: classified request as %s",
            task_classification,
        )

        # ── 3. Decompose into task specs ─────────────────────────
        task_specs = self._decompose_to_tasks(
            user_input, task_classification, hermes_context,
        )
        log.info(
            "Mode B: decomposed into %d task(s): %s",
            len(task_specs),
            [s["type"] for s in task_specs],
        )

        if not task_specs:
            return PipelineResult(
                success=True,
                phases_executed=[],
                phase_results={},
                total_duration_ms=(time.time() - start_time) * 1000,
                final_output="No tasks to execute.",
            )

        # ── 4. Publish tasks to board ────────────────────────────
        task_ids: Dict[str, str] = {}  # task_type -> task_id
        for spec in task_specs:
            task = task_board.create_task(
                task_type=spec["type"],
                specialist=spec.get("specialist", ""),
                title=spec.get("title", user_input[:60]),
                description=spec.get("description", user_input),
                context=spec.get("context", {}),
                assigned_by="architect",
            )
            task_ids[spec["type"].value] = task.id
            log.debug(
                "Mode B: created task %s [%s]",
                task.id[:12], spec["type"].value,
            )

        # ── 5. Execute tasks via specialist collaboration ────────
        # Each phase reads from the blackboard, executes, and writes back.

        # Phase 1: RESEARCH (ORACLE)
        if "research" in task_ids:
            phase = PipelinePhase.RESEARCH
            phases_executed.append(phase)
            try:
                result = await self._execute_research(
                    task_board, blackboard,
                    task_ids["research"], agent,
                )
                phase_results[phase] = result
            except Exception as e:
                log.error("Mode B RESEARCH phase failed: %s", e)
                failures.append((phase, str(e)))
                phase_results[phase] = PhaseResult(
                    phase=phase, specialist_name="ORACLE",
                    success=False, error=str(e),
                    duration_ms=0.0,
                )

        # Phase 2: IMPLEMENTATION (FORGE)
        if "implement" in task_ids:
            phase = PipelinePhase.IMPLEMENTATION
            phases_executed.append(phase)
            try:
                result = await self._execute_implementation(
                    task_board, blackboard,
                    task_ids["implement"], agent,
                )
                phase_results[phase] = result
            except Exception as e:
                log.error("Mode B IMPLEMENTATION phase failed: %s", e)
                failures.append((phase, str(e)))
                phase_results[phase] = PhaseResult(
                    phase=phase, specialist_name="FORGE",
                    success=False, error=str(e),
                    duration_ms=0.0,
                )

        # Phase 3: SECURITY (SENTINEL)
        if "security_review" in task_ids:
            phase = PipelinePhase.SECURITY
            phases_executed.append(phase)
            try:
                result = await self._execute_security_review(
                    task_board, blackboard,
                    task_ids["security_review"], agent,
                )
                phase_results[phase] = result
            except Exception as e:
                log.error("Mode B SECURITY phase failed: %s", e)
                failures.append((phase, str(e)))
                phase_results[phase] = PhaseResult(
                    phase=phase, specialist_name="SENTINEL",
                    success=False, error=str(e),
                    duration_ms=0.0,
                )

        # Phase 4: EXECUTION (TERMINUS)
        if "execute" in task_ids:
            phase = PipelinePhase.EXECUTION
            phases_executed.append(phase)
            try:
                result = await self._execute_command(
                    task_board, blackboard,
                    task_ids["execute"], agent,
                )
                phase_results[phase] = result
            except Exception as e:
                log.error("Mode B EXECUTION phase failed: %s", e)
                failures.append((phase, str(e)))
                phase_results[phase] = PhaseResult(
                    phase=phase, specialist_name="TERMINUS",
                    success=False, error=str(e),
                    duration_ms=0.0,
                )

        # Phase 5: REPORT (HERALD)
        if "report" in task_ids:
            phase = PipelinePhase.REPORTING
            phases_executed.append(phase)
            try:
                result = await self._execute_report(
                    task_board, blackboard,
                    task_ids["report"], agent,
                )
                phase_results[phase] = result
            except Exception as e:
                log.error("Mode B REPORT phase failed: %s", e)
                failures.append((phase, str(e)))
                phase_results[phase] = PhaseResult(
                    phase=phase, specialist_name="HERALD",
                    success=False, error=str(e),
                    duration_ms=0.0,
                )

        # ── 6. Aggregate final output ────────────────────────────
        total_duration = (time.time() - start_time) * 1000

        # Collect final output from blackboard
        final_output = self._aggregate_output(blackboard, task_board)

        return PipelineResult(
            success=len(failures) == 0,
            phases_executed=phases_executed,
            phase_results=phase_results,
            total_duration_ms=total_duration,
            final_output=final_output,
            failures=failures,
            memory_consolidated=True,
            verification_summary="Mode B — task-board collaboration",
            recovery_actions=[],
        )

    # ==================================================================
    # Resource Creation
    # ==================================================================

    def _create_task_board(self) -> Any:
        """Create a fresh in-memory SharedTaskBoard for this session."""
        from shared_task_board.board import SharedTaskBoard, TaskBoardConfig

        config = TaskBoardConfig(
            db_path="",
            auto_persist=False,
            enable_events=False,
        )
        return SharedTaskBoard(config=config)

    def _create_blackboard(self) -> Any:
        """Create a fresh CognitiveBlackboard for this session."""
        from cognition.blackboard import CognitiveBlackboard

        return CognitiveBlackboard(db_path="")

    # ==================================================================
    # Request Classification / Decomposition
    # ==================================================================

    def _classify_request(
        self,
        user_input: str,
        hermes_context: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Classify the user request to determine task composition.

        Analyzes the input text for keywords indicating what types of
        tasks are needed.  This mirrors the logic in RuntimePipeline's
        _determine_phases but produces task specs for the board.

        Returns:
            Dict with boolean flags for each task type.
        """
        lower = user_input.lower()

        needs_research = any(kw in lower for kw in [
            "research", "search", "find", "investigate", "explain",
            "what is", "who is", "how does", "latest", "documentation",
        ])
        needs_implementation = any(kw in lower for kw in [
            "implement", "code", "write", "refactor", "fix", "build",
            "create", "add feature", "update", "modify", "change",
        ])
        needs_security = any(kw in lower for kw in [
            "security", "vulnerability", "audit", "secret", "leak",
            "cve", "injection", "xss",
        ]) or needs_implementation  # security review follows implementation
        needs_execution = any(kw in lower for kw in [
            "run", "execute", "deploy", "docker", "git", "commit",
            "push", "npm", "pip install", "terminal", "bash",
            "command", "ci/cd",
        ])

        return {
            "research": needs_research,
            "implementation": needs_implementation,
            "security": needs_security,
            "execution": needs_execution,
            "report": True,  # always generate a report
        }

    def _decompose_to_tasks(
        self,
        user_input: str,
        classification: Dict[str, Any],
        hermes_context: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """Decompose a classified request into task specifications.

        Returns a list of task spec dicts with keys:
            type: TaskType enum value
            specialist: specialist name string
            title: short title
            description: full description
            context: dict of type-specific context
        """
        from shared_task_board.task import TaskType

        specs: List[Dict[str, Any]] = []

        # Research task
        if classification.get("research"):
            specs.append({
                "type": TaskType.RESEARCH,
                "specialist": "ORACLE",
                "title": f"Research: {user_input[:80]}",
                "description": user_input,
                "context": {
                    "query": user_input,
                    "scope": "codebase",
                },
            })

        # Implementation task
        if classification.get("implementation"):
            specs.append({
                "type": TaskType.IMPLEMENT,
                "specialist": "FORGE",
                "title": f"Implement: {user_input[:80]}",
                "description": user_input,
                "context": {
                    "specification": user_input,
                    "test_required": True,
                    "security_review_required": True,
                },
            })

        # Security review task
        if classification.get("security"):
            specs.append({
                "type": TaskType.SECURITY_REVIEW,
                "specialist": "SENTINEL",
                "title": f"Security Review: {user_input[:80]}",
                "description": user_input,
                "context": {
                    "risk_focus": "all",
                },
            })

        # Execution task
        if classification.get("execution"):
            specs.append({
                "type": TaskType.EXECUTE,
                "specialist": "TERMINUS",
                "title": f"Execute: {user_input[:80]}",
                "description": user_input,
                "context": {
                    "commands": [],
                    "timeout_seconds": 30,
                },
            })

        # Report task (always)
        specs.append({
            "type": TaskType.REPORT,
            "specialist": "HERALD",
            "title": f"Report: {user_input[:80]}",
            "description": user_input,
            "context": {
                "include_details": True,
                "format": "terminal",
            },
        })

        return specs

    # ==================================================================
    # Phase Execution — Specialist Dispatch
    # ==================================================================

    async def _execute_research(
        self,
        task_board: Any,
        blackboard: Any,
        task_id: str,
        agent: Any,
    ) -> PhaseResult:
        """Execute RESEARCH phase: ORACLE picks up task, publishes findings."""
        phase_start = time.time()

        oracle = self.specialists.get("ORACLE")
        if oracle is None:
            return PhaseResult(
                phase=PipelinePhase.RESEARCH, specialist_name="ORACLE",
                success=False, error="ORACLE specialist not registered",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 1. Pick up the task from the board
        tasks = oracle.pickup_task(
            task_board=task_board,
            max_tasks=1,
        )
        if not tasks:
            return PhaseResult(
                phase=PipelinePhase.RESEARCH, specialist_name="ORACLE",
                success=False, error="No research task to pick up",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        picked_task = tasks[0]

        # 2. Publish a research finding to the blackboard
        query = picked_task.context.get("query", picked_task.description)
        entry_id = oracle.publish_finding(
            blackboard=blackboard,
            summary=f"Research finding for: {query[:80]}",
            detail=f"Task: {picked_task.title}\nDescription: {picked_task.description}",
            confidence=0.6,
            tags=["research", "oracle", f"task:{task_id}"],
        )

        # 3. Log routing via router (ORACLE produced -> FORGE, SENTINEL)
        router = getattr(self.orchestrator, 'router', None)
        if entry_id and router:
            router.route_publication(
                evidence_type="finding",
                specialist="ORACLE",
                entry_id=entry_id,
                content_preview=f"Research finding for: {query[:60]}",
            )

        # 4. Mark task complete on the board
        task_board.complete_task(
            task_id,
            result={
                "entry_id": entry_id,
                "summary": query[:100],
                "confidence": 0.6,
            },
        )

        return PhaseResult(
            phase=PipelinePhase.RESEARCH, specialist_name="ORACLE",
            success=True,
            output=f"ORACLE published finding (entry={entry_id[:12]}) for query: {query[:100]}",
            handoff_data={"research_findings": [{"entry_id": entry_id}]},
            duration_ms=(time.time() - phase_start) * 1000,
        )

    async def _execute_implementation(
        self,
        task_board: Any,
        blackboard: Any,
        task_id: str,
        agent: Any,
    ) -> PhaseResult:
        """Execute IMPLEMENTATION phase: FORGE picks up task, publishes implementation."""
        phase_start = time.time()

        forge = self.specialists.get("FORGE")
        if forge is None:
            return PhaseResult(
                phase=PipelinePhase.IMPLEMENTATION, specialist_name="FORGE",
                success=False, error="FORGE specialist not registered",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 1. Check for research findings accumulated via subscription
        #    (setup_subscriptions was called in run() before phase execution).
        findings = getattr(forge, '_findings', [])[:5]
        finding_summaries = [
            f.summary[:50] for f in findings
        ] if findings else []

        # 2. Record FORGE consuming research findings
        for finding in findings:
            if hasattr(finding, 'id'):
                blackboard.consume(finding.id, "FORGE")

        # 3. Pick up the task from the board
        tasks = forge.pickup_task(
            task_board=task_board,
            max_tasks=1,
        )
        if not tasks:
            return PhaseResult(
                phase=PipelinePhase.IMPLEMENTATION, specialist_name="FORGE",
                success=False, error="No implementation task to pick up",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        picked_task = tasks[0]

        # 4. Submit for review (publishes ImplementationEntry to blackboard)
        impl_summary = (
            f"Implementation for: {picked_task.title[:60]}"
        )
        if finding_summaries:
            impl_summary += f"\nBased on findings: {'; '.join(finding_summaries)}"

        entry_id = forge.submit_for_review(
            blackboard=blackboard,
            summary=impl_summary,
            changes_description=picked_task.description,
            security_review_requested=True,
        )

        # 5. Log routing via router (FORGE implementation -> SENTINEL, ARCHITECT)
        router = getattr(self.orchestrator, 'router', None)
        if entry_id and router:
            router.route_publication(
                evidence_type="implementation",
                specialist="FORGE",
                entry_id=entry_id,
                content_preview=impl_summary[:60],
            )

        # 6. Mark task complete
        task_board.complete_task(
            task_id,
            result={
                "entry_id": entry_id,
                "summary": impl_summary[:100],
                "findings_used": len(finding_summaries),
            },
        )

        return PhaseResult(
            phase=PipelinePhase.IMPLEMENTATION, specialist_name="FORGE",
            success=True,
            output=f"FORGE submitted implementation (entry={entry_id[:12]})",
            handoff_data={
                "code_changes": [{"entry_id": entry_id}],
                "research_findings_consumed": len(finding_summaries),
            },
            duration_ms=(time.time() - phase_start) * 1000,
        )

    async def _execute_security_review(
        self,
        task_board: Any,
        blackboard: Any,
        task_id: str,
        agent: Any,
    ) -> PhaseResult:
        """Execute SECURITY phase: SENTINEL reviews, challenges, triggers consensus.

        Phase 6-7 workflow:
        1. SENTINEL accumulates implementations via subscription
        2. Reviews confidence levels — challenges low-confidence findings
        3. Challenges trigger consensus for resolution
        4. ARCHITECT reviews consensus and makes final decision
        5. Execution is gated on Architect's approval
        """
        phase_start = time.time()

        sentinel = self.specialists.get("SENTINEL")
        if sentinel is None:
            return PhaseResult(
                phase=PipelinePhase.SECURITY, specialist_name="SENTINEL",
                success=False, error="SENTINEL specialist not registered",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 1. Read implementations accumulated via subscription
        implementations = getattr(sentinel, '_implementations', [])[:5]

        # 2. Record SENTINEL consuming FORGE's implementations
        for impl in implementations:
            if hasattr(impl, 'id'):
                blackboard.consume(impl.id, "SENTINEL")

        # 3. Review findings — challenge low-confidence entries
        challenged_entries = sentinel.review_findings(
            blackboard=blackboard,
            max_results=5,
            confidence_threshold=0.7,
        )
        if challenged_entries:
            log.warning(
                "SENTINEL raised %d challenges against low-confidence findings",
                len(challenged_entries),
            )
            for c in challenged_entries:
                log.info(
                    "  Challenge: entry=%s confidence=%.2f challenge_id=%s",
                    c['entry_id'][:8], c['confidence'], c['challenge_id'][:8],
                )
        else:
            log.info("SENTINEL review: all findings meet confidence threshold (>=0.7)")

        # 4. Resolve challenges through consensus
        challenges_resolved = await self._resolve_challenges(
            blackboard, challenged_entries, task_id,
        )

        # 5. Pick up the security review task
        tasks = sentinel.pickup_task(
            task_board=task_board,
            max_tasks=1,
        )
        if not tasks:
            return PhaseResult(
                phase=PipelinePhase.SECURITY, specialist_name="SENTINEL",
                success=False, error="No security review task to pick up",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        picked_task = tasks[0]

        # 6. Check Architect decision gate for execution approval
        architect_granted = await self._architect_approve_execution(
            blackboard, task_id, picked_task, challenged_entries,
            challenges_resolved,
        )

        # 7. If Architect approved, publish approval; else reject
        review_summary = f"Security review for: {picked_task.title[:60]}"
        cleared = False
        entry_id = ""

        if architect_granted:
            cleared = True
            if implementations:
                for impl in implementations:
                    if hasattr(impl, "security_review_requested") and impl.security_review_requested:
                        review_summary += f"\nReviewed implementation: {impl.summary[:50]}"

            entry_id = sentinel.approve_implementation(
                blackboard=blackboard,
                implementation_summary=review_summary,
                approved_by="SENTINEL",
                notes="Mode B — security review with challenge workflow",
            )
        else:
            entry_id = sentinel.reject_implementation(
                blackboard=blackboard,
                reason=(
                    "Execution blocked by Architect decision: challenges "
                    f"({len(challenged_entries)}) or consensus outcome requires revision"
                ),
                severity="medium",
            )

        # 8. Log routing via router
        router = getattr(self.orchestrator, 'router', None)
        routing_type = "review_approve" if cleared else "review_reject"
        if entry_id and router:
            router.route_publication(
                evidence_type=routing_type,
                specialist="SENTINEL",
                entry_id=entry_id,
                content_preview=review_summary[:60],
            )

        # 9. Mark task complete
        task_board.complete_task(
            task_id,
            result={
                "entry_id": entry_id,
                "cleared": cleared,
                "summary": review_summary[:100],
                "challenges_raised": len(challenged_entries),
                "challenges_resolved": challenges_resolved,
            },
        )

        return PhaseResult(
            phase=PipelinePhase.SECURITY, specialist_name="SENTINEL",
            success=cleared,
            output=(
                f"SENTINEL {'approved' if cleared else 'rejected'} "
                f"implementation (entry={entry_id[:12]})"
                f" | challenges raised: {len(challenged_entries)}"
            ),
            handoff_data={
                "security_clearance": {"cleared": cleared, "entry_id": entry_id},
                "challenges": challenged_entries,
            },
            duration_ms=(time.time() - phase_start) * 1000,
        )

    # ==================================================================
    # Phase 6-7: Challenge → Consensus → Architect Workflow
    # ==================================================================

    async def _resolve_challenges(
        self,
        blackboard: Any,
        challenged_entries: List[Dict[str, Any]],
        session_task_id: str,
    ) -> int:
        """Resolve raised challenges through consensus and Architect review.

        For each challenged entry:
        1. Get the challenge details from blackboard
        2. Request consensus from ExtendedConsensusEngine
        3. Specialists vote (simulated: FORGE, SENTINEL, ORACLE)
        4. Consensus produces advisory outcome
        5. ARCHITECT reviews and makes final decision

        Args:
            blackboard: The CognitiveBlackboard instance.
            challenged_entries: List of challenged entry dicts.
            session_task_id: The current session task ID.

        Returns:
            Number of challenges resolved.
        """
        if not challenged_entries:
            return 0

        resolved_count = 0
        architect = self.specialists.get("ARCHITECT")
        forge = self.specialists.get("FORGE")
        oracle = self.specialists.get("ORACLE")
        sentinel = self.specialists.get("SENTINEL")

        for c_entry in challenged_entries:
            entry_id = c_entry["entry_id"]
            challenge_id = c_entry["challenge_id"]

            # Get the challenge from blackboard
            challenges = blackboard.get_challenges(entry_id=entry_id)
            challenge = next(
                (ch for ch in challenges if ch.challenge_id == challenge_id),
                None,
            )
            if challenge is None:
                continue

            # Build topic for consensus
            topic = (
                f"Challenge on entry {entry_id[:8]}: "
                f"{challenge.challenged_claim[:100]}"
            )

            # Create consensus request via ExtendedConsensusEngine
            from cognition.consensus_extended import (
                ExtendedConsensusEngine,
                ResolutionStrategy,
            )
            consensus_engine = ExtendedConsensusEngine()

            request = consensus_engine.request_consensus(
                topic=topic,
                participants=["FORGE", "SENTINEL", "ORACLE", "ARCHITECT"],
                context={
                    "challenge_id": challenge_id,
                    "entry_id": entry_id,
                },
                resolution_strategy=ResolutionStrategy.MAJORITY,
            )

            # Emit CONSENSUS_STARTED on runtime EventBus
            if self.runtime_bus:
                try:
                    from runtime_next.models.events import BaseEvent as BusBaseEvent, EventType as BusEventType
                    start_event = BusBaseEvent(
                        id=f"cons_start_{request.consensus_id[:8]}_{int(time.time())}",
                        type=BusEventType.CONSENSUS_FORMED,
                        payload={
                            "consensus_id": request.consensus_id,
                            "topic": topic[:100],
                            "participants": request.participants,
                            "status": "started",
                        },
                    )
                    loop = asyncio.get_running_loop()
                    if loop.is_running():
                        asyncio.ensure_future(self.runtime_bus.publish(start_event))
                except RuntimeError:
                    pass
                except Exception as e:
                    log.debug("Failed to emit CONSENSUS_STARTED: %s", e)

            # Submit positions (simulated — in production, specialists would
            # evaluate the challenge and submit their positions)
            if forge:
                consensus_engine.submit_position(
                    request.consensus_id, "FORGE", "FOR",
                    confidence=0.8,
                )
            if oracle:
                consensus_engine.submit_position(
                    request.consensus_id, "ORACLE", "FOR",
                    confidence=0.9,
                )
            if sentinel:
                consensus_engine.submit_position(
                    request.consensus_id, "SENTINEL", "AGAINST",
                    confidence=c_entry.get("confidence", 0.6),
                    conditions=["Confidence below threshold — must be improved"],
                )

            # Get consensus outcome (all participants voted above)
            outcome = consensus_engine.get_outcome(request.consensus_id)
            if outcome is None:
                # Manually resolve since we submitted all positions
                outcome = consensus_engine.submit_position(
                    request.consensus_id,
                    "ARCHITECT",
                    "NEUTRAL",
                    confidence=0.7,
                )

            if outcome is None:
                continue

            # Publish consensus outcome to blackboard
            consensus_engine.publish_to_blackboard(outcome, blackboard)

            # Emit CONSENSUS_FORMED event on runtime EventBus
            if self.runtime_bus:
                try:
                    from runtime_next.models.events import ConsensusEvent as ConsensusBusEvent
                    consensus_bus_event = ConsensusBusEvent(
                        id=outcome.consensus_id,
                        consensus_id=outcome.consensus_id,
                        target_id=entry_id,
                        recommendation=outcome.recommendation(),
                        confidence=outcome.confidence,
                        positions={p.specialist: p.position for p in outcome.positions},
                        method=outcome.resolution_strategy.value,
                    )
                    loop = asyncio.get_running_loop()
                    if loop.is_running():
                        asyncio.ensure_future(self.runtime_bus.publish(consensus_bus_event))
                except RuntimeError:
                    pass
                except Exception as e:
                    log.debug("Failed to emit ConsensusEvent: %s", e)

            # ARCHITECT reviews consensus and makes final decision
            if architect and hasattr(architect, 'review_consensus'):
                arch_decision = architect.review_consensus(
                    consensus_recommendation=outcome.recommendation(),
                    consensus_confidence=outcome.confidence,
                    consensus_id=outcome.consensus_id,
                    positions={p.specialist: p.position for p in outcome.positions},
                    task=topic,
                    risk_profile="medium" if c_entry.get("confidence", 0.5) < 0.6 else "low",
                    complexity=5,
                )

                # Apply the decision
                if hasattr(architect, 'apply_decision'):
                    architect.apply_decision(arch_decision)

                # Emit ARCHITECT_DECISION event on runtime EventBus
                if self.runtime_bus:
                    try:
                        from runtime_next.models.events import ArchitectDecisionEvent
                        arch_bus_event = ArchitectDecisionEvent(
                            id=arch_decision.decision_id,
                            decision_id=arch_decision.decision_id,
                            outcome=arch_decision.outcome.value,
                            target_type="challenge",
                            target_id=entry_id,
                            reason=arch_decision.reason,
                            conditions=arch_decision.conditions,
                            assigned_to=arch_decision.assigned_to,
                            overridden_recommendation=arch_decision.overridden_recommendation,
                            override_rationale=arch_decision.override_rationale,
                        )
                        loop = asyncio.get_running_loop()
                        if loop.is_running():
                            asyncio.ensure_future(self.runtime_bus.publish(arch_bus_event))
                    except RuntimeError:
                        pass
                    except Exception as e:
                        log.debug("Failed to emit ArchitectDecisionEvent: %s", e)

                # Resolve the challenge on blackboard with Architect's decision
                resolution_text = (
                    f"Architect decision: {arch_decision.outcome.value.upper()} "
                    f"| {arch_decision.reason[:100]}"
                )
                blackboard.resolve_challenge(
                    challenge_id=challenge_id,
                    resolution=resolution_text,
                    resolver=f"ARCHITECT:{arch_decision.decision_id[:8]}",
                )

                log.info(
                    "Challenge %s resolved: Architect %s — %s",
                    challenge_id[:8], arch_decision.outcome.value, arch_decision.reason[:80],
                )
            else:
                # Fallback: resolve directly based on consensus outcome
                blackboard.resolve_challenge(
                    challenge_id=challenge_id,
                    resolution=f"Resolved by consensus: {outcome.outcome.value}",
                    resolver="CONSENSUS",
                )

            # Log routing for this challenge resolution
            router = getattr(self.orchestrator, 'router', None)
            if router:
                router.route_publication(
                    evidence_type="challenge",
                    specialist="CONSENSUS",
                    entry_id=entry_id,
                    content_preview=f"Challenge {challenge_id[:8]} resolved via consensus",
                )

            resolved_count += 1

        return resolved_count

    async def _architect_approve_execution(
        self,
        blackboard: Any,
        task_id: str,
        picked_task: Any,
        challenged_entries: List[Dict[str, Any]],
        challenges_resolved: int,
    ) -> bool:
        """Gate execution on Architect's review of challenges and consensus.

        The Architect evaluates:
        - Were any challenges raised?
        - Were all challenges resolved?
        - What was the consensus outcome?
        - Does execution proceed?

        Per Amendment 3: Consensus is advisory, Architect is authoritative.
        """
        # No challenges — auto-approved
        if not challenged_entries:
            return True

        # All challenges resolved — check Architect's decision(s)
        if challenges_resolved == len(challenged_entries):
            # Check blackboard for challenge resolutions
            all_approved = True
            for c in challenged_entries:
                challenges = blackboard.get_challenges(entry_id=c["entry_id"])
                resolved = [
                    ch for ch in challenges
                    if ch.challenge_id == c["challenge_id"] and ch.resolved
                ]
                if resolved:
                    resolution = resolved[0].resolution or ""
                    # If Architect rejected or escalated, block execution
                    if "REJECT" in resolution.upper() or "ESCALATE" in resolution.upper():
                        all_approved = False
                else:
                    all_approved = False

            if all_approved:
                log.info(
                    "Architect execution gate: APPROVED — all %d challenges resolved",
                    challenges_resolved,
                )
                return True
            else:
                log.warning(
                    "Architect execution gate: BLOCKED — one or more challenges rejected/escalated",
                )
                return False

        # Some challenges not resolved
        log.warning(
            "Architect execution gate: BLOCKED — %d/%d challenges not yet resolved",
            len(challenged_entries) - challenges_resolved,
            len(challenged_entries),
        )
        return False

    async def _execute_command(
        self,
        task_board: Any,
        blackboard: Any,
        task_id: str,
        agent: Any,
    ) -> PhaseResult:
        """Execute EXECUTION phase: TERMINUS picks up task, gates on architect decision."""
        phase_start = time.time()

        terminus = self.specialists.get("TERMINUS")
        if terminus is None:
            return PhaseResult(
                phase=PipelinePhase.EXECUTION, specialist_name="TERMINUS",
                success=False, error="TERMINUS specialist not registered",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 1. Pick up the execution task
        tasks = terminus.pickup_task(
            task_board=task_board,
            max_tasks=1,
        )
        if not tasks:
            return PhaseResult(
                phase=PipelinePhase.EXECUTION, specialist_name="TERMINUS",
                success=False, error="No execution task to pick up",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        picked_task = tasks[0]

        # 2. Check architect decision (execution gate)
        try:
            terminus.check_architect_decision(
                blackboard=blackboard,
                target_id=task_id,
                command=picked_task.description[:100],
            )
            gate_passed = True
            gate_message = "Architect decision: approved"
        except Exception as gate_err:
            gate_passed = False
            gate_message = f"Execution gate blocked: {gate_err}"

        if not gate_passed:
            terminus.publish_failure_report(
                blackboard=blackboard,
                command=picked_task.description[:100],
                exit_code=-1,
                stderr=gate_message,
                task_id=task_id,
            )
            task_board.fail_task(
                task_id,
                error=gate_message,
            )
            return PhaseResult(
                phase=PipelinePhase.EXECUTION, specialist_name="TERMINUS",
                success=False,
                error=gate_message,
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 3. Emit EXECUTION_STARTED event
        if self.runtime_bus:
            try:
                from runtime_next.models.events import ExecutionStartedEvent
                start_event = ExecutionStartedEvent(
                    id=f"exec_start_{task_id[:8]}_{int(time.time())}",
                    task_id=task_id,
                    command=picked_task.description[:100],
                )
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    asyncio.ensure_future(self.runtime_bus.publish(start_event))
            except RuntimeError:
                pass
            except Exception as e:
                log.debug("Failed to emit ExecutionStartedEvent: %s", e)

        # 4. Log routing via router (decision -> TERMINUS can proceed)
        router = getattr(self.orchestrator, 'router', None)
        if router:
            router.route_publication(
                evidence_type="decision",
                specialist="ARCHITECT",
                entry_id=task_id,
                content_preview=f"Execution gate passed for: {picked_task.description[:60]}",
            )

        # 5. Publish execution result (simulated — no real command execution)
        entry_id = terminus.publish_execution_result(
            blackboard=blackboard,
            command=picked_task.description[:100],
            exit_code=0,
            stdout="Mode B — simulated execution (no real command)",
            stderr="",
            task_id=task_id,
        )

        # 6. Emit EXECUTION_COMPLETED event
        if self.runtime_bus:
            try:
                from runtime_next.models.events import ExecutionCompletedEvent
                complete_event = ExecutionCompletedEvent(
                    id=f"exec_end_{task_id[:8]}_{int(time.time())}",
                    task_id=task_id,
                    entry_id=entry_id,
                    exit_code=0,
                )
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    asyncio.ensure_future(self.runtime_bus.publish(complete_event))
            except RuntimeError:
                pass
            except Exception as e:
                log.debug("Failed to emit ExecutionCompletedEvent: %s", e)

        # 7. Log routing via router (execution_result -> HERALD)
        if entry_id and router:
            router.route_publication(
                evidence_type="execution_result",
                specialist="TERMINUS",
                entry_id=entry_id,
                content_preview=f"Execution completed for: {picked_task.description[:60]}",
            )

        # 8. Mark task complete
        task_board.complete_task(
            task_id,
            result={
                "entry_id": entry_id,
                "exit_code": 0,
                "summary": "Execution completed",
            },
        )

        return PhaseResult(
            phase=PipelinePhase.EXECUTION, specialist_name="TERMINUS",
            success=True,
            output=f"TERMINUS executed command (entry={entry_id[:12]})",
            handoff_data={"execution_results": [{"entry_id": entry_id, "exit_code": 0}]},
            duration_ms=(time.time() - phase_start) * 1000,
        )

    async def _execute_report(
        self,
        task_board: Any,
        blackboard: Any,
        task_id: str,
        agent: Any,
    ) -> PhaseResult:
        """Execute REPORT phase: HERALD generates and publishes session report."""
        phase_start = time.time()

        herald = self.specialists.get("HERALD")
        if herald is None:
            return PhaseResult(
                phase=PipelinePhase.REPORTING, specialist_name="HERALD",
                success=False, error="HERALD specialist not registered",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 1. Pick up the report task
        tasks = herald.pickup_task(
            task_board=task_board,
            max_tasks=1,
        )
        if not tasks:
            return PhaseResult(
                phase=PipelinePhase.REPORTING, specialist_name="HERALD",
                success=False, error="No report task to pick up",
                duration_ms=(time.time() - phase_start) * 1000,
            )

        # 2. Generate collaboration summary from blackboard
        summary = herald.generate_collaboration_summary(
            blackboard=blackboard,
            task_board=task_board,
            session_title="Mode B — Task Board Collaboration",
        )

        # 3. Publish session report
        entry_id = herald.generate_session_report(
            blackboard=blackboard,
            summary=summary,
            session_title="Mode B — Session Report",
        )

        # 4. Emit REPORT_GENERATED event on runtime EventBus
        if self.runtime_bus:
            try:
                from runtime_next.models.events import ReportGeneratedEvent
                report_event = ReportGeneratedEvent(
                    id=f"report_{task_id[:8]}_{int(time.time())}",
                    report_id=entry_id,
                    session_title="Mode B — Session Report",
                    summary_length=len(summary.get("full_narrative", "")),
                    evidence_count=len(blackboard.evidence()) if hasattr(blackboard, 'evidence') else 0,
                    challenge_count=len(blackboard.get_challenges()) if hasattr(blackboard, 'get_challenges') else 0,
                )
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    asyncio.ensure_future(self.runtime_bus.publish(report_event))
            except RuntimeError:
                pass
            except Exception as e:
                log.debug("Failed to emit ReportGeneratedEvent: %s", e)

        # 5. Mark task complete
        task_board.complete_task(
            task_id,
            result={
                "entry_id": entry_id,
                "summary_length": len(summary.get("full_narrative", "")),
            },
        )

        report_text = summary.get("full_narrative", "No report generated.")

        return PhaseResult(
            phase=PipelinePhase.REPORTING, specialist_name="HERALD",
            success=True,
            output=report_text,
            handoff_data={"final_response": report_text},
            duration_ms=(time.time() - phase_start) * 1000,
        )

    # ==================================================================
    # Output Aggregation
    # ==================================================================

    def _aggregate_output(
        self,
        blackboard: Any,
        task_board: Any,
    ) -> str:
        """Aggregate the final output from blackboard and task board state.

        Reads the user_reports slot from the blackboard for the HERALD
        report, falling back to a task-board summary.
        """
        # Try to get the HERALD report from the blackboard
        try:
            user_reports = blackboard.read(slot_name="user_reports")
            if user_reports:
                latest = max(
                    user_reports,
                    key=lambda e: getattr(e, "timestamp", ""),
                )
                return latest.content
        except Exception:
            pass

        # Fallback: task board summary
        try:
            snapshot = task_board.snapshot()
            lines = [
                "# Mode B — Task Board Execution",
                "",
                f"Total tasks: {snapshot.get('total_tasks', 0)}",
                f"Active tasks: {snapshot.get('active_tasks', 0)}",
                "",
                "By status:",
            ]
            for status, count in snapshot.get("by_status", {}).items():
                lines.append(f"  {status}: {count}")
            lines.append("")
            lines.append("By type:")
            for ttype, count in snapshot.get("by_type", {}).items():
                lines.append(f"  {ttype}: {count}")
            return "\n".join(lines)
        except Exception as e:
            return f"Mode B execution completed. No detailed report available ({e})."

    # ==================================================================
    # Mode Detection
    # ==================================================================

    @staticmethod
    def detect_mode(user_input: str) -> str:
        """Detect whether to use Mode A (consolidated) or Mode B (task board).

        Heuristic detection:
        - Explicit "@MODE_B" prefix forces Mode B
        - Explicit "@MODE_A" prefix forces Mode A
        - Complex multi-step tasks default to Mode A for now
        - Simple lookup/research tasks use Mode A

        Returns MODE_A or MODE_B constant.
        """
        lower = user_input.strip().lower()

        if lower.startswith("@mode_b"):
            return MODE_B
        if lower.startswith("@mode_a"):
            return MODE_A

        # Default to Mode A for now (backward compatible)
        return MODE_A

    @staticmethod
    def strip_mode_prefix(user_input: str) -> str:
        """Strip the @MODE_A or @MODE_B prefix from input."""
        lower = user_input.strip().lower()
        if lower.startswith("@mode_b"):
            return user_input.strip()[7:].strip()
        if lower.startswith("@mode_a"):
            return user_input.strip()[7:].strip()
        return user_input
