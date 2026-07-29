from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set, Tuple, Callable
from pydantic import BaseModel, Field

from cognition.types import (
    Goal, SubGoal, GoalStatus, PlanStatus, EntryType, Provenance, ProvenanceType,
    BlackboardEntry, ConsensusEvent, ConflictRecord, ConflictSeverity,
    ResearchHypothesis, ResearchFinding, HypothesisStatus,
    StrategicMemoryEntry, MemoryType, UncertaintyClass,
    CognitiveStateSnapshot, ExecutionHypothesis, BlockedPath,
)
from cognition.blackboard import CognitiveBlackboard
from cognition.state import CognitiveStateEngine
from cognition.planner import LongHorizonPlanner, DecompositionStrategy
from cognition.strategy_memory import StrategicMemory
from cognition.research import AutonomousResearchRuntime
from cognition.replan import DynamicReplanningEngine, ReplanTrigger
from cognition.coordination import SpecialistCoordinationRuntime, DelegationMode
from cognition.consensus import MultiAgentConsensusSystem, GovernanceDecision
from cognition.autonomous_learning import AutonomousLearningPipeline

from runtime_next.models.plan import ExecutionPlan, ExecutionNode, NodeType, Criticality

log = logging.getLogger("aelvo.cognition.engine")


class CognitiveEngineConfig(BaseModel):
    max_active_goals: int = 5
    default_plan_budget: int = 30
    auto_research: bool = True
    auto_consensus: bool = True
    governance_enabled: bool = True
    auto_learning: bool = True                    # Phase 9: autonomous learning on/off
    strategy_injection: bool = True               # Phase 9: inject strategies into plans


class CognitiveEngine:
    """Full Integration: Ties all cognitive subsystems together.

    Orchestrates blackboard, state engine, planner (route through
    ArchitectIntelligence when available), strategic memory,
    research runtime, replanning engine, coordination runtime, and
    consensus system. Provides a unified API for the rest of AELVO.

    The planning pipeline is:
        User Goal
            â†“
        Architect Intelligence (when available) or LongHorizonPlanner
            â†“
        Strategic Plan â†’ Execution Graph â†’ Verification â†’ Recovery â†’ Learning
    """

    def __init__(
        self,
        config: Optional[CognitiveEngineConfig] = None,
        repo_intelligence=None,
        forge_memory=None,
        governance_kernel=None,
        specialist_registry: Optional[Dict[str, Any]] = None,
        architect_orchestrator: Optional[Any] = None,
    ):
        """Initialize the CognitiveEngine.

        Args:
            config: Engine configuration.
            repo_intelligence: Repository intelligence engine.
            forge_memory: Forge memory instance.
            governance_kernel: Governance kernel.
            specialist_registry: Registry of all specialists.
            architect_orchestrator: Optional Architect orchestrator.
        """
        self.config = config or CognitiveEngineConfig()
        self.blackboard = CognitiveBlackboard()
        self.state = CognitiveStateEngine()
        self._architect = architect_orchestrator
        # LongHorizonPlanner is the fallback when architect is unavailable
        self.planner = LongHorizonPlanner(repo_intelligence=repo_intelligence)
        self.strategic_memory = StrategicMemory(forge_memory=forge_memory)
        self.research = AutonomousResearchRuntime()
        self.replan = DynamicReplanningEngine()
        self.coordination = SpecialistCoordinationRuntime(
            specialist_registry=specialist_registry
        )
        self.consensus = MultiAgentConsensusSystem(
            governance_kernel=governance_kernel
        )
        # Phase 9: Autonomous Learning Pipeline
        self.learning = AutonomousLearningPipeline(
            strategic_memory=self.strategic_memory,
        )

        self._repo_intel = repo_intelligence
        self._forge = forge_memory
        self._governance = governance_kernel
        self._goal_plan_map: Dict[str, str] = {}
        self._goal_architect_plan_map: Dict[str, Any] = {}
        self._active_plan_id: Optional[str] = None

    def submit_goal(
        self,
        description: str,
        priority: int = 5,
        constraints: Optional[List[str]] = None,
        owner: Optional[str] = None,
    ) -> Goal:
        goal_id = self._generate_id("goal", description)
        goal = Goal(
            id=goal_id,
            description=description,
            priority=priority,
            constraints=constraints or [],
            owner=owner,
        )
        self.state.register_goal(goal)
        self.blackboard.publish(
            slot_name="goals",
            content=f"Goal submitted: {description}",
            entry_type=EntryType.COMMAND,
            provenance=Provenance(
                source_type=ProvenanceType.SYSTEM,
                source_id="cognitive_engine",
            ),
            tags=["goal", "new"],
        )
        log.info("Goal %s submitted: %s", goal_id, description[:60])
        return goal

    def decompose_goal(
        self,
        goal_id: str,
        sub_goal_descriptions: List[str],
    ) -> List[SubGoal]:
        goal = self.state.get_goal(goal_id)
        if goal is None:
            raise ValueError(f"Goal {goal_id} not found")
        sub_goals = []
        for i, desc in enumerate(sub_goal_descriptions):
            sg = SubGoal(
                id=self._generate_id("subgoal", f"{goal_id}_{desc}"),
                parent_goal_id=goal_id,
                description=desc,
                order=i,
            )
            self.state.register_sub_goal(sg)
            sub_goals.append(sg)
        goal.status = GoalStatus.IN_PROGRESS
        log.info("Decomposed goal %s into %d sub-goals", goal_id, len(sub_goals))
        return sub_goals

    def plan_goal(
        self,
        goal_id: str,
        strategy: DecompositionStrategy = DecompositionStrategy.TOP_DOWN,
    ) -> ExecutionPlan:
        """Plan a goal through the unified strategic cognition pipeline.

        When an ArchitectOrchestrator is available, routes through the
        full 14-engine Architect Intelligence Brain (objective analysis,
        repository intelligence, strategic selection, risk analysis,
        execution design, specialist assignment, verification design,
        recovery design, self-critique) â€” producing a repository-aware,
        risk-assessed, verifiable strategic plan.

        Falls back to LongHorizonPlanner heuristic decomposition when
        architect is unavailable.

        Returns an ExecutionPlan that the orchestrator can consume.
        """
        goal = self.state.get_goal(goal_id)
        if goal is None:
            raise ValueError(f"Goal {goal_id} not found")
        sub_goals = self.state.get_sub_goals(goal_id)

        # PHASE 9: Strategy Injection — enrich the plan with learned strategies
        relevant_strategies: List[StrategicMemoryEntry] = []
        if self.config.strategy_injection:
            relevant_strategies = self.learning.get_strategies_for_planning(
                goal_description=goal.description,
                max_results=5,
            )
            if relevant_strategies:
                log.info(
                    "Injected %d learned strategies into plan for goal %s",
                    len(relevant_strategies), goal_id,
                )

        # Route through Architect Intelligence when available
        if self._architect is not None:
            try:
                constraints = {}
                if goal.constraints:
                    for c in goal.constraints:
                        constraints[c] = {"value": True}

                context = {
                    "task": goal.description,
                    "constraints": constraints,
                    "project": "",
                }
                if self._repo_intel:
                    context["repo_intelligence"] = self._repo_intel

                # Create the full 14-section ArchitectPlan
                architect_plan = self._architect.create_plan(
                    goal.description, context
                )

                # Store the architect plan reference
                self._goal_architect_plan_map[goal_id] = architect_plan
                self._goal_plan_map[goal_id] = architect_plan.id
                self._active_plan_id = architect_plan.id

                # Build an ExecutionPlan from the architect plan for compatibility
                plan = self._architect_plan_to_execution_plan(
                    architect_plan, goal
                )

                log.info(
                    "Architect plan %s for goal %s: %d phases, %d checks, score=%.2f",
                    architect_plan.id[:12], goal_id,
                    len(architect_plan.execution_strategy.phases),
                    len(architect_plan.verification_plan.checks),
                    architect_plan.self_review.score,
                )

                self.blackboard.publish(
                    slot_name="plans",
                    content=(
                        f"Architect plan {architect_plan.id[:12]} created for "
                        f"goal {goal_id}: {architect_plan.title[:60]}"
                    ),
                    entry_type=EntryType.DECISION,
                    provenance=Provenance(
                        source_type=ProvenanceType.SYSTEM,
                        source_id="architect",
                    ),
                    tags=["plan", "architect", "strategic"],
                )

                return plan

            except Exception as e:
                log.warning(
                    "Architect planning failed for goal %s, falling back: %s",
                    goal_id, e,
                )

        # Fallback: LongHorizonPlanner heuristic decomposition
        plan = self.planner.create_plan(
            goal=goal,
            sub_goals=sub_goals or None,
            strategy=strategy,
        )
        self._goal_plan_map[goal_id] = plan.id
        self._active_plan_id = plan.id
        self.blackboard.publish(
            slot_name="plans",
            content=f"Plan {plan.id} created for goal {goal_id}: {plan.task_description[:60]}",
            entry_type=EntryType.DECISION,
            provenance=Provenance(
                source_type=ProvenanceType.SYSTEM,
                source_id="planner",
            ),
            tags=["plan", "new"],
        )
        return plan

    def _architect_plan_to_execution_plan(
        self,
        architect_plan: Any,
        goal: Goal,
    ) -> ExecutionPlan:
        """Bridge an ArchitectPlan into an ExecutionPlan for compatibility.

        Extracts phases as ExecutionNodes, maps dependency edges, and
        sets critical path from the architect's strategic output.
        """
        plan = ExecutionPlan(
            id=architect_plan.id,
            task_description=architect_plan.objective.goal,
            total_budget=sum(
                p.estimated_effort
                for p in architect_plan.execution_strategy.phases
            ) or 30,
        )

        # Map each execution phase to an ExecutionNode
        phase_node_map = {}
        for phase in architect_plan.execution_strategy.phases:
            node_id = f"n_{phase.id}"
            phase_node_map[phase.id] = node_id

            # Find specialist for this phase
            specialist = ""
            for assignment in architect_plan.specialist_assignments.assignments:
                if assignment.phase_id == phase.id:
                    specialist = assignment.specialist.value
                    break

            node = ExecutionNode(
                id=node_id,
                description=phase.description,
                node_type=NodeType.SPECIALIST_CALL if specialist else NodeType.TOOL_CALL,
                specialist=specialist,
                estimated_steps=phase.estimated_effort,
            )
            plan.add_node(node)

        # Map dependency edges
        from runtime_next.models.plan import ExecutionEdge as EE
        for edge in architect_plan.execution_strategy.dependency_edges:
            src = phase_node_map.get(edge.source)
            tgt = phase_node_map.get(edge.target)
            if src and tgt:
                plan.add_edge(EE(
                    id=f"e_{src}->{tgt}",
                    source_node_id=src,
                    target_node_id=tgt,
                ))

        # Set critical path
        if architect_plan.execution_strategy.critical_path:
            plan.critical_path = [
                phase_node_map.get(pid, pid)
                for pid in architect_plan.execution_strategy.critical_path
            ]

        # Set entry/exit nodes
        topo = plan.topological_sort()
        if topo:
            plan.entry_node_id = topo[0]
        plan.exit_node_ids = [
            nid for nid in plan.nodes
            if not plan.get_dependent_ids(nid)
        ]

        return plan

    def execute_plan(self, plan_id: str) -> List[str]:
        """Execute a plan and trigger post-execution learning.

        Returns the topological execution order.  After execution,
        the autonomous learning pipeline automatically stores learnings.
        """
        plan = self.planner.get_plan(plan_id)
        if plan is None:
            raise ValueError(f"Plan {plan_id} not found")
        log.info("Executing plan %s (%d nodes)", plan_id, len(plan.nodes))
        execution_order = plan.topological_sort()
        for goal_id, gplan_id in self._goal_plan_map.items():
            if gplan_id == plan_id:
                self.state.update_goal_status(goal_id, GoalStatus.IN_PROGRESS)
                break
        return execution_order

    def report_execution_outcome(
        self,
        goal_id: str,
        outcome: str,
        specialist: str = "",
        execution_summary: str = "",
        successful_strategy_ids: Optional[List[str]] = None,
        failed_strategy_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Report an execution outcome to the autonomous learning pipeline.

        This is the primary entry point for post-execution learning.
        Called by the orchestrator after a pipeline execution completes.

        Automatically:
        1. Extracts and stores learnings from the outcome
        2. Reinforces successful strategies
        3. Flags and penalizes failed strategies
        4. Periodically decays stale entries and consolidates similar ones

        Args:
            goal_id: The goal that was executed.
            outcome: ``"success"`` or ``"failure"``.
            specialist: Which specialist handled the execution.
            execution_summary: Brief summary of what happened.
            successful_strategy_ids: IDs of strategies that contributed.
            failed_strategy_ids: IDs of strategies that contributed to failure.

        Returns:
            Dict with learning results.
        """
        if not self.config.auto_learning:
            return {"stored": 0, "reinforced": 0, "flagged": 0, "decayed": 0, "consolidated": 0}

        goal = self.state.get_goal(goal_id)
        goal_description = goal.description if goal else goal_id

        results = self.learning.process_execution_outcome(
            goal_description=goal_description,
            outcome=outcome,
            specialist=specialist,
            execution_summary=execution_summary,
            successful_strategy_ids=successful_strategy_ids,
            failed_strategy_ids=failed_strategy_ids,
        )

        log.info(
            "Execution outcome reported for goal %s: outcome=%s, learning=%s",
            goal_id[:12], outcome, results,
        )

        return results


    def research_topic(
        self,
        topic: str,
        proposed_by: str = "system",
        tags: Optional[List[str]] = None,
    ) -> ResearchHypothesis:
        hypothesis = self.research.propose_hypothesis(
            description=topic,
            proposed_by=proposed_by,
            tags=tags,
        )
        if self.config.auto_research:
            self.research.investigate(hypothesis.id)
            findings = self.research.search_knowledge(topic)
            for f in findings:
                self.research.add_evidence(
                    hypothesis_id=hypothesis.id,
                    description=f.description,
                    source=f.source,
                    content=f.content,
                    relevance=f.relevance,
                    reliability=f.reliability,
                    supports=True,
                )
        self.blackboard.publish(
            slot_name="research",
            content=f"Research hypothesis: {topic}",
            entry_type=EntryType.HYPOTHESIS,
            provenance=Provenance(
                source_type=ProvenanceType.RESEARCH,
                source_id=hypothesis.id,
            ),
            tags=tags or [],
        )
        return hypothesis

    def conclude_research(self, hypothesis_id: str) -> Optional[ResearchFinding]:
        return self.research.conclude_hypothesis(hypothesis_id)

    def store_memory(
        self,
        memory_type: MemoryType,
        content: str,
        importance: float = 0.5,
        source_goal_id: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> StrategicMemoryEntry:
        return self.strategic_memory.store(
            memory_type=memory_type,
            content=content,
            importance=importance,
            source_goal_id=source_goal_id,
            tags=tags,
        )

    def recall_memories(
        self,
        query: str,
        memory_type: Optional[MemoryType] = None,
        max_results: int = 5,
    ) -> List[StrategicMemoryEntry]:
        return self.strategic_memory.search(query, memory_type=memory_type, max_results=max_results)

    def handle_failure(
        self,
        plan: ExecutionPlan,
        node_id: str,
        reason: str,
    ) -> Optional[Any]:
        blocked = BlockedPath(
            id=self._generate_id("blocked", f"{node_id}_{reason}"),
            step_id=node_id,
            reason=reason,
            blocker_type="execution_failure",
        )
        self.state.add_blocked_path(blocked)

        replan = self.replan.evaluate(
            plan=plan,
            trigger=ReplanTrigger.NODE_FAILURE,
            context={"failed_node_id": node_id, "failure_reason": reason},
        )
        if replan and replan.requires_consensus and self.config.auto_consensus:
            conflict = ConflictRecord(
                id=self._generate_id("conflict", f"failure_{node_id}"),
                description=f"Replan requires consensus: {replan.description}",
                severity=ConflictSeverity.MEDIUM,
            )
            self.consensus.resolve_conflict(conflict)
        return replan

    def apply_governance(self, event_id: str) -> GovernanceDecision:
        return self.consensus.apply_governance(event_id)

    def snapshot(self) -> CognitiveStateSnapshot:
        snapshot = self.state.snapshot()
        snapshot.blackboard_slot_count = len(self.blackboard.slot_names())
        snapshot.memory_entries_count = self.strategic_memory.snapshot()["total_entries"]
        snapshot.consensus_events_count = self.consensus.snapshot()["total_events"]
        snapshot.research_hypotheses_count = self.research.snapshot()["total_hypotheses"]
        # Phase 9: Learning metrics
        snapshot.metadata["learning"] = self.learning.snapshot()
        return snapshot

    def to_terminal_display(self) -> str:
        state_str = self.state.to_terminal_display()
        bb = self.blackboard.snapshot()
        planning = f"  Plans: {len(self.planner.list_plans())}"
        memory = self.strategic_memory.snapshot()
        mem_str = f"  Memory: {memory['total_entries']} entries"
        consensus = self.consensus.snapshot()
        con_str = f"  Consensus: {consensus['total_events']} events"
        research = self.research.snapshot()
        res_str = f"  Research: {research['total_hypotheses']} hypotheses"
        coord = self.coordination.snapshot()
        coo_str = f"  Delegations: {coord['total_delegations']}"
        return "\n".join([
            state_str,
            f"  Blackboard: {bb['active_entry_count']} active entries across {bb['slot_count']} slots",
            planning,
            mem_str,
            con_str,
            res_str,
            coo_str,
        ])

    def status(self) -> Dict[str, Any]:
        return {
            "state": self.state.snapshot().model_dump(),
            "blackboard": self.blackboard.snapshot(),
            "planner": {"plans": len(self.planner.list_plans())},
            "memory": self.strategic_memory.snapshot(),
            "research": self.research.snapshot(),
            "replan": self.replan.snapshot(),
            "coordination": self.coordination.snapshot(),
            "consensus": self.consensus.snapshot(),
            "learning": self.learning.snapshot(),
        }

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
