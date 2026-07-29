from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set, Tuple
from enum import Enum

from runtime_next.models.plan import (
    ExecutionPlan, ExecutionNode, ExecutionEdge, ExecutionPattern,
    NodeType, Criticality,
)

from cognition.types import (
    Goal, SubGoal, PlanStep, PlanDependency,
)

log = logging.getLogger("aelvo.cognition.planner")


class DecompositionStrategy(str, Enum):
    TOP_DOWN = "top_down"
    BOTTOM_UP = "bottom_up"
    PATTERN_MATCHED = "pattern_matched"
    RESEARCH_GUIDED = "research_guided"


class LongHorizonPlanner:
    """Long-Horizon Planning Engine.

    Decomposes goals into execution plans. Queries repo_intelligence for
    codebase grounding. Produces ExecutionPlan graphs consumable by the
    experimental graph-based execution engine.
    """

    def __init__(self, repo_intelligence=None, patterns: Optional[List[ExecutionPattern]] = None):
        self._repo_intel = repo_intelligence
        self._patterns = patterns or []
        self._plans: Dict[str, ExecutionPlan] = {}
        self._plan_steps: Dict[str, Dict[str, PlanStep]] = {}
        self._plan_dependencies: Dict[str, List[PlanDependency]] = {}

    def create_plan(
        self,
        goal: Goal,
        sub_goals: Optional[List[SubGoal]] = None,
        context: Optional[Dict[str, Any]] = None,
        strategy: DecompositionStrategy = DecompositionStrategy.TOP_DOWN,
    ) -> ExecutionPlan:
        context = context or {}
        plan_id = self._generate_plan_id(goal)
        existing = self._get_grounding_context(goal, sub_goals)
        context.update(existing)

        plan = ExecutionPlan(
            id=plan_id,
            task_description=goal.description,
            total_budget=context.get("budget", 30),
        )

        steps = self._decompose_goal(goal, sub_goals, plan, context, strategy)

        self._apply_dependencies(plan, steps, context)
        self._identify_parallelism(plan)
        self._assign_criticality(plan)

        topo = plan.topological_sort()
        if topo:
            plan.entry_node_id = topo[0]
        plan.exit_node_ids = self._find_exit_nodes(plan)
        plan.critical_path = plan.calculate_critical_path()

        self._plans[plan_id] = plan
        log.info(
            "Created plan %s for goal %s: %d nodes, %d edges, strategy=%s",
            plan_id, goal.id, len(plan.nodes), len(plan.edges), strategy.value,
        )
        return plan

    def get_plan(self, plan_id: str) -> Optional[ExecutionPlan]:
        return self._plans.get(plan_id)

    def get_plan_steps(self, plan_id: str) -> List[PlanStep]:
        return list(self._plan_steps.get(plan_id, {}).values())

    def update_plan(self, plan_id: str, plan: ExecutionPlan) -> None:
        self._plans[plan_id] = plan

    def list_plans(self) -> List[str]:
        return list(self._plans.keys())

    def _decompose_goal(
        self,
        goal: Goal,
        sub_goals: Optional[List[SubGoal]],
        plan: ExecutionPlan,
        context: Dict[str, Any],
        strategy: DecompositionStrategy,
    ) -> List[PlanStep]:
        steps: List[PlanStep] = []
        node_map: Dict[str, str] = {}
        actual_sub_goals = sub_goals or []
        matched_pattern = self._match_pattern(goal.description)

        if matched_pattern:
            strategy = DecompositionStrategy.PATTERN_MATCHED

        for i, sg in enumerate(actual_sub_goals):
            node_id = self._sub_goal_to_node(sg, goal, plan)
            node_map[sg.id] = node_id
            step = PlanStep(
                id=sg.id,
                plan_id=plan.id,
                description=sg.description,
                goal_id=goal.id,
                sub_goal_id=sg.id,
                execution_node_id=node_id,
            )
            steps.append(step)

        if not steps:
            nodes_and_steps = self._decompose_from_description(goal, plan, strategy)
            for node, step in nodes_and_steps:
                plan.add_node(node)
                steps.append(step)
                node_map[step.id] = node.id

        self._plan_steps[plan.id] = {s.id: s for s in steps}
        plan.pattern_source = matched_pattern.id if matched_pattern else None
        return steps

    def _sub_goal_to_node(self, sub_goal: SubGoal, goal: Goal, plan: ExecutionPlan) -> str:
        node_id = self._next_node_id(plan, sub_goal.id)
        node = ExecutionNode(
            id=node_id,
            description=sub_goal.description,
            node_type=self._infer_node_type(sub_goal.description),
            criticality=Criticality.IMPORTANT,
            estimated_steps=sub_goal.order + 1,
        )
        plan.add_node(node)
        return node_id

    def _decompose_from_description(
        self,
        goal: Goal,
        plan: ExecutionPlan,
        strategy: DecompositionStrategy,
    ) -> List[Tuple[ExecutionNode, PlanStep]]:
        results: List[Tuple[ExecutionNode, PlanStep]] = []
        task_lower = goal.description.lower()

        is_refactor = any(w in task_lower for w in ["refactor", "rewrite", "restructure", "redesign"])
        is_fix = any(w in task_lower for w in ["fix", "bug", "error", "issue", "broken"])
        is_feature = any(w in task_lower for w in ["add", "create", "implement", "build", "new"])
        has_files = any(w in task_lower for w in ["file", "module", "class", "function", "component"])
        has_security = any(w in task_lower for w in ["security", "auth", "oauth", "vulnerability", "permission"])
        has_test = any(w in task_lower for w in ["test", "verify", "validate", "check"])

        step_counter = 0

        def make_step(desc: str, nt: NodeType, spec: str = "", tool: str = "",
                      crit: Criticality = Criticality.IMPORTANT, est: int = 1,
                      sc: int = 0) -> Tuple[ExecutionNode, PlanStep]:
            nonlocal step_counter
            step_counter += 1
            sid = f"step_{step_counter:04d}"
            nid = self._next_node_id(plan, sid)
            node = ExecutionNode(
                id=nid,
                description=desc,
                node_type=nt,
                specialist=spec,
                tool_name=tool,
                criticality=crit,
                estimated_steps=est,
                steps_consumed=sc,
            )
            step = PlanStep(
                id=sid,
                plan_id=plan.id,
                description=desc,
                goal_id=goal.id,
                execution_node_id=nid,
            )
            return node, step

        if is_feature or is_refactor:
            results.append(make_step(
                "Read current files/modules", NodeType.TOOL_CALL, tool="read_file", est=2
            ))
            results.append(make_step(
                "Analyze structure and dependencies", NodeType.SPECIALIST_CALL, spec="ARCHITECT", est=3
            ))
            if is_refactor:
                results.append(make_step(
                    "Identify all callers and usages", NodeType.TOOL_CALL, tool="search_code", est=2
                ))
            results.append(make_step(
                "Make the changes", NodeType.SPECIALIST_CALL, spec="FORGE", est=5
            ))
            if is_refactor:
                results.append(make_step(
                    "Update all callers", NodeType.SPECIALIST_CALL, spec="FORGE", est=3
                ))

        elif is_fix:
            results.append(make_step(
                "Read the failing code", NodeType.TOOL_CALL, tool="read_file", est=2
            ))
            results.append(make_step(
                "Diagnose the root cause", NodeType.SPECIALIST_CALL, spec="FORGE", est=2
            ))
            results.append(make_step(
                "Apply the fix", NodeType.SPECIALIST_CALL, spec="FORGE", est=2
            ))

        else:
            results.append(make_step(
                "Research the topic", NodeType.MEMORY_QUERY, est=1, crit=Criticality.OPTIONAL
            ))
            results.append(make_step(
                "Respond to user", NodeType.TOOL_CALL, tool="respond", est=1, crit=Criticality.CRITICAL
            ))

        if has_files and not is_feature:
            results.append(make_step(
                "Query memory for prior context", NodeType.MEMORY_QUERY, est=1, crit=Criticality.OPTIONAL
            ))

        results.append(make_step(
            "Verify the output",
            NodeType.VERIFICATION,
            spec="SENTINEL" if has_security else "FORGE",
            est=1,
        ))

        if has_security:
            results.append(make_step(
                "Security review", NodeType.SPECIALIST_CALL, spec="SENTINEL", est=3
            ))

        if has_test:
            results.append(make_step(
                "Run type checker", NodeType.TOOL_CALL, tool="bash_exec", est=1
            ))
            results.append(make_step(
                "Run test suite", NodeType.TOOL_CALL, tool="bash_exec", est=2
            ))

        results.append(make_step(
            "Synthesize final result", NodeType.SYNTHESIS, est=1, crit=Criticality.CRITICAL
        ))

        return results

    def _apply_dependencies(
        self,
        plan: ExecutionPlan,
        steps: List[PlanStep],
        context: Dict[str, Any],
    ) -> None:
        edges_added: Set[Tuple[str, str]] = set()

        def add_edge(src: str, tgt: str):
            key = (src, tgt)
            if key not in edges_added and src in plan.nodes and tgt in plan.nodes:
                plan.add_edge(ExecutionEdge(
                    id=f"e_{src}->{tgt}",
                    source_node_id=src,
                    target_node_id=tgt,
                ))
                edges_added.add(key)

        nid_list = [s.execution_node_id for s in steps if s.execution_node_id]
        if not nid_list:
            return

        for i in range(len(nid_list) - 1):
            add_edge(nid_list[i], nid_list[i + 1])

        dep_steps = [s for s in steps if s.dependencies]
        for step in dep_steps:
            step_node = step.execution_node_id
            if not step_node:
                continue
            for dep_id in step.dependencies:
                dep_step = self._plan_steps.get(plan.id, {}).get(dep_id)
                if dep_step and dep_step.execution_node_id:
                    add_edge(dep_step.execution_node_id, step_node)

    def _identify_parallelism(self, plan: ExecutionPlan) -> None:
        tiers: List[List[str]] = []
        remaining = set(plan.nodes.keys())
        completed: Set[str] = set()
        while remaining:
            ready = {nid for nid in remaining
                     if all(d in completed for d in plan.get_dependency_ids(nid))}
            if not ready:
                ready = {next(iter(remaining))}
            tiers.append(list(ready))
            completed.update(ready)
            remaining -= ready
        branches = []
        for tier in tiers:
            if len(tier) > 1:
                memory_related = [n for n in tier if "memory" in n or "research" in n]
                tool_related = [n for n in tier if "read" in n or "search" in n]
                if memory_related and tool_related:
                    branches.append(memory_related + tool_related)
                else:
                    branches.append(tier)
        plan.parallel_branches = branches

    def _assign_criticality(self, plan: ExecutionPlan) -> None:
        for node in plan.nodes.values():
            if node.node_type == NodeType.SYNTHESIS:
                node.criticality = Criticality.CRITICAL
            elif node.node_type == NodeType.VERIFICATION:
                node.criticality = Criticality.IMPORTANT
            elif node.node_type == NodeType.MEMORY_QUERY:
                node.criticality = Criticality.OPTIONAL

    def _find_exit_nodes(self, plan: ExecutionPlan) -> List[str]:
        return [nid for nid in plan.nodes if not plan.get_dependent_ids(nid)]

    def _infer_node_type(self, description: str) -> NodeType:
        d = description.lower()
        if any(w in d for w in ["read", "search", "lookup", "find", "list"]):
            return NodeType.TOOL_CALL
        if any(w in d for w in ["analyze", "design", "plan", "architect"]):
            return NodeType.SPECIALIST_CALL
        if any(w in d for w in ["verify", "validate", "check", "test"]):
            return NodeType.VERIFICATION
        if any(w in d for w in ["decide", "choose", "select"]):
            return NodeType.DECISION
        if any(w in d for w in ["synthesize", "summarize", "report"]):
            return NodeType.SYNTHESIS
        if any(w in d for w in ["memory", "recall"]):
            return NodeType.MEMORY_QUERY
        return NodeType.TOOL_CALL

    def _get_grounding_context(
        self,
        goal: Goal,
        sub_goals: Optional[List[SubGoal]],
    ) -> Dict[str, Any]:
        context: Dict[str, Any] = {}
        if self._repo_intel is None:
            return context
        try:
            query_text = goal.description
            if sub_goals:
                query_text += " " + " ".join(sg.description for sg in sub_goals)
            symbol_graph = self._repo_intel.get_symbol_graph()
            result_obj = self._repo_intel.query_engine.search(
                query_text, symbol_graph=symbol_graph, max_results=5
            )
            results = result_obj.data or []
            context["repo_intel_results"] = results
            files_found = set()
            for r in results:
                if hasattr(r, "file_path"):
                    files_found.add(r.file_path)
            context["relevant_files"] = list(files_found)
            log.debug("Repo intelligence grounding found %d files", len(files_found))
        except Exception as e:
            log.warning("Repo intelligence grounding failed: %s", e)
        return context

    def _match_pattern(self, task: str) -> Optional[ExecutionPattern]:
        best_score = 0.0
        best_pattern = None
        for pattern in self._patterns:
            score = pattern.similarity_to(task)
            if score > best_score and score > 0.6:
                best_score = score
                best_pattern = pattern
        return best_pattern

    def _generate_plan_id(self, goal: Goal) -> str:
        raw = f"plan_{goal.id}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _next_node_id(self, plan: ExecutionPlan, base: str) -> str:
        counter = len(plan.nodes) + 1
        return f"n_{counter:04d}_{base[:12]}"
