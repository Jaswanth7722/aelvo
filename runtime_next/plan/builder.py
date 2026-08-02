"""Upgraded PlanBuilder with hierarchical goal decomposition, preconditions,
uncertainty tracking, and integration with the cognitive layer types.

Builds execution plans that the cognitive state engine can track and the
execution graph engine can consume directly.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from ..models.plan import (
    ExecutionPlan,
    ExecutionNode,
    ExecutionEdge,
    ExecutionPattern,
    NodeType,
    Criticality,
    OutputContract,
)
from ..models.node import NodeDefinition as LegacyNode

# Import cognitive types for upgraded planning
from cognition.types import (
    ConfidenceLevel,
    Goal,
    PlanPrecondition,
    PlanStep,
    PlanUncertainty,
    SpecialistRole,
    SubGoal,
)

log = logging.getLogger("aelvo.plan.builder")


class PlanBuilder:
    """Builds execution plans from task descriptions with hierarchical goal support.

    Upgraded with:
    - Hierarchical goal decomposition (Goal â†’ SubGoal â†’ PlanStep)
    - Precondition tracking for each step
    - Uncertainty levels on steps based on task type heuristics
    - Dependency tracking between goals and steps
    - Integration with the cognitive state engine
    """

    def __init__(self, patterns: Optional[List[ExecutionPattern]] = None):
        self._patterns = patterns or []
        self._counter = 0

    def build(self, task_description: str, context: Optional[Dict[str, Any]] = None) -> ExecutionPlan:
        """Build an execution plan from a task description.

        Also creates a Goal hierarchy in context for cognitive state tracking.
        """
        context = context or {}
        plan_id = hashlib.sha256(f"plan_{task_description}_{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:16]
        plan = ExecutionPlan(id=plan_id, task_description=task_description)

        # Build the goal hierarchy
        goal = self._build_goal(task_description, context, plan_id)
        context["cognitive_goal"] = goal

        stage1_nodes = self._decompose_task(task_description, context, goal)
        for node in stage1_nodes:
            plan.add_node(node)

        self._analyze_dependencies(plan, task_description, context)
        self._assign_criticality(plan, task_description)
        self._identify_parallelism(plan)
        self._allocate_budget(plan, context)

        topo = plan.topological_sort()
        if topo:
            plan.entry_node_id = topo[0]
        plan.exit_node_ids = self._find_exit_nodes(plan)
        plan.critical_path = plan.calculate_critical_path()

        matched_pattern = self._match_pattern(task_description)
        if matched_pattern:
            plan.pattern_source = matched_pattern.id
            self._apply_pattern(plan, matched_pattern)

        log.info(
            f"Plan {plan_id}: {len(plan.nodes)} nodes, {len(plan.edges)} edges, "
            f"critical_path={len(plan.critical_path)}, budget={plan.total_budget}, "
            f"goal='{goal.description[:40]}'"
        )
        return plan

    def build_with_goal(
        self,
        goal: Goal,
        context: Optional[Dict[str, Any]] = None,
    ) -> ExecutionPlan:
        """Build an execution plan directly from a Goal hierarchy.

        This allows the cognitive state engine to set the goal first, then
        have the plan builder generate the corresponding execution graph.
        """
        context = context or {}
        context["cognitive_goal"] = goal

        task_description = goal.description
        plan_id = hashlib.sha256(f"plan_{goal.id}_{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:16]
        plan = ExecutionPlan(id=plan_id, task_description=task_description)

        # Use the goal's sub-goals and steps to build nodes
        for sg in goal.sub_goals:
            self._subgoal_to_nodes(sg, plan)

        # If no sub-goals with steps, fall back to heuristic decomposition
        if len(plan.nodes) == 0:
            return self.build(task_description, context)

        self._analyze_dependencies(plan, task_description, context)
        self._assign_criticality(plan, task_description)
        self._identify_parallelism(plan)
        self._allocate_budget(plan, context)

        topo = plan.topological_sort()
        if topo:
            plan.entry_node_id = topo[0]
        plan.exit_node_ids = self._find_exit_nodes(plan)
        plan.critical_path = plan.calculate_critical_path()

        return plan

    def _build_goal(self, task: str, context: Dict[str, Any], plan_id: str) -> Goal:
        """Build a Goal hierarchy from the task description."""
        task_lower = task.lower()

        # Determine goal type for success criteria
        is_refactor = any(w in task_lower for w in ["refactor", "rewrite", "restructure"])
        is_fix = any(w in task_lower for w in ["fix", "bug", "error", "issue", "broken"])
        is_feature = any(w in task_lower for w in ["add", "create", "implement", "build", "new"])
        has_security = any(w in task_lower for w in ["security", "auth", "oauth", "vulnerability"])
        has_test = any(w in task_lower for w in ["test", "verify", "validate"])

        # Build success criteria based on task type
        success_criteria = self._infer_success_criteria(task, is_refactor, is_fix, is_feature, has_security, has_test)

        # Build sub-goals
        goal_id = self._next_id("goal")
        sub_goals = self._build_sub_goals(
            task, is_refactor, is_fix, is_feature, has_security, has_test,
            goal_id=goal_id, plan_id=plan_id,
        )

        return Goal(
            id=goal_id,
            description=task,
            success_criteria=success_criteria,
            sub_goals=sub_goals,
            sub_goal_ids=[sg.id for sg in sub_goals],
        )

    def _infer_success_criteria(
        self, task: str, is_refactor: bool, is_fix: bool,
        is_feature: bool, has_security: bool, has_test: bool,
    ) -> List[str]:
        """Infer success criteria from task type."""
        criteria = []

        if is_refactor:
            criteria.extend([
                "Existing functionality is preserved",
                "All references to changed code are updated",
                "Type annotations are correct and complete",
            ])
        if is_fix:
            criteria.extend([
                "Root cause is identified and addressed",
                "Test coverage for the fix is added",
                "No regressions in related functionality",
            ])
        if is_feature:
            criteria.extend([
                "Feature is implemented according to requirements",
                "Error handling and edge cases are covered",
                "Feature is properly tested",
            ])
        if has_security:
            criteria.append("No security vulnerabilities introduced")
        if has_test:
            criteria.append("All tests pass")

        if not criteria:
            criteria.append("Task is completed correctly")

        return criteria

    def _make_step(
        self,
        description: str,
        sub_goal: SubGoal,
        plan_id: str,
        goal_id: str,
        specialist: Optional[SpecialistRole] = None,
        preconditions: Optional[List[PlanPrecondition]] = None,
        uncertainty: Optional[PlanUncertainty] = None,
        estimated_effort: int = 1,
    ) -> PlanStep:
        """Construct a PlanStep with all identity fields populated."""
        return PlanStep(
            id=self._next_id(description.split()[0].lower() if description else "step"),
            plan_id=plan_id,
            goal_id=goal_id,
            sub_goal_id=sub_goal.id,
            description=description,
            specialist=specialist.value if isinstance(specialist, SpecialistRole) else specialist,
            preconditions=preconditions or [],
            uncertainty=uncertainty,
            estimated_effort=estimated_effort,
        )

    def _build_sub_goals(
        self, task: str, is_refactor: bool, is_fix: bool,
        is_feature: bool, has_security: bool, has_test: bool,
        goal_id: str, plan_id: str,
    ) -> List[SubGoal]:
        """Build sub-goals for the task."""
        sub_goals: List[SubGoal] = []

        # Phase 1: Understanding / Investigation
        investigate = SubGoal(
            id=self._next_id("investigate"),
            parent_goal_id=goal_id,
            description=f"Investigate and understand the current state of {self._extract_topic(task)}",
            success_criteria=["Current implementation is understood", "Relevant files are identified"],
        )
        investigate.steps = [
            self._make_step(
                "Read relevant files and understand current implementation",
                sub_goal=investigate, plan_id=plan_id, goal_id=goal_id,
                specialist=SpecialistRole.ARCHITECT,
                preconditions=[PlanPrecondition(description="Files exist", check_type="automated")],
            ),
            self._make_step(
                f"Query memory for prior context about {self._extract_topic(task)}",
                sub_goal=investigate, plan_id=plan_id, goal_id=goal_id,
                specialist=SpecialistRole.ORACLE,
                uncertainty=PlanUncertainty(level=ConfidenceLevel.LOW),
            ),
        ]
        sub_goals.append(investigate)

        # Phase 2: Implementation
        implement = SubGoal(
            id=self._next_id("implement"),
            parent_goal_id=goal_id,
            description=f"Implement the {self._extract_action(task)}",
            success_criteria=[f"Changes are correctly applied to {self._extract_topic(task)}"],
        )

        if is_refactor:
            implement.steps = [
                self._make_step(
                    "Identify all callers and usages of the code being refactored",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                ),
                self._make_step(
                    "Make the refactoring changes",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                    preconditions=[
                        PlanPrecondition(description="Current implementation is understood", check_type="verified"),
                    ],
                ),
                self._make_step(
                    "Update all callers of the changed code",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                ),
            ]
        elif is_fix:
            implement.steps = [
                self._make_step(
                    "Diagnose the root cause of the issue",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.ARCHITECT,
                    preconditions=[
                        PlanPrecondition(description="Relevant code has been read", check_type="verified"),
                    ],
                ),
                self._make_step(
                    "Apply the fix",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                    uncertainty=PlanUncertainty(level=ConfidenceLevel.MEDIUM),
                ),
            ]
        elif is_feature:
            implement.steps = [
                self._make_step(
                    "Design the implementation approach",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.ARCHITECT,
                ),
                self._make_step(
                    "Implement the feature",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                    estimated_effort=5,
                ),
            ]
        else:
            implement.steps = [
                self._make_step(
                    "Execute the required changes",
                    sub_goal=implement, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                    estimated_effort=3,
                ),
            ]

        sub_goals.append(implement)

        # Phase 3: Verification
        verify = SubGoal(
            id=self._next_id("verify"),
            parent_goal_id=goal_id,
            description=f"Verify the {self._extract_action(task)} is correct",
            success_criteria=["Changes are verified", "No regressions"],
        )
        verify.steps = []

        if has_security:
            verify.steps.append(
                self._make_step(
                    "Security review of all changes",
                    sub_goal=verify, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.SENTINEL,
                    preconditions=[PlanPrecondition(description="Implementation is complete", check_type="verified")],
                ),
            )

        verify.steps.append(
            self._make_step(
                "Verify correctness of the changes",
                sub_goal=verify, plan_id=plan_id, goal_id=goal_id,
                specialist=SpecialistRole.SENTINEL if has_security else SpecialistRole.FORGE,
                preconditions=[PlanPrecondition(description="Implementation is complete", check_type="verified")],
            ),
        )

        if has_test:
            verify.steps.extend([
                self._make_step(
                    "Run type checker",
                    sub_goal=verify, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                    estimated_effort=1,
                ),
                self._make_step(
                    "Run test suite",
                    sub_goal=verify, plan_id=plan_id, goal_id=goal_id,
                    specialist=SpecialistRole.FORGE,
                    estimated_effort=2,
                ),
            ])

        sub_goals.append(verify)

        # Phase 4: Synthesis
        synthesize = SubGoal(
            id=self._next_id("synthesize"),
            parent_goal_id=goal_id,
            description="Synthesize and report results",
            success_criteria=["Results are clearly communicated"],
        )
        synthesize.steps = [
            self._make_step(
                "Synthesize findings and produce final output",
                sub_goal=synthesize, plan_id=plan_id, goal_id=goal_id,
                specialist=SpecialistRole.HERMES,
            ),
        ]
        sub_goals.append(synthesize)

        return sub_goals

    def _subgoal_to_nodes(self, sg: SubGoal, plan: ExecutionPlan):
        """Convert a SubGoal and its steps into execution nodes."""
        for step in sg.steps:
            # Determine node type based on specialist
            node_type = self._specialist_to_node_type(step.specialist)

            # Set criticality from step uncertainty
            criticality = Criticality.IMPORTANT
            if step.uncertainty and step.uncertainty.is_high_uncertainty:
                criticality = Criticality.CRITICAL

            node = ExecutionNode(
                id=self._next_id(step.description.split()[0].lower() if step.description else "task"),
                description=step.description,
                node_type=node_type,
                specialist=step.specialist,
                criticality=criticality,
                estimated_steps=step.estimated_effort,
            )
            plan.add_node(node)

        # Recursively handle sub-sub-goals
        for child_sg in sg.sub_goals:
            self._subgoal_to_nodes(child_sg, plan)

    def _specialist_to_node_type(self, specialist: Any) -> NodeType:
        if isinstance(specialist, SpecialistRole):
            role = specialist
        elif isinstance(specialist, str):
            try:
                role = SpecialistRole(specialist.upper())
            except ValueError:
                return NodeType.TOOL_CALL
        else:
            return NodeType.TOOL_CALL
        mapping = {
            SpecialistRole.FORGE: NodeType.TOOL_CALL,
            SpecialistRole.ARCHITECT: NodeType.SPECIALIST_CALL,
            SpecialistRole.ORACLE: NodeType.MEMORY_QUERY,
            SpecialistRole.SENTINEL: NodeType.VERIFICATION,
            SpecialistRole.HERMES: NodeType.SYNTHESIS,
            SpecialistRole.HERALD: NodeType.TOOL_CALL,
            SpecialistRole.TERMINUS: NodeType.DECISION,
        }
        return mapping.get(role, NodeType.TOOL_CALL)

    def _extract_topic(self, task: str) -> str:
        """Extract the topic/area from a task description."""
        stop_words = {"the", "a", "an", "to", "in", "of", "for", "and", "or", "is", "are"}
        words = task.split()
        # Find the key noun phrases (skip verbs at the beginning)
        meaningful = [w for w in words if w.lower() not in stop_words and len(w) > 2]
        if len(meaningful) > 3:
            return " ".join(meaningful[-3:])
        return task[:40]

    def _extract_action(self, task: str) -> str:
        """Extract the action from a task description."""
        task_lower = task.lower()
        if "refactor" in task_lower:
            return "refactoring"
        if "fix" in task_lower or "bug" in task_lower:
            return "fix"
        if "implement" in task_lower or "add" in task_lower or "create" in task_lower:
            return "implementation"
        return "change"

    # â”€â”€ Legacy methods (preserved for backward compatibility) â”€â”€â”€â”€â”€â”€â”€â”€

    def _decompose_task(self, task: str, context: Dict[str, Any], goal: Optional[Goal] = None) -> List[ExecutionNode]:
        """Heuristic task decomposition into execution nodes."""
        nodes: List[ExecutionNode] = []
        task_lower = task.lower()

        is_refactor = any(w in task_lower for w in ["refactor", "rewrite", "restructure", "redesign"])
        is_fix = any(w in task_lower for w in ["fix", "bug", "error", "issue", "broken"])
        is_feature = any(w in task_lower for w in ["add", "create", "implement", "build", "new"])
        has_security = any(w in task_lower for w in ["security", "auth", "oauth", "vulnerability", "permission"])
        has_test = any(w in task_lower for w in ["test", "verify", "validate", "check"])

        def nid(base):
            return self._next_id(base)

        if is_feature or is_refactor:
            nodes.append(ExecutionNode(
                id=nid("read"), description="Read current files/modules",
                node_type=NodeType.TOOL_CALL, tool_name="read_file",
                output_contract=OutputContract(required_fields=["status", "data"]),
                estimated_steps=2,
            ))
            nodes.append(ExecutionNode(
                id=nid("analyze"), description="Analyze structure and dependencies",
                node_type=NodeType.SPECIALIST_CALL, specialist="ARCHITECT",
                estimated_steps=3,
            ))
            if is_refactor:
                nodes.append(ExecutionNode(
                    id=nid("identify_callers"), description="Identify all callers and usages",
                    node_type=NodeType.TOOL_CALL, tool_name="search_code",
                    estimated_steps=2,
                ))
            nodes.append(ExecutionNode(
                id=nid("make_changes"), description="Make the changes",
                node_type=NodeType.SPECIALIST_CALL, specialist="FORGE",
                estimated_steps=5,
            ))
            if is_refactor:
                nodes.append(ExecutionNode(
                    id=nid("update_callers"), description="Update all callers of changed code",
                    node_type=NodeType.SPECIALIST_CALL, specialist="FORGE",
                    estimated_steps=3,
                ))

        elif is_fix:
            nodes.append(ExecutionNode(
                id=nid("read"), description="Read the failing code",
                node_type=NodeType.TOOL_CALL, tool_name="read_file",
                output_contract=OutputContract(required_fields=["status", "data"]),
                estimated_steps=2,
            ))
            nodes.append(ExecutionNode(
                id=nid("diagnose"), description="Diagnose the root cause",
                node_type=NodeType.SPECIALIST_CALL, specialist="FORGE",
                estimated_steps=2,
            ))
            nodes.append(ExecutionNode(
                id=nid("fix"), description="Apply the fix",
                node_type=NodeType.SPECIALIST_CALL, specialist="FORGE",
                estimated_steps=2,
            ))

        else:
            nodes.append(ExecutionNode(
                id=nid("research"), description="Research the topic",
                node_type=NodeType.MEMORY_QUERY,
                criticality=Criticality.OPTIONAL,
                estimated_steps=1,
            ))
            nodes.append(ExecutionNode(
                id=nid("respond"), description="Respond to user",
                node_type=NodeType.TOOL_CALL, tool_name="respond",
                criticality=Criticality.CRITICAL,
                estimated_steps=1,
            ))

        if has_files(task_lower) and not is_feature:
            nodes.append(ExecutionNode(
                id=nid("memory"), description="Query memory for prior context",
                node_type=NodeType.MEMORY_QUERY,
                criticality=Criticality.OPTIONAL,
                estimated_steps=1,
            ))

        verification_node = ExecutionNode(
            id=nid("verify"), description="Verify the output",
            node_type=NodeType.VERIFICATION,
            specialist="SENTINEL" if has_security else "FORGE",
            criticality=Criticality.IMPORTANT,
            estimated_steps=1,
        )
        nodes.append(verification_node)

        if has_security:
            nodes.append(ExecutionNode(
                id=nid("security_review"), description="Security review of changes",
                node_type=NodeType.SPECIALIST_CALL, specialist="SENTINEL",
                criticality=Criticality.IMPORTANT,
                estimated_steps=3,
            ))

        if has_test:
            nodes.append(ExecutionNode(
                id=nid("type_check"), description="Run type checker",
                node_type=NodeType.TOOL_CALL, tool_name="bash_exec",
                criticality=Criticality.IMPORTANT,
                estimated_steps=1,
            ))
            nodes.append(ExecutionNode(
                id=nid("run_tests"), description="Run test suite",
                node_type=NodeType.TOOL_CALL, tool_name="bash_exec",
                criticality=Criticality.IMPORTANT,
                estimated_steps=2,
            ))

        nodes.append(ExecutionNode(
            id=nid("synthesize"), description="Synthesize final result",
            node_type=NodeType.SYNTHESIS,
            criticality=Criticality.CRITICAL,
            estimated_steps=1,
        ))

        return nodes

    def _analyze_dependencies(self, plan: ExecutionPlan, task: str, context: Dict[str, Any]):
        """Analyze and set up dependency edges between nodes."""
        task_lower = task.lower()
        is_refactor = "refactor" in task_lower
        any(w in task_lower for w in ["security", "auth", "vulnerability"])

        nids = list(plan.nodes.keys())
        edges_added: Set[tuple] = set()

        def add_edge(src: str, tgt: str):
            key = (src, tgt)
            if key not in edges_added and src in plan.nodes and tgt in plan.nodes:
                plan.add_edge(ExecutionEdge(
                    id=f"e_{src}->{tgt}",
                    source_node_id=src,
                    target_node_id=tgt,
                ))
                edges_added.add(key)

        read_ids = [n for n in nids if "read" in n or "research" in n]
        analyze_ids = [n for n in nids if "analyze" in n or "diagnose" in n or "memory" in n]
        change_ids = [n for n in nids if "make_changes" in n or "fix" in n]
        caller_ids = [n for n in nids if "caller" in n]
        verify_ids = [n for n in nids if "verify" in n]
        security_ids = [n for n in nids if "security" in n]
        test_ids = [n for n in nids if "type_check" in n or "run_tests" in n]
        synth_ids = [n for n in nids if "synthesize" in n]
        respond_ids = [n for n in nids if "respond" in n]

        for aid in analyze_ids:
            for rid in read_ids:
                add_edge(rid, aid)
            for mid in [n for n in nids if "memory" in n]:
                add_edge(mid, aid)
                if rid := next(iter(read_ids), None):
                    add_edge(rid, mid)

        for cid in change_ids:
            for aid in analyze_ids:
                add_edge(aid, cid)
            if is_refactor:
                for rid in read_ids:
                    add_edge(rid, cid)

        for cid in caller_ids:
            for change in change_ids:
                add_edge(change, cid)

        for vid in verify_ids:
            for src in (caller_ids if caller_ids else change_ids):
                add_edge(src, vid)

        for sid in security_ids:
            for vid in verify_ids:
                add_edge(vid, sid)
            if not verify_ids:
                for src in (caller_ids if caller_ids else change_ids):
                    add_edge(src, sid)

        for sid in synth_ids:
            sources: List[str] = []
            if security_ids:
                sources.extend(security_ids)
            elif verify_ids:
                sources.extend(verify_ids)
            elif test_ids:
                sources.extend(test_ids)
            elif change_ids:
                sources.extend(change_ids)
            elif caller_ids:
                sources.extend(caller_ids)
            else:
                sources = [n for n in nids if n not in synth_ids]
            for src in sources:
                add_edge(src, sid)

        for tid in test_ids:
            for src in (change_ids if change_ids else verify_ids if verify_ids else synth_ids[:1] if synth_ids else read_ids):
                add_edge(src, tid)

        for rid in respond_ids:
            for src in (synth_ids if synth_ids else nids):
                if src != rid:
                    add_edge(src, rid)

    def _assign_criticality(self, plan: ExecutionPlan, task: str):
        for node in plan.nodes.values():
            if node.node_type == NodeType.SYNTHESIS:
                node.criticality = Criticality.CRITICAL
            elif node.node_type == NodeType.VERIFICATION:
                node.criticality = Criticality.IMPORTANT
            elif node.node_type == NodeType.MEMORY_QUERY:
                node.criticality = Criticality.OPTIONAL
        for nid in plan.nodes:
            deps = plan.get_dependent_ids(nid)
            if len(deps) > 3:
                plan.nodes[nid].criticality = Criticality.CRITICAL

    def _identify_parallelism(self, plan: ExecutionPlan):
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

    def _allocate_budget(self, plan: ExecutionPlan, context: Dict[str, Any]):
        total = context.get("budget", 30)
        plan.total_budget = total
        for node in plan.nodes.values():
            node.estimated_steps = max(1, node.estimated_steps)

    def _find_exit_nodes(self, plan: ExecutionPlan) -> List[str]:
        return [nid for nid in plan.nodes if not plan.get_dependent_ids(nid)]

    def _next_id(self, base: str) -> str:
        self._counter += 1
        return f"n_{self._counter:04d}_{base}"

    def _match_pattern(self, task: str) -> Optional[ExecutionPattern]:
        best_score = 0.0
        best_pattern = None
        task_sig = self._normalize_signature(task)
        for pattern in self._patterns:
            score = pattern.similarity_to(task_sig)
            if score > best_score and score > 0.6:
                best_score = score
                best_pattern = pattern
        return best_pattern

    def _normalize_signature(self, task: str) -> str:
        t = task.lower()
        for word in ["please", "can you", "could you", "i need", "i want"]:
            t = t.replace(word, "")
        return " ".join(t.split())

    def _apply_pattern(self, plan: ExecutionPlan, pattern: ExecutionPattern):
        log.info(f"Applying pattern {pattern.id} to plan {plan.id}")
        seq = pattern.node_type_sequence
        if seq:
            ordered = plan.topological_sort()
            plan.critical_path = list(dict.fromkeys(
                [n for n in ordered if plan.nodes[n].criticality == Criticality.CRITICAL]
                + seq[:len(ordered)]
            ))

    @classmethod
    def from_legacy_node(cls, node: LegacyNode, plan_id: str) -> ExecutionNode:
        return ExecutionNode(
            id=node.id,
            description=node.description,
            node_type=NodeType.TOOL_CALL,
            specialist=node.specialist,
            tool_name=node.tools[0] if node.tools else "",
            criticality=Criticality.IMPORTANT,
            estimated_steps=node.estimated_steps,
        )


def has_files(task_lower: str) -> bool:
    """Check if the task mentions specific files/modules."""
    return any(w in task_lower for w in ["file", "module", "class", "function", "component"])
