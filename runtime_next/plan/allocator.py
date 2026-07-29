from __future__ import annotations
import logging
from typing import Dict, List, Set

from ..models.plan import ExecutionPlan, Criticality, NodeState

log = logging.getLogger("aelvo.plan.allocator")


class BudgetEnvelope:
    def __init__(self, section_id: str, allocated: int, consumed: int = 0):
        self.section_id = section_id
        self.allocated = allocated
        self.consumed = consumed

    @property
    def remaining(self) -> int:
        return self.allocated - self.consumed

    @property
    def exhausted(self) -> bool:
        return self.remaining <= 0


class SubBudgetAllocator:
    """Distributes total step budget across execution plan sections.

    Three priorities:
    1. Guarantee critical path completion
    2. Allocate important branch budgets
    3. Allocate optional enrichment budget
    """

    def __init__(self, plan: ExecutionPlan):
        self.plan = plan
        self.envelopes: Dict[str, BudgetEnvelope] = {}
        self._critical_path_set: Set[str] = set(plan.critical_path)

    def allocate(self) -> Dict[str, BudgetEnvelope]:
        total_budget = self.plan.total_budget
        # 1. Critical path minimum
        critical_min = sum(
            self.plan.nodes[nid].estimated_steps
            for nid in self.plan.critical_path
            if nid in self.plan.nodes
        )
        # Reserve with 20% buffer for retries
        critical_budget = int(critical_min * 1.2)
        if critical_budget > total_budget:
            log.warning(f"Plan {self.plan.id}: critical path min ({critical_min}) exceeds total budget ({total_budget})")
            critical_budget = total_budget

        remaining = total_budget - critical_budget
        env = BudgetEnvelope("critical_path", critical_budget)
        self.envelopes["critical_path"] = env

        # 2. Important branches
        important_nodes = [
            nid for nid, node in self.plan.nodes.items()
            if node.criticality == Criticality.IMPORTANT
            and nid not in self._critical_path_set
        ]
        important_min = sum(self.plan.nodes[nid].estimated_steps for nid in important_nodes)
        important_budget = min(int(important_min * 1.1), remaining)
        remaining -= important_budget
        self.envelopes["important"] = BudgetEnvelope("important", important_budget)

        # 3. Optional enrichment
        optional_nodes = [
            nid for nid, node in self.plan.nodes.items()
            if node.criticality == Criticality.OPTIONAL
            and nid not in self._critical_path_set
        ]
        optional_min = sum(self.plan.nodes[nid].estimated_steps for nid in optional_nodes)
        optional_budget = min(optional_min, remaining)
        self.envelopes["optional"] = BudgetEnvelope("optional", optional_budget)

        # Log what was skipped
        if optional_budget < optional_min:
            skipped = optional_min - optional_budget
            log.info(f"Budget constraint: skipping ~{skipped} steps of optional enrichment")

        return self.envelopes

    def consume(self, section: str, steps: int) -> bool:
        env = self.envelopes.get(section)
        if not env:
            return True
        env.consumed += steps
        if env.exhausted:
            log.warning(f"Section {section} budget exhausted ({env.allocated}/{env.consumed})")
            return False
        return True

    def can_dispatch(self, node_id: str) -> bool:
        node = self.plan.nodes.get(node_id)
        if not node:
            return True

        if node.criticality == Criticality.CRITICAL:
            env = self.envelopes.get("critical_path")
            return env is None or not env.exhausted
        elif node.criticality == Criticality.IMPORTANT:
            if node_id in self._critical_path_set:
                env = self.envelopes.get("critical_path")
                return env is None or not env.exhausted
            env = self.envelopes.get("important")
            if env is None or env.exhausted:
                return False
            if node.estimated_steps > env.remaining:
                return False
            return True
        else:
            env = self.envelopes.get("optional")
            if env is None or env.exhausted:
                return False
            if node.estimated_steps > env.remaining:
                return False
            return True

    def skip_optional_budget(self) -> int:
        env = self.envelopes.get("optional")
        if env:
            skipped = env.allocated - env.consumed
            env.consumed = env.allocated
            return max(0, skipped)
        return 0
