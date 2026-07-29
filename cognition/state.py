from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Set, Tuple

from cognition.types import (
    Goal, SubGoal, GoalStatus, BlockedPath, UncertaintyModel, UncertaintyClass,
    ExecutionHypothesis, HypothesisStatus, CognitiveStateSnapshot,
)

log = logging.getLogger("aelvo.cognition.state")


class CognitiveStateEngine:
    """Tracks the cognitive state of the system.

    Manages goals (top-level and decomposed sub-goals), uncertainty model,
    execution hypotheses, and blocked paths. Provides invariant checking
    and terminal display.
    """

    def __init__(self):
        self._goals: Dict[str, Goal] = {}
        self._sub_goals: Dict[str, SubGoal] = {}
        self._blocked_paths: Dict[str, BlockedPath] = {}
        self._uncertainty: UncertaintyModel = UncertaintyModel()
        self._hypotheses: Dict[str, ExecutionHypothesis] = {}
        self._goal_order: List[str] = []

    def register_goal(self, goal: Goal) -> None:
        self._goals[goal.id] = goal
        if goal.id not in self._goal_order:
            self._goal_order.append(goal.id)
        log.info("Registered goal %s: %s", goal.id, goal.description[:60])

    def get_goal(self, goal_id: str) -> Optional[Goal]:
        return self._goals.get(goal_id)

    def update_goal_status(self, goal_id: str, status: GoalStatus) -> bool:
        goal = self._goals.get(goal_id)
        if goal is None:
            return False
        goal.status = status
        goal.updated_at = datetime.now(timezone.utc)
        return True

    def get_active_goals(self) -> List[Goal]:
        return [g for g in self._goals.values() if g.status == GoalStatus.IN_PROGRESS]

    def get_pending_goals(self) -> List[Goal]:
        return [g for g in self._goals.values() if g.status == GoalStatus.PENDING]

    def get_blocked_goals(self) -> List[Goal]:
        return [g for g in self._goals.values() if g.status == GoalStatus.BLOCKED]

    def get_completed_goal_ids(self) -> List[str]:
        return [g.id for g in self._goals.values() if g.status == GoalStatus.COMPLETED]

    def register_sub_goal(self, sub_goal: SubGoal) -> None:
        self._sub_goals[sub_goal.id] = sub_goal
        parent = self._goals.get(sub_goal.parent_goal_id)
        if parent is not None:
            if sub_goal.id not in parent.sub_goal_ids:
                parent.sub_goal_ids.append(sub_goal.id)

    def get_sub_goals(self, parent_goal_id: str) -> List[SubGoal]:
        return [sg for sg in self._sub_goals.values() if sg.parent_goal_id == parent_goal_id]

    def update_sub_goal_status(self, sub_goal_id: str, status: GoalStatus) -> bool:
        sg = self._sub_goals.get(sub_goal_id)
        if sg is None:
            return False
        sg.status = status
        return True

    def add_blocked_path(self, blocked: BlockedPath) -> None:
        self._blocked_paths[blocked.id] = blocked
        log.info("Blocked path: %s â€” %s", blocked.step_id, blocked.reason[:60])

    def resolve_blocked_path(self, path_id: str) -> bool:
        bp = self._blocked_paths.get(path_id)
        if bp is None:
            return False
        bp.resolved = True
        bp.resolved_at = datetime.now(timezone.utc)
        return True

    def get_active_blocked_paths(self) -> List[BlockedPath]:
        return [bp for bp in self._blocked_paths.values() if not bp.resolved]

    def register_uncertainty(self, area: str, uc: UncertaintyClass) -> None:
        self._uncertainty.register_uncertainty(area, uc)

    def resolve_uncertainty(self, area: str, uc: UncertaintyClass) -> None:
        self._uncertainty.resolve_uncertainty(area, uc)

    def get_uncertainty_summary(self) -> Dict[str, List[str]]:
        result = {}
        for area, classes in self._uncertainty.uncertain_areas.items():
            result[area] = [c.value for c in classes]
        return result

    def add_hypothesis(self, hypothesis: ExecutionHypothesis) -> None:
        self._hypotheses[hypothesis.id] = hypothesis

    def update_hypothesis_status(self, hyp_id: str, status: HypothesisStatus) -> bool:
        h = self._hypotheses.get(hyp_id)
        if h is None:
            return False
        h.status = status
        return True

    def get_active_hypotheses(self) -> List[ExecutionHypothesis]:
        return [h for h in self._hypotheses.values() if h.status in (HypothesisStatus.PROPOSED, HypothesisStatus.INVESTIGATING)]

    def snapshot(self) -> CognitiveStateSnapshot:
        return CognitiveStateSnapshot(
            id=self._generate_snapshot_id(),
            active_goals=self.get_active_goals(),
            completed_goals=self.get_completed_goal_ids(),
            blocked_paths=self.get_active_blocked_paths(),
            uncertainty_model=self._uncertainty,
            execution_hypotheses=list(self._hypotheses.values()),
            blackboard_slot_count=0,
            memory_entries_count=0,
            consensus_events_count=0,
            research_hypotheses_count=0,
        )

    def to_terminal_display(self) -> str:
        lines = [
            "â•”â•â• COGNITIVE STATE â•â•â•—",
            f"  Goals: {len(self._goals)} ({len(self.get_active_goals())} active, "
            f"{len(self.get_blocked_goals())} blocked, {len(self.get_completed_goal_ids())} completed)",
        ]
        if self.get_active_goals():
            lines.append("")
            lines.append("  ACTIVE GOALS:")
            for g in self.get_active_goals():
                lines.append(f"    [{g.id[:8]}] {g.description[:60]} (P{g.priority})")
        if self.get_active_blocked_paths():
            lines.append("")
            lines.append("  BLOCKED PATHS:")
            for bp in self.get_active_blocked_paths():
                lines.append(f"    [{bp.id[:8]}] {bp.reason[:60]}")
        if self._uncertainty.uncertain_areas:
            lines.append("")
            lines.append("  UNCERTAINTIES:")
            for area, classes in self._uncertainty.uncertain_areas.items():
                cls_str = ", ".join(c.value for c in classes)
                lines.append(f"    {area}: {cls_str}")
        if self.get_active_hypotheses():
            lines.append("")
            lines.append("  HYPOTHESES:")
            for h in self.get_active_hypotheses():
                lines.append(f"    [{h.id[:8]}] {h.description[:55]} (c={h.confidence:.2f})")
        lines.append(f"â•šâ•â• {'â•' * 18}â•â•â•")
        return "\n".join(lines)

    def check_invariants(self) -> List[str]:
        violations: List[str] = []
        active_goal_ids = {g.id for g in self.get_active_goals()}
        for sg in self._sub_goals.values():
            if sg.status == GoalStatus.IN_PROGRESS:
                if sg.parent_goal_id not in active_goal_ids:
                    violations.append(
                        f"Sub-goal {sg.id} is IN_PROGRESS but parent {sg.parent_goal_id} is not active"
                    )
        for bp in self.get_active_blocked_paths():
            related_goal = self._find_goal_for_step(bp.step_id)
            if related_goal and related_goal.status == GoalStatus.COMPLETED:
                violations.append(
                    f"Blocked path {bp.id} exists for completed goal {related_goal.id}"
                )
        return violations

    def _generate_snapshot_id(self) -> str:
        return hashlib.sha256(f"snapshot_{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:16]

    def _find_goal_for_step(self, step_id: str) -> Optional[Goal]:
        for goal in self._goals.values():
            if step_id in goal.sub_goal_ids:
                return goal
        for sg in self._sub_goals.values():
            if sg.id == step_id:
                return self._goals.get(sg.parent_goal_id)
        return None
