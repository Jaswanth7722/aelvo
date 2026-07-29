# cognition/autonomous_learning.py — Autonomous Learning Pipeline
#
# Phase 9: Refactored CognitiveEngine for Autonomous Learning & Strategy Memory
#
# The AutonomousLearningPipeline runs after every execution to:
# 1. Extract learnings from execution outcomes automatically
# 2. Reinforce successful strategies (boost importance)
# 3. Flag and penalize failed strategies (reduce importance)
# 4. Auto-consolidate similar memory entries
# 5. Decay stale entries to prevent memory bloat
# 6. Feed relevant strategies into planning via find_relevant_strategies()

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from cognition.types import MemoryType, StrategicMemoryEntry

log = logging.getLogger("aelvo.cognition.autonomous_learning")

# How often to run decay/consolidation passes (turns)
_DECAY_INTERVAL_TURNS = 10
_CONSOLIDATION_INTERVAL_TURNS = 5


class AutonomousLearningPipeline:
    """Autonomous learning pipeline that runs after every execution.

    No explicit calls required — the pipeline is triggered automatically
    by the CognitiveEngine after each execution completes.

    Phases:
    1. Outcome Learning — extract learnings from execution results
    2. Strategy Reinforcement — boost importance of successful strategies
    3. Failure Flagging — reduce importance of failed strategies
    4. Periodic Maintenance — decay stale entries, consolidate similar ones
    """

    def __init__(self, strategic_memory):
        self._strategic_memory = strategic_memory
        self._turn_count: int = 0
        self._total_learnings_stored: int = 0
        self._total_reinforcements: int = 0
        self._total_failures_flagged: int = 0

    # ======================================================================
    # Main Pipeline Entry Point
    # ======================================================================

    def process_execution_outcome(
        self,
        goal_description: str,
        outcome: str,
        specialist: str = "",
        execution_summary: str = "",
        successful_strategy_ids: Optional[List[str]] = None,
        failed_strategy_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Process an execution outcome through the full learning pipeline.

        Called by CognitiveEngine after every execution completes.
        Runs all four phases autonomously.

        Args:
            goal_description: What the goal/task was.
            outcome: ``"success"`` or ``"failure"``.
            specialist: Which specialist handled the execution.
            execution_summary: Brief summary of what happened.
            successful_strategy_ids: IDs of strategies that contributed to success.
            failed_strategy_ids: IDs of strategies that contributed to failure.

        Returns:
            Dict with learning results (stored_count, reinforced_count, etc.).
        """
        self._turn_count += 1
        results = {
            "stored": 0,
            "reinforced": 0,
            "flagged": 0,
            "decayed": 0,
            "consolidated": 0,
        }

        # Phase 1: Outcome Learning
        stored = self._phase_outcome_learning(
            goal_description, outcome, specialist, execution_summary,
        )
        results["stored"] = 1 if stored else 0

        # Phase 2: Strategy Reinforcement
        reinforced = self._phase_strategy_reinforcement(
            successful_strategy_ids or [],
        )
        results["reinforced"] = reinforced

        # Phase 3: Failure Flagging
        flagged = self._phase_failure_flagging(
            failed_strategy_ids or [],
        )
        results["flagged"] = flagged

        # Phase 4: Periodic Maintenance
        if self._turn_count % _DECAY_INTERVAL_TURNS == 0:
            results["decayed"] = self._strategic_memory.decay_stale_entries()

        if self._turn_count % _CONSOLIDATION_INTERVAL_TURNS == 0:
            results["consolidated"] = self._strategic_memory.consolidate_similar_entries()

        log.info(
            "Learning pipeline: stored=%d, reinforced=%d, flagged=%d, "
            "decayed=%d, consolidated=%d",
            results["stored"], results["reinforced"], results["flagged"],
            results["decayed"], results["consolidated"],
        )

        return results

    # ======================================================================
    # Phase 1: Outcome Learning
    # ======================================================================

    def _phase_outcome_learning(
        self,
        goal_description: str,
        outcome: str,
        specialist: str = "",
        execution_summary: str = "",
    ) -> Optional[StrategicMemoryEntry]:
        """Extract and store a learning from an execution outcome.

        Delegates to StrategicMemory.auto_store_from_outcome().
        """
        if not goal_description:
            return None

        entry = self._strategic_memory.auto_store_from_outcome(
            goal_description=goal_description,
            outcome=outcome,
            specialist=specialist,
            execution_summary=execution_summary,
        )
        if entry:
            self._total_learnings_stored += 1
        return entry

    # ======================================================================
    # Phase 2: Strategy Reinforcement
    # ======================================================================

    def _phase_strategy_reinforcement(
        self,
        strategy_ids: List[str],
    ) -> int:
        """Boost importance of strategies that contributed to success."""
        count = 0
        for sid in strategy_ids:
            if self._strategic_memory.boost(sid, amount=0.08):
                count += 1
                log.debug("Reinforced strategy %s", sid[:12])
        self._total_reinforcements += count
        return count

    # ======================================================================
    # Phase 3: Failure Flagging
    # ======================================================================

    def _phase_failure_flagging(
        self,
        strategy_ids: List[str],
    ) -> int:
        """Reduce importance of strategies that contributed to failure."""
        count = 0
        for sid in strategy_ids:
            if self._strategic_memory.decay(sid, amount=0.06):
                count += 1
                log.debug("Flagged failed strategy %s", sid[:12])
        self._total_failures_flagged += count
        return count

    # ======================================================================
    # Strategy Injection for Planning
    # ======================================================================

    def get_strategies_for_planning(
        self,
        goal_description: str,
        max_results: int = 5,
    ) -> List[StrategicMemoryEntry]:
        """Get relevant strategies for injection into a new plan.

        Designed to be called by CognitiveEngine.plan_goal() to enrich
        plans with prior knowledge.
        """
        return self._strategic_memory.find_relevant_strategies(
            goal_description=goal_description,
            max_results=max_results,
        )

    # ======================================================================
    # Status / Metrics
    # ======================================================================

    def snapshot(self) -> Dict[str, Any]:
        """Get learning pipeline metrics."""
        return {
            "turn_count": self._turn_count,
            "total_learnings_stored": self._total_learnings_stored,
            "total_reinforcements": self._total_reinforcements,
            "total_failures_flagged": self._total_failures_flagged,
        }
