"""
calibration.py â€” Plan Calibration & Learning System for AELVO OMEGA

Tracks outcomes against plans to improve future planning. Records:
- Unplanned failures â†’ new failure modes for future analysis
- Incorrect risk assessments â†’ recalibrate probability estimates
- Inefficient verification â†’ improve verification strategy coverage
- Unnecessary specialist activations â†’ refine activation thresholds
- Incorrect dependency analysis â†’ expand dependency detection

All learning is persisted to disk via JSON file and loaded on restart.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

log = logging.getLogger("aelvo.plan.calibration")


# ===========================================================================
# Learning Types
# ===========================================================================


class DeviationType(str, Enum):
    """Types of plan-vs-reality deviations that generate learning."""
    UNPLANNED_FAILURE = "unplanned_failure"
    INCORRECT_RISK = "incorrect_risk"
    INEFFICIENT_VERIFICATION = "inefficient_verification"
    UNNECESSARY_SPECIALIST = "unnecessary_specialist"
    INCORRECT_DEPENDENCY = "incorrect_dependency"
    STRATEGY_MISMATCH = "strategy_mismatch"
    OBJECTIVE_MISALIGNMENT = "objective_misalignment"


class LearningEntry(BaseModel):
    """A single learning from a completed task."""
    id: str
    deviation_type: DeviationType
    description: str
    plan_prediction: str
    actual_outcome: str
    severity: float = Field(default=0.5, ge=0.0, le=1.0)
    applicable_task_types: List[str] = Field(default_factory=list)
    applicable_strategy_classes: List[str] = Field(default_factory=list)
    recommendation: str = ""
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    times_applied: int = 0
    effectiveness_score: float = 0.5


class PlanOutcome(BaseModel):
    """Recorded outcome of a plan execution."""
    plan_id: str
    objective: str
    task_type: str
    strategy_class: str
    planned_phases: int
    completed_phases: int
    planned_specialists: List[str]
    actual_specialists: List[str]
    planned_risks: int
    materialized_risks: int
    verification_checks_run: int
    verification_failures_caught: int
    verification_type_failures: Dict[str, int] = Field(
        default_factory=dict,
        description="Per-check-type failure counts (e.g. {'security_scan': 2, 'lint': 1})",
    )
    unplanned_failures: int
    total_duration_ms: float
    success: bool
    deviations: List[LearningEntry] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class CalibrationAdjustment(BaseModel):
    """An adjustment to future planning based on accumulated learning."""
    field: str
    old_value: Any
    new_value: Any
    reason: str
    confidence: float = 0.5
    learning_count: int = 1


# Default filename for calibration data in the runtime directory
CALIBRATION_DATA_FILENAME = "plan_calibration_data.json"


# ===========================================================================
# Plan Calibration System
# ===========================================================================


class PlanCalibrationSystem:
    """Tracks plan outcomes and generates learning to improve future plans.

    After every task completes, compare the actual execution against the
    planned execution. Where they deviate, record the deviation and
    analyze its cause. Future plans retrieve this learning and use it
    to produce better initial plans.

    All data is persisted to a JSON file for survival across restarts.
    """

    def __init__(self, storage_path: Optional[str] = None, save_interval: int = 1):
        """Initialize the calibration system.

        Args:
            storage_path: Directory path for JSON persistence. If None, no disk I/O.
            save_interval: Persist to disk once every N record_outcome() calls.
                           Higher values reduce I/O during bulk recording.
                           Default 1 (save after every outcome, backward compatible).
        """
        self._storage_path: Optional[Path] = Path(storage_path) / CALIBRATION_DATA_FILENAME if storage_path else None
        self._save_interval = max(1, save_interval)
        self._outcomes_since_save = 0
        self._outcomes: Dict[str, PlanOutcome] = {}
        self._learnings: Dict[str, LearningEntry] = {}
        self._adjustments: List[CalibrationAdjustment] = []
        self._task_type_stats: Dict[str, Dict[str, Any]] = {}
        self._strategy_stats: Dict[str, Dict[str, Any]] = {}

        # Load persisted data from disk
        self._load()

    def record_outcome(
        self,
        plan_id: str,
        objective: str,
        task_type: str,
        strategy_class: str,
        planned_phases: int,
        completed_phases: int,
        planned_specialists: List[str],
        actual_specialists: List[str],
        planned_risks: int,
        materialized_risks: int,
        verification_checks_run: int,
        verification_failures_caught: int,
        verification_type_failures: Optional[Dict[str, int]] = None,
        unplanned_failures: int = 0,
        total_duration_ms: float = 0.0,
        success: bool = True,
    ) -> PlanOutcome:
        """Record the actual outcome of a plan execution."""
        outcome = PlanOutcome(
            plan_id=plan_id,
            objective=objective,
            task_type=task_type,
            strategy_class=strategy_class,
            planned_phases=planned_phases,
            completed_phases=completed_phases,
            planned_specialists=planned_specialists,
            actual_specialists=actual_specialists,
            planned_risks=planned_risks,
            materialized_risks=materialized_risks,
            verification_checks_run=verification_checks_run,
            verification_failures_caught=verification_failures_caught,
            verification_type_failures=verification_type_failures or {},
            unplanned_failures=unplanned_failures,
            total_duration_ms=total_duration_ms,
            success=success,
        )
        self._outcomes[plan_id] = outcome
        log.info("Recorded outcome for plan %s: success=%s, deviations=%d",
                 plan_id[:12], success, 0)

        # Analyze deviations
        deviations = self._analyze_deviations(outcome)
        outcome.deviations = deviations

        # Record learnings
        for deviation in deviations:
            self._learnings[deviation.id] = deviation
            log.info("Learning recorded: %s â€” %s", deviation.deviation_type.value, deviation.description[:60])

        # Update statistics
        self._update_stats(outcome, deviations)

        # Persist to disk (batched by save_interval to avoid O(nÂ²) I/O)
        self._outcomes_since_save += 1
        if self._outcomes_since_save >= self._save_interval:
            self._save()
            self._outcomes_since_save = 0

        return outcome

    def _analyze_deviations(self, outcome: PlanOutcome) -> List[LearningEntry]:
        """Analyze deviations between plan and reality."""
        deviations: List[LearningEntry] = []

        # 1. Unplanned failures
        if outcome.unplanned_failures > 0:
            deviations.append(LearningEntry(
                id=self._generate_id("deviation", f"{outcome.plan_id}_unplanned"),
                deviation_type=DeviationType.UNPLANNED_FAILURE,
                description=f"{outcome.unplanned_failures} failures occurred that were not in the recovery plan",
                plan_prediction="All failures would be anticipated and covered by recovery strategies",
                actual_outcome=f"{outcome.unplanned_failures} failures were not anticipated",
                severity=min(1.0, outcome.unplanned_failures * 0.2),
                applicable_task_types=[outcome.task_type],
                applicable_strategy_classes=[outcome.strategy_class],
                recommendation="Add new failure modes to the recovery design for similar tasks",
                confidence=0.7,
            ))

        # 2. Risk assessment accuracy
        if outcome.planned_risks > 0:
            risk_accuracy = 1.0 - abs(outcome.planned_risks - outcome.materialized_risks) / max(outcome.planned_risks, 1)
            if risk_accuracy < 0.5:
                overestimated = outcome.planned_risks > outcome.materialized_risks
                deviations.append(LearningEntry(
                    id=self._generate_id("deviation", f"{outcome.plan_id}_risk"),
                    deviation_type=DeviationType.INCORRECT_RISK,
                    description=f"Risk assessment was {'overestimated' if overestimated else 'underestimated'}: "
                                f"planned {outcome.planned_risks}, materialized {outcome.materialized_risks}",
                    plan_prediction=f"{outcome.planned_risks} risks identified",
                    actual_outcome=f"{outcome.materialized_risks} risks materialized",
                    severity=1.0 - risk_accuracy,
                    applicable_task_types=[outcome.task_type],
                    recommendation="Recalibrate risk probability estimates for this task type",
                    confidence=0.6,
                ))

        # 3. Verification efficiency â€” per-check-type failure analysis
        if outcome.verification_type_failures:
            # Generate specific learnings for each failing check type
            failing_types = {vt: count for vt, count in outcome.verification_type_failures.items() if count > 0}
            if failing_types:
                for vt_name, fail_count in failing_types.items():
                    deviations.append(LearningEntry(
                        id=self._generate_id("deviation", f"{outcome.plan_id}_vtype_{vt_name}"),
                        deviation_type=DeviationType.INEFFICIENT_VERIFICATION,
                        description=f"Check type '{vt_name}' failed {fail_count} time(s) for {outcome.task_type} task",
                        plan_prediction=f"'{vt_name}' checks would pass",
                        actual_outcome=f"'{vt_name}' failed {fail_count} time(s)",
                        severity=min(0.9, 0.3 * fail_count),
                        applicable_task_types=[outcome.task_type],
                        applicable_strategy_classes=[outcome.strategy_class],
                        recommendation=f"Strengthen '{vt_name}' check design or adjust verification scope for {outcome.task_type} tasks",
                        confidence=0.6,
                    ))

        if outcome.verification_checks_run > 0 and outcome.verification_failures_caught == 0:
            # All verification passed on first try â€” might be over-verifying
            if outcome.verification_checks_run > 5:
                deviations.append(LearningEntry(
                    id=self._generate_id("deviation", f"{outcome.plan_id}_verify"),
                    deviation_type=DeviationType.INEFFICIENT_VERIFICATION,
                    description=f"{outcome.verification_checks_run} verification checks ran but none caught failures",
                    plan_prediction="Verification checks would catch failures",
                    actual_outcome="No failures detected by verification",
                    severity=0.2,
                    applicable_task_types=[outcome.task_type],
                    recommendation="Consider reducing verification layer depth for low-risk tasks",
                    confidence=0.4,
                ))
        elif outcome.verification_failures_caught > 0 and outcome.verification_failures_caught < outcome.verification_checks_run:
            # Some checks caught failures â€” good calibration
            pass
        elif outcome.verification_failures_caught == 0 and outcome.unplanned_failures > 0:
            # Failures occurred but verification didn't catch them
            deviations.append(LearningEntry(
                id=self._generate_id("deviation", f"{outcome.plan_id}_verify_miss"),
                deviation_type=DeviationType.INEFFICIENT_VERIFICATION,
                description=f"Verification missed {outcome.unplanned_failures} failures that occurred",
                plan_prediction="Verification would catch all failures",
                actual_outcome=f"{outcome.unplanned_failures} failures were not caught",
                severity=0.8,
                applicable_task_types=[outcome.task_type],
                recommendation="Add new verification types to cover the missed failure modes",
                confidence=0.8,
            ))

        # 4. Specialist efficiency
        unused = set(outcome.planned_specialists) - set(outcome.actual_specialists)
        if unused:
            deviations.append(LearningEntry(
                id=self._generate_id("deviation", f"{outcome.plan_id}_specialist"),
                deviation_type=DeviationType.UNNECESSARY_SPECIALIST,
                description=f"Specialists activated but not needed: {', '.join(unused)}",
                plan_prediction=f"Specialists needed: {', '.join(outcome.planned_specialists)}",
                actual_outcome=f"Specialists actually used: {', '.join(outcome.actual_specialists)}",
                severity=0.1 * len(unused),
                applicable_task_types=[outcome.task_type],
                recommendation=f"Refine activation thresholds for: {', '.join(unused)}",
                confidence=0.5,
            ))

        # 5. Phase completion accuracy
        if outcome.completed_phases < outcome.planned_phases:
            completion_rate = outcome.completed_phases / max(outcome.planned_phases, 1)
            if completion_rate < 0.8:
                deviations.append(LearningEntry(
                    id=self._generate_id("deviation", f"{outcome.plan_id}_phase"),
                    deviation_type=DeviationType.STRATEGY_MISMATCH,
                    description=f"Only {outcome.completed_phases}/{outcome.planned_phases} phases completed",
                    plan_prediction=f"All {outcome.planned_phases} phases would complete",
                    actual_outcome=f"Only {outcome.completed_phases} completed",
                    severity=1.0 - completion_rate,
                    applicable_task_types=[outcome.task_type],
                    applicable_strategy_classes=[outcome.strategy_class],
                    recommendation="Consider simpler strategies for this task type",
                    confidence=0.7,
                ))

        return deviations

    def _update_stats(self, outcome: PlanOutcome, deviations: List[LearningEntry]) -> None:
        """Update cumulative statistics for task types and strategies."""
        # Task type stats
        tt = outcome.task_type
        if tt not in self._task_type_stats:
            self._task_type_stats[tt] = {
                "count": 0, "successes": 0, "total_phases": 0,
                "total_unplanned": 0, "total_deviations": 0,
                "verification_stats": {},  # per-check-type failure counts
            }
        stats = self._task_type_stats[tt]
        stats["count"] += 1
        stats["successes"] += 1 if outcome.success else 0
        stats["total_phases"] += outcome.planned_phases
        stats["total_unplanned"] += outcome.unplanned_failures
        stats["total_deviations"] += len(deviations)

        # Track verification failure patterns by check type
        vstats = stats.setdefault("verification_stats", {})
        for vtype_name, fail_count in outcome.verification_type_failures.items():
            if vtype_name not in vstats:
                vstats[vtype_name] = {"total_runs": 0, "total_failures": 0, "last_fail_count": 0}
            vstats[vtype_name]["total_runs"] += 1
            vstats[vtype_name]["total_failures"] += fail_count
            vstats[vtype_name]["last_fail_count"] = fail_count

        # Strategy stats
        sc = outcome.strategy_class
        if sc not in self._strategy_stats:
            self._strategy_stats[sc] = {
                "count": 0, "successes": 0, "avg_phases": 0.0,
                "avg_unplanned": 0.0,
            }
        ss = self._strategy_stats[sc]
        ss["count"] += 1
        ss["successes"] += 1 if outcome.success else 0
        ss["avg_phases"] = (
            ss["avg_phases"] * (ss["count"] - 1) + outcome.planned_phases
        ) / ss["count"]
        ss["avg_unplanned"] = (
            ss["avg_unplanned"] * (ss["count"] - 1) + outcome.unplanned_failures
        ) / ss["count"]

    def get_adjustments_for_task(
        self,
        task_type: str,
        strategy_class: str,
    ) -> List[CalibrationAdjustment]:
        """Get calibration adjustments applicable to a new task."""
        adjustments: List[CalibrationAdjustment] = []

        # Apply learnings from similar task types
        for learning in self._learnings.values():
            if task_type in learning.applicable_task_types:
                if learning.deviation_type == DeviationType.UNNECESSARY_SPECIALIST:
                    adjustments.append(CalibrationAdjustment(
                        field="specialist_activation",
                        old_value="default_threshold",
                        new_value="adjusted_threshold",
                        reason=learning.recommendation,
                        confidence=learning.confidence,
                        learning_count=learning.times_applied + 1,
                    ))
                elif learning.deviation_type == DeviationType.INEFFICIENT_VERIFICATION:
                    adjustments.append(CalibrationAdjustment(
                        field="verification_depth",
                        old_value="standard",
                        new_value="adjusted",
                        reason=learning.recommendation,
                        confidence=learning.confidence,
                    ))

        return adjustments

    def get_task_type_success_rate(self, task_type: str) -> float:
        """Get historical success rate for a task type."""
        stats = self._task_type_stats.get(task_type)
        if not stats or stats["count"] == 0:
            return 0.5  # default
        return stats["successes"] / stats["count"]

    def get_strategy_effectiveness(self, strategy_class: str) -> Dict[str, Any]:
        """Get effectiveness metrics for a strategy class."""
        return self._strategy_stats.get(strategy_class, {})

    def get_recent_learnings(self, limit: int = 10) -> List[LearningEntry]:
        """Get the most recent learnings."""
        sorted_learnings = sorted(
            self._learnings.values(),
            key=lambda l: l.created_at,
            reverse=True,
        )
        return sorted_learnings[:limit]

    def get_calibration_summary(self) -> Dict[str, Any]:
        """Get a summary of the calibration system's state."""
        total_outcomes = len(self._outcomes)
        success_rate = (
            sum(1 for o in self._outcomes.values() if o.success) / max(total_outcomes, 1)
        )
        total_learnings = len(self._learnings)
        by_type = {}
        for l in self._learnings.values():
            by_type[l.deviation_type.value] = by_type.get(l.deviation_type.value, 0) + 1

        return {
            "total_outcomes_recorded": total_outcomes,
            "overall_success_rate": round(success_rate, 4),
            "total_learnings": total_learnings,
            "learnings_by_type": by_type,
            "task_types_observed": len(self._task_type_stats),
            "strategies_observed": len(self._strategy_stats),
        }

    def snapshot(self) -> Dict[str, Any]:
        """Quick snapshot for monitoring."""
        return self.get_calibration_summary()

    # ==================================================================
    # Persistence (survive restarts)
    # ==================================================================

    def _save(self) -> None:
        """Persist calibration data to disk as JSON."""
        if self._storage_path is None:
            return

        try:
            self._storage_path.parent.mkdir(parents=True, exist_ok=True)

            data = {
                "outcomes": {
                    pid: outcome.model_dump(mode="json")
                    for pid, outcome in self._outcomes.items()
                },
                "learnings": {
                    lid: learning.model_dump(mode="json")
                    for lid, learning in self._learnings.items()
                },
                "task_type_stats": self._task_type_stats,
                "strategy_stats": self._strategy_stats,
            }

            self._storage_path.write_text(
                json.dumps(data, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception as e:
            log.error("Failed to save calibration data: %s", e)

    def _load(self) -> None:
        """Load calibration data from disk."""
        if self._storage_path is None or not self._storage_path.exists():
            return

        try:
            data = json.loads(
                self._storage_path.read_text(encoding="utf-8")
            )

            # Restore outcomes â€” Pydantic handles nested model reconstruction automatically
            outcomes_data = data.get("outcomes", {})
            for pid, outcome_dict in outcomes_data.items():
                self._outcomes[pid] = PlanOutcome(**outcome_dict)

            # Restore learnings
            learnings_data = data.get("learnings", {})
            for lid, learning_dict in learnings_data.items():
                self._learnings[lid] = LearningEntry(**learning_dict)

            # Restore stats
            self._task_type_stats = data.get("task_type_stats", {})
            self._strategy_stats = data.get("strategy_stats", {})

            log.info(
                "Loaded calibration data: %d outcomes, %d learnings, "
                "%d task types, %d strategies",
                len(self._outcomes), len(self._learnings),
                len(self._task_type_stats), len(self._strategy_stats),
            )
        except Exception as e:
            log.error("Failed to load calibration data: %s", e)
            # Reset to clean state on corrupt data
            self._outcomes.clear()
            self._learnings.clear()
            self._task_type_stats.clear()
            self._strategy_stats.clear()

    def flush(self) -> None:
        """Force an immediate save to disk. Useful before shutdown or tests."""
        if self._outcomes_since_save > 0:
            self._save()
            self._outcomes_since_save = 0

    def clear_persistence(self) -> None:
        """Delete the persisted calibration data file."""
        if self._storage_path and self._storage_path.exists():
            try:
                self._storage_path.unlink()
                log.info("Cleared calibration data file: %s", self._storage_path)
            except Exception as e:
                log.warning("Failed to clear calibration data: %s", e)

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{datetime.now(timezone.utc).isoformat()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
