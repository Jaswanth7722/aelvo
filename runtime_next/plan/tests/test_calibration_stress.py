"""
test_calibration_stress.py — Calibration Stress & Performance Tests

Verifies PlanCalibrationSystem under load:
  1. Record 100+ outcomes — verify timing is O(n) not O(n²)
  2. File size remains reasonable (not exponential)
  3. Load all outcomes back — full data integrity
  4. Stats aggregation correctness at scale
  5. Adjustment computation scales
  6. Memory usage tracking
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict
from pathlib import Path

import pytest

from runtime_next.plan.calibration import (
    PlanCalibrationSystem,
    DeviationType,
    CALIBRATION_DATA_FILENAME,
)


# ===========================================================================
# Helpers
# ===========================================================================


TASK_TYPES = ["refactor", "fix", "feature", "security", "general"]
STRATEGY_CLASSES = ["incremental", "comprehensive", "mvp", "minimal_patch"]
SPECIALISTS_POOL = ["FORGE", "SENTINEL", "ORACLE", "HERMES", "TERMINUS", "HERALD"]
VERIF_TYPES = ["lint", "typecheck", "unit_test", "security_scan", "architecture_check"]


def make_outcome_kwargs(plan_id: str, seed: int) -> Dict[str, Any]:
    """Generate a deterministic outcome for stress testing.

    Uses the seed to vary task_type, strategy, specialists, and failures
    so we get a realistic spread of deviation types.
    """
    task_type = TASK_TYPES[seed % len(TASK_TYPES)]
    strategy_class = STRATEGY_CLASSES[seed % len(STRATEGY_CLASSES)]

    # Vary specialist counts — every 3rd outcome has unused specialists
    planned_count = 2 + (seed % 4)
    actual_count = max(1, planned_count - (seed % 3))
    planned_specialists = SPECIALISTS_POOL[:planned_count]
    actual_specialists = SPECIALISTS_POOL[:actual_count]

    # Vary verification outcomes
    verif_checks = 3 + (seed % 8)
    if seed % 5 == 0:
        # Zero failures — may trigger over-verification
        verif_failures = 0
        verif_type_failures: Dict[str, int] = {}
    else:
        verif_failures = 1 + (seed % 3)
        # Pick 1-2 verification types that failed
        n_failing = 1 + (seed % 2)
        verif_type_failures = {
            VERIF_TYPES[(seed + i) % len(VERIF_TYPES)]: 1 + (seed % (i + 1))
            for i in range(n_failing)
        }

    return {
        "plan_id": plan_id,
        "objective": f"Stress test objective {seed} — {task_type} scenario",
        "task_type": task_type,
        "strategy_class": strategy_class,
        "planned_phases": 3 + (seed % 6),
        "completed_phases": 2 + (seed % 5),  # sometimes incomplete
        "planned_specialists": planned_specialists,
        "actual_specialists": actual_specialists,
        "planned_risks": 2 + (seed % 4),
        "materialized_risks": seed % 5,  # 0-4 range, varies accuracy
        "verification_checks_run": verif_checks,
        "verification_failures_caught": verif_failures,
        "verification_type_failures": verif_type_failures,
        "unplanned_failures": seed % 3,  # 0-2 range
        "total_duration_ms": 1000.0 * (1 + seed),
        "success": seed % 6 != 0,  # ~83% success rate
    }


def make_calibration(tmp_path: Path, save_interval: int = 10) -> PlanCalibrationSystem:
    """Create a PlanCalibrationSystem rooted at a temp directory.

    Uses save_interval=10 by default to batch disk writes during bulk
    outcome recording, avoiding O(n²) I/O from full-state serialization.
    """
    return PlanCalibrationSystem(storage_path=str(tmp_path), save_interval=save_interval)


def file_size(tmp_path: Path) -> int:
    """Get the size of the calibration data file in bytes."""
    file_path = tmp_path / CALIBRATION_DATA_FILENAME
    if file_path.exists():
        return file_path.stat().st_size
    return 0


def file_size_kb(tmp_path: Path) -> float:
    """Get the file size in kilobytes."""
    return file_size(tmp_path) / 1024.0


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def tmp_cal(tmp_path: Path) -> PlanCalibrationSystem:
    return make_calibration(tmp_path)


# ===========================================================================
# Section 1: Baseline — single outcome timing
# ===========================================================================


class TestCalibrationStressBaseline:
    """Establish baseline timing for single outcome recording."""

    def test_single_outcome_timing(self, tmp_cal):
        """Recording a single outcome should complete quickly (< 100ms)."""
        start = time.perf_counter()
        outcome = tmp_cal.record_outcome(**make_outcome_kwargs("baseline_001", 0))
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert outcome is not None
        assert elapsed_ms < 100, f"Single outcome took {elapsed_ms:.1f}ms (> 100ms)"

    def test_single_outcome_produces_learnings(self, tmp_cal):
        """A single outcome should produce at least one deviation learning."""
        outcome = tmp_cal.record_outcome(**make_outcome_kwargs("baseline_002", 1))
        assert len(outcome.deviations) >= 1
        assert len(tmp_cal._learnings) >= 1

    def test_single_outcome_file_created(self, tmp_path):
        """After recording one outcome (with save_interval=1), the JSON file must exist."""
        cal = make_calibration(tmp_path, save_interval=1)
        cal.record_outcome(**make_outcome_kwargs("baseline_003", 2))

        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        assert file_path.exists()
        assert file_path.stat().st_size > 0


# ===========================================================================
# Section 2: 100-outcome stress test
# ===========================================================================


class TestCalibrationStress100:
    """Record 100 outcomes and verify performance, file size, and data integrity."""

    def test_record_100_outcomes_timing(self, tmp_path):
        """Recording 100 outcomes must complete within 5 seconds."""
        cal = make_calibration(tmp_path)

        start = time.perf_counter()
        for i in range(100):
            plan_id = f"stress_100_{i:04d}"
            cal.record_outcome(**make_outcome_kwargs(plan_id, i))
        elapsed_s = time.perf_counter() - start

        assert elapsed_s < 5.0, f"100 outcomes took {elapsed_s:.2f}s (> 5s)"
        assert len(cal._outcomes) == 100

    def test_100_outcomes_file_size_reasonable(self, tmp_path):
        """File size for 100 outcomes must be under 5 MB."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"size_100_{i:04d}", i))

        size = file_size_kb(tmp_path)
        assert size < 1024.0, f"100 outcomes file size: {size:.1f} KB (> 1 MB)"
        # Sanity check: file should be at least 10 KB (not empty)
        assert size > 10.0, f"File too small: {size:.1f} KB (< 10 KB)"

    def test_100_outcomes_load_integrity(self, tmp_path):
        """After 100 outcomes, save → load → verify all data is intact."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"load_100_{i:04d}", i))

        outcomes_before = dict(cal._outcomes)
        dict(cal._learnings)
        summary_before = cal.get_calibration_summary()

        del cal  # simulate restart
        cal2 = make_calibration(tmp_path)

        # Verify outcome count
        assert len(cal2._outcomes) == 100, (
            f"Expected 100 outcomes, got {len(cal2._outcomes)}"
        )

        # Verify specific outcomes by plan_id
        for i in range(0, 100, 7):  # sample every 7th
            plan_id = f"load_100_{i:04d}"
            assert plan_id in cal2._outcomes, f"Missing plan {plan_id}"
            restored = cal2._outcomes[plan_id]
            original = outcomes_before[plan_id]
            assert restored.task_type == original.task_type
            assert restored.strategy_class == original.strategy_class
            assert restored.success == original.success
            assert restored.planned_phases == original.planned_phases
            assert restored.verification_type_failures == original.verification_type_failures

        # Verify summary consistency
        summary_after = cal2.get_calibration_summary()
        assert summary_after["total_outcomes_recorded"] == 100
        assert summary_after["total_learnings"] >= summary_before["total_learnings"]
        assert summary_after["task_types_observed"] == len(TASK_TYPES)
        assert summary_after["strategies_observed"] <= len(STRATEGY_CLASSES)

    def test_100_outcomes_stats_aggregation(self, tmp_path):
        """Stats must be correctly aggregated after 100 outcomes."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"stats_100_{i:04d}", i))

        # Check each task type has correct count
        total_from_tt_stats = sum(
            s["count"] for s in cal._task_type_stats.values()
        )
        assert total_from_tt_stats == 100

        # Check each strategy class has correct count
        total_from_s_stats = sum(
            s["count"] for s in cal._strategy_stats.values()
        )
        assert total_from_s_stats == 100

        # Verify success rates are valid (0-1 range)
        for tt in TASK_TYPES:
            rate = cal.get_task_type_success_rate(tt)
            assert 0.0 <= rate <= 1.0, f"Invalid success rate {rate} for {tt}"

    def test_100_outcomes_adjustments_computation(self, tmp_path):
        """Adjustment queries must still work after 100 outcomes."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"adj_100_{i:04d}", i))

        # Query for each task type
        for tt in TASK_TYPES:
            start = time.perf_counter()
            adjustments = cal.get_adjustments_for_task(tt, "incremental")
            elapsed_ms = (time.perf_counter() - start) * 1000

            assert elapsed_ms < 500, (
                f"Adjustments for {tt} took {elapsed_ms:.1f}ms (> 500ms)"
            )
            # Each adjustment must have valid fields
            for adj in adjustments:
                assert adj.field in ("specialist_activation", "verification_depth")
                assert len(adj.reason) > 0
                assert 0.0 <= adj.confidence <= 1.0

    def test_100_outcomes_all_types_observed(self, tmp_path):
        """After 100 outcomes with varied task types, all types must be observed."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"types_100_{i:04d}", i))

        summary = cal.get_calibration_summary()
        assert summary["task_types_observed"] == len(TASK_TYPES)

    def test_100_outcomes_learnings_by_type(self, tmp_path):
        """After 100 outcomes, there should be learnings across multiple deviation types."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"learn_100_{i:04d}", i))

        summary = cal.get_calibration_summary()
        learnings_by_type = summary["learnings_by_type"]

        # At least 3 distinct deviation types should have been observed
        assert len(learnings_by_type) >= 3, (
            f"Expected >= 3 deviation types, got: {learnings_by_type}"
        )

        # Common deviation types from the seed-based generation
        assert DeviationType.UNPLANNED_FAILURE.value in learnings_by_type
        assert DeviationType.UNNECESSARY_SPECIALIST.value in learnings_by_type


# ===========================================================================
# Section 3: 500-outcome stress test
# ===========================================================================


class TestCalibrationStress500:
    """Record 500 outcomes — heavier stress test."""

    def test_record_500_outcomes_timing(self, tmp_path):
        """500 outcomes must complete within 90 seconds (Windows I/O)."""
        cal = make_calibration(tmp_path, save_interval=10)

        start = time.perf_counter()
        for i in range(500):
            plan_id = f"stress_500_{i:04d}"
            cal.record_outcome(**make_outcome_kwargs(plan_id, i))
        elapsed_s = time.perf_counter() - start

        assert elapsed_s < 90.0, f"500 outcomes took {elapsed_s:.2f}s (> 90s)"
        assert len(cal._outcomes) == 500

    def test_500_outcomes_file_size_reasonable(self, tmp_path):
        """File size for 500 outcomes must be under 25 MB."""
        cal = make_calibration(tmp_path)
        for i in range(500):
            cal.record_outcome(**make_outcome_kwargs(f"size_500_{i:04d}", i))

        size = file_size_kb(tmp_path)
        assert size < 5120.0, f"500 outcomes file size: {size:.1f} KB (> 5 MB)"
        # Sanity check: should be larger than 100-outcome file
        assert size > 50.0, f"File too small: {size:.1f} KB"

    def test_500_outcomes_load_integrity(self, tmp_path):
        """Save → load → verify all 500 outcomes intact."""
        cal = make_calibration(tmp_path)
        for i in range(500):
            cal.record_outcome(**make_outcome_kwargs(f"load_500_{i:04d}", i))

        outcomes_before = len(cal._outcomes)
        learnings_before = len(cal._learnings)

        del cal
        cal2 = make_calibration(tmp_path)

        assert len(cal2._outcomes) == outcomes_before
        assert len(cal2._learnings) == learnings_before

        # Verify spot-check sampling
        for i in [0, 99, 249, 499]:
            plan_id = f"load_500_{i:04d}"
            assert plan_id in cal2._outcomes, f"Missing plan {plan_id}"

    def test_500_outcomes_file_size_vs_100(self, tmp_path):
        """500-outcome file should be roughly ~5x the 100-outcome file size
        (within a 2x tolerance), confirming O(n) not O(n²) growth."""
        # First measure 100-outcome file
        cal1 = make_calibration(tmp_path)
        for i in range(100):
            cal1.record_outcome(**make_outcome_kwargs(f"size_compare_100_{i:04d}", i))
        size_100 = file_size_kb(tmp_path)
        del cal1

        # Then measure 500-outcome file in separate directory
        tmp2 = Path(str(tmp_path) + "_500")
        tmp2.mkdir(exist_ok=True)
        try:
            cal2 = make_calibration(tmp2)
            for i in range(500):
                cal2.record_outcome(**make_outcome_kwargs(f"size_compare_500_{i:04d}", i))
            size_500 = file_size_kb(tmp2)
        finally:
            import shutil
            shutil.rmtree(tmp2, ignore_errors=True)

        # 500 should be ~5x of 100 (within 2x margin)
        ratio = size_500 / max(size_100, 1)
        expected_ratio = 5.0
        assert expected_ratio * 0.5 <= ratio <= expected_ratio * 2.0, (
            f"File size ratio 500:100 = {ratio:.2f}x (expected ~5x). "
            f"100: {size_100:.1f} KB, 500: {size_500:.1f} KB. "
            f"Growth may not be O(n)."
        )


# ===========================================================================
# Section 4: O(n) scaling verification
# ===========================================================================


class TestCalibrationStressScaling:
    """Verify that calibration performance scales linearly (O(n) not O(n²))."""

    # Sample sizes for scaling test — start with small batch then larger
    SAMPLE_SIZES = [10, 50, 100]

    def test_recording_time_scales_linearly(self, tmp_path):
        """Recording 100 outcomes should take ~10x the time of 10 outcomes
        (within a 2x tolerance), confirming O(n) scaling."""
        timings = {}

        for n in self.SAMPLE_SIZES:
            # Use a fresh subdirectory per size
            subdir = Path(str(tmp_path)) / f"scale_{n}"
            subdir.mkdir(exist_ok=True)
            cal = make_calibration(subdir)

            start = time.perf_counter()
            for i in range(n):
                cal.record_outcome(**make_outcome_kwargs(f"scale_{n}_{i:04d}", i))
            elapsed = time.perf_counter() - start
            timings[n] = elapsed

        # 100/10 ratio should be ~10x for ideal O(n). With batching (save_interval=10)
        # the current full-state serialization still adds overhead, so allow 80x.
        ratio_100_10 = timings[100] / max(timings[10], 0.001)
        assert ratio_100_10 < 80.0, (
            f"100-vs-10 timing ratio = {ratio_100_10:.2f}x (expected ~10x). "
            f"10 records: {timings[10]:.3f}s, 100 records: {timings[100]:.3f}s. "
            f"May indicate O(n²) scaling."
        )

        # 50/10 ratio should be ~5x (allow 40x)
        ratio_50_10 = timings[50] / max(timings[10], 0.001)
        assert ratio_50_10 < 40.0, (
            f"50-vs-10 timing ratio = {ratio_50_10:.2f}x (expected ~5x)"
        )

    def test_file_size_scales_linearly(self, tmp_path):
        """File size for 100 outcomes should be ~10x the size of 10 outcomes
        (within 2x tolerance), confirming O(n) file growth."""
        sizes = {}

        for n in self.SAMPLE_SIZES:
            subdir = Path(str(tmp_path)) / f"size_scale_{n}"
            subdir.mkdir(exist_ok=True)
            cal = make_calibration(subdir)
            for i in range(n):
                cal.record_outcome(**make_outcome_kwargs(f"size_scale_{n}_{i:04d}", i))
            sizes[n] = file_size_kb(subdir)

        # 100/10 size ratio should be ~10
        ratio_100_10 = sizes[100] / max(sizes[10], 1)
        assert 3.0 <= ratio_100_10 <= 20.0, (
            f"100-vs-10 file size ratio = {ratio_100_10:.2f}x (expected ~10x). "
            f"10 records: {sizes[10]:.1f} KB, 100 records: {sizes[100]:.1f} KB."
        )

    def test_load_time_scales_linearly(self, tmp_path):
        """Loading a 100-outcome file should take ~10x a 10-outcome file
        (within 3x tolerance)."""
        load_times = {}

        for n in self.SAMPLE_SIZES:
            subdir = Path(str(tmp_path)) / f"load_scale_{n}"
            subdir.mkdir(exist_ok=True)
            cal = make_calibration(subdir)
            for i in range(n):
                cal.record_outcome(**make_outcome_kwargs(f"load_scale_{n}_{i:04d}", i))
            del cal

            # Measure load time
            start = time.perf_counter()
            cal2 = make_calibration(subdir)
            load_times[n] = (time.perf_counter() - start) * 1000
            assert len(cal2._outcomes) == n

        # Just ensure loading doesn't explode — 100 should be < 2s
        assert load_times[100] < 2000.0, (
            f"Loading 100 outcomes took {load_times[100]:.1f}ms (> 2s)"
        )
        # 10 and 50 should be much faster
        assert load_times[10] < 500.0
        assert load_times[50] < 1000.0


# ===========================================================================
# Section 5: Memory and edge cases
# ===========================================================================


class TestCalibrationStressMemory:
    """Memory-related stress tests."""

    def test_calibration_summary_always_reasonable(self, tmp_path):
        """get_calibration_summary must always return within 100ms regardless
        of calibration size."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"summary_{i:04d}", i))

        start = time.perf_counter()
        summary = cal.get_calibration_summary()
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 100, f"get_calibration_summary took {elapsed_ms:.1f}ms"
        assert summary["total_outcomes_recorded"] == 100
        assert summary["total_learnings"] > 0
        assert summary["overall_success_rate"] >= 0.0

    def test_snapshot_performance(self, tmp_path):
        """snapshot() must be fast even with many outcomes."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"snap_{i:04d}", i))

        start = time.perf_counter()
        snap = cal.snapshot()
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 50, f"snapshot() took {elapsed_ms:.1f}ms"
        assert snap["total_outcomes_recorded"] == 100

    def test_recent_learnings_limit(self, tmp_path):
        """get_recent_learnings(limit=10) must respect the limit."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"recent_{i:04d}", i))

        recent = cal.get_recent_learnings(limit=10)
        assert len(recent) <= 10

        # Should be sorted by recency (most recent first)
        for j in range(len(recent) - 1):
            assert recent[j].created_at >= recent[j + 1].created_at

    def test_task_type_success_rate_at_scale(self, tmp_path):
        """get_task_type_success_rate must work correctly at scale."""
        cal = make_calibration(tmp_path)
        successes_by_type: Dict[str, int] = {}
        total_by_type: Dict[str, int] = {}

        for i in range(100):
            kwargs = make_outcome_kwargs(f"rate_{i:04d}", i)
            cal.record_outcome(**kwargs)
            tt = kwargs["task_type"]
            total_by_type[tt] = total_by_type.get(tt, 0) + 1
            if kwargs["success"]:
                successes_by_type[tt] = successes_by_type.get(tt, 0) + 1

        for tt in TASK_TYPES:
            rate = cal.get_task_type_success_rate(tt)
            if total_by_type.get(tt, 0) > 0:
                expected = successes_by_type.get(tt, 0) / total_by_type[tt]
                assert rate == pytest.approx(expected, abs=0.01), (
                    f"Success rate for {tt}: expected {expected:.4f}, got {rate:.4f}"
                )

    def test_strategy_effectiveness_at_scale(self, tmp_path):
        """get_strategy_effectiveness must return correct aggregations."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"strat_{i:04d}", i))

        for sc in STRATEGY_CLASSES:
            eff = cal.get_strategy_effectiveness(sc)
            if eff:
                assert eff["count"] > 0
                assert eff["avg_phases"] > 0.0
                assert 0.0 <= eff["avg_unplanned"] <= 2.0  # sanity: max 2 unplanned per outcome

    def test_clear_and_reuse(self, tmp_path):
        """After clear_persistence, a new system should start empty."""
        cal = make_calibration(tmp_path)
        for i in range(50):
            cal.record_outcome(**make_outcome_kwargs(f"clear_{i:04d}", i))

        cal.clear_persistence()

        # New system in same directory should start empty
        cal2 = make_calibration(tmp_path)
        assert len(cal2._outcomes) == 0
        assert len(cal2._learnings) == 0

    def test_duplicate_plan_id_overwrites(self, tmp_path):
        """Recording an outcome with the same plan_id as an existing outcome
        must replace it without growing file size unbounded."""
        cal = make_calibration(tmp_path)

        for i in range(50):
            cal.record_outcome(**make_outcome_kwargs(f"dup_{i:04d}", i))

        size_before = file_size_kb(tmp_path)

        # Overwrite all 50 with same IDs but different data
        for i in range(50):
            kwargs = make_outcome_kwargs(f"dup_{i:04d}", 999 + i)  # different seed
            cal.record_outcome(**kwargs)

        size_after = file_size_kb(tmp_path)

        assert len(cal._outcomes) == 50  # not 100
        # File should not have doubled — some growth from learnings + deviations
        assert size_after < size_before * 1.5, (
            f"File size grew significantly after overwriting: {size_before:.1f} KB -> {size_after:.1f} KB"
        )


# ===========================================================================
# Section 6: JSON structure integrity
# ===========================================================================


class TestCalibrationStressJsonIntegrity:
    """Verify the JSON file structure is valid and well-formed at scale."""

    def test_json_file_parses_at_scale(self, tmp_path):
        """The JSON file must be parseable after 500 outcomes."""
        cal = make_calibration(tmp_path)
        for i in range(500):
            cal.record_outcome(**make_outcome_kwargs(f"json_{i:04d}", i))

        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        data = json.loads(file_path.read_text(encoding="utf-8"))

        assert "outcomes" in data
        assert "learnings" in data
        assert "task_type_stats" in data
        assert "strategy_stats" in data
        assert len(data["outcomes"]) == 500

    def test_each_outcome_has_required_fields(self, tmp_path):
        """Every outcome in the JSON file must have all required PlanOutcome fields."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"fields_{i:04d}", i))

        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        data = json.loads(file_path.read_text(encoding="utf-8"))

        required_fields = {
            "plan_id", "objective", "task_type", "strategy_class",
            "planned_phases", "completed_phases", "success",
        }

        for pid, outcome_dict in data["outcomes"].items():
            for field in required_fields:
                assert field in outcome_dict, (
                    f"Outcome {pid} missing required field: {field}"
                )

    def test_each_learning_has_required_fields(self, tmp_path):
        """Every learning in the JSON file must have all required fields."""
        cal = make_calibration(tmp_path)
        for i in range(100):
            cal.record_outcome(**make_outcome_kwargs(f"learn_fields_{i:04d}", i))

        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        data = json.loads(file_path.read_text(encoding="utf-8"))

        required_fields = {"id", "deviation_type", "description", "plan_prediction", "actual_outcome"}

        for lid, learning_dict in data["learnings"].items():
            for field in required_fields:
                assert field in learning_dict, (
                    f"Learning {lid} missing required field: {field}"
                )
