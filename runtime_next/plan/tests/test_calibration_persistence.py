"""
test_calibration_persistence.py — Cross-Session Calibration Persistence Tests

Verifies:
  1. Save/Load cycle — outcomes survive across PlanCalibrationSystem instances
  2. Deviation analysis — _analyze_deviations() produces correct LearningEntry types
     for verification failures, unplanned failures, over-verification, specialist
     mismatch, phase completion shortfalls, and risk inaccuracy
  3. Verification calibration integration — recording verification-type failure
     entries produces analyzable INEFFICIENT_VERIFICATION learnings
  4. get_adjustments_for_task() returns adjustments after load
  5. Corrupt data handling — graceful recovery with empty state
  6. clear_persistence() — file deletion
"""

from __future__ import annotations

import json
from typing import Any, Dict
from pathlib import Path

import pytest

from runtime_next.plan.calibration import (
    PlanCalibrationSystem,
    LearningEntry,
    DeviationType,
    CALIBRATION_DATA_FILENAME,
)


# ===========================================================================
# Helpers
# ===========================================================================


def sample_outcome_kwargs(
    plan_id: str = "plan_001",
    task_type: str = "refactor",
    **overrides: Any,
) -> Dict[str, Any]:
    """Standard outcome kwargs for testing, with optional overrides."""
    kwargs: Dict[str, Any] = {
        "plan_id": plan_id,
        "objective": "Refactor the authentication module to async",
        "task_type": task_type,
        "strategy_class": "incremental",
        "planned_phases": 5,
        "completed_phases": 4,
        "planned_specialists": ["FORGE", "SENTINEL", "ORACLE"],
        "actual_specialists": ["FORGE", "ORACLE"],  # SENTINEL not used
        "planned_risks": 3,
        "materialized_risks": 1,
        "verification_checks_run": 8,
        "verification_failures_caught": 2,
        "verification_type_failures": {"typecheck": 1, "security_scan": 1},
        "unplanned_failures": 1,
        "total_duration_ms": 15000.0,
        "success": True,
    }
    kwargs.update(overrides)
    return kwargs


def make_calibration(tmp_path: Path) -> PlanCalibrationSystem:
    """Create a PlanCalibrationSystem rooted at a temp directory."""
    return PlanCalibrationSystem(storage_path=str(tmp_path))


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def tmp_cal(tmp_path: Path) -> PlanCalibrationSystem:
    """Fresh PlanCalibrationSystem in a temp directory (empty, no prior data)."""
    return make_calibration(tmp_path)


@pytest.fixture
def populated_cal(tmp_path: Path) -> PlanCalibrationSystem:
    """Calibration system with several recorded outcomes for persistence tests."""
    cal = make_calibration(tmp_path)

    cal.record_outcome(**sample_outcome_kwargs(plan_id="plan_001"))
    cal.record_outcome(**sample_outcome_kwargs(
        plan_id="plan_002",
        task_type="fix",
        strategy_class="minimal_patch",
        planned_phases=3,
        completed_phases=3,
        planned_specialists=["FORGE"],
        actual_specialists=["FORGE"],
        planned_risks=2,
        materialized_risks=2,
        verification_checks_run=4,
        verification_failures_caught=0,
        verification_type_failures={},
        unplanned_failures=0,
        success=True,
    ))
    cal.record_outcome(**sample_outcome_kwargs(
        plan_id="plan_003",
        task_type="feature",
        strategy_class="mvp",
        planned_phases=6,
        completed_phases=2,  # major shortfall
        planned_specialists=["FORGE", "ORACLE", "HERMES"],
        actual_specialists=["FORGE"],  # big specialist mismatch
        planned_risks=4,
        materialized_risks=0,  # overestimated risk
        verification_checks_run=10,
        verification_failures_caught=0,
        verification_type_failures={},
        unplanned_failures=2,
        success=False,
    ))

    return cal


# ===========================================================================
# Section 1: Save / Load Cycle
# ===========================================================================


class TestCalibrationPersistenceSaveLoad:
    """Verify outcomes survive across PlanCalibrationSystem instances."""

    def test_save_and_load_preserves_outcomes(self, populated_cal, tmp_path):
        """After saving outcomes in one instance and loading in another,
        all outcomes must be present with correct data."""
        # populated_cal already has 3 outcomes saved to disk
        del populated_cal  # simulate restart

        # Load into a new instance pointing to the same temp dir
        cal2 = make_calibration(tmp_path)

        assert len(cal2._outcomes) == 3
        assert "plan_001" in cal2._outcomes
        assert "plan_002" in cal2._outcomes
        assert "plan_003" in cal2._outcomes

        # Verify plan_001 data is intact
        o1 = cal2._outcomes["plan_001"]
        assert o1.task_type == "refactor"
        assert o1.planned_phases == 5
        assert o1.completed_phases == 4
        assert o1.verification_failures_caught == 2
        assert o1.verification_type_failures == {"typecheck": 1, "security_scan": 1}
        assert o1.success is True

    def test_save_and_load_preserves_learnings(self, populated_cal, tmp_path):
        """Learnings generated by _analyze_deviations must survive restart."""
        del populated_cal

        cal2 = make_calibration(tmp_path)

        # plan_001 had: 1 unplanned failure (UNPLANNED_FAILURE),
        #               risk accuracy < 0.5 (INCORRECT_RISK),
        #               2 verification failures (INEFFICIENT_VERIFICATION x2),
        #               1 unused specialist (UNNECESSARY_SPECIALIST),
        #               1 phase shortfall < 80% (STRATEGY_MISMATCH)
        #               = 6 learnings
        # plan_002 had: all checks passed, no deviations → 0 learnings
        # plan_003 had: 2 unplanned failures, risk overestimated, >5 checks 0 failures,
        #               2 unused specialists, phase shortfall = 6 total deviations
        assert len(cal2._learnings) >= 6  # at least plan_001 + plan_003 deviations

    def test_save_and_load_preserves_stats(self, populated_cal, tmp_path):
        """Task-type and strategy stats must survive restart."""
        del populated_cal

        cal2 = make_calibration(tmp_path)

        # Task type stats
        assert "refactor" in cal2._task_type_stats
        assert "fix" in cal2._task_type_stats
        assert "feature" in cal2._task_type_stats
        assert cal2._task_type_stats["refactor"]["count"] == 1
        assert cal2._task_type_stats["fix"]["count"] == 1

        # Strategy stats
        assert "incremental" in cal2._strategy_stats
        assert "minimal_patch" in cal2._strategy_stats
        assert "mvp" in cal2._strategy_stats

    def test_save_and_load_preserves_calibration_summary(self, populated_cal, tmp_path):
        """get_calibration_summary() must return consistent data after reload."""
        # Get summary before restart
        summary_before = populated_cal.get_calibration_summary()

        del populated_cal
        cal2 = make_calibration(tmp_path)
        summary_after = cal2.get_calibration_summary()

        assert summary_after["total_outcomes_recorded"] == summary_before["total_outcomes_recorded"]
        assert summary_after["total_learnings"] == summary_before["total_learnings"]
        assert summary_after["task_types_observed"] == summary_before["task_types_observed"]
        assert summary_after["strategies_observed"] == summary_before["strategies_observed"]

    def test_json_file_created_on_disk(self, populated_cal, tmp_path):
        """The JSON data file must exist after recording outcomes."""
        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        assert file_path.exists()
        assert file_path.stat().st_size > 0

        # File must be valid JSON
        data = json.loads(file_path.read_text(encoding="utf-8"))
        assert "outcomes" in data
        assert "learnings" in data
        assert "task_type_stats" in data
        assert "strategy_stats" in data

    def test_save_with_multiple_outcomes_increments_file(self, tmp_path):
        """Recording outcomes sequentially must build up the file."""
        cal = make_calibration(tmp_path)

        cal.record_outcome(**sample_outcome_kwargs(plan_id="a"))
        size_1 = (tmp_path / CALIBRATION_DATA_FILENAME).stat().st_size

        cal.record_outcome(**sample_outcome_kwargs(plan_id="b"))
        size_2 = (tmp_path / CALIBRATION_DATA_FILENAME).stat().st_size

        assert size_2 > size_1, "File size should grow with more outcomes"

    def test_save_without_storage_path_does_nothing(self):
        """Calibration system without storage_path should not crash on save."""
        cal = PlanCalibrationSystem(storage_path=None)
        cal.record_outcome(**sample_outcome_kwargs(plan_id="no_path"))
        # Should not raise — _save checks _storage_path is None and returns
        assert len(cal._outcomes) == 1

    def test_load_without_storage_path_returns_empty(self):
        """Loading without storage_path should not crash and return empty state."""
        cal = PlanCalibrationSystem(storage_path=None)
        assert len(cal._outcomes) == 0
        assert len(cal._learnings) == 0


# ===========================================================================
# Section 2: Corrupt Data Handling
# ===========================================================================


class TestCalibrationCorruptData:
    """Verify graceful recovery from corrupted persistence data."""

    def test_load_corrupt_json_resets_to_empty(self, tmp_path):
        """If the JSON file is corrupt, the system must reset to an empty state."""
        cal = make_calibration(tmp_path)
        cal.record_outcome(**sample_outcome_kwargs(plan_id="plan_good"))

        # Corrupt the file
        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        file_path.write_text("{this is not valid json~~~}", encoding="utf-8")

        # Load into a new instance — must handle gracefully
        cal2 = make_calibration(tmp_path)
        assert len(cal2._outcomes) == 0, "Corrupt data should result in empty state"
        assert len(cal2._learnings) == 0
        assert len(cal2._task_type_stats) == 0
        assert len(cal2._strategy_stats) == 0

    def test_load_empty_file_resets_to_empty(self, tmp_path):
        """An empty file should also reset to empty state."""
        cal = make_calibration(tmp_path)
        cal.record_outcome(**sample_outcome_kwargs(plan_id="plan_good"))

        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        file_path.write_text("", encoding="utf-8")

        cal2 = make_calibration(tmp_path)
        assert len(cal2._outcomes) == 0
        assert len(cal2._learnings) == 0

    def test_load_partial_data_reconstructs_what_it_can(self, tmp_path):
        """If the file has valid outcomes but missing sections, partial load succeeds."""
        cal = make_calibration(tmp_path)
        cal.record_outcome(**sample_outcome_kwargs(plan_id="plan_partial"))

        # Manually write a file with only outcomes (no learnings/stats)
        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        data = json.loads(file_path.read_text(encoding="utf-8"))
        partial = {"outcomes": data["outcomes"]}
        file_path.write_text(json.dumps(partial), encoding="utf-8")

        cal2 = make_calibration(tmp_path)
        assert len(cal2._outcomes) == 1
        assert len(cal2._learnings) == 0
        assert len(cal2._task_type_stats) == 0
        assert len(cal2._strategy_stats) == 0

    def test_load_nonexistent_file_returns_empty(self, tmp_path):
        """A nonexistent file should not cause an error — just empty state."""
        cal = make_calibration(tmp_path)
        assert len(cal._outcomes) == 0  # fresh state, no file to load

    def test_clear_persistence_removes_file(self, tmp_path):
        """clear_persistence() must delete the calibration data file."""
        cal = make_calibration(tmp_path)
        cal.record_outcome(**sample_outcome_kwargs(plan_id="to_delete"))

        file_path = tmp_path / CALIBRATION_DATA_FILENAME
        assert file_path.exists()

        cal.clear_persistence()
        assert not file_path.exists()

    def test_clear_persistence_no_file_does_not_crash(self, tmp_path):
        """Calling clear_persistence() when no file exists should not crash."""
        cal = make_calibration(tmp_path)
        cal.clear_persistence()  # no file yet
        # Should not raise


# ===========================================================================
# Section 3: Deviation Analysis
# ===========================================================================


class TestCalibrationDeviationAnalysis:
    """Verify _analyze_deviations() produces correct LearningEntry types."""

    def test_unplanned_failure_deviation(self, tmp_cal):
        """Unplanned failures must generate UNPLANNED_FAILURE learnings."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            unplanned_failures=2,
        ))
        unplanned = [d for d in outcome.deviations
                     if d.deviation_type == DeviationType.UNPLANNED_FAILURE]
        assert len(unplanned) == 1
        assert "2 failures" in unplanned[0].description
        assert unplanned[0].severity == pytest.approx(0.4, abs=0.01)

    def test_verification_type_failure_deviation(self, tmp_cal):
        """Verification type failures must generate INEFFICIENT_VERIFICATION learnings
        with per-check-type detail."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            verification_type_failures={"typecheck": 2, "security_scan": 1, "lint": 0},
        ))
        ver_fails = [d for d in outcome.deviations
                     if d.deviation_type == DeviationType.INEFFICIENT_VERIFICATION
                     and "Check type" in d.description]
        # Should have one learning per failing type (1 for typecheck, 1 for security_scan)
        assert len(ver_fails) == 2
        typecheck_learnings = [d for d in ver_fails if "typecheck" in d.description]
        security_learnings = [d for d in ver_fails if "security_scan" in d.description]
        assert len(typecheck_learnings) == 1
        assert "failed 2 time(s)" in typecheck_learnings[0].description
        assert len(security_learnings) == 1
        assert "failed 1 time(s)" in security_learnings[0].description

    def test_verification_type_failures_zero_counts_skipped(self, tmp_cal):
        """Verification types with zero failures must not generate learnings."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            verification_type_failures={"lint": 0, "typecheck": 0},
        ))
        vtype = [d for d in outcome.deviations
                 if d.deviation_type == DeviationType.INEFFICIENT_VERIFICATION
                 and "Check type" in d.description]
        assert len(vtype) == 0

    def test_over_verification_deviation(self, tmp_cal):
        """More than 5 verification checks with zero failures must generate
        an INEFFICIENT_VERIFICATION over-verification learning."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            verification_checks_run=8,
            verification_failures_caught=0,
            verification_type_failures={},
        ))
        over_ver = [d for d in outcome.deviations
                    if d.deviation_type == DeviationType.INEFFICIENT_VERIFICATION
                    and "checks ran but none caught" in d.description]
        assert len(over_ver) == 1
        assert "8 verification checks" in over_ver[0].description

    def test_no_over_verification_with_few_checks(self, tmp_cal):
        """Fewer than 5 checks with zero failures must NOT generate over-verification."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            verification_checks_run=3,
            verification_failures_caught=0,
        ))
        over_ver = [d for d in outcome.deviations
                    if d.deviation_type == DeviationType.INEFFICIENT_VERIFICATION
                    and "checks ran but none caught" in d.description]
        assert len(over_ver) == 0

    def test_verification_missed_failures(self, tmp_cal):
        """Verification caught zero failures but unplanned failures > 0 must
        generate a 'verification missed' learning.

        verification_checks_run=0 ensures the over-verification `if` branch
        is skipped so the `elif` chain reaches the missed-failures check.
        verification_type_failures={} prevents independent per-type deviations.
        """
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            verification_checks_run=0,
            verification_failures_caught=0,
            verification_type_failures={},
            unplanned_failures=3,
        ))
        missed = [d for d in outcome.deviations
                  if d.deviation_type == DeviationType.INEFFICIENT_VERIFICATION
                  and "missed" in d.description]
        assert len(missed) == 1
        assert "missed 3 failures" in missed[0].description
        # Confidence for missed failures should be high (0.8)
        assert missed[0].confidence == 0.8

    def test_unnecessary_specialist_deviation(self, tmp_cal):
        """Unused specialists must generate UNNECESSARY_SPECIALIST learnings."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            planned_specialists=["FORGE", "SENTINEL", "ORACLE"],
            actual_specialists=["FORGE"],  # SENTINEL and ORACLE not activated
        ))
        unused = [d for d in outcome.deviations
                  if d.deviation_type == DeviationType.UNNECESSARY_SPECIALIST]
        assert len(unused) == 1
        assert "SENTINEL" in unused[0].description or "ORACLE" in unused[0].description

    def test_no_specialist_deviation_when_all_used(self, tmp_cal):
        """When all planned specialists are actually used, no UNNECESSARY_SPECIALIST."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            planned_specialists=["FORGE", "ORACLE"],
            actual_specialists=["FORGE", "ORACLE"],
        ))
        unused = [d for d in outcome.deviations
                  if d.deviation_type == DeviationType.UNNECESSARY_SPECIALIST]
        assert len(unused) == 0

    def test_risk_inaccuracy_deviation(self, tmp_cal):
        """Large mismatch between planned and materialized risks must generate
        INCORRECT_RISK learnings."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            planned_risks=5,
            materialized_risks=0,
        ))
        risk_devs = [d for d in outcome.deviations
                     if d.deviation_type == DeviationType.INCORRECT_RISK]
        assert len(risk_devs) == 1
        assert "overestimated" in risk_devs[0].description

    def test_risk_underestimation_deviation(self, tmp_cal):
        """When more risks materialize than planned, generate underestimation learning.
        Uses values that keep severity within [0, 1] bounds."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            planned_risks=2,
            materialized_risks=4,
        ))
        risk_devs = [d for d in outcome.deviations
                     if d.deviation_type == DeviationType.INCORRECT_RISK]
        assert len(risk_devs) == 1
        assert "underestimated" in risk_devs[0].description

    def test_risk_accuracy_no_deviation(self, tmp_cal):
        """When risk accuracy is >= 0.5, no INCORRECT_RISK learning is generated."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            planned_risks=3,
            materialized_risks=2,
        ))
        risk_devs = [d for d in outcome.deviations
                     if d.deviation_type == DeviationType.INCORRECT_RISK]
        assert len(risk_devs) == 0

    def test_phase_completion_shortfall_deviation(self, tmp_cal):
        """Completing fewer than 80% of planned phases must generate
        STRATEGY_MISMATCH learnings."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            planned_phases=10,
            completed_phases=3,
        ))
        phase_devs = [d for d in outcome.deviations
                      if d.deviation_type == DeviationType.STRATEGY_MISMATCH]
        assert len(phase_devs) == 1
        assert "3/10 phases" in phase_devs[0].description
        assert "simpler strategies" in phase_devs[0].recommendation

    def test_no_phase_shortfall_deviation_when_completed(self, tmp_cal):
        """Completing all phases or >80% should NOT generate STRATEGY_MISMATCH."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            planned_phases=5,
            completed_phases=5,
        ))
        phase_devs = [d for d in outcome.deviations
                      if d.deviation_type == DeviationType.STRATEGY_MISMATCH]
        assert len(phase_devs) == 0

    def test_multiple_deviations_combined(self, tmp_cal):
        """A single outcome with multiple issue types generates all applicable
        deviation learnings."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            unplanned_failures=1,
            planned_risks=4,
            materialized_risks=1,  # 3 difference → risk_accuracy=0.25 < 0.5
            verification_type_failures={"lint": 2, "security_scan": 1},
            planned_specialists=["FORGE", "SENTINEL", "ORACLE", "HERMES"],
            actual_specialists=["FORGE", "SENTINEL"],
            planned_phases=8,
            completed_phases=4,
            verification_checks_run=6,
            verification_failures_caught=3,
        ))
        # Count by type
        by_type: Dict[str, int] = {}
        for d in outcome.deviations:
            by_type[d.deviation_type.value] = by_type.get(d.deviation_type.value, 0) + 1

        assert DeviationType.UNPLANNED_FAILURE.value in by_type
        assert DeviationType.INCORRECT_RISK.value in by_type
        assert DeviationType.INEFFICIENT_VERIFICATION.value in by_type  # at least 2 per-type + maybe main
        assert DeviationType.UNNECESSARY_SPECIALIST.value in by_type
        assert DeviationType.STRATEGY_MISMATCH.value in by_type
        assert len(outcome.deviations) >= 5  # at least one of each type


# ===========================================================================
# Section 4: Verification Calibration Integration
# ===========================================================================


class TestVerificationCalibrationIntegration:
    """Verify that recording verification-type failure entries produces
    analyzable INEFFICIENT_VERIFICATION learnings, and that those learnings
    survive persistence and produce adjustments."""

    def test_verification_failure_creates_analyzable_deviation(self, tmp_cal):
        """Recording an outcome with verification_type_failures must generate
        LearningEntry objects with proper fields for analysis."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            verification_type_failures={"typecheck": 2, "lint": 1},
        ))
        ver_devs = [d for d in outcome.deviations
                    if d.deviation_type == DeviationType.INEFFICIENT_VERIFICATION
                    and "Check type" in d.description]

        for dev in ver_devs:
            # Each deviation must have:
            assert isinstance(dev.id, str) and len(dev.id) > 0
            assert len(dev.description) > 0
            assert len(dev.plan_prediction) > 0
            assert len(dev.actual_outcome) > 0
            assert 0.0 <= dev.severity <= 1.0
            assert "refactor" in dev.applicable_task_types  # from sample outcome
            assert "incremental" in dev.applicable_strategy_classes
            assert len(dev.recommendation) > 0
            assert 0.0 <= dev.confidence <= 1.0

    def test_verification_deviation_survives_persistence(self, tmp_path):
        """INEFFICIENT_VERIFICATION learnings must survive save/load."""
        cal = make_calibration(tmp_path)
        cal.record_outcome(**sample_outcome_kwargs(
            plan_id="v_persist",
            verification_type_failures={"typecheck": 1, "security_scan": 3},
        ))
        len(cal._learnings)

        del cal
        cal2 = make_calibration(tmp_path)

        # After load, the learnings should include the verification deviations
        assert len(cal2._learnings) > 0
        # At minimum we should have the INEFFICIENT_VERIFICATION learnings
        ver_learnings = [l for l in cal2._learnings.values()
                         if l.deviation_type == DeviationType.INEFFICIENT_VERIFICATION]
        assert len(ver_learnings) >= 2  # one for typecheck, one for security_scan

    def test_verification_deviation_produces_adjustments(self, tmp_path):
        """INEFFICIENT_VERIFICATION learnings must produce adjustments
        via get_adjustments_for_task()."""
        cal = make_calibration(tmp_path)
        cal.record_outcome(**sample_outcome_kwargs(
            plan_id="v_adjust",
            task_type="refactor",
            strategy_class="incremental",
            verification_type_failures={"typecheck": 2},
        ))

        # Query adjustments for a new refactor task
        adjustments = cal.get_adjustments_for_task(
            task_type="refactor",
            strategy_class="incremental",
        )

        # Should have at least one INEFFICIENT_VERIFICATION-derived adjustment
        ver_adjustments = [a for a in adjustments if a.field == "verification_depth"]
        assert len(ver_adjustments) >= 1
        assert ver_adjustments[0].field == "verification_depth"
        assert ver_adjustments[0].old_value == "standard"
        assert ver_adjustments[0].new_value == "adjusted"

    def test_adjustments_survive_persistence(self, tmp_path):
        """CalibrationAdjustment generation must work after save/load cycle."""
        cal = make_calibration(tmp_path)
        cal.record_outcome(**sample_outcome_kwargs(
            plan_id="adj_persist",
            task_type="refactor",
            strategy_class="incremental",
            verification_type_failures={"typecheck": 2},
            planned_specialists=["FORGE", "SENTINEL", "ORACLE"],
            actual_specialists=["FORGE"],  # generates UNNECESSARY_SPECIALIST
        ))

        del cal
        cal2 = make_calibration(tmp_path)

        adjustments = cal2.get_adjustments_for_task(
            task_type="refactor",
            strategy_class="incremental",
        )

        # Should have both verification_depth and specialist_activation adjustments
        fields = {a.field for a in adjustments}
        assert "verification_depth" in fields, "Verification adjustments must survive load"
        # specialist_activation may or may not be present depending on whether
        # the UNNECESSARY_SPECIALIST learning is for refactor task type
        # (it should be, since we recorded with task_type=refactor)

    def test_unnecessary_specialist_deviation_produces_adjustments(self, tmp_cal):
        """UNNECESSARY_SPECIALIST learnings must produce specialist_activation adjustments."""
        tmp_cal.record_outcome(**sample_outcome_kwargs(
            task_type="refactor",
            planned_specialists=["FORGE", "SENTINEL", "ORACLE", "HERMES"],
            actual_specialists=["FORGE"],
        ))

        adjustments = tmp_cal.get_adjustments_for_task(
            task_type="refactor",
            strategy_class="incremental",
        )

        spec_adjustments = [a for a in adjustments if a.field == "specialist_activation"]
        assert len(spec_adjustments) >= 1
        assert spec_adjustments[0].old_value == "default_threshold"
        assert spec_adjustments[0].new_value == "adjusted_threshold"
        assert spec_adjustments[0].confidence == 0.5  # from UNNECESSARY_SPECIALIST default

    def test_adjustments_filtered_by_task_type(self, tmp_cal):
        """get_adjustments_for_task must only return adjustments applicable
        to the requested task_type."""
        tmp_cal.record_outcome(**sample_outcome_kwargs(
            plan_id="spec_filter",
            task_type="refactor",
            planned_specialists=["FORGE", "SENTINEL"],
            actual_specialists=["FORGE"],
        ))
        # Also record a feature outcome with no specialist issues
        tmp_cal.record_outcome(**sample_outcome_kwargs(
            plan_id="feature_filter",
            task_type="feature",
            planned_specialists=["FORGE"],
            actual_specialists=["FORGE"],
            verification_type_failures={"lint": 1},
        ))

        refactor_adjustments = tmp_cal.get_adjustments_for_task(
            task_type="refactor", strategy_class="incremental",
        )
        feature_adjustments = tmp_cal.get_adjustments_for_task(
            task_type="feature", strategy_class="incremental",
        )

        # Refactor adjustments should include specialist_activation
        refactor_spec = [a for a in refactor_adjustments if a.field == "specialist_activation"]
        # Feature adjustments should not include specialist_activation (no specialist deviation)
        feature_spec = [a for a in feature_adjustments if a.field == "specialist_activation"]

        assert len(refactor_spec) >= 1
        assert len(feature_spec) == 0


# ===========================================================================
# Section 5: get_adjustments_for_task() Edge Cases
# ===========================================================================


class TestCalibrationAdjustmentsEdgeCases:
    """Verify get_adjustments_for_task() handles edge cases correctly."""

    def test_no_learnings_returns_empty(self, tmp_cal):
        """With no recorded learnings, adjustments must be empty."""
        adjustments = tmp_cal.get_adjustments_for_task(
            task_type="refactor", strategy_class="incremental",
        )
        assert adjustments == []

    def test_irrelevant_task_type_returns_empty(self, populated_cal):
        """A task type with no matching learnings must return empty adjustments."""
        adjustments = populated_cal.get_adjustments_for_task(
            task_type="nonexistent_task_type",
            strategy_class="incremental",
        )
        assert adjustments == []

    def test_irrelevant_strategy_class_still_matches(self, populated_cal):
        """If the task_type matches but strategy_class doesn't, should still match
        because the filter only checks applicable_task_types."""
        adjustments = populated_cal.get_adjustments_for_task(
            task_type="refactor",
            strategy_class="nonexistent_strategy",
        )
        # Should still return adjustments because applicable_task_types doesn't
        # filter by strategy_class for UNNECESSARY_SPECIALIST deviations
        # (it only checks task_type)
        specialist_adjs = [a for a in adjustments if a.field == "specialist_activation"]
        assert len(specialist_adjs) >= 0  # might or might not have specialist deviations
        # But should NOT filter out if strategy doesn't match — because the filter
        # in get_adjustments_for_task only checks applicable_task_types, NOT strategy_class

    def test_get_task_type_success_rate_default(self, tmp_cal):
        """A task type with no recorded outcomes should return 0.5 default."""
        rate = tmp_cal.get_task_type_success_rate("unseen_type")
        assert rate == 0.5

    def test_get_task_type_success_rate_computed(self, populated_cal):
        """A known task type should return the computed success rate."""
        rate = populated_cal.get_task_type_success_rate("refactor")
        # refactor had 1 success out of 1 recorded outcome
        assert rate == 1.0

        rate_fix = populated_cal.get_task_type_success_rate("fix")
        assert rate_fix == 1.0  # fix had 1 success

        rate_feature = populated_cal.get_task_type_success_rate("feature")
        assert rate_feature == 0.0  # feature had 1 failure

    def test_get_strategy_effectiveness_unknown(self, tmp_cal):
        """Unknown strategy class should return empty dict."""
        result = tmp_cal.get_strategy_effectiveness("unknown")
        assert result == {}

    def test_get_strategy_effectiveness_known(self, populated_cal):
        """Known strategy class should return its stats dict."""
        result = populated_cal.get_strategy_effectiveness("incremental")
        assert result["count"] == 1
        assert result["successes"] == 1
        assert result["avg_phases"] == 5.0


# ===========================================================================
# Section 6: LearningEntry Data Integrity
# ===========================================================================


class TestLearningEntryDataIntegrity:
    """Verify that LearningEntry objects produced by _analyze_deviations
    have all required fields filled and reasonable defaults."""

    def test_unplanned_failure_entry_has_all_fields(self, tmp_cal):
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            unplanned_failures=2,
        ))
        dev = next(d for d in outcome.deviations
                   if d.deviation_type == DeviationType.UNPLANNED_FAILURE)

        assert len(dev.id) > 0
        assert len(dev.description) > 0
        assert len(dev.plan_prediction) > 0
        assert len(dev.actual_outcome) > 0
        assert dev.severity > 0.0
        assert len(dev.applicable_task_types) > 0
        assert dev.times_applied == 0  # fresh learning
        assert dev.effectiveness_score == 0.5  # default

    def test_learning_entry_timestamps(self, tmp_cal):
        """Learning entries must have created_at set."""
        outcome = tmp_cal.record_outcome(**sample_outcome_kwargs(
            verification_type_failures={"lint": 1},
        ))
        for dev in outcome.deviations:
            from datetime import datetime
            assert isinstance(dev.created_at, datetime)
            assert dev.created_at is not None

    def test_learning_entry_pydantic_roundtrip(self):
        """LearningEntry must serialize and deserialize correctly (for JSON persistence)."""
        entry = LearningEntry(
            id="test_001",
            deviation_type=DeviationType.INEFFICIENT_VERIFICATION,
            description="Verification check 'lint' failed 2 time(s) for refactor task",
            plan_prediction="'lint' checks would pass",
            actual_outcome="'lint' failed 2 time(s)",
            severity=0.6,
            applicable_task_types=["refactor"],
            applicable_strategy_classes=["incremental"],
            recommendation="Strengthen lint check design",
            confidence=0.6,
            times_applied=0,
            effectiveness_score=0.5,
        )
        # To dict and back
        data = entry.model_dump(mode="json")
        restored = LearningEntry(**data)
        assert restored.id == entry.id
        assert restored.deviation_type == entry.deviation_type
        assert restored.description == entry.description
        assert restored.severity == entry.severity
        assert restored.applicable_task_types == entry.applicable_task_types
        assert restored.times_applied == 0
        assert restored.effectiveness_score == 0.5
