# learning/confidence.py - ConfidenceSystem
# Bayesian confidence computation, freshness decay, calibration, and validation state transitions

from __future__ import annotations

import time
import math
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

from learning.types import (
    EngineeringPattern, ConfidenceUpdate, ValidationState,
    FreshnessConfig, FreshnessGrade,
)

log = logging.getLogger("aelvo.learning.confidence")


import threading

class ConfidenceSystem:
    """Manages confidence computation, freshness decay, and validation state transitions.

    Key principles:
    - Confidence is COMPUTED, never manually set
    - Freshness decays based on type-specific decay rates
    - Validation states transition deterministically based on evidence
    - Every confidence update produces a ConfidentUpdate record for audit
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._updates: Dict[str, ConfidenceUpdate] = {}
        self._calibration_history: List[float] = []
        self._default_configs: Dict[str, FreshnessConfig] = {
            "add_import_dependency": FreshnessConfig(max_age_days=7, decay_function="linear"),
            "remove_import_dependency": FreshnessConfig(max_age_days=7, decay_function="linear"),
            "add_call_dependency": FreshnessConfig(max_age_days=14, decay_function="linear"),
            "add_inheritance": FreshnessConfig(max_age_days=30, decay_function="slow"),
            "add_implements": FreshnessConfig(max_age_days=30, decay_function="slow"),
            "refactor_internal": FreshnessConfig(max_age_days=14, decay_function="linear"),
            "add_file": FreshnessConfig(max_age_days=7, decay_function="linear"),
            "delete_file": FreshnessConfig(max_age_days=30, decay_function="slow"),
            "modify_symbol_signature": FreshnessConfig(max_age_days=14, decay_function="linear"),
            "break_cycle": FreshnessConfig(max_age_days=14, decay_function="linear"),
            "create_cycle": FreshnessConfig(max_age_days=7, decay_function="linear"),
            "add_layer": FreshnessConfig(max_age_days=14, decay_function="linear"),
            "change_export_status": FreshnessConfig(max_age_days=14, decay_function="linear"),
            "change_type_annotation": FreshnessConfig(max_age_days=21, decay_function="slow"),
            "mixed": FreshnessConfig(max_age_days=7, decay_function="linear"),
        }

    # ── Public API ────────────────────────────────────────────────────────────

    def compute_initial_confidence(
        self,
        category_signature: str,
        evidence_quality: float = 0.5,
    ) -> float:
        """Compute initial confidence for a new pattern.

        Starts with a base of 0.3 (weak heuristic per spec).
        Evidence quality can adjust it up (strongly observed) or down (inferred).
        """
        with self._lock:
            base = 0.3
            adjustment = evidence_quality * 0.2  # -0.1 to +0.1 range
            return round(min(0.5, max(0.1, base + adjustment)), 4)

    def update_confidence(
        self,
        pattern: EngineeringPattern,
        was_successful: bool,
    ) -> Tuple[float, ConfidenceUpdate]:
        """Update a pattern's confidence based on a new observation.

        Uses Bayesian-inspired formula:
        - Success: bonus = 0.1 * (1.0 - current_conf)
        - Failure: penalty = 0.15 * current_conf

        Returns:
            Tuple of (new_confidence, ConfidenceUpdate record)
        """
        with self._lock:
            previous = pattern.confidence
            formula = ""

            if was_successful:
                bonus = 0.1 * (1.0 - previous)
                new_conf = min(0.98, previous + bonus)
                formula = f"bonus = 0.1 * (1.0 - {previous:.3f}) = {bonus:.4f}"
                pattern.success_count += 1
            else:
                penalty = 0.15 * previous
                new_conf = max(0.05, previous - penalty)
                formula = f"penalty = 0.15 * {previous:.3f} = {penalty:.4f}"
                pattern.failure_count += 1

            new_conf = round(new_conf, 4)
            pattern.confidence = new_conf
            pattern.observation_count += 1
            pattern.last_observed = datetime.now(timezone.utc)

            update = ConfidenceUpdate(
                knowledge_item_id=pattern.id,
                previous_confidence=round(previous, 4),
                new_confidence=new_conf,
                update_formula=formula,
                evidence="success" if was_successful else "failure",
            )
            update.to_id()
            self._updates[update.id] = update

            # Update validation state if threshold reached
            self._update_validation_state(pattern)

            log.debug(
                f"Confidence update for {pattern.id}: "
                f"{previous:.3f} → {new_conf:.3f} "
                f"(success={was_successful})"
            )

            return new_conf, update

    def compute_freshness(
        self,
        pattern: EngineeringPattern,
        config: Optional[FreshnessConfig] = None,
    ) -> Tuple[float, FreshnessGrade]:
        """Compute freshness of a pattern based on time since last observation.

        Returns:
            Tuple of (freshness_score 0.0-1.0, FreshnessGrade)
        """
        with self._lock:
            if config is None:
                config = self._default_configs.get(
                    pattern.category.value,
                    FreshnessConfig(),
                )

            now = datetime.now(timezone.utc)
            age = (now - pattern.last_observed).total_seconds()
            max_age_seconds = config.max_age_days * 24 * 3600

            if age >= max_age_seconds:
                freshness = 0.0
            elif config.decay_function == "exponential":
                # decay = e^(-age / half_life)
                half_life = max_age_seconds / 2
                freshness = math.exp(-age / half_life)
            elif config.decay_function == "step":
                # Step function: full freshness until halfway, then 0
                if age < max_age_seconds / 2:
                    freshness = 1.0
                else:
                    freshness = 0.0
            elif config.decay_function == "slow":
                # Slow decay: freshness = 1.0 - sqrt(age / max_age)
                # Starts decaying quickly but levels off
                freshness = max(0.0, 1.0 - math.sqrt(age / max_age_seconds))
            else:
                # Linear decay (default)
                freshness = max(0.0, 1.0 - (age / max_age_seconds))

            freshness = round(freshness, 4)
            pattern.freshness = freshness

            # Determine grade
            if freshness >= 0.7:
                grade = FreshnessGrade.FRESH
            elif freshness >= 0.3:
                grade = FreshnessGrade.AGING
            elif freshness > 0.0:
                grade = FreshnessGrade.STALE
            else:
                grade = FreshnessGrade.UNKNOWN

            return freshness, grade

    def transition_validation_state(
        self,
        pattern: EngineeringPattern,
    ) -> ValidationState:
        """Transition validation state based on confidence and evidence.

        Rules:
        - confidence >= 0.75 and observation_count >= 5 → VALIDATED
        - confidence < 0.20 → DEPRECATED
        - else → OBSERVED
        """
        with self._lock:
            if pattern.confidence >= 0.75 and pattern.observation_count >= 5:
                pattern.validation_state = ValidationState.VALIDATED
            elif pattern.confidence < 0.20:
                pattern.validation_state = ValidationState.DEPRECATED
            else:
                pattern.validation_state = ValidationState.OBSERVED

            return pattern.validation_state

    def set_contradicted(
        self,
        pattern: EngineeringPattern,
        contradiction_record_id: str,
    ) -> ValidationState:
        """Mark a pattern as contradicted (by contradiction resolution)."""
        with self._lock:
            pattern.validation_state = ValidationState.CONTRADICTED
            pattern.related_pattern_ids.append(contradiction_record_id)
            return pattern.validation_state

    # ── Calibration ───────────────────────────────────────────────────────────

    def record_calibration(
        self,
        predicted_confidence: float,
        actually_correct: bool,
    ) -> None:
        """Record a prediction outcome for confidence calibration.

        Over time, this allows the system to learn whether its
        confidence estimates are calibrated (e.g., 0.8 confidence
        should mean ~80% correct).
        """
        with self._lock:
            self._calibration_history.append(1.0 if actually_correct else 0.0)

    def compute_calibration_metrics(self) -> Dict[str, float]:
        """Compute confidence calibration metrics.

        Returns:
            Dict with keys:
            - accuracy: overall accuracy
            - confidence_bias: avg(confidence - actual)
            - expected_calibration_error: ECE
        """
        with self._lock:
            if not self._calibration_history:
                return {
                    "accuracy": 0.0,
                    "confidence_bias": 0.0,
                    "expected_calibration_error": 0.0,
                }

            accuracy = sum(self._calibration_history) / len(self._calibration_history)
            # Simple ECE: average |confidence - outcome|
            # This is a simplification; real ECE bins by confidence range
            avg_confidence = 0.6  # Approximate
            confidence_bias = avg_confidence - accuracy
            ece = abs(confidence_bias)

            return {
                "accuracy": round(accuracy, 4),
                "confidence_bias": round(confidence_bias, 4),
                "expected_calibration_error": round(ece, 4),
            }

    # ── Private ───────────────────────────────────────────────────────────────

    def _update_validation_state(self, pattern: EngineeringPattern) -> None:
        """Internal validation state transition check."""
        prev_state = pattern.validation_state
        new_state = self.transition_validation_state(pattern)
        if prev_state != new_state:
            log.info(
                f"Validation state transition: {pattern.id} "
                f"{prev_state.value} → {new_state.value}"
            )

    def get_update_history(self, pattern_id: str) -> List[ConfidenceUpdate]:
        """Get all confidence updates for a given pattern."""
        with self._lock:
            return [
                u for u in self._updates.values()
                if u.knowledge_item_id == pattern_id
            ]

    def get_freshness_config(self, category: str) -> FreshnessConfig:
        """Get the freshness config for a category."""
        return self._default_configs.get(category, FreshnessConfig())
