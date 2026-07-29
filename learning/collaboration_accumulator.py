# learning/collaboration_accumulator.py - CollaborationAccumulator
# Phase 10: Extracts and accumulates collaboration patterns
# (consensus outcomes, blackboard publications, specialist interactions)

from __future__ import annotations

import time
import hashlib
import logging
import threading
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict
from datetime import datetime, timezone

from learning.types import (
    CollaborationObservation,
    CollaborationPattern,
    CollaborationSignature,
    CollaborationEventType,
    ConsensusOutcome,
    ConsensusMemoryRecord,
)

log = logging.getLogger("aelvo.learning.collaboration")

# ── Outcome classification constants ──────────────────────────────────────
# Using typed constants instead of raw strings prevents silent breakage
# if outcome conventions change.
_SUCCEEDED_OUTCOMES = frozenset({"success", "agreed"})
_FAILED_OUTCOMES = frozenset({"failure", "disagreed", "vetoed"})
_CONFLICT_OUTCOMES = frozenset({"disagreed", "vetoed", "failure"})


def _is_success(outcome: str) -> bool:
    """Check if an outcome string represents success."""
    return outcome in _SUCCEEDED_OUTCOMES


def _is_failure(outcome: str) -> bool:
    """Check if an outcome string represents failure."""
    return outcome in _FAILED_OUTCOMES


def _is_conflict(outcome: str) -> bool:
    """Check if an outcome string represents conflict."""
    return outcome in _CONFLICT_OUTCOMES


def _is_escalated(outcome: str) -> bool:
    return outcome == "escalated"


class CollaborationAccumulator:
    """Accumulates collaboration events and promotes them to patterns.

    Similar to PatternAccumulator but for collaboration data rather than
    dependency graph deltas. Tracks recurring collaboration structures:

    - Consensus patterns: which specialist pairs agree/disagree frequently
    - Collaboration sequences: typical workflows (e.g., FORGE→SENTINEL→ARCHITECT)
    - Conflict resolution patterns: how disagreements are typically resolved

    Accumulation thresholds:
    - min_observations_for_pattern: Minimum events of same signature → pattern (default 3)
    - min_confidence_for_active: Pattern confidence must be >= this to be active (default 0.4)
    - max_patterns: Limit total patterns to prevent bloat (default 100)
    """

    def __init__(
        self,
        min_observations_for_pattern: int = 3,
        min_confidence_for_active: float = 0.4,
        max_patterns: int = 100,
    ):
        self._patterns: Dict[str, CollaborationPattern] = {}
        self._observations: Dict[str, List[CollaborationObservation]] = defaultdict(list)
        self._sig_hash_to_pattern_id: Dict[str, str] = {}

        self._min_observations = min_observations_for_pattern
        self._min_confidence = min_confidence_for_active
        self._max_patterns = max_patterns

        self._persist_callback: Optional[Callable[[CollaborationPattern], None]] = None
        self._lock = threading.RLock()
        self._metrics: List[Dict] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest_consensus(
        self,
        record: ConsensusMemoryRecord,
    ) -> Optional[CollaborationPattern]:
        """Ingest a consensus outcome as a collaboration observation.

        Creates or updates a CollaborationPattern based on the
        signature of this consensus event.

        Args:
            record: The ConsensusMemoryRecord to learn from.

        Returns:
            CollaborationPattern (new, updated, or None if below threshold).
        """
        with self._lock:
            time.time()

            # Build an observation from the consensus record
            outcome_label = "success" if record.outcome in (
                ConsensusOutcome.AGREED, ConsensusOutcome.PARTIAL
            ) else "failure"

            observation = CollaborationObservation(
                collaboration_id=record.consensus_id,
                event_type=CollaborationEventType.CONSENSUS_REACHED,
                specialists_involved=record.specialists_involved,
                outcome=outcome_label,
                confidence=record.confidence,
                description=record.topic,
            )
            observation.to_id()

            # Build signature
            signature = CollaborationSignature(
                event_type=CollaborationEventType.CONSENSUS_REACHED,
                participant_count=record.participant_count,
                specialist_roles=sorted(record.specialists_involved),
                had_conflict=record.vetoed,
                required_architect_override=record.architect_override,
            )

            # Process through the standard pipeline
            return self._ingest(
                observation=observation,
                signature=signature,
                duration_ms=0.0,
            )

    def ingest_collaboration_event(
        self,
        event_type: CollaborationEventType,
        specialists_involved: List[str],
        outcome: str,
        confidence: float = 0.5,
        description: str = "",
        duration_ms: float = 0.0,
    ) -> Optional[CollaborationPattern]:
        """Ingest a raw collaboration event as an observation.

        Args:
            event_type: Type of collaboration event.
            specialists_involved: Which specialists participated.
            outcome: "success", "failure", "agreed", "disagreed", etc.
            confidence: Confidence in the outcome.
            description: Human-readable description.
            duration_ms: Duration of the collaboration step.

        Returns:
            CollaborationPattern or None if below threshold.
        """
        with self._lock:
            observation = CollaborationObservation(
                collaboration_id=self._generate_id("collab", description or str(event_type.value)),
                event_type=event_type,
                specialists_involved=specialists_involved,
                outcome=outcome,
                confidence=confidence,
                duration_ms=duration_ms,
                description=description,
            )
            observation.to_id()

            had_conflict = _is_conflict(outcome)
            signature = CollaborationSignature(
                event_type=event_type,
                participant_count=len(specialists_involved),
                specialist_roles=sorted(specialists_involved),
                had_conflict=had_conflict,
                required_architect_override=_is_escalated(outcome),
            )

            return self._ingest(
                observation=observation,
                signature=signature,
                duration_ms=duration_ms,
            )

    def set_persistence_callback(
        self, callback: Callable[[CollaborationPattern], None]
    ) -> None:
        """Register a callback invoked after each pattern is created/updated."""
        with self._lock:
            self._persist_callback = callback

    def get_pattern(self, pattern_id: str) -> Optional[CollaborationPattern]:
        with self._lock:
            return self._patterns.get(pattern_id)

    def get_patterns(self) -> List[CollaborationPattern]:
        with self._lock:
            return list(self._patterns.values())

    def get_patterns_by_type(
        self, event_type: CollaborationEventType
    ) -> List[CollaborationPattern]:
        with self._lock:
            return [
                p for p in self._patterns.values()
                if p.signature.event_type == event_type
            ]

    def get_observation_count(self, sig_hash: str) -> int:
        with self._lock:
            return len(self._observations.get(sig_hash, []))

    def get_statistics(self) -> Dict[str, Any]:
        """Get aggregate statistics about accumulated collaboration patterns."""
        with self._lock:
            total = len(self._patterns)
            if total == 0:
                return {
                    "total_patterns": 0,
                    "by_event_type": {},
                    "avg_confidence": 0.0,
                    "total_observations": 0,
                }

            by_type: Dict[str, int] = {}
            total_obs = 0
            total_conf = 0.0

            for p in self._patterns.values():
                et = p.signature.event_type.value
                by_type[et] = by_type.get(et, 0) + 1
                total_obs += p.observation_count
                total_conf += p.confidence

            return {
                "total_patterns": total,
                "by_event_type": dict(sorted(by_type.items())),
                "avg_confidence": round(total_conf / total, 4),
                "total_observations": total_obs,
            }

    def load_from_persistence(
        self, patterns: List[CollaborationPattern]
    ) -> int:
        """Load patterns from persistent storage."""
        with self._lock:
            count = 0
            for p in patterns:
                if p.id not in self._patterns:
                    self._patterns[p.id] = p
                sig_hash = p.signature.signature_hash
                existing_id = self._sig_hash_to_pattern_id.get(sig_hash)
                if existing_id and existing_id != p.id:
                    log.warning(
                        f"Signature hash collision during load: "
                        f"sig={sig_hash[:8]} maps to both "
                        f"{existing_id[:8]} and {p.id[:8]}"
                    )
                self._sig_hash_to_pattern_id[sig_hash] = p.id
                count += 1
            log.info(f"Loaded {count} collaboration patterns from persistence")
            return count

    def flush(self) -> int:
        """Flush all patterns to persistence."""
        with self._lock:
            if not self._persist_callback:
                log.warning("No persistence callback registered — cannot flush")
                return 0
            count = 0
            for pattern in self._patterns.values():
                self._persist_callback(pattern)
                count += 1
            log.info(f"Flushed {count} collaboration patterns to persistence")
            return count

    def get_metrics(self) -> List[Dict]:
        with self._lock:
            return self._metrics.copy()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _ingest(
        self,
        observation: CollaborationObservation,
        signature: CollaborationSignature,
        duration_ms: float,
    ) -> Optional[CollaborationPattern]:
        """Core ingest logic — shared by all ingestion entry points."""
        sig_hash = signature.signature_hash

        # Store observation
        self._observations[sig_hash].append(observation)
        group_size = len(self._observations[sig_hash])

        if group_size >= self._min_observations:
            pattern = self._create_or_update_pattern(
                sig_hash=sig_hash,
                signature=signature,
                observation=observation,
                group_size=group_size,
                duration_ms=duration_ms,
            )

            if pattern and self._persist_callback:
                try:
                    self._persist_callback(pattern)
                except Exception as e:
                    log.warning("Persistence callback failed: %s", e)

            return pattern

        log.debug(
            f"Collaboration observation grouped under sig={sig_hash[:8]} "
            f"({group_size}/{self._min_observations} needed)"
        )
        return None

    def _create_or_update_pattern(
        self,
        sig_hash: str,
        signature: CollaborationSignature,
        observation: CollaborationObservation,
        group_size: int,
        duration_ms: float,
    ) -> Optional[CollaborationPattern]:
        """Create or update a pattern for a given signature."""
        with self._lock:
            existing_id = self._sig_hash_to_pattern_id.get(sig_hash)
            existing = self._patterns.get(existing_id) if existing_id else None

            if existing:
                pattern = existing
                pattern.observation_count = group_size
                if _is_success(observation.outcome):
                    pattern.success_count += 1
                else:
                    pattern.failure_count += 1
                total = pattern.success_count + pattern.failure_count
                # Use same formula as creation: Beta(1,1) prior on success count
                # Confidence = min(0.9, 0.3 + success_rate * 0.6)
                success_rate = pattern.success_count / max(total, 1)
                pattern.confidence = min(0.9, 0.3 + success_rate * 0.6)
                total_duration = pattern.avg_duration_ms * (total - 1) + duration_ms
                pattern.avg_duration_ms = total_duration / total if total > 0 else 0.0
                pattern.last_observed = datetime.now(timezone.utc)
                if observation.description and observation.description not in pattern.provenance:
                    pattern.provenance.append(observation.description)
            else:
                # Check max pattern limit
                if len(self._patterns) >= self._max_patterns:
                    log.warning(
                        f"Max collaboration patterns reached "
                        f"({self._max_patterns}) — skipping"
                    )
                    return None

                success_rate = 1.0 if _is_success(observation.outcome) else 0.0
                confidence = min(0.9, 0.3 + success_rate * 0.6)
                pattern = CollaborationPattern(
                    signature=signature,
                    confidence=confidence,
                    observation_count=group_size,
                    success_count=1 if _is_success(observation.outcome) else 0,
                    failure_count=1 if _is_failure(observation.outcome) else 0,
                    avg_duration_ms=duration_ms,
                    provenance=[observation.description] if observation.description else [],
                )
                pattern.to_digest()
                self._patterns[pattern.id] = pattern
                self._sig_hash_to_pattern_id[sig_hash] = pattern.id

                log.info(
                    f"New collaboration pattern: {pattern.id[:8]} "
                    f"type={signature.event_type.value} "
                    f"confidence={pattern.confidence:.3f}"
                )

            return pattern

    def _generate_id(self, prefix: str, content: str) -> str:
        raw = f"{prefix}_{content}_{time.time()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
