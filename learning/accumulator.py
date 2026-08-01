# learning/accumulator.py - PatternAccumulator
# Collects deltas by category signature, promotes to EngineeringPattern at threshold

from __future__ import annotations

import time
import logging
from typing import Callable, Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import threading

from learning.types import (
    EngineeringPattern, PatternObservation, DependencyGraphDelta,
    EditCategorySignature, SubgraphSpec, EditCategory,
    ValidationState, PatternQuery, PatternQueryResult,
    ConfidenceUpdate,
)
from learning.confidence import ConfidenceSystem
from learning.subgraph import SubgraphExtractor, SubgraphSimilarity
from learning.classifier import EditClassifier

log = logging.getLogger("aelvo.learning.accumulator")


class PatternAccumulator:
    """Collects dependency graph deltas grouped by structural signature and
    promotes them to EngineeringPattern instances when sufficient evidence
    accumulates.

    Accumulation thresholds (configurable):
    - min_observations_for_pattern: Minimum deltas with same signature → pattern candidate (default 3)
    - min_confidence_for_active: Pattern confidence must be >= this to be active (default 0.4)
    - max_patterns_per_category: Limit patterns per category to prevent bloat (default 50)
    """
    def __init__(
        self,
        min_observations_for_pattern: int = 3,
        min_confidence_for_active: float = 0.4,
        max_patterns_per_category: int = 50,
        classifier: Optional[EditClassifier] = None,
        extractor: Optional[SubgraphExtractor] = None,
        similarity: Optional[SubgraphSimilarity] = None,
        confidence: Optional[ConfidenceSystem] = None,
 ):
        self._patterns: Dict[str, EngineeringPattern] = {}
        self._observations: Dict[str, List[PatternObservation]] = defaultdict(list)
        self._delta_log: List[DependencyGraphDelta] = []
        self._signature_groups: Dict[str, List[Tuple[DependencyGraphDelta, SubgraphSpec, EditCategorySignature]]] = defaultdict(list)
        # O(1) reverse index: sig_hash → pattern_id
        self._sig_hash_to_pattern_id: Dict[str, str] = {}
        self._classifier = classifier or EditClassifier()
        self._extractor = extractor or SubgraphExtractor()
        self._similarity = similarity or SubgraphSimilarity()
        self._confidence = confidence or ConfidenceSystem()

        # Optional persistence callback (set by PatternExtractionEngine)
        self._persist_callback: Optional[Callable[[EngineeringPattern], None]] = None
        self._confidence_update_callback: Optional[Callable[[ConfidenceUpdate], None]] = None

        # Configuration
        self._min_observations = min_observations_for_pattern
        self._min_confidence = min_confidence_for_active
        self._max_per_category = max_patterns_per_category

        self._metrics: List[Dict] = []
        self._lock = threading.RLock()

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest(
        self,
        delta: DependencyGraphDelta,
        subgraph: SubgraphSpec,
        signature: EditCategorySignature,
        outcome: str = "success",
        task_description: str = "",
        source_specialist: Optional[str] = None,
        project_scope: Optional[str] = None,
    ) -> Optional[EngineeringPattern]:
        """Ingest a single delta observation into the accumulator.

        Steps:
        1. Produce a signature hash from the EditCategorySignature
        2. Group this observation with others of the same signature
        3. If the group size exceeds the threshold, create/promote a pattern
        4. Update the pattern's confidence based on the outcome

        Args:
            delta: The computed dependency graph delta.
            subgraph: The extracted minimal subgraph.
            signature: The classified edit category signature.
            outcome: "success" or "failure".
            task_description: Description of the task that produced this delta.
            source_specialist: Which specialist produced this observation.
            project_scope: Which project this observation belongs to.

        Returns:
            The EngineeringPattern (new, updated, or None if below threshold).
        """
        with self._lock:
            start = time.time()

            sig_hash = signature.signature_hash

            # Store the observation group
            self._delta_log.append(delta)
            self._signature_groups[sig_hash].append((delta, subgraph, signature))

            group_size = len(self._signature_groups[sig_hash])

            # Check if we have enough observations to create a pattern
            if group_size >= self._min_observations:
                pattern = self._create_or_update_pattern(
                    sig_hash=sig_hash,
                    signature=signature,
                    subgraph=subgraph,
                    outcome=outcome,
                    task_description=task_description,
                    source_specialist=source_specialist,
                    project_scope=project_scope,
                    group_size=group_size,
                )

                # Persist if callback is registered
                if pattern and self._persist_callback:
                    self._persist_callback(pattern)

                elapsed = (time.time() - start) * 1000
                self._record_metric("ingest", elapsed, pattern_id=pattern.id if pattern else None)
                return pattern

            # Not enough observations yet
            log.debug(
                f"Observation grouped under sig={sig_hash[:8]} "
                f"({group_size}/{self._min_observations} needed)"
            )
            elapsed = (time.time() - start) * 1000
            self._record_metric("ingest_below_threshold", elapsed)
            return None

    def set_persistence_callback(
        self, callback: Callable[[EngineeringPattern], None]
    ) -> None:
        """Register a callback invoked after each pattern is created/updated."""
        with self._lock:
            self._persist_callback = callback

    def set_confidence_update_callback(
        self, callback: Callable[[ConfidenceUpdate], None]
    ) -> None:
        """Register a callback invoked after each confidence update is generated."""
        with self._lock:
            self._confidence_update_callback = callback

    def get_pattern(self, pattern_id: str) -> Optional[EngineeringPattern]:
        with self._lock:
            return self._patterns.get(pattern_id)

    def query(self, query: PatternQuery) -> PatternQueryResult:
        """Query patterns by structured criteria.

        Args:
            query: PatternQuery with filters.

        Returns:
            PatternQueryResult with ranked matches.
        """
        with self._lock:
            results: List[EngineeringPattern] = []
            for pattern in self._patterns.values():
                if query.match(pattern):
                    results.append(pattern)

            results.sort(key=lambda p: p.confidence, reverse=True)
            return PatternQueryResult(
                query=query,
                patterns=results[:query.max_results],
                total_matched=len(results),
            )

    def get_patterns(self) -> List[EngineeringPattern]:
        """Get all patterns currently tracked."""
        with self._lock:
            return list(self._patterns.values())

    def get_patterns_by_category(
        self, category: EditCategory
    ) -> List[EngineeringPattern]:
        """Get all patterns of a given category."""
        with self._lock:
            return [p for p in self._patterns.values() if p.category == category]

    def get_observation_count(self, sig_hash: str) -> int:
        with self._lock:
            return len(self._signature_groups.get(sig_hash, []))

    def load_from_persistence(
        self, patterns: List[EngineeringPattern]
    ) -> int:
        """Load patterns from persistent storage into the accumulator.

        Populates the in-memory pattern store AND the reverse index
        so that future deltas matching a loaded pattern's signature
        correctly update the existing pattern rather than creating a
        duplicate.

        Args:
            patterns: List of patterns loaded from KnowledgeGraph.

        Returns:
            Number of patterns loaded.
        """
        with self._lock:
            count = 0
            skipped = 0
            for p in patterns:
                if p.id in self._patterns:
                    skipped += 1
                    continue
                self._patterns[p.id] = p
                # Rebuild reverse index from loaded pattern's signature hash
                sig_hash = p.category_signature.signature_hash
                existing_id = self._sig_hash_to_pattern_id.get(sig_hash)
                if existing_id and existing_id != p.id:
                    log.warning(
                        f"Signature hash collision during load: "
                        f"sig={sig_hash[:8]} maps to both {existing_id[:8]} "
                        f"and {p.id[:8]} — overwriting with {p.id[:8]}"
                    )
                    self._sig_hash_to_pattern_id[sig_hash] = p.id
                count += 1
            if skipped:
                log.debug(f"Skipped {skipped} duplicate patterns during load")
            log.info(f"Loaded {count} patterns from persistence")
            return count

    def flush(self) -> int:
        """Flush all accumulator patterns to persistence.

        Returns:
            Number of patterns flushed.
        """
        with self._lock:
            if not self._persist_callback:
                log.warning("No persistence callback registered — cannot flush")
                return 0

            count = 0
            for pattern in self._patterns.values():
                self._persist_callback(pattern)
                count += 1
            log.info(f"Flushed {count} patterns to persistence")
            return count

    def get_statistics(self) -> Dict:
        """Get aggregate statistics about accumulated patterns.

        Returns:
            Dict with keys: total_patterns, by_category, avg_confidence,
            total_observations, validated_count, deprecated_count.
        """
        with self._lock:
            total = len(self._patterns)
            if total == 0:
                return {
                    "total_patterns": 0,
                    "by_category": {},
                    "avg_confidence": 0.0,
                    "total_observations": 0,
                    "validated_count": 0,
                    "deprecated_count": 0,
                    "contradicted_count": 0,
                }

            by_category: Dict[str, int] = {}
            validated = 0
            deprecated = 0
            contradicted = 0
            total_obs = 0
            total_conf = 0.0

            for p in self._patterns.values():
                cat = p.category.value
                by_category[cat] = by_category.get(cat, 0) + 1
                if p.validation_state == ValidationState.VALIDATED:
                    validated += 1
                elif p.validation_state == ValidationState.DEPRECATED:
                    deprecated += 1
                elif p.validation_state == ValidationState.CONTRADICTED:
                    contradicted += 1
                total_obs += p.observation_count
                total_conf += p.confidence

            return {
                "total_patterns": total,
                "by_category": dict(sorted(by_category.items())),
                "avg_confidence": round(total_conf / total, 4),
                "total_observations": total_obs,
                "validated_count": validated,
                "deprecated_count": deprecated,
                "contradicted_count": contradicted,
            }

            # ── Pattern Creation / Update ─────────────────────────────────────────────

    def _create_or_update_pattern(
        self,
        sig_hash: str,
        signature: EditCategorySignature,
        subgraph: SubgraphSpec,
        outcome: str,
        task_description: str,
        source_specialist: Optional[str],
        project_scope: Optional[str],
        group_size: int,
    ) -> Optional[EngineeringPattern]:
        """Create a new pattern or update an existing one for a given signature."""
        with self._lock:
            existing = self._find_pattern_by_sig_hash(sig_hash)

            if existing:
                pattern = existing
                was_success = (outcome == "success")
                new_conf, update = self._confidence.update_confidence(pattern, was_success)
                self._confidence.compute_freshness(pattern)
                pattern.last_observed = datetime.now(timezone.utc)
                pattern.validation_state = self._confidence.transition_validation_state(pattern)
                if self._confidence_update_callback:
                    try:
                        self._confidence_update_callback(update)
                    except Exception as e:
                        log.warning("Failed to persist confidence update: %s", e)
            else:
                # Check per-category limit
                cat_count = sum(
                    1 for p in self._patterns.values()
                    if p.category == signature.category
                )
                if cat_count >= self._max_per_category:
                    log.warning(
                        f"Max patterns for category {signature.category.value} reached "
                        f"({cat_count}/{self._max_per_category}) — skipping"
                    )
                    return None

                # Create new pattern
                initial_conf = self._confidence.compute_initial_confidence(
                    category_signature=sig_hash,
                    evidence_quality=0.5,
                )

                # Check for isomorphic subgraphs in the same category
                for existing_pattern in self.get_patterns_by_category(signature.category):
                    if self._similarity.are_isomorphic(subgraph, existing_pattern.subgraph):
                        subgraph.is_isomorphic_to = existing_pattern.id
                        break

                pattern = EngineeringPattern(
                    category=signature.category,
                    category_signature=signature,
                    subgraph=subgraph,
                    confidence=initial_conf,
                    observation_count=group_size,
                    success_count=1 if outcome == "success" else 0,
                    failure_count=1 if outcome == "failure" else 0,
                    validation_state=ValidationState.OBSERVED,
                    freshness=1.0,
                    source_specialist=source_specialist,
                    project_scope=project_scope,
                    provenance=[task_description] if task_description else [],
                )
                pattern.to_digest()

                self._patterns[pattern.id] = pattern
                self._sig_hash_to_pattern_id[sig_hash] = pattern.id
                log.info(
                    f"New pattern: {pattern.id[:8]} "
                    f"category={pattern.category.value} "
                    f"sig={sig_hash[:8]} "
                    f"confidence={pattern.confidence:.3f}"
                )

            # Record observation
            observation = PatternObservation(
                pattern_id=pattern.id,
                delta_digest=_sha256_digest(outcome),
                outcome=outcome,
                task_description=task_description,
            )
            observation.to_id()
            self._observations[pattern.id].append(observation)

            if task_description and task_description not in pattern.provenance:
                pattern.provenance.append(task_description)

            return pattern

    def _find_pattern_by_sig_hash(self, sig_hash: str) -> Optional[EngineeringPattern]:
        """Find an existing pattern matching a signature hash via reverse index."""
        with self._lock:
            pattern_id = self._sig_hash_to_pattern_id.get(sig_hash)
            if pattern_id:
                return self._patterns.get(pattern_id)
            return None

            # ── Maintenance ───────────────────────────────────────────────────────────

    def prune_deprecated(
        self, max_age_days: int = 90, min_observations_for_retention: int = 1
    ) -> int:
        """Remove deprecated patterns that haven't been updated."""
        with self._lock:
            now = datetime.now(timezone.utc)
            to_prune: List[str] = []
            for pid, pattern in self._patterns.items():
                if pattern.validation_state in (ValidationState.DEPRECATED, ValidationState.CONTRADICTED):
                    age = (now - pattern.last_observed).total_seconds()
                    if age > max_age_days * 86400:
                        if pattern.observation_count < min_observations_for_retention:
                            to_prune.append(pid)
            for pid in to_prune:
                del self._patterns[pid]
                self._observations.pop(pid, None)
                # Clean up reverse index
                sig_hashes_to_remove = [
                    k for k, v in self._sig_hash_to_pattern_id.items() if v == pid
                ]
                for h in sig_hashes_to_remove:
                    del self._sig_hash_to_pattern_id[h]
            if to_prune:
                log.info(f"Pruned {len(to_prune)} deprecated patterns")
            return len(to_prune)

    def reset(self) -> None:
        with self._lock:
            self._patterns.clear()
            self._observations.clear()
            self._delta_log.clear()
            self._signature_groups.clear()
            self._sig_hash_to_pattern_id.clear()
            self._metrics.clear()

            # ── Metrics ───────────────────────────────────────────────────────────────

    def _record_metric(self, operation: str, duration_ms: float, **kwargs) -> None:
        with self._lock:
            record = {"operation": operation, "duration_ms": round(duration_ms, 2)}
            record.update(kwargs)
            self._metrics.append(record)

    def get_metrics(self) -> List[Dict]:
        return self._metrics.copy()


def _sha256_digest(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]
