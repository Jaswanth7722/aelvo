# learning/engine.py - PatternExtractionEngine
# Orchestrates the full pipeline: capture → classify → extract → accumulate → persist

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Set
from datetime import datetime, timezone

from learning.types import (
    DependencyGraphDelta,
    EngineeringPattern, ValidationState, PatternQuery, PatternQueryResult,
    DeltaSource, ContradictionRecord,
)
from learning.delta import DeltaComputer
from learning.classifier import EditClassifier
from learning.subgraph import SubgraphExtractor, SubgraphSimilarity
from learning.confidence import ConfidenceSystem
from learning.accumulator import PatternAccumulator
from learning.knowledge_graph import KnowledgeGraph
from learning.analytics import AnalyticsEngine

log = logging.getLogger("aelvo.learning.engine")


class PatternExtractionEngine:
    """Orchestrates the full pattern extraction pipeline.

    Flows:
    1. Raw execution events arrive → DeltaComputer computes graph deltas
    2. Deltas → EditClassifier classifies the edit category
    3. Deltas → SubgraphExtractor extracts minimal affected subgraph
    4. Classified deltas → PatternAccumulator creates/updates patterns
    5. Accumulated patterns → KnowledgeGraph persists to SQLite
    6. KnowledgeGraph → Query API for specialists to retrieve patterns

    Integration points:
    - Receives GraphSnapshots from RepoIntelligenceEngine
    - Publishes learned patterns for injection into specialist context
    """

    def __init__(
        self,
        knowledge_graph: Optional[KnowledgeGraph] = None,
        delta_computer: Optional[DeltaComputer] = None,
        classifier: Optional[EditClassifier] = None,
        extractor: Optional[SubgraphExtractor] = None,
        similarity: Optional[SubgraphSimilarity] = None,
        confidence: Optional[ConfidenceSystem] = None,
        accumulator: Optional[PatternAccumulator] = None,
    ):
        # Core subsystems
        self.delta_computer = delta_computer or DeltaComputer()
        self.classifier = classifier or EditClassifier()
        self.extractor = extractor or SubgraphExtractor()
        self.similarity = similarity or SubgraphSimilarity()
        self.confidence = confidence or ConfidenceSystem()

        # Accumulator — the in-memory pattern store
        self.accumulator = accumulator or PatternAccumulator(
            classifier=self.classifier,
            extractor=self.extractor,
            similarity=self.similarity,
            confidence=self.confidence,
        )

        # Knowledge graph — persistent storage
        self.knowledge_graph = knowledge_graph
        # Wire persistence callback
        if self.knowledge_graph:
            self.accumulator.set_persistence_callback(
                self.knowledge_graph.save_pattern
            )
            self.accumulator.set_confidence_update_callback(
                self.knowledge_graph.save_confidence_update
            )

        # Callbacks
        self._pattern_created_callbacks: List[Callable] = []
        self._pattern_updated_callbacks: List[Callable] = []
        self._contradiction_callbacks: List[Callable] = []

        # Analytics engine
        self.analytics = AnalyticsEngine()

        # Thread safety
        self._lock = threading.RLock()

        # Runtime state
        self._is_running = False
        self._session_id: Optional[str] = None
        self._metrics: List[Dict] = []
        self._total_deltas_processed = 0
        self._contradictions_detected = 0
        self._known_pattern_ids: Set[str] = set()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start_session(self, session_id: str) -> None:
        """Start a new learning session. Loads previous patterns from persistence."""
        with self._lock:
            self._session_id = session_id
            self._is_running = True

            # Clear known pattern IDs from any previous session to prevent
            # cross-session state leaks
            self._known_pattern_ids.clear()

            if self.knowledge_graph:
                persistent_patterns = self.knowledge_graph.load_patterns(
                    min_confidence=0.3, limit=500,
                )
                self.accumulator.load_from_persistence(persistent_patterns)

                # Track loaded pattern IDs to prevent incorrect "created" callbacks
                for p in persistent_patterns:
                    self._known_pattern_ids.add(p.id)

                _ = self.knowledge_graph.load_session_checkpoint(session_id)

            self.analytics.start_session(session_id)

            pattern_count = len(self.accumulator.get_patterns())
            log.info(
                f"Learning session started: {session_id} "
                f"({pattern_count} patterns loaded)"
            )

    def end_session(self) -> None:
        """End the learning session, flushing state to persistence."""
        with self._lock:
            if self.knowledge_graph and self._session_id:
                self.accumulator.flush()
                stats = self.accumulator.get_statistics()
                self.knowledge_graph.save_session_checkpoint(self._session_id, stats)
            else:
                stats = self.accumulator.get_statistics()

            session_record = self.analytics.end_session(stats)

            self._is_running = False
            self._known_pattern_ids.clear()
            log.info(
                f"Learning session ended: {self._session_id} "
                f"({self._total_deltas_processed} deltas processed, "
                f"first-attempt success: {session_record.first_attempt_success_rate:.0%})"
            )
            self._session_id = None

    def reset(self) -> None:
        """Reset the engine to its initial state."""
        with self._lock:
            self._is_running = False
            self._session_id = None
            self._metrics.clear()
            self._total_deltas_processed = 0
            self._contradictions_detected = 0
            self._known_pattern_ids.clear()

    # ── Core Pipeline ─────────────────────────────────────────────────────────

    def process_graph_transition(
        self,
        before_snapshot,
        after_snapshot,
        source: Optional[DeltaSource] = None,
    ) -> Optional[EngineeringPattern]:
        """Process a transition between two dependency graph snapshots.

        The full pipeline:
        1. DeltaComputer computes the structured diff
        2. EditClassifier classifies the edit category
        3. SubgraphExtractor extracts the minimal affected subgraph
        4. PatternAccumulator creates or updates a pattern
        5. Contradiction detection runs against existing patterns

        Args:
            before_snapshot: GraphSnapshort — graph state before the task.
            after_snapshot: GraphSnapshort — graph state after the task.
            source: Optional DeltaSource metadata.

        Returns:
            Created/updated EngineeringPattern, or None if no learning signal.
        """
        with self._lock:
            if not self._is_running:
                raise RuntimeError(
                    "PatternExtractionEngine is not running. Call start_session() first."
                )

            start = time.time()

            # 1. Compute delta
            delta = self.delta_computer.compute(before_snapshot, after_snapshot, source)
            if delta.is_empty:
                log.debug("Empty delta — no learning signal")
                return None

            self._total_deltas_processed += 1
            specialist_name = source.specialist if source else None
            self.analytics.record_delta_processed(specialist_name)

            # 2. Classify the edit category
            signature = self.classifier.classify(delta)

            # 3. Extract minimal affected subgraph
            subgraph = self.extractor.extract(delta, after_graph=after_snapshot)

            # 4. Ingest into accumulator
            task_desc = source.task_description if source else ""
            outcome = source.outcome if source else "success"
            specialist = source.specialist if source else None
            project = source.project if source else None

            pattern = self.accumulator.ingest(
                delta=delta,
                subgraph=subgraph,
                signature=signature,
                outcome=outcome,
                task_description=task_desc,
                source_specialist=specialist,
                project_scope=project,
            )

            if pattern:
                is_new = pattern.id not in self._known_pattern_ids
                if is_new:
                    self._known_pattern_ids.add(pattern.id)
                    self._fire_callbacks(self._pattern_created_callbacks, pattern, source)
                    self.analytics.record_pattern_created()
                else:
                    self._fire_callbacks(self._pattern_updated_callbacks, pattern, source)
                    self.analytics.record_pattern_updated()

                # 5. Check contradictions
                self._check_contradictions(pattern, delta, source)

            elapsed = (time.time() - start) * 1000
            self._record_metric("process_graph_transition", elapsed, {
                "has_pattern": pattern is not None,
                "pattern_id": pattern.id[:8] if pattern else None,
                "category": pattern.category.value if pattern else None,
            })

            return pattern

    def process_execution_event(
        self,
        task_id: str,
        before_snapshot,
        after_snapshot,
        specialist: str = "",
        project: str = "",
        outcome: str = "success",
        execution_duration_ms: float = 0.0,
        task_description: str = "",
    ) -> Optional[EngineeringPattern]:
        """Process a task execution event through the full pipeline.

        Convenience wrapper over process_graph_transition.
        """
        with self._lock:
            source = DeltaSource(
                task_id=task_id,
                specialist=specialist,
                project=project,
                outcome=outcome,
                execution_duration_ms=execution_duration_ms,
                task_description=task_description,
            )
            return self.process_graph_transition(before_snapshot, after_snapshot, source)

    # ── Contradiction Detection ───────────────────────────────────────────────

    def _check_contradictions(
        self,
        pattern: EngineeringPattern,
        delta: DependencyGraphDelta,
        source: Optional[DeltaSource] = None,
    ) -> None:
        """Check if a new/updated pattern contradicts any existing pattern.

        Detects scope conflicts: same category, same project, different structure.
        """
        with self._lock:
            for existing in self.accumulator.get_patterns_by_category(pattern.category):
                if existing.id == pattern.id:
                    continue

                if (
                    existing.project_scope == pattern.project_scope
                    and existing.category_signature.signature_hash
                    != pattern.category_signature.signature_hash
                ):
                    sim_score = self.similarity.compute(existing.subgraph, pattern.subgraph)

                    if 0.4 <= sim_score <= 0.75:
                        record = ContradictionRecord(
                            old_knowledge_id=existing.id,
                            new_knowledge_id=pattern.id,
                            contradiction_type="scope_conflict",
                            resolution_strategy="retain_both_with_scope",
                            reasoning=(
                                f"Same category ({pattern.category.value}), same project "
                                f"({pattern.project_scope}), but different structural "
                                f"signatures (similarity={sim_score:.2f}). "
                                f"Both retained with scope qualifiers."
                            ),
                            resolved=True,
                        )
                        record.to_id()

                        if self.knowledge_graph:
                            self.knowledge_graph.save_contradiction(record)

                        self._contradictions_detected += 1
                        self.analytics.record_contradiction()
                        log.info(
                            f"Contradiction: {existing.id[:8]} ↔ "
                            f"{pattern.id[:8]} (sim={sim_score:.2f})"
                        )

    # ── Query API ─────────────────────────────────────────────────────────────

    def query_patterns(self, query: PatternQuery) -> PatternQueryResult:
        """Query patterns from both accumulator and knowledge graph.

        Returns the best available results, preferring the in-memory
        accumulator as it's most current.
        """
        with self._lock:
            result = self.accumulator.query(query)

            # If accumulator has insufficient results, fall back to knowledge graph
            if len(result.patterns) < min(query.max_results, 5) and self.knowledge_graph:
                kg_result = self.knowledge_graph.query(query)
                existing_ids = {p.id for p in result.patterns}
                for p in kg_result.patterns:
                    if p.id not in existing_ids:
                        result.patterns.append(p)
                        existing_ids.add(p.id)
                result.total_matched = max(result.total_matched, kg_result.total_matched)

            return result

    def get_pattern(self, pattern_id: str) -> Optional[EngineeringPattern]:
        """Get a single pattern by ID (accumulator first, then KG)."""
        pattern = self.accumulator.get_pattern(pattern_id)
        if not pattern and self.knowledge_graph:
            pattern = self.knowledge_graph.load_pattern(pattern_id)
        return pattern

    def get_patterns_for_context(
        self,
        project: Optional[str] = None,
        specialist: Optional[str] = None,
        max_tokens: int = 2000,
    ) -> List[EngineeringPattern]:
        """Get relevant patterns for injection into specialist context.

        This is the method that specialists call to get learned patterns.
        Returns high-confidence, fresh patterns filtered by project/specialist.
        """
        with self._lock:
            query = PatternQuery(
                min_confidence=0.5,
                validation_state=ValidationState.VALIDATED,
                project_scope=project,
                max_results=20,
            )
            result = self.query_patterns(query)

            if specialist:
                result.patterns = [
                    p for p in result.patterns
                    if p.source_specialist is None or p.source_specialist == specialist
                ]

            max_patterns = max(1, max_tokens // 100)
            selected = result.patterns[:max_patterns]
            now = datetime.now(timezone.utc)
            for p in selected:
                p.last_used = now
                if self.knowledge_graph:
                    try:
                        self.knowledge_graph.save_pattern(p)
                    except Exception as e:
                        log.debug("Failed to persist pattern last_used update: %s", e)
            return selected

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def on_pattern_created(self, callback: Callable) -> None:
        self._pattern_created_callbacks.append(callback)

    def on_pattern_updated(self, callback: Callable) -> None:
        self._pattern_updated_callbacks.append(callback)

    def on_contradiction(self, callback: Callable) -> None:
        self._contradiction_callbacks.append(callback)

    def _fire_callbacks(
        self,
        callbacks: List[Callable],
        pattern: EngineeringPattern,
        source: Optional[DeltaSource] = None,
    ) -> None:
        for cb in callbacks:
            try:
                cb(pattern, source)
            except Exception as e:
                log.warning(f"Callback error: {e}")

    # ── Analytics ─────────────────────────────────────────────────────────────

    def get_learning_statistics(self) -> Dict[str, Any]:
        """Get comprehensive learning statistics."""
        stats = self.accumulator.get_statistics()

        calibration = self.confidence.compute_calibration_metrics()
        stats.update({
            "calibration_accuracy": calibration["accuracy"],
            "calibration_bias": calibration["confidence_bias"],
            "calibration_ece": calibration["expected_calibration_error"],
            "total_deltas_processed": self._total_deltas_processed,
            "contradictions_detected": self._contradictions_detected,
            "session_active": self._is_running,
            "session_id": self._session_id,
        })

        # Add analytics data
        session_report = self.get_session_report()
        if session_report:
            stats["session_analytics"] = session_report

        if self.knowledge_graph:
            stats["knowledge_graph"] = self.knowledge_graph.get_statistics()

        return stats

    # ── Analytics Integration ────────────────────────────────────────────────

    def record_first_attempt(
        self,
        specialist: str,
        task_description: str,
        succeeded: bool,
        pattern_id: Optional[str] = None,
        confidence_at_time: float = 0.0,
    ) -> Any:
        """Record a first-attempt outcome for analytics tracking.

        Args:
            specialist: Which specialist made the attempt.
            task_description: What task was attempted.
            succeeded: Whether the first attempt succeeded.
            pattern_id: Optional ID of the pattern used.
            confidence_at_time: The pattern's confidence at time of attempt.

        Returns:
            The FirstAttemptRecord or None if no active session.
        """
        with self._lock:
            if not self._is_running:
                return None
            return self.analytics.record_first_attempt(
                specialist=specialist,
                task_description=task_description,
                succeeded=succeeded,
                pattern_id=pattern_id,
                confidence_at_time=confidence_at_time,
            )

    def get_session_report(self) -> Optional[Dict[str, Any]]:
        """Get the current session's analytics report."""
        if not self._session_id:
            return None
        record = self.analytics.get_session_report(self._session_id)
        if not record:
            return None
        return record.model_dump()

    def get_analytics_report(
        self,
        include_calibration: bool = True,
        include_trends: bool = True,
        include_learning_curves: bool = True,
    ) -> Dict[str, Any]:
        """Generate a comprehensive analytics report.

        Includes session-level metrics, trend analysis, confidence
        calibration tracking, and per-specialist learning curves.

        Args:
            include_calibration: Include confidence calibration.
            include_trends: Include across-session trend analysis.
            include_learning_curves: Include per-specialist learning curves.

        Returns:
            Comprehensive analytics report dict.
        """
        with self._lock:
            return self.analytics.generate_analytics_report(
                include_calibration=include_calibration,
                include_trends=include_trends,
                include_learning_curves=include_learning_curves,
            )

    def get_specialist_learning_curve(
        self, specialist: str
    ) -> Dict[str, Any]:
        """Get the learning curve for a specific specialist."""
        with self._lock:
            curve = self.analytics.compute_specialist_learning_curve(specialist)
            return curve.model_dump()

    def _record_metric(
        self, operation: str, duration_ms: float, extra: Optional[Dict] = None
    ) -> None:
        metric = {"operation": operation, "duration_ms": round(duration_ms, 2)}
        if extra:
            metric.update(extra)
        self._metrics.append(metric)

    def get_metrics(self) -> List[Dict]:
        return self._metrics.copy()
