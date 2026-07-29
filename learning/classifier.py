# learning/classifier.py - EditClassifier
# Maps graph deltas to structured EditCategory classifications

from __future__ import annotations

import time
import logging
from typing import Dict, List, Optional, Set
from collections import Counter

from repo_intelligence.types import EdgeType, SymbolKind
from learning.types import (
    EditCategory, EditCategorySignature, DependencyGraphDelta,
)

log = logging.getLogger("aelvo.learning.classifier")


import threading

class EditClassifier:
    """Classifies dependency graph deltas into structured EditCategory values.

    The classifier examines the delta's edge changes, file changes,
    and cycle changes to determine what kind of structural edit
    occurred. Classification is deterministic — the same delta always
    produces the same category.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._metrics: List[Dict] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def classify(self, delta: DependencyGraphDelta) -> EditCategorySignature:
        """Classify a delta into its structural edit category.

        Args:
            delta: The computed dependency graph delta.

        Returns:
            An EditCategorySignature with the determined category and
            structural fingerprint.
        """
        with self._lock:
            start = time.time()

            # Rule-based classification in order of specificity
            category = self._determine_category(delta)
            signature = EditCategorySignature(
                category=category,
                dominant_edge_type=delta.dominant_edge_type,
                file_count_delta=delta.file_count_delta,
                edge_count_delta=delta.edge_count_delta,
                cycle_introduced=len(delta.new_cycles) > 0,
                cycle_resolved=len(delta.resolved_cycles) > 0,
                added_symbol_kinds=self._extract_added_symbol_kinds(delta),
                topological_position=self._determine_topological_position(delta),
            )

            elapsed = (time.time() - start) * 1000
            self._record_metric("classify", elapsed)
            log.debug(f"Classified delta as {category.value} (sig={signature.signature_hash[:8]})")
            return signature

    # ── Rule-Based Classification ─────────────────────────────────────────────

    def _determine_category(self, delta: DependencyGraphDelta) -> EditCategory:
        """Apply rule chain to determine edit category.

        Rules are ordered from most specific to most general.
        The first matching rule wins.
        """
        new_types = Counter(e.edge_type for e in delta.new_edges)
        removed_types = Counter(e.edge_type for e in delta.removed_edges)
        all_new_types = set(new_types.keys())
        all_removed_types = set(removed_types.keys())

        # ── Cycle changes take priority ───────────────────────────────────

        if len(delta.new_cycles) > 0:
            # Introducing a cycle is usually an error pattern
            return EditCategory.CREATE_CYCLE

        if len(delta.resolved_cycles) > 0:
            return EditCategory.BREAK_CYCLE

        # ── File-level changes ────────────────────────────────────────────

        if len(delta.added_files) > 0 and delta.file_count_delta > 0:
            n_imports = new_types.get(EdgeType.IMPORTS, 0)
            n_defines = new_types.get(EdgeType.DEFINES, 0)
            if n_imports > n_defines:
                return EditCategory.ADD_LAYER
            return EditCategory.ADD_FILE

        if len(delta.removed_files) > 0 and delta.file_count_delta < 0:
            return EditCategory.DELETE_FILE

        # ── Edge-level changes ────────────────────────────────────────────

        has_imports = EdgeType.IMPORTS in all_new_types or EdgeType.IMPORTS in all_removed_types
        has_calls = EdgeType.CALLS in all_new_types or EdgeType.CALLS in all_removed_types
        has_inherits = EdgeType.INHERITS in all_new_types
        has_implements = EdgeType.IMPLEMENTS in all_new_types

        # New import edges
        if (
            EdgeType.IMPORTS in all_new_types
            and not has_inherits
            and not has_implements
            and not has_calls
        ):
            if EdgeType.IMPORTS in all_removed_types:
                # Both added and removed imports = migration pattern
                return EditCategory.REFACTOR_INTERNAL
            return EditCategory.ADD_IMPORT_DEPENDENCY

        # Removed import edges (without adds)
        if (
            EdgeType.IMPORTS in all_removed_types
            and EdgeType.IMPORTS not in all_new_types
        ):
            return EditCategory.REMOVE_IMPORT_DEPENDENCY

        # New call edges
        if EdgeType.CALLS in all_new_types and not has_imports:
            return EditCategory.ADD_CALL_DEPENDENCY

        # New inheritance
        if has_inherits and not has_implements:
            return EditCategory.ADD_INHERITANCE

        # New implements
        if has_implements:
            return EditCategory.ADD_IMPLEMENTS

        # ── Internal changes ──────────────────────────────────────────────

        if len(delta.modified_files) > 0 and not delta.has_structural_change:
            return EditCategory.REFACTOR_INTERNAL

        # ── No clear category → mixed ────────────────────────────────────

        if delta.has_structural_change:
            return EditCategory.MIXED

        return EditCategory.REFACTOR_INTERNAL

    # ── Signature Enrichment ──────────────────────────────────────────────────

    def _extract_added_symbol_kinds(
        self, delta: DependencyGraphDelta
    ) -> List[SymbolKind]:
        """Extract the kinds of symbols that were added (e.g., class, function)."""
        # In a full implementation, we'd look up the actual symbols from
        # the after-snapshot. For now, we infer from edge types.
        kind_map = {
            EdgeType.INHERITS: SymbolKind.CLASS,
            EdgeType.IMPLEMENTS: SymbolKind.CLASS,
            EdgeType.CALLS: SymbolKind.FUNCTION,
        }
        kinds: Set[SymbolKind] = set()
        for edge in delta.new_edges:
            if edge.edge_type in kind_map:
                kinds.add(kind_map[edge.edge_type])
        return list(kinds)

    def _determine_topological_position(
        self, delta: DependencyGraphDelta
    ) -> Optional[str]:
        """Determine the topological position of the change.

        'entry': Change is near the top of the dependency order (few dependents)
        'leaf': Change is near the bottom (many dependents)
        'middle': Somewhere in between
        """
        if len(delta.new_edges) == 0 and len(delta.modified_files) == 0:
            return None

        if delta.topological_shift > 2:
            return "leaf"
        elif delta.topological_shift < -2:
            return "entry"

        # Check edge types as secondary heuristic
        edge_types = [e.edge_type for e in delta.new_edges]
        if EdgeType.INHERITS in edge_types or EdgeType.IMPLEMENTS in edge_types:
            return "entry"

        return "middle"

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        with self._lock:
            self._metrics.append({
                "operation": operation,
                "duration_ms": round(duration_ms, 2),
            })

    def get_metrics(self) -> List[Dict]:
        with self._lock:
            return self._metrics.copy()
