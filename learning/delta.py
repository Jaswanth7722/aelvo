# learning/delta.py - DeltaComputer
# Computes structured differences between two GraphSnapshots

from __future__ import annotations

import time
import logging
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict

from repo_intelligence.types import (
    GraphSnapshot, SymbolEdge, EdgeType, ConfidenceLevel,
    DependencyGraphSnapshot, ParsedFile,
)
from learning.types import (
    GraphDeltaEdge, DependencyGraphDelta, DeltaSource,
)

log = logging.getLogger("aelvo.learning.delta")


import threading

class DeltaComputer:
    """Computes structured differences between dependency graph snapshots.

    Given a "before" and "after" GraphSnapshot (symbol graph), this engine
    produces a DependencyGraphDelta that captures:
    - New and removed edges (typed by EdgeType)
    - Added, removed, and modified files
    - New and resolved dependency cycles
    - Topological order shifts
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._metrics: List[Dict] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def compute(
        self,
        before: GraphSnapshot,
        after: GraphSnapshot,
        source: Optional[DeltaSource] = None,
    ) -> DependencyGraphDelta:
        """Compute the delta between two graph snapshots.

        Args:
            before: The symbol graph state before the task.
            after: The symbol graph state after the task.
            source: Optional metadata about the delta's origin.

        Returns:
            A DependencyGraphDelta with all structural changes.
        """
        with self._lock:
            start = time.time()

            before_files = set(before.files.keys())
            after_files = set(after.files.keys())

            added_files = list(after_files - before_files)
            removed_files = list(before_files - after_files)
            modified_files = self._find_modified_files(before, after, added_files, removed_files)

            new_edges, removed_edges = self._compute_edge_delta(before, after)

            before_cycles = self._extract_cycles_from_graph(before)
            after_cycles = self._extract_cycles_from_graph(after)
            new_cycles, resolved_cycles = self._compute_cycle_delta(
                before_cycles, after_cycles
            )

            topo_shift = len(modified_files) + len(added_files)

            delta = DependencyGraphDelta(
                before_version=before.version,
                after_version=after.version,
                new_edges=new_edges,
                removed_edges=removed_edges,
                added_files=[before.files[fid].file_path for fid in added_files if fid in before.files]
                            or [after.files[fid].file_path for fid in added_files if fid in after.files],
                removed_files=[before.files[fid].file_path for fid in removed_files if fid in before.files],
                modified_files=[after.files[fid].file_path for fid in modified_files if fid in after.files]
                               or [before.files[fid].file_path for fid in modified_files if fid in before.files],
                new_cycles=list(new_cycles),
                resolved_cycles=list(resolved_cycles),
                topological_shift=topo_shift,
                edge_count_delta=len(new_edges) - len(removed_edges),
                file_count_delta=len(added_files) - len(removed_files),
            )

            elapsed = (time.time() - start) * 1000
            self._record_metric("compute_delta", elapsed)

            if not delta.is_empty:
                log.info(
                    f"Delta: +{len(new_edges)}/-{len(removed_edges)} edges, "
                    f"+{len(added_files)}/-{len(removed_files)} files, "
                    f"{len(modified_files)} modified "
                    f"({elapsed:.1f}ms)"
                )

            return delta

    def compute_lightweight(
        self,
        before: DependencyGraphSnapshot,
        after: DependencyGraphSnapshot,
    ) -> DependencyGraphDelta:
        """Lightweight delta from DependencyGraphSnapshot (file-level only).

        Useful when only file-level dependency changes are needed
        (no symbol-level resolution).
        """
        with self._lock:
            start = time.time()

            before_files = set(before.dependencies.keys())
            after_files = set(after.dependencies.keys())

            added_files = list(after_files - before_files)
            removed_files = list(before_files - after_files)
            modified_files = self._find_modified_dep_files(before, after, added_files, removed_files)

            new_deps, removed_deps = self._compute_dep_edge_delta(before, after)

            before_cycles = before.cycles
            after_cycles = after.cycles
            new_cycles, resolved_cycles = self._compute_cycle_delta(
                [set(c) for c in before_cycles],
                [set(c) for c in after_cycles],
            )

            delta = DependencyGraphDelta(
                before_version=before.version,
                after_version=after.version,
                added_files=list(added_files),
                removed_files=list(removed_files),
                modified_files=list(modified_files),
                new_cycles=list(new_cycles),
                resolved_cycles=list(resolved_cycles),
                topological_shift=len(modified_files),
                edge_count_delta=len(new_deps) - len(removed_deps),
                file_count_delta=len(added_files) - len(removed_files),
            )

            elapsed = (time.time() - start) * 1000
            self._record_metric("compute_lightweight", elapsed)
            return delta

    # ── Edge Delta Computation ───────────────────────────────────────────────

    def _compute_edge_delta(
        self,
        before: GraphSnapshot,
        after: GraphSnapshot,
    ) -> Tuple[List[GraphDeltaEdge], List[GraphDeltaEdge]]:
        """Compute added and removed edges between two symbol graph snapshots."""
        before_edges = self._index_edges(before)
        after_edges = self._index_edges(after)

        added_keys = set(after_edges.keys()) - set(before_edges.keys())
        removed_keys = set(before_edges.keys()) - set(after_edges.keys())

        new_edges: List[GraphDeltaEdge] = []
        for key in added_keys:
            edge = after_edges[key]
            new_edges.append(self._edge_to_delta(edge, after))

        removed_edges: List[GraphDeltaEdge] = []
        for key in removed_keys:
            edge = before_edges[key]
            removed_edges.append(self._edge_to_delta(edge, before))

        return new_edges, removed_edges

    def _compute_dep_edge_delta(
        self,
        before: DependencyGraphSnapshot,
        after: DependencyGraphSnapshot,
    ) -> Tuple[List[GraphDeltaEdge], List[GraphDeltaEdge]]:
        """Compute added and removed dependency edges between dep graph snapshots."""
        new_edges: List[GraphDeltaEdge] = []
        removed_edges: List[GraphDeltaEdge] = []

        # Build edge sets from dependency dicts
        def build_edge_set(deps):
            result = set()
            for source, targets in deps.items():
                for target in targets:
                    result.add((source, target))
            return result

        before_set = build_edge_set(before.dependencies)
        after_set = build_edge_set(after.dependencies)

        added = after_set - before_set
        removed = before_set - after_set

        for src, tgt in added:
            new_edges.append(GraphDeltaEdge(
                edge_type=EdgeType.IMPORTS,
                source_file_id=src,
                target_file_id=tgt,
            ))

        for src, tgt in removed:
            removed_edges.append(GraphDeltaEdge(
                edge_type=EdgeType.IMPORTS,
                source_file_id=src,
                target_file_id=tgt,
            ))

        return new_edges, removed_edges

    # ── Cycle Delta ──────────────────────────────────────────────────────────

    def _compute_cycle_delta(
        self,
        before_cycles: List[Set[str]],
        after_cycles: List[Set[str]],
    ) -> Tuple[List[Set[str]], List[Set[str]]]:
        """Find new and resolved cycles."""
        before_set = set(frozenset(c) for c in before_cycles)
        after_set = set(frozenset(c) for c in after_cycles)

        new_cycles = after_set - before_set
        resolved_cycles = before_set - after_set

        return [set(c) for c in new_cycles], [set(c) for c in resolved_cycles]

    # ── File Change Detection ────────────────────────────────────────────────

    def _find_modified_files(
        self,
        before: GraphSnapshot,
        after: GraphSnapshot,
        added_files: List[str],
        removed_files: List[str],
    ) -> List[str]:
        """Detect files that changed between snapshots (excluding adds/removes)."""
        modified: List[str] = []
        for fid in before.files:
            if fid in added_files or fid in removed_files:
                continue
            if fid in after.files:
                before_fp = before.files[fid].fingerprint
                after_fp = after.files[fid].fingerprint
                if before_fp != after_fp:
                    modified.append(fid)
        return modified

    def _find_modified_dep_files(
        self,
        before: DependencyGraphSnapshot,
        after: DependencyGraphSnapshot,
        added_files: List[str],
        removed_files: List[str],
    ) -> List[str]:
        """Detect files with changed dependency sets."""
        modified: List[str] = []
        for fid in before.dependencies:
            if fid in added_files or fid in removed_files:
                continue
            if fid in after.dependencies:
                before_deps = before.dependencies[fid]
                after_deps = after.dependencies[fid]
                if before_deps != after_deps:
                    modified.append(fid)
        return modified

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _index_edges(self, graph: GraphSnapshot) -> Dict[str, SymbolEdge]:
        """Create a map from edge key → SymbolEdge for fast lookup."""
        indexed: Dict[str, SymbolEdge] = {}
        for edge in graph.edges:
            key = f"{edge.edge_type.value}:{edge.source_id}:{edge.target_id}"
            indexed[key] = edge
        return indexed

    def _edge_to_delta(self, edge: SymbolEdge, graph: GraphSnapshot) -> GraphDeltaEdge:
        """Convert a SymbolEdge to a GraphDeltaEdge with enriched context."""
        source_sym = graph.symbols.get(edge.source_id)
        target_sym = graph.symbols.get(edge.target_id)

        source_file_path = edge.file_path
        target_file_path = edge.file_path
        source_name = edge.source_id[:8]
        target_name = edge.target_id[:8]

        if source_sym:
            source_file_path = source_sym.file_path
            source_name = source_sym.symbol_name
        if target_sym:
            target_file_path = target_sym.file_path
            target_name = target_sym.symbol_name

        # Resolve file IDs
        source_fid = ""
        target_fid = ""
        if source_sym:
            source_fid = source_sym.file_id
        else:
            for fid, pf in graph.files.items():
                if pf.file_path == source_file_path:
                    source_fid = fid
                    break

        if target_sym:
            target_fid = target_sym.file_id
        else:
            for fid, pf in graph.files.items():
                if pf.file_path == target_file_path:
                    target_fid = fid
                    break

        return GraphDeltaEdge(
            edge_type=edge.edge_type,
            source_file_id=source_fid,
            source_file_path=source_file_path,
            source_symbol_name=source_name,
            target_file_id=target_fid,
            target_file_path=target_file_path,
            target_symbol_name=target_name,
            confidence=edge.confidence,
        )

    def _extract_cycles_from_graph(self, graph: GraphSnapshot) -> List[Set[str]]:
        """Simple cycle detection: find mutual imports between files."""
        import_map: Dict[str, Set[str]] = defaultdict(set)
        for edge in graph.edges:
            if edge.edge_type in (EdgeType.IMPORTS, EdgeType.REFERENCES):
                src_file = graph.symbols.get(edge.source_id)
                tgt_file = graph.symbols.get(edge.target_id)
                if src_file and tgt_file:
                    import_map[src_file.file_id].add(tgt_file.file_id)

        cycles: List[Set[str]] = []
        visited: Set[str] = set()
        path: List[str] = []

        def dfs(node: str):
            visited.add(node)
            path.append(node)
            for neighbor in import_map.get(node, set()):
                if neighbor in path:
                    cycle_start = path.index(neighbor)
                    cycles.append(set(path[cycle_start:]))
                elif neighbor not in visited:
                    dfs(neighbor)
            path.pop()

        for fid in import_map:
            if fid not in visited:
                dfs(fid)

        # Deduplicate
        seen: Set[frozenset] = set()
        unique_cycles = []
        for c in cycles:
            fs = frozenset(c)
            if fs not in seen:
                seen.add(fs)
                unique_cycles.append(c)
        return unique_cycles

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        with self._lock:
            self._metrics.append({
                "operation": operation,
                "duration_ms": round(duration_ms, 2),
            })

    def get_metrics(self) -> List[Dict]:
        with self._lock:
            return self._metrics.copy()
