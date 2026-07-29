# learning/subgraph.py - SubgraphExtractor & SubgraphSimilarity
# Extracts minimal affected subgraphs from deltas and matches them for pattern similarity

from __future__ import annotations

import time
import logging
import threading
from typing import Dict, List, Optional
from collections import Counter, defaultdict

from repo_intelligence.types import (
    GraphSnapshot,
)
from learning.types import (
    DependencyGraphDelta, SubgraphSpec, SubgraphNode, SubgraphEdge,
)

log = logging.getLogger("aelvo.learning.subgraph")


class SubgraphExtractor:
    """Extracts the minimal affected subgraph from a dependency graph delta.

    The subgraph captures only the files, symbols, and edges that were
    directly touched by the edit. This minimal representation is what
    gets stored with patterns and used for similarity matching.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._metrics: List[Dict] = []

    # ── Public API ────────────────────────────────────────────────────────────

    def extract(
        self,
        delta: DependencyGraphDelta,
        after_graph: Optional[GraphSnapshot] = None,
    ) -> SubgraphSpec:
        """Extract the minimal affected subgraph from a delta.

        Args:
            delta: The computed dependency graph delta.
            after_graph: Optional snapshot of the graph after the edit,
                         used to enrich subgraph with symbol context.

        Returns:
            A SubgraphSpec containing only the affected nodes and edges.
        """
        with self._lock:
            start = time.time()

            nodes: Dict[str, SubgraphNode] = {}
            edges: List[SubgraphEdge] = []
            anchor_node_key = ""

            # Add nodes from new edges
            for edge in delta.new_edges:
                self._add_node_from_edge(nodes, edge, is_new=False, after_graph=after_graph)
                self._edge_to_subgraph_edge(edge, edges, nodes)

            # Add nodes from removed edges
            for edge in delta.removed_edges:
                self._add_node_from_edge(nodes, edge, is_new=False, after_graph=after_graph)
                # Don't add removed edges to the subgraph (they no longer exist)

            # Add nodes for added files
            for file_path in delta.added_files:
                node_key = file_path
                nodes[node_key] = SubgraphNode(
                    file_path=file_path,
                    is_new=True,
                    is_anchor=(anchor_node_key == ""),
                )
                if anchor_node_key == "":
                    anchor_node_key = node_key

            # Add nodes for modified files
            for file_path in delta.modified_files:
                node_key = file_path
                if node_key not in nodes:
                    nodes[node_key] = SubgraphNode(
                        file_path=file_path,
                        is_new=False,
                        is_anchor=(anchor_node_key == ""),
                    )
                if anchor_node_key == "":
                    anchor_node_key = node_key

            # If no edges were involved, create single-node subgraph
            if not nodes and not edges:
                if delta.modified_files:
                    file_path = delta.modified_files[0]
                    nodes[file_path] = SubgraphNode(
                        file_path=file_path,
                        is_anchor=True,
                    )
                    anchor_node_key = file_path

            # If still no anchor, use the first node
            if not anchor_node_key and nodes:
                # Set first node as anchor
                first_key = next(iter(nodes))
                nodes[first_key].is_anchor = True
                anchor_node_key = first_key

            subgraph = SubgraphSpec(
                anchor_node_key=anchor_node_key,
                nodes=list(nodes.values()),
                edges=edges,
                node_count=len(nodes),
                edge_count=len(edges),
            )

            elapsed = (time.time() - start) * 1000
            self._record_metric("extract_subgraph", elapsed)
            log.debug(
                f"Extracted subgraph: {subgraph.node_count} nodes, "
                f"{subgraph.edge_count} edges"
            )
            return subgraph

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _add_node_from_edge(
        self,
        nodes: Dict[str, SubgraphNode],
        edge,
        is_new: bool,
        after_graph: Optional[GraphSnapshot] = None,
    ) -> None:
        """Add source and target nodes from an edge to the node map."""
        source_key = edge.source_file_path or edge.source_file_id or edge.source_symbol_name
        target_key = edge.target_file_path or edge.target_file_id or edge.target_symbol_name

        if source_key and source_key not in nodes:
            nodes[source_key] = SubgraphNode(
                file_id=edge.source_file_id,
                file_path=edge.source_file_path,
                symbol_name=edge.source_symbol_name,
                is_new=False,
            )
        if target_key and target_key not in nodes:
            nodes[target_key] = SubgraphNode(
                file_id=edge.target_file_id,
                file_path=edge.target_file_path,
                symbol_name=edge.target_symbol_name,
                is_new=False,
            )

    def _edge_to_subgraph_edge(
        self,
        edge,
        edges: List[SubgraphEdge],
        nodes: Dict[str, SubgraphNode],
    ) -> str:
        """Convert a GraphDeltaEdge to a SubgraphEdge and add it."""
        source_key = edge.source_file_path or edge.source_file_id or edge.source_symbol_name
        target_key = edge.target_file_path or edge.target_file_id or edge.target_symbol_name

        if not source_key or not target_key:
            return ""

        sub_edge = SubgraphEdge(
            source_key=source_key,
            target_key=target_key,
            edge_type=edge.edge_type,
        )

        # Deduplicate
        edge_key = sub_edge.edge_key
        if not any(e.edge_key == edge_key for e in edges):
            edges.append(sub_edge)

        return edge_key

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        with self._lock:
            self._metrics.append({
                "operation": operation,
                "duration_ms": round(duration_ms, 2),
            })

    def get_metrics(self) -> List[Dict]:
        with self._lock:
            return self._metrics.copy()


# ── SubgraphSimilarity ────────────────────────────────────────────────────────

class SubgraphSimilarity:
    """Computes structural similarity between two SubgraphSpec instances.

    Uses a combination of:
    - Category match (50% weight)
    - Node count similarity (15% weight)
    - Edge type distribution similarity (20% weight)
    - Topological structure similarity (15% weight)
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._metrics: List[Dict] = []

    def compute(
        self,
        a: SubgraphSpec,
        b: SubgraphSpec,
    ) -> float:
        """Compute structural similarity score between two subgraphs.

        Returns:
            Float 0.0 (completely different) to 1.0 (identical).
        """
        with self._lock:
            start = time.time()

            score = 0.0

            # 1. Category match (50% weight)
            if a.category == b.category:
                score += 0.50

            # 2. Node count similarity (15% weight)
            if a.node_count == 0 and b.node_count == 0:
                score += 0.15
            elif a.node_count > 0 and b.node_count > 0:
                ratio = min(a.node_count, b.node_count) / max(a.node_count, b.node_count)
                score += 0.15 * ratio

            # 3. Edge type distribution similarity (20% weight)
            edge_types_a = Counter(e.edge_type for e in a.edges)
            edge_types_b = Counter(e.edge_type for e in b.edges)
            all_types = set(edge_types_a.keys()) | set(edge_types_b.keys())
            if not all_types:
                score += 0.20
            else:
                total_diff = 0.0
                for et in all_types:
                    ca = edge_types_a.get(et, 0)
                    cb = edge_types_b.get(et, 0)
                    total_diff += abs(ca - cb) / max(ca, cb, 1)
                type_similarity = 1.0 - (total_diff / len(all_types))
                score += 0.20 * type_similarity

            # 4. Topological structure similarity (15% weight)
            in_degrees_a = defaultdict(int)
            out_degrees_a = defaultdict(int)
            for edge in a.edges:
                out_degrees_a[edge.source_key] += 1
                in_degrees_a[edge.target_key] += 1

            in_degrees_b = defaultdict(int)
            out_degrees_b = defaultdict(int)
            for edge in b.edges:
                out_degrees_b[edge.source_key] += 1
                in_degrees_b[edge.target_key] += 1

            if a.nodes:
                deg_seq_a = sorted([in_degrees_a[n.node_key] + out_degrees_a[n.node_key] for n in a.nodes])
            else:
                deg_seq_a = [0] * a.node_count

            if b.nodes:
                deg_seq_b = sorted([in_degrees_b[n.node_key] + out_degrees_b[n.node_key] for n in b.nodes])
            else:
                deg_seq_b = [0] * b.node_count

            if not deg_seq_a and not deg_seq_b:
                score += 0.15
            elif deg_seq_a and deg_seq_b:
                max_len = max(len(deg_seq_a), len(deg_seq_b))
                deg_seq_a_padded = deg_seq_a + [0] * (max_len - len(deg_seq_a))
                deg_seq_b_padded = deg_seq_b + [0] * (max_len - len(deg_seq_b))
                diff = sum(abs(da - db) for da, db in zip(deg_seq_a_padded, deg_seq_b_padded))
                max_val = sum(max(da, db, 1) for da, db in zip(deg_seq_a_padded, deg_seq_b_padded))
                topo_similarity = 1.0 - (diff / max_val)
                if a.node_count > 0 and b.node_count > 0:
                    node_ratio = min(a.node_count, b.node_count) / max(a.node_count, b.node_count)
                    topo_similarity *= node_ratio
                score += 0.15 * topo_similarity

            # 5. Anchor match bonus (added on top, doesn't replace anything)
            if a.anchor_node_key and b.anchor_node_key:
                if a.anchor_node_key == b.anchor_node_key:
                    score += 0.10

            score = min(1.0, max(0.0, score))

            elapsed = (time.time() - start) * 1000
            self._record_metric("similarity", elapsed)
            return score

    def are_isomorphic(
        self,
        a: SubgraphSpec,
        b: SubgraphSpec,
        threshold: float = 0.75,
    ) -> bool:
        """Check if two subgraphs are structurally isomorphic above a threshold."""
        return self.compute(a, b) >= threshold

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        with self._lock:
            self._metrics.append({
                "operation": operation,
                "duration_ms": round(duration_ms, 2),
            })

    def get_metrics(self) -> List[Dict]:
        with self._lock:
            return self._metrics.copy()



