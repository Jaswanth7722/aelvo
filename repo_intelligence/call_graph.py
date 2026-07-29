# call_graph.py - Call Graph Engine
# Layer 5: Tracks which functions call which other functions

import time
import logging
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict, deque

from repo_intelligence.types import (
    SymbolNode, SymbolEdge, EdgeType, ConfidenceLevel,
    GraphSnapshot, CallGraphSnapshot, ParsedFile, PerformanceMetrics
)

log = logging.getLogger("aelvo.repo_intelligence.call_graph")


class CallGraphEngine:
    def __init__(self):
        self.graph: CallGraphSnapshot = CallGraphSnapshot()
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def build_from_symbol_graph(
        self, symbol_graph: GraphSnapshot
    ) -> CallGraphSnapshot:
        start = time.time()
        self.graph.version += 1
        self.graph.calls.clear()
        for edge in symbol_graph.edges:
            if edge.edge_type == EdgeType.CALLS:
                caller_id = self._resolve_to_function(edge.source_id, symbol_graph)
                callee_id = self._resolve_to_function(edge.target_id, symbol_graph)
                if caller_id and callee_id:
                    if caller_id not in self.graph.calls:
                        self.graph.calls[caller_id] = []
                    edge_exists = any(
                        e.target_id == callee_id and e.confidence == edge.confidence
                        for e in self.graph.calls[caller_id]
                    )
                    if not edge_exists:
                        self.graph.calls[caller_id].append(edge)
        self._infer_from_methods(symbol_graph)
        self._infer_from_self_calls(symbol_graph)
        elapsed = (time.time() - start) * 1000
        self._record_metric("build_from_symbol_graph", elapsed)
        total_calls = sum(len(v) for v in self.graph.calls.values())
        log.info(f"Built call graph v{self.graph.version}: {total_calls} call edges ({elapsed:.0f}ms)")
        return self.graph

    def _resolve_to_function(
        self, entity_id: str, symbol_graph: GraphSnapshot
    ) -> Optional[str]:
        if entity_id in symbol_graph.symbols:
            return entity_id
        for sym_id, sym in symbol_graph.symbols.items():
            if sym.symbol_id == entity_id:
                return sym_id
        return None

    def _infer_from_methods(self, symbol_graph: GraphSnapshot) -> None:
        class_methods: Dict[str, List[SymbolNode]] = defaultdict(list)
        for sym in symbol_graph.symbols.values():
            if sym.parent_symbol_id:
                parent = symbol_graph.symbols.get(sym.parent_symbol_id)
                if parent and parent.symbol_kind.value == 'class':
                    class_methods[parent.symbol_id].append(sym)
        for class_id, methods in class_methods.items():
            class_sym = symbol_graph.symbols.get(class_id)
            if not class_sym:
                continue
            mro = self._compute_mro(class_sym, symbol_graph)
            for method in methods:
                for cls_id in mro:
                    if cls_id == class_id:
                        continue
                    cls_sym = symbol_graph.symbols.get(cls_id)
                    if cls_sym:
                        parent_methods = [
                            s for s in symbol_graph.symbols.values()
                            if s.parent_symbol_id == cls_id
                            and s.symbol_name == method.symbol_name
                        ]
                        for pm in parent_methods:
                            if method.symbol_id not in self.graph.calls:
                                self.graph.calls[method.symbol_id] = []
                            edge = SymbolEdge(
                                source_id=method.symbol_id,
                                target_id=pm.symbol_id,
                                edge_type=EdgeType.CALLS,
                                file_path=method.file_path,
                                line_number=method.line_range[0],
                                confidence=ConfidenceLevel.INFERRED,
                            )
                            existing = any(
                                e.target_id == pm.symbol_id and e.confidence == ConfidenceLevel.INFERRED
                                for e in self.graph.calls[method.symbol_id]
                            )
                            if not existing:
                                self.graph.calls[method.symbol_id].append(edge)

    def _infer_from_self_calls(self, symbol_graph: GraphSnapshot) -> None:
        for sym in symbol_graph.symbols.values():
            if sym.parent_symbol_id:
                parent = symbol_graph.symbols.get(sym.parent_symbol_id)
                if parent and parent.symbol_kind.value == 'class':
                    if sym.symbol_name == sym.symbol_name:
                        for edge in symbol_graph.get_edges_from(sym.symbol_id):
                            if edge.edge_type == EdgeType.CALLS:
                                target_sym = symbol_graph.symbols.get(edge.target_id)
                                if target_sym:
                                    new_edge = SymbolEdge(
                                        source_id=sym.symbol_id,
                                        target_id=target_sym.symbol_id,
                                        edge_type=EdgeType.CALLS,
                                        file_path=edge.file_path,
                                        line_number=edge.line_number,
                                        confidence=ConfidenceLevel.CERTAIN,
                                    )
                                    if sym.symbol_id in self.graph.calls:
                                        existing = any(
                                            e.target_id == target_sym.symbol_id
                                            and e.confidence == ConfidenceLevel.CERTAIN
                                            for e in self.graph.calls[sym.symbol_id]
                                        )
                                        if not existing:
                                            self.graph.calls[sym.symbol_id].append(new_edge)

    def _compute_mro(
        self, class_sym: SymbolNode, symbol_graph: GraphSnapshot
    ) -> List[str]:
        mro = [class_sym.symbol_id]
        visited = {class_sym.symbol_id}
        queue = deque([class_sym.symbol_id])
        while queue:
            current = queue.popleft()
            for edge in symbol_graph.get_edges_from(current):
                if edge.edge_type == EdgeType.INHERITS:
                    if edge.target_id not in visited:
                        visited.add(edge.target_id)
                        queue.append(edge.target_id)
                        mro.append(edge.target_id)
        return mro

    def get_calls_from(self, symbol_id: str) -> List[SymbolEdge]:
        return self.graph.calls.get(symbol_id, [])

    def get_callers_of(self, symbol_id: str) -> List[SymbolEdge]:
        callers = []
        for caller_id, edges in self.graph.calls.items():
            for edge in edges:
                if edge.target_id == symbol_id:
                    callers.append(edge)
        return callers

    def get_call_chain(
        self, source_id: str, target_id: str, max_depth: int = 10
    ) -> Optional[List[str]]:
        queue = deque([(source_id, [source_id])])
        visited = {source_id}
        while queue:
            current, path = queue.popleft()
            if current == target_id:
                return path
            if len(path) >= max_depth:
                continue
            for edge in self.graph.calls.get(current, []):
                if edge.target_id not in visited:
                    visited.add(edge.target_id)
                    queue.append((edge.target_id, path + [edge.target_id]))
        return None

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
