# query.py - Query Engine
# Layer 9: Typed interface for specialists to access repository intelligence

import time
import logging
from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path
from collections import defaultdict

from repo_intelligence.types import (
    SymbolNode, SymbolEdge, EdgeType, ConfidenceLevel,
    GraphSnapshot, ImpactReport, ArchitectureMap, FileDependencyInfo,
    DependencyGraphSnapshot, CallGraphSnapshot, QueryResult, QueryProvenance,
    PerformanceMetrics, IndexStatus, RiskLevel, ParsedFile, FileId
)

log = logging.getLogger("aelvo.repo_intelligence.query")


class QueryEngine:
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def make_provenance(
        self, source: str, graph_version: int, is_stale: bool = False
    ) -> QueryProvenance:
        return QueryProvenance(
            source=source, graph_version=graph_version, is_stale=is_stale
        )

    def lookup_symbol_definition(
        self,
        name: str,
        symbol_graph: GraphSnapshot,
        file_context: Optional[str] = None,
    ) -> QueryResult:
        start = time.time()
        results = []
        for sym in symbol_graph.symbols.values():
            if sym.symbol_name == name:
                results.append(sym)
            elif sym.fully_qualified_name == name:
                results.append(sym)
            elif name in sym.fully_qualified_name:
                results.append(sym)
        if file_context:
            context_fid = None
            for fid, pf in symbol_graph.files.items():
                if file_context in pf.file_path:
                    context_fid = fid
                    break
            if context_fid:
                same_file = [r for r in results if r.file_id == context_fid]
                same_pkg = [
                    r for r in results
                    if r.file_id != context_fid
                    and Path(r.file_path).parent == Path(
                        symbol_graph.files.get(context_fid, ParsedFile(
                            file_id="", file_path="", language="", fingerprint=""
                        )).file_path
                    ).parent
                ]
                others = [r for r in results if r not in same_file and r not in same_pkg]
                results = same_file + same_pkg + others
        results.sort(key=lambda s: (
            0 if s.symbol_name == name else 1,
            0 if s.confidence == ConfidenceLevel.CERTAIN else 1,
        ))
        elapsed = (time.time() - start) * 1000
        self._record_metric("lookup_symbol_definition", elapsed)
        return QueryResult(
            data=results,
            confidence=ConfidenceLevel.CERTAIN if results else ConfidenceLevel.APPROXIMATE,
            provenance=self.make_provenance("symbol_graph", symbol_graph.version),
        )

    def lookup_references(
        self,
        symbol_id: str,
        symbol_graph: GraphSnapshot,
    ) -> QueryResult:
        start = time.time()
        references = []
        for edge in symbol_graph.get_edges_to(symbol_id):
            references.append(edge)
        for edge in symbol_graph.get_edges_from(symbol_id):
            if edge.edge_type in (EdgeType.CALLS, EdgeType.REFERENCES):
                references.append(edge)
        by_file: Dict[str, List[SymbolEdge]] = defaultdict(list)
        for ref in references:
            by_file[ref.file_path].append(ref)
        elapsed = (time.time() - start) * 1000
        self._record_metric("lookup_references", elapsed)
        return QueryResult(
            data=dict(by_file),
            confidence=ConfidenceLevel.CERTAIN if references else ConfidenceLevel.INFERRED,
            provenance=self.make_provenance("symbol_graph", symbol_graph.version),
        )

    def lookup_dependencies(
        self,
        file_id: str,
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
        transitive: bool = False,
        depth: int = 1,
    ) -> QueryResult:
        start = time.time()
        deps = self._get_file_deps(file_id, dep_graph, transitive, depth)
        dep_info = []
        for fid in deps:
            info = file_info.get(fid)
            if info:
                dep_info.append(info)
        elapsed = (time.time() - start) * 1000
        self._record_metric("lookup_dependencies", elapsed)
        return QueryResult(
            data=dep_info,
            confidence=ConfidenceLevel.CERTAIN if not transitive else ConfidenceLevel.INFERRED,
            provenance=self.make_provenance("dep_graph", dep_graph.version),
        )

    def lookup_dependents(
        self,
        file_id: str,
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
        transitive: bool = False,
        depth: int = 1,
    ) -> QueryResult:
        start = time.time()
        deps = self._get_file_dependents(file_id, dep_graph, transitive, depth)
        dep_info = []
        for fid in deps:
            info = file_info.get(fid)
            if info:
                dep_info.append(info)
        elapsed = (time.time() - start) * 1000
        self._record_metric("lookup_dependents", elapsed)
        return QueryResult(
            data=dep_info,
            confidence=ConfidenceLevel.CERTAIN if not transitive else ConfidenceLevel.INFERRED,
            provenance=self.make_provenance("dep_graph", dep_graph.version),
        )

    def _get_file_deps(
        self,
        file_id: str,
        dep_graph: DependencyGraphSnapshot,
        transitive: bool,
        depth: int,
    ) -> List[str]:
        if not transitive:
            return list(dep_graph.dependencies.get(file_id, set()))
        visited = set()
        result = []
        from collections import deque
        queue = deque([(file_id, 0)])
        visited.add(file_id)
        while queue:
            current, d = queue.popleft()
            if 0 <= depth <= d:
                continue
            for dep in dep_graph.dependencies.get(current, set()):
                if dep not in visited:
                    visited.add(dep)
                    result.append(dep)
                    queue.append((dep, d + 1))
        return result

    def _get_file_dependents(
        self,
        file_id: str,
        dep_graph: DependencyGraphSnapshot,
        transitive: bool,
        depth: int,
    ) -> List[str]:
        if not transitive:
            return list(dep_graph.dependents.get(file_id, set()))
        visited = set()
        result = []
        from collections import deque
        queue = deque([(file_id, 0)])
        visited.add(file_id)
        while queue:
            current, d = queue.popleft()
            if 0 <= depth <= d:
                continue
            for dep in dep_graph.dependents.get(current, set()):
                if dep not in visited:
                    visited.add(dep)
                    result.append(dep)
                    queue.append((dep, d + 1))
        return result

    def lookup_test_coverage(
        self,
        file_id: str,
        symbol_graph: GraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> QueryResult:
        start = time.time()
        test_files = []
        for fid, info in file_info.items():
            if info.is_test_file:
                imports = info.imports
                if file_id in imports:
                    test_files.append(info.file_path)
        if not test_files:
            pf = symbol_graph.files.get(file_id)
            if pf:
                name_stem = Path(pf.file_path).stem
                for fid, info in file_info.items():
                    if info.is_test_file:
                        if name_stem.lower() in Path(info.file_path).stem.lower():
                            test_files.append(info.file_path)
        elapsed = (time.time() - start) * 1000
        self._record_metric("lookup_test_coverage", elapsed)
        return QueryResult(
            data=list(set(test_files)),
            confidence=ConfidenceLevel.INFERRED if test_files else ConfidenceLevel.APPROXIMATE,
            provenance=self.make_provenance("dep_graph", symbol_graph.version),
        )

    def find_path(
        self,
        source_name: str,
        target_name: str,
        symbol_graph: GraphSnapshot,
    ) -> QueryResult:
        start = time.time()
        source_syms = self.lookup_symbol_definition(source_name, symbol_graph)
        target_syms = self.lookup_symbol_definition(target_name, symbol_graph)
        source_data = source_syms.data
        target_data = target_syms.data
        if not source_data or not target_data:
            elapsed = (time.time() - start) * 1000
            self._record_metric("find_path", elapsed)
            return QueryResult(
                data=None,
                confidence=ConfidenceLevel.APPROXIMATE,
                provenance=self.make_provenance("symbol_graph", symbol_graph.version),
            )
        source_id = source_data[0].symbol_id if isinstance(source_data, list) else None
        target_id = target_data[0].symbol_id if isinstance(target_data, list) else None
        if not source_id or not target_id:
            elapsed = (time.time() - start) * 1000
            self._record_metric("find_path", elapsed)
            return QueryResult(
                data=None,
                confidence=ConfidenceLevel.APPROXIMATE,
                provenance=self.make_provenance("symbol_graph", symbol_graph.version),
            )
        path = self._bfs_path(source_id, target_id, symbol_graph)
        elapsed = (time.time() - start) * 1000
        self._record_metric("find_path", elapsed)
        if path:
            path_symbols = []
            for sid in path:
                sym = symbol_graph.symbols.get(sid)
                if sym:
                    path_symbols.append(sym)
            return QueryResult(
                data=path_symbols,
                confidence=ConfidenceLevel.CERTAIN,
                provenance=self.make_provenance("symbol_graph", symbol_graph.version),
            )
        return QueryResult(
            data=None,
            confidence=ConfidenceLevel.INFERRED,
            provenance=self.make_provenance("symbol_graph", symbol_graph.version),
        )

    def _bfs_path(
        self, source_id: str, target_id: str, symbol_graph: GraphSnapshot
    ) -> Optional[List[str]]:
        from collections import deque
        if source_id not in symbol_graph.symbols or target_id not in symbol_graph.symbols:
            return None
        queue = deque([(source_id, [source_id])])
        visited = {source_id}
        while queue:
            current, path = queue.popleft()
            if current == target_id:
                return path
            neighbors = set()
            for edge in symbol_graph.get_edges_from(current):
                neighbors.add(edge.target_id)
            for edge in symbol_graph.get_edges_to(current):
                neighbors.add(edge.source_id)
            for neighbor in neighbors:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        return None

    def query_call_graph(
        self,
        symbol_id: str,
        call_graph: CallGraphSnapshot,
        direction: str = 'outgoing',
    ) -> QueryResult:
        start = time.time()
        if direction == 'outgoing':
            edges = call_graph.calls.get(symbol_id, [])
        else:
            edges = []
            for caller_id, call_edges in call_graph.calls.items():
                for edge in call_edges:
                    if edge.target_id == symbol_id:
                        edges.append(edge)
        elapsed = (time.time() - start) * 1000
        self._record_metric("query_call_graph", elapsed)
        return QueryResult(
            data=edges,
            confidence=ConfidenceLevel.INFERRED,
            provenance=self.make_provenance("call_graph", call_graph.version),
        )

    def search(
        self,
        query: str,
        symbol_graph: GraphSnapshot,
        max_results: int = 10,
    ) -> QueryResult:
        start = time.time()
        query_lower = query.lower()
        query_terms = set(query_lower.split())

        scored = []
        for sym in symbol_graph.symbols.values():
            name_lower = sym.symbol_name.lower()
            fqname_lower = sym.fully_qualified_name.lower()
            path_lower = sym.file_path.lower()

            score = 0
            if query_lower in name_lower:
                score += 3
            if query_lower in fqname_lower:
                score += 2
            if query_lower in path_lower:
                score += 1

            name_terms = set(name_lower.split('_')) | set(name_lower.split())
            overlap = len(query_terms & name_terms)
            score += overlap * 0.5

            if score > 0:
                scored.append((sym, score))

        scored.sort(key=lambda x: -x[1])
        top_results = [sym for sym, _ in scored[:max_results]]

        elapsed = (time.time() - start) * 1000
        self._record_metric("search", elapsed)
        return QueryResult(
            data=top_results,
            confidence=ConfidenceLevel.APPROXIMATE if top_results else ConfidenceLevel.APPROXIMATE,
            provenance=self.make_provenance("symbol_graph", symbol_graph.version),
        )

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
