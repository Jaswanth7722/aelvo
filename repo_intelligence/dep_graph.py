# dep_graph.py - Dependency Graph Engine
# Layer 4: File-level and module-level dependency tracking

import time
import logging
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict, deque
from pathlib import Path

from repo_intelligence.types import (
    SymbolEdge, EdgeType, ParsedFile, GraphSnapshot,
    DependencyGraphSnapshot, FileDependencyInfo, PerformanceMetrics
)

log = logging.getLogger("aelvo.repo_intelligence.dep_graph")


class DependencyGraphEngine:
    def __init__(self):
        self.graph: DependencyGraphSnapshot = DependencyGraphSnapshot()
        self.file_info: Dict[str, FileDependencyInfo] = {}
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def _find_test_files(self) -> Set[str]:
        test_files = set()
        for fid, info in self.file_info.items():
            path = info.file_path.replace('\\', '/')
            parts = path.split('/')
            filename = parts[-1] if parts else path
            if ('test' in parts or 'tests' in parts or
                'spec' in parts or 'specs' in parts or
                filename.startswith('test_') or
                filename.endswith('_test.py') or
                filename.endswith('.spec.ts') or
                filename.endswith('.test.ts') or
                filename.endswith('_test.go')):
                test_files.add(fid)
        return test_files

    def _find_entry_points(self) -> Set[str]:
        entry_points = set()
        for fid, info in self.file_info.items():
            path = info.file_path.replace('\\', '/')
            filename = Path(path).name
            if filename in ('main.py', 'main.ts', 'index.py', 'app.py',
                            'cli.py', 'manage.py', 'entrypoint.py',
                            '__main__.py'):
                entry_points.add(fid)
            if info.is_entry_point:
                entry_points.add(fid)
        return entry_points

    def build_from_symbol_graph(
        self, symbol_graph: GraphSnapshot
    ) -> DependencyGraphSnapshot:
        start = time.time()
        self.graph.version += 1
        file_imports: Dict[str, Set[str]] = defaultdict(set)
        for edge in symbol_graph.edges:
            if edge.edge_type in (EdgeType.IMPORTS, EdgeType.REFERENCES):
                source_fid = self._find_file_id_for_entity(
                    edge.source_id, symbol_graph
                )
                target_fid = self._find_file_id_for_entity(
                    edge.target_id, symbol_graph
                )
                if source_fid and target_fid and source_fid != target_fid:
                    file_imports[source_fid].add(target_fid)
        self.graph.dependencies = {}
        self.graph.dependents = defaultdict(set)
        for source_fid, targets in file_imports.items():
            self.graph.dependencies[source_fid] = targets
            for target in targets:
                self.graph.dependents[target].add(source_fid)
        for fid, pf in symbol_graph.files.items():
            if fid not in self.graph.dependencies:
                self.graph.dependencies[fid] = set()
            info = FileDependencyInfo(
                file_id=fid,
                file_path=pf.file_path,
                imports=list(self.graph.dependencies.get(fid, set())),
                imported_by=list(self.graph.dependents.get(fid, set())),
                is_entry_point=fid not in self.graph.dependents,
                is_test_file=False,
            )
            self.file_info[fid] = info
        test_files = self._find_test_files()
        entry_pts = self._find_entry_points()
        for fid in test_files:
            if fid in self.file_info:
                self.file_info[fid].is_test_file = True
        for fid in entry_pts:
            if fid in self.file_info:
                self.file_info[fid].is_entry_point = True
        self.graph.cycles = self._detect_cycles()
        self.graph.topological_order = self._compute_topological_order()
        elapsed = (time.time() - start) * 1000
        self._record_metric("build_from_symbol_graph", elapsed)
        log.info(f"Built dependency graph v{self.graph.version}: "
                 f"{len(self.graph.dependencies)} files, "
                 f"{len(self.graph.cycles)} cycles ({elapsed:.0f}ms)")
        return self.graph

    def _find_file_id_for_entity(
        self, entity_id: str, symbol_graph: GraphSnapshot
    ) -> Optional[str]:
        if entity_id in symbol_graph.files:
            return entity_id
        sym = symbol_graph.symbols.get(entity_id)
        if sym:
            return sym.file_id
        for fid, pf in symbol_graph.files.items():
            if fid == entity_id:
                return fid
        return None

    def _detect_cycles(self) -> List[Set[str]]:
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {}
        parent: Dict[str, Optional[str]] = {}
        cycles = []

        for fid in self.graph.dependencies:
            color[fid] = WHITE

        def dfs(node: str, path: Set[str]) -> None:
            color[node] = GRAY
            for neighbor in self.graph.dependencies.get(node, set()):
                if neighbor not in color:
                    color[neighbor] = WHITE
                if color[neighbor] == GRAY:
                    cycle = set()
                    current = node
                    while current != neighbor:
                        cycle.add(str(current))
                        current = parent.get(current, "")
                        if not current:
                            break
                    cycle.add(str(neighbor))
                    cycle.add(str(node))
                    cycles.append(cycle)
                elif color[neighbor] == WHITE:
                    parent[neighbor] = node
                    dfs(neighbor, path | {neighbor})
            color[node] = BLACK

        for fid in list(color.keys()):
            if color[fid] == WHITE:
                dfs(fid, {fid})

        unique_cycles = []
        seen_sets = set()
        for cycle in cycles:
            frozen = frozenset(cycle)
            if frozen not in seen_sets:
                seen_sets.add(frozen)
                unique_cycles.append(cycle)
        return unique_cycles

    def _compute_topological_order(self) -> List[str]:
        in_degree: Dict[str, int] = {}
        for fid in self.graph.dependencies:
            in_degree.setdefault(fid, 0)
        for fid, deps in self.graph.dependencies.items():
            for dep in deps:
                in_degree.setdefault(dep, 0)
        for fid, deps in self.graph.dependencies.items():
            for dep in deps:
                if dep in in_degree:
                    in_degree[dep] = in_degree.get(dep, 0) + 1
        cycle_nodes = set()
        for cycle in self.graph.cycles:
            cycle_nodes.update(cycle)
        order = []
        queue = deque()
        for fid, degree in in_degree.items():
            if degree == 0 and fid not in cycle_nodes:
                queue.append(fid)
        temp_in_degree = dict(in_degree)
        while queue:
            node = queue.popleft()
            order.append(node)
            for dep in self.graph.dependencies.get(node, set()):
                if dep in temp_in_degree:
                    temp_in_degree[dep] -= 1
                    if temp_in_degree[dep] == 0 and dep not in cycle_nodes:
                        queue.append(dep)
        for fid in cycle_nodes:
            order.append(fid)
        return order

    def get_dependencies(
        self, file_id: str, transitive: bool = False, depth: int = -1
    ) -> List[str]:
        if not transitive:
            return list(self.graph.dependencies.get(file_id, set()))
        visited = set()
        result = []
        queue = deque([(file_id, 0)])
        visited.add(file_id)
        while queue:
            current, d = queue.popleft()
            if 0 <= depth <= d:
                continue
            for dep in self.graph.dependencies.get(current, set()):
                if dep not in visited:
                    visited.add(dep)
                    result.append(dep)
                    queue.append((dep, d + 1))
        return result

    def get_dependents(
        self, file_id: str, transitive: bool = False, depth: int = -1
    ) -> List[str]:
        if not transitive:
            return list(self.graph.dependents.get(file_id, set()))
        visited = set()
        result = []
        queue = deque([(file_id, 0)])
        visited.add(file_id)
        while queue:
            current, d = queue.popleft()
            if 0 <= depth <= d:
                continue
            for dep in self.graph.dependents.get(current, set()):
                if dep not in visited:
                    visited.add(dep)
                    result.append(dep)
                    queue.append((dep, d + 1))
        return result

    def get_package_dependencies(self, package_path: str) -> Dict[str, Set[str]]:
        package_files = {
            fid: info
            for fid, info in self.file_info.items()
            if info.file_path.startswith(package_path)
        }
        intra = defaultdict(set)
        inter = defaultdict(set)
        for fid, info in package_files.items():
            for imp in info.imports:
                imp_info = self.file_info.get(imp)
                if imp_info:
                    if imp_info.file_path.startswith(package_path):
                        intra[fid].add(imp)
                    else:
                        inter[fid].add(imp)
        return {'intra': dict(intra), 'inter': dict(inter)}

    def is_in_cycle(self, file_id: str) -> bool:
        for cycle in self.graph.cycles:
            if file_id in cycle:
                return True
        return False

    def get_file_info(self, file_id: str) -> Optional[FileDependencyInfo]:
        return self.file_info.get(file_id)

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
