# context.py - Context Injection Builder
# Layer 10: Converts repository intelligence into compact, task-relevant context

import time
import logging
from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path
from collections import defaultdict
import re

from repo_intelligence.types import (
    SymbolNode, SymbolEdge, EdgeType, ConfidenceLevel,
    GraphSnapshot, ContextPacket, QueryResult, FileDependencyInfo,
    ArchitectureMap, CallGraphSnapshot, DependencyGraphSnapshot,
    PerformanceMetrics
)

log = logging.getLogger("aelvo.repo_intelligence.context")


class ContextInjectionBuilder:
    def __init__(self, max_tokens: int = 4000):
        self.max_tokens = max_tokens
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def build_context(
        self,
        task_description: str,
        active_specialist: str,
        symbol_graph: GraphSnapshot,
        dep_graph: DependencyGraphSnapshot,
        call_graph: CallGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
        architecture: Optional[ArchitectureMap] = None,
        stale_files: Optional[Set[str]] = None,
    ) -> ContextPacket:
        start = time.time()
        task_keywords = self._extract_keywords(task_description)
        task_symbols = self._discover_relevant_symbols(
            task_keywords, symbol_graph
        )
        expanded = self._expand_symbol_set(
            task_symbols, symbol_graph, call_graph
        )
        if active_specialist == 'sentinel':
            expanded = self._apply_sentinel_filter(
                expanded, symbol_graph, dep_graph
            )
        elif active_specialist == 'forge':
            expanded = self._apply_forge_filter(
                expanded, symbol_graph, dep_graph, file_info
            )
        elif active_specialist == 'architect':
            expanded = self._apply_architect_filter(
                expanded, symbol_graph, dep_graph, file_info
            )
        elif active_specialist == 'oracle':
            expanded = self._apply_oracle_filter(
                expanded, symbol_graph
            )
        ranked = self._rank_by_relevance(
            expanded, task_keywords, symbol_graph
        )
        budget = self.max_tokens
        selected_symbols, budget = self._select_within_budget(
            ranked, budget, symbol_graph
        )
        call_edges = self._get_relevant_call_edges(
            [s.symbol_id for s in selected_symbols], call_graph
        )
        dep_edges = self._get_relevant_dep_edges(
            [s.symbol_id for s in selected_symbols], symbol_graph
        )
        boundaries = self._get_architectural_boundaries(
            [s.file_id for s in selected_symbols], architecture, file_info
        )
        token_estimate = self._estimate_tokens(
            selected_symbols, call_edges, dep_edges, boundaries
        )
        is_stale = self._check_staleness(
            selected_symbols, stale_files, symbol_graph
        )
        provenance = self._build_provenance(
            selected_symbols, symbol_graph
        )
        packet = ContextPacket(
            task_description=task_description,
            active_specialist=active_specialist,
            relevant_symbols=selected_symbols,
            call_relationships=call_edges[:15],
            dependency_relationships=dep_edges[:10],
            architectural_boundaries=boundaries,
            token_estimate=token_estimate,
            staleness_flag=is_stale,
            provenance=provenance,
        )
        elapsed = (time.time() - start) * 1000
        self._record_metric("build_context", elapsed)
        log.info(f"Built context for {active_specialist}: "
                 f"{len(selected_symbols)} symbols, "
                 f"~{token_estimate} tokens ({elapsed:.0f}ms)")
        return packet

    def _extract_keywords(self, task: str) -> Set[str]:
        stop_words = {
            'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
            'being', 'have', 'has', 'had', 'do', 'does', 'did', 'but',
            'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at',
            'by', 'for', 'with', 'about', 'against', 'between', 'into',
            'through', 'during', 'before', 'after', 'above', 'below',
            'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over',
            'under', 'again', 'further', 'then', 'once', 'here', 'there',
            'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each',
            'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor',
            'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very',
            'just', 'should', 'now', 'this', 'that', 'these', 'those',
            'it', 'its', 'i', 'me', 'my', 'we', 'our', 'you', 'your',
            'he', 'him', 'his', 'she', 'her', 'they', 'them', 'their',
            'what', 'which', 'who', 'whom',
        }
        words = set(re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', task.lower()))
        return words - stop_words

    def _discover_relevant_symbols(
        self, keywords: Set[str], symbol_graph: GraphSnapshot
    ) -> List[SymbolNode]:
        matched = []
        for sym in symbol_graph.symbols.values():
            name_lower = sym.symbol_name.lower()
            fqn_lower = sym.fully_qualified_name.lower()
            for kw in keywords:
                if kw in name_lower or kw in fqn_lower:
                    matched.append(sym)
                    break
        return matched

    def _expand_symbol_set(
        self,
        symbols: List[SymbolNode],
        symbol_graph: GraphSnapshot,
        call_graph: CallGraphSnapshot,
    ) -> List[SymbolNode]:
        expanded = {s.symbol_id for s in symbols}
        queue = list(expanded)
        for i in range(2):
            batch = []
            for sym_id in queue:
                for edge in symbol_graph.get_edges_from(sym_id):
                    if edge.target_id not in expanded:
                        target = symbol_graph.symbols.get(edge.target_id)
                        if target:
                            expanded.add(edge.target_id)
                            batch.append(edge.target_id)
                for edge in symbol_graph.get_edges_to(sym_id):
                    if edge.source_id not in expanded:
                        source = symbol_graph.symbols.get(edge.source_id)
                        if source:
                            expanded.add(edge.source_id)
                            batch.append(edge.source_id)
            queue = batch
        return [symbol_graph.symbols[sid] for sid in expanded if sid in symbol_graph.symbols]

    def _apply_sentinel_filter(
        self,
        symbols: List[SymbolNode],
        symbol_graph: GraphSnapshot,
        dep_graph: DependencyGraphSnapshot,
    ) -> List[SymbolNode]:
        entry_keywords = {'request', 'input', 'param', 'form', 'file', 'upload',
                          'url', 'query', 'cookie', 'header', 'body'}
        sink_keywords = {'query', 'execute', 'run', 'shell', 'open', 'write',
                         'save', 'delete', 'send', 'request', 'http'}
        filtered = []
        for sym in symbols:
            name_lower = sym.symbol_name.lower()
            doc_lower = (sym.docstring or '').lower()
            if any(kw in name_lower for kw in entry_keywords):
                filtered.append(sym)
            elif any(kw in name_lower for kw in sink_keywords):
                filtered.append(sym)
            elif any(kw in doc_lower for kw in sink_keywords):
                filtered.append(sym)
            elif sym.symbol_id in dep_graph.dependents:
                dependents_ids = dep_graph.dependents.get(sym.file_id, set())
                for did in dependents_ids:
                    dep_pf = symbol_graph.files.get(did)
                    if dep_pf and 'api' in dep_pf.file_path.lower():
                        filtered.append(sym)
                        break
        return filtered or symbols

    def _apply_forge_filter(
        self,
        symbols: List[SymbolNode],
        symbol_graph: GraphSnapshot,
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> List[SymbolNode]:
        test_files = {
            fid for fid, info in file_info.items()
            if info.is_test_file
        }
        test_symbols = [
            s for s in symbols
            if s.file_id in test_files
        ]
        non_test = [s for s in symbols if s.file_id not in test_files]
        return non_test + test_symbols[:5]

    def _apply_architect_filter(
        self,
        symbols: List[SymbolNode],
        symbol_graph: GraphSnapshot,
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> List[SymbolNode]:
        entry_files = {
            fid for fid, info in file_info.items()
            if info.is_entry_point
        }
        high_indegree = set()
        for fid, info in file_info.items():
            if len(info.imported_by) >= 5:
                high_indegree.add(fid)
        filtered = []
        for sym in symbols:
            if sym.file_id in entry_files:
                filtered.append(sym)
            elif sym.file_id in high_indegree:
                filtered.append(sym)
        seen = {s.symbol_id for s in filtered}
        for sym in symbols:
            if sym.symbol_id not in seen:
                filtered.append(sym)
                seen.add(sym.symbol_id)
        return filtered

    def _apply_oracle_filter(
        self,
        symbols: List[SymbolNode],
        symbol_graph: GraphSnapshot,
    ) -> List[SymbolNode]:
        return sorted(
            symbols,
            key=lambda s: (
                0 if s.confidence == ConfidenceLevel.CERTAIN else
                1 if s.confidence == ConfidenceLevel.INFERRED else 2,
                s.symbol_name,
            )
        )

    def _rank_by_relevance(
        self,
        symbols: List[SymbolNode],
        keywords: Set[str],
        symbol_graph: GraphSnapshot,
    ) -> List[SymbolNode]:
        def score(sym: SymbolNode) -> int:
            s = 0
            name_lower = sym.symbol_name.lower()
            fqn_lower = sym.fully_qualified_name.lower()
            for kw in keywords:
                if kw == name_lower or kw == fqn_lower:
                    s += 10
                elif kw in name_lower:
                    s += 5
                elif kw in fqn_lower:
                    s += 3
            if sym.confidence == ConfidenceLevel.CERTAIN:
                s += 2
            elif sym.confidence == ConfidenceLevel.INFERRED:
                s += 1
            if sym.is_exported:
                s += 1
            in_degree = len(symbol_graph.get_edges_to(sym.symbol_id))
            out_degree = len(symbol_graph.get_edges_from(sym.symbol_id))
            s += min(in_degree + out_degree, 5)
            return s

        return sorted(symbols, key=score, reverse=True)

    def _select_within_budget(
        self,
        ranked: List[SymbolNode],
        budget: int,
        symbol_graph: GraphSnapshot,
    ) -> Tuple[List[SymbolNode], int]:
        selected = []
        tokens_per_symbol = 30
        for sym in ranked:
            est = tokens_per_symbol
            if sym.docstring:
                est += len(sym.docstring) // 4
            if est > budget:
                break
            selected.append(sym)
            budget -= est
        return selected, budget

    def _get_relevant_call_edges(
        self,
        symbol_ids: List[str],
        call_graph: CallGraphSnapshot,
    ) -> List[SymbolEdge]:
        edges = []
        seen_targets = set()
        for sym_id in symbol_ids:
            for edge in call_graph.calls.get(sym_id, []):
                if edge.target_id not in seen_targets:
                    edges.append(edge)
                    seen_targets.add(edge.target_id)
        return edges

    def _get_relevant_dep_edges(
        self,
        symbol_ids: List[str],
        symbol_graph: GraphSnapshot,
    ) -> List[SymbolEdge]:
        file_ids = set()
        for sym_id in symbol_ids:
            sym = symbol_graph.symbols.get(sym_id)
            if sym:
                file_ids.add(sym.file_id)
        edges = []
        for edge in symbol_graph.edges:
            if edge.edge_type == EdgeType.IMPORTS:
                if edge.source_id in file_ids or edge.target_id in file_ids:
                    edges.append(edge)
        return edges

    def _get_architectural_boundaries(
        self,
        file_ids: List[str],
        architecture: Optional[ArchitectureMap],
        file_info: Dict[str, FileDependencyInfo],
    ) -> Dict[str, List[str]]:
        boundaries = {}
        if architecture:
            for layer in architecture.layers:
                layer_files = []
                for fid in file_ids:
                    info = file_info.get(fid)
                    if info and fid in layer.files:
                        layer_files.append(info.file_path)
                if layer_files:
                    boundaries[layer.name] = layer_files
        return boundaries

    def _estimate_tokens(
        self,
        symbols: List[SymbolNode],
        call_edges: List[SymbolEdge],
        dep_edges: List[SymbolEdge],
        boundaries: Dict[str, List[str]],
    ) -> int:
        total = 50
        for sym in symbols:
            total += 15 + len(sym.symbol_name) // 2
            if sym.docstring:
                total += len(sym.docstring) // 4
        total += len(call_edges) * 10
        total += len(dep_edges) * 10
        for bf in boundaries.values():
            total += len(bf) * 5
        return total

    def _check_staleness(
        self,
        symbols: List[SymbolNode],
        stale_files: Optional[Set[str]],
        symbol_graph: GraphSnapshot,
    ) -> bool:
        if not stale_files:
            return False
        for sym in symbols:
            if sym.file_path in stale_files:
                return True
            pf = symbol_graph.files.get(sym.file_id)
            if pf and pf.file_path in stale_files:
                return True
        return False

    def _build_provenance(
        self,
        symbols: List[SymbolNode],
        symbol_graph: GraphSnapshot,
    ) -> Dict[str, str]:
        provenance = {}
        for sym in symbols:
            provenance[sym.symbol_id] = (
                f"{sym.file_path}:{sym.line_range[0]} "
                f"(graph v{symbol_graph.version})"
            )
        return provenance

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
