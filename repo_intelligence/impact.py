# impact.py - Change Impact Analyzer
# Layer 7: Answers "what will be affected if I make this change"
from __future__ import annotations

import time
import logging
from typing import Dict, List, Set, Optional, Tuple
from collections import deque
from pathlib import Path

from repo_intelligence.types import (
    EdgeType, ConfidenceLevel, RiskLevel,
    GraphSnapshot, ImpactReport, FileDependencyInfo,
    PerformanceMetrics
)

log = logging.getLogger("aelvo.repo_intelligence.impact")


class ChangeImpactAnalyzer:
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def analyze(
        self,
        changed_file: str,
        changed_symbols: Optional[List[str]],
        symbol_graph: GraphSnapshot,
        dep_graph: 'DependencyGraphSnapshot',
        file_info: Dict[str, FileDependencyInfo],
        max_depth: int = 5,
    ) -> ImpactReport:
        start = time.time()
        fid = None
        for file_id, pf in symbol_graph.files.items():
            if pf.file_path == changed_file:
                fid = file_id
                break
        if fid is None:
            return ImpactReport(
                changed_file=changed_file,
                risk_level=RiskLevel.LOW,
                confidence=ConfidenceLevel.INFERRED,
            )
        if changed_symbols:
            changed_symbol_ids = self._find_symbol_ids(
                changed_symbols, fid, symbol_graph
            )
        else:
            changed_symbol_ids = [
                s.symbol_id for s in symbol_graph.get_symbols_in_file(fid)
            ]
        stage1_direct = self._find_direct_impact(
            changed_symbol_ids, symbol_graph
        )
        stage2_transitive = self._find_transitive_impact(
            stage1_direct, symbol_graph, max_depth
        )
        stage3_files = self._aggregate_to_files(
            stage2_transitive, symbol_graph
        )
        stage4_tests = self._identify_tests(
            stage3_files, file_info, symbol_graph, fid
        )
        stage5_risk = self._assess_risk(
            changed_symbol_ids, stage1_direct, stage3_files,
            stage4_tests, fid, symbol_graph, file_info
        )
        risk_level, risk_reasoning, confidence = stage5_risk
        report = ImpactReport(
            changed_file=changed_file,
            changed_symbols=changed_symbol_ids,
            directly_affected_symbols=list(stage1_direct),
            transitively_affected_files=stage3_files,
            affected_tests=list(stage4_tests),
            risk_level=risk_level,
            risk_reasoning=risk_reasoning,
            confidence=confidence,
        )
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze", elapsed)
        log.info(f"Impact analysis for {changed_file}: "
                 f"{len(stage1_direct)} direct, {len(stage3_files)} files, "
                 f"risk={risk_level.value} ({elapsed:.0f}ms)")
        return report

    def _find_symbol_ids(
        self, names: List[str], file_id: str, symbol_graph: GraphSnapshot
    ) -> List[str]:
        found = []
        for sym_id, sym in symbol_graph.symbols.items():
            if sym.file_id == file_id and sym.symbol_name in names:
                found.append(sym_id)
        return found

    def _find_direct_impact(
        self, changed_symbol_ids: List[str], symbol_graph: GraphSnapshot
    ) -> Set[str]:
        affected = set()
        for sym_id in changed_symbol_ids:
            for edge in symbol_graph.get_edges_to(sym_id):
                if edge.source_id not in changed_symbol_ids:
                    affected.add(edge.source_id)
            for edge in symbol_graph.get_edges_from(sym_id):
                if edge.edge_type == EdgeType.CONTAINS:
                    for inner_edge in symbol_graph.get_edges_to(edge.target_id):
                        if inner_edge.source_id not in changed_symbol_ids:
                            affected.add(inner_edge.source_id)
        return affected

    def _find_transitive_impact(
        self, direct: Set[str], symbol_graph: GraphSnapshot, max_depth: int
    ) -> Set[str]:
        all_affected = set(direct)
        queue = deque((s, 1) for s in direct)
        visited = set(direct)
        while queue:
            current, depth = queue.popleft()
            if depth >= max_depth:
                continue
            for edge in symbol_graph.get_edges_to(current):
                src = edge.source_id
                if src not in visited:
                    visited.add(src)
                    all_affected.add(src)
                    queue.append((src, depth + 1))
        return all_affected

    def _aggregate_to_files(
        self, symbol_ids: Set[str], symbol_graph: GraphSnapshot
    ) -> Set[str]:
        file_ids = set()
        for sym_id in symbol_ids:
            sym = symbol_graph.symbols.get(sym_id)
            if sym:
                file_ids.add(sym.file_id)
        return file_ids

    def _identify_tests(
        self,
        affected_files: Set[str],
        file_info: Dict[str, FileDependencyInfo],
        symbol_graph: GraphSnapshot,
        changed_fid: str,
    ) -> Set[str]:
        test_files = set()
        for fid, info in file_info.items():
            if info.is_test_file:
                for imp in info.imports:
                    if imp in affected_files or imp == changed_fid:
                        test_files.add(fid)
                        break
        for fid, pf in symbol_graph.files.items():
            path = pf.file_path.replace('\\', '/')
            name = Path(path).stem
            for af in affected_files:
                af_info = file_info.get(af)
                if af_info:
                    af_name = Path(af_info.file_path).stem
                    if af_name.lower() in name.lower() or name.lower() in af_name.lower():
                        test_files.add(fid)
        return test_files

    def _assess_risk(
        self,
        changed_symbol_ids: List[str],
        direct_impact: Set[str],
        affected_files: Set[str],
        affected_tests: Set[str],
        changed_fid: str,
        symbol_graph: GraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> Tuple[RiskLevel, List[str], ConfidenceLevel]:
        reasoning = []
        score = 0
        total_files = len(affected_files)
        total_tests = len(affected_tests)
        total_direct = len(direct_impact)

        if total_files > 20:
            score += 4
            reasoning.append(f"High file count: {total_files} files affected")
        elif total_files > 10:
            score += 3
            reasoning.append(f"Moderate file count: {total_files} files affected")
        elif total_files > 5:
            score += 2
        elif total_files > 0:
            score += 1

        if total_tests > 5:
            score += 2
            reasoning.append(f"Large test surface: {total_tests} tests affected")
        elif total_tests > 0:
            score += 1

        for fid in affected_files:
            info = file_info.get(fid)
            if info and info.is_entry_point:
                score += 3
                reasoning.append(f"Entry point affected: {info.file_path}")
                break

        affected_info = file_info.get(changed_fid)
        if affected_info and affected_info.is_entry_point:
            score += 2
            reasoning.append("Changed file is an entry point")

        for fid in affected_files:
            info = file_info.get(fid)
            if info and len(info.imported_by) > 10:
                score += 2
                reasoning.append(f"Highly depended-on file affected: {info.file_path} (used by {len(info.imported_by)} files)")
                break

        public_api_changes = 0
        for sym_id in changed_symbol_ids:
            sym = symbol_graph.symbols.get(sym_id)
            if sym and sym.is_exported:
                public_api_changes += 1
        if public_api_changes > 0:
            score += public_api_changes
            reasoning.append(f"{public_api_changes} public API symbols changed")

        if total_direct > 10:
            score += 2
        elif total_direct > 5:
            score += 1

        if score >= 10:
            risk = RiskLevel.CRITICAL
        elif score >= 6:
            risk = RiskLevel.HIGH
        elif score >= 3:
            risk = RiskLevel.MEDIUM
        else:
            risk = RiskLevel.LOW

        if score >= 5:
            confidence = ConfidenceLevel.CERTAIN
        elif score >= 2:
            confidence = ConfidenceLevel.INFERRED
        else:
            confidence = ConfidenceLevel.APPROXIMATE

        return risk, reasoning, confidence

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
