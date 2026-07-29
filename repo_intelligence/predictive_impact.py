# predictive_impact.py - Predictive Impact Analyzer for Repository Intelligence
# Layer 17: Predicts impact before modifications occur

import time
import logging
from typing import List, Set
from collections import deque
from datetime import datetime
from pathlib import Path

from repo_intelligence.types import EdgeType, PerformanceMetrics
from repo_intelligence.types_extended import (
    ProposedChange, BlastRadiusAnalysis, FailurePath, PredictiveImpactReport,
    ConfidenceLevel, RiskLevel
)

log = logging.getLogger("aelvo.repo_intelligence.predictive_impact")


class PredictiveImpactAnalyzer:
    """Predicts impact before modifications occur"""
    
    def __init__(self, symbol_graph_engine, dep_graph_engine):
        """
        Initialize with graph engines.
        
        Args:
            symbol_graph_engine: SymbolGraphEngine instance
            dep_graph_engine: DependencyGraphEngine instance
        """
        self.symbol_graph = symbol_graph_engine.graph
        self.dep_graph = dep_graph_engine.graph
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def predict_impact(
        self,
        proposed_changes: List[ProposedChange],
        prediction_horizon: int = 5
    ) -> PredictiveImpactReport:
        """Predicts impact of proposed changes before implementation"""
        start = time.time()
        
        # Analyze all proposed changes
        all_affected_symbols = set()
        all_affected_files = set()
        all_affected_tests = set()
        
        for change in proposed_changes:
            # Predict impact for this specific change
            affected_symbols, affected_files, affected_tests = self._predict_single_change_impact(
                change, prediction_horizon
            )
            all_affected_symbols.update(affected_symbols)
            all_affected_files.update(affected_files)
            all_affected_tests.update(affected_tests)
        
        # Calculate blast radius
        blast_radius = self._calculate_blast_radius(
            all_affected_symbols, all_affected_files, proposed_changes
        )
        
        # Predict cascading failures
        cascading_failures = self._predict_cascading_failures(
            proposed_changes, all_affected_symbols
        )
        
        # Determine confidence level
        confidence = self._calculate_prediction_confidence(proposed_changes)
        
        report = PredictiveImpactReport(
            change_id=f"batch_{int(time.time())}",
            predicted_affected_symbols=list(all_affected_symbols),
            predicted_affected_files=list(all_affected_files),
            predicted_affected_tests=list(all_affected_tests),
            blast_radius=blast_radius,
            cascading_failures=cascading_failures,
            confidence=confidence,
            prediction_timestamp=datetime.now()
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("predict_impact", elapsed)
        log.info(f"Impact prediction completed: {len(all_affected_symbols)} symbols, "
                 f"{len(all_affected_files)} files affected ({elapsed:.0f}ms)")
        
        return report
    
    def _predict_single_change_impact(
        self,
        change: ProposedChange,
        prediction_horizon: int
    ) -> tuple:
        """Predict impact for a single proposed change"""
        affected_symbols = set()
        affected_files = set()
        affected_tests = set()
        
        # Start with directly affected symbols
        for symbol_id in change.target_symbols:
            if symbol_id in self.symbol_graph.symbols:
                affected_symbols.add(symbol_id)
                symbol = self.symbol_graph.symbols[symbol_id]
                affected_files.add(symbol.file_id)
        
        # For direct file changes
        for file_id in change.target_files:
            affected_files.add(file_id)
            file = self.symbol_graph.files.get(file_id)
            if file:
                for symbol in file.symbols:
                    affected_symbols.add(symbol.symbol_id)
        
        # Predict transitive impact
        transitive_symbols = self._predict_transitive_impact(
            affected_symbols, prediction_horizon
        )
        affected_symbols.update(transitive_symbols)
        
        # Add files for transitive symbols
        for symbol_id in transitive_symbols:
            symbol = self.symbol_graph.symbols.get(symbol_id)
            if symbol:
                affected_files.add(symbol.file_id)
        
        # Predict affected tests
        for file_id in affected_files:
            test_files = self._find_related_test_files(file_id)
            affected_tests.update(test_files)
        
        return affected_symbols, affected_files, affected_tests
    
    def _predict_transitive_impact(
        self,
        initial_symbols: Set[str],
        max_depth: int
    ) -> Set[str]:
        """Predict transitive impact using graph traversal"""
        all_affected = set(initial_symbols)
        queue = deque((sym_id, 1) for sym_id in initial_symbols)
        visited = set(initial_symbols)
        
        while queue:
            current_id, depth = queue.popleft()
            
            if depth >= max_depth:
                continue
            
            # Get symbols that depend on current symbol
            dependent_symbols = set()
            for edge in self.symbol_graph.get_edges_to(current_id):
                if edge.edge_type in [EdgeType.IMPORTS, EdgeType.CALLS, EdgeType.REFERENCES]:
                    dependent_symbols.add(edge.source_id)
            
            for dependent_id in dependent_symbols:
                if dependent_id not in visited:
                    visited.add(dependent_id)
                    all_affected.add(dependent_id)
                    queue.append((dependent_id, depth + 1))
        
        return all_affected
    
    def _find_related_test_files(self, file_id: str) -> Set[str]:
        """Find test files related to a file"""
        test_files = set()
        
        # Get the base name of the file
        file = self.symbol_graph.files.get(file_id)
        if not file:
            return test_files
        
        file_path = file.file_path.lower()
        base_name = Path(file_path).stem
        
        # Look for test files with similar names
        for test_file_id in self.symbol_graph.files:
            test_file = self.symbol_graph.files[test_file_id]
            if "test" in test_file.file_path.lower():
                test_base_name = Path(test_file.file_path).stem
                if base_name in test_base_name or test_base_name in base_name:
                    test_files.add(test_file_id)
        
        return test_files
    
    def _calculate_blast_radius(
        self,
        affected_symbols: Set[str],
        affected_files: Set[str],
        proposed_changes: List[ProposedChange]
    ) -> BlastRadiusAnalysis:
        """Estimate the blast radius of potential modifications"""
        
        # Calculate direct vs transitive impact
        direct_symbols = set()
        for change in proposed_changes:
            direct_symbols.update(change.target_symbols)
            # Add symbols from target files
            for file_id in change.target_files:
                file = self.symbol_graph.files.get(file_id)
                if file:
                    direct_symbols.update([sym.symbol_id for sym in file.symbols])
        
        transitive_count = len(affected_symbols) - len(direct_symbols)
        direct_count = len(direct_symbols)
        
        # Calculate max depth
        max_depth = self._calculate_max_depth(direct_symbols, affected_symbols)
        
        # Identify critical components affected
        critical_components = self._identify_critical_components(affected_symbols)
        
        # Determine risk level
        risk_level = RiskLevel.LOW
        if len(critical_components) > 0:
            risk_level = RiskLevel.CRITICAL
        elif transitive_count > 20:
            risk_level = RiskLevel.HIGH
        elif transitive_count > 5:
            risk_level = RiskLevel.MEDIUM
        
        return BlastRadiusAnalysis(
            direct_impact_count=direct_count,
            transitive_impact_count=transitive_count,
            max_depth=max_depth,
            critical_components_affected=list(critical_components),
            risk_level=risk_level
        )
    
    def _calculate_max_depth(self, direct_symbols: Set[str], all_symbols: Set[str]) -> int:
        """Calculate the maximum depth of impact"""
        max_depth = 0
        
        for direct_id in direct_symbols:
            # BFS to find max distance from direct symbol
            visited = {direct_id}
            queue = [(direct_id, 0)]
            
            while queue:
                current_id, depth = queue.pop(0)
                max_depth = max(max_depth, depth)
                
                for edge in self.symbol_graph.get_edges_to(current_id):
                    if edge.edge_type in [EdgeType.IMPORTS, EdgeType.CALLS]:
                        if edge.source_id not in visited:
                            visited.add(edge.source_id)
                            queue.append((edge.source_id, depth + 1))
        
        return max_depth
    
    def _identify_critical_components(self, affected_symbols: Set[str]) -> Set[str]:
        """Identify which affected components are critical"""
        critical_components = set()
        
        # Critical indicators
        critical_keywords = ["config", "auth", "security", "database", "storage", "core"]
        
        for symbol_id in affected_symbols:
            symbol = self.symbol_graph.symbols.get(symbol_id)
            if symbol:
                # Check naming
                symbol_name_lower = symbol.symbol_name.lower()
                if any(kw in symbol_name_lower for kw in critical_keywords):
                    critical_components.add(symbol_id)
                    continue
                
                # Check dependency count (highly depended upon)
                dependents = len([e for e in self.symbol_graph.get_edges_to(symbol_id)])
                if dependents > 10:
                    critical_components.add(symbol_id)
        
        return critical_components
    
    def _predict_cascading_failures(
        self,
        proposed_changes: List[ProposedChange],
        affected_symbols: Set[str]
    ) -> List[FailurePath]:
        """Predict potential cascading failure paths"""
        failure_paths = []
        
        # For each proposed change, identify potential failure paths
        for change in proposed_changes:
            for symbol_id in change.target_symbols:
                if symbol_id in self.symbol_graph.symbols:
                    # Find chains of dependent symbols
                    paths = self._identify_failure_chains(symbol_id, affected_symbols)
                    failure_paths.extend(paths)
        
        return failure_paths
    
    def _identify_failure_chains(self, start_symbol: str, affected_symbols: Set[str]) -> List[FailurePath]:
        """Identify potential failure chains starting from a symbol"""
        chains = []
        
        # Find dependent symbols that could fail if this symbol fails
        dependents = []
        for edge in self.symbol_graph.get_edges_to(start_symbol):
            if edge.edge_type in [EdgeType.IMPORTS, EdgeType.CALLS]:
                dependents.append(edge.source_id)
        
        # Build failure chains
        for dependent_id in dependents:
            chain = [start_symbol]
            current_id = dependent_id
            
            # Follow dependency chain
            for _ in range(5):  # Limit chain length
                chain.append(current_id)
                
                # Find next dependent
                next_dependents = []
                for edge in self.symbol_graph.get_edges_to(current_id):
                    if edge.edge_type in [EdgeType.IMPORTS, EdgeType.CALLS]:
                        next_dependents.append(edge.source_id)
                
                if not next_dependents:
                    break
                
                current_id = next_dependents[0]  # Take first path
            
            if len(chain) > 2:  # Only chains with 3+ symbols
                path = FailurePath(
                    path_id=f"fail_chain_{hash(tuple(chain))}",
                    components=chain,
                    failure_mode="cascading_dependency",
                    probability=0.6,  # Simplified probability
                    impact_severity="high" if len(chain) > 5 else "medium",
                    description="Potential cascading failure through dependencies"
                )
                chains.append(path)
        
        return chains
    
    def _calculate_prediction_confidence(self, proposed_changes: List[ProposedChange]) -> ConfidenceLevel:
        """Calculate confidence level for predictions"""
        if not proposed_changes:
            return ConfidenceLevel.APPROXIMATE
        
        # High confidence if changes are well-specified
        well_specified = sum(1 for change in proposed_changes 
                          if change.target_symbols or change.target_files)
        
        if well_specified == len(proposed_changes):
            return ConfidenceLevel.CERTAIN
        elif well_specified >= len(proposed_changes) * 0.5:
            return ConfidenceLevel.INFERRED
        else:
            return ConfidenceLevel.APPROXIMATE
    
    def estimate_blast_radius(self, symbol_id: str) -> BlastRadiusAnalysis:
        """Estimates blast radius of potential modifications"""
        start = time.time()
        
        # Calculate direct impact
        direct_symbols = {symbol_id}
        transitive_symbols = self._predict_transitive_impact(direct_symbols, max_depth=5)
        
        # Calculate files affected
        direct_files = set()
        transitive_files = set()
        
        if symbol_id in self.symbol_graph.symbols:
            symbol = self.symbol_graph.symbols[symbol_id]
            direct_files.add(symbol.file_id)
        
        for transitive_id in transitive_symbols:
            symbol = self.symbol_graph.symbols.get(transitive_id)
            if symbol:
                transitive_files.add(symbol.file_path)
        
        # Calculate max depth
        max_depth = self._calculate_max_depth(direct_symbols, transitive_symbols)
        
        # Identify critical components
        critical_components = self._identify_critical_components(transitive_symbols)
        
        # Determine risk level
        total_impact = len(transitive_symbols)
        risk_level = RiskLevel.LOW
        if len(critical_components) > 0:
            risk_level = RiskLevel.CRITICAL
        elif total_impact > 20:
            risk_level = RiskLevel.HIGH
        elif total_impact > 5:
            risk_level = RiskLevel.MEDIUM
        
        analysis = BlastRadiusAnalysis(
            direct_impact_count=len(direct_symbols),
            transitive_impact_count=len(transitive_symbols),
            max_depth=max_depth,
            critical_components_affected=list(critical_components),
            risk_level=risk_level
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("estimate_blast_radius", elapsed)
        
        return analysis
