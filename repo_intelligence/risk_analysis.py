# risk_analysis.py - Repository Risk Analyzer for Repository Intelligence
# Layer 11.3: Evaluates repository-wide and component-specific risks

import time
import logging
from typing import Dict, List, Tuple
from collections import defaultdict
from pathlib import Path
from datetime import datetime

from repo_intelligence.types import GraphSnapshot, EdgeType, PerformanceMetrics
from repo_intelligence.types_extended import (
    CouplingRiskReport, RefactorRiskReport, StabilityRiskReport,
    SecurityRiskReport, DependencyRiskReport, RiskLevel
)

log = logging.getLogger("aelvo.repo_intelligence.risk_analysis")


class RepositoryRiskAnalyzer:
    """Evaluates repository-wide and component-specific risks"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def analyze_coupling_risk(self, symbol_id: str, symbol_graph: GraphSnapshot) -> CouplingRiskReport:
        """
        Analyzes coupling risk for a component.
        
        Args:
            symbol_id: ID of the symbol to analyze
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            CouplingRiskReport with coupling risk analysis
        """
        start = time.time()
        
        symbol = symbol_graph.symbols.get(symbol_id)
        if not symbol:
            return CouplingRiskReport(
                component_id=symbol_id,
                coupling_score=0.0,
                incoming_coupling=0,
                outgoing_coupling=0,
                risk_level=RiskLevel.LOW,
                risk_factors=["Symbol not found"],
                timestamp=datetime.now()
            )
        
        # Calculate incoming and outgoing coupling
        incoming_edges = symbol_graph.get_edges_to(symbol_id)
        outgoing_edges = symbol_graph.get_edges_from(symbol_id)
        
        incoming_coupling = len(incoming_edges)
        outgoing_coupling = len(outgoing_edges)
        
        # Calculate coupling score (0-1, higher = more coupled)
        total_symbols = len(symbol_graph.symbols)
        coupling_score = min((incoming_coupling + outgoing_coupling) / max(total_symbols, 1), 1.0)
        
        # Determine risk level
        risk_level = self._calculate_coupling_risk_level(coupling_score, incoming_coupling, outgoing_coupling)
        
        # Identify risk factors
        risk_factors = self._identify_coupling_risk_factors(
            incoming_coupling, outgoing_coupling, coupling_score, symbol_graph
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_coupling_risk", elapsed)
        
        return CouplingRiskReport(
            component_id=symbol_id,
            coupling_score=coupling_score,
            incoming_coupling=incoming_coupling,
            outgoing_coupling=outgoing_coupling,
            risk_level=risk_level,
            risk_factors=risk_factors,
            timestamp=datetime.now()
        )
    
    def _calculate_coupling_risk_level(self, coupling_score: float, incoming: int, outgoing: int) -> RiskLevel:
        """Calculate risk level based on coupling metrics"""
        if coupling_score > 0.8:
            return RiskLevel.CRITICAL
        elif coupling_score > 0.6:
            return RiskLevel.HIGH
        elif coupling_score > 0.3:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    def _identify_coupling_risk_factors(self, incoming: int, outgoing: int, score: float, graph: GraphSnapshot) -> List[str]:
        """Identify specific coupling risk factors"""
        factors = []
        
        if incoming > 20:
            factors.append(f"High incoming coupling ({incoming} dependents)")
        if outgoing > 20:
            factors.append(f"High outgoing coupling ({outgoing} dependencies)")
        if incoming > outgoing * 2:
            factors.append("Disproportionately high incoming coupling (hub component)")
        if outgoing > incoming * 2:
            factors.append("Disproportionately high outgoing coupling (high dependency)")
        if score > 0.7:
            factors.append("Overall coupling score above 70%")
        
        return factors
    
    def analyze_refactor_risk(self, file_id: str, symbol_graph: GraphSnapshot) -> RefactorRiskReport:
        """
        Evaluates risk of refactoring a file.
        
        Args:
            file_id: File ID to analyze
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            RefactorRiskReport with refactor risk analysis
        """
        start = time.time()
        
        file = symbol_graph.files.get(file_id)
        if not file:
            return RefactorRiskReport(
                file_id=file_id,
                refactor_risk_score=0.0,
                complexity_metrics=None,
                dependency_count=0,
                test_coverage=0.0,
                risk_level=RiskLevel.LOW,
                mitigation_suggestions=["File not found"],
                timestamp=datetime.now()
            )
        
        # Calculate complexity metrics
        complexity_metrics = self._calculate_file_complexity(file, symbol_graph)
        
        # Count dependencies
        dependency_count = self._count_file_dependencies(file_id, symbol_graph)
        
        # Estimate test coverage (simplified)
        test_coverage = self._estimate_test_coverage(file, symbol_graph)
        
        # Calculate refactor risk score (0-1, higher = riskier)
        complexity_score = complexity_metrics.get("cyclomatic_avg", 0) / 20.0
        dependency_score = min(dependency_count / 50.0, 1.0)
        coverage_penalty = (1.0 - test_coverage) * 0.5
        
        refactor_risk_score = min(complexity_score * 0.4 + dependency_score * 0.4 + coverage_penalty, 1.0)
        
        # Determine risk level
        risk_level = self._calculate_refactor_risk_level(refactor_risk_score, test_coverage)
        
        # Generate mitigation suggestions
        mitigation_suggestions = self._generate_refactor_mitigations(
            complexity_metrics, dependency_count, test_coverage
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_refactor_risk", elapsed)
        
        return RefactorRiskReport(
            file_id=file_id,
            refactor_risk_score=refactor_risk_score,
            complexity_metrics=complexity_metrics,
            dependency_count=dependency_count,
            test_coverage=test_coverage,
            risk_level=risk_level,
            mitigation_suggestions=mitigation_suggestions,
            timestamp=datetime.now()
        )
    
    def _calculate_file_complexity(self, file, symbol_graph: GraphSnapshot) -> Dict[str, float]:
        """Calculate complexity metrics for a file"""
        total_complexity = 0
        symbol_count = 0
        max_complexity = 0
        total_lines = 0
        
        for symbol in file.symbols:
            if symbol.symbol_kind.value in ["function", "method"]:
                # Simplified cyclomatic complexity based on structure
                complexity = 1
                complexity += len(symbol.arguments) // 2
                complexity += (symbol.line_range[1] - symbol.line_range[0]) // 20
                total_complexity += complexity
                max_complexity = max(max_complexity, complexity)
                symbol_count += 1
            total_lines += symbol.line_range[1] - symbol.line_range[0]
        
        return {
            "cyclomatic_avg": total_complexity / max(symbol_count, 1),
            "cyclomatic_max": max_complexity,
            "symbol_count": symbol_count,
            "total_lines": total_lines
        }
    
    def _count_file_dependencies(self, file_id: str, symbol_graph: GraphSnapshot) -> int:
        """Count dependencies for a file"""
        file = symbol_graph.files.get(file_id)
        if not file:
            return 0
        
        unique_imports = set()
        for symbol in file.symbols:
            for edge in symbol_graph.get_edges_from(symbol.symbol_id):
                if edge.edge_type == EdgeType.IMPORTS:
                    unique_imports.add(edge.target_id)
        
        return len(unique_imports)
    
    def _estimate_test_coverage(self, file, symbol_graph: GraphSnapshot) -> float:
        """Estimate test coverage for a file"""
        # Look for corresponding test files
        base_name = Path(file.file_path).stem
        test_files = 0
        
        for fid, f in symbol_graph.files.items():
            if "test" in f.file_path.lower():
                if base_name in Path(f.file_path).stem or Path(f.file_path).stem in base_name:
                    test_files += 1
        
        # Simplified coverage estimate
        return min(test_files / 2.0, 1.0)
    
    def _calculate_refactor_risk_level(self, risk_score: float, test_coverage: float) -> RiskLevel:
        """Calculate refactor risk level"""
        if risk_score > 0.7 or test_coverage < 0.2:
            return RiskLevel.CRITICAL
        elif risk_score > 0.5 or test_coverage < 0.4:
            return RiskLevel.HIGH
        elif risk_score > 0.3:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    def _generate_refactor_mitigations(self, complexity: Dict, deps: int, coverage: float) -> List[str]:
        """Generate mitigation suggestions for refactor risk"""
        suggestions = []
        
        if complexity.get("cyclomatic_avg", 0) > 10:
            suggestions.append("Consider breaking down complex functions into smaller units")
        if complexity.get("cyclomatic_max", 0) > 20:
            suggestions.append("Extract highly complex functions into separate modules")
        if deps > 20:
            suggestions.append("Reduce dependencies through interface extraction")
        if coverage < 0.5:
            suggestions.append("Increase test coverage before refactoring")
        if coverage < 0.3:
            suggestions.append("Add integration tests for critical paths")
        
        if not suggestions:
            suggestions.append("Refactor risk is manageable with standard testing")
        
        return suggestions
    
    def compute_stability_risk(self, symbol_graph: GraphSnapshot) -> StabilityRiskReport:
        """
        Computes overall repository stability risk.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            StabilityRiskReport with stability risk analysis
        """
        start = time.time()
        
        # Calculate stability scores for each component
        component_stability_scores = {}
        fragile_components = []
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            # Stability = 1 - (outgoing / (incoming + outgoing))
            # More stable components have more dependents than dependencies
            incoming = len(symbol_graph.get_edges_to(symbol_id))
            outgoing = len(symbol_graph.get_edges_from(symbol_id))
            
            total = incoming + outgoing
            if total > 0:
                stability = 1.0 - (outgoing / total)
            else:
                stability = 0.5
            
            component_stability_scores[symbol_id] = stability
            
            # Identify fragile components (low stability, high coupling)
            if stability < 0.3 and (incoming + outgoing) > 10:
                fragile_components.append(symbol_id)
        
        # Calculate overall stability score
        if component_stability_scores:
            overall_stability = sum(component_stability_scores.values()) / len(component_stability_scores)
        else:
            overall_stability = 0.5
        
        # Determine risk trend (simplified - would need historical data)
        risk_trend = "stable"  # Could be improving, stable, degrading with historical data
        
        # Determine risk level
        if overall_stability < 0.4:
            pass
        elif overall_stability < 0.6:
            pass
        elif overall_stability < 0.75:
            pass
        else:
            pass
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_stability_risk", elapsed)
        
        return StabilityRiskReport(
            repository_id=str(time.time()),
            overall_stability_score=overall_stability,
            component_stability_scores=component_stability_scores,
            fragile_components=fragile_components,
            risk_trend=risk_trend,
            timestamp=datetime.now()
        )
    
    def analyze_security_risk(self, symbol_id: str, symbol_graph: GraphSnapshot) -> SecurityRiskReport:
        """
        Analyzes security-related risks for a component.
        
        Args:
            symbol_id: ID of the symbol to analyze
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            SecurityRiskReport with security risk analysis
        """
        start = time.time()
        
        symbol = symbol_graph.symbols.get(symbol_id)
        if not symbol:
            return SecurityRiskReport(
                symbol_id=symbol_id,
                security_risk_score=0.0,
                risk_categories=[],
                sensitive_data_access=False,
                external_interaction=False,
                risk_level=RiskLevel.LOW,
                recommendations=[],
                timestamp=datetime.now()
            )
        
        # Analyze security risk factors
        risk_categories = []
        sensitive_data_access = False
        external_interaction = False
        security_risk_score = 0.0
        
        # Check symbol name and docstring for security-related keywords
        symbol_name_lower = symbol.symbol_name.lower()
        docstring_lower = (symbol.docstring or "").lower()
        
        security_keywords = {
            "password": 0.3,
            "secret": 0.3,
            "key": 0.2,
            "token": 0.3,
            "auth": 0.2,
            "credential": 0.3,
            "encrypt": 0.2,
            "decrypt": 0.2,
            "hash": 0.1,
            "salt": 0.2,
            "crypto": 0.2
        }
        
        for keyword, risk in security_keywords.items():
            if keyword in symbol_name_lower or keyword in docstring_lower:
                security_risk_score += risk
                risk_categories.append(f"Security-related keyword: {keyword}")
                if keyword in ["password", "secret", "key", "token", "credential"]:
                    sensitive_data_access = True
        
        # Check for external interaction patterns
        external_keywords = ["http", "api", "request", "fetch", "download", "upload", "network"]
        if any(kw in symbol_name_lower or kw in docstring_lower for kw in external_keywords):
            external_interaction = True
            security_risk_score += 0.2
            risk_categories.append("External network interaction")
        
        # Check dependencies on security-sensitive modules
        for edge in symbol_graph.get_edges_from(symbol_id):
            target = symbol_graph.symbols.get(edge.target_id)
            if target:
                target_name_lower = target.symbol_name.lower()
                if any(kw in target_name_lower for kw in ["crypto", "ssl", "tls", "security"]):
                    security_risk_score += 0.1
                    risk_categories.append("Depends on security module")
        
        # Normalize score to 0-1
        security_risk_score = min(security_risk_score, 1.0)
        
        # Determine risk level
        if security_risk_score > 0.7:
            risk_level = RiskLevel.CRITICAL
        elif security_risk_score > 0.5:
            risk_level = RiskLevel.HIGH
        elif security_risk_score > 0.3:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW
        
        # Generate recommendations
        recommendations = self._generate_security_recommendations(
            risk_categories, sensitive_data_access, external_interaction
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_security_risk", elapsed)
        
        return SecurityRiskReport(
            symbol_id=symbol_id,
            security_risk_score=security_risk_score,
            risk_categories=risk_categories,
            sensitive_data_access=sensitive_data_access,
            external_interaction=external_interaction,
            risk_level=risk_level,
            recommendations=recommendations,
            timestamp=datetime.now()
        )
    
    def _generate_security_recommendations(self, categories: List[str], sensitive: bool, external: bool) -> List[str]:
        """Generate security recommendations"""
        recommendations = []
        
        if sensitive:
            recommendations.append("Ensure sensitive data is properly encrypted at rest and in transit")
            recommendations.append("Use secure credential management (e.g., environment variables, secret managers)")
            recommendations.append("Implement proper access controls and auditing")
        
        if external:
            recommendations.append("Validate and sanitize all external inputs")
            recommendations.append("Implement rate limiting and authentication for external endpoints")
            recommendations.append("Use HTTPS and secure protocols for network communication")
        
        if "crypto" in " ".join(categories).lower():
            recommendations.append("Review cryptographic implementations for best practices")
            recommendations.append("Use well-vetted cryptographic libraries")
        
        if not recommendations:
            recommendations.append("No specific security risks identified, but follow general security best practices")
        
        return recommendations
    
    def compute_dependency_risk(self, symbol_graph: GraphSnapshot) -> DependencyRiskReport:
        """
        Evaluates dependency-related risks for the repository.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            DependencyRiskReport with dependency risk analysis
        """
        start = time.time()
        
        # Analyze internal dependencies
        dependency_health_score = 1.0
        outdated_dependencies = []
        vulnerable_dependencies = []
        unused_dependencies = []
        
        # Check for orphaned files (no incoming dependencies)
        orphaned_files = []
        for file_id, file in symbol_graph.files.items():
            has_incoming = any(
                edge.edge_type in [EdgeType.IMPORTS, EdgeType.REFERENCES]
                for edge in symbol_graph.edges
                if any(sym.symbol_id == edge.target_id for sym in file.symbols)
            )
            if not has_incoming and not file.file_path.lower().endswith("__init__.py"):
                orphaned_files.append(file_id)
                unused_dependencies.append(file_id)
        
        # Check for circular dependencies
        circular_deps = self._detect_circular_dependencies(symbol_graph)
        
        # Calculate dependency health score
        penalty = len(orphaned_files) * 0.05 + len(circular_deps) * 0.1
        dependency_health_score = max(1.0 - penalty, 0.0)
        
        # Determine risk level
        if dependency_health_score < 0.5:
            risk_level = RiskLevel.CRITICAL
        elif dependency_health_score < 0.7:
            risk_level = RiskLevel.HIGH
        elif dependency_health_score < 0.9:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_dependency_risk", elapsed)
        
        return DependencyRiskReport(
            repository_id=str(time.time()),
            dependency_health_score=dependency_health_score,
            outdated_dependencies=outdated_dependencies,
            vulnerable_dependencies=vulnerable_dependencies,
            unused_dependencies=unused_dependencies,
            risk_level=risk_level,
            timestamp=datetime.now()
        )
    
    def _detect_circular_dependencies(self, symbol_graph: GraphSnapshot) -> List[Tuple[str, str]]:
        """Detect circular dependencies in the graph"""
        circular_deps = []
        
        # Build adjacency list for imports
        adj = defaultdict(set)
        for edge in symbol_graph.edges:
            if edge.edge_type == EdgeType.IMPORTS:
                adj[edge.source_id].add(edge.target_id)
        
        # Detect cycles using DFS
        visited = set()
        rec_stack = set()
        
        def dfs(node, path):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in adj[node]:
                if neighbor not in visited:
                    result = dfs(neighbor, path + [node])
                    if result:
                        return result
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    return tuple(cycle)
            
            rec_stack.remove(node)
            return None
        
        for node in adj:
            if node not in visited:
                cycle = dfs(node, [])
                if cycle:
                    circular_deps.append(cycle)
        
        return circular_deps
