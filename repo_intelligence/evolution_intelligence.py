# evolution_intelligence.py - Repository Evolution Intelligence for Repository Intelligence
# Layer 17: Predicts future bottlenecks, scaling issues, and evolution patterns

import time
import logging
from typing import List
from collections import defaultdict

from repo_intelligence.types import GraphSnapshot, EdgeType, PerformanceMetrics
from repo_intelligence.types_extended import (
    ScalingBottleneck,
    MaintenanceEffortPrediction, TechnicalDebtPrediction,
    DependencyGrowthPrediction, ObsoleteDependencyPrediction,
    EvolutionReport, RiskLevel
)

log = logging.getLogger("aelvo.repo_intelligence.evolution_intelligence")


class RepositoryEvolutionIntelligence:
    """Predicts future bottlenecks, scaling issues, and repository evolution patterns"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def predict_bottlenecks(self, symbol_graph: GraphSnapshot) -> List[ScalingBottleneck]:
        """
        Predicts scaling, performance, and complexity bottlenecks.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            List of predicted bottlenecks
        """
        start = time.time()
        
        bottlenecks = []
        
        # Predict performance bottlenecks
        performance_bottlenecks = self._predict_performance_bottlenecks(symbol_graph)
        for pb in performance_bottlenecks:
            bottlenecks.append(ScalingBottleneck(
                bottleneck_id=f"perf_{pb['function_id']}",
                bottleneck_type="performance",
                affected_components=["repository"],
                current_capacity=0.0,
                predicted_limit=0.0,
                time_to_limit=pb.get("timeframe_months", 6),
                recommendations=pb.get("mitigation_strategies", [])
            ))
        
        # Predict complexity bottlenecks
        complexity_bottlenecks = self._predict_complexity_bottlenecks(symbol_graph)
        for cb in complexity_bottlenecks:
            bottlenecks.append(ScalingBottleneck(
                bottleneck_id=f"complexity_{cb['module_id']}",
                bottleneck_type="complexity",
                affected_components=["repository"],
                current_capacity=0.0,
                predicted_limit=0.0,
                time_to_limit=cb.get("timeframe_months", 12),
                recommendations=cb.get("mitigation_strategies", [])
            ))
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("predict_bottlenecks", elapsed)
        
        return bottlenecks
    
    def _predict_performance_bottlenecks(self, symbol_graph: GraphSnapshot) -> list:
        """Predict performance bottlenecks based on code structure"""
        bottlenecks = []
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            if symbol.symbol_kind.value not in ["function", "method"]:
                continue
            
            # Analyze function characteristics
            lines = symbol.line_range[1] - symbol.line_range[0]
            param_count = len(symbol.arguments)
            
            # Performance risk factors
            risk_score = 0.0
            risk_factors = []
            
            # Large functions may indicate performance issues
            if lines > 100:
                risk_score += 0.3
                risk_factors.append("Large function size")
            
            # High parameter count may indicate complex operations
            if param_count > 5:
                risk_score += 0.2
                risk_factors.append("High parameter count")
            
            # Check for loops in docstring (heuristic)
            if symbol.docstring:
                if "for" in symbol.docstring.lower() or "while" in symbol.docstring.lower():
                    risk_score += 0.2
                    risk_factors.append("Potential loop operations")
            
            # Check for recursion patterns
            if "recursive" in (symbol.docstring or "").lower():
                risk_score += 0.3
                risk_factors.append("Recursive pattern")
            
            if risk_score > 0.4:
                bottlenecks.append({
                    "function_id": symbol_id,
                    "severity": RiskLevel.CRITICAL if risk_score > 0.7 else (RiskLevel.HIGH if risk_score > 0.5 else RiskLevel.MEDIUM),
                    "impact_description": f"Performance bottleneck predicted: {', '.join(risk_factors)}",
                    "timeframe_months": 6,
                    "mitigation_strategies": [
                        "Profile function to identify hot paths",
                        "Consider memoization or caching",
                        "Break down large functions",
                        "Optimize algorithm complexity"
                    ]
                })
        
        return bottlenecks
    
    def _predict_complexity_bottlenecks(self, symbol_graph: GraphSnapshot) -> list:
        """Predict complexity bottlenecks based on module structure"""
        bottlenecks = []
        
        # Analyze file-level complexity
        for file_id, file in symbol_graph.files.items():
            symbol_count = len(file.symbols)
            total_lines = sum(s.line_range[1] - s.line_range[0] for s in file.symbols)
            
            # Complexity risk factors
            risk_score = 0.0
            risk_factors = []
            
            # Files with many symbols may be hard to maintain
            if symbol_count > 20:
                risk_score += 0.3
                risk_factors.append(f"High symbol count ({symbol_count})")
            
            # Large files may be hard to navigate
            if total_lines > 500:
                risk_score += 0.3
                risk_factors.append(f"Large file size ({total_lines} lines)")
            
            # Check for high coupling
            incoming_deps = len([
                e for e in symbol_graph.edges
                if e.target_id in [s.symbol_id for s in file.symbols]
            ])
            
            if incoming_deps > 30:
                risk_score += 0.3
                risk_factors.append(f"High incoming dependencies ({incoming_deps})")
            
            if risk_score > 0.4:
                bottlenecks.append({
                    "module_id": file_id,
                    "severity": RiskLevel.CRITICAL if risk_score > 0.7 else (RiskLevel.HIGH if risk_score > 0.5 else RiskLevel.MEDIUM),
                    "impact_description": f"Complexity bottleneck predicted: {', '.join(risk_factors)}",
                    "timeframe_months": 12,
                    "mitigation_strategies": [
                        "Split large files into smaller modules",
                        "Extract related functionality into separate classes",
                        "Reduce coupling through interface segregation",
                        "Apply single responsibility principle"
                    ]
                })
        
        return bottlenecks
    
    def predict_scaling_issues(self, symbol_graph: GraphSnapshot) -> list:
        """
        Predicts data, team, and dependency scaling issues.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            List of predicted scaling issues (dicts for polymorphic structure)
        """
        start = time.time()
        
        scaling_issues = []
        
        # Predict data scaling issues
        data_issues = self._predict_data_scaling_issues(symbol_graph)
        scaling_issues.extend(data_issues)
        
        # Predict team scaling issues
        team_issues = self._predict_team_scaling_issues(symbol_graph)
        scaling_issues.extend(team_issues)
        
        # Predict dependency scaling issues
        dep_issues = self._predict_dependency_scaling_issues(symbol_graph)
        scaling_issues.extend(dep_issues)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("predict_scaling_issues", elapsed)
        
        return scaling_issues
    
    def _predict_data_scaling_issues(self, symbol_graph: GraphSnapshot) -> list:
        """Predict data scaling issues"""
        issues = []
        
        # Look for data-intensive patterns
        data_keywords = ["database", "storage", "cache", "queue", "stream", "batch", "bulk"]
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            symbol_name_lower = symbol.symbol_name.lower()
            docstring_lower = (symbol.docstring or "").lower()
            
            data_score = 0.0
            patterns = []
            
            for keyword in data_keywords:
                if keyword in symbol_name_lower or keyword in docstring_lower:
                    data_score += 0.2
                    patterns.append(f"Data pattern: {keyword}")
            
            # Check for iteration/processing patterns
            if "iterate" in docstring_lower or "process" in docstring_lower or "batch" in docstring_lower:
                data_score += 0.3
                patterns.append("Batch processing pattern")
            
            if data_score > 0.4:
                issues.append({
                    "symbol_id": symbol_id,
                    "data_score": data_score,
                    "patterns": patterns,
                    "mitigation_strategies": [
                        "Implement pagination for large datasets",
                        "Consider data sharding or partitioning",
                        "Add caching layers",
                        "Implement lazy loading strategies"
                    ]
                })
        
        return issues
    
    def _predict_team_scaling_issues(self, symbol_graph: GraphSnapshot) -> list:
        """Predict team scaling issues"""
        issues = []
        
        # Analyze module overlap and potential merge conflicts
        file_coupling = defaultdict(set)
        for edge in symbol_graph.edges:
            if edge.edge_type in [EdgeType.IMPORTS, EdgeType.REFERENCES]:
                source_file = symbol_graph.symbols.get(edge.source_id)
                target_file = symbol_graph.symbols.get(edge.target_id)
                if source_file and target_file:
                    file_coupling[source_file.file_id].add(target_file.file_id)
        
        # Identify highly coupled files (potential merge conflict zones)
        for file_id, dependencies in file_coupling.items():
            if len(dependencies) > 15:
                issues.append({
                    "file_id": file_id,
                    "dependency_count": len(dependencies),
                    "mitigation_strategies": [
                        "Reduce coupling through interface extraction",
                        "Implement feature toggles",
                        "Improve modular boundaries",
                        "Establish clear ownership patterns"
                    ]
                })
        
        return issues
    
    def _predict_dependency_scaling_issues(self, symbol_graph: GraphSnapshot) -> list:
        """Predict dependency scaling issues"""
        issues = []
        
        # Calculate dependency growth rate (simplified - would need historical data)
        total_deps = len([
            e for e in symbol_graph.edges
            if e.edge_type == EdgeType.IMPORTS
        ])
        total_symbols = len(symbol_graph.symbols)
        
        if total_symbols > 0:
            dep_ratio = total_deps / total_symbols
        else:
            dep_ratio = 0
        
        # High dependency ratio may indicate future scaling issues
        if dep_ratio > 3.0:
            issues.append({
                "dep_ratio": dep_ratio,
                "predicted_growth_rate": 1.2,
                "mitigation_strategies": [
                    "Consolidate similar dependencies",
                    "Remove unused dependencies",
                    "Implement dependency injection",
                    "Use facade pattern for complex subsystems"
                ]
            })
        
        return issues
    
    def predict_maintenance_effort(self, symbol_graph: GraphSnapshot) -> MaintenanceEffortPrediction:
        """
        Predicts maintenance effort for the repository.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            MaintenanceEffortPrediction with maintenance effort analysis
        """
        start = time.time()
        
        # Calculate complexity metrics
        total_complexity = 0
        total_symbols = 0
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            if symbol.symbol_kind.value in ["function", "method"]:
                # Simplified complexity calculation
                complexity = 1 + len(symbol.arguments) // 2
                lines = symbol.line_range[1] - symbol.line_range[0]
                complexity += lines // 20
                total_complexity += complexity
                total_symbols += 1
        
        # Calculate coupling
        avg_coupling = 0
        if symbol_graph.symbols:
            total_coupling = sum(
                len(symbol_graph.get_edges_to(sid)) + len(symbol_graph.get_edges_from(sid))
                for sid in symbol_graph.symbols
            )
            avg_coupling = total_coupling / len(symbol_graph.symbols)
        
        # Estimate maintenance effort (hours per month)
        base_effort = total_complexity * 0.5  # Base effort from complexity
        coupling_effort = avg_coupling * 0.3  # Additional effort from coupling
        total_effort = base_effort + coupling_effort
        
        # Normalize effort
        if total_symbols > 0:
            total_effort / total_symbols
        else:
            pass
        
        # Build contributing factors
        factors = []
        if base_effort > 50:
            factors.append(f"High complexity base effort ({base_effort:.1f})")
        if coupling_effort > 20:
            factors.append(f"High coupling overhead ({coupling_effort:.1f})")
        if total_symbols > 0:
            factors.append(f"{total_symbols} functions to maintain")
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("predict_maintenance_effort", elapsed)
        
        return MaintenanceEffortPrediction(
            component_id="repository",
            predicted_effort=int(total_effort),
            confidence=0.7 if total_symbols > 10 else 0.4,
            contributing_factors=factors,
            time_horizon=6
        )
    
    def predict_technical_debt(self, symbol_graph: GraphSnapshot) -> TechnicalDebtPrediction:
        """
        Predicts technical debt accumulation.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            TechnicalDebtPrediction with technical debt analysis
        """
        start = time.time()
        
        debt_items = []
        
        # Identify potential debt indicators
        for symbol_id, symbol in symbol_graph.symbols.items():
            if symbol.symbol_kind.value not in ["function", "method"]:
                continue
            
            lines = symbol.line_range[1] - symbol.line_range[0]
            debt_indicators = []
            
            # Long functions (potential debt)
            if lines > 100:
                debt_indicators.append(f"Long function ({lines} lines)")
            
            # High parameter count (potential debt)
            if len(symbol.arguments) > 5:
                debt_indicators.append(f"High parameter count ({len(symbol.arguments)})")
            
            # TODO comments in docstring
            if symbol.docstring and "todo" in symbol.docstring.lower():
                debt_indicators.append("TODO marker found")
            
            # Missing docstring
            if not symbol.docstring:
                debt_indicators.append("Missing documentation")
            
            if debt_indicators:
                debt_items.append({
                    "symbol_id": symbol_id,
                    "indicators": debt_indicators,
                    "severity": RiskLevel.MEDIUM
                })
        
        # Calculate debt score
        debt_score = min(len(debt_items) / max(len(symbol_graph.symbols), 1), 1.0)
        
        # Predict debt accumulation rate (simplified)
        accumulation_rate = debt_score * 0.1  # 10% growth per month (simplified)
        
        # Build intervention points
        intervention_points = []
        for item in debt_items[:5]:
            intervention_points.append(f"Refactor {item['symbol_id']}: {', '.join(item['indicators'][:2])}")
        
        predicted_debt = min(debt_score + accumulation_rate * 12, 1.0)
        
        # Calculate time to critical
        time_to_critical = None
        if accumulation_rate > 0:
            remaining_to_critical = max(0.8 - debt_score, 0)
            months_to_critical = int(remaining_to_critical / accumulation_rate) if accumulation_rate > 0 else None
            if months_to_critical and months_to_critical > 0:
                time_to_critical = months_to_critical
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("predict_technical_debt", elapsed)
        
        return TechnicalDebtPrediction(
            repository_id=str(time.time()),
            current_debt=debt_score,
            predicted_debt=predicted_debt,
            debt_accumulation_rate=accumulation_rate,
            time_to_critical=time_to_critical,
            intervention_points=intervention_points
        )
    
    def predict_dependency_growth(self, symbol_graph: GraphSnapshot) -> DependencyGrowthPrediction:
        """
        Predicts dependency growth and obsolescence.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            DependencyGrowthPrediction with dependency growth analysis
        """
        start = time.time()
        
        # Count current dependencies
        current_dependency_count = len([
            e for e in symbol_graph.edges
            if e.edge_type == EdgeType.IMPORTS
        ])
        
        # Calculate growth rate (simplified - would need historical data)
        # Assuming 10% growth rate based on typical projects
        growth_rate = 0.1
        
        # Predict future dependency counts
        predicted_dependency_count = int(current_dependency_count * (1 + growth_rate * 6))
        
        # Identify concerning dependencies (orphaned files with no deps but high symbol count)
        concerning = []
        for file_id, file in symbol_graph.files.items():
            file_deps = 0
            for symbol in file.symbols:
                for edge in symbol_graph.get_edges_from(symbol.symbol_id):
                    if edge.edge_type == EdgeType.IMPORTS:
                        file_deps += 1
            if file_deps == 0 and len(file.symbols) > 10:
                concerning.append(file_id)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("predict_dependency_growth", elapsed)
        
        return DependencyGrowthPrediction(
            repository_id=str(time.time()),
            current_dependency_count=current_dependency_count,
            predicted_dependency_count=predicted_dependency_count,
            growth_rate=growth_rate,
            time_horizon=6,
            concerning_dependencies=concerning[:10]
        )
    
    def predict_obsolete_dependencies(self, symbol_graph: GraphSnapshot) -> ObsoleteDependencyPrediction:
        """
        Predicts obsolete dependencies.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            ObsoleteDependencyPrediction with obsolete dependency analysis
        """
        start = time.time()
        
        # Analyze dependency patterns to identify potentially obsolete ones
        deprecation_signs = []
        
        # Look for unused imports
        for file_id, file in symbol_graph.files.items():
            file_imports = set()
            for symbol in file.symbols:
                for edge in symbol_graph.get_edges_from(symbol.symbol_id):
                    if edge.edge_type == EdgeType.IMPORTS:
                        file_imports.add(edge.target_id)
            
            # Check if imports are actually used
            for import_id in file_imports:
                import_symbol = symbol_graph.symbols.get(import_id)
                if import_symbol:
                    # Check if this import is referenced in the file
                    is_used = any(
                        edge.edge_type == EdgeType.REFERENCES or edge.edge_type == EdgeType.CALLS
                        for symbol in file.symbols
                        for edge in symbol_graph.get_edges_from(symbol.symbol_id)
                        if edge.target_id == import_id
                    )
                    
                    if not is_used:
                        deprecation_signs.append(f"Unused import {import_symbol.symbol_name} in {file_id}")
        
        # Determine a migration path suggestion
        migration_path = "Review and remove unused imports across the codebase" if deprecation_signs else None
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("predict_obsolete_dependencies", elapsed)
        
        return ObsoleteDependencyPrediction(
            dependency_id=str(time.time()),
            current_version="current",
            predicted_obsolescence_date=None,
            deprecation_signs=deprecation_signs[:20],
            migration_path=migration_path
        )
    
    def generate_evolution_report(self, symbol_graph: GraphSnapshot) -> EvolutionReport:
        """
        Generates a comprehensive evolution report.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            EvolutionReport with comprehensive evolution intelligence
        """
        start = time.time()
        
        # Run all evolution predictions
        bottlenecks = self.predict_bottlenecks(symbol_graph)
        scaling_issues = self.predict_scaling_issues(symbol_graph)
        maintenance = self.predict_maintenance_effort(symbol_graph)
        self.predict_technical_debt(symbol_graph)
        dep_growth = self.predict_dependency_growth(symbol_graph)
        self.predict_obsolete_dependencies(symbol_graph)
        
        # Calculate overall evolution risk score
        len(bottlenecks) / max(len(symbol_graph.symbols), 1) * 10
        len(scaling_issues) / max(len(symbol_graph.symbols), 1) * 10
        
        maintenance_cost_risk = min(maintenance.predicted_effort / 100.0, 1.0) if maintenance.predicted_effort else 0.5
        dependency_growth_risk = min(dep_growth.growth_rate * 3, 1.0) if dep_growth.growth_rate else 0.5
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("generate_evolution_report", elapsed)
        
        return EvolutionReport(
            repository_id=str(time.time()),
            prediction_horizon=6,
            predicted_bottlenecks=len(bottlenecks),
            scaling_concerns=len(scaling_issues),
            maintenance_cost_risk=round(maintenance_cost_risk, 4),
            dependency_growth_risk=round(dependency_growth_risk, 4),
        )
