# health_analysis.py - Repository Health Analysis for Repository Intelligence
# Layer 15: Measures and tracks repository health metrics

import time
import logging
from typing import List
from collections import defaultdict

from repo_intelligence.types import GraphSnapshot, EdgeType, PerformanceMetrics
from repo_intelligence.types_extended import (
    ComplexityMetrics, CognitiveComplexityMetrics, CouplingMetrics, CohesionMetrics, ArchitecturalCouplingReport,
    ExactDuplicationGroup, NearDuplicationGroup, ArchitecturalDuplication,
    MaintainabilityIndex, TechnicalDebtItem, TechnicalDebtReport,
    CoverageMetrics, UntestedCriticalPath, PatternConsistencyReport, ArchitecturalViolation, NamingViolation,
    NamingConsistencyReport, HealthReport
)

log = logging.getLogger("aelvo.repo_intelligence.health_analysis")


class HealthAnalysisSystem:
    """Main health analysis system that coordinates all health analyzers"""
    
    def __init__(self):
        self.complexity_analyzer = ComplexityAnalyzer()
        self.coupling_analyzer = CouplingCohesionAnalyzer()
        self.duplication_detector = DuplicationDetector()
        self.maintainability_analyzer = MaintainabilityAnalyzer()
        self.test_analyzer = TestCoverageAnalyzer()
        self.consistency_analyzer = ArchitecturalConsistencyAnalyzer()
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def analyze_health(self, symbol_graph: GraphSnapshot) -> HealthReport:
        """Perform comprehensive repository health analysis"""
        start = time.time()
        
        # Run all health analyzers
        complexity_score = self._analyze_overall_complexity(symbol_graph)
        coupling_score = self._analyze_overall_coupling(symbol_graph)
        cohesion_score = self._analyze_overall_cohesion(symbol_graph)
        duplication_score = self._analyze_duplication_level(symbol_graph)
        maintainability_score = self._analyze_maintainability(symbol_graph)
        test_coverage_score = self._analyze_test_coverage(symbol_graph)
        
        # Calculate overall health score
        overall_health = (complexity_score + coupling_score + cohesion_score + 
                         (1.0 - duplication_score) + maintainability_score + test_coverage_score) / 6.0
        
        health_report = HealthReport(
            repository_id=str(time.time()),  # Could use actual repository ID
            overall_health_score=overall_health,
            complexity_score=complexity_score,
            coupling_score=coupling_score,
            cohesion_score=cohesion_score,
            duplication_score=duplication_score,
            maintainability_score=maintainability_score,
            test_coverage_score=test_coverage_score
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_health", elapsed)
        log.info(f"Health analysis completed: overall health {overall_health:.2f} ({elapsed:.0f}ms)")
        
        return health_report
    
    def _analyze_overall_complexity(self, symbol_graph: GraphSnapshot) -> float:
        """Analyze overall repository complexity"""
        total_complexity = 0.0
        count = 0
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            if symbol.symbol_kind.value in ["function", "method"]:
                metrics = self.complexity_analyzer.compute_cyclomatic_complexity(symbol_id, symbol_graph)
                # Normalize to 0-1 scale (assuming 20 is max complexity)
                normalized = min(metrics.cyclomatic_complexity / 20.0, 1.0)
                total_complexity += normalized
                count += 1
        
        return 1.0 - (total_complexity / count) if count > 0 else 0.5
    
    def _analyze_overall_coupling(self, symbol_graph: GraphSnapshot) -> float:
        """Analyze overall repository coupling"""
        total_coupling = 0.0
        count = 0
        
        for file_id in symbol_graph.files:
            metrics = self.coupling_analyzer.compute_coupling_metrics(file_id, symbol_graph)
            # Normalize to 0-1 scale (higher coupling = worse)
            normalized = min(metrics.coupling_score, 1.0)
            total_coupling += normalized
            count += 1
        
        return 1.0 - (total_coupling / count) if count > 0 else 0.5
    
    def _analyze_overall_cohesion(self, symbol_graph: GraphSnapshot) -> float:
        """Analyze overall repository cohesion"""
        total_cohesion = 0.0
        count = 0
        
        for file_id in symbol_graph.files:
            metrics = self.coupling_analyzer.compute_cohesion_metrics(file_id, symbol_graph)
            total_cohesion += metrics.cohesion_score
            count += 1
        
        return total_cohesion / count if count > 0 else 0.5
    
    def _analyze_duplication_level(self, symbol_graph: GraphSnapshot) -> float:
        """Analyze overall duplication level (higher = worse)"""
        exact_duplicates = self.duplication_detector.detect_exact_duplicates(symbol_graph)
        if not symbol_graph.symbols:
            return 0.0
        
        # Calculate duplication ratio
        duplicate_count = sum(len(g.files) for g in exact_duplicates)
        total_symbols = len(symbol_graph.symbols)
        
        return min(duplicate_count / total_symbols, 1.0) if total_symbols > 0 else 0.0
    
    def _analyze_maintainability(self, symbol_graph: GraphSnapshot) -> float:
        """Analyze overall maintainability"""
        total_maintainability = 0.0
        count = 0
        
        for file_id in symbol_graph.files:
            metrics = self.maintainability_analyzer.compute_maintainability_index(file_id, symbol_graph)
            # Normalize to 0-1 scale
            normalized = metrics.maintainability_index / 100.0
            total_maintainability += normalized
            count += 1
        
        return total_maintainability / count if count > 0 else 0.5
    
    def _analyze_test_coverage(self, symbol_graph: GraphSnapshot) -> float:
        """Analyze overall test coverage"""
        # Simplified test coverage analysis
        # In practice, would integrate with actual test coverage tools
        test_files = 0
        total_files = len(symbol_graph.files)
        
        for file_id, file in symbol_graph.files.items():
            if "test" in file.file_path.lower():
                test_files += 1
        
        # Assume test files represent coverage
        return min(test_files / (total_files / 2), 1.0) if total_files > 0 else 0.0


class ComplexityAnalyzer:
    """Analyzes code complexity"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def compute_cyclomatic_complexity(self, symbol_id: str, symbol_graph: GraphSnapshot) -> ComplexityMetrics:
        """Computes cyclomatic complexity for a function/method"""
        start = time.time()
        
        symbol = symbol_graph.symbols.get(symbol_id)
        if not symbol:
            return ComplexityMetrics()
        
        # Simplified cyclomatic complexity calculation
        # Count decision points based on control flow edges
        complexity = 1  # Base complexity
        
        # Add complexity based on branching patterns in docstring or structure
        if symbol.docstring:
            doc_lower = symbol.docstring.lower()
            complexity += doc_lower.count("if") + doc_lower.count("for") + doc_lower.count("while")
        
        # Consider nesting depth
        complexity += len(symbol.symbol_name.split('_')) // 2  # Simple heuristic
        
        # Parameter count contributes to complexity
        complexity += len(symbol.arguments) // 3
        
        metrics = ComplexityMetrics(
            cyclomatic_complexity=complexity,
            cognitive_complexity=complexity + 1,  # Slightly higher than cyclomatic
            lines_of_code=symbol.line_range[1] - symbol.line_range[0],
            parameter_count=len(symbol.arguments),
            nesting_depth=len(symbol.symbol_name.split('_')),
            complexity_score=min(complexity / 20.0, 1.0)  # Normalize to 0-1
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_cyclomatic_complexity", elapsed)
        
        return metrics
    
    def compute_cognitive_complexity(self, file_id: str, symbol_graph: GraphSnapshot) -> CognitiveComplexityMetrics:
        """Computes cognitive complexity for a file"""
        start = time.time()
        
        file = symbol_graph.files.get(file_id)
        if not file:
            return CognitiveComplexityMetrics(file_id=file_id)
        
        # Calculate complexity for each function
        function_complexities = {}
        for symbol in file.symbols:
            if symbol.symbol_kind.value in ["function", "method"]:
                metrics = self.compute_cyclomatic_complexity(symbol.symbol_id, symbol_graph)
                function_complexities[symbol.symbol_name] = metrics.cyclomatic_complexity
        
        # Calculate overall complexity
        overall_complexity = sum(function_complexities.values()) if function_complexities else 1
        
        # Complexity distribution
        distribution = defaultdict(int)
        for complexity in function_complexities.values():
            if complexity < 5:
                distribution["low"] += 1
            elif complexity < 10:
                distribution["medium"] += 1
            else:
                distribution["high"] += 1
        
        metrics = CognitiveComplexityMetrics(
            file_id=file_id,
            overall_complexity=overall_complexity,
            function_complexities=function_complexities,
            complexity_distribution=dict(distribution)
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_cognitive_complexity", elapsed)
        
        return metrics


class CouplingCohesionAnalyzer:
    """Analyzes coupling and cohesion metrics"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def compute_coupling_metrics(self, file_id: str, symbol_graph: GraphSnapshot) -> CouplingMetrics:
        """Computes coupling metrics for a component"""
        start = time.time()
        
        # Calculate incoming and outgoing dependencies
        incoming = len([e for e in symbol_graph.get_edges_to(file_id) if e.edge_type == EdgeType.IMPORTS])
        outgoing = len([e for e in symbol_graph.get_edges_from(file_id) if e.edge_type == EdgeType.IMPORTS])
        
        # Calculate instability (efferent / (afferent + efferent))
        total = incoming + outgoing
        instability = outgoing / total if total > 0 else 0.0
        
        # Calculate coupling score (0-1, higher = more coupled)
        coupling_score = min((incoming + outgoing) / 20.0, 1.0)
        
        metrics = CouplingMetrics(
            afferent_coupling=incoming,
            efferent_coupling=outgoing,
            instability=instability,
            coupling_score=coupling_score
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_coupling_metrics", elapsed)
        
        return metrics
    
    def compute_cohesion_metrics(self, file_id: str, symbol_graph: GraphSnapshot) -> CohesionMetrics:
        """Computes cohesion metrics for a component"""
        start = time.time()
        
        file = symbol_graph.files.get(file_id)
        if not file:
            return CohesionMetrics()
        
        # Calculate functional cohesion based on symbol names
        symbols = file.symbols
        if not symbols:
            return CohesionMetrics()
        
        # Extract common prefixes/patterns in symbol names
        name_parts = []
        for symbol in symbols:
            parts = symbol.symbol_name.lower().split('_')
            name_parts.extend(parts)
        
        # Count common patterns
        pattern_counts = defaultdict(int)
        for part in name_parts:
            pattern_counts[part] += 1
        
        # Calculate cohesion based on pattern repetition
        if len(name_parts) == 0:
            return CohesionMetrics()
        
        repetition_ratio = sum(count for count in pattern_counts.values() if count > 1) / len(name_parts)
        cohesion_level = min(repetition_ratio * 2, 1.0)
        cohesion_score = cohesion_level
        
        metrics = CohesionMetrics(
            cohesion_level=cohesion_level,
            related_functions=len([s for s in symbols if pattern_counts.get(s.symbol_name.split('_')[0], 0) > 1]),
            total_functions=len(symbols),
            cohesion_score=cohesion_score
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_cohesion_metrics", elapsed)
        
        return metrics
    
    def analyze_architectural_coupling(self, symbol_graph: GraphSnapshot) -> ArchitecturalCouplingReport:
        """Analyzes coupling between architectural layers"""
        start = time.time()
        
        # Simplified layer analysis
        layer_coupling = {}
        
        # Group files by apparent layer (based on path patterns)
        layer_groups = {
            'api': [],
            'service': [],
            'domain': [],
            'infrastructure': [],
            'other': []
        }
        
        for file_id, file in symbol_graph.files.items():
            path_lower = file.file_path.lower()
            if 'api' in path_lower or 'controller' in path_lower:
                layer_groups['api'].append(file_id)
            elif 'service' in path_lower:
                layer_groups['service'].append(file_id)
            elif 'domain' in path_lower or 'model' in path_lower:
                layer_groups['domain'].append(file_id)
            elif 'db' in path_lower or 'config' in path_lower:
                layer_groups['infrastructure'].append(file_id)
            else:
                layer_groups['other'].append(file_id)
        
        # Calculate inter-layer coupling
        for layer_name, file_ids in layer_groups.items():
            coupling = {}
            for file_id in file_ids:
                for edge in symbol_graph.get_edges_from(file_id):
                    if edge.edge_type == EdgeType.IMPORTS:
                        # Find which layer the target belongs to
                        target_layer = 'other'
                        for layer, files in layer_groups.items():
                            if edge.target_id in files:
                                target_layer = layer
                                break
                        
                        if target_layer != layer_name:
                            coupling[target_layer] = coupling.get(target_layer, 0) + 1
            
            layer_coupling[layer_name] = coupling
        
        # Calculate overall coupling score
        total_coupling = sum(sum(coupling.values()) for coupling in layer_coupling.values())
        max_possible = len(symbol_graph.files) * 2  # Heuristic
        overall_score = min(total_coupling / max_possible, 1.0)
        
        # Identify violations (e.g., infrastructure depending on api)
        violations = []
        if 'infrastructure' in layer_coupling:
            if 'api' in layer_coupling['infrastructure']:
                violations.append("Infrastructure layer depends on API layer")
        
        report = ArchitecturalCouplingReport(
            repository_id=str(time.time()),
            layer_coupling=layer_coupling,
            violations=violations,
            overall_coupling_score=overall_score
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_architectural_coupling", elapsed)
        
        return report


class DuplicationDetector:
    """Detects code duplication"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def detect_exact_duplicates(self, symbol_graph: GraphSnapshot) -> List[ExactDuplicationGroup]:
        """Detects exact code duplications"""
        start = time.time()
        
        # Simplified duplication detection based on symbol names and signatures
        symbol_signatures = defaultdict(list)
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            # Create a simple signature based on name and argument count
            signature = f"{symbol.symbol_name}_{len(symbol.arguments)}"
            symbol_signatures[signature].append((symbol_id, symbol.file_path))
        
        # Find duplicates
        duplicate_groups = []
        group_id = 0
        
        for signature, symbols in symbol_signatures.items():
            if len(symbols) > 1:
                group = ExactDuplicationGroup(
                    group_id=f"dup_{group_id}",
                    files=[symbol[1] for symbol in symbols],
                    lines=[],
                    duplication_size=len(symbols),
                    similarity=1.0
                )
                duplicate_groups.append(group)
                group_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_exact_duplicates", elapsed)
        
        return duplicate_groups
    
    def detect_near_duplicates(self, symbol_graph: GraphSnapshot, similarity_threshold: float = 0.8) -> List[NearDuplicationGroup]:
        """Detects near-duplicate code"""
        start = time.time()
        
        # Simplified near-duplication detection
        # In practice, would use more sophisticated text similarity algorithms
        near_duplicates = []
        
        # Group symbols by name similarity
        symbol_groups = defaultdict(list)
        for symbol_id, symbol in symbol_graph.symbols.items():
            base_name = ''.join(c for c in symbol.symbol_name if c.isalnum())
            symbol_groups[base_name.lower()].append(symbol_id)
        
        group_id = 0
        for base_name, symbol_ids in symbol_groups.items():
            if len(symbol_ids) > 1 and len(base_name) > 3:
                group = NearDuplicationGroup(
                    group_id=f"near_dup_{group_id}",
                    files=[symbol_graph.symbols[sid].file_path for sid in symbol_ids],
                    similarity=0.8,  # Simplified
                    differences=[],
                    refactoring_opportunity=True
                )
                near_duplicates.append(group)
                group_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_near_duplicates", elapsed)
        
        return near_duplicates
    
    def detect_architectural_duplication(self, symbol_graph: GraphSnapshot) -> List[ArchitecturalDuplication]:
        """Detects duplicated architectural patterns"""
        start = time.time()
        
        # Simplified architectural duplication detection
        # Look for repeated structural patterns
        file_structures = defaultdict(list)
        
        for file_id, file in symbol_graph.files.items():
            # Create a structure signature based on symbol types
            symbol_types = [sym.symbol_kind.value for sym in file.symbols]
            structure_sig = '-'.join(sorted(symbol_types))
            file_structures[structure_sig].append(file_id)
        
        architectural_duplications = []
        duplication_id = 0
        
        for structure_sig, file_ids in file_structures.items():
            if len(file_ids) > 1:
                dup = ArchitecturalDuplication(
                    duplication_id=f"arch_dup_{duplication_id}",
                    pattern_type=structure_sig,
                    instances=file_ids,
                    convergence_opportunity=len(file_ids) > 2
                )
                architectural_duplications.append(dup)
                duplication_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_architectural_duplication", elapsed)
        
        return architectural_duplications


class MaintainabilityAnalyzer:
    """Analyzes maintainability metrics"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def compute_maintainability_index(self, file_id: str, symbol_graph: GraphSnapshot) -> MaintainabilityIndex:
        """Computes maintainability index for a file"""
        start = time.time()
        
        file = symbol_graph.files.get(file_id)
        if not file:
            return MaintainabilityIndex(file_id=file_id)
        
        # Calculate maintainability factors
        # Complexity (lower is better)
        complexity_analyzer = ComplexityAnalyzer()
        total_complexity = 0
        for symbol in file.symbols:
            if symbol.symbol_kind.value in ["function", "method"]:
                metrics = complexity_analyzer.compute_cyclomatic_complexity(symbol.symbol_id, symbol_graph)
                total_complexity += metrics.cyclomatic_complexity
        
        avg_complexity = total_complexity / len(file.symbols) if file.symbols else 1
        
        # Volume (based on lines of code)
        volume = file.size_bytes / 1000  # Normalized by KB
        
        # Duplication (assume none for now)
        duplication = 0.0
        
        # Test coverage (assume 50% as baseline)
        test_coverage = 0.5
        
        # Calculate maintainability index (simplified formula)
        # MI = 171 - 5.2 * log(V) - 0.23 * C - 16.2 * log(D) (standard formula)
        # Simplified: base - complexity penalty - volume penalty
        maintainability_index = max(100 - (avg_complexity * 2) - (volume * 5), 0)
        
        metrics = MaintainabilityIndex(
            file_id=file_id,
            maintainability_index=maintainability_index,
            complexity=avg_complexity,
            volume=volume,
            duplication=duplication,
            test_coverage=test_coverage
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_maintainability_index", elapsed)
        
        return metrics
    
    def analyze_technical_debt(self, symbol_graph: GraphSnapshot) -> TechnicalDebtReport:
        """Analyzes technical debt across the repository"""
        start = time.time()
        
        # Simplified technical debt analysis
        debt_items = []
        item_id = 0
        
        for file_id in symbol_graph.files:
            metrics = self.compute_maintainability_index(file_id, symbol_graph)
            
            if metrics.maintainability_index < 50:
                debt_item = TechnicalDebtItem(
                    item_id=f"debt_{item_id}",
                    component_id=file_id,
                    debt_type="maintainability",
                    severity="high" if metrics.maintainability_index < 30 else "medium",
                    description=f"Low maintainability index: {metrics.maintainability_index:.1f}",
                    estimated_effort=int((100 - metrics.maintainability_index) / 10)
                )
                debt_items.append(debt_item)
                item_id += 1
        
        # Calculate overall debt score
        total_effort = sum(item.estimated_effort for item in debt_items)
        debt_score = min(total_effort / 100, 1.0)
        
        report = TechnicalDebtReport(
            repository_id=str(time.time()),
            overall_debt_score=debt_score,
            debt_categories={
                "maintainability": debt_score
            },
            priority_debt_items=debt_items[:10],  # Top 10 items
            estimated_remediation_time=total_effort if debt_items else None
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_technical_debt", elapsed)
        
        return report


class TestCoverageAnalyzer:
    """Analyzes test coverage"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def compute_coverage_metrics(self, file_id: str, symbol_graph: GraphSnapshot) -> CoverageMetrics:
        """Computes test coverage metrics for a file"""
        start = time.time()
        
        # Simplified coverage calculation
        # In practice, would integrate with actual coverage tools like pytest-cov
        file = symbol_graph.files.get(file_id)
        if not file:
            return CoverageMetrics(file_id=file_id)
        
        # Heuristic: if file is a test file, assume high coverage
        is_test_file = "test" in file.file_path.lower()
        
        if is_test_file:
            metrics = CoverageMetrics(
                file_id=file_id,
                line_coverage=80.0,
                branch_coverage=70.0,
                function_coverage=90.0,
                overall_coverage=80.0
            )
        else:
            # For non-test files, estimate based on existence of corresponding test file
            has_test = any(
                "test" in f.file_path.lower() and file.file_path.replace('.py', '') in f.file_path
                for f in symbol_graph.files.values()
            )
            
            coverage = 50.0 if has_test else 20.0
            metrics = CoverageMetrics(
                file_id=file_id,
                line_coverage=coverage,
                branch_coverage=coverage * 0.8,
                function_coverage=coverage * 0.9,
                overall_coverage=coverage
            )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("compute_coverage_metrics", elapsed)
        
        return metrics
    
    def identify_untested_critical_paths(self, symbol_graph: GraphSnapshot) -> List[UntestedCriticalPath]:
        """Identifies critical paths lacking test coverage"""
        start = time.time()
        
        untested_paths = []
        
        # Find critical components (highly depended upon)
        criticality_scores = {}
        for symbol_id, symbol in symbol_graph.symbols.items():
            dependents = len([e for e in symbol_graph.get_edges_to(symbol_id)])
            criticality_scores[symbol_id] = dependents
        
        # Consider top 20% as critical
        if criticality_scores:
            threshold = sorted(criticality_scores.values())[int(len(criticality_scores) * 0.8)]
            critical_symbols = [sid for sid, score in criticality_scores.items() if score >= threshold]
            
            path_id = 0
            for symbol_id in critical_symbols:
                file = symbol_graph.files.get(symbol_graph.symbols[symbol_id].file_id)
                if file:
                    coverage = self.compute_coverage_metrics(file.file_id, symbol_graph)
                    
                    if coverage.overall_coverage < 50:
                        path = UntestedCriticalPath(
                            path_id=f"untested_path_{path_id}",
                            components=[symbol_id],
                            criticality="important" if criticality_scores[symbol_id] > threshold * 1.5 else "moderate",
                            risk_exposure=1.0 - (coverage.overall_coverage / 100.0)
                        )
                        untested_paths.append(path)
                        path_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("identify_untested_critical_paths", elapsed)
        
        return untested_paths


class ArchitecturalConsistencyAnalyzer:
    """Analyzes architectural consistency"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def check_pattern_consistency(self, symbol_graph: GraphSnapshot) -> PatternConsistencyReport:
        """Checks consistency of architectural patterns"""
        start = time.time()
        
        # Simplified pattern consistency check
        # Look for naming convention consistency
        naming_patterns = defaultdict(list)
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            naming_style = self._get_naming_style(symbol.symbol_name)
            naming_patterns[naming_style].append(symbol_id)
        
        # Calculate consistency score
        dominant_pattern = max(naming_patterns.values(), key=len) if naming_patterns else []
        total_symbols = len(symbol_graph.symbols)
        consistency_score = len(dominant_pattern) / total_symbols if total_symbols > 0 else 0.5
        
        # Identify inconsistent patterns
        inconsistent_patterns = [pattern for pattern, symbols in naming_patterns.items() 
                               if len(symbols) < len(dominant_pattern) * 0.5]
        
        # Identify pattern drift areas
        pattern_drift_areas = []
        for file_id, file in symbol_graph.files.items():
            file_patterns = [self._get_naming_style(sym.symbol_name) for sym in file.symbols]
            pattern_diversity = len(set(file_patterns))
            if pattern_diversity > 2:  # More than 2 naming styles in a file
                pattern_drift_areas.append(file_id)
        
        report = PatternConsistencyReport(
            repository_id=str(time.time()),
            pattern_consistency_score=consistency_score,
            inconsistent_patterns=inconsistent_patterns,
            pattern_drift_areas=pattern_drift_areas
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("check_pattern_consistency", elapsed)
        
        return report
    
    def _get_naming_style(self, name: str) -> str:
        """Determine naming style (snake_case, camelCase, etc.)"""
        if '_' in name:
            return "snake_case"
        elif name[0].isupper():
            return "PascalCase"
        else:
            return "camelCase"
    
    def detect_architectural_violations(self, symbol_graph: GraphSnapshot) -> List[ArchitecturalViolation]:
        """Detects violations of architectural principles"""
        start = time.time()
        
        violations = []
        violation_id = 0
        
        # Check for common violations
        for file_id, file in symbol_graph.files.items():
            # Check for overly long files
            if file.size_bytes > 10000:  # More than 10KB
                violation = ArchitecturalViolation(
                    violation_id=f"arch_viol_{violation_id}",
                    violation_type="file_size",
                    component_id=file_id,
                    description=f"File exceeds size threshold: {file.size_bytes} bytes",
                    severity="medium"
                )
                violations.append(violation)
                violation_id += 1
            
            # Check for files with too many symbols
            if len(file.symbols) > 50:
                violation = ArchitecturalViolation(
                    violation_id=f"arch_viol_{violation_id}",
                    violation_type="symbol_count",
                    component_id=file_id,
                    description=f"File contains too many symbols: {len(file.symbols)}",
                    severity="medium"
                )
                violations.append(violation)
                violation_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_architectural_violations", elapsed)
        
        return violations
    
    def analyze_naming_consistency(self, symbol_graph: GraphSnapshot) -> NamingConsistencyReport:
        """Analyzes naming convention consistency"""
        start = time.time()
        
        naming_violations = []
        violation_id = 0
        
        # Check for naming convention violations
        for symbol_id, symbol in symbol_graph.symbols.items():
            # Check class naming (should be PascalCase)
            if symbol.symbol_kind.value == "class":
                if not symbol.symbol_name[0].isupper():
                    violation = NamingViolation(
                        violation_id=f"naming_viol_{violation_id}",
                        component_id=symbol_id,
                        expected_pattern="PascalCase",
                        actual_name=symbol.symbol_name,
                        suggestion="Class names should be PascalCase"
                    )
                    naming_violations.append(violation)
                    violation_id += 1
            
            # Check function/method naming (should be snake_case)
            if symbol.symbol_kind.value in ["function", "method"]:
                if not all(c.islower() or c == '_' for c in symbol.symbol_name):
                    violation = NamingViolation(
                        violation_id=f"naming_viol_{violation_id}",
                        component_id=symbol_id,
                        expected_pattern="snake_case",
                        actual_name=symbol.symbol_name,
                        suggestion="Function/method names should be snake_case"
                    )
                    naming_violations.append(violation)
                    violation_id += 1
        
        # Calculate overall consistency
        total_symbols = len(symbol_graph.symbols)
        consistency_score = 1.0 - (len(naming_violations) / total_symbols) if total_symbols > 0 else 0.5
        
        # Identify inconsistent areas
        inconsistent_files = set()
        for violation in naming_violations:
            symbol = symbol_graph.symbols.get(violation.component_id)
            if symbol:
                inconsistent_files.add(symbol.file_id)
        
        report = NamingConsistencyReport(
            repository_id=str(time.time()),
            overall_consistency=consistency_score,
            naming_violations=naming_violations,
            inconsistent_areas=list(inconsistent_files)
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_naming_consistency", elapsed)
        
        return report
