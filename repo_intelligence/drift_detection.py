# drift_detection.py - Architectural Drift Detection for Repository Intelligence
# Layer 16: Detects architectural drift and decay

import time
import logging
from typing import Dict, List, Optional, Set
from collections import defaultdict
from pathlib import Path

from repo_intelligence.types import GraphSnapshot, EdgeType, PerformanceMetrics
from repo_intelligence.types_extended import (
    FunctionalDuplication, PatternDrift, CompetingSubsystem, SubsystemOverlap,
    UnusedAbstraction, DeadArchitecture, SubsystemDivergence, ArchitecturalDecay,
    DriftReport, ConfidenceLevel
)

log = logging.getLogger("aelvo.repo_intelligence.drift_detection")


class DriftDetectionSystem:
    """Main drift detection system that coordinates all drift detectors"""
    
    def __init__(self):
        self.duplicate_detector = DuplicatedImplementationDetector()
        self.competing_detector = CompetingSubsystemDetector()
        self.unused_detector = UnusedAbstractionDetector()
        self.divergence_analyzer = SubsystemDivergenceAnalyzer()
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def detect_drift(self, symbol_graph: GraphSnapshot) -> DriftReport:
        """Detect architectural drift across the repository"""
        start = time.time()
        
        # Run all drift detectors
        functional_duplications = self.duplicate_detector.detect_functional_duplicates(symbol_graph)
        pattern_drift = self.duplicate_detector.detect_pattern_drift(symbol_graph)
        competing_subsystems = self.competing_detector.identify_competing_subsystems(symbol_graph)
        subsystem_overlaps = self.competing_detector.analyze_subsystem_overlap(symbol_graph)
        unused_abstractions = self.unused_detector.detect_unused_abstractions(symbol_graph)
        dead_architecture = self.unused_detector.detect_dead_architecture(symbol_graph)
        subsystem_divergence = self.divergence_analyzer.analyze_subsystem_divergence(symbol_graph)
        architectural_decay = self.divergence_analyzer.detect_architectural_decay(symbol_graph)
        
        # Calculate overall drift score
        drift_indicators = (
            len(functional_duplications) + len(pattern_drift) +
            len(competing_subsystems) + len(unused_abstractions) +
            len(architectural_decay)
        )
        total_components = len(symbol_graph.symbols)
        drift_score = min(drift_indicators / max(total_components / 10, 1), 1.0)
        
        report = DriftReport(
            repository_id=str(time.time()),
            overall_drift_score=drift_score,
            functional_duplications=len(functional_duplications),
            competing_subsystems=len(competing_subsystems),
            unused_abstractions=len(unused_abstractions),
            architectural_violations=len(pattern_drift) + len(architectural_decay),
            timestamp=datetime.now()
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_drift", elapsed)
        log.info(f"Drift detection completed: overall drift {drift_score:.2f} ({elapsed:.0f}ms)")
        
        return report


class DuplicatedImplementationDetector:
    """Detects duplicated implementations of similar functionality"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def detect_functional_duplicates(self, symbol_graph: GraphSnapshot) -> List[FunctionalDuplication]:
        """Detects functionally duplicated implementations"""
        start = time.time()
        
        # Group symbols by similar names/purposes
        symbol_groups = defaultdict(list)
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            # Create a functional signature based on name patterns
            base_name = self._extract_base_function_name(symbol.symbol_name)
            symbol_groups[base_name].append(symbol_id)
        
        # Identify potential duplicates
        duplications = []
        dup_id = 0
        
        for base_name, symbol_ids in symbol_groups.items():
            if len(symbol_ids) > 1:
                # Check if they're in different files (actual duplication, not overloads)
                file_paths = set()
                for symbol_id in symbol_ids:
                    symbol = symbol_graph.symbols.get(symbol_id)
                    if symbol:
                        file_paths.add(symbol.file_path)
                
                if len(file_paths) > 1:
                    dup = FunctionalDuplication(
                        duplication_id=f"func_dup_{dup_id}",
                        function_purpose=base_name,
                        implementations=list(file_paths),
                        similarity_score=0.8,  # Simplified similarity
                        consolidation_opportunity=len(file_paths) > 2
                    )
                    duplications.append(dup)
                    dup_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_functional_duplicates", elapsed)
        
        return duplications
    
    def _extract_base_function_name(self, name: str) -> str:
        """Extract the base functional name from a symbol name"""
        # Remove common prefixes/suffixes
        name_lower = name.lower()
        for prefix in ["get_", "set_", "is_", "has_", "compute_", "calculate_", "process_"]:
            if name_lower.startswith(prefix):
                name_lower = name_lower[len(prefix):]
                break
        for suffix in ["_impl", "_handler", "_helper", "_util"]:
            if name_lower.endswith(suffix):
                name_lower = name_lower[:-len(suffix)]
                break
        return name_lower
    
    def detect_pattern_drift(self, symbol_graph: GraphSnapshot) -> List[PatternDrift]:
        """Detects drift from established patterns"""
        start = time.time()
        
        pattern_drifts = []
        drift_id = 0
        
        # Analyze naming pattern consistency
        pattern_groups = defaultdict(list)
        for symbol_id, symbol in symbol_graph.symbols.items():
            naming_pattern = self._get_naming_pattern(symbol.symbol_name, symbol.symbol_kind.value)
            pattern_groups[naming_pattern].append(symbol_id)
        
        # Find dominant patterns
        dominant_patterns = {}
        for kind in ["class", "function", "method", "variable"]:
            kind_symbols = [sid for sid, sym in symbol_graph.symbols.items() if sym.symbol_kind.value == kind]
            if kind_symbols:
                # Find most common pattern for this kind
                pattern_counts = defaultdict(int)
                for symbol_id in kind_symbols:
                    symbol = symbol_graph.symbols.get(symbol_id)
                    if symbol:
                        pattern = self._get_naming_pattern(symbol.symbol_name, kind)
                        pattern_counts[pattern] += 1
                
                if pattern_counts:
                    dominant = max(pattern_counts, key=pattern_counts.get)
                    dominant_patterns[kind] = dominant
        
        # Identify symbols that don't follow dominant patterns
        for symbol_id, symbol in symbol_graph.symbols.items():
            dominant = dominant_patterns.get(symbol.symbol_kind.value)
            if dominant:
                current_pattern = self._get_naming_pattern(symbol.symbol_name, symbol.symbol_kind.value)
                if current_pattern != dominant:
                    drift = PatternDrift(
                        drift_id=f"pattern_drift_{drift_id}",
                        intended_pattern=dominant,
                        drifted_components=[symbol_id],
                        drift_severity="medium"  # Could be more sophisticated
                    )
                    pattern_drifts.append(drift)
                    drift_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_pattern_drift", elapsed)
        
        return pattern_drifts
    
    def _get_naming_pattern(self, name: str, kind: str) -> str:
        """Determine the naming pattern for a symbol"""
        if kind == "class":
            return "PascalCase" if name[0].isupper() else "snake_case"
        elif kind in ["function", "method"]:
            return "snake_case" if "_" in name else "camelCase"
        else:
            return "snake_case" if "_" in name else "camelCase"


class CompetingSubsystemDetector:
    """Detects competing or redundant subsystems"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def identify_competing_subsystems(self, symbol_graph: GraphSnapshot) -> List[CompetingSubsystem]:
        """Identifies competing or redundant subsystems"""
        start = time.time()
        
        competing_subsystems = []
        
        # Group files by directory structure to identify subsystems
        subsystem_groups = defaultdict(list)
        for file_id, file in symbol_graph.files.items():
            # Use second-level directory as subsystem identifier
            path_parts = Path(file.file_path).parts
            if len(path_parts) >= 2:
                subsystem = path_parts[1]
            else:
                subsystem = path_parts[0] if path_parts else "root"
            subsystem_groups[subsystem].append(file_id)
        
        # Look for subsystems with similar names or purposes
        subsystem_names = list(subsystem_groups.keys())
        
        for i, subsystem1 in enumerate(subsystem_names):
            for subsystem2 in subsystem_names[i+1:]:
                # Check if subsystems have similar names
                if self._subsystems_are_similar(subsystem1, subsystem2):
                    overlap_percentage = self._calculate_subsystem_overlap(
                        subsystem_groups[subsystem1], subsystem_groups[subsystem2], symbol_graph
                    )
                    
                    if overlap_percentage > 0.3:  # More than 30% overlap
                        competing = CompetingSubsystem(
                            subsystem_id=f"competing_{subsystem1}_{subsystem2}",
                            competing_subsystems=[subsystem1, subsystem2],
                            overlap_percentage=overlap_percentage,
                            consolidation_potential=overlap_percentage > 0.5
                        )
                        competing_subsystems.append(competing)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("identify_competing_subsystems", elapsed)
        
        return competing_subsystems
    
    def _subsystems_are_similar(self, subsystem1: str, subsystem2: str) -> bool:
        """Check if two subsystems are similar"""
        # Simple string similarity
        s1, s2 = subsystem1.lower(), subsystem2.lower()
        
        # Check for substring similarity
        if s1 in s2 or s2 in s1:
            return True
        
        # Check for common prefixes
        min_len = min(len(s1), len(s2))
        if min_len > 3 and s1[:min_len//2] == s2[:min_len//2]:
            return True
        
        return False
    
    def _calculate_subsystem_overlap(self, files1: List[str], files2: List[str], symbol_graph: GraphSnapshot) -> float:
        """Calculate the functional overlap between two subsystems"""
        # Calculate overlap based on symbol names and purposes
        symbols1 = set()
        for file_id in files1:
            file = symbol_graph.files.get(file_id)
            if file:
                symbols1.update([sym.symbol_name.lower() for sym in file.symbols])
        
        symbols2 = set()
        for file_id in files2:
            file = symbol_graph.files.get(file_id)
            if file:
                symbols2.update([sym.symbol_name.lower() for sym in file.symbols])
        
        if not symbols1 or not symbols2:
            return 0.0
        
        # Calculate intersection
        intersection = symbols1.intersection(symbols2)
        union = symbols1.union(symbols2)
        
        return len(intersection) / len(union) if union else 0.0
    
    def analyze_subsystem_overlap(self, symbol_graph: GraphSnapshot) -> List[SubsystemOverlap]:
        """Analyzes overlap between subsystems"""
        start = time.time()
        
        overlaps = []
        
        # Group files by directory to identify subsystems
        subsystem_groups = defaultdict(list)
        for file_id, file in symbol_graph.files.items():
            path_parts = Path(file.file_path).parts
            if len(path_parts) >= 2:
                subsystem = path_parts[1]
            else:
                subsystem = path_parts[0] if path_parts else "root"
            subsystem_groups[subsystem].append(file_id)
        
        # Analyze overlaps between subsystems
        subsystem_names = list(subsystem_groups.keys())
        overlap_id = 0
        
        for i, subsystem1 in enumerate(subsystem_names):
            for subsystem2 in subsystem_names[i+1:]:
                overlap_percentage = self._calculate_subsystem_overlap(
                    subsystem_groups[subsystem1], subsystem_groups[subsystem2], symbol_graph
                )
                
                if overlap_percentage > 0.2:  # More than 20% overlap
                    overlapping_functionality = self._identify_overlapping_functionality(
                        subsystem_groups[subsystem1], subsystem_groups[subsystem2], symbol_graph
                    )
                    
                    overlap = SubsystemOverlap(
                        overlap_id=f"overlap_{overlap_id}",
                        subsystems=[subsystem1, subsystem2],
                        overlapping_functionality=overlapping_functionality,
                        overlap_area=f"{subsystem1}-{subsystem2}"
                    )
                    overlaps.append(overlap)
                    overlap_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_subsystem_overlap", elapsed)
        
        return overlaps
    
    def _identify_overlapping_functionality(self, files1: List[str], files2: List[str], symbol_graph: GraphSnapshot) -> List[str]:
        """Identify the overlapping functionality between subsystems"""
        overlapping = []
        
        symbols1 = set()
        for file_id in files1:
            file = symbol_graph.files.get(file_id)
            if file:
                symbols1.update([sym.symbol_name for sym in file.symbols])
        
        symbols2 = set()
        for file_id in files2:
            file = symbol_graph.files.get(file_id)
            if file:
                symbols2.update([sym.symbol_name for sym in file.symbols])
        
        # Find common symbol names
        common = symbols1.intersection(symbols2)
        
        for name in common:
            overlapping.append(name)
        
        return overlapping[:5]  # Return top 5 overlapping functions


class UnusedAbstractionDetector:
    """Detects unused or dead abstractions"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def detect_unused_abstractions(self, symbol_graph: GraphSnapshot) -> List[UnusedAbstraction]:
        """Detects unused abstractions"""
        start = time.time()
        
        unused_abstractions = []
        unused_id = 0
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            # Check if symbol is referenced by other symbols
            incoming_edges = symbol_graph.get_edges_to(symbol_id)
            usage_count = len([e for e in incoming_edges if e.edge_type == EdgeType.REFERENCES])
            usage_count += len([e for e in incoming_edges if e.edge_type == EdgeType.CALLS])
            
            # Consider abstraction unused if it's not used and is exported
            if symbol.is_exported and usage_count == 0:
                unused = UnusedAbstraction(
                    abstraction_id=symbol_id,
                    abstraction_type=symbol.symbol_kind.value,
                    defined_components=[symbol_id],
                    usage_count=usage_count,
                    removal_safe=True  # Could be enhanced with more sophisticated checks
                )
                unused_abstractions.append(unused)
                unused_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_unused_abstractions", elapsed)
        
        return unused_abstractions
    
    def detect_dead_architecture(self, symbol_graph: GraphSnapshot) -> List[DeadArchitecture]:
        """Detects dead architecture components"""
        start = time.time()
        
        dead_architecture = []
        dead_id = 0
        
        # Identify files/symbols that haven't been modified or used
        current_time = time.time()
        
        for file_id, file in symbol_graph.files.items():
            # Check if file has any symbols
            if not file.symbols:
                dead = DeadArchitecture(
                    architecture_id=f"dead_file_{file_id}",
                    architecture_type="empty_file",
                    components=[file_id],
                    last_used=None,
                    removal_candidate=True
                )
                dead_architecture.append(dead)
                dead_id += 1
                continue
            
            # Check if file is orphaned (no imports or imports from it)
            has_incoming = any(e.target_id == file_id for e in symbol_graph.edges if e.edge_type == EdgeType.IMPORTS)
            has_outgoing = any(e.source_id == file_id for e in symbol_graph.edges if e.edge_type == EdgeType.IMPORTS)
            
            if not has_incoming and not has_outgoing and not file.is_entry_point:
                dead = DeadArchitecture(
                    architecture_id=f"dead_orphan_{file_id}",
                    architecture_type="orphaned_file",
                    components=[file_id],
                    last_used=file.parse_timestamp if hasattr(file, 'parse_timestamp') else None,
                    removal_candidate=True
                )
                dead_architecture.append(dead)
                dead_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_dead_architecture", elapsed)
        
        return dead_architecture


class SubsystemDivergenceAnalyzer:
    """Analyzes divergence between subsystems"""
    
    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def analyze_subsystem_divergence(self, symbol_graph: GraphSnapshot) -> List[SubsystemDivergence]:
        """Analyzes divergence between similar subsystems"""
        start = time.time()
        
        divergences = []
        
        # Group files by directory to identify subsystems
        subsystem_groups = defaultdict(list)
        for file_id, file in symbol_graph.files.items():
            path_parts = Path(file.file_path).parts
            if len(path_parts) >= 2:
                subsystem = path_parts[1]
            else:
                subsystem = path_parts[0] if path_parts else "root"
            subsystem_groups[subsystem].append(file_id)
        
        # Analyze subsystems that should be similar but aren't
        subsystem_names = list(subsystem_groups.keys())
        
        for i, subsystem1 in enumerate(subsystem_names):
            for subsystem2 in subsystem_names[i+1:]:
                if self._subsystems_should_be_converged(subsystem1, subsystem2):
                    divergence_degree = self._calculate_divergence_degree(
                        subsystem_groups[subsystem1], subsystem_groups[subsystem2], symbol_graph
                    )
                    
                    if divergence_degree > 0.3:  # More than 30% divergence
                        divergence = SubsystemDivergence(
                            divergence_id=f"divergence_{subsystem1}_{subsystem2}",
                            subsystems=[subsystem1, subsystem2],
                            divergence_type="implementation",
                            divergence_degree=divergence_degree,
                            convergence_recommendation="Standardize implementation patterns"
                        )
                        divergences.append(divergence)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_subsystem_divergence", elapsed)
        
        return divergences
    
    def _subsystems_should_be_converged(self, subsystem1: str, subsystem2: str) -> bool:
        """Check if subsystems should have converged implementations"""
        # Subsystems with similar names should be converged
        s1, s2 = subsystem1.lower(), subsystem2.lower()
        
        # Check for name similarity
        if s1 in s2 or s2 in s1:
            return True
        
        # Check for common architectural patterns
        common_patterns = ["service", "controller", "handler", "manager"]
        if any(p in s1 for p in common_patterns) and any(p in s2 for p in common_patterns):
            return True
        
        return False
    
    def _calculate_divergence_degree(self, files1: List[str], files2: List[str], symbol_graph: GraphSnapshot) -> float:
        """Calculate the degree of divergence between subsystems"""
        # Analyze structural differences
        structure1 = self._analyze_structure(files1, symbol_graph)
        structure2 = self._analyze_structure(files2, symbol_graph)
        
        # Calculate difference
        differences = 0
        total_features = 0
        
        for feature in set(structure1.keys()) | set(structure2.keys()):
            total_features += 1
            if structure1.get(feature, 0) != structure2.get(feature, 0):
                differences += 1
        
        return differences / total_features if total_features > 0 else 0.0
    
    def _analyze_structure(self, files: List[str], symbol_graph: GraphSnapshot) -> Dict[str, int]:
        """Analyze the structure of a subsystem"""
        structure = {
            "file_count": len(files),
            "symbol_count": 0,
            "class_count": 0,
            "function_count": 0
        }
        
        for file_id in files:
            file = symbol_graph.files.get(file_id)
            if file:
                structure["symbol_count"] += len(file.symbols)
                for symbol in file.symbols:
                    if symbol.symbol_kind.value == "class":
                        structure["class_count"] += 1
                    elif symbol.symbol_kind.value in ["function", "method"]:
                        structure["function_count"] += 1
        
        return structure
    
    def detect_architectural_decay(self, symbol_graph: GraphSnapshot) -> List[ArchitecturalDecay]:
        """Detects architectural decay indicators"""
        start = time.time()
        
        decay_indicators = []
        decay_id = 0
        
        # Look for signs of architectural decay
        for file_id, file in symbol_graph.files.items():
            # Check for overly complex files
            if len(file.symbols) > 100:
                decay = ArchitecturalDecay(
                    decay_id=f"decay_{decay_id}",
                    decay_type="complexity_bloat",
                    affected_area=file_id,
                    severity="medium",
                    progression_rate=0.1  # Could be calculated from history
                )
                decay_indicators.append(decay)
                decay_id += 1
            
            # Check for lack of modularity (everything in one file)
            if len(file.symbols) > 20 and file.symbols[0].symbol_kind.value != "class":
                decay = ArchitecturalDecay(
                    decay_id=f"decay_{decay_id}",
                    decay_type="monolithic_structure",
                    affected_area=file_id,
                    severity="low",
                    progression_rate=0.05
                )
                decay_indicators.append(decay)
                decay_id += 1
        
        # Check for circular dependencies
        for symbol_id in symbol_graph.symbols:
            # Simple circular dependency check
            visited = set()
            if self._has_circular_dependency(symbol_id, symbol_id, symbol_graph, visited):
                decay = ArchitecturalDecay(
                    decay_id=f"decay_{decay_id}",
                    decay_type="circular_dependency",
                    affected_area=symbol_id,
                    severity="medium",
                    progression_rate=0.15
                )
                decay_indicators.append(decay)
                decay_id += 1
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_architectural_decay", elapsed)
        
        return decay_indicators
    
    def _has_circular_dependency(self, start: str, current: str, symbol_graph: GraphSnapshot, visited: Set[str]) -> bool:
        """Check if there's a circular dependency starting from current"""
        if current in visited:
            return True
        
        visited.add(current)
        
        for edge in symbol_graph.get_edges_from(current):
            if edge.edge_type == EdgeType.IMPORTS:
                if self._has_circular_dependency(start, edge.target_id, symbol_graph, visited):
                    return True
        
        visited.remove(current)
        return False
