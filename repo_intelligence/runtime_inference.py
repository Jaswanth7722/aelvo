# runtime_inference.py - Runtime Relationship Inference for Repository Intelligence
# Layer 12: Infers runtime relationships from static analysis

import time
import logging
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict

from repo_intelligence.types import (
    EdgeType, ConfidenceLevel
)
from repo_intelligence.types_extended import (
    RuntimeDependency, ExecutionPath, DataFlowPath, PerformanceMetrics
)

log = logging.getLogger("aelvo.repo_intelligence.runtime_inference")


class RuntimeRelationshipInference:
    """Infers runtime relationships beyond static dependencies"""
    
    def __init__(self, symbol_graph):
        """
        Initialize with a SymbolGraphEngine instance.
        
        Args:
            symbol_graph: SymbolGraphEngine instance to analyze
        """
        self.symbol_graph = symbol_graph
        self.metrics: List[PerformanceMetrics] = []
        
        # Runtime relationship caches
        self.runtime_dependencies: Dict[str, List[RuntimeDependency]] = defaultdict(list)
        self.execution_paths: Dict[str, List[ExecutionPath]] = defaultdict(list)
        self.data_flows: Dict[str, List[DataFlowPath]] = defaultdict(list)
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def infer_runtime_dependencies(self, symbol_id: str) -> List[RuntimeDependency]:
        """
        Infers runtime dependencies beyond static imports.
        
        Args:
            symbol_id: ID of the symbol to analyze
            
        Returns:
            List of inferred runtime dependencies
        """
        start = time.time()
        dependencies = []
        
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return dependencies
        
        # Analyze different runtime dependency patterns
        
        # 1. Dynamic imports (import statements inside functions)
        dynamic_import_patterns = self._detect_dynamic_imports(symbol_id)
        for target_id, confidence in dynamic_import_patterns:
            dep = RuntimeDependency(
                source_id=symbol_id,
                target_id=target_id,
                dependency_type="dynamic_import",
                inference_confidence=confidence,
                evidence=["Dynamic import pattern detected"]
            )
            dependencies.append(dep)
        
        # 2. Plugin/hook patterns (functions that might be called externally)
        plugin_patterns = self._detect_plugin_patterns(symbol_id)
        for target_id, confidence in plugin_patterns:
            dep = RuntimeDependency(
                source_id=symbol_id,
                target_id=target_id,
                dependency_type="plugin_hook",
                inference_confidence=confidence,
                evidence=["Plugin/hook pattern detected"]
            )
            dependencies.append(dep)
        
        # 3. Configuration-driven dependencies
        config_patterns = self._detect_config_dependencies(symbol_id)
        for target_id, confidence in config_patterns:
            dep = RuntimeDependency(
                source_id=symbol_id,
                target_id=target_id,
                dependency_type="config_driven",
                inference_confidence=confidence,
                evidence=["Configuration-driven dependency detected"]
            )
            dependencies.append(dep)
        
        # 4. Event handler patterns
        event_patterns = self._detect_event_handlers(symbol_id)
        for target_id, confidence in event_patterns:
            dep = RuntimeDependency(
                source_id=symbol_id,
                target_id=target_id,
                dependency_type="event_handler",
                inference_confidence=confidence,
                evidence=["Event handler pattern detected"]
            )
            dependencies.append(dep)
        
        # Cache results
        self.runtime_dependencies[symbol_id] = dependencies
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("infer_runtime_dependencies", elapsed)
        log.debug(f"Inferred {len(dependencies)} runtime dependencies for {symbol_id} ({elapsed:.0f}ms)")
        
        return dependencies
    
    def _detect_dynamic_imports(self, symbol_id: str) -> List[Tuple[str, float]]:
        """Detect potential dynamic import patterns"""
        candidates = []
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return candidates
        
        # Look for patterns like __import__, importlib, etc.
        docstring_lower = (symbol.docstring or "").lower()
        
        dynamic_keywords = ["__import__", "importlib", "dynamic", "plugin"]
        if any(keyword in docstring_lower for keyword in dynamic_keywords):
            # Check what this symbol might dynamically import
            # by looking at its static dependencies
            for edge in self.symbol_graph.graph.get_edges_from(symbol_id):
                if edge.edge_type == EdgeType.IMPORTS:
                    candidates.append((edge.target_id, 0.6))
        
        return candidates
    
    def _detect_plugin_patterns(self, symbol_id: str) -> List[Tuple[str, float]]:
        """Detect plugin/hook registration patterns"""
        candidates = []
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return candidates
        
        # Look for common plugin naming patterns
        plugin_keywords = ["plugin", "hook", "register", "handler", "callback"]
        symbol_name_lower = symbol.symbol_name.lower()
        
        if any(keyword in symbol_name_lower for keyword in plugin_keywords):
            # This might be a plugin, so look for things that might call it
            for edge in self.symbol_graph.graph.get_edges_to(symbol_id):
                if edge.edge_type == EdgeType.CALLS:
                    candidates.append((edge.source_id, 0.5))
        
        return candidates
    
    def _detect_config_dependencies(self, symbol_id: str) -> List[Tuple[str, float]]:
        """Detect configuration-driven dependencies"""
        candidates = []
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return candidates
        
        # Look for configuration-related patterns
        config_keywords = ["config", "settings", "env", "environment"]
        symbol_name_lower = symbol.symbol_name.lower()
        docstring_lower = (symbol.docstring or "").lower()
        
        if any(keyword in symbol_name_lower for keyword in config_keywords) or \
           any(keyword in docstring_lower for keyword in config_keywords):
            # Config symbols might be used by many components
            for edge in self.symbol_graph.graph.get_edges_to(symbol_id):
                if edge.edge_type == EdgeType.REFERENCES:
                    candidates.append((edge.source_id, 0.4))
        
        return candidates
    
    def _detect_event_handlers(self, symbol_id: str) -> List[Tuple[str, float]]:
        """Detect event handler patterns"""
        candidates = []
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return candidates
        
        # Look for event-related patterns
        event_keywords = ["on_", "handle_", "event", "emit", "dispatch"]
        symbol_name_lower = symbol.symbol_name.lower()
        
        if any(keyword in symbol_name_lower for keyword in event_keywords):
            # Event handlers might be called by event emitters
            for edge in self.symbol_graph.graph.get_edges_to(symbol_id):
                if edge.edge_type == EdgeType.CALLS:
                    candidates.append((edge.source_id, 0.5))
        
        return candidates
    
    def detect_execution_paths(self, entry_point: str) -> List[ExecutionPath]:
        """
        Detects likely execution paths from entry points.
        
        Args:
            entry_point: File ID or symbol ID to use as entry point
            
        Returns:
            List of detected execution paths
        """
        start = time.time()
        
        # Determine if entry_point is a file or symbol
        if entry_point in self.symbol_graph.graph.files:
            # It's a file ID, find the main symbol
            file = self.symbol_graph.graph.files[entry_point]
            # Look for main functions, classes, or exported symbols
            candidates = [s for s in file.symbols if s.is_exported or 
                         any(kw in s.symbol_name.lower() for kw in ["main", "run", "start"])]
            if candidates:
                start_symbol = candidates[0].symbol_id
            else:
                start_symbol = file.symbols[0].symbol_id if file.symbols else None
        else:
            # It's a symbol ID
            start_symbol = entry_point
        
        if not start_symbol or start_symbol not in self.symbol_graph.graph.symbols:
            elapsed = (time.time() - start) * 1000
            self._record_metric("detect_execution_paths", elapsed)
            return []
        
        paths = []
        
        # Detect happy path (most likely execution)
        happy_path = self._trace_execution_path(start_symbol, "happy")
        if happy_path:
            paths.append(happy_path)
        
        # Detect error paths
        error_path = self._trace_execution_path(start_symbol, "error")
        if error_path and len(error_path.components) > 1:  # Only if different from happy path
            paths.append(error_path)
        
        # Cache results
        self.execution_paths[entry_point] = paths
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_execution_paths", elapsed)
        log.debug(f"Detected {len(paths)} execution paths from {entry_point} ({elapsed:.0f}ms)")
        
        return paths
    
    def _trace_execution_path(self, start_symbol: str, path_type: str = "happy") -> ExecutionPath:
        """
        Trace an execution path from a starting symbol.
        
        Args:
            start_symbol: Symbol ID to start from
            path_type: Type of path to trace ("happy", "error", "alternative")
            
        Returns:
            Execution path with components and probability
        """
        components = [start_symbol]
        visited = {start_symbol}
        current = start_symbol
        
        # Trace based on path type
        for _ in range(10):  # Limit path depth
            if path_type == "happy":
                next_symbol = self._get_most_likely_next(current, visited)
            elif path_type == "error":
                next_symbol = self._get_error_handling_next(current, visited)
            else:
                next_symbol = self._get_alternative_next(current, visited)
            
            if not next_symbol or next_symbol in visited:
                break
            
            visited.add(next_symbol)
            components.append(next_symbol)
            current = next_symbol
        
        # Calculate path probability based on edge confidences
        probability = self._calculate_path_probability(components)
        
        return ExecutionPath(
            path_id=f"{start_symbol}_{path_type}_{hash(tuple(components))}",
            entry_point=start_symbol,
            components=components,
            probability=probability,
            path_type=path_type
        )
    
    def _get_most_likely_next(self, current: str, visited: Set[str]) -> Optional[str]:
        """Get the most likely next symbol in happy path"""
        candidates = []
        
        for edge in self.symbol_graph.graph.get_edges_from(current):
            if edge.target_id not in visited and edge.edge_type == EdgeType.CALLS:
                candidates.append((edge.target_id, edge.confidence))
        
        if not candidates:
            return None
        
        # Sort by confidence and return the highest
        candidates.sort(key=lambda x: x[1].value, reverse=True)
        return candidates[0][0] if candidates else None
    
    def _get_error_handling_next(self, current: str, visited: Set[str]) -> Optional[str]:
        """Get the next symbol in error handling path"""
        # Look for exception handling patterns
        symbol = self.symbol_graph.graph.symbols.get(current)
        if not symbol:
            return None
        
        # Look for try/except patterns in docstring or name
        docstring_lower = (symbol.docstring or "").lower()
        if "except" in docstring_lower or "error" in docstring_lower or "exception" in docstring_lower:
            # This might be error handling, continue normal flow
            return self._get_most_likely_next(current, visited)
        
        # Otherwise, look for error-related symbols
        error_keywords = ["error", "exception", "fail", "raise", "catch"]
        for edge in self.symbol_graph.graph.get_edges_from(current):
            target = self.symbol_graph.graph.symbols.get(edge.target_id)
            if target and target not in visited:
                if any(kw in target.symbol_name.lower() for kw in error_keywords):
                    return edge.target_id
        
        return self._get_most_likely_next(current, visited)
    
    def _get_alternative_next(self, current: str, visited: Set[str]) -> Optional[str]:
        """Get the next symbol in alternative path"""
        # Look for branching patterns
        for edge in self.symbol_graph.graph.get_edges_from(current):
            if edge.target_id not in visited and edge.edge_type == EdgeType.CALLS:
                # Return an alternative (not the most likely)
                return edge.target_id
        
        return None
    
    def _calculate_path_probability(self, components: List[str]) -> float:
        """Calculate the probability of a given execution path"""
        if len(components) < 2:
            return 1.0
        
        total_confidence = 0.0
        edge_count = 0
        
        for i in range(len(components) - 1):
            current, next_sym = components[i], components[i + 1]
            edges = [
                e for e in self.symbol_graph.graph.get_edges_from(current)
                if e.target_id == next_sym
            ]
            if edges:
                # Use confidence value
                conf_value = 1.0 if edges[0].confidence == ConfidenceLevel.CERTAIN else \
                            0.7 if edges[0].confidence == ConfidenceLevel.INFERRED else 0.5
                total_confidence += conf_value
                edge_count += 1
        
        if edge_count == 0:
            return 0.5
        
        return total_confidence / edge_count
    
    def infer_data_flow(self, symbol_id: str) -> List[DataFlowPath]:
        """
        Infers data flow patterns for a symbol.
        
        Args:
            symbol_id: Symbol ID to analyze
            
        Returns:
            List of inferred data flow paths
        """
        start = time.time()
        flows = []
        
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return flows
        
        # Identify data sources (inputs)
        sources = self._identify_data_sources(symbol_id)
        
        # Identify data sinks (outputs/external writes)
        sinks = self._identify_data_sinks(symbol_id)
        
        # For each source-sink pair, create a data flow path
        for source in sources:
            for sink in sinks:
                # Find transformations between source and sink
                transformations = self._find_transformations(source, sink, symbol_id)
                
                flow = DataFlowPath(
                    path_id=f"dataflow_{symbol_id}_{source}_{sink}",
                    data_source=source,
                    data_transformations=transformations,
                    data_sink=sink,
                    confidence=0.6  # Base confidence for inferred data flow
                )
                flows.append(flow)
        
        # Cache results
        self.data_flows[symbol_id] = flows
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("infer_data_flow", elapsed)
        log.debug(f"Inferred {len(flows)} data flow paths for {symbol_id} ({elapsed:.0f}ms)")
        
        return flows
    
    def _identify_data_sources(self, symbol_id: str) -> List[str]:
        """Identify potential data sources for a symbol"""
        sources = []
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return sources
        
        # Look for parameters (data inputs)
        sources.extend([arg.name for arg in symbol.arguments])
        
        # Look for external data access patterns
        docstring_lower = (symbol.docstring or "").lower()
        if any(kw in docstring_lower for kw in ["read", "fetch", "get", "load", "input"]):
            sources.append("external_data")
        
        return sources
    
    def _identify_data_sinks(self, symbol_id: str) -> List[str]:
        """Identify potential data sinks for a symbol"""
        sinks = []
        symbol = self.symbol_graph.graph.symbols.get(symbol_id)
        if not symbol:
            return sinks
        
        # Look for external write patterns
        docstring_lower = (symbol.docstring or "").lower()
        if any(kw in docstring_lower for kw in ["write", "save", "output", "return", "send"]):
            sinks.append("external_write")
        
        # If it's a function/method, the return is a sink
        if symbol.symbol_kind.value in ["function", "method"]:
            sinks.append("return_value")
        
        return sinks
    
    def _find_transformations(self, source: str, sink: str, context_symbol: str) -> List[str]:
        """Find data transformations between source and sink"""
        transformations = []
        
        # Look for function calls that might transform data
        symbol = self.symbol_graph.graph.symbols.get(context_symbol)
        if not symbol:
            return transformations
        
        # Analyze calls within the symbol
        for edge in self.symbol_graph.graph.get_edges_from(context_symbol):
            if edge.edge_type == EdgeType.CALLS:
                target = self.symbol_graph.graph.symbols.get(edge.target_id)
                if target:
                    transformations.append(target.symbol_name)
        
        return transformations
    
    def analyze_all_runtime_relationships(self) -> Dict[str, List[RuntimeDependency]]:
        """
        Analyze runtime relationships for all symbols in the graph.
        
        Returns:
            Dictionary mapping symbol IDs to their runtime dependencies
        """
        start = time.time()
        log.info("Analyzing runtime relationships for all symbols")
        
        all_dependencies = {}
        for symbol_id in self.symbol_graph.graph.symbols:
            dependencies = self.infer_runtime_dependencies(symbol_id)
            if dependencies:
                all_dependencies[symbol_id] = dependencies
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("analyze_all_runtime_relationships", elapsed)
        log.info(f"Analyzed runtime relationships for {len(all_dependencies)} symbols ({elapsed:.0f}ms)")
        
        return all_dependencies
    
    def get_entry_points(self) -> List[str]:
        """
        Identify likely entry points in the codebase.
        
        Returns:
            List of file IDs or symbol IDs that are likely entry points
        """
        entry_points = []
        
        # Look for files with common entry point patterns
        for file_id, file in self.symbol_graph.graph.files.items():
            file_path_lower = file.file_path.lower()
            
            # Common entry point file names
            if any(pattern in file_path_lower for pattern in ["main", "app", "server", "run", "start"]):
                entry_points.append(file_id)
                continue
            
            # Look for main functions
            for symbol in file.symbols:
                if symbol.symbol_name.lower() in ["main", "run", "start", "app"]:
                    entry_points.append(symbol.symbol_id)
                    break
        
        return entry_points
    
    def get_metrics(self) -> List[PerformanceMetrics]:
        """Return all recorded metrics"""
        return self.metrics.copy()
