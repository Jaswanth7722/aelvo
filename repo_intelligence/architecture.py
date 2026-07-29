# architecture.py - Architecture Mapper
# Layer 8: Builds high-level structural understanding from graph data

import time
import logging
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict, deque
from pathlib import Path

from repo_intelligence.types import (
    SymbolNode, SymbolEdge, EdgeType, GraphSnapshot,
    ArchitectureLayer, ArchitectureMap, FileDependencyInfo,
    ParsedFile, PerformanceMetrics, DependencyGraphSnapshot
)
from repo_intelligence.types_extended import (
    ComponentIntent, DesignDecision, OwnershipPattern, ConfidenceLevel
)

log = logging.getLogger("aelvo.repo_intelligence.architecture")


class ArchitectureMapper:
    ARCHITECTURAL_PATTERNS = {
        'infrastructure': ['db', 'database', 'storage', 'cache', 'queue',
                           'config', 'settings', 'logger', 'logging'],
        'domain': ['model', 'entity', 'domain', 'value_object', 'aggregate',
                   'repository_interface', 'service_interface'],
        'application': ['service', 'usecase', 'use_case', 'application',
                        'dto', 'command', 'query', 'handler'],
        'api': ['api', 'route', 'controller', 'endpoint', 'handler',
                'middleware', 'view', 'template'],
        'cli': ['cli', 'command', 'console', 'terminal'],
    }

    def __init__(self):
        self.metrics: List[PerformanceMetrics] = []

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def build_map(
        self,
        symbol_graph: GraphSnapshot,
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> ArchitectureMap:
        start = time.time()
        layers = self._identify_layers(dep_graph, file_info)
        entry_points = self._identify_entry_points(dep_graph, file_info)
        module_boundaries = self._identify_module_boundaries(
            symbol_graph, file_info
        )
        violations = self._identify_violations(layers, dep_graph, file_info)
        arch_map = ArchitectureMap(
            layers=layers,
            entry_points=list(entry_points),
            module_boundaries=module_boundaries,
            violations=violations,
        )
        elapsed = (time.time() - start) * 1000
        self._record_metric("build_map", elapsed)
        log.info(f"Built architecture map: {len(layers)} layers, "
                 f"{len(entry_points)} entry points, "
                 f"{len(violations)} violations ({elapsed:.0f}ms)")
        return arch_map

    def _identify_layers(
        self,
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> List[ArchitectureLayer]:
        package_deps: Dict[str, Set[str]] = defaultdict(set)
        package_files: Dict[str, Set[str]] = defaultdict(set)
        for fid, info in file_info.items():
            path = info.file_path.replace('\\', '/')
            parts = path.split('/')
            package = parts[0] if len(parts) > 1 else '.'
            package_files[package].add(fid)
            for imp in info.imports:
                imp_info = file_info.get(imp)
                if imp_info:
                    imp_path = imp_info.file_path.replace('\\', '/')
                    imp_parts = imp_path.split('/')
                    imp_pkg = imp_parts[0] if len(imp_parts) > 1 else '.'
                    if imp_pkg != package:
                        package_deps[package].add(imp_pkg)
        service_degrees: Dict[str, Tuple[int, int]] = {}
        for pkg in package_files:
            in_deg = sum(1 for deps in package_deps.values() if pkg in deps)
            out_deg = len(package_deps.get(pkg, set()))
            service_degrees[pkg] = (in_deg, out_deg)
        layers_map: Dict[str, Set[str]] = {}
        for pkg, (in_deg, out_deg) in service_degrees.items():
            layer_name = self._classify_package(pkg, in_deg, out_deg)
            if layer_name not in layers_map:
                layers_map[layer_name] = set()
            layers_map[layer_name].update(package_files[pkg])
        layers = []
        layer_order = ['infrastructure', 'domain', 'application', 'api', 'cli']
        seen = set()
        for name in layer_order:
            if name in layers_map:
                layer = ArchitectureLayer(
                    name=name,
                    files=layers_map[name],
                )
                layers.append(layer)
                seen.add(name)
        for name in layers_map:
            if name not in seen:
                layer = ArchitectureLayer(
                    name=name,
                    files=layers_map[name],
                )
                layers.append(layer)
        return layers

    def _classify_package(
        self, pkg_name: str, in_deg: int, out_deg: int
    ) -> str:
        lower_pkg = pkg_name.lower()
        for layer_name, patterns in self.ARCHITECTURAL_PATTERNS.items():
            for pattern in patterns:
                if pattern in lower_pkg:
                    return layer_name
        if out_deg == 0 and in_deg > 0:
            return 'infrastructure'
        if in_deg == 0 and out_deg > 0:
            return 'api'
        if in_deg > out_deg:
            return 'domain'
        if out_deg > in_deg:
            return 'application'
        return 'shared'

    def _identify_entry_points(
        self,
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> List[str]:
        entry_points = []
        for fid, info in file_info.items():
            if info.is_entry_point:
                entry_points.append(info.file_path)
            elif (not info.imported_by and info.imports):
                entry_points.append(info.file_path)
        return entry_points

    def _identify_module_boundaries(
        self,
        symbol_graph: GraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> Dict[str, List[str]]:
        module_interfaces: Dict[str, List[str]] = {}
        for fid, info in file_info.items():
            path = info.file_path.replace('\\', '/')
            if path.endswith('__init__.py'):
                module_dir = str(Path(path).parent)
                public_symbols = [
                    s.symbol_name for s in symbol_graph.get_symbols_in_file(fid)
                    if s.is_exported
                ]
                if module_dir:
                    module_interfaces[module_dir] = public_symbols
        return module_interfaces

    def _identify_violations(
        self,
        layers: List[ArchitectureLayer],
        dep_graph: DependencyGraphSnapshot,
        file_info: Dict[str, FileDependencyInfo],
    ) -> List[str]:
        violations = []
        layer_names = [l.name for l in layers]
        layer_index = {l.name: i for i, l in enumerate(layers)}
        layer_files: Dict[str, Set[str]] = {}
        for l in layers:
            layer_files[l.name] = l.files
        for layer in layers:
            layer_idx = layer_index.get(layer.name, -1)
            if layer_idx < 0:
                continue
            for fid in layer.files:
                info = file_info.get(fid)
                if not info:
                    continue
                for imp in info.imports:
                    imp_info = file_info.get(imp)
                    if not imp_info:
                        continue
                    imp_path = imp_info.file_path.replace('\\', '/')
                    imp_layer = self._find_layer_for_file(
                        imp_path, layer_names, layer_files
                    )
                    if imp_layer:
                        imp_idx = layer_index.get(imp_layer, -1)
                        if imp_idx >= 0 and imp_idx < layer_idx:
                            violations.append(
                                f"Dependency inversion: {info.file_path} ({layer.name}) "
                                f"depends on {imp_info.file_path} ({imp_layer})"
                            )
        for fid, info in file_info.items():
            path = info.file_path.replace('\\', '/')
            if 'test' in Path(path).parts or 'tests' in Path(path).parts:
                continue
            if len(info.imports) == 0 and not info.is_entry_point:
                violations.append(
                    f"Orphan file: {info.file_path} has no dependencies and is not an entry point"
                )
        return violations

    def _find_layer_for_file(
        self,
        file_path: str,
        layer_names: List[str],
        layer_files: Dict[str, Set[str]],
    ) -> Optional[str]:
        for layer_name, files in layer_files.items():
            for fid in files:
                info = None
                for fn in layer_files.values():
                    pass
        path_lower = file_path.lower()
        for layer_name, patterns in self.ARCHITECTURAL_PATTERNS.items():
            for pattern in patterns:
                if pattern in path_lower:
                    return layer_name
        return None

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
    
    # ===========================================================================
    # Architectural Intent Inference Methods
    # ===========================================================================
    
    def infer_component_intent(self, symbol_id: str, symbol_graph: GraphSnapshot) -> ComponentIntent:
        """
        Infers why a component exists and its design purpose.
        
        Args:
            symbol_id: ID of the symbol to analyze
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            ComponentIntent with inferred purpose and architectural role
        """
        start = time.time()
        
        symbol = symbol_graph.symbols.get(symbol_id)
        if not symbol:
            return ComponentIntent(
                component_id=symbol_id,
                inferred_purpose="unknown",
                architectural_role="unknown",
                confidence=ConfidenceLevel.APPROXIMATE,
                evidence=["Symbol not found in graph"]
            )
        
        # Analyze naming conventions
        symbol_name_lower = symbol.symbol_name.lower()
        fqn_lower = symbol.fully_qualified_name.lower()
        
        # Determine purpose based on name and context
        purpose, role, pattern, confidence = self._analyze_purpose_from_name(symbol_name_lower, fqn_lower, symbol)
        
        # Analyze structural context
        structural_evidence = self._analyze_structural_context(symbol, symbol_graph)
        evidence = [purpose, role] + structural_evidence
        
        # Determine domain responsibility from file path
        domain = self._infer_domain_responsibility(symbol)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("infer_component_intent", elapsed)
        
        return ComponentIntent(
            component_id=symbol_id,
            inferred_purpose=purpose,
            architectural_role=role,
            domain_responsibility=domain,
            design_pattern=pattern,
            confidence=confidence,
            evidence=evidence
        )
    
    def _analyze_purpose_from_name(self, symbol_name: str, fqn: str, symbol: SymbolNode) -> Tuple[str, str, Optional[str], ConfidenceLevel]:
        """Analyze component purpose from naming patterns"""
        purpose = "general_functionality"
        role = "utility"
        pattern = None
        confidence = ConfidenceLevel.INFERRED
        
        # Check for common architectural patterns in names
        if any(kw in symbol_name for kw in ["service", "manager", "handler"]):
            purpose = f"{symbol_name}_orchestration"
            role = "application"
            pattern = "service_layer"
            confidence = ConfidenceLevel.INFERRED
        elif any(kw in symbol_name for kw in ["repository", "dao", "storage"]):
            purpose = f"{symbol_name}_data_access"
            role = "infrastructure"
            pattern = "repository_pattern"
            confidence = ConfidenceLevel.INFERRED
        elif any(kw in symbol_name for kw in ["model", "entity", "dto"]):
            purpose = f"{symbol_name}_data_representation"
            role = "domain"
            pattern = "domain_model"
            confidence = ConfidenceLevel.INFERRED
        elif any(kw in symbol_name for kw in ["controller", "view", "api"]):
            purpose = f"{symbol_name}_request_handling"
            role = "api"
            pattern = "mvc_pattern"
            confidence = ConfidenceLevel.INFERRED
        elif any(kw in symbol_name for kw in ["config", "settings"]):
            purpose = f"{symbol_name}_configuration"
            role = "infrastructure"
            pattern = "configuration_pattern"
            confidence = ConfidenceLevel.CERTAIN
        elif any(kw in symbol_name for kw in ["util", "helper", "common"]):
            purpose = f"{symbol_name}_reusable_functionality"
            role = "shared"
            pattern = "utility_pattern"
            confidence = ConfidenceLevel.INFERRED
        
        # Check docstring for additional clues
        if symbol.docstring:
            doc_lower = symbol.docstring.lower()
            if any(kw in doc_lower for kw in ["business", "domain", "logic"]):
                role = "domain"
                confidence = ConfidenceLevel.INFERRED
            elif any(kw in doc_lower for kw in ["api", "http", "request", "response"]):
                role = "api"
                confidence = ConfidenceLevel.INFERRED
        
        return purpose, role, pattern, confidence
    
    def _analyze_structural_context(self, symbol: SymbolNode, symbol_graph: GraphSnapshot) -> List[str]:
        """Analyze structural context for additional evidence"""
        evidence = []
        
        # Check dependencies
        incoming_edges = symbol_graph.get_edges_to(symbol.symbol_id)
        outgoing_edges = symbol_graph.get_edges_from(symbol.symbol_id)
        
        if len(incoming_edges) > 10:
            evidence.append("highly_coupled_component")
        elif len(incoming_edges) == 0 and len(outgoing_edges) == 0:
            evidence.append("isolated_component")
        
        # Check if it's an exported symbol
        if symbol.is_exported:
            evidence.append("public_api_component")
        
        # Check for inheritance
        if symbol.base_class_names:
            evidence.append(f"inherits_from_{','.join(symbol.base_class_names)}")
        
        # Check for implemented interfaces
        if symbol.implemented_interfaces:
            evidence.append(f"implements_{','.join(symbol.implemented_interfaces)}")
        
        return evidence
    
    def _infer_domain_responsibility(self, symbol: SymbolNode) -> str:
        """Infer domain responsibility from file path and naming"""
        path = Path(symbol.file_path)
        parts = path.parts
        
        # Look for domain indicators in path
        domain_keywords = ["user", "order", "product", "account", "payment", "inventory", "catalog"]
        for part in parts:
            part_lower = part.lower()
            if any(kw in part_lower for kw in domain_keywords):
                return part_lower
        
        # Check symbol name for domain indicators
        symbol_name_lower = symbol.symbol_name.lower()
        for keyword in domain_keywords:
            if keyword in symbol_name_lower:
                return keyword
        
        return "general_domain"
    
    def detect_design_decisions(self, file_id: str, symbol_graph: GraphSnapshot) -> List[DesignDecision]:
        """
        Detects design decisions from code structure and patterns.
        
        Args:
            file_id: File ID to analyze
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            List of detected design decisions
        """
        start = time.time()
        decisions = []
        
        file = symbol_graph.files.get(file_id)
        if not file:
            return decisions
        
        # Analyze symbols in the file for design patterns
        for symbol in file.symbols:
            symbol_decisions = self._analyze_symbol_for_decisions(symbol, symbol_graph)
            decisions.extend(symbol_decisions)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("detect_design_decisions", elapsed)
        
        return decisions
    
    def _analyze_symbol_for_decisions(self, symbol: SymbolNode, symbol_graph: GraphSnapshot) -> List[DesignDecision]:
        """Analyze a single symbol for design decisions"""
        decisions = []
        
        # Check for singleton pattern
        if any(kw in symbol.symbol_name.lower() for kw in ["instance", "singleton"]):
            decision = DesignDecision(
                decision_id=f"singleton_{symbol.symbol_id}",
                component_id=symbol.symbol_id,
                decision_type="architectural",
                rationale="Singleton pattern detected for controlling instance creation",
                evidence=[f"Symbol name '{symbol.symbol_name}' suggests singleton pattern"],
                confidence=ConfidenceLevel.INFERRED,
                decision_maker="system"
            )
            decisions.append(decision)
        
        # Check for factory pattern
        if "factory" in symbol.symbol_name.lower() or "create" in symbol.symbol_name.lower():
            decision = DesignDecision(
                decision_id=f"factory_{symbol.symbol_id}",
                component_id=symbol.symbol_id,
                decision_type="architectural",
                rationale="Factory pattern detected for object creation",
                evidence=[f"Symbol name '{symbol.symbol_name}' suggests factory pattern"],
                confidence=ConfidenceLevel.INFERRED,
                decision_maker="system"
            )
            decisions.append(decision)
        
        # Check for strategy pattern
        if "strategy" in symbol.symbol_name.lower() or any(
            "interface" in symbol.symbol_name.lower() for symbol in symbol_graph.symbols.values()
            if symbol.symbol_id in [e.target_id for e in symbol_graph.get_edges_to(symbol.symbol_id)]
        ):
            decision = DesignDecision(
                decision_id=f"strategy_{symbol.symbol_id}",
                component_id=symbol.symbol_id,
                decision_type="architectural",
                rationale="Strategy pattern detected for algorithm interchangeability",
                evidence=[f"Symbol name '{symbol.symbol_name}' suggests strategy pattern"],
                confidence=ConfidenceLevel.INFERRED,
                decision_maker="system"
            )
            decisions.append(decision)
        
        # Check for observer pattern
        if any(kw in symbol.symbol_name.lower() for kw in ["observer", "listener", "subscriber"]):
            decision = DesignDecision(
                decision_id=f"observer_{symbol.symbol_id}",
                component_id=symbol.symbol_id,
                decision_type="architectural",
                rationale="Observer pattern detected for event handling",
                evidence=[f"Symbol name '{symbol.symbol_name}' suggests observer pattern"],
                confidence=ConfidenceLevel.INFERRED,
                decision_maker="system"
            )
            decisions.append(decision)
        
        return decisions
    
    def identify_ownership_patterns(self, symbol_graph: GraphSnapshot) -> Dict[str, OwnershipPattern]:
        """
        Identifies ownership patterns across the codebase.
        
        Args:
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            Dictionary mapping owner IDs to ownership patterns
        """
        start = time.time()
        ownership_patterns = {}
        
        # Group symbols by directory structure to infer ownership
        directory_groups: Dict[str, List[str]] = defaultdict(list)
        
        for symbol_id, symbol in symbol_graph.symbols.items():
            path = Path(symbol.file_path)
            # Use second-level directory as potential ownership indicator
            if len(path.parts) >= 2:
                owner = path.parts[1]
            else:
                owner = path.parts[0] if path.parts else "root"
            directory_groups[owner].append(symbol_id)
        
        # Create ownership patterns for each group
        for owner, symbol_ids in directory_groups.items():
            if len(symbol_ids) > 0:
                # Determine ownership type based on group size
                if len(symbol_ids) > 50:
                    ownership_type = "layer"
                elif len(symbol_ids) > 10:
                    ownership_type = "module"
                else:
                    ownership_type = "domain"
                
                pattern = OwnershipPattern(
                    owner_id=owner,
                    owned_components=symbol_ids,
                    ownership_type=ownership_type,
                    responsibility_boundary=owner,
                    confidence=ConfidenceLevel.INFERRED
                )
                ownership_patterns[owner] = pattern
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("identify_ownership_patterns", elapsed)
        
        return ownership_patterns
    
    def infer_architectural_role_for_file(self, file_id: str, symbol_graph: GraphSnapshot) -> str:
        """
        Infer the architectural role of a file based on its contents and structure.
        
        Args:
            file_id: File ID to analyze
            symbol_graph: Current symbol graph snapshot
            
        Returns:
            Inferred architectural role
        """
        file = symbol_graph.files.get(file_id)
        if not file:
            return "unknown"
        
        path_lower = file.file_path.lower()
        
        # Check path for architectural indicators
        for layer, patterns in self.ARCHITECTURAL_PATTERNS.items():
            for pattern in patterns:
                if pattern in path_lower:
                    return layer
        
        # Check symbol names in the file
        for symbol in file.symbols:
            symbol_name_lower = symbol.symbol_name.lower()
            for layer, patterns in self.ARCHITECTURAL_PATTERNS.items():
                for pattern in patterns:
                    if pattern in symbol_name_lower:
                        return layer
        
        # Default based on imports and structure
        if len(file.imports) > 10:
            return "application"
        elif len(file.imports) == 0:
            return "domain"
        else:
            return "shared"
