# graph.py - Symbol Graph Engine for Repository Intelligence Engine
# Layer 3: Builds integrated graph of symbols and their relationships

import asyncio
import time
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path
import logging
import json
from collections import defaultdict, deque

from repo_intelligence.types import (
    SymbolId, FileId, SymbolNode, SymbolEdge, EdgeType, ConfidenceLevel,
    ParsedFile, GraphSnapshot, SymbolMap, PerformanceMetrics
)
from repo_intelligence.types_extended import (
    OwnershipInfo, ResponsibilityBoundary, OwnershipChange, OwnershipPattern
)

log = logging.getLogger("aelvo.repo_intelligence.graph")


class SymbolGraphEngine:
    def __init__(self):
        self.graph: GraphSnapshot = GraphSnapshot(files={}, symbols={}, edges=[], version=0)
        self.name_index: Dict[str, str] = {}
        self.exported_symbols: Set[str] = set()
        self.import_index: Dict[str, Dict[str, str]] = {}
        self.metrics: List[PerformanceMetrics] = []
        
        # Ownership tracking
        self.ownership_map: Dict[str, OwnershipInfo] = {}  # symbol_id -> OwnershipInfo
        self.responsibility_boundaries: Dict[str, ResponsibilityBoundary] = {}  # boundary_id -> ResponsibilityBoundary
        self.ownership_patterns: Dict[str, OwnershipPattern] = {}  # owner_id -> OwnershipPattern
        self.ownership_history: List[OwnershipChange] = []  # historical ownership changes

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def _add_symbol(self, symbol: SymbolNode) -> None:
        self.graph.symbols[symbol.symbol_id] = symbol
        if symbol.is_exported:
            self.exported_symbols.add(symbol.symbol_id)
            self.name_index[symbol.symbol_name] = symbol.symbol_id
            self.name_index[symbol.fully_qualified_name] = symbol.symbol_id
        else:
            existing = self.name_index.get(symbol.symbol_name)
            if existing is None:
                self.name_index[symbol.symbol_name] = symbol.symbol_id

    def _add_edge(self, edge: SymbolEdge) -> None:
        existing_ids = {(e.source_id, e.target_id, e.edge_type.value) for e in self.graph.edges}
        key = (edge.source_id, edge.target_id, edge.edge_type.value)
        if key not in existing_ids:
            self.graph.edges.append(edge)

    def _build_within_file_edges(self, parsed_file: ParsedFile) -> List[SymbolEdge]:
        edges = []
        for symbol in parsed_file.symbols:
            if symbol.parent_symbol_id:
                edge = SymbolEdge(
                    source_id=symbol.parent_symbol_id,
                    target_id=symbol.symbol_id,
                    edge_type=EdgeType.CONTAINS,
                    file_path=parsed_file.file_path,
                    line_number=symbol.line_range[0],
                    confidence=ConfidenceLevel.CERTAIN,
                )
                edges.append(edge)
        return edges

    def _build_file_to_file_map(self) -> Dict[str, str]:
        return {
            pf.file_path: pf.file_id
            for pf in self.graph.files.values()
        }

    def _resolve_relative_import(
        self, module: str, level: int, importing_file_path: str
    ) -> Optional[str]:
        importing_path = Path(importing_file_path)
        parts = list(importing_path.parts)
        if level > 0:
            parts = parts[:-1]
            for _ in range(level - 1):
                if parts:
                    parts.pop()
        if not module:
            return "/".join(parts) if parts else None
        module_parts = module.split('.')
        result_parts = parts + module_parts
        return "/".join(result_parts)

    def _resolve_star_import(self, module_name: str) -> List[str]:
        resolved = []
        for sym_id, sym in self.graph.symbols.items():
            if sym.is_exported:
                file_path = sym.file_path.replace('\\', '/')
                module_path = file_path.replace('.py', '').replace('__init__', '').strip('/')
                module_path = module_path.replace('/', '.')
                if module_path == module_name or module_path.startswith(module_name):
                    resolved.append(sym_id)
        return resolved

    def _resolve_cross_file_reference(
        self, reference: str, parsing_file_id: str
    ) -> Optional[str]:
        if reference.startswith("__star_import__:"):
            module = reference.split(":", 1)[1]
            resolved = self._resolve_star_import(module)
            return resolved[0] if resolved else None
        if reference in self.name_index:
            return self.name_index[reference]
        parts = reference.split('.')
        for i in range(max(1, len(parts) - 1), len(parts)):
            candidate = '.'.join(parts[i:])
            if candidate in self.name_index:
                return self.name_index[candidate]
        return None

    def _compute_module_path_for_file(self, file_id: str) -> str:
        pf = self.graph.files.get(file_id)
        if not pf:
            return ""
        path = pf.file_path.replace('\\', '/')
        if path.endswith('__init__.py'):
            path = path[:-len('__init__.py')].rstrip('/')
        elif path.endswith('.py'):
            path = path[:-3]
        return path.replace('/', '.')

    def _get_re_exports(self) -> Dict[str, str]:
        re_exports = {}
        for sym in self.graph.symbols.values():
            for edge in self.graph.get_edges_to(sym.symbol_id):
                if edge.edge_type == EdgeType.IMPORTS:
                    source_sym = self.graph.symbols.get(edge.source_id)
                    if source_sym and source_sym.symbol_name == sym.symbol_name and source_sym.is_exported:
                        re_exports[source_sym.fully_qualified_name] = sym.symbol_id
        return re_exports

    def _build_cross_file_edges(self) -> List[SymbolEdge]:
        edges = []
        for file_id, pf in self.graph.files.items():
            for ref in pf.unresolved_references:
                if ref.startswith("__star_import__:"):
                    module = ref.split(":", 1)[1]
                    star_symbols = self._resolve_star_import(module)
                    for sym_id in star_symbols:
                        edge = SymbolEdge(
                            source_id=file_id,
                            target_id=sym_id,
                            edge_type=EdgeType.IMPORTS,
                            file_path=pf.file_path,
                            line_number=0,
                            confidence=ConfidenceLevel.INFERRED,
                        )
                        edges.append(edge)
                    continue
                resolved_id = self._resolve_cross_file_reference(ref, file_id)
                if resolved_id:
                    edge = SymbolEdge(
                        source_id=file_id,
                        target_id=resolved_id,
                        edge_type=EdgeType.IMPORTS,
                        file_path=pf.file_path,
                        line_number=0,
                        confidence=ConfidenceLevel.INFERRED,
                    )
                    edges.append(edge)
        re_exports = self._get_re_exports()
        for fq_name, target_id in re_exports.items():
            for sym in self.graph.symbols.values():
                if sym.fully_qualified_name == fq_name and sym.symbol_id != target_id:
                    edge = SymbolEdge(
                        source_id=sym.symbol_id,
                        target_id=target_id,
                        edge_type=EdgeType.REFERENCES,
                        file_path=sym.file_path,
                        line_number=sym.line_range[0],
                        confidence=ConfidenceLevel.INFERRED,
                    )
                    edges.append(edge)
        return edges

    def _build_definition_edges(self) -> List[SymbolEdge]:
        edges = []
        for file_id, pf in self.graph.files.items():
            if pf.fingerprint:
                for sym in pf.symbols:
                    edge = SymbolEdge(
                        source_id=file_id,
                        target_id=sym.symbol_id,
                        edge_type=EdgeType.DEFINES,
                        file_path=pf.file_path,
                        line_number=sym.line_range[0],
                        confidence=ConfidenceLevel.CERTAIN,
                    )
                    edges.append(edge)
        return edges

    async def build_graph(self, parsed_files: List[ParsedFile]) -> GraphSnapshot:
        start = time.time()
        self.graph.version += 1
        for parsed_file in parsed_files:
            self.graph.files[parsed_file.file_id] = parsed_file
            for symbol in parsed_file.symbols:
                self._add_symbol(symbol)
            within_edges = self._build_within_file_edges(parsed_file)
            for edge in within_edges:
                self._add_edge(edge)
        def_edges = self._build_definition_edges()
        for edge in def_edges:
            self._add_edge(edge)
        cross_edges = self._build_cross_file_edges()
        for edge in cross_edges:
            self._add_edge(edge)
        self._build_inheritance_edges()
        elapsed = (time.time() - start) * 1000
        self._record_metric("build_graph", elapsed)
        log.info(f"Built graph v{self.graph.version}: {len(self.graph.symbols)} symbols, {len(self.graph.edges)} edges ({elapsed:.0f}ms)")
        return self.graph

    def _build_inheritance_edges(self) -> None:
        for sym in self.graph.symbols.values():
            for base_name in sym.base_class_names:
                resolved = self._resolve_cross_file_reference(base_name, sym.file_id)
                if resolved:
                    edge = SymbolEdge(
                        source_id=sym.symbol_id,
                        target_id=resolved,
                        edge_type=EdgeType.INHERITS,
                        file_path=sym.file_path,
                        line_number=sym.line_range[0],
                        confidence=ConfidenceLevel.CERTAIN,
                    )
                    self._add_edge(edge)

    def _get_mro_symbols(self, class_sym: SymbolNode) -> List[str]:
        mro = [class_sym.symbol_id]
        visited = {class_sym.symbol_id}
        queue = deque([class_sym.symbol_id])
        while queue:
            current = queue.popleft()
            for edge in self.graph.get_edges_from(current):
                if edge.edge_type == EdgeType.INHERITS:
                    if edge.target_id not in visited:
                        visited.add(edge.target_id)
                        queue.append(edge.target_id)
                        mro.append(edge.target_id)
        return mro

    def update_file(self, parsed_file: ParsedFile) -> None:
        old_file = self.graph.files.get(parsed_file.file_id)
        if old_file:
            for old_sym in old_file.symbols:
                self.graph.symbols.pop(old_sym.symbol_id, None)
                self.exported_symbols.discard(old_sym.symbol_id)
            self.graph.edges = [
                e for e in self.graph.edges
                if not (e.file_path == old_file.file_path)
            ]
        self.graph.files[parsed_file.file_id] = parsed_file
        for symbol in parsed_file.symbols:
            self._add_symbol(symbol)
        within_edges = self._build_within_file_edges(parsed_file)
        for edge in within_edges:
            self._add_edge(edge)
        def_edges = self._build_definition_edges()
        for edge in def_edges:
            if edge.source_id == parsed_file.file_id:
                self._add_edge(edge)
        self.graph.version += 1

    def resolve_cross_file_references_for_file(self, file_id: str) -> None:
        pf = self.graph.files.get(file_id)
        if not pf:
            return
        for ref in pf.unresolved_references:
            resolved_id = self._resolve_cross_file_reference(ref, file_id)
            if resolved_id:
                edge = SymbolEdge(
                    source_id=file_id,
                    target_id=resolved_id,
                    edge_type=EdgeType.IMPORTS,
                    file_path=pf.file_path,
                    line_number=0,
                    confidence=ConfidenceLevel.INFERRED,
                )
                self._add_edge(edge)

    def remove_file(self, file_id: str) -> None:
        pf = self.graph.files.pop(file_id, None)
        if pf:
            for sym in self.graph.get_symbols_in_file(file_id):
                self.graph.symbols.pop(sym.symbol_id, None)
                self.exported_symbols.discard(sym.symbol_id)
            self.graph.edges = [
                e for e in self.graph.edges
                if not (e.file_path == pf.file_path)
            ]
            self.graph.version += 1

    def get_symbol_by_id(self, symbol_id: str) -> Optional[SymbolNode]:
        return self.graph.symbols.get(symbol_id)

    def get_symbols_by_name(self, name: str) -> List[SymbolNode]:
        return [s for s in self.graph.symbols.values() if s.symbol_name == name]

    def get_symbols_in_file(self, file_id: str) -> List[SymbolNode]:
        return self.graph.get_symbols_in_file(file_id)

    def get_edges_from(self, symbol_id: str) -> List[SymbolEdge]:
        return self.graph.get_edges_from(symbol_id)

    def get_edges_to(self, symbol_id: str) -> List[SymbolEdge]:
        return self.graph.get_edges_to(symbol_id)

    def get_edges_by_type(self, edge_type: EdgeType) -> List[SymbolEdge]:
        return self.graph.get_edges_by_type(edge_type)

    def get_dependencies(self, symbol_id: str, depth: int = -1) -> List[SymbolNode]:
        visited = set()
        result = []
        queue = deque([(symbol_id, 0)])
        visited.add(symbol_id)
        while queue:
            current, d = queue.popleft()
            if 0 <= depth <= d:
                continue
            for edge in self.graph.get_edges_from(current):
                target = self.graph.symbols.get(edge.target_id)
                if target and edge.target_id not in visited:
                    visited.add(edge.target_id)
                    result.append(target)
                    queue.append((edge.target_id, d + 1))
        return result

    def get_dependents(self, symbol_id: str, depth: int = -1) -> List[SymbolNode]:
        visited = set()
        result = []
        queue = deque([(symbol_id, 0)])
        visited.add(symbol_id)
        while queue:
            current, d = queue.popleft()
            if 0 <= depth <= d:
                continue
            for edge in self.graph.get_edges_to(current):
                source = self.graph.symbols.get(edge.source_id)
                if source and edge.source_id not in visited:
                    visited.add(edge.source_id)
                    result.append(source)
                    queue.append((edge.source_id, d + 1))
        return result

    def find_shortest_path(self, source_id: str, target_id: str) -> Optional[List[str]]:
        if source_id not in self.graph.symbols or target_id not in self.graph.symbols:
            return None
        queue = deque([(source_id, [source_id])])
        visited = {source_id}
        while queue:
            current_id, path = queue.popleft()
            if current_id == target_id:
                return path
            neighbors = set()
            for edge in self.graph.get_edges_from(current_id):
                neighbors.add(edge.target_id)
            for edge in self.graph.get_edges_to(current_id):
                neighbors.add(edge.source_id)
            for neighbor in neighbors:
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))
        return None

    def get_mro_for_class(self, class_name: str) -> List[str]:
        for sym in self.graph.symbols.values():
            if sym.symbol_name == class_name and sym.symbol_kind.value == 'class':
                return self._get_mro_symbols(sym)
        return []

    def serialize(self) -> str:
        data = {
            'files': {fid: pf.model_dump(mode='json') for fid, pf in self.graph.files.items()},
            'symbols': {sid: sym.model_dump(mode='json') for sid, sym in self.graph.symbols.items()},
            'edges': [edge.model_dump(mode='json') for edge in self.graph.edges],
            'version': self.graph.version,
            'timestamp': self.graph.timestamp.isoformat(),
        }
        return json.dumps(data, indent=2)

    def deserialize(self, data: str) -> None:
        parsed = json.loads(data)
        self.graph = GraphSnapshot(
            files={fid: ParsedFile(**pf_data) for fid, pf_data in parsed['files'].items()},
            symbols={sid: SymbolNode(**sym_data) for sid, sym_data in parsed['symbols'].items()},
            edges=[SymbolEdge(**edge_data) for edge_data in parsed['edges']],
            version=parsed.get('version', 0),
            timestamp=parsed.get('timestamp', None),
        )
        self.name_index = {}
        self.exported_symbols = set()
        for symbol in self.graph.symbols.values():
            if symbol.is_exported:
                self.exported_symbols.add(symbol.symbol_id)
                self.name_index[symbol.symbol_name] = symbol.symbol_id
                self.name_index[symbol.fully_qualified_name] = symbol.symbol_id

    def save_to_disk(self, path: str) -> None:
        with open(path, 'w') as f:
            f.write(self.serialize())
        log.info(f"Graph saved to {path} (v{self.graph.version})")

    def load_from_disk(self, path: str) -> bool:
        try:
            with open(path, 'r') as f:
                data = f.read()
            self.deserialize(data)
            log.info(f"Graph loaded from {path} (v{self.graph.version})")
            return True
        except Exception as e:
            log.error(f"Failed to load graph from {path}: {e}")
            return False

    def get_metrics(self) -> List[PerformanceMetrics]:
        return self.metrics.copy()
    
    # ===========================================================================
    # Ownership Tracking Methods
    # ===========================================================================
    
    def infer_ownership(self, symbol_id: str) -> OwnershipInfo:
        """Infers ownership based on directory structure and patterns"""
        symbol = self.graph.symbols.get(symbol_id)
        if not symbol:
            return OwnershipInfo(
                component_id=symbol_id,
                owner="unknown",
                ownership_confidence=0.0,
                ownership_evidence=[],
                responsibility_boundaries=[]
            )
        
        # Extract ownership from file path
        file_path = Path(symbol.file_path)
        parts = file_path.parts
        
        # Common ownership patterns
        owner = "unknown"
        confidence = 0.5
        evidence = []
        responsibility_boundaries = []
        
        # Directory-based ownership
        if len(parts) >= 2:
            # Second-level directory often indicates module ownership
            potential_owner = parts[1] if len(parts) > 1 else parts[0]
            owner = potential_owner
            confidence = 0.7
            evidence.append(f"Directory structure suggests ownership by {potential_owner}")
            responsibility_boundaries.append(str(Path(*parts[:2])))
        
        # Naming convention ownership
        if symbol.symbol_name.startswith('_'):
            evidence.append("Private symbol suggests internal ownership")
            confidence += 0.1
        
        # Export status
        if symbol.is_exported:
            evidence.append("Exported symbol suggests public API ownership")
            responsibility_boundaries.append("public_api")
        
        return OwnershipInfo(
            component_id=symbol_id,
            owner=owner,
            ownership_confidence=min(confidence, 1.0),
            ownership_evidence=evidence,
            responsibility_boundaries=responsibility_boundaries
        )
    
    def get_ownership_hierarchy(self, symbol_id: str) -> List[OwnershipInfo]:
        """Returns ownership hierarchy for a symbol"""
        hierarchy = []
        current_id = symbol_id
        visited = set()
        
        while current_id and current_id not in visited:
            visited.add(current_id)
            ownership = self.ownership_map.get(current_id)
            if not ownership:
                ownership = self.infer_ownership(current_id)
                self.ownership_map[current_id] = ownership
            
            hierarchy.append(ownership)
            
            # Move to parent symbol
            symbol = self.graph.symbols.get(current_id)
            if symbol and symbol.parent_symbol_id:
                current_id = symbol.parent_symbol_id
            else:
                break
        
        return hierarchy
    
    def track_ownership_changes(self, old_graph: GraphSnapshot, new_graph: GraphSnapshot) -> List[OwnershipChange]:
        """Tracks changes in ownership between graph versions"""
        changes = []
        
        # Check for new symbols
        for symbol_id in new_graph.symbols:
            if symbol_id not in old_graph.symbols:
                new_ownership = self.infer_ownership(symbol_id)
                change = OwnershipChange(
                    component_id=symbol_id,
                    old_owner=None,
                    new_owner=new_ownership.owner,
                    change_type="assigned"
                )
                changes.append(change)
                self.ownership_history.append(change)
        
        # Check for removed symbols
        for symbol_id in old_graph.symbols:
            if symbol_id not in new_graph.symbols:
                old_ownership = self.ownership_map.get(symbol_id)
                if old_ownership:
                    change = OwnershipChange(
                        component_id=symbol_id,
                        old_owner=old_ownership.owner,
                        new_owner="none",
                        change_type="released"
                    )
                    changes.append(change)
                    self.ownership_history.append(change)
        
        return changes
    
    def identify_ownership_patterns(self) -> Dict[str, OwnershipPattern]:
        """Identifies ownership patterns across the codebase"""
        ownership_groups: Dict[str, List[str]] = defaultdict(list)
        
        # Group symbols by inferred owner
        for symbol_id in self.graph.symbols:
            ownership = self.ownership_map.get(symbol_id)
            if not ownership:
                ownership = self.infer_ownership(symbol_id)
                self.ownership_map[symbol_id] = ownership
            ownership_groups[ownership.owner].append(symbol_id)
        
        # Create ownership patterns
        patterns = {}
        for owner, component_ids in ownership_groups.items():
            # Determine ownership type based on component count and structure
            if len(component_ids) > 50:
                ownership_type = "layer"
            elif len(component_ids) > 10:
                ownership_type = "module"
            else:
                ownership_type = "domain"
            
            # Find common responsibility boundary
            boundaries = set()
            for component_id in component_ids:
                ownership = self.ownership_map.get(component_id)
                if ownership:
                    boundaries.update(ownership.responsibility_boundaries)
            
            primary_boundary = list(boundaries)[0] if boundaries else owner
            
            pattern = OwnershipPattern(
                owner_id=owner,
                owned_components=component_ids,
                ownership_type=ownership_type,
                responsibility_boundary=primary_boundary,
                confidence=ConfidenceLevel.INFERRED
            )
            patterns[owner] = pattern
            self.ownership_patterns[owner] = pattern
        
        return patterns
    
    def set_ownership(self, symbol_id: str, owner: str, reason: str = "") -> None:
        """Manually set ownership for a symbol"""
        old_ownership = self.ownership_map.get(symbol_id)
        old_owner = old_ownership.owner if old_ownership else None
        
        new_ownership = OwnershipInfo(
            component_id=symbol_id,
            owner=owner,
            ownership_confidence=1.0,  # Manual assignment has high confidence
            ownership_evidence=[f"Manually assigned: {reason}"] if reason else ["Manually assigned"],
            responsibility_boundaries=old_ownership.responsibility_boundaries if old_ownership else [owner]
        )
        
        self.ownership_map[symbol_id] = new_ownership
        
        if old_owner and old_owner != owner:
            change = OwnershipChange(
                component_id=symbol_id,
                old_owner=old_owner,
                new_owner=owner,
                change_type="transferred"
            )
            self.ownership_history.append(change)
    
    def get_components_by_owner(self, owner: str) -> List[str]:
        """Get all components owned by a specific owner"""
        pattern = self.ownership_patterns.get(owner)
        if pattern:
            return pattern.owned_components
        
        # Fallback to scanning ownership map
        return [
            symbol_id for symbol_id, ownership in self.ownership_map.items()
            if ownership.owner == owner
        ]
    
    def get_responsibility_boundaries(self) -> Dict[str, ResponsibilityBoundary]:
        """Get all identified responsibility boundaries"""
        if not self.responsibility_boundaries:
            self.identify_ownership_patterns()
            
            # Create responsibility boundaries from ownership patterns
            for pattern in self.ownership_patterns.values():
                boundary = ResponsibilityBoundary(
                    boundary_id=pattern.responsibility_boundary,
                    name=pattern.responsibility_boundary,
                    components=pattern.owned_components,
                    boundary_type=pattern.ownership_type,
                    interface=[]  # Could be populated by analyzing exported symbols
                )
                self.responsibility_boundaries[boundary.boundary_id] = boundary
        
        return self.responsibility_boundaries
    
    def enhance_serialization_with_ownership(self) -> str:
        """Enhanced serialization that includes ownership data"""
        data = {
            'files': {fid: pf.model_dump(mode='json') for fid, pf in self.graph.files.items()},
            'symbols': {sid: sym.model_dump(mode='json') for sid, sym in self.graph.symbols.items()},
            'edges': [edge.model_dump(mode='json') for edge in self.graph.edges],
            'version': self.graph.version,
            'timestamp': self.graph.timestamp.isoformat(),
            'ownership': {
                sid: ownership.model_dump(mode='json') 
                for sid, ownership in self.ownership_map.items()
            },
            'ownership_patterns': {
                oid: pattern.model_dump(mode='json') 
                for oid, pattern in self.ownership_patterns.items()
            },
            'responsibility_boundaries': {
                bid: boundary.model_dump(mode='json') 
                for bid, boundary in self.responsibility_boundaries.items()
            }
        }
        return json.dumps(data, indent=2)
    
    def enhance_deserialization_with_ownership(self, data: str) -> None:
        """Enhanced deserialization that includes ownership data"""
        parsed = json.loads(data)
        self.graph = GraphSnapshot(
            files={fid: ParsedFile(**pf_data) for fid, pf_data in parsed['files'].items()},
            symbols={sid: SymbolNode(**sym_data) for sid, sym_data in parsed['symbols'].items()},
            edges=[SymbolEdge(**edge_data) for edge_data in parsed['edges']],
            version=parsed.get('version', 0),
            timestamp=parsed.get('timestamp', None),
        )
        self.name_index = {}
        self.exported_symbols = set()
        for symbol in self.graph.symbols.values():
            if symbol.is_exported:
                self.exported_symbols.add(symbol.symbol_id)
                self.name_index[symbol.symbol_name] = symbol.symbol_id
                self.name_index[symbol.fully_qualified_name] = symbol.symbol_id
        
        # Restore ownership data
        if 'ownership' in parsed:
            self.ownership_map = {
                sid: OwnershipInfo(**ownership_data) 
                for sid, ownership_data in parsed['ownership'].items()
            }
        
        if 'ownership_patterns' in parsed:
            self.ownership_patterns = {
                oid: OwnershipPattern(**pattern_data) 
                for oid, pattern_data in parsed['ownership_patterns'].items()
            }
        
        if 'responsibility_boundaries' in parsed:
            self.responsibility_boundaries = {
                bid: ResponsibilityBoundary(**boundary_data) 
                for bid, boundary_data in parsed['responsibility_boundaries'].items()
            }
