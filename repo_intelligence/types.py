# types.py - Foundation Types for Repository Intelligence Engine
# Layer 0: Complete type system that every subsystem uses

from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Any
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
import hashlib


class LanguageId(str, Enum):
    PYTHON = "python"
    TYPESCRIPT = "typescript"
    JAVASCRIPT = "javascript"
    RUST = "rust"
    GO = "go"
    JAVA = "java"
    C = "c"
    CPP = "cpp"
    CSHARP = "csharp"
    SWIFT = "swift"
    KOTLIN = "kotlin"
    RUBY = "ruby"
    PHP = "php"
    UNKNOWN = "unknown"


class EdgeType(str, Enum):
    IMPORTS = "imports"
    CALLS = "calls"
    DEFINES = "defines"
    CONTAINS = "contains"
    INHERITS = "inherits"
    IMPLEMENTS = "implements"
    REFERENCES = "references"
    TESTS = "tests"


class ConfidenceLevel(str, Enum):
    CERTAIN = "certain"
    INFERRED = "inferred"
    APPROXIMATE = "approximate"


class SymbolKind(str, Enum):
    CLASS = "class"
    FUNCTION = "function"
    METHOD = "method"
    VARIABLE = "variable"
    MODULE = "module"
    IMPORT = "import"
    INTERFACE = "interface"
    TYPE_ALIAS = "type_alias"
    ENUM = "enum"
    TRAIT = "trait"
    STRUCT = "struct"
    CONSTANT = "constant"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IndexStatus(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    PARTIAL = "partial"
    UNAVAILABLE = "unavailable"


class SymbolId(str):
    @classmethod
    def create(cls, file_path: str, symbol_path: str) -> str:
        raw = f"{file_path}:{symbol_path}"
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]


class FileId(str):
    @classmethod
    def create(cls, relative_path: str) -> str:
        clean = relative_path.replace('\\', '/')
        return hashlib.sha256(clean.encode('utf-8')).hexdigest()[:12]


class ArgumentInfo(BaseModel):
    name: str
    type_annotation: Optional[str] = None
    has_default: bool = False


class SymbolNode(BaseModel):
    symbol_id: str
    file_id: str
    file_path: str
    line_range: Tuple[int, int]
    symbol_kind: SymbolKind
    symbol_name: str
    fully_qualified_name: str
    parent_symbol_id: Optional[str] = None
    docstring: Optional[str] = None
    decorators: List[str] = Field(default_factory=list)
    type_annotation: Optional[str] = None
    is_exported: bool = False
    confidence: ConfidenceLevel
    is_async: bool = False
    arguments: List[ArgumentInfo] = Field(default_factory=list)
    base_class_names: List[str] = Field(default_factory=list)
    implemented_interfaces: List[str] = Field(default_factory=list)

    @field_validator('symbol_id')
    @classmethod
    def validate_symbol_id(cls, v):
        if not v or len(v) != 16:
            raise ValueError(f"SymbolId must be 16 characters, got {len(v) if v else 0}")
        return v


class SymbolEdge(BaseModel):
    source_id: str
    target_id: str
    edge_type: EdgeType
    file_path: str
    line_number: int
    confidence: ConfidenceLevel


class ParsedFile(BaseModel):
    file_id: str
    file_path: str
    language: LanguageId
    symbols: List[SymbolNode] = Field(default_factory=list)
    unresolved_references: List[str] = Field(default_factory=list)
    parse_success: bool = True
    parse_error: Optional[str] = None
    fingerprint: str
    parse_timestamp: datetime = Field(default_factory=datetime.now)
    size_bytes: int = 0


class GraphSnapshot(BaseModel):
    files: Dict[str, ParsedFile]
    symbols: Dict[str, SymbolNode]
    edges: List[SymbolEdge] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)
    version: int = 0

    def get_symbol_by_name(self, name: str, file_id: Optional[str] = None) -> List[SymbolNode]:
        results = []
        for symbol in self.symbols.values():
            if symbol.symbol_name == name:
                if file_id is None or symbol.file_id == file_id:
                    results.append(symbol)
        return results

    def get_symbols_in_file(self, file_id: str) -> List[SymbolNode]:
        return [s for s in self.symbols.values() if s.file_id == file_id]

    def get_edges_from(self, source_id: str) -> List[SymbolEdge]:
        return [e for e in self.edges if e.source_id == source_id]

    def get_edges_to(self, target_id: str) -> List[SymbolEdge]:
        return [e for e in self.edges if e.target_id == target_id]

    def get_edges_by_type(self, edge_type: EdgeType) -> List[SymbolEdge]:
        return [e for e in self.edges if e.edge_type == edge_type]


class FileScanResult(BaseModel):
    new_files: List[str] = Field(default_factory=list)
    changed_files: List[str] = Field(default_factory=list)
    unchanged_files: List[str] = Field(default_factory=list)
    deleted_files: List[str] = Field(default_factory=list)
    scan_timestamp: datetime = Field(default_factory=datetime.now)

    @property
    def has_changes(self) -> bool:
        return bool(self.new_files or self.changed_files or self.deleted_files)


class ImpactReport(BaseModel):
    changed_file: str
    changed_symbols: List[str] = Field(default_factory=list)
    directly_affected_symbols: List[str] = Field(default_factory=list)
    transitively_affected_files: Set[str] = Field(default_factory=set)
    affected_tests: List[str] = Field(default_factory=list)
    risk_level: RiskLevel
    risk_reasoning: List[str] = Field(default_factory=list)
    confidence: ConfidenceLevel
    analysis_timestamp: datetime = Field(default_factory=datetime.now)


class ContextPacket(BaseModel):
    task_description: str
    active_specialist: str
    relevant_symbols: List[SymbolNode] = Field(default_factory=list)
    dependency_relationships: List[SymbolEdge] = Field(default_factory=list)
    call_relationships: List[SymbolEdge] = Field(default_factory=list)
    architectural_boundaries: Dict[str, List[str]] = Field(default_factory=dict)
    token_estimate: int = 0
    staleness_flag: bool = False
    provenance: Dict[str, str] = Field(default_factory=dict)

    def format_for_prompt(self) -> str:
        lines = [
            "CODEBASE CONTEXT (from Repository Intelligence Engine):",
            ""
        ]
        if self.relevant_symbols:
            lines.append("RELEVANT SYMBOLS:")
            for symbol in self.relevant_symbols[:20]:
                location = f"{symbol.file_path}:{symbol.line_range[0]}-{symbol.line_range[1]}"
                lines.append(f"  {symbol.symbol_name} [{location}] {symbol.symbol_kind.value} ({symbol.confidence.value})")
                if symbol.docstring:
                    short_doc = symbol.docstring[:100].replace('\n', ' ')
                    lines.append(f"    Doc: {short_doc}")
            lines.append("")
        if self.call_relationships:
            lines.append("CALL RELATIONSHIPS:")
            for edge in self.call_relationships[:15]:
                src_name = edge.source_id[:8]
                tgt_name = edge.target_id[:8]
                lines.append(f"  {src_name} -> {tgt_name} ({edge.confidence.value})")
            lines.append("")
        if self.dependency_relationships:
            lines.append("DEPENDENCY CHAIN:")
            for edge in self.dependency_relationships[:10]:
                lines.append(f"  {edge.source_id[:8]} -> {edge.target_id[:8]} ({edge.edge_type.value})")
            lines.append("")
        if self.architectural_boundaries:
            lines.append("ARCHITECTURAL CONTEXT:")
            for boundary, files in self.architectural_boundaries.items():
                shown = files[:5]
                suffix = f" (+{len(files)-5} more)" if len(files) > 5 else ""
                lines.append(f"  {boundary}: {', '.join(shown)}{suffix}")
            lines.append("")
        if self.staleness_flag:
            lines.append("WARNING: Some context data may be stale due to recent file changes.")
        return "\n".join(lines)


class ArchitectureLayer(BaseModel):
    name: str
    files: Set[str] = Field(default_factory=set)
    dependency_direction: Optional[str] = None


class ArchitectureMap(BaseModel):
    layers: List[ArchitectureLayer] = Field(default_factory=list)
    entry_points: List[str] = Field(default_factory=list)
    module_boundaries: Dict[str, List[str]] = Field(default_factory=dict)
    violations: List[str] = Field(default_factory=list)
    analysis_timestamp: datetime = Field(default_factory=datetime.now)


class CallGraphSnapshot(BaseModel):
    calls: Dict[str, List[SymbolEdge]] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.now)
    version: int = 0


class DependencyGraphSnapshot(BaseModel):
    dependencies: Dict[str, Set[str]] = Field(default_factory=dict)
    dependents: Dict[str, Set[str]] = Field(default_factory=dict)
    cycles: List[Set[str]] = Field(default_factory=list)
    topological_order: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)
    version: int = 0


class FileDependencyInfo(BaseModel):
    file_id: str
    file_path: str
    imports: List[str] = Field(default_factory=list)
    imported_by: List[str] = Field(default_factory=list)
    is_entry_point: bool = False
    is_test_file: bool = False


class PerformanceMetrics(BaseModel):
    operation: str
    duration_ms: float
    timestamp: datetime = Field(default_factory=datetime.now)


class QueryProvenance(BaseModel):
    source: str
    timestamp: datetime = Field(default_factory=datetime.now)
    graph_version: int = 0
    is_stale: bool = False


class QueryResult(BaseModel):
    data: Any
    confidence: ConfidenceLevel
    provenance: QueryProvenance
    staleness_flag: bool = False


class GenerationRecord(BaseModel):
    artifact_id: str
    generation: int = 0
    dependency_file_ids: Set[str] = Field(default_factory=set)
    timestamp: datetime = Field(default_factory=datetime.now)


class IndexerState(BaseModel):
    generation_counter: int = 0
    artifact_generations: Dict[str, GenerationRecord] = Field(default_factory=dict)
    invalidated_artifacts: Set[str] = Field(default_factory=set)
    is_rebuilding: bool = False


SymbolMap = Dict[str, SymbolNode]
EdgeList = List[SymbolEdge]
FileMap = Dict[str, ParsedFile]
