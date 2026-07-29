# engine.py - Repository Intelligence Engine
# Main entry point that coordinates all subsystems

import asyncio
import time
import os
import logging
from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime

from repo_intelligence.types import (
    LanguageId, SymbolId, FileId, SymbolNode, SymbolEdge, EdgeType,
    ConfidenceLevel, ParsedFile, GraphSnapshot, ImpactReport,
    ArchitectureMap, ContextPacket, FileScanResult, FileDependencyInfo,
    CallGraphSnapshot, DependencyGraphSnapshot, IndexStatus,
    QueryResult, PerformanceMetrics
)
from repo_intelligence.types_extended import ModificationRecord
from repo_intelligence.scanner import FileScanner
from repo_intelligence.parser import ASTParser
from repo_intelligence.graph import SymbolGraphEngine
from repo_intelligence.dep_graph import DependencyGraphEngine
from repo_intelligence.call_graph import CallGraphEngine
from repo_intelligence.indexer import IncrementalIndexer
from repo_intelligence.impact import ChangeImpactAnalyzer
from repo_intelligence.architecture import ArchitectureMapper
from repo_intelligence.query import QueryEngine
from repo_intelligence.context import ContextInjectionBuilder
from repo_intelligence.repository_memory import RepositoryMemorySystem
from repo_intelligence.governance import GovernanceSystem
from repo_intelligence.runtime_inference import RuntimeRelationshipInference
from repo_intelligence.health_analysis import HealthAnalysisSystem
from repo_intelligence.drift_detection import DriftDetectionSystem
from repo_intelligence.risk_analysis import RepositoryRiskAnalyzer
from repo_intelligence.evolution_intelligence import RepositoryEvolutionIntelligence
from repo_intelligence.specialist_integrations import SpecialistIntegrations
from repo_intelligence.reports import ReportGenerator

log = logging.getLogger("aelvo.repo_intelligence.engine")


class RepoIntelligenceEngine:
    def __init__(
        self,
        workspace_root: str,
        exclusions: Optional[Set[str]] = None,
        max_workers: int = 4,
        max_context_tokens: int = 4000,
        watch_enabled: bool = False,
        enable_memory: bool = True,
        enable_governance: bool = True,
    ):
        self.workspace_root = Path(workspace_root).resolve()
        self.scanner = FileScanner(
            workspace_root=str(self.workspace_root),
            exclusions=exclusions,
            max_workers=max_workers,
        )
        self.parser = ASTParser(max_workers=max_workers)
        self.symbol_graph = SymbolGraphEngine()
        self.dep_graph_engine = DependencyGraphEngine()
        self.call_graph_engine = CallGraphEngine()
        self.indexer = IncrementalIndexer(watch_enabled=watch_enabled)
        self.impact_analyzer = ChangeImpactAnalyzer()
        self.architecture_mapper = ArchitectureMapper()
        self.query_engine = QueryEngine()
        self.context_builder = ContextInjectionBuilder(max_tokens=max_context_tokens)

        self._is_initialized = False
        self._architecture: Optional[ArchitectureMap] = None
        self.metrics: List[PerformanceMetrics] = []
        
        # New advanced systems
        self.runtime_inference = RuntimeRelationshipInference(self.symbol_graph)
        self.health_system = HealthAnalysisSystem()
        self.drift_system = DriftDetectionSystem()
        self.risk_analyzer = RepositoryRiskAnalyzer()
        self.evolution_intelligence = RepositoryEvolutionIntelligence()
        self.specialist_integrations = SpecialistIntegrations(self)
        self.report_generator = ReportGenerator(self)
        
        if enable_memory:
            memory_db_path = str(self.workspace_root / ".aelvo_runtime" / "repository_memory.db")
            self.repository_memory = RepositoryMemorySystem(memory_db_path)
        else:
            self.repository_memory = None
        
        if enable_governance:
            governance_db_path = str(self.workspace_root / ".aelvo_runtime" / "repository_governance.db")
            self.governance_system = GovernanceSystem(governance_db_path)
        else:
            self.governance_system = None

    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))

    def _get_file_id(self, path: str) -> str:
        abs_p = Path(path).resolve()
        try:
            rel = abs_p.relative_to(self.workspace_root)
        except ValueError:
            rel = abs_p
        return FileId.create(str(rel).replace('\\', '/'))

    @property
    def status(self) -> IndexStatus:
        if not self._is_initialized:
            return IndexStatus.UNAVAILABLE
        stale = self.indexer.get_stale_files()
        if stale:
            return IndexStatus.STALE
        return IndexStatus.CURRENT

    def _resolve_parsed_paths(self, parsed_files):
        resolved = []
        for pf in parsed_files:
            abs_path = Path(self.workspace_root / pf.file_path).resolve()
            pf.file_path = str(abs_path).replace('\\', '/')
            pf.file_id = self._get_file_id(pf.file_path)
            resolved.append(pf)
        return resolved

    async def initialize(self, full_scan: bool = True) -> IndexStatus:
        start = time.time()
        log.info(f"Initializing Repository Intelligence Engine: {self.workspace_root}")
        scanned = await self.scanner.scan_directory()
        self._resolve_parsed_paths(scanned)
        parsed = await self.parser.parse_files(scanned)
        await self.symbol_graph.build_graph(parsed)
        self.dep_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
        self.call_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
        self._architecture = self.architecture_mapper.build_map(
            self.symbol_graph.graph,
            self.dep_graph_engine.graph,
            self.dep_graph_engine.file_info,
        )
        for pf in parsed:
            fid = pf.file_id
            deps = self.indexer.compute_file_dependencies(fid, self.symbol_graph.graph.files)
            self.indexer.register_generation(fid, deps)
        self._is_initialized = True
        
        # Initialize ownership tracking on the symbol graph
        self.symbol_graph.identify_ownership_patterns()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("initialize", elapsed)
        log.info(f"Engine initialized: {len(parsed)} files, "
                 f"{len(self.symbol_graph.graph.symbols)} symbols, "
                 f"{len(self.symbol_graph.graph.edges)} edges ({elapsed:.0f}ms)")
        return self.status

    async def refresh(self) -> IndexStatus:
        start = time.time()
        if not self._is_initialized:
            return await self.initialize()
        previous_fingerprints = self.scanner.get_fingerprints()
        scan_result = await self.scanner.scan_incremental(previous_fingerprints)
        if not scan_result.has_changes:
            self._record_metric("refresh_noop", (time.time() - start) * 1000)
            return self.status
        self.indexer.process_scan_result(scan_result)
        files_to_reparse = scan_result.new_files + scan_result.changed_files
        files_to_remove = scan_result.deleted_files
        
        abs_files_to_reparse = {
            str(Path(self.workspace_root / p).resolve()).replace('\\', '/') for p in files_to_reparse
        }
        
        for path in files_to_remove:
            fid = self._get_file_id(path)
            self.symbol_graph.remove_file(fid)
            self.indexer.mark_file_fresh(fid)
            
        all_scanned = await self.scanner.scan_directory()
        self._resolve_parsed_paths(all_scanned)
        new_scanned = [s for s in all_scanned if s.file_path in abs_files_to_reparse]
        parsed_new = await self.parser.parse_files(new_scanned)
        
        for pf in parsed_new:
            pf.file_path = str(Path(pf.file_path).resolve()).replace('\\', '/')
            pf.file_id = self._get_file_id(pf.file_path)
            if pf.file_id in self.symbol_graph.graph.files:
                self.symbol_graph.update_file(pf)
            else:
                self.symbol_graph.graph.files[pf.file_id] = pf
                for sym in pf.symbols:
                    self.symbol_graph._add_symbol(sym)
                    
        self.symbol_graph.graph.version += 1
        
        for path in files_to_reparse:
            fid = self._get_file_id(path)
            pf = self.symbol_graph.graph.files.get(fid)
            if pf:
                self.symbol_graph.resolve_cross_file_references_for_file(pf.file_id)
                self.indexer.mark_file_fresh(fid)
                
        self.dep_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
        self.call_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
        self._architecture = self.architecture_mapper.build_map(
            self.symbol_graph.graph,
            self.dep_graph_engine.graph,
            self.dep_graph_engine.file_info,
        )
        
        for path in files_to_reparse:
            fid = self._get_file_id(path)
            deps = self.indexer.compute_file_dependencies(fid, self.symbol_graph.graph.files)
            self.indexer.register_generation(fid, deps)
            self.indexer.mark_rebuilt(fid)
            
        self.scanner.update_fingerprints({
            str(Path(pf.file_path).relative_to(self.workspace_root)).replace('\\', '/'): pf.fingerprint
            for pf in parsed_new
        })
        
        # Track modifications in repository memory
        if self.repository_memory and files_to_reparse:
            for path in files_to_reparse:
                fid = self._get_file_id(path)
                pf = self.symbol_graph.graph.files.get(fid)
                if pf:
                    modified_symbols = [sym.symbol_id for sym in pf.symbols]
                    modification = ModificationRecord(
                        modification_id=f"refresh_{fid}_{int(time.time())}",
                        modified_files=[path],
                        modified_symbols=modified_symbols,
                        modification_type="refresh",
                        specialist="system",
                        success=True,
                        issues=[],
                        task_context="Scheduled refresh"
                    )
                    self.repository_memory.record_modification(modification)
        
        # Update ownership patterns after refresh
        self.symbol_graph.identify_ownership_patterns()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("refresh", elapsed)
        log.info(f"Engine refreshed: {len(files_to_reparse)} files re-parsed ({elapsed:.0f}ms)")
        return self.status

    async def refresh_file(self, file_path: str) -> IndexStatus:
        start = time.time()
        fid = self._get_file_id(file_path)
        abs_path = Path(file_path).resolve()
        scanned = await self.scanner.scan_file(abs_path)
        if not scanned:
            pf = self.symbol_graph.graph.files.get(fid)
            if pf:
                self.symbol_graph.remove_file(fid)
            self.indexer.mark_file_fresh(fid)
            return self.status
            
        scanned.file_path = str(abs_path).replace('\\', '/')
        parsed = await self.parser.parse_file(scanned)
        parsed.file_path = str(abs_path).replace('\\', '/')
        parsed.file_id = fid
        
        if parsed.file_id in self.symbol_graph.graph.files:
            self.symbol_graph.update_file(parsed)
        else:
            self.symbol_graph.graph.files[parsed.file_id] = parsed
            for sym in parsed.symbols:
                self.symbol_graph._add_symbol(sym)
                
        self.symbol_graph.resolve_cross_file_references_for_file(parsed.file_id)
        self.dep_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
        self.call_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
        
        deps = self.indexer.compute_file_dependencies(fid, self.symbol_graph.graph.files)
        self.indexer.register_generation(fid, deps)
        self.indexer.mark_file_fresh(fid)
        self.indexer.mark_rebuilt(fid)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("refresh_file", elapsed)
        log.info(f"File refreshed: {file_path} ({elapsed:.0f}ms)")
        return self.status

    def analyze_impact(
        self,
        changed_file: str,
        changed_symbols: Optional[List[str]] = None,
        max_depth: int = 5,
    ) -> ImpactReport:
        return self.impact_analyzer.analyze(
            changed_file=changed_file,
            changed_symbols=changed_symbols,
            symbol_graph=self.symbol_graph.graph,
            dep_graph=self.dep_graph_engine.graph,
            file_info=self.dep_graph_engine.file_info,
            max_depth=max_depth,
        )

    def get_architecture(self) -> Optional[ArchitectureMap]:
        return self._architecture

    def query(
        self,
        query_type: str,
        **kwargs,
    ) -> QueryResult:
        if query_type == 'symbol_definition':
            return self.query_engine.lookup_symbol_definition(
                name=kwargs.get('name', ''),
                symbol_graph=self.symbol_graph.graph,
                file_context=kwargs.get('file_context'),
            )
        elif query_type == 'references':
            return self.query_engine.lookup_references(
                symbol_id=kwargs.get('symbol_id', ''),
                symbol_graph=self.symbol_graph.graph,
            )
        elif query_type == 'dependencies':
            return self.query_engine.lookup_dependencies(
                file_id=kwargs.get('file_id', ''),
                dep_graph=self.dep_graph_engine.graph,
                file_info=self.dep_graph_engine.file_info,
                transitive=kwargs.get('transitive', False),
                depth=kwargs.get('depth', 1),
            )
        elif query_type == 'dependents':
            return self.query_engine.lookup_dependents(
                file_id=kwargs.get('file_id', ''),
                dep_graph=self.dep_graph_engine.graph,
                file_info=self.dep_graph_engine.file_info,
                transitive=kwargs.get('transitive', False),
                depth=kwargs.get('depth', 1),
            )
        elif query_type == 'test_coverage':
            return self.query_engine.lookup_test_coverage(
                file_id=kwargs.get('file_id', ''),
                symbol_graph=self.symbol_graph.graph,
                file_info=self.dep_graph_engine.file_info,
            )
        elif query_type == 'path':
            return self.query_engine.find_path(
                source_name=kwargs.get('source_name', ''),
                target_name=kwargs.get('target_name', ''),
                symbol_graph=self.symbol_graph.graph,
            )
        elif query_type == 'call_graph':
            return self.query_engine.query_call_graph(
                symbol_id=kwargs.get('symbol_id', ''),
                call_graph=self.call_graph_engine.graph,
                direction=kwargs.get('direction', 'outgoing'),
            )
        else:
            return QueryResult(
                data=None,
                confidence=ConfidenceLevel.APPROXIMATE,
                provenance=self.query_engine.make_provenance(
                    "unknown", self.symbol_graph.graph.version
                ),
            )

    def build_context(
        self,
        task_description: str,
        active_specialist: str,
    ) -> ContextPacket:
        return self.context_builder.build_context(
            task_description=task_description,
            active_specialist=active_specialist,
            symbol_graph=self.symbol_graph.graph,
            dep_graph=self.dep_graph_engine.graph,
            call_graph=self.call_graph_engine.graph,
            file_info=self.dep_graph_engine.file_info,
            architecture=self._architecture,
            stale_files=self.indexer.get_stale_files(),
        )
    
    def check_governance_for_modification(self, file_id: str, specialist: str, task_description: str = ""):
        """Check governance for a proposed modification"""
        if not self.governance_system:
            from repo_intelligence.types_extended import PermissionResult, ProtectionLevel
            return PermissionResult(
                permitted=True,
                protection_level=ProtectionLevel.NONE,
                requirements=[],
                approval_required=False,
                reason="Governance system not enabled"
            )
        
        from repo_intelligence.types_extended import ModificationContext
        context = ModificationContext(
            file_id=file_id,
            specialist=specialist,
            task_description=task_description,
            timestamp=datetime.now()
        )
        
        return self.governance_system.check_modification_permission(file_id, context)
    
    def record_breakage(self, component_id: str, breakage_type: str, context: str = ""):
        """Record a component breakage in the repository memory"""
        if not self.repository_memory:
            return
        
        from repo_intelligence.types_extended import ComponentBreakage
        breakage = ComponentBreakage(
            breakage_id=f"breakage_{component_id}_{int(time.time())}",
            component_id=component_id,
            breakage_type=breakage_type,
            context=context,
            timestamp=datetime.now()
        )
        self.repository_memory.register_breakage(breakage)
    
    def record_architectural_decision(self, title: str, context: str, decision: str, components_affected: list, decision_maker: str = "system"):
        """Record an architectural decision"""
        if not self.repository_memory:
            return
        
        from repo_intelligence.types_extended import ArchitecturalDecision
        arch_decision = ArchitecturalDecision(
            decision_id=f"decision_{hash(title)}_{int(time.time())}",
            title=title,
            context=context,
            decision=decision,
            consequences=[],
            components_affected=components_affected,
            timestamp=datetime.now(),
            decision_maker=decision_maker,
            status="active"
        )
        self.repository_memory.record_decision(arch_decision)
    
    def get_repository_hotspots(self):
        """Get repository modification hotspots"""
        if not self.repository_memory:
            return []
        
        return self.repository_memory.get_hotspots()
    
    def get_fragile_components(self):
        """Get fragile components that frequently cause issues"""
        if not self.repository_memory:
            return []
        
        return self.repository_memory.get_fragile_components()
    
    def get_active_risks(self, component: str = None):
        """Get active risks, optionally filtered by component"""
        if not self.repository_memory:
            return []
        
        return self.repository_memory.get_active_risks(component)
    
    def analyze_repository_health(self):
        """Perform comprehensive repository health analysis"""
        if not self._is_initialized:
            return None
        
        return self.health_system.analyze_health(self.symbol_graph.graph)
    
    def detect_architectural_drift(self):
        """Detect architectural drift and decay"""
        if not self._is_initialized:
            return None
        
        return self.drift_system.detect_drift(self.symbol_graph.graph)
    
    def get_complexity_metrics(self, symbol_id: str = None):
        """Get complexity metrics for a symbol or overall repository"""
        if not self._is_initialized:
            return None
        
        if symbol_id:
            return self.health_system.complexity_analyzer.compute_cyclomatic_complexity(symbol_id, self.symbol_graph.graph)
        else:
            # Return overall complexity summary
            total_complexity = 0
            count = 0
            for sid in self.symbol_graph.graph.symbols:
                symbol = self.symbol_graph.graph.symbols[sid]
                if symbol.symbol_kind.value in ["function", "method"]:
                    metrics = self.health_system.complexity_analyzer.compute_cyclomatic_complexity(sid, self.symbol_graph.graph)
                    total_complexity += metrics.cyclomatic_complexity
                    count += 1
            return {"overall_complexity": total_complexity / count if count > 0 else 0, "analyzed_functions": count}
    
    def get_duplication_analysis(self):
        """Get duplication analysis results"""
        if not self._is_initialized:
            return None
        
        exact_dupes = self.health_system.duplication_detector.detect_exact_duplicates(self.symbol_graph.graph)
        near_dupes = self.health_system.duplication_detector.detect_near_duplicates(self.symbol_graph.graph)
        arch_dupes = self.health_system.duplication_detector.detect_architectural_duplication(self.symbol_graph.graph)
        
        return {
            "exact_duplicates": len(exact_dupes),
            "near_duplicates": len(near_dupes),
            "architectural_duplications": len(arch_dupes)
        }
    
    def analyze_coupling_risk(self, symbol_id: str):
        """Analyze coupling risk for a component"""
        if not self._is_initialized:
            return None
        
        return self.risk_analyzer.analyze_coupling_risk(symbol_id, self.symbol_graph.graph)
    
    def analyze_refactor_risk(self, file_id: str):
        """Analyze refactor risk for a file"""
        if not self._is_initialized:
            return None
        
        return self.risk_analyzer.analyze_refactor_risk(file_id, self.symbol_graph.graph)
    
    def compute_stability_risk(self):
        """Compute overall repository stability risk"""
        if not self._is_initialized:
            return None
        
        return self.risk_analyzer.compute_stability_risk(self.symbol_graph.graph)
    
    def analyze_security_risk(self, symbol_id: str):
        """Analyze security risk for a component"""
        if not self._is_initialized:
            return None
        
        return self.risk_analyzer.analyze_security_risk(symbol_id, self.symbol_graph.graph)
    
    def compute_dependency_risk(self):
        """Compute dependency-related risks for the repository"""
        if not self._is_initialized:
            return None
        
        return self.risk_analyzer.compute_dependency_risk(self.symbol_graph.graph)
    
    def predict_bottlenecks(self):
        """Predict scaling, performance, and complexity bottlenecks"""
        if not self._is_initialized:
            return None
        
        return self.evolution_intelligence.predict_bottlenecks(self.symbol_graph.graph)
    
    def predict_scaling_issues(self):
        """Predict data, team, and dependency scaling issues"""
        if not self._is_initialized:
            return None
        
        return self.evolution_intelligence.predict_scaling_issues(self.symbol_graph.graph)
    
    def predict_maintenance_effort(self):
        """Predict maintenance effort for the repository"""
        if not self._is_initialized:
            return None
        
        return self.evolution_intelligence.predict_maintenance_effort(self.symbol_graph.graph)
    
    def predict_technical_debt(self):
        """Predict technical debt accumulation"""
        if not self._is_initialized:
            return None
        
        return self.evolution_intelligence.predict_technical_debt(self.symbol_graph.graph)
    
    def predict_dependency_growth(self):
        """Predict dependency growth and obsolescence"""
        if not self._is_initialized:
            return None
        
        return self.evolution_intelligence.predict_dependency_growth(self.symbol_graph.graph)
    
    def predict_obsolete_dependencies(self):
        """Predict obsolete dependencies"""
        if not self._is_initialized:
            return None
        
        return self.evolution_intelligence.predict_obsolete_dependencies(self.symbol_graph.graph)
    
    def generate_evolution_report(self):
        """Generate comprehensive evolution report"""
        if not self._is_initialized:
            return None
        
        return self.evolution_intelligence.generate_evolution_report(self.symbol_graph.graph)
    
    def infer_component_intent(self, symbol_id: str):
        """Infer architectural intent for a component"""
        if not self._is_initialized:
            return None
        
        return self.architecture_mapper.infer_component_intent(symbol_id, self.symbol_graph.graph)
    
    def detect_design_decisions(self, file_id: str):
        """Detect design decisions from code structure"""
        if not self._is_initialized:
            return None
        
        return self.architecture_mapper.detect_design_decisions(file_id, self.symbol_graph.graph)
    
    def get_ownership_info(self, symbol_id: str):
        """Get ownership information for a symbol"""
        if not self._is_initialized:
            return None
        
        return self.symbol_graph.infer_ownership(symbol_id)

    def get_symbol_graph(self) -> GraphSnapshot:
        return self.symbol_graph.graph

    def get_dep_graph(self) -> DependencyGraphSnapshot:
        return self.dep_graph_engine.graph

    def get_call_graph(self) -> CallGraphSnapshot:
        return self.call_graph_engine.graph

    def get_file_info(self) -> Dict[str, FileDependencyInfo]:
        return self.dep_graph_engine.file_info

    def save_state(self, path: str) -> None:
        self.symbol_graph.save_to_disk(path)

    def load_state(self, path: str) -> bool:
        loaded = self.symbol_graph.load_from_disk(path)
        if loaded:
            self.dep_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
            self.call_graph_engine.build_from_symbol_graph(self.symbol_graph.graph)
            self._architecture = self.architecture_mapper.build_map(
                self.symbol_graph.graph,
                self.dep_graph_engine.graph,
                self.dep_graph_engine.file_info,
            )
            self._is_initialized = True
        return loaded

    def get_all_metrics(self) -> Dict[str, List[PerformanceMetrics]]:
        return {
            'scanner': self.scanner.get_metrics(),
            'parser': self.parser.get_metrics(),
            'symbol_graph': self.symbol_graph.get_metrics(),
            'dep_graph': self.dep_graph_engine.get_metrics(),
            'call_graph': self.call_graph_engine.get_metrics(),
            'indexer': self.indexer.get_metrics(),
            'impact': self.impact_analyzer.get_metrics(),
            'architecture': self.architecture_mapper.get_metrics(),
            'query': self.query_engine.get_metrics(),
            'context': self.context_builder.get_metrics(),
            'engine': self.metrics,
            'runtime_inference': self.runtime_inference.metrics if hasattr(self.runtime_inference, 'metrics') else [],
            'health_system': self.health_system.get_metrics(),
            'drift_system': self.drift_system.get_metrics(),
            'risk_analyzer': self.risk_analyzer.get_metrics(),
            'evolution_intelligence': self.evolution_intelligence.get_metrics(),
            'repository_memory': self.repository_memory.get_metrics() if self.repository_memory else [],
            'governance_system': self.governance_system.get_metrics() if self.governance_system else [],
            'specialist_integrations': self.specialist_integrations.get_metrics(),
            'report_generator': self.report_generator.get_metrics(),
        }

    async def close(self) -> None:
        self.scanner.close()
        self.parser.close()
