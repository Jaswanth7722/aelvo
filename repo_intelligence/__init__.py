# Repository Intelligence Engine for AELVO
# A codebase brain that understands code structure, relationships, and change impact

from repo_intelligence.types import (
    LanguageId, EdgeType, ConfidenceLevel, SymbolKind, RiskLevel, IndexStatus,
    SymbolId, FileId, ArgumentInfo,
    SymbolNode, SymbolEdge, ParsedFile, GraphSnapshot, FileScanResult,
    ImpactReport, ContextPacket, ArchitectureLayer, ArchitectureMap,
    CallGraphSnapshot, DependencyGraphSnapshot, FileDependencyInfo,
    PerformanceMetrics, QueryProvenance, QueryResult, GenerationRecord,
    IndexerState, SymbolMap, EdgeList, FileMap,
)
from repo_intelligence.types_extended import (
    # Architectural Intent
    ComponentIntent, DesignDecision, OwnershipPattern,
    # Predictive Impact
    ProposedChange, BlastRadiusAnalysis, FailurePath, PredictiveImpactReport,
    # Risk Analysis
    CouplingRiskReport, RefactorRiskReport, StabilityRiskReport,
    SecurityRiskReport, DependencyRiskReport,
    # Knowledge Graph
    OwnershipInfo, ResponsibilityBoundary, RuntimeDependency,
    ExecutionPath, LayerRelationship, DataFlowPath,
    # Repository Memory
    TimeWindow, ModificationRecord, ModificationPattern, Hotspot,
    ComponentBreakage, FragileComponent, BreakagePattern,
    ArchitecturalDecision, DecisionEvolution, QueryContext,
    KnownRisk, RiskStatus,
    # Governance
    ProtectionLevel, ProtectedModule, PermissionResult, ModificationContext,
    CriticalityLevel, CriticalComponent, CriticalityClassification,
    SecuritySensitivityLevel, SecuritySensitiveComponent, ProposedModification,
    PolicyViolation, RiskAssessment, Mitigation, GovernanceEvaluation,
    # Health Analysis
    ComplexityMetrics, CognitiveComplexityMetrics, ComplexityTrendReport,
    CouplingMetrics, CohesionMetrics, ArchitecturalCouplingReport,
    ExactDuplicationGroup, NearDuplicationGroup, ArchitecturalDuplication,
    MaintainabilityIndex, TechnicalDebtItem, TechnicalDebtReport,
    CodeChurnReport, CoverageMetrics, UntestedCriticalPath,
    TestEffectivenessReport, PatternConsistencyReport, ArchitecturalViolation,
    NamingViolation, NamingConsistencyReport,
    # Drift Detection
    FunctionalDuplication, PatternDrift, CompetingSubsystem, SubsystemOverlap,
    UnusedAbstraction, DeadArchitecture, SubsystemDivergence, ArchitecturalDecay,
    # Evolution Intelligence
    ScalingBottleneck, PerformanceBottleneck, ComplexityBottleneck,
    DataScalingIssue, TeamScalingIssue, DependencyScalingIssue,
    MaintenanceEffortPrediction, TechnicalDebtPrediction,
    DependencyGrowthPrediction, ObsoleteDependencyPrediction,
    # Integration
    PlanningContext, ImplementationStep, ImplementationOrder,
    RollbackStep, RollbackPlan,
    ModificationGuidance, ValidationResult,
    TrustBoundary, AttackSurfaceMap, SecurityContext,
    ExecutionPathAnalysis, DestructiveRiskAssessment,
    RepositoryPreferences, CommunicationStyle, CommunicationContext,
    ArchitectureEvidence, KnowledgeItem, SearchResult,
    Change, ValidationScope, TestScope, RegressionRiskAssessment,
    Failure, RollbackTarget, RollbackPath, RecoveryPriority,
    # Utility
    OwnershipChange, HealthReport, DriftReport, EvolutionReport,
    RepositoryCognitionReport,
)
from repo_intelligence.scanner import FileScanner
from repo_intelligence.parser import PythonASTParser, TypeScriptRegexParser, ASTParser
from repo_intelligence.graph import SymbolGraphEngine
from repo_intelligence.dep_graph import DependencyGraphEngine
from repo_intelligence.call_graph import CallGraphEngine
from repo_intelligence.indexer import IncrementalIndexer
from repo_intelligence.impact import ChangeImpactAnalyzer
from repo_intelligence.architecture import ArchitectureMapper
from repo_intelligence.query import QueryEngine
from repo_intelligence.context import ContextInjectionBuilder
from repo_intelligence.engine import RepoIntelligenceEngine
from repo_intelligence.runtime_inference import RuntimeRelationshipInference
from repo_intelligence.repository_memory import (
    RepositoryMemorySystem, HistoricalModificationTracker, FragileComponentRegistry,
    ArchitecturalDecisionRecorder, KnownRiskRegistry
)
from repo_intelligence.governance import (
    GovernanceSystem, ProtectedModuleRegistry, CriticalInfrastructureIdentifier,
    SecuritySensitiveTracker, GovernancePolicyEngine
)
from repo_intelligence.health_analysis import (
    HealthAnalysisSystem, ComplexityAnalyzer, CouplingCohesionAnalyzer,
    DuplicationDetector, MaintainabilityAnalyzer, TestCoverageAnalyzer,
    ArchitecturalConsistencyAnalyzer
)
from repo_intelligence.drift_detection import (
    DriftDetectionSystem, DuplicatedImplementationDetector, CompetingSubsystemDetector,
    UnusedAbstractionDetector, SubsystemDivergenceAnalyzer
)
from repo_intelligence.predictive_impact import PredictiveImpactAnalyzer
from repo_intelligence.risk_analysis import RepositoryRiskAnalyzer
from repo_intelligence.evolution_intelligence import RepositoryEvolutionIntelligence
from repo_intelligence.specialist_integrations import SpecialistIntegrations
from repo_intelligence.reports import ReportGenerator

__all__ = [
    # Original types
    'LanguageId', 'EdgeType', 'ConfidenceLevel', 'SymbolKind', 'RiskLevel', 'IndexStatus',
    'SymbolId', 'FileId', 'ArgumentInfo',
    'SymbolNode', 'SymbolEdge', 'ParsedFile', 'GraphSnapshot', 'FileScanResult',
    'ImpactReport', 'ContextPacket', 'ArchitectureLayer', 'ArchitectureMap',
    'CallGraphSnapshot', 'DependencyGraphSnapshot', 'FileDependencyInfo',
    'PerformanceMetrics', 'QueryProvenance', 'QueryResult', 'GenerationRecord',
    'IndexerState', 'SymbolMap', 'EdgeList', 'FileMap',
    # Extended types - Architectural Intent
    'ComponentIntent', 'DesignDecision', 'OwnershipPattern',
    # Extended types - Predictive Impact
    'ProposedChange', 'BlastRadiusAnalysis', 'FailurePath', 'PredictiveImpactReport',
    # Extended types - Risk Analysis
    'CouplingRiskReport', 'RefactorRiskReport', 'StabilityRiskReport',
    'SecurityRiskReport', 'DependencyRiskReport',
    # Extended types - Knowledge Graph
    'OwnershipInfo', 'ResponsibilityBoundary', 'RuntimeDependency',
    'ExecutionPath', 'LayerRelationship', 'DataFlowPath',
    # Extended types - Repository Memory
    'TimeWindow', 'ModificationRecord', 'ModificationPattern', 'Hotspot',
    'ComponentBreakage', 'FragileComponent', 'BreakagePattern',
    'ArchitecturalDecision', 'DecisionEvolution', 'QueryContext',
    'KnownRisk', 'RiskStatus',
    # Extended types - Governance
    'ProtectionLevel', 'ProtectedModule', 'PermissionResult', 'ModificationContext',
    'CriticalityLevel', 'CriticalComponent', 'CriticalityClassification',
    'SecuritySensitivityLevel', 'SecuritySensitiveComponent', 'ProposedModification',
    'PolicyViolation', 'RiskAssessment', 'Mitigation', 'GovernanceEvaluation',
    # Extended types - Health Analysis
    'ComplexityMetrics', 'CognitiveComplexityMetrics', 'ComplexityTrendReport',
    'CouplingMetrics', 'CohesionMetrics', 'ArchitecturalCouplingReport',
    'ExactDuplicationGroup', 'NearDuplicationGroup', 'ArchitecturalDuplication',
    'MaintainabilityIndex', 'TechnicalDebtItem', 'TechnicalDebtReport',
    'CodeChurnReport', 'CoverageMetrics', 'UntestedCriticalPath',
    'TestEffectivenessReport', 'PatternConsistencyReport', 'ArchitecturalViolation',
    'NamingViolation', 'NamingConsistencyReport',
    # Extended types - Drift Detection
    'FunctionalDuplication', 'PatternDrift', 'CompetingSubsystem', 'SubsystemOverlap',
    'UnusedAbstraction', 'DeadArchitecture', 'SubsystemDivergence', 'ArchitecturalDecay',
    # Extended types - Evolution Intelligence
    'ScalingBottleneck', 'PerformanceBottleneck', 'ComplexityBottleneck',
    'DataScalingIssue', 'TeamScalingIssue', 'DependencyScalingIssue',
    'MaintenanceEffortPrediction', 'TechnicalDebtPrediction',
    'DependencyGrowthPrediction', 'ObsoleteDependencyPrediction',
    # Extended types - Integration
    'PlanningContext', 'ImplementationStep', 'ImplementationOrder',
    'RollbackStep', 'RollbackPlan',
    'ModificationGuidance', 'ValidationResult',
    'TrustBoundary', 'AttackSurfaceMap', 'SecurityContext',
    'ExecutionPathAnalysis', 'DestructiveRiskAssessment',
    'RepositoryPreferences', 'CommunicationStyle', 'CommunicationContext',
    'ArchitectureEvidence', 'KnowledgeItem', 'SearchResult',
    'Change', 'ValidationScope', 'TestScope', 'RegressionRiskAssessment',
    'Failure', 'RollbackTarget', 'RollbackPath', 'RecoveryPriority',
    # Extended types - Utility
    'OwnershipChange', 'HealthReport', 'DriftReport', 'EvolutionReport',
    'RepositoryCognitionReport',
    # Original components
    'FileScanner', 'PythonASTParser', 'TypeScriptRegexParser', 'ASTParser',
    'SymbolGraphEngine', 'DependencyGraphEngine', 'CallGraphEngine',
    'IncrementalIndexer', 'ChangeImpactAnalyzer', 'ArchitectureMapper',
    'QueryEngine', 'ContextInjectionBuilder', 'RepoIntelligenceEngine',
    # Extended components
    'RuntimeRelationshipInference',
    'RepositoryRiskAnalyzer',
    'RepositoryEvolutionIntelligence',
    'SpecialistIntegrations',
    'ReportGenerator',
    # Repository Memory System
    'RepositoryMemorySystem', 'HistoricalModificationTracker', 'FragileComponentRegistry',
    'ArchitecturalDecisionRecorder', 'KnownRiskRegistry',
    # Repository Governance System
    'GovernanceSystem', 'ProtectedModuleRegistry', 'CriticalInfrastructureIdentifier',
    'SecuritySensitiveTracker', 'GovernancePolicyEngine',
    # Repository Health Analysis System
    'HealthAnalysisSystem', 'ComplexityAnalyzer', 'CouplingCohesionAnalyzer',
    'DuplicationDetector', 'MaintainabilityAnalyzer', 'TestCoverageAnalyzer',
    'ArchitecturalConsistencyAnalyzer',
    # Architectural Drift Detection System
    'DriftDetectionSystem', 'DuplicatedImplementationDetector', 'CompetingSubsystemDetector',
    'UnusedAbstractionDetector', 'SubsystemDivergenceAnalyzer',
    # Predictive Impact Analyzer
    'PredictiveImpactAnalyzer',
]
