# types_extended.py - Extended Type System for Repository Intelligence Evolution
# Layer 0+: Complete type system for repository reasoning capabilities

from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel, Field
from datetime import datetime

# Import base types to avoid duplication
from repo_intelligence.types import (
    ConfidenceLevel, RiskLevel
)

# ===========================================================================
# Architectural Intent Types
# ===========================================================================

class ComponentIntent(BaseModel):
    component_id: str
    inferred_purpose: str
    architectural_role: str
    domain_responsibility: Optional[str] = None
    design_pattern: Optional[str] = None
    confidence: ConfidenceLevel = ConfidenceLevel.INFERRED
    evidence: List[str] = Field(default_factory=list)


class DesignDecision(BaseModel):
    decision_id: str
    component_id: str
    decision_type: str  # architectural, technical, framework
    rationale: str
    evidence: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)
    confidence: ConfidenceLevel = ConfidenceLevel.INFERRED
    decision_maker: str = "system"  # specialist or user or system


class OwnershipPattern(BaseModel):
    owner_id: str
    owned_components: List[str] = Field(default_factory=list)
    ownership_type: str  # module, layer, domain
    responsibility_boundary: str
    confidence: ConfidenceLevel = ConfidenceLevel.INFERRED


# ===========================================================================
# Predictive Impact Types
# ===========================================================================

class ProposedChange(BaseModel):
    change_id: str
    change_type: str  # add, modify, delete, refactor
    target_symbols: List[str] = Field(default_factory=list)
    target_files: List[str] = Field(default_factory=list)
    description: str
    estimated_complexity: int = 1  # 1-10 scale
    proposed_by: str = "unknown"
    timestamp: datetime = Field(default_factory=datetime.now)


class BlastRadiusAnalysis(BaseModel):
    direct_impact_count: int
    transitive_impact_count: int
    max_depth: int
    critical_components_affected: List[str] = Field(default_factory=list)
    risk_level: RiskLevel = RiskLevel.LOW
    confidence: ConfidenceLevel = ConfidenceLevel.INFERRED


class FailurePath(BaseModel):
    path_id: str
    components: List[str] = Field(default_factory=list)
    failure_mode: str
    probability: float = 0.0
    impact_severity: RiskLevel = RiskLevel.LOW
    description: str = ""


class PredictiveImpactReport(BaseModel):
    change_id: str
    predicted_affected_symbols: List[str] = Field(default_factory=list)
    predicted_affected_files: List[str] = Field(default_factory=list)
    predicted_affected_tests: List[str] = Field(default_factory=list)
    blast_radius: BlastRadiusAnalysis
    cascading_failures: List[FailurePath] = Field(default_factory=list)
    confidence: ConfidenceLevel = ConfidenceLevel.INFERRED
    prediction_timestamp: datetime = Field(default_factory=datetime.now)


# ===========================================================================
# Risk Analysis Types
# ===========================================================================

class CouplingRiskReport(BaseModel):
    component_id: str
    coupling_score: float  # 0-1
    incoming_coupling: int = 0
    outgoing_coupling: int = 0
    risk_level: RiskLevel = RiskLevel.LOW
    risk_factors: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


class RefactorRiskReport(BaseModel):
    file_id: str
    refactor_risk_score: float  # 0-1
    complexity_metrics: Optional[Dict[str, Any]] = None
    dependency_count: int = 0
    test_coverage: float = 0.0
    risk_level: RiskLevel = RiskLevel.LOW
    mitigation_suggestions: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


class StabilityRiskReport(BaseModel):
    repository_id: str
    overall_stability_score: float  # 0-1
    component_stability_scores: Dict[str, float] = Field(default_factory=dict)
    fragile_components: List[str] = Field(default_factory=list)
    risk_trend: str = "stable"  # improving, stable, degrading
    timestamp: datetime = Field(default_factory=datetime.now)


class SecurityRiskReport(BaseModel):
    symbol_id: str
    security_risk_score: float  # 0-1
    risk_categories: List[str] = Field(default_factory=list)
    sensitive_data_access: bool = False
    external_interaction: bool = False
    risk_level: RiskLevel = RiskLevel.LOW
    recommendations: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


class DependencyRiskReport(BaseModel):
    repository_id: str
    dependency_health_score: float  # 0-1
    outdated_dependencies: List[str] = Field(default_factory=list)
    vulnerable_dependencies: List[str] = Field(default_factory=list)
    unused_dependencies: List[str] = Field(default_factory=list)
    risk_level: RiskLevel = RiskLevel.LOW
    timestamp: datetime = Field(default_factory=datetime.now)


# ===========================================================================
# Knowledge Graph Types
# ===========================================================================

class OwnershipInfo(BaseModel):
    component_id: str
    owner: str
    ownership_confidence: float = 0.0
    ownership_evidence: List[str] = Field(default_factory=list)
    responsibility_boundaries: List[str] = Field(default_factory=list)


class ResponsibilityBoundary(BaseModel):
    boundary_id: str
    name: str
    components: List[str] = Field(default_factory=list)
    boundary_type: str  # module, layer, domain
    interface: List[str] = Field(default_factory=list)


class RuntimeDependency(BaseModel):
    source_id: str
    target_id: str
    dependency_type: str
    inference_confidence: float = 0.0
    evidence: List[str] = Field(default_factory=list)


class ExecutionPath(BaseModel):
    path_id: str
    entry_point: str
    components: List[str] = Field(default_factory=list)
    probability: float = 0.0
    path_type: str = "happy"  # happy, error, alternative


class LayerRelationship(BaseModel):
    source_layer: str
    target_layer: str
    relationship_type: str
    dependency_strength: float = 0.0
    violation_count: int = 0


class DataFlowPath(BaseModel):
    path_id: str
    data_source: str
    data_transformations: List[str] = Field(default_factory=list)
    data_sink: str
    confidence: float = 0.0


# ===========================================================================
# Repository Memory Types
# ===========================================================================

class TimeWindow(BaseModel):
    start: datetime
    end: datetime


class ModificationRecord(BaseModel):
    modification_id: str
    timestamp: datetime = Field(default_factory=datetime.now)
    modified_files: List[str] = Field(default_factory=list)
    modified_symbols: List[str] = Field(default_factory=list)
    modification_type: str
    specialist: str = "unknown"
    success: bool = True
    issues: List[str] = Field(default_factory=list)
    task_context: str = ""


class ModificationPattern(BaseModel):
    pattern_id: str
    description: str
    frequency: int = 0
    typical_components: List[str] = Field(default_factory=list)
    typical_outcomes: Dict[str, int] = Field(default_factory=dict)
    confidence: float = 0.0
    last_seen: datetime = Field(default_factory=datetime.now)


class Hotspot(BaseModel):
    component_id: str
    modification_frequency: int = 0
    time_window: Optional[TimeWindow] = None
    trend: str = "stable"  # increasing, stable, decreasing
    associated_breakages: int = 0
    last_modified: datetime = Field(default_factory=datetime.now)


class ComponentBreakage(BaseModel):
    breakage_id: str
    component_id: str
    timestamp: datetime = Field(default_factory=datetime.now)
    breakage_type: str
    context: str = ""
    modification_cause: Optional[str] = None
    severity: RiskLevel = RiskLevel.MEDIUM


class FragileComponent(BaseModel):
    component_id: str
    fragility_score: float  # 0-1
    breakage_count: int = 0
    breakage_types: List[str] = Field(default_factory=list)
    last_breakage: datetime = Field(default_factory=datetime.now)
    risk_level: RiskLevel = RiskLevel.LOW


class BreakagePattern(BaseModel):
    pattern_id: str
    description: str
    component_types: List[str] = Field(default_factory=list)
    common_causes: List[str] = Field(default_factory=list)
    frequency: int = 0
    confidence: float = 0.0


class ArchitecturalDecision(BaseModel):
    decision_id: str
    title: str
    context: str
    decision: str
    consequences: List[str] = Field(default_factory=list)
    components_affected: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)
    decision_maker: str = "unknown"
    status: str = "active"  # active, superseded, deprecated


class DecisionEvolution(BaseModel):
    component_id: str
    decisions: List[ArchitecturalDecision] = Field(default_factory=list)
    evolution_timeline: List[Tuple[datetime, str]] = Field(default_factory=list)
    current_state: str = ""
    evolution_pattern: str = ""


class QueryContext(BaseModel):
    query: str
    component_id: Optional[str] = None
    time_window: Optional[TimeWindow] = None
    specialist: Optional[str] = None


class KnownRisk(BaseModel):
    risk_id: str
    description: str
    risk_category: str
    components_affected: List[str] = Field(default_factory=list)
    severity: RiskLevel = RiskLevel.MEDIUM
    likelihood: float = 0.5
    mitigation_status: str = "open"
    mitigation_strategies: List[str] = Field(default_factory=list)
    created_timestamp: datetime = Field(default_factory=datetime.now)
    updated_timestamp: datetime = Field(default_factory=datetime.now)


class RiskStatus(str, Enum):
    OPEN = "open"
    MITIGATING = "mitigating"
    MITIGATED = "mitigated"
    ACCEPTED = "accepted"
    CLOSED = "closed"


# ===========================================================================
# Governance Types
# ===========================================================================

class ProtectionLevel(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class ProtectedModule(BaseModel):
    module_id: str
    protection_level: ProtectionLevel = ProtectionLevel.MEDIUM
    protection_reason: str = ""
    allowed_modifiers: List[str] = Field(default_factory=list)
    modification_requirements: List[str] = Field(default_factory=list)
    approval_required: bool = False


class PermissionResult(BaseModel):
    permitted: bool
    protection_level: ProtectionLevel = ProtectionLevel.NONE
    requirements: List[str] = Field(default_factory=list)
    approval_required: bool = False
    reason: str = ""


class ModificationContext(BaseModel):
    file_id: str
    specialist: str = "unknown"
    task_description: str = ""
    proposed_change: Optional[ProposedChange] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class CriticalityLevel(str, Enum):
    ESSENTIAL = "essential"
    IMPORTANT = "important"
    MODERATE = "moderate"
    LOW = "low"


class CriticalComponent(BaseModel):
    component_id: str
    criticality_level: CriticalityLevel = CriticalityLevel.MODERATE
    criticality_reason: str = ""
    dependencies_count: int = 0
    dependents_count: int = 0
    entry_point: bool = False


class CriticalityClassification(BaseModel):
    component_id: str
    classification: CriticalityLevel
    factors: List[str] = Field(default_factory=list)
    confidence: ConfidenceLevel = ConfidenceLevel.INFERRED


class SecuritySensitivityLevel(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class SecuritySensitiveComponent(BaseModel):
    component_id: str
    sensitivity_level: SecuritySensitivityLevel = SecuritySensitivityLevel.MEDIUM
    sensitivity_reasons: List[str] = Field(default_factory=list)
    data_types: List[str] = Field(default_factory=list)
    external_access: bool = False


class ProposedModification(BaseModel):
    modification_id: str
    file_id: str
    modification_type: str
    description: str
    specialist: str = "unknown"
    timestamp: datetime = Field(default_factory=datetime.now)


class PolicyViolation(BaseModel):
    violation_id: str
    policy_id: str
    policy_name: str
    severity: RiskLevel = RiskLevel.MEDIUM
    description: str = ""
    affected_components: List[str] = Field(default_factory=list)


class RiskAssessment(BaseModel):
    overall_risk: RiskLevel = RiskLevel.LOW
    risk_factors: List[str] = Field(default_factory=list)
    risk_scores: Dict[str, float] = Field(default_factory=dict)


class Mitigation(BaseModel):
    mitigation_id: str
    description: str
    effort: int = 1  # 1-10 scale
    effectiveness: float = 0.0


class GovernanceEvaluation(BaseModel):
    modification_id: str
    permitted: bool = True
    policy_violations: List[PolicyViolation] = Field(default_factory=list)
    risk_assessment: RiskAssessment = RiskAssessment()
    required_mitigations: List[Mitigation] = Field(default_factory=list)
    approval_required: bool = False
    timestamp: datetime = Field(default_factory=datetime.now)


# ===========================================================================
# Health Analysis Types
# ===========================================================================

class ComplexityMetrics(BaseModel):
    cyclomatic_complexity: int = 1
    cognitive_complexity: int = 1
    lines_of_code: int = 0
    parameter_count: int = 0
    nesting_depth: int = 0
    complexity_score: float = 0.0  # 0-1


class CognitiveComplexityMetrics(BaseModel):
    file_id: str
    overall_complexity: int = 1
    function_complexities: Dict[str, int] = Field(default_factory=dict)
    complexity_distribution: Dict[str, int] = Field(default_factory=dict)


class ComplexityTrendReport(BaseModel):
    repository_id: str
    time_series: List[Tuple[datetime, float]] = Field(default_factory=list)
    trend: str = "stable"  # increasing, stable, decreasing
    components_degrading: List[str] = Field(default_factory=list)
    components_improving: List[str] = Field(default_factory=list)


class CouplingMetrics(BaseModel):
    afferent_coupling: int = 0  # incoming dependencies
    efferent_coupling: int = 0  # outgoing dependencies
    instability: float = 0.0  # efferent / (afferent + efferent)
    coupling_score: float = 0.0  # 0-1


class CohesionMetrics(BaseModel):
    cohesion_level: float = 0.0  # 0-1
    related_functions: int = 0
    total_functions: int = 0
    cohesion_score: float = 0.0  # 0-1


class ArchitecturalCouplingReport(BaseModel):
    repository_id: str
    layer_coupling: Dict[str, Dict[str, float]] = Field(default_factory=dict)
    violations: List[str] = Field(default_factory=list)
    overall_coupling_score: float = 0.0  # 0-1


class ExactDuplicationGroup(BaseModel):
    group_id: str
    files: List[str] = Field(default_factory=list)
    lines: List[Tuple[str, int, int]] = Field(default_factory=list)  # (file, start, end)
    duplication_size: int = 0
    similarity: float = 1.0  # 1.0 for exact


class NearDuplicationGroup(BaseModel):
    group_id: str
    files: List[str] = Field(default_factory=list)
    similarity: float = 0.0
    differences: List[str] = Field(default_factory=list)
    refactoring_opportunity: bool = False


class ArchitecturalDuplication(BaseModel):
    duplication_id: str
    pattern_type: str
    instances: List[str] = Field(default_factory=list)
    convergence_opportunity: bool = False


class MaintainabilityIndex(BaseModel):
    file_id: str
    maintainability_index: float = 0.0  # 0-100
    complexity: float = 0.0
    volume: float = 0.0
    duplication: float = 0.0
    test_coverage: float = 0.0


class TechnicalDebtItem(BaseModel):
    item_id: str
    component_id: str
    debt_type: str
    severity: RiskLevel = RiskLevel.MEDIUM
    description: str = ""
    estimated_effort: int = 1  # hours


class TechnicalDebtReport(BaseModel):
    repository_id: str
    overall_debt_score: float = 0.0  # 0-1
    debt_categories: Dict[str, float] = Field(default_factory=dict)
    priority_debt_items: List[TechnicalDebtItem] = Field(default_factory=list)
    estimated_remediation_time: Optional[int] = None  # hours


class CodeChurnReport(BaseModel):
    repository_id: str
    time_window: TimeWindow
    total_lines_changed: int = 0
    files_changed: int = 0
    churn_rate: float = 0.0
    high_churn_files: List[str] = Field(default_factory=list)


class CoverageMetrics(BaseModel):
    file_id: str
    line_coverage: float = 0.0  # 0-100
    branch_coverage: float = 0.0  # 0-100
    function_coverage: float = 0.0  # 0-100
    overall_coverage: float = 0.0  # 0-100


class UntestedCriticalPath(BaseModel):
    path_id: str
    components: List[str] = Field(default_factory=list)
    criticality: CriticalityLevel = CriticalityLevel.MODERATE
    risk_exposure: float = 0.0


class TestEffectivenessReport(BaseModel):
    repository_id: str
    overall_effectiveness: float = 0.0  # 0-1
    flaky_tests: List[str] = Field(default_factory=list)
    slow_tests: List[str] = Field(default_factory=list)
    coverage_gaps: List[str] = Field(default_factory=list)


class PatternConsistencyReport(BaseModel):
    repository_id: str
    pattern_consistency_score: float = 0.0  # 0-1
    inconsistent_patterns: List[str] = Field(default_factory=list)
    pattern_drift_areas: List[str] = Field(default_factory=list)


class ArchitecturalViolation(BaseModel):
    violation_id: str
    violation_type: str
    component_id: str
    description: str = ""
    severity: RiskLevel = RiskLevel.MEDIUM


class NamingViolation(BaseModel):
    violation_id: str
    component_id: str
    expected_pattern: str
    actual_name: str
    suggestion: str = ""


class NamingConsistencyReport(BaseModel):
    repository_id: str
    overall_consistency: float = 0.0  # 0-1
    naming_violations: List[NamingViolation] = Field(default_factory=list)
    inconsistent_areas: List[str] = Field(default_factory=list)


# ===========================================================================
# Architectural Drift Types
# ===========================================================================

class FunctionalDuplication(BaseModel):
    duplication_id: str
    function_purpose: str
    implementations: List[str] = Field(default_factory=list)
    similarity_score: float = 0.0
    consolidation_opportunity: bool = False


class PatternDrift(BaseModel):
    drift_id: str
    intended_pattern: str
    drifted_components: List[str] = Field(default_factory=list)
    drift_severity: RiskLevel = RiskLevel.LOW


class CompetingSubsystem(BaseModel):
    subsystem_id: str
    competing_subsystems: List[str] = Field(default_factory=list)
    overlap_percentage: float = 0.0
    consolidation_potential: float = 0.0


class SubsystemOverlap(BaseModel):
    overlap_id: str
    subsystems: List[str] = Field(default_factory=list)
    overlapping_functionality: List[str] = Field(default_factory=list)
    overlap_area: str = ""


class UnusedAbstraction(BaseModel):
    abstraction_id: str
    abstraction_type: str
    defined_components: List[str] = Field(default_factory=list)
    usage_count: int = 0
    removal_safe: bool = False


class DeadArchitecture(BaseModel):
    architecture_id: str
    architecture_type: str
    components: List[str] = Field(default_factory=list)
    last_used: Optional[datetime] = None
    removal_candidate: bool = False


class SubsystemDivergence(BaseModel):
    divergence_id: str
    subsystems: List[str] = Field(default_factory=list)
    divergence_type: str
    divergence_degree: float = 0.0
    convergence_recommendation: str = ""


class ArchitecturalDecay(BaseModel):
    decay_id: str
    decay_type: str
    affected_area: str
    severity: RiskLevel = RiskLevel.LOW
    progression_rate: float = 0.0


# ===========================================================================
# Evolution Intelligence Types
# ===========================================================================

class ScalingBottleneck(BaseModel):
    bottleneck_id: str
    bottleneck_type: str
    affected_components: List[str] = Field(default_factory=list)
    current_capacity: float = 0.0
    predicted_limit: float = 0.0
    time_to_limit: Optional[int] = None  # months
    recommendations: List[str] = Field(default_factory=list)


class PerformanceBottleneck(BaseModel):
    bottleneck_id: str
    component_id: str
    bottleneck_type: str
    current_performance: float = 0.0
    predicted_degradation: float = 0.0
    optimization_opportunities: List[str] = Field(default_factory=list)


class ComplexityBottleneck(BaseModel):
    bottleneck_id: str
    area_id: str
    current_complexity: float = 0.0
    predicted_complexity: float = 0.0
    complexity_growth_rate: float = 0.0
    refactoring_recommendations: List[str] = Field(default_factory=list)


class DataScalingIssue(BaseModel):
    issue_id: str
    data_structure: str
    current_size: float = 0.0
    predicted_size: float = 0.0
    performance_impact: float = 0.0
    mitigation_strategies: List[str] = Field(default_factory=list)


class TeamScalingIssue(BaseModel):
    issue_id: str
    area_id: str
    current_team_size: int = 0
    optimal_team_size: int = 0
    coordination_overhead: float = 0.0
    knowledge_distribution: Dict[str, float] = Field(default_factory=dict)


class DependencyScalingIssue(BaseModel):
    issue_id: str
    component_id: str
    current_dependencies: int = 0
    predicted_dependencies: int = 0
    dependency_complexity: float = 0.0
    mitigation_needed: bool = False


class MaintenanceEffortPrediction(BaseModel):
    component_id: str
    predicted_effort: int = 0  # person-hours
    confidence: float = 0.0
    contributing_factors: List[str] = Field(default_factory=list)
    time_horizon: int = 6  # months


class TechnicalDebtPrediction(BaseModel):
    repository_id: str
    current_debt: float = 0.0
    predicted_debt: float = 0.0
    debt_accumulation_rate: float = 0.0
    time_to_critical: Optional[int] = None  # months
    intervention_points: List[str] = Field(default_factory=list)


class DependencyGrowthPrediction(BaseModel):
    repository_id: str
    current_dependency_count: int = 0
    predicted_dependency_count: int = 0
    growth_rate: float = 0.0
    time_horizon: int = 6  # months
    concerning_dependencies: List[str] = Field(default_factory=list)


class ObsoleteDependencyPrediction(BaseModel):
    dependency_id: str
    current_version: str
    predicted_obsolescence_date: Optional[datetime] = None
    deprecation_signs: List[str] = Field(default_factory=list)
    migration_path: Optional[str] = None


# ===========================================================================
# Integration Types
# ===========================================================================

class PlanningContext(BaseModel):
    task: str
    architectural_intent: Dict[str, ComponentIntent] = Field(default_factory=dict)
    impact_predictions: Dict[str, PredictiveImpactReport] = Field(default_factory=dict)
    risk_analysis: RiskAssessment = RiskAssessment()
    dependency_constraints: List[str] = Field(default_factory=list)
    implementation_recommendations: List[str] = Field(default_factory=list)


class ImplementationStep(BaseModel):
    step_id: int
    component_id: str
    description: str
    dependencies: List[int] = Field(default_factory=list)
    estimated_effort: int = 1  # hours
    risk_level: RiskLevel = RiskLevel.LOW


class ImplementationOrder(BaseModel):
    steps: List[ImplementationStep] = Field(default_factory=list)
    total_estimated_effort: int = 0  # hours
    parallel_opportunities: List[Tuple[int, int]] = Field(default_factory=list)
    critical_path: List[int] = Field(default_factory=list)


class RollbackStep(BaseModel):
    step_id: int
    component_id: str
    rollback_action: str
    dependencies: List[int] = Field(default_factory=list)
    verification_required: bool = True


class RollbackPlan(BaseModel):
    plan_id: str
    rollback_steps: List[RollbackStep] = Field(default_factory=list)
    rollback_order: List[int] = Field(default_factory=list)
    data_consistency_checks: List[str] = Field(default_factory=list)
    estimated_rollback_time: int = 0  # minutes


class ModificationGuidance(BaseModel):
    change_id: str
    affected_symbols: List[str] = Field(default_factory=list)
    affected_files: List[str] = Field(default_factory=list)
    dependency_chains: List[List[str]] = Field(default_factory=list)
    risk_assessment: RiskAssessment = RiskAssessment()
    governance_constraints: List[str] = Field(default_factory=list)
    suggested_order: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class ValidationResult(BaseModel):
    change_id: str
    valid: bool = True
    violations: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    required_modifications: List[str] = Field(default_factory=list)
    confidence: float = 0.0


class TrustBoundary(BaseModel):
    boundary_id: str
    boundary_type: str
    components: List[str] = Field(default_factory=list)
    trust_level: str = "medium"
    enforcement_points: List[str] = Field(default_factory=list)


class AttackSurfaceMap(BaseModel):
    entry_points: List[str] = Field(default_factory=list)
    data_flow: List[str] = Field(default_factory=list)
    external_interfaces: List[str] = Field(default_factory=list)
    sensitive_operations: List[str] = Field(default_factory=list)
    risk_areas: List[str] = Field(default_factory=list)


class SecurityContext(BaseModel):
    symbol_id: str
    security_sensitive_components: List[SecuritySensitiveComponent] = Field(default_factory=list)
    attack_surface: Optional[AttackSurfaceMap] = None
    trust_boundaries: List[TrustBoundary] = Field(default_factory=list)
    security_risk_assessment: Optional[SecurityRiskReport] = None


class ExecutionPathAnalysis(BaseModel):
    command: str
    affected_execution_paths: List[ExecutionPath] = Field(default_factory=list)
    runtime_dependencies: List[RuntimeDependency] = Field(default_factory=list)
    critical_infrastructure_affected: List[CriticalComponent] = Field(default_factory=list)
    rollback_targets: List[str] = Field(default_factory=list)


class DestructiveRiskAssessment(BaseModel):
    operation: str
    risk_level: RiskLevel = RiskLevel.LOW
    irreversible_changes: List[str] = Field(default_factory=list)
    recovery_options: List[str] = Field(default_factory=list)
    data_loss_risk: bool = False
    service_interruption_risk: bool = False


class RepositoryPreferences(BaseModel):
    coding_style: Dict[str, str] = Field(default_factory=dict)
    architectural_patterns: List[str] = Field(default_factory=list)
    naming_conventions: Dict[str, str] = Field(default_factory=dict)
    framework_preferences: List[str] = Field(default_factory=list)
    testing_preferences: Dict[str, str] = Field(default_factory=dict)


class CommunicationStyle(BaseModel):
    formality_level: str = "medium"
    technical_depth: int = 5  # 1-10
    verbosity: int = 5  # 1-10
    explanation_style: str = "balanced"
    example_preference: bool = True


class CommunicationContext(BaseModel):
    task_description: str
    specialist: str = "unknown"
    user_expertise: str = "medium"
    context_available: bool = True


class ArchitectureEvidence(BaseModel):
    query: str
    relevant_decisions: List[ArchitecturalDecision] = Field(default_factory=list)
    supporting_patterns: List[str] = Field(default_factory=list)
    conflicting_evidence: List[str] = Field(default_factory=list)
    confidence: float = 0.0
    reasoning: str = ""


class KnowledgeItem(BaseModel):
    item_id: str
    knowledge_type: str
    content: str
    source: str = "repository_intelligence"
    confidence: float = 0.0
    relevance_score: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.now)


class SearchResult(BaseModel):
    item_id: str
    title: str
    content: str
    relevance_score: float = 0.0
    source: str = "repository_intelligence"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Change(BaseModel):
    change_id: str
    file_id: str
    change_type: str
    description: str
    timestamp: datetime = Field(default_factory=datetime.now)


class ValidationScope(BaseModel):
    change: Change
    affected_validators: List[str] = Field(default_factory=list)
    validation_priority: List[str] = Field(default_factory=list)
    skip_validators: List[str] = Field(default_factory=list)
    custom_validation_rules: List[str] = Field(default_factory=list)


class TestScope(BaseModel):
    change: Change
    affected_tests: List[str] = Field(default_factory=list)
    critical_tests: List[str] = Field(default_factory=list)
    test_execution_order: List[str] = Field(default_factory=list)
    test_isolation_requirements: List[str] = Field(default_factory=list)


class RegressionRiskAssessment(BaseModel):
    change: Change
    overall_risk: RiskLevel = RiskLevel.LOW
    high_risk_areas: List[str] = Field(default_factory=list)
    probability_of_regression: float = 0.0
    recommended_test_coverage: float = 0.0


class Failure(BaseModel):
    failure_id: str
    component_id: str
    failure_type: str
    context: str = ""
    timestamp: datetime = Field(default_factory=datetime.now)


class RollbackTarget(BaseModel):
    target_id: str
    component_id: str
    rollback_type: str
    rollback_complexity: int = 1  # 1-10
    data_consistency_risk: RiskLevel = RiskLevel.LOW
    estimated_rollback_time: int = 0  # minutes


class RollbackPath(BaseModel):
    path_id: str
    rollback_sequence: List[RollbackTarget] = Field(default_factory=list)
    total_complexity: int = 0
    total_risk: RiskLevel = RiskLevel.LOW
    estimated_time: int = 0  # minutes


class RecoveryPriority(BaseModel):
    component_id: str
    priority: int = 0
    priority_reason: str = ""
    dependencies: List[str] = Field(default_factory=list)
    estimated_recovery_time: int = 0  # minutes


# ===========================================================================
# Utility Types
# ===========================================================================

class OwnershipChange(BaseModel):
    component_id: str
    old_owner: Optional[str] = None
    new_owner: str
    change_type: str  # assigned, transferred, released
    timestamp: datetime = Field(default_factory=datetime.now)


class HealthReport(BaseModel):
    repository_id: str
    overall_health_score: float = 0.0  # 0-1
    complexity_score: float = 0.0  # 0-1
    coupling_score: float = 0.0  # 0-1
    cohesion_score: float = 0.0  # 0-1
    duplication_score: float = 0.0  # 0-1 (lower is better)
    maintainability_score: float = 0.0  # 0-1
    test_coverage_score: float = 0.0  # 0-1
    timestamp: datetime = Field(default_factory=datetime.now)


class DriftReport(BaseModel):
    repository_id: str
    overall_drift_score: float = 0.0  # 0-1 (lower is better)
    functional_duplications: int = 0
    competing_subsystems: int = 0
    unused_abstractions: int = 0
    architectural_violations: int = 0
    timestamp: datetime = Field(default_factory=datetime.now)


class EvolutionReport(BaseModel):
    repository_id: str
    prediction_horizon: int = 6  # months
    predicted_bottlenecks: int = 0
    scaling_concerns: int = 0
    maintenance_cost_risk: float = 0.0  # 0-1
    dependency_growth_risk: float = 0.0  # 0-1
    timestamp: datetime = Field(default_factory=datetime.now)


class RepositoryCognitionReport(BaseModel):
    """Comprehensive report combining all repository intelligence aspects"""
    repository_id: str
    health: HealthReport
    drift: DriftReport
    evolution: EvolutionReport
    active_risks: List[KnownRisk] = Field(default_factory=list)
    governance_status: str = "compliant"
    architectural_intent_understanding: float = 0.0  # 0-1
    timestamp: datetime = Field(default_factory=datetime.now)
    
    def get_summary(self) -> str:
        """Generate a human-readable summary"""
        lines = [
            f"Repository Cognition Report for {self.repository_id}",
            f"Generated: {self.timestamp.isoformat()}",
            "",
            f"Health Score: {self.health.overall_health_score:.2f}",
            f"Drift Score: {self.drift.overall_drift_score:.2f}",
            f"Active Risks: {len(self.active_risks)}",
            f"Governance: {self.governance_status}",
            f"Intent Understanding: {self.architectural_intent_understanding:.2f}",
            "",
            "Key Health Metrics:",
            f"  Complexity: {self.health.complexity_score:.2f}",
            f"  Coupling: {self.health.coupling_score:.2f}",
            f"  Cohesion: {self.health.cohesion_score:.2f}",
            f"  Duplication: {self.health.duplication_score:.2f}",
            f"  Maintainability: {self.health.maintainability_score:.2f}",
            f"  Test Coverage: {self.health.test_coverage_score:.2f}",
            "",
            "Drift Indicators:",
            f"  Functional Duplications: {self.drift.functional_duplications}",
            f"  Competing Subsystems: {self.drift.competing_subsystems}",
            f"  Unused Abstractions: {self.drift.unused_abstractions}",
            f"  Architectural Violations: {self.drift.architectural_violations}",
            "",
            "Evolution Predictions:",
            f"  Predicted Bottlenecks: {self.evolution.predicted_bottlenecks}",
            f"  Scaling Concerns: {self.evolution.scaling_concerns}",
            f"  Maintenance Cost Risk: {self.evolution.maintenance_cost_risk:.2f}",
            f"  Dependency Growth Risk: {self.evolution.dependency_growth_risk:.2f}",
        ]
        return "\n".join(lines)


# Export all new types
__all__ = [
    # Architectural Intent
    'ComponentIntent', 'DesignDecision', 'OwnershipPattern',
    
    # Predictive Impact
    'ProposedChange', 'BlastRadiusAnalysis', 'FailurePath', 'PredictiveImpactReport',
    
    # Risk Analysis
    'CouplingRiskReport', 'RefactorRiskReport', 'StabilityRiskReport',
    'SecurityRiskReport', 'DependencyRiskReport',
    
    # Knowledge Graph
    'OwnershipInfo', 'ResponsibilityBoundary', 'RuntimeDependency',
    'ExecutionPath', 'LayerRelationship', 'DataFlowPath',
    
    # Repository Memory
    'TimeWindow', 'ModificationRecord', 'ModificationPattern', 'Hotspot',
    'ComponentBreakage', 'FragileComponent', 'BreakagePattern',
    'ArchitecturalDecision', 'DecisionEvolution', 'QueryContext',
    'KnownRisk', 'RiskStatus',
    
    # Governance
    'ProtectionLevel', 'ProtectedModule', 'PermissionResult', 'ModificationContext',
    'CriticalityLevel', 'CriticalComponent', 'CriticalityClassification',
    'SecuritySensitivityLevel', 'SecuritySensitiveComponent', 'ProposedModification',
    'PolicyViolation', 'RiskAssessment', 'Mitigation', 'GovernanceEvaluation',
    
    # Health Analysis
    'ComplexityMetrics', 'CognitiveComplexityMetrics', 'ComplexityTrendReport',
    'CouplingMetrics', 'CohesionMetrics', 'ArchitecturalCouplingReport',
    'ExactDuplicationGroup', 'NearDuplicationGroup', 'ArchitecturalDuplication',
    'MaintainabilityIndex', 'TechnicalDebtItem', 'TechnicalDebtReport',
    'CodeChurnReport', 'CoverageMetrics', 'UntestedCriticalPath',
    'TestEffectivenessReport', 'PatternConsistencyReport', 'ArchitecturalViolation',
    'NamingViolation', 'NamingConsistencyReport',
    
    # Drift Detection
    'FunctionalDuplication', 'PatternDrift', 'CompetingSubsystem', 'SubsystemOverlap',
    'UnusedAbstraction', 'DeadArchitecture', 'SubsystemDivergence', 'ArchitecturalDecay',
    
    # Evolution Intelligence
    'ScalingBottleneck', 'PerformanceBottleneck', 'ComplexityBottleneck',
    'DataScalingIssue', 'TeamScalingIssue', 'DependencyScalingIssue',
    'MaintenanceEffortPrediction', 'TechnicalDebtPrediction',
    'DependencyGrowthPrediction', 'ObsoleteDependencyPrediction',
    
    # Integration
    'PlanningContext', 'ImplementationStep', 'ImplementationOrder',
    'RollbackStep', 'RollbackPlan',
    'ModificationGuidance', 'ValidationResult',
    'TrustBoundary', 'AttackSurfaceMap', 'SecurityContext',
    'ExecutionPathAnalysis', 'DestructiveRiskAssessment',
    'RepositoryPreferences', 'CommunicationStyle', 'CommunicationContext',
    'ArchitectureEvidence', 'KnowledgeItem', 'SearchResult',
    'Change', 'ValidationScope', 'TestScope', 'RegressionRiskAssessment',
    'Failure', 'RollbackTarget', 'RollbackPath', 'RecoveryPriority',
    
    # Utility
    'OwnershipChange', 'HealthReport', 'DriftReport', 'EvolutionReport',
    'RepositoryCognitionReport',
]
