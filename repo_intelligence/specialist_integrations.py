# specialist_integrations.py - Specialist and Runtime Integrations for Repository Intelligence
# Provides integration points for ARCHITECT, FORGE, SENTINEL, TERMINUS, HERMES, ORACLE
# and Verification/Recovery Runtimes

import time
import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime

from repo_intelligence.types import GraphSnapshot, EdgeType, PerformanceMetrics, ConfidenceLevel
from repo_intelligence.types_extended import (
    # ARCHITECT integration types
    PlanningContext, ImplementationStep, ImplementationOrder, RollbackStep, RollbackPlan,
    # FORGE integration types
    ModificationGuidance, ValidationResult,
    # SENTINEL integration types
    TrustBoundary, AttackSurfaceMap, SecurityContext,
    # TERMINUS integration types
    ExecutionPathAnalysis, DestructiveRiskAssessment,
    # HERMES integration types
    RepositoryPreferences, CommunicationStyle, CommunicationContext,
    # ORACLE integration types
    ArchitectureEvidence, KnowledgeItem, SearchResult,
    # Verification Runtime integration types
    Change, ValidationScope, TestScope, RegressionRiskAssessment,
    # Recovery Runtime integration types
    Failure, RollbackTarget, RollbackPath, RecoveryPriority,
    RiskLevel
)

log = logging.getLogger("aelvo.repo_intelligence.specialist_integrations")


class SpecialistIntegrations:
    """Provides integration methods for all AELVO specialists and runtimes"""
    
    def __init__(self, repo_intelligence_engine):
        """
        Initialize with a RepositoryIntelligenceEngine instance.
        
        Args:
            repo_intelligence_engine: RepoIntelligenceEngine instance
        """
        self.engine = repo_intelligence_engine
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    # ===========================================================================
    # ARCHITECT Specialist Integration
    # ===========================================================================
    
    def provide_planning_context(self, task_description: str) -> PlanningContext:
        """
        Provides planning context for ARCHITECT specialist.
        
        Args:
            task_description: Description of the planning task
            
        Returns:
            PlanningContext with repository-aware planning information
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return PlanningContext(
                task_id=f"plan_{int(time.time())}",
                task_description=task_description,
                repository_structure={},
                dependency_order=[],
                implementation_order=ImplementationOrder(steps=[]),
                risk_assessment={},
                rollout_sequence=[],
                rollback_plan=RollbackPlan(steps=[]),
                confidence=ConfidenceLevel.APPROXIMATE
            )
        
        # Get repository structure
        repository_structure = self._extract_repository_structure()
        
        # Determine dependency order
        dependency_order = self._determine_dependency_order()
        
        # Determine implementation order
        implementation_order = self._determine_implementation_order(task_description)
        
        # Risk assessment
        risk_assessment = self._assess_planning_risks(task_description)
        
        # Rollout sequence
        rollout_sequence = self._determine_rollout_sequence(task_description)
        
        # Rollback plan
        rollback_plan = self._create_rollback_plan(task_description)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_planning_context", elapsed)
        
        return PlanningContext(
            task_id=f"plan_{int(time.time())}",
            task_description=task_description,
            repository_structure=repository_structure,
            dependency_order=dependency_order,
            implementation_order=implementation_order,
            risk_assessment=risk_assessment,
            rollout_sequence=rollout_sequence,
            rollback_plan=rollback_plan,
            confidence=ConfidenceLevel.INFERRED
        )
    
    def _extract_repository_structure(self) -> Dict:
        """Extract repository structure for planning"""
        structure = {
            "layers": [layer.name for layer in self.engine._architecture.layers],
            "entry_points": self.engine._architecture.entry_points,
            "module_boundaries": self.engine._architecture.module_boundaries,
            "file_count": len(self.engine.symbol_graph.graph.files),
            "symbol_count": len(self.engine.symbol_graph.graph.symbols),
            "violations": self.engine._architecture.violations
        }
        return structure
    
    def _determine_dependency_order(self) -> List[str]:
        """Determine dependency order for implementation"""
        # Use topological sort based on dependency graph
        order = []
        visited = set()
        
        def visit(file_id):
            if file_id in visited:
                return
            visited.add(file_id)
            
            file_info = self.engine.dep_graph_engine.file_info.get(file_id)
            if file_info:
                for dep in file_info.imports:
                    visit(dep)
            order.append(file_id)
        
        for file_id in self.engine.dep_graph_engine.file_info:
            if file_id not in visited:
                visit(file_id)
        
        return order
    
    def _determine_implementation_order(self, task_description: str) -> ImplementationOrder:
        """Determine safe implementation order"""
        steps = []
        
        # Analyze task to identify components
        task_keywords = self._extract_task_keywords(task_description)
        
        # Find relevant files
        relevant_files = []
        for file_id, file in self.engine.symbol_graph.graph.files.items():
            if any(kw in file.file_path.lower() for kw in task_keywords):
                relevant_files.append(file_id)
        
        # Order by dependency
        dependency_order = self._determine_dependency_order()
        ordered_files = [f for f in dependency_order if f in relevant_files]
        
        # Create implementation steps
        for i, file_id in enumerate(ordered_files):
            file = self.engine.symbol_graph.graph.files.get(file_id)
            if file:
                step = ImplementationStep(
                    step_id=f"step_{i}",
                    file_id=file_id,
                    file_path=file.file_path,
                    dependencies=[],
                    estimated_risk=self._assess_step_risk(file_id),
                    validation_required=True
                )
                steps.append(step)
        
        return ImplementationOrder(steps=steps)
    
    def _extract_task_keywords(self, task_description: str) -> List[str]:
        """Extract relevant keywords from task description"""
        # Simple keyword extraction
        keywords = []
        common_terms = ["user", "auth", "api", "database", "model", "service", "controller"]
        task_lower = task_description.lower()
        for term in common_terms:
            if term in task_lower:
                keywords.append(term)
        return keywords
    
    def _assess_step_risk(self, file_id: str) -> RiskLevel:
        """Assess risk level for an implementation step"""
        risk_report = self.engine.analyze_refactor_risk(file_id)
        if risk_report:
            return risk_report.risk_level
        return RiskLevel.LOW
    
    def _assess_planning_risks(self, task_description: str) -> Dict:
        """Assess risks for the planning task"""
        risks = {}
        
        # Overall stability risk
        stability_risk = self.engine.compute_stability_risk()
        if stability_risk:
            risks["stability"] = stability_risk.overall_stability_score
        
        # Dependency risk
        dep_risk = self.engine.compute_dependency_risk()
        if dep_risk:
            risks["dependency"] = dep_risk.dependency_health_score
        
        return risks
    
    def _determine_rollout_sequence(self, task_description: str) -> List[str]:
        """Determine safe rollout sequence"""
        # For now, use reverse implementation order
        implementation_order = self._determine_implementation_order(task_description)
        return [step.file_id for step in reversed(implementation_order.steps)]
    
    def _create_rollback_plan(self, task_description: str) -> RollbackPlan:
        """Create rollback plan"""
        steps = []
        rollout_sequence = self._determine_rollout_sequence(task_description)
        
        for i, file_id in enumerate(rollout_sequence):
            file = self.engine.symbol_graph.graph.files.get(file_id)
            if file:
                step = RollbackStep(
                    step_id=f"rollback_{i}",
                    file_id=file_id,
                    file_path=file.file_path,
                    rollback_action="revert",
                    dependencies=[],
                    priority=i  # Earlier rollback steps have higher priority
                )
                steps.append(step)
        
        return RollbackPlan(steps=steps)
    
    # ===========================================================================
    # FORGE Specialist Integration
    # ===========================================================================
    
    def provide_modification_guidance(self, proposed_files: List[str], task_description: str) -> ModificationGuidance:
        """
        Provides modification guidance for FORGE specialist.
        
        Args:
            proposed_files: List of files to be modified
            task_description: Description of the modification task
            
        Returns:
            ModificationGuidance with modification constraints and recommendations
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return ModificationGuidance(
                modification_id=f"mod_{int(time.time())}",
                task_description=task_description,
                affected_files=proposed_files,
                affected_symbols=[],
                governance_checks={},
                impact_analysis=None,
                recommendations=[],
                validation_required=True
            )
        
        # Get affected symbols
        affected_symbols = self._get_affected_symbols(proposed_files)
        
        # Perform governance checks
        governance_checks = self._perform_governance_checks(proposed_files)
        
        # Perform impact analysis
        impact_analysis = self._perform_impact_analysis(proposed_files)
        
        # Generate recommendations
        recommendations = self._generate_modification_recommendations(proposed_files, governance_checks)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_modification_guidance", elapsed)
        
        return ModificationGuidance(
            modification_id=f"mod_{int(time.time())}",
            task_description=task_description,
            affected_files=proposed_files,
            affected_symbols=affected_symbols,
            governance_checks=governance_checks,
            impact_analysis=impact_analysis,
            recommendations=recommendations,
            validation_required=True
        )
    
    def _get_affected_symbols(self, file_ids: List[str]) -> List[str]:
        """Get symbols that would be affected by file modifications"""
        affected = []
        for file_id in file_ids:
            file = self.engine.symbol_graph.graph.files.get(file_id)
            if file:
                affected.extend([s.symbol_id for s in file.symbols])
        return affected
    
    def _perform_governance_checks(self, file_ids: List[str]) -> Dict:
        """Perform governance checks for proposed modifications"""
        checks = {}
        for file_id in file_ids:
            permission = self.engine.check_governance_for_modification(file_id, "FORGE", "")
            checks[file_id] = {
                "permitted": permission.permitted,
                "protection_level": permission.protection_level.value,
                "approval_required": permission.approval_required,
                "requirements": permission.requirements
            }
        return checks
    
    def _perform_impact_analysis(self, file_ids: List[str]) -> Dict:
        """Perform impact analysis for proposed modifications"""
        impact = {}
        for file_id in file_ids:
            # Get dependents
            dependents = []
            file_info = self.engine.dep_graph_engine.file_info.get(file_id)
            if file_info:
                dependents = file_info.imported_by
            
            # Assess risk
            risk_report = self.engine.analyze_refactor_risk(file_id)
            
            impact[file_id] = {
                "dependents": dependents,
                "risk_level": risk_report.risk_level.value if risk_report else "unknown",
                "risk_score": risk_report.refactor_risk_score if risk_report else 0.0
            }
        return impact
    
    def _generate_modification_recommendations(self, file_ids: List[str], governance_checks: Dict) -> List[str]:
        """Generate recommendations for proposed modifications"""
        recommendations = []
        
        for file_id in file_ids:
            check = governance_checks.get(file_id, {})
            if not check.get("permitted"):
                recommendations.append(f"File {file_id} is protected and requires approval")
            
            risk_report = self.engine.analyze_refactor_risk(file_id)
            if risk_report and risk_report.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
                recommendations.extend(risk_report.mitigation_suggestions)
        
        return recommendations
    
    # ===========================================================================
    # SENTINEL Specialist Integration
    # ===========================================================================
    
    def provide_security_context(self, component_id: Optional[str] = None) -> SecurityContext:
        """
        Provides security context for SENTINEL specialist.
        
        Args:
            component_id: Optional component ID to analyze
            
        Returns:
            SecurityContext with security-related repository information
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return SecurityContext(
                context_id=f"security_{int(time.time())}",
                security_sensitive_code=[],
                trust_boundaries=[],
                attack_surface_map=AttackSurfaceMap(
                    external_endpoints=[],
                    data_flow_paths=[],
                    authentication_points=[]
                ),
                security_risk_assessment={},
                recommendations=[]
            )
        
        # Identify security-sensitive code
        security_sensitive_code = []
        if self.engine.governance_system:
            security_sensitive_code = self.engine.governance_system.identify_security_sensitive_code(
                self.engine.symbol_graph.graph
            )
        
        # Identify trust boundaries
        trust_boundaries = self._identify_trust_boundaries()
        
        # Map attack surface
        attack_surface_map = self._map_attack_surface()
        
        # Assess security risks
        security_risk_assessment = {}
        if component_id:
            risk_report = self.engine.analyze_security_risk(component_id)
            if risk_report:
                security_risk_assessment[component_id] = {
                    "risk_score": risk_report.security_risk_score,
                    "risk_level": risk_report.risk_level.value,
                    "categories": risk_report.risk_categories
                }
        
        # Generate recommendations
        recommendations = self._generate_security_recommendations(security_sensitive_code)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_security_context", elapsed)
        
        return SecurityContext(
            context_id=f"security_{int(time.time())}",
            security_sensitive_code=[s.component_id for s in security_sensitive_code],
            trust_boundaries=trust_boundaries,
            attack_surface_map=attack_surface_map,
            security_risk_assessment=security_risk_assessment,
            recommendations=recommendations
        )
    
    def _identify_trust_boundaries(self) -> List[TrustBoundary]:
        """Identify trust boundaries in the codebase"""
        boundaries = []
        
        # Look for authentication/authorization modules
        auth_keywords = ["auth", "login", "permission", "access"]
        for symbol_id, symbol in self.engine.symbol_graph.graph.symbols.items():
            if any(kw in symbol.symbol_name.lower() for kw in auth_keywords):
                boundaries.append(TrustBoundary(
                    boundary_id=symbol_id,
                    boundary_name=symbol.symbol_name,
                    boundary_type="authentication",
                    components=[symbol_id],
                    trust_level="low"
                ))
        
        return boundaries
    
    def _map_attack_surface(self) -> AttackSurfaceMap:
        """Map the attack surface of the repository"""
        external_endpoints = []
        data_flow_paths = []
        authentication_points = []
        
        # Identify API endpoints
        for symbol_id, symbol in self.engine.symbol_graph.graph.symbols.items():
            symbol_name_lower = symbol.symbol_name.lower()
            if any(kw in symbol_name_lower for kw in ["route", "endpoint", "handler", "controller"]):
                external_endpoints.append(symbol_id)
            
            if any(kw in symbol_name_lower for kw in ["auth", "login", "authenticate"]):
                authentication_points.append(symbol_id)
        
        return AttackSurfaceMap(
            external_endpoints=external_endpoints,
            data_flow_paths=data_flow_paths,
            authentication_points=authentication_points
        )
    
    def _generate_security_recommendations(self, security_sensitive_code: List) -> List[str]:
        """Generate security recommendations"""
        recommendations = [
            "Review all security-sensitive code for potential vulnerabilities",
            "Implement proper input validation and sanitization",
            "Use secure authentication and authorization mechanisms",
            "Encrypt sensitive data at rest and in transit",
            "Implement proper error handling to avoid information leakage"
        ]
        
        if security_sensitive_code:
            recommendations.append(f"Pay special attention to {len(security_sensitive_code)} security-sensitive components")
        
        return recommendations
    
    # ===========================================================================
    # TERMINUS Specialist Integration
    # ===========================================================================
    
    def provide_execution_path_analysis(self, entry_point: str) -> ExecutionPathAnalysis:
        """
        Provides execution path analysis for TERMINUS specialist.
        
        Args:
            entry_point: Entry point symbol or file ID
            
        Returns:
            ExecutionPathAnalysis with execution path information
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return ExecutionPathAnalysis(
                analysis_id=f"exec_{int(time.time())}",
                entry_point=entry_point,
                execution_paths=[],
                critical_infrastructure=[],
                destructive_operations=[],
                rollback_targets=[]
            )
        
        # Detect execution paths
        execution_paths = self.engine.runtime_inference.detect_execution_paths(entry_point)
        
        # Identify critical infrastructure
        critical_infrastructure = []
        if self.engine.governance_system:
            critical_infrastructure = self.engine.governance_system.identify_critical_infrastructure(
                self.engine.symbol_graph.graph
            )
        
        # Identify destructive operations
        destructive_operations = self._identify_destructive_operations()
        
        # Identify rollback targets
        rollback_targets = self._identify_rollback_targets()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_execution_path_analysis", elapsed)
        
        return ExecutionPathAnalysis(
            analysis_id=f"exec_{int(time.time())}",
            entry_point=entry_point,
            execution_paths=[ep.path_id for ep in execution_paths],
            critical_infrastructure=[c.component_id for c in critical_infrastructure],
            destructive_operations=destructive_operations,
            rollback_targets=rollback_targets
        )
    
    def _identify_destructive_operations(self) -> List[str]:
        """Identify potentially destructive operations"""
        destructive = []
        destructive_keywords = ["delete", "remove", "drop", "truncate", "destroy", "wipe"]
        
        for symbol_id, symbol in self.engine.symbol_graph.graph.symbols.items():
            if any(kw in symbol.symbol_name.lower() for kw in destructive_keywords):
                destructive.append(symbol_id)
        
        return destructive
    
    def _identify_rollback_targets(self) -> List[str]:
        """Identify potential rollback targets"""
        # For now, return entry points and critical infrastructure
        targets = []
        
        # Add entry points
        targets.extend(self.engine._architecture.entry_points)
        
        # Add critical infrastructure if available
        if self.engine.governance_system:
            critical = self.engine.governance_system.identify_critical_infrastructure(
                self.engine.symbol_graph.graph
            )
            targets.extend([c.component_id for c in critical])
        
        return targets
    
    # ===========================================================================
    # HERMES Specialist Integration
    # ===========================================================================
    
    def provide_communication_context(self, task_type: str) -> CommunicationContext:
        """
        Provides communication context for HERMES specialist.
        
        Args:
            task_type: Type of task being performed
            
        Returns:
            CommunicationContext with repository communication preferences
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return CommunicationContext(
                context_id=f"comm_{int(time.time())}",
                task_type=task_type,
                repository_preferences=RepositoryPreferences(
                    verbosity="balanced",
                    detail_level="medium",
                    format_preference="structured",
                    language_style="professional"
                ),
                communication_style=CommunicationStyle(
                    tone="professional",
                    formality="formal",
                    technical_depth="balanced"
                ),
                domain_vocabulary={}
            )
        
        # Infer repository preferences from code structure
        repository_preferences = self._infer_repository_preferences()
        
        # Infer communication style
        communication_style = self._infer_communication_style()
        
        # Extract domain vocabulary
        domain_vocabulary = self._extract_domain_vocabulary()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_communication_context", elapsed)
        
        return CommunicationContext(
            context_id=f"comm_{int(time.time())}",
            task_type=task_type,
            repository_preferences=repository_preferences,
            communication_style=communication_style,
            domain_vocabulary=domain_vocabulary
        )
    
    def _infer_repository_preferences(self) -> RepositoryPreferences:
        """Infer repository preferences from code structure"""
        return RepositoryPreferences(
            verbosity="balanced",
            detail_level="medium",
            format_preference="structured",
            language_style="professional"
        )
    
    def _infer_communication_style(self) -> CommunicationStyle:
        """Infer communication style from repository patterns"""
        return CommunicationStyle(
            tone="professional",
            formality="formal",
            technical_depth="balanced"
        )
    
    def _extract_domain_vocabulary(self) -> Dict[str, str]:
        """Extract domain-specific vocabulary from the codebase"""
        vocabulary = {}
        
        # Extract common symbol names as vocabulary
        symbol_names = set()
        for symbol in self.engine.symbol_graph.graph.symbols.values():
            if symbol.symbol_kind.value in ["class", "function"]:
                symbol_names.add(symbol.symbol_name)
        
        # Create vocabulary mapping (simplified)
        for name in list(symbol_names)[:50]:  # Limit to 50 common terms
            vocabulary[name.lower()] = name
        
        return vocabulary
    
    # ===========================================================================
    # ORACLE Specialist Integration
    # ===========================================================================
    
    def retrieve_architecture_evidence(self, query: str) -> List[ArchitectureEvidence]:
        """
        Retrieves architecture evidence for ORACLE specialist.
        
        Args:
            query: Query to search for
            
        Returns:
            List of ArchitectureEvidence items
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return []
        
        # Search for relevant symbols
        relevant_symbols = self.engine.symbol_graph.graph.get_symbol_by_name(query)
        
        # Extract evidence from symbols
        evidence_list = []
        for symbol in relevant_symbols:
            # Get architectural intent
            intent = self.engine.infer_component_intent(symbol.symbol_id)
            
            evidence = ArchitectureEvidence(
                evidence_id=symbol.symbol_id,
                component_id=symbol.symbol_id,
                component_name=symbol.symbol_name,
                architectural_role=intent.architectural_role if intent else "unknown",
                evidence_type="symbol_definition",
                evidence_content=symbol.docstring or "",
                confidence=intent.confidence if intent else ConfidenceLevel.INFERRED,
                timestamp=datetime.now()
            )
            evidence_list.append(evidence)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("retrieve_architecture_evidence", elapsed)
        
        return evidence_list
    
    def search_knowledge(self, query: str) -> SearchResult:
        """
        Searches repository knowledge for ORACLE specialist.
        
        Args:
            query: Search query
            
        Returns:
            SearchResult with matching knowledge items
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return SearchResult(
                search_id=f"search_{int(time.time())}",
                query=query,
                results=[],
                total_count=0,
                search_time_ms=0
            )
        
        # Search for symbols matching query
        matching_symbols = self.engine.symbol_graph.graph.get_symbol_by_name(query)
        
        # Create knowledge items
        knowledge_items = []
        for symbol in matching_symbols:
            item = KnowledgeItem(
                item_id=symbol.symbol_id,
                item_type=symbol.symbol_kind.value,
                title=symbol.symbol_name,
                content=symbol.docstring or "",
                file_path=symbol.file_path,
                line_range=symbol.line_range,
                relevance_score=0.8  # Simplified relevance
            )
            knowledge_items.append(item)
        
        elapsed = (time.time() - start) * 1000
        
        return SearchResult(
            search_id=f"search_{int(time.time())}",
            query=query,
            results=knowledge_items,
            total_count=len(knowledge_items),
            search_time_ms=int(elapsed)
        )
    
    # ===========================================================================
    # Verification Runtime Integration
    # ===========================================================================
    
    def provide_validation_scope(self, proposed_changes: List[Change]) -> ValidationScope:
        """
        Provides validation scope for Verification Runtime.
        
        Args:
            proposed_changes: List of proposed changes
            
        Returns:
            ValidationScope with validation requirements
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return ValidationScope(
                scope_id=f"validation_{int(time.time())}",
                affected_files=[],
                affected_symbols=[],
                validation_rules=[],
                regression_risk=RegressionRiskAssessment(
                    risk_level=RiskLevel.LOW,
                    high_risk_components=[],
                    confidence=ConfidenceLevel.INFERRED
                )
            )
        
        # Get affected files and symbols
        affected_files = [c.file_id for c in proposed_changes if hasattr(c, 'file_id')]
        affected_symbols = self._get_affected_symbols(affected_files)
        
        # Determine validation rules
        validation_rules = self._determine_validation_rules(affected_files)
        
        # Assess regression risk
        regression_risk = self._assess_regression_risk(affected_files, affected_symbols)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_validation_scope", elapsed)
        
        return ValidationScope(
            scope_id=f"validation_{int(time.time())}",
            affected_files=affected_files,
            affected_symbols=affected_symbols,
            validation_rules=validation_rules,
            regression_risk=regression_risk
        )
    
    def _determine_validation_rules(self, file_ids: List[str]) -> List[str]:
        """Determine validation rules for affected files"""
        rules = [
            "All modified files must pass syntax validation",
            "No circular dependencies should be introduced",
            "All imports must be resolved",
            "Public API changes require documentation updates"
        ]
        
        # Add file-specific rules
        for file_id in file_ids:
            if "test" in file_id.lower():
                rules.append("Test files must pass all tests")
            if "api" in file_id.lower():
                rules.append("API changes require endpoint validation")
        
        return rules
    
    def _assess_regression_risk(self, file_ids: List[str], symbol_ids: List[str]) -> RegressionRiskAssessment:
        """Assess regression risk for proposed changes"""
        high_risk_components = []
        
        for file_id in file_ids:
            risk_report = self.engine.analyze_refactor_risk(file_id)
            if risk_report and risk_report.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
                high_risk_components.append(file_id)
        
        for symbol_id in symbol_ids:
            risk_report = self.engine.analyze_coupling_risk(symbol_id)
            if risk_report and risk_report.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
                high_risk_components.append(symbol_id)
        
        risk_level = RiskLevel.CRITICAL if len(high_risk_components) > 2 else (
            RiskLevel.HIGH if len(high_risk_components) > 0 else RiskLevel.LOW
        )
        
        return RegressionRiskAssessment(
            risk_level=risk_level,
            high_risk_components=high_risk_components,
            confidence=ConfidenceLevel.INFERRED
        )
    
    def provide_test_scope(self, proposed_changes: List[Change]) -> TestScope:
        """
        Provides test scope for Verification Runtime.
        
        Args:
            proposed_changes: List of proposed changes
            
        Returns:
            TestScope with testing requirements
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return TestScope(
                scope_id=f"test_{int(time.time())}",
                affected_tests=[],
                required_tests=[],
                coverage_gaps=[],
                test_priority=[]
            )
        
        # Get affected files
        affected_files = [c.file_id for c in proposed_changes if hasattr(c, 'file_id')]
        
        # Find affected tests
        affected_tests = self._find_affected_tests(affected_files)
        
        # Determine required tests
        required_tests = self._determine_required_tests(affected_files)
        
        # Identify coverage gaps
        coverage_gaps = self._identify_coverage_gaps(affected_files)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_test_scope", elapsed)
        
        return TestScope(
            scope_id=f"test_{int(time.time())}",
            affected_tests=affected_tests,
            required_tests=required_tests,
            coverage_gaps=coverage_gaps,
            test_priority=[]
        )
    
    def _find_affected_tests(self, file_ids: List[str]) -> List[str]:
        """Find tests affected by file changes"""
        affected_tests = []
        
        for file_id in file_ids:
            file = self.engine.symbol_graph.graph.files.get(file_id)
            if file:
                base_name = Path(file.file_path).stem
                for test_file_id, test_file in self.engine.symbol_graph.graph.files.items():
                    if "test" in test_file.file_path.lower():
                        test_base_name = Path(test_file.file_path).stem
                        if base_name in test_base_name or test_base_name in base_name:
                            affected_tests.append(test_file_id)
        
        return affected_tests
    
    def _determine_required_tests(self, file_ids: List[str]) -> List[str]:
        """Determine required tests for affected files"""
        required = []
        
        for file_id in file_ids:
            # If file has no corresponding test, recommend creating one
            if not self._find_affected_tests([file_id]):
                required.append(f"Create tests for {file_id}")
        
        return required
    
    def _identify_coverage_gaps(self, file_ids: List[str]) -> List[str]:
        """Identify coverage gaps in affected files"""
        gaps = []
        
        for file_id in file_ids:
            tests = self._find_affected_tests([file_id])
            if not tests:
                gaps.append(f"No test coverage for {file_id}")
        
        return gaps
    
    # ===========================================================================
    # Recovery Runtime Integration
    # ===========================================================================
    
    def provide_recovery_priority(self, failure: Failure) -> List[RecoveryPriority]:
        """
        Provides recovery priority for Recovery Runtime.
        
        Args:
            failure: Failure that occurred
            
        Returns:
            List of RecoveryPriority items
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return []
        
        # Identify affected components
        affected_components = self._identify_affected_components(failure)
        
        # Determine recovery priorities
        priorities = []
        for component_id in affected_components:
            # Check if component is critical
            if self.engine.governance_system:
                criticality = self.engine.governance_system.classify_criticality(
                    component_id, self.engine.symbol_graph.graph
                )
                is_critical = criticality.criticality_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]
            else:
                is_critical = False
            
            # Check if component is fragile
            fragile_components = self.engine.get_fragile_components()
            is_fragile = any(fc.component_id == component_id for fc in fragile_components)
            
            # Calculate priority
            priority_score = 0
            if is_critical:
                priority_score += 0.6
            if is_fragile:
                priority_score += 0.3
            priority_score += 0.1  # Base priority
            
            priorities.append(RecoveryPriority(
                component_id=component_id,
                priority_score=priority_score,
                recovery_action="restore" if is_critical else "investigate",
                estimated_recovery_time_ms=1000 if is_critical else 5000
            ))
        
        # Sort by priority score (descending)
        priorities.sort(key=lambda p: p.priority_score, reverse=True)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_recovery_priority", elapsed)
        
        return priorities
    
    def _identify_affected_components(self, failure: Failure) -> List[str]:
        """Identify components affected by a failure"""
        # For now, return the failed component and its dependents
        affected = []
        
        if hasattr(failure, 'component_id') and failure.component_id:
            affected.append(failure.component_id)
            
            # Get dependents
            symbol = self.engine.symbol_graph.graph.symbols.get(failure.component_id)
            if symbol:
                dependents = [e.source_id for e in self.engine.symbol_graph.graph.get_edges_to(failure.component_id)]
                affected.extend(dependents)
        
        return affected
    
    def provide_rollback_targets(self, failure: Failure) -> List[RollbackTarget]:
        """
        Provides rollback targets for Recovery Runtime.
        
        Args:
            failure: Failure that occurred
            
        Returns:
            List of RollbackTarget items
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return []
        
        # Identify rollback targets
        affected_components = self._identify_affected_components(failure)
        
        # Create rollback targets
        targets = []
        for component_id in affected_components:
            symbol = self.engine.symbol_graph.graph.symbols.get(component_id)
            if symbol:
                target = RollbackTarget(
                    target_id=component_id,
                    file_id=symbol.file_id,
                    rollback_action="revert",
                    dependencies=[e.target_id for e in self.engine.symbol_graph.graph.get_edges_from(component_id)],
                    rollback_order=0  # Will be calculated
                )
                targets.append(target)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("provide_rollback_targets", elapsed)
        
        return targets
    
    def get_metrics(self) -> List[PerformanceMetrics]:
        """Return all recorded metrics"""
        return self.metrics.copy()
