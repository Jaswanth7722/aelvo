# governance.py - Repository Governance System for Repository Intelligence
# Layer 14: Enforces governance policies and protects critical infrastructure

import time
import logging
import sqlite3
import json
from typing import List
from datetime import datetime
from pathlib import Path

from repo_intelligence.types import GraphSnapshot, EdgeType, PerformanceMetrics, ConfidenceLevel
from repo_intelligence.types_extended import (
    ProtectedModule, PermissionResult, ModificationContext, ProtectionLevel,
    CriticalComponent, CriticalityLevel, CriticalityClassification,
    SecuritySensitiveComponent, SecuritySensitivityLevel, ProposedModification,
    PolicyViolation, RiskAssessment, Mitigation, GovernanceEvaluation
)

log = logging.getLogger("aelvo.repo_intelligence.governance")


class GovernanceSystem:
    """Main governance system that coordinates all governance components"""
    
    def __init__(self, db_path: str = "repository_governance.db"):
        """
        Initialize the governance system.
        
        Args:
            db_path: Path to SQLite database for persistent storage
        """
        self.db_path = db_path
        
        # Ensure parent directory exists before SQLite connect
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.protected_registry = ProtectedModuleRegistry(db_path)
        self.critical_identifier = CriticalInfrastructureIdentifier(db_path)
        self.security_tracker = SecuritySensitiveTracker(db_path)
        self.policy_engine = GovernancePolicyEngine(db_path)
        self.metrics: List[PerformanceMetrics] = []
        
        # Initialize database schema
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the database schema for all governance components"""
        self.protected_registry._initialize_schema()
        self.critical_identifier._initialize_schema()
        self.security_tracker._initialize_schema()
        self.policy_engine._initialize_schema()
        log.info(f"Governance system initialized at {self.db_path}")
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        """Record a performance metric"""
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    # Delegate methods to individual components
    def register_protected_module(self, module: ProtectedModule) -> None:
        """Register a protected module with governance policies"""
        self.protected_registry.register_protected_module(module)
    
    def check_modification_permission(self, file_id: str, context: ModificationContext) -> PermissionResult:
        """Check if modification is permitted"""
        return self.protected_registry.check_modification_permission(file_id, context)
    
    def get_protection_level(self, file_id: str) -> ProtectionLevel:
        """Returns the protection level for a file"""
        return self.protected_registry.get_protection_level(file_id)
    
    def identify_critical_infrastructure(self, symbol_graph: GraphSnapshot) -> List[CriticalComponent]:
        """Identify critical infrastructure components"""
        return self.critical_identifier.identify_critical_infrastructure(symbol_graph)
    
    def classify_criticality(self, symbol_id: str, symbol_graph: GraphSnapshot) -> CriticalityClassification:
        """Classify the criticality level of a component"""
        return self.critical_identifier.classify_criticality(symbol_id, symbol_graph)
    
    def identify_security_sensitive_code(self, symbol_graph: GraphSnapshot) -> List[SecuritySensitiveComponent]:
        """Identify security-sensitive code"""
        return self.security_tracker.identify_security_sensitive_code(symbol_graph)
    
    def classify_security_sensitivity(self, symbol_id: str, symbol_graph: GraphSnapshot) -> SecuritySensitivityLevel:
        """Classify security sensitivity level"""
        return self.security_tracker.classify_security_sensitivity(symbol_id, symbol_graph)
    
    def evaluate_modification(self, modification: ProposedModification) -> GovernanceEvaluation:
        """Evaluate a modification against governance policies"""
        return self.policy_engine.evaluate_modification(modification)
    
    def suggest_mitigations(self, violation: PolicyViolation) -> List[Mitigation]:
        """Suggest mitigations for policy violations"""
        return self.policy_engine.suggest_mitigations(violation)
    
    def get_metrics(self) -> List[PerformanceMetrics]:
        """Return all recorded metrics"""
        return self.metrics.copy()


class ProtectedModuleRegistry:
    """Manages protected modules and access policies"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for protected module tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS protected_modules (
                    module_id TEXT PRIMARY KEY,
                    protection_level TEXT,
                    protection_reason TEXT,
                    allowed_modifiers TEXT,
                    modification_requirements TEXT,
                    approval_required BOOLEAN
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_protected_level 
                ON protected_modules(protection_level)
            ''')
            conn.commit()
    
    def register_protected_module(self, module: ProtectedModule) -> None:
        """Register a protected module with governance policies"""
        start = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO protected_modules 
                (module_id, protection_level, protection_reason, 
                 allowed_modifiers, modification_requirements, approval_required)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                module.module_id,
                module.protection_level.value,
                module.protection_reason,
                json.dumps(module.allowed_modifiers),
                json.dumps(module.modification_requirements),
                module.approval_required
            ))
            conn.commit()
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("register_protected_module", elapsed)
    
    def check_modification_permission(self, file_id: str, context: ModificationContext) -> PermissionResult:
        """Check if modification is permitted"""
        start = time.time()
        
        # Check if file or its parent module is protected
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Try exact match first
            cursor.execute('''
                SELECT * FROM protected_modules WHERE module_id = ?
            ''', (file_id,))
            row = cursor.fetchone()
            
            # If not found, try parent directory matching
            if not row:
                path_parts = Path(file_id).parts
                for i in range(len(path_parts), 0, -1):
                    parent = '/'.join(path_parts[:i])
                    cursor.execute('''
                        SELECT * FROM protected_modules WHERE module_id = ?
                    ''', (parent,))
                    row = cursor.fetchone()
                    if row:
                        break
            
            if not row:
                # No protection found, permit by default
                result = PermissionResult(
                    permitted=True,
                    protection_level=ProtectionLevel.NONE,
                    requirements=[],
                    approval_required=False,
                    reason="No protection policy found"
                )
            else:
                # Parse protection data
                protection_level = ProtectionLevel(row[1])
                protection_reason = row[2]
                allowed_modifiers = json.loads(row[3])
                modification_requirements = json.loads(row[4])
                approval_required = row[5]
                
                # Check if specialist is allowed
                permitted = True
                reason = protection_reason
                
                if allowed_modifiers and context.specialist not in allowed_modifiers:
                    permitted = False
                    reason = f"Specialist {context.specialist} not in allowed modifiers"
                
                result = PermissionResult(
                    permitted=permitted,
                    protection_level=protection_level,
                    requirements=modification_requirements,
                    approval_required=approval_required or not permitted,
                    reason=reason
                )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("check_modification_permission", elapsed)
        
        return result
    
    def get_protection_level(self, file_id: str) -> ProtectionLevel:
        """Returns the protection level for a file"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT protection_level FROM protected_modules WHERE module_id = ?
            ''', (file_id,))
            row = cursor.fetchone()
            
            if row:
                return ProtectionLevel(row[0])
            
            # Try parent directory matching
            path_parts = Path(file_id).parts
            for i in range(len(path_parts), 0, -1):
                parent = '/'.join(path_parts[:i])
                cursor.execute('''
                    SELECT protection_level FROM protected_modules WHERE module_id = ?
                ''', (parent,))
                row = cursor.fetchone()
                if row:
                    return ProtectionLevel(row[0])
        
        return ProtectionLevel.NONE


class CriticalInfrastructureIdentifier:
    """Identifies and tracks critical infrastructure"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for critical infrastructure tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS critical_components (
                    component_id TEXT PRIMARY KEY,
                    criticality_level TEXT,
                    criticality_reason TEXT,
                    dependencies_count INTEGER,
                    dependents_count INTEGER,
                    entry_point BOOLEAN
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_critical_level 
                ON critical_components(criticality_level)
            ''')
            conn.commit()
    
    def identify_critical_infrastructure(self, symbol_graph: GraphSnapshot) -> List[CriticalComponent]:
        """Identify critical infrastructure components"""
        start = time.time()
        
        critical_components = []
        
        # Analyze each symbol/file in the graph
        for symbol_id, symbol in symbol_graph.symbols.items():
            classification = self.classify_criticality(symbol_id, symbol_graph)
            
            if classification.classification in [CriticalityLevel.ESSENTIAL, CriticalityLevel.IMPORTANT]:
                # Get dependency and dependent counts
                dependencies_count = len([
                    e for e in symbol_graph.get_edges_from(symbol_id)
                    if e.edge_type == EdgeType.IMPORTS
                ])
                dependents_count = len([
                    e for e in symbol_graph.get_edges_to(symbol_id)
                    if e.edge_type == EdgeType.IMPORTS
                ])
                
                # Check if it's an entry point
                is_entry_point = (dependents_count == 0 and dependencies_count > 0) or \
                                any(kw in symbol.symbol_name.lower() for kw in ["main", "run", "start"])
                
                component = CriticalComponent(
                    component_id=symbol_id,
                    criticality_level=classification.classification,
                    criticality_reason=classification.factors[0] if classification.factors else "High dependency impact",
                    dependencies_count=dependencies_count,
                    dependents_count=dependents_count,
                    entry_point=is_entry_point
                )
                critical_components.append(component)
                
                # Store in database
                self._store_critical_component(component)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("identify_critical_infrastructure", elapsed)
        
        return critical_components
    
    def _store_critical_component(self, component: CriticalComponent):
        """Store a critical component in the database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO critical_components 
                (component_id, criticality_level, criticality_reason, 
                 dependencies_count, dependents_count, entry_point)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                component.component_id,
                component.criticality_level.value,
                component.criticality_reason,
                component.dependencies_count,
                component.dependents_count,
                component.entry_point
            ))
            conn.commit()
    
    def classify_criticality(self, symbol_id: str, symbol_graph: GraphSnapshot) -> CriticalityClassification:
        """Classify the criticality level of a component"""
        symbol = symbol_graph.symbols.get(symbol_id)
        if not symbol:
            return CriticalityClassification(
                component_id=symbol_id,
                classification=CriticalityLevel.LOW,
                factors=["Component not found"],
                confidence=ConfidenceLevel.APPROXIMATE
            )
        
        factors = []
        criticality_score = 0
        
        # Check dependency count (incoming dependencies)
        incoming_deps = len([
            e for e in symbol_graph.get_edges_to(symbol_id)
            if e.edge_type == EdgeType.IMPORTS
        ])
        
        if incoming_deps > 20:
            criticality_score += 3
            factors.append(f"High dependency count: {incoming_deps}")
        elif incoming_deps > 10:
            criticality_score += 2
            factors.append(f"Moderate dependency count: {incoming_deps}")
        elif incoming_deps > 5:
            criticality_score += 1
            factors.append(f"Some dependencies: {incoming_deps}")
        
        # Check if it's an entry point
        if incoming_deps == 0 and len([
            e for e in symbol_graph.get_edges_from(symbol_id)
            if e.edge_type == EdgeType.IMPORTS
        ]) > 0:
            criticality_score += 2
            factors.append("Entry point component")
        
        # Check if exported
        if symbol.is_exported:
            criticality_score += 1
            factors.append("Public API component")
        
        # Check naming patterns for critical components
        symbol_name_lower = symbol.symbol_name.lower()
        critical_keywords = ["config", "auth", "security", "database", "storage", "core"]
        if any(kw in symbol_name_lower for kw in critical_keywords):
            criticality_score += 2
            factors.append("Critical naming pattern detected")
        
        # Determine classification
        if criticality_score >= 5:
            classification = CriticalityLevel.ESSENTIAL
        elif criticality_score >= 3:
            classification = CriticalityLevel.IMPORTANT
        elif criticality_score >= 1:
            classification = CriticalityLevel.MODERATE
        else:
            classification = CriticalityLevel.LOW
        
        confidence = ConfidenceLevel.CERTAIN if criticality_score >= 3 else ConfidenceLevel.INFERRED
        
        return CriticalityClassification(
            component_id=symbol_id,
            classification=classification,
            factors=factors,
            confidence=confidence
        )


class SecuritySensitiveTracker:
    """Tracks security-sensitive code"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for security-sensitive tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS security_sensitive_components (
                    component_id TEXT PRIMARY KEY,
                    sensitivity_level TEXT,
                    sensitivity_reasons TEXT,
                    data_types TEXT,
                    external_access BOOLEAN
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_security_level 
                ON security_sensitive_components(sensitivity_level)
            ''')
            conn.commit()
    
    def identify_security_sensitive_code(self, symbol_graph: GraphSnapshot) -> List[SecuritySensitiveComponent]:
        """Identify security-sensitive code"""
        start = time.time()
        
        security_components = []
        
        # Analyze each symbol in the graph
        for symbol_id, symbol in symbol_graph.symbols.items():
            classification = self.classify_security_sensitivity(symbol_id, symbol_graph)
            
            if classification != SecuritySensitivityLevel.LOW:
                # Analyze data types and external access
                data_types = self._identify_data_types(symbol)
                external_access = self._has_external_access(symbol_id, symbol_graph)
                
                component = SecuritySensitiveComponent(
                    component_id=symbol_id,
                    sensitivity_level=classification,
                    sensitivity_reasons=[f"Classification: {classification.value}"],
                    data_types=data_types,
                    external_access=external_access
                )
                security_components.append(component)
                
                # Store in database
                self._store_security_component(component)
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("identify_security_sensitive_code", elapsed)
        
        return security_components
    
    def _store_security_component(self, component: SecuritySensitiveComponent):
        """Store a security-sensitive component in the database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO security_sensitive_components 
                (component_id, sensitivity_level, sensitivity_reasons, 
                 data_types, external_access)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                component.component_id,
                component.sensitivity_level.value,
                json.dumps(component.sensitivity_reasons),
                json.dumps(component.data_types),
                component.external_access
            ))
            conn.commit()
    
    def classify_security_sensitivity(self, symbol_id: str, symbol_graph: GraphSnapshot) -> SecuritySensitivityLevel:
        """Classify security sensitivity level"""
        symbol = symbol_graph.symbols.get(symbol_id)
        if not symbol:
            return SecuritySensitivityLevel.LOW
        
        sensitivity_score = 0
        
        # Check naming patterns
        symbol_name_lower = symbol.symbol_name.lower()
        security_keywords = [
            "password", "secret", "key", "token", "auth", "login", "credential",
            "encrypt", "decrypt", "hash", "cipher", "crypto"
        ]
        
        if any(kw in symbol_name_lower for kw in security_keywords):
            sensitivity_score += 3
        
        # Check file path
        file_path_lower = symbol.file_path.lower()
        security_dirs = ["auth", "security", "crypto", "credential"]
        if any(dir in file_path_lower for dir in security_dirs):
            sensitivity_score += 2
        
        # Check docstring for security indicators
        if symbol.docstring:
            docstring_lower = symbol.docstring.lower()
            if any(kw in docstring_lower for kw in security_keywords):
                sensitivity_score += 2
        
        # Check if it handles external data
        if self._has_external_access(symbol_id, symbol_graph):
            sensitivity_score += 1
        
        # Determine classification
        if sensitivity_score >= 5:
            return SecuritySensitivityLevel.CRITICAL
        elif sensitivity_score >= 3:
            return SecuritySensitivityLevel.HIGH
        elif sensitivity_score >= 1:
            return SecuritySensitivityLevel.MEDIUM
        else:
            return SecuritySensitivityLevel.LOW
    
    def _identify_data_types(self, symbol) -> List[str]:
        """Identify data types handled by a symbol"""
        data_types = []
        
        # Check argument names for data type hints
        for arg in symbol.arguments:
            arg_name_lower = arg.name.lower()
            if any(kw in arg_name_lower for kw in ["password", "token", "key", "secret"]):
                data_types.append(arg.name)
        
        # Check type annotations
        if symbol.type_annotation:
            type_lower = symbol.type_annotation.lower()
            if "str" in type_lower and any(kw in symbol.symbol_name.lower() for kw in ["password", "key", "token"]):
                data_types.append("sensitive_string")
        
        return data_types
    
    def _has_external_access(self, symbol_id: str, symbol_graph: GraphSnapshot) -> bool:
        """Check if symbol has external access"""
        symbol = symbol_graph.symbols.get(symbol_id)
        if not symbol:
            return False
        
        # Check for external imports
        for edge in symbol_graph.get_edges_from(symbol_id):
            if edge.edge_type == EdgeType.IMPORTS:
                target = symbol_graph.symbols.get(edge.target_id)
                if target and "external" in target.file_path.lower():
                    return True
        
        # Check docstring for external access indicators
        if symbol.docstring:
            docstring_lower = symbol.docstring.lower()
            external_keywords = ["http", "api", "network", "socket", "request"]
            if any(kw in docstring_lower for kw in external_keywords):
                return True
        
        return False


class GovernancePolicyEngine:
    """Enforces governance policies"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.metrics: List[PerformanceMetrics] = []
    
    def _record_metric(self, operation: str, duration_ms: float) -> None:
        self.metrics.append(PerformanceMetrics(
            operation=operation, duration_ms=duration_ms
        ))
    
    def _initialize_schema(self):
        """Initialize the database schema for governance policy tracking"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS governance_policies (
                    policy_id TEXT PRIMARY KEY,
                    policy_name TEXT,
                    policy_type TEXT,
                    description TEXT,
                    enforcement_level TEXT,
                    active BOOLEAN
                )
            ''')
            conn.commit()
    
    def evaluate_modification(self, modification: ProposedModification) -> GovernanceEvaluation:
        """Evaluate a modification against governance policies"""
        start = time.time()
        
        # This is a simplified evaluation - in practice, would check against actual policies
        violations = []
        risk_factors = []
        
        # Check if modification affects critical infrastructure
        # (In practice, would query the CriticalInfrastructureIdentifier)
        if "critical" in modification.file_id.lower():
            violation = PolicyViolation(
                violation_id=f"crit_inf_{modification.modification_id}",
                policy_id="critical_infrastructure_protection",
                policy_name="Critical Infrastructure Protection",
                severity="high",
                description="Modification affects critical infrastructure",
                affected_components=[modification.file_id]
            )
            violations.append(violation)
            risk_factors.append("Critical infrastructure modification")
        
        # Check if modification is potentially destructive
        if modification.modification_type in ["delete", "remove"]:
            violation = PolicyViolation(
                violation_id=f"destructive_{modification.modification_id}",
                policy_id="destructive_operation_control",
                policy_name="Destructive Operation Control",
                severity="medium",
                description="Destructive modification requires additional approval",
                affected_components=[modification.file_id]
            )
            violations.append(violation)
            risk_factors.append("Destructive operation")
        
        # Calculate overall risk
        overall_risk = "low"
        if len(violations) > 0:
            if any(v.severity == "high" for v in violations):
                overall_risk = "critical"
            elif any(v.severity == "medium" for v in violations):
                overall_risk = "medium"
            else:
                overall_risk = "low"
        
        # Determine if permitted
        permitted = len(violations) == 0 or overall_risk == "low"
        approval_required = not permitted or overall_risk in ["medium", "critical"]
        
        # Generate required mitigations
        required_mitigations = []
        for violation in violations:
            mitigations = self.suggest_mitigations(violation)
            required_mitigations.extend(mitigations)
        
        risk_assessment = RiskAssessment(
            overall_risk=overall_risk,
            risk_factors=risk_factors,
            risk_scores={"governance": len(violations) * 0.2}
        )
        
        evaluation = GovernanceEvaluation(
            modification_id=modification.modification_id,
            permitted=permitted,
            policy_violations=violations,
            risk_assessment=risk_assessment,
            required_mitigations=required_mitigations,
            approval_required=approval_required,
            timestamp=datetime.now()
        )
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("evaluate_modification", elapsed)
        
        return evaluation
    
    def suggest_mitigations(self, violation: PolicyViolation) -> List[Mitigation]:
        """Suggest mitigations for policy violations"""
        mitigations = []
        
        # Generate mitigations based on violation type
        if "critical" in violation.policy_name.lower():
            mitigations.append(Mitigation(
                mitigation_id=f"mit_{violation.violation_id}_1",
                description="Obtain explicit approval from system architect",
                effort=8,
                effectiveness=0.9
            ))
            mitigations.append(Mitigation(
                mitigation_id=f"mit_{violation.violation_id}_2",
                description="Create comprehensive backup before modification",
                effort=5,
                effectiveness=0.8
            ))
        elif "destructive" in violation.policy_name.lower():
            mitigations.append(Mitigation(
                mitigation_id=f"mit_{violation.violation_id}_1",
                description="Verify no active dependencies on affected components",
                effort=3,
                effectiveness=0.7
            ))
            mitigations.append(Mitigation(
                mitigation_id=f"mit_{violation.violation_id}_2",
                description="Schedule maintenance window for the change",
                effort=2,
                effectiveness=0.6
            ))
        else:
            mitigations.append(Mitigation(
                mitigation_id=f"mit_{violation.violation_id}_1",
                description="Review change with team lead",
                effort=2,
                effectiveness=0.5
            ))
        
        return mitigations
