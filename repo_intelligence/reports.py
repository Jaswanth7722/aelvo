# reports.py - Comprehensive Output Reports for Repository Intelligence
# Generates Architecture Reports, Change Reports, Refactor Reports, Repository Health Reports, and Evolution Reports

import time
import logging
from typing import Dict, List
from datetime import datetime
import json

from repo_intelligence.types import PerformanceMetrics
from repo_intelligence.types_extended import (
    HealthReport, RepositoryCognitionReport, RiskLevel
)

log = logging.getLogger("aelvo.repo_intelligence.reports")


class ReportGenerator:
    """Generates comprehensive repository intelligence reports"""
    
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
    
    def generate_architecture_report(self) -> Dict:
        """
        Generates a comprehensive architecture report.
        
        Returns:
            Dictionary containing architecture analysis
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return {"error": "Repository intelligence not initialized"}
        
        # Get current architecture
        architecture = self.engine._architecture
        
        # Get architectural drift
        drift_report = self.engine.detect_architectural_drift()
        
        # Get architectural violations
        violations = architecture.violations if architecture else []
        
        # Get module boundaries
        module_boundaries = architecture.module_boundaries if architecture else {}
        
        # Get ownership patterns
        ownership_patterns = self.engine.symbol_graph.ownership_patterns if hasattr(self.engine.symbol_graph, 'ownership_patterns') else {}
        
        report = {
            "report_type": "architecture",
            "timestamp": datetime.now().isoformat(),
            "current_architecture": {
                "layers": [{"name": layer.name, "file_count": len(layer.files)} for layer in (architecture.layers if architecture else [])],
                "entry_points": architecture.entry_points if architecture else [],
                "module_boundaries": module_boundaries,
                "ownership_patterns": {
                    owner_id: {
                        "owned_components": len(pattern.owned_components),
                        "ownership_type": pattern.ownership_type,
                        "responsibility_boundary": pattern.responsibility_boundary
                    }
                    for owner_id, pattern in ownership_patterns.items()
                }
            },
            "architectural_drift": {
                "overall_drift_score": drift_report.overall_drift_score if drift_report else 0.0,
                "functional_duplications": drift_report.functional_duplications if drift_report else 0,
                "competing_subsystems": drift_report.competing_subsystems if drift_report else 0,
                "unused_abstractions": drift_report.unused_abstractions if drift_report else 0,
                "architectural_violations": drift_report.architectural_violations if drift_report else 0
            },
            "violations": violations,
            "recommendations": self._generate_architecture_recommendations(drift_report, violations)
        }
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("generate_architecture_report", elapsed)
        
        return report
    
    def _generate_architecture_recommendations(self, drift_report, violations) -> List[str]:
        """Generate recommendations based on architecture analysis"""
        recommendations = []
        
        if drift_report and drift_report.overall_drift_score > 0.5:
            recommendations.append("High architectural drift detected - consider refactoring to align with intended architecture")
        
        if drift_report and drift_report.functional_duplications > 5:
            recommendations.append(f"{drift_report.functional_duplications} functional duplications found - consolidate duplicated implementations")
        
        if drift_report and drift_report.competing_subsystems > 2:
            recommendations.append(f"{drift_report.competing_subsystems} competing subsystems detected - resolve conflicts and clarify responsibilities")
        
        if violations:
            recommendations.append(f"{len(violations)} architectural violations detected - review and address dependency inversion issues")
        
        if not recommendations:
            recommendations.append("Architecture is healthy with no major issues detected")
        
        return recommendations
    
    def generate_change_report(self, proposed_files: List[str], task_description: str = "") -> Dict:
        """
        Generates a change impact and risk report.
        
        Args:
            proposed_files: List of files to be modified
            task_description: Description of the change task
            
        Returns:
            Dictionary containing change analysis
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return {"error": "Repository intelligence not initialized"}
        
        # Get modification guidance
        guidance = self.engine.specialist_integrations.provide_modification_guidance(proposed_files, task_description)
        
        # Analyze individual file risks
        file_risks = {}
        for file_id in proposed_files:
            risk_report = self.engine.analyze_refactor_risk(file_id)
            if risk_report:
                file_risks[file_id] = {
                    "risk_level": risk_report.risk_level.value,
                    "risk_score": risk_report.refactor_risk_score,
                    "dependency_count": risk_report.dependency_count,
                    "test_coverage": risk_report.test_coverage,
                    "mitigation_suggestions": risk_report.mitigation_suggestions
                }
        
        # Get governance checks
        governance_checks = guidance.governance_checks if guidance else {}
        
        # Get impact analysis
        impact_analysis = guidance.impact_analysis if guidance else {}
        
        report = {
            "report_type": "change",
            "timestamp": datetime.now().isoformat(),
            "task_description": task_description,
            "proposed_files": proposed_files,
            "governance_checks": governance_checks,
            "impact_analysis": impact_analysis,
            "file_risks": file_risks,
            "overall_risk_assessment": self._assess_overall_change_risk(file_risks, governance_checks),
            "recommendations": guidance.recommendations if guidance else []
        }
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("generate_change_report", elapsed)
        
        return report
    
    def _assess_overall_change_risk(self, file_risks: Dict, governance_checks: Dict) -> Dict:
        """Assess overall risk for the proposed change"""
        high_risk_count = sum(1 for r in file_risks.values() if r.get("risk_level") in ["HIGH", "CRITICAL"])
        blocked_count = sum(1 for c in governance_checks.values() if not c.get("permitted", True))
        
        if high_risk_count > 2 or blocked_count > 0:
            risk_level = "CRITICAL"
        elif high_risk_count > 0:
            risk_level = "HIGH"
        else:
            risk_level = "MEDIUM"
        
        return {
            "overall_risk_level": risk_level,
            "high_risk_files": high_risk_count,
            "blocked_files": blocked_count,
            "approved": blocked_count == 0
        }
    
    def generate_refactor_report(self, target_files: List[str]) -> Dict:
        """
        Generates a refactor planning and execution report.
        
        Args:
            target_files: List of files to be refactored
            
        Returns:
            Dictionary containing refactor analysis
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return {"error": "Repository intelligence not initialized"}
        
        # Get planning context
        planning_context = self.engine.specialist_integrations.provide_planning_context("Refactor operation")
        
        # Analyze each target file
        file_analysis = {}
        for file_id in target_files:
            risk_report = self.engine.analyze_refactor_risk(file_id)
            coupling_risk = self.engine.analyze_coupling_risk(file_id, self.engine.symbol_graph.graph) if file_id in self.engine.symbol_graph.graph.symbols else None
            
            file_analysis[file_id] = {
                "refactor_risk": risk_report.risk_level.value if risk_report else "unknown",
                "refactor_risk_score": risk_report.refactor_risk_score if risk_report else 0.0,
                "coupling_risk": coupling_risk.risk_level.value if coupling_risk else "unknown",
                "coupling_score": coupling_risk.coupling_score if coupling_risk else 0.0,
                "complexity_metrics": risk_report.complexity_metrics if risk_report else {},
                "mitigation_suggestions": risk_report.mitigation_suggestions if risk_report else []
            }
        
        # Determine safe refactor order
        implementation_order = planning_context.implementation_order if planning_context else None
        safe_order = [step.file_id for step in implementation_order.steps if step.file_id in target_files] if implementation_order else target_files
        
        # Get rollback plan
        rollback_plan = planning_context.rollback_plan if planning_context else None
        rollback_steps = [step.file_id for step in rollback_plan.steps if step.file_id in target_files] if rollback_plan else []
        
        report = {
            "report_type": "refactor",
            "timestamp": datetime.now().isoformat(),
            "target_files": target_files,
            "file_analysis": file_analysis,
            "safe_refactor_order": safe_order,
            "rollback_plan": {
                "rollback_order": rollback_steps,
                "rollback_targets": rollback_steps
            },
            "implementation_steps": len(safe_order),
            "estimated_risk": self._calculate_refactor_estimated_risk(file_analysis),
            "recommendations": self._generate_refactor_recommendations(file_analysis)
        }
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("generate_refactor_report", elapsed)
        
        return report
    
    def _calculate_refactor_estimated_risk(self, file_analysis: Dict) -> str:
        """Calculate estimated risk for refactor operation"""
        high_risk_count = sum(1 for fa in file_analysis.values() if fa.get("refactor_risk") in ["HIGH", "CRITICAL"])
        
        if high_risk_count > len(file_analysis) / 2:
            return "HIGH"
        elif high_risk_count > 0:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _generate_refactor_recommendations(self, file_analysis: Dict) -> List[str]:
        """Generate refactor recommendations"""
        recommendations = []
        
        for file_id, analysis in file_analysis.items():
            if analysis.get("refactor_risk") == "CRITICAL":
                recommendations.append(f"File {file_id} has CRITICAL refactor risk - consider alternative approaches")
            
            if analysis.get("coupling_score", 0) > 0.7:
                recommendations.append(f"File {file_id} has high coupling - reduce dependencies before refactoring")
            
            if analysis.get("mitigation_suggestions"):
                recommendations.extend([f"{file_id}: {s}" for s in analysis.get("mitigation_suggestions", [])])
        
        return recommendations
    
    def generate_repository_health_report(self) -> Dict:
        """
        Generates a comprehensive repository health report.
        
        Returns:
            Dictionary containing repository health analysis
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return {"error": "Repository intelligence not initialized"}
        
        # Get health report from health system
        health_report = self.engine.analyze_repository_health()
        
        # Get complexity metrics
        complexity_summary = self.engine.get_complexity_metrics()
        
        # Get duplication analysis
        duplication_analysis = self.engine.get_duplication_analysis()
        
        # Get fragile components
        fragile_components = self.engine.get_fragile_components()
        
        # Get active risks
        active_risks = self.engine.get_active_risks()
        
        # Get drift analysis
        drift_report = self.engine.detect_architectural_drift()
        
        report = {
            "report_type": "repository_health",
            "timestamp": datetime.now().isoformat(),
            "overall_health": {
                "overall_health_score": health_report.overall_health_score if health_report else 0.0,
                "complexity_score": health_report.complexity_score if health_report else 0.0,
                "coupling_score": health_report.coupling_score if health_report else 0.0,
                "cohesion_score": health_report.cohesion_score if health_report else 0.0,
                "duplication_score": health_report.duplication_score if health_report else 0.0,
                "maintainability_score": health_report.maintainability_score if health_report else 0.0,
                "test_coverage_score": health_report.test_coverage_score if health_report else 0.0
            },
            "complexity_analysis": complexity_summary,
            "duplication_analysis": duplication_analysis,
            "fragile_components": [
                {
                    "component_id": fc.component_id,
                    "breakage_count": fc.breakage_count,
                    "fragility_score": fc.fragility_score
                }
                for fc in fragile_components
            ],
            "active_risks": [
                {
                    "risk_id": r.risk_id,
                    "component": r.component,
                    "severity": r.severity.value,
                    "status": r.status.value
                }
                for r in active_risks
            ],
            "architectural_drift": {
                "overall_drift_score": drift_report.overall_drift_score if drift_report else 0.0,
                "functional_duplications": drift_report.functional_duplications if drift_report else 0,
                "competing_subsystems": drift_report.competing_subsystems if drift_report else 0,
                "unused_abstractions": drift_report.unused_abstractions if drift_report else 0
            },
            "health_recommendations": self._generate_health_recommendations(health_report, drift_report, fragile_components)
        }
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("generate_repository_health_report", elapsed)
        
        return report
    
    def _generate_health_recommendations(self, health_report, drift_report, fragile_components) -> List[str]:
        """Generate health recommendations"""
        recommendations = []
        
        if health_report:
            if health_report.overall_health_score < 0.5:
                recommendations.append("Overall repository health is below 50% - immediate attention required")
            
            if health_report.complexity_score < 0.6:
                recommendations.append("High complexity detected - consider simplification and refactoring")
            
            if health_report.coupling_score < 0.6:
                recommendations.append("High coupling detected - reduce dependencies and improve modularity")
            
            if health_report.test_coverage_score < 0.5:
                recommendations.append("Low test coverage - increase testing to improve maintainability")
        
        if fragile_components:
            recommendations.append(f"{len(fragile_components)} fragile components identified - prioritize stabilization")
        
        if drift_report and drift_report.overall_drift_score > 0.5:
            recommendations.append("Significant architectural drift detected - align implementation with intended architecture")
        
        if not recommendations:
            recommendations.append("Repository health is good - continue current practices")
        
        return recommendations
    
    def generate_evolution_report(self) -> Dict:
        """
        Generates a comprehensive evolution and future prediction report.
        
        Returns:
            Dictionary containing evolution intelligence
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return {"error": "Repository intelligence not initialized"}
        
        # Get evolution report from evolution intelligence
        evolution_report = self.engine.generate_evolution_report()
        
        # Get bottleneck predictions
        bottlenecks = self.engine.predict_bottlenecks()
        
        # Get scaling issues
        scaling_issues = self.engine.predict_scaling_issues()
        
        # Get maintenance effort prediction
        maintenance_effort = self.engine.predict_maintenance_effort()
        
        # Get technical debt prediction
        technical_debt = self.engine.predict_technical_debt()
        
        # Get dependency growth prediction
        dependency_growth = self.engine.predict_dependency_growth()
        
        # Get obsolete dependencies
        obsolete_deps = self.engine.predict_obsolete_dependencies()
        
        report = {
            "report_type": "evolution",
            "timestamp": datetime.now().isoformat(),
            "overall_evolution_risk": evolution_report.overall_evolution_risk if evolution_report else 0.0,
            "bottleneck_predictions": {
                "count": len(bottlenecks) if bottlenecks else 0,
                "bottlenecks": [
                    {
                        "id": b.bottleneck_id,
                        "type": b.bottleneck_type,
                        "severity": b.severity.value,
                        "impact": b.predicted_impact,
                        "timeframe_months": b.timeframe_months
                    }
                    for b in (bottlenecks or [])
                ]
            },
            "scaling_predictions": {
                "count": len(scaling_issues) if scaling_issues else 0,
                "issues": [
                    {
                        "component_id": s.component_id,
                        "issue_type": s.issue_type,
                        "severity": s.severity.value if hasattr(s, 'severity') else "MEDIUM",
                        "description": s.description,
                        "timeframe_months": s.timeframe_months
                    }
                    for s in (scaling_issues or [])
                ]
            },
            "maintenance_predictions": {
                "estimated_monthly_hours": maintenance_effort.estimated_monthly_hours if maintenance_effort else 0,
                "effort_per_component": maintenance_effort.effort_per_component if maintenance_effort else 0,
                "trend": maintenance_effort.trend if maintenance_effort else "stable"
            },
            "technical_debt_predictions": {
                "current_debt_score": technical_debt.current_debt_score if technical_debt else 0.0,
                "predicted_debt_score": technical_debt.predicted_debt_score if technical_debt else 0.0,
                "accumulation_rate": technical_debt.accumulation_rate if technical_debt else 0.0,
                "debt_items_count": len(technical_debt.debt_items) if technical_debt else 0
            },
            "dependency_predictions": {
                "current_count": dependency_growth.current_count if dependency_growth else 0,
                "predicted_6_months": dependency_growth.predicted_6_months if dependency_growth else 0,
                "predicted_12_months": dependency_growth.predicted_12_months if dependency_growth else 0,
                "growth_rate": dependency_growth.growth_rate if dependency_growth else 0.0
            },
            "obsolete_dependencies": {
                "count": obsolete_deps.obsolete_count if obsolete_deps else 0,
                "potentially_obsolete": [
                    {
                        "file_id": dep["file_id"],
                        "symbol_name": dep["symbol_name"],
                        "reason": dep["reason"]
                    }
                    for dep in (obsolete_deps.potentially_obsolete if obsolete_deps else [])
                ]
            },
            "evolution_recommendations": self._generate_evolution_recommendations(
                bottlenecks, scaling_issues, technical_debt, obsolete_deps
            )
        }
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("generate_evolution_report", elapsed)
        
        return report
    
    def _generate_evolution_recommendations(self, bottlenecks, scaling_issues, technical_debt, obsolete_deps) -> List[str]:
        """Generate evolution recommendations"""
        recommendations = []
        
        if bottlenecks and len([b for b in bottlenecks if b.severity == RiskLevel.CRITICAL]) > 0:
            recommendations.append("Critical bottlenecks predicted - address scalability concerns immediately")
        
        if scaling_issues and len(scaling_issues) > 5:
            recommendations.append("Multiple scaling issues predicted - plan for architectural improvements")
        
        if technical_debt and technical_debt.current_debt_score > 0.5:
            recommendations.append(f"High technical debt ({technical_debt.current_debt_score:.2f}) - allocate resources for debt reduction")
        
        if obsolete_deps and obsolete_deps.obsolete_count > 5:
            recommendations.append(f"{obsolete_deps.obsolete_count} potentially obsolete dependencies - review and remove unused code")
        
        if not recommendations:
            recommendations.append("Evolution trajectory is healthy - continue current practices")
        
        return recommendations
    
    def generate_repository_cognition_report(self) -> RepositoryCognitionReport:
        """
        Generates the ultimate repository cognition report combining all intelligence.
        
        Returns:
            RepositoryCognitionReport with comprehensive repository understanding
        """
        start = time.time()
        
        if not self.engine._is_initialized:
            return RepositoryCognitionReport(
                repository_id="unknown",
                timestamp=datetime.now(),
                architecture_report=None,
                health_report=None,
                drift_report=None,
                evolution_report=None,
                repository_reasoning_summary={}
            )
        
        # Generate all sub-reports
        architecture_report = self.generate_architecture_report()
        health_report_data = self.generate_repository_health_report()
        evolution_report_data = self.generate_evolution_report()
        
        # Convert to proper types
        HealthReport(
            repository_id=str(self.engine.workspace_root),
            overall_health_score=health_report_data["overall_health"]["overall_health_score"],
            complexity_score=health_report_data["overall_health"]["complexity_score"],
            coupling_score=health_report_data["overall_health"]["coupling_score"],
            cohesion_score=health_report_data["overall_health"]["cohesion_score"],
            duplication_score=health_report_data["overall_health"]["duplication_score"],
            maintainability_score=health_report_data["overall_health"]["maintainability_score"],
            test_coverage_score=health_report_data["overall_health"]["test_coverage_score"]
        )
        
        drift_report = self.engine.detect_architectural_drift()
        self.engine.generate_evolution_report()
        
        # Generate repository reasoning summary
        repository_reasoning_summary = {
            "repository_understanding": {
                "total_files": len(self.engine.symbol_graph.graph.files),
                "total_symbols": len(self.engine.symbol_graph.graph.symbols),
                "total_edges": len(self.engine.symbol_graph.graph.edges),
                "architectural_layers": len(architecture_report["current_architecture"]["layers"]),
                "module_boundaries": len(architecture_report["current_architecture"]["module_boundaries"]),
                "ownership_patterns": len(architecture_report["current_architecture"]["ownership_patterns"])
            },
            "cognitive_capabilities": {
                "architectural_intent_inference": "available",
                "change_impact_prediction": "available",
                "risk_analysis": "available",
                "architectural_drift_detection": "available",
                "repository_memory": "available" if self.engine.repository_memory else "disabled",
                "governance": "available" if self.engine.governance_system else "disabled",
                "health_monitoring": "available",
                "evolution_intelligence": "available"
            },
            "key_insights": [
                f"Repository has {health_report_data['overall_health']['overall_health_score']:.2%} overall health",
                f"Architecture drift score: {health_report_data['architectural_drift']['overall_drift_score']:.2%}",
                f"Evolution risk: {evolution_report_data['overall_evolution_risk']:.2%}",
                f"{len(health_report_data['fragile_components'])} fragile components identified",
                f"{health_report_data['active_risks']['count'] if isinstance(health_report_data['active_risks'], dict) else len(health_report_data['active_risks'])} active risks"
            ],
            "strategic_recommendations": self._generate_strategic_recommendations(
                architecture_report, health_report_data, evolution_report_data
            )
        }
        
        elapsed = (time.time() - start) * 1000
        self._record_metric("generate_repository_cognition_report", elapsed)
        
        return RepositoryCognitionReport(
            repository_id=str(self.engine.workspace_root),
            timestamp=datetime.now(),
            architecture_report=architecture_report,
            health_report=health_report_data,
            drift_report=drift_report,
            evolution_report=evolution_report_data,
            repository_reasoning_summary=repository_reasoning_summary
        )
    
    def _generate_strategic_recommendations(self, architecture_report, health_report, evolution_report) -> List[str]:
        """Generate strategic recommendations based on all reports"""
        recommendations = []
        
        # Architecture-based recommendations
        if architecture_report["architectural_drift"]["overall_drift_score"] > 0.5:
            recommendations.append("ARCHITECTURE: Address architectural drift to maintain system integrity")
        
        # Health-based recommendations
        if health_report["overall_health"]["overall_health_score"] < 0.6:
            recommendations.append("HEALTH: Improve repository health through refactoring and testing")
        
        # Evolution-based recommendations
        if evolution_report["overall_evolution_risk"] > 0.6:
            recommendations.append("EVOLUTION: Address predicted scaling and maintenance challenges")
        
        # Fragile components
        if health_report["fragile_components"]:
            recommendations.append(f"STABILITY: Stabilize {len(health_report['fragile_components'])} fragile components to improve reliability")
        
        # Bottlenecks
        if evolution_report["bottleneck_predictions"]["count"] > 3:
            recommendations.append(f"SCALABILITY: Address {evolution_report['bottleneck_predictions']['count']} predicted bottlenecks")
        
        # Technical debt
        if evolution_report["technical_debt_predictions"]["current_debt_score"] > 0.5:
            recommendations.append("DEBT: Allocate resources for technical debt reduction")
        
        if not recommendations:
            recommendations.append("Repository is in excellent condition - continue current practices")
        
        return recommendations
    
    def export_report(self, report: Dict, format: str = "json") -> str:
        """
        Exports a report in the specified format.
        
        Args:
            report: Report dictionary to export
            format: Export format ("json" or "text")
            
        Returns:
            Formatted report string
        """
        if format == "json":
            return json.dumps(report, indent=2, default=str)
        elif format == "text":
            return self._format_report_as_text(report)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _format_report_as_text(self, report: Dict) -> str:
        """Format report as human-readable text"""
        lines = []
        lines.append(f"=== {report.get('report_type', 'Repository Intelligence Report')} ===")
        lines.append(f"Generated: {report.get('timestamp', 'Unknown')}")
        lines.append("")
        
        for key, value in report.items():
            if key == "timestamp" or key == "report_type":
                continue
            lines.append(f"{key.replace('_', ' ').title()}:")
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    lines.append(f"  {subkey}: {subvalue}")
            elif isinstance(value, list):
                lines.append(f"  {len(value)} items")
            else:
                lines.append(f"  {value}")
            lines.append("")
        
        return "\n".join(lines)
    
    def get_metrics(self) -> List[PerformanceMetrics]:
        """Return all recorded metrics"""
        return self.metrics.copy()
