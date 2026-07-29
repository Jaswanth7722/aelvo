# planning/__init__.py - Long-Horizon Planning System for AELVO OMEGA
"""
Long-Horizon Planning (LHP) gives AELVO the ability to reason about the arc
of many sessions rather than just the present task.

Subsystems:
- memory_types: Pydantic schemas for strategic plan entries
- goal_hierarchy: CRUD for the six-level goal hierarchy (Mission → Task)
- multi_session: Session boundary write and restore operations
- plan_evolution: Four-trigger conservative revision engine
- debt_forecasting: Technical debt detection and projection
- self_critique: Continuous plan quality auditor
- integration: Orchestrator attachment at the three seam points
"""

from planning.memory_types import (
    StrategicPlanEntry,
    HierarchyLevel,
    PlanNodeState,
    RiskLevel,
    EvolutionTriggerType,
    RevisionRecord,
    VerificationStrategy,
    RiskAssessment,
    SessionBoundaryRecord,
    DebtForecastEntry,
    SelfCritiqueDefect,
    DefectType,
    MEMORY_TYPE_STRATEGIC_PLAN,
    MEMORY_TYPE_SESSION_BOUNDARY,
    MEMORY_TYPE_DEBT_FORECAST,
    MEMORY_TYPE_CRITIQUE_AUDIT,
    IMPORTANCE_STRATEGIC_PLAN,
    IMPORTANCE_SESSION_BOUNDARY,
)

from planning.goal_hierarchy import GoalHierarchyEngine
from planning.multi_session import MultiSessionPlanningEngine
from planning.plan_evolution import PlanEvolutionEngine, EvolutionTrigger
from planning.debt_forecasting import TechnicalDebtForecaster
from planning.self_critique import SelfCritiqueEngine
from planning.integration import LongHorizonPlanningIntegration
from planning.critique_evolution_pipeline import (
    SelfCritiqueEvolutionPipeline,
    PipelineResult,
    PipelineStatus,
    PipelineIteration,
    EvolutionAction,
    SeverityLevel,
)

__all__ = [
    # Memory types
    "StrategicPlanEntry",
    "HierarchyLevel",
    "PlanNodeState",
    "RiskLevel",
    "EvolutionTriggerType",
    "RevisionRecord",
    "VerificationStrategy",
    "RiskAssessment",
    "SessionBoundaryRecord",
    "DebtForecastEntry",
    "SelfCritiqueDefect",
    "DefectType",
    "MEMORY_TYPE_STRATEGIC_PLAN",
    "MEMORY_TYPE_SESSION_BOUNDARY",
    "MEMORY_TYPE_DEBT_FORECAST",
    "MEMORY_TYPE_CRITIQUE_AUDIT",
    "IMPORTANCE_STRATEGIC_PLAN",
    "IMPORTANCE_SESSION_BOUNDARY",
    # Engines
    "GoalHierarchyEngine",
    "MultiSessionPlanningEngine",
    "PlanEvolutionEngine",
    "EvolutionTrigger",
    "TechnicalDebtForecaster",
    "SelfCritiqueEngine",
    "LongHorizonPlanningIntegration",
    # Pipeline
    "SelfCritiqueEvolutionPipeline",
    "PipelineResult",
    "PipelineStatus",
    "PipelineIteration",
    "EvolutionAction",
    "SeverityLevel",
]
