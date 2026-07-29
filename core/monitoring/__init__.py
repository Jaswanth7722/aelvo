"""core/monitoring — System Monitoring Dashboard

Phase 16: Live dashboard aggregating health from SystemHealthMonitor,
execution stats from ToolExecutionRegistry, session activity from
PersistentSandboxSession, and learning insights from ExperienceLearningPipeline
into a unified terminal display.
"""

from core.monitoring.system_dashboard import (
    SystemDashboard,
    DashboardDataSource,
    DashboardSection,
)

__all__ = [
    "SystemDashboard",
    "DashboardDataSource",
    "DashboardSection",
]
