"""core/monitoring/system_dashboard.py — System Monitoring Dashboard

Phase 16: Live dashboard aggregating health from SystemHealthMonitor,
execution stats from ToolExecutionRegistry, session activity from
PersistentSandboxSession, and learning insights from ExperienceLearningPipeline
into a unified terminal display.

Key components:
  - DashboardDataSource: Config holder for all subsystem references
  - DashboardSection: Enum identifying each dashboard section
  - SystemDashboard: Polls all subsystems and produces unified display
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.health.system_health_monitor import (
    SystemHealthMonitor,
    SystemHealthReport,
    ComponentStatus,
)
from core.execution.tool_registry import ToolExecutionRegistry
from core.execution.sandbox_session import PersistentSandboxSession
from core.execution.experience_pipeline import (
    ExperienceLearningPipeline,
    PatternSeverity,
)

log = logging.getLogger("aelvo.core.monitoring.dashboard")


class DashboardSection(str, Enum):
    """Identifies each section of the dashboard."""
    HEADER = "header"
    HEALTH = "health"
    EXECUTION = "execution"
    SESSIONS = "sessions"
    LEARNING = "learning"
    ALL = "all"


# ============================================================================
# Data Source Configuration
# ============================================================================


@dataclass
class DashboardDataSource:
    """Configuration holder for all subsystem references.

    All fields are optional — sections with no data source
    will show 'Not connected' status.
    """

    health_monitor: Optional[SystemHealthMonitor] = None
    """Health monitor for subsystem health checks."""

    tool_registry: Optional[ToolExecutionRegistry] = None
    """Registry for tool execution stats."""

    active_sessions: Optional[Callable[[], List[PersistentSandboxSession]]] = None
    """Callable returning list of active sandbox sessions.
       Using a callable allows lazy polling of session state."""

    experience_pipeline: Optional[ExperienceLearningPipeline] = None
    """Pipeline for failure pattern and retry suggestion data."""


# ============================================================================
# SystemDashboard
# ============================================================================


class SystemDashboard:
    """Unified system monitoring dashboard.

    Polls all configured subsystems and produces a rich terminal display
    with sections for health, execution, sessions, and learning insights.

    Usage:
        dashboard = SystemDashboard(data_source)
        display = await dashboard.refresh()
        print(display.to_terminal_display())
    """

    def __init__(
        self,
        data_source: DashboardDataSource,
        refresh_interval: float = 30.0,
        max_history: int = 100,
    ):
        self._source = data_source
        self._refresh_interval = refresh_interval

        # Cached/last-known data
        self._last_health_report: Optional[SystemHealthReport] = None
        self._last_registry_snapshot: Dict[str, Any] = {}
        self._last_session_snapshots: List[Dict[str, Any]] = []
        self._last_experience_summary: Dict[str, Any] = {}
        self._last_patterns: List[Dict[str, Any]] = []
        self._last_suggestions: List[Dict[str, Any]] = []

        # Tracking
        self._refresh_count: int = 0
        self._last_refresh_time: float = 0.0
        self._last_refresh_duration_ms: float = 0.0
        self._errors: List[str] = []

        log.info("SystemDashboard initialized (refresh_interval=%ds)", refresh_interval)

    # ── Refresh ─────────────────────────────────────────────────────

    async def refresh(
        self,
        sections: Optional[List[DashboardSection]] = None,
    ) -> SystemDashboard:
        """Poll all configured subsystems for fresh data.

        Args:
            sections: Optional list of sections to refresh.
                      If None, refreshes ALL sections.

        Returns:
            Self for chaining.
        """
        start = time.perf_counter()
        sections = sections or [DashboardSection.ALL]

        tasks = []

        if DashboardSection.ALL in sections or DashboardSection.HEALTH in sections:
            tasks.append(self._refresh_health())

        if DashboardSection.ALL in sections or DashboardSection.EXECUTION in sections:
            tasks.append(self._refresh_execution())

        if DashboardSection.ALL in sections or DashboardSection.SESSIONS in sections:
            tasks.append(self._refresh_sessions())

        if DashboardSection.ALL in sections or DashboardSection.LEARNING in sections:
            tasks.append(self._refresh_learning())

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._refresh_count += 1
        self._last_refresh_time = time.time()
        self._last_refresh_duration_ms = (time.perf_counter() - start) * 1000

        return self

    # ── Section Refresh Methods ─────────────────────────────────────

    async def _refresh_health(self) -> None:
        """Refresh health monitor data."""
        try:
            monitor = self._source.health_monitor
            if monitor is None:
                return
            report = await monitor.generate_report()
            self._last_health_report = report
        except Exception as e:
            self._errors.append(f"Health refresh failed: {e}")
            log.warning("Health refresh error: %s", e)

    async def _refresh_execution(self) -> None:
        """Refresh tool registry data."""
        try:
            registry = self._source.tool_registry
            if registry is None:
                return
            self._last_registry_snapshot = registry.snapshot()

            # Enrich with per-tool stats
            tool_stats = {}
            for spec in registry.list_tools():
                stats = registry.get_statistics(spec.name)
                if stats["total"] > 0:
                    tool_stats[spec.name] = stats
            self._last_registry_snapshot["tool_stats"] = tool_stats
        except Exception as e:
            self._errors.append(f"Execution refresh failed: {e}")
            log.warning("Execution refresh error: %s", e)

    async def _refresh_sessions(self) -> None:
        """Refresh sandbox session data."""
        try:
            getter = self._source.active_sessions
            if getter is None:
                return
            sessions = getter()
            snapshots = []
            for s in sessions:
                snapshots.append(s.get_summary())
            self._last_session_snapshots = snapshots
        except Exception as e:
            self._errors.append(f"Sessions refresh failed: {e}")
            log.warning("Sessions refresh error: %s", e)

    async def _refresh_learning(self) -> None:
        """Refresh experience pipeline data."""
        try:
            pipeline = self._source.experience_pipeline
            if pipeline is None:
                return
            self._last_experience_summary = pipeline.get_experience_summary()
            self._last_patterns = [
                p.to_dict() for p in pipeline.get_failure_patterns(
                    min_severity=PatternSeverity.LOW,
                )[:15]
            ]
            self._last_suggestions = [
                s.to_dict() for s in pipeline.get_retry_suggestions(
                    min_confidence=0.3,
                )[:10]
            ]
        except Exception as e:
            self._errors.append(f"Learning refresh failed: {e}")
            log.warning("Learning refresh error: %s", e)

    # ── Display ─────────────────────────────────────────────────────

    def to_terminal_display(self) -> str:
        """Generate a unified terminal display from cached data."""
        lines: List[str] = []
        lines.append(self._render_header())
        lines.append(self._render_health_section())
        lines.append(self._render_execution_section())
        lines.append(self._render_sessions_section())
        lines.append(self._render_learning_section())
        lines.append(self._render_footer())
        return "\n".join(lines)

    def _render_header(self) -> str:
        """Render the dashboard header with timestamp and status."""
        now = datetime.fromtimestamp(
            self._last_refresh_time or time.time()
        ).strftime("%Y-%m-%d %H:%M:%S")

        # Overall status indicator
        status_text = "UNKNOWN"
        status_color = "⚪"
        if self._last_health_report:
            status = self._last_health_report.overall_status
            if status == ComponentStatus.HEALTHY:
                status_text = "HEALTHY"
                status_color = "🟢"
            elif status == ComponentStatus.DEGRADED:
                status_text = "DEGRADED"
                status_color = "🟡"
            elif status == ComponentStatus.UNHEALTHY:
                status_text = "UNHEALTHY"
                status_color = "🔴"

        lines = [
            "",
            "  ╔══════════════════════════════════════════════════════════╗",
            f"  ║           AELVO SYSTEM MONITORING DASHBOARD            ║",
            "  ╚══════════════════════════════════════════════════════════╝",
            f"  {status_color} Status: {status_text}  |  "
            f"Refreshed: {now}  |  "
            f"Refresh #{self._refresh_count}",
        ]

        if self._last_refresh_duration_ms > 0:
            lines[-1] += f"  |  {self._last_refresh_duration_ms:.0f}ms"

        return "\n".join(lines)

    def _render_health_section(self) -> str:
        """Render the health monitoring section."""
        lines = ["", "  ── SYSTEM HEALTH ──"]

        monitor = self._source.health_monitor
        if monitor is None:
            lines.append("  ⚪ Health monitor: Not connected")
            return "\n".join(lines)

        report = self._last_health_report
        if report is None:
            lines.append("  ⏳ No health data yet. Run refresh() to poll.")
            return "\n".join(lines)

        # Component counts
        lines.append(
            f"  Components: {report.component_count} total  |  "
            f"🟢 {report.healthy_count} healthy  |  "
            f"🟡 {report.degraded_count} degraded  |  "
            f"🔴 {report.unhealthy_count} unhealthy"
        )

        # Score
        lines.append(
            f"  Score: {report.overall_score:.0%}  |  "
            f"Checks: {report.checks_duration_ms:.0f}ms"
        )

        # Unhealthy/degraded components detail
        bad_components = [
            c for c in report.components
            if c.status in (ComponentStatus.UNHEALTHY, ComponentStatus.DEGRADED)
        ]
        if bad_components:
            lines.append("")
            lines.append("  Issues:")
            for c in bad_components[:5]:
                icon = "🔴" if c.status == ComponentStatus.UNHEALTHY else "🟡"
                err = f" — {c.error[:80]}" if c.error else ""
                lines.append(f"    {icon} [{c.component_type}] {c.component_id}{err}")

        # Healing history
        if report.healing_actions:
            lines.append("")
            lines.append("  Recent Healing:")
            for a in report.healing_actions[-3:]:
                icon = "✓" if a.success else "✗"
                lines.append(f"    {icon} {a.action_name} ({a.duration_ms:.0f}ms)")

        return "\n".join(lines)

    def _render_execution_section(self) -> str:
        """Render the tool execution registry section."""
        lines = ["", "  ── TOOL EXECUTION ──"]

        registry = self._source.tool_registry
        if registry is None:
            lines.append("  ⚪ Tool registry: Not connected")
            return "\n".join(lines)

        snap = self._last_registry_snapshot
        if not snap:
            lines.append("  ⏳ No execution data yet. Run refresh() to poll.")
            return "\n".join(lines)

        # Overview
        lines.append(
            f"  Tools: {snap.get('registered_tools', 0)} registered  |  "
            f"Cache: {snap.get('cache_entries', 0)} entries  |  "
            f"Executions: {snap.get('execution_history', 0)} total"
        )

        # Statistics
        stats = snap.get("statistics", {})
        if stats.get("total", 0) > 0:
            lines.append(
                f"  Success rate: {stats['success_rate']:.1%}  |  "
                f"Avg duration: {stats['avg_duration_ms']:.0f}ms  |  "
                f"Cached: {stats.get('cached_count', 0)}"
            )

        # Per-tool stats
        tool_stats = snap.get("tool_stats", {})
        if tool_stats:
            lines.append("")
            lines.append("  Tool Details:")
            for tool_name in sorted(tool_stats.keys())[:10]:
                ts = tool_stats[tool_name]
                lines.append(
                    f"    {tool_name}: "
                    f"{ts['success_rate']:.0%} success  |  "
                    f"{ts['total']} execs  |  "
                    f"avg {ts.get('avg_duration_ms', 0):.0f}ms"
                )

        # Categories
        by_category = snap.get("by_category", {})
        if by_category:
            lines.append("")
            lines.append("  Categories:")
            for cat, count in sorted(by_category.items()):
                lines.append(f"    {cat}: {count}")

        return "\n".join(lines)

    def _render_sessions_section(self) -> str:
        """Render the sandbox session section."""
        lines = ["", "  ── SANDBOX SESSIONS ──"]

        getter = self._source.active_sessions
        if getter is None:
            lines.append("  ⚪ Session tracker: Not connected")
            return "\n".join(lines)

        snapshots = self._last_session_snapshots
        if not snapshots:
            lines.append("  No active sessions")
            return "\n".join(lines)

        lines.append(f"  Active sessions: {len(snapshots)}")
        lines.append("")

        for s in snapshots[:5]:
            sid = s.get("session_id", "?")[:12]
            status = s.get("status", "?").upper()
            tools = s.get("tool_executions", 0)
            errors = s.get("errors", 0)
            created = s.get("files_created", 0)
            modified = s.get("files_modified", 0)
            deleted = s.get("files_deleted", 0)

            lines.append(
                f"  [{sid}] {status}"
            )
            lines.append(
                f"     Tools: {tools}  |  Errors: {errors}  |  "
                f"+{created} ~{modified} -{deleted}"
            )

        if len(snapshots) > 5:
            lines.append(f"  ... and {len(snapshots) - 5} more session(s)")

        return "\n".join(lines)

    def _render_learning_section(self) -> str:
        """Render the experience learning section."""
        lines = ["", "  ── EXPERIENCE LEARNING ──"]

        pipeline = self._source.experience_pipeline
        if pipeline is None:
            lines.append("  ⚪ Learning pipeline: Not connected")
            return "\n".join(lines)

        summary = self._last_experience_summary
        if not summary:
            lines.append("  ⏳ No learning data yet. Run refresh() to poll.")
            return "\n".join(lines)

        # Overview
        lines.append(
            f"  Executions: {summary.get('total_executions', 0)}  |  "
            f"Success rate: {summary.get('success_rate', 0):.1%}  |  "
            f"Rollbacks: {summary.get('total_rollbacks', 0)}"
        )

        # Patterns
        patterns = self._last_patterns
        if patterns:
            critical = sum(1 for p in patterns if p.get("severity") == "critical")
            high = sum(1 for p in patterns if p.get("severity") == "high")
            lines.append(
                f"  Patterns: {len(patterns)} total  |  "
                f"🔴 {critical} critical  |  "
                f"🟡 {high} high"
            )

            # Show top patterns
            lines.append("")
            for p in patterns[:5]:
                sev_icon = {
                    "critical": "🔴",
                    "high": "🟡",
                    "medium": "  ",
                    "low": "  ",
                }.get(p.get("severity", ""), "  ")
                lines.append(
                    f"  {sev_icon} [{p.get('severity', '?').upper()}] "
                    f"{p.get('error_category', '?')} "
                    f"({p.get('occurrence_count', 0)}x) "
                    f"success={p.get('success_rate', 0):.0%}"
                )

        # Suggestions
        suggestions = self._last_suggestions
        if suggestions:
            lines.append("")
            lines.append("  Retry Suggestions:")
            for s in suggestions[:5]:
                lines.append(
                    f"    {s.get('tool_name', '?')}: "
                    f"{s.get('current_policy', '?')} → "
                    f"{s.get('suggested_policy', '?')} "
                    f"(confidence={s.get('confidence', 0):.0%})"
                )

        return "\n".join(lines)

    def _render_footer(self) -> str:
        """Render the dashboard footer."""
        lines = [
            "",
            "  ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ── ──",
        ]
        if self._errors:
            lines.append(f"  ⚠ Recent errors: {len(self._errors)}")
            for err in self._errors[-3:]:
                lines.append(f"    {err[:100]}")
        lines.append(
            f"  Refresh interval: {self._refresh_interval}s  |  "
            f"Last refresh: {self._last_refresh_duration_ms:.0f}ms"
        )
        lines.append("")
        return "\n".join(lines)

    # ── Data Access ─────────────────────────────────────────────────

    def to_dict(self) -> Dict[str, Any]:
        """Get structured dashboard data."""
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "refresh_count": self._refresh_count,
            "last_refresh_duration_ms": round(self._last_refresh_duration_ms, 2),
            "health": self._last_health_report.to_dict()
                if self._last_health_report else None,
            "execution": self._last_registry_snapshot,
            "sessions": self._last_session_snapshots,
            "learning": {
                "summary": self._last_experience_summary,
                "patterns": self._last_patterns,
                "suggestions": self._last_suggestions,
            },
            "errors": self._errors[-10:],
        }

    def snapshot(self) -> Dict[str, Any]:
        """Get a quick snapshot of the dashboard state."""
        return {
            "source_connected": {
                "health": self._source.health_monitor is not None,
                "execution": self._source.tool_registry is not None,
                "sessions": self._source.active_sessions is not None,
                "learning": self._source.experience_pipeline is not None,
            },
            "refresh_count": self._refresh_count,
            "last_refresh_duration_ms": round(self._last_refresh_duration_ms, 2),
            "has_data": {
                "health": self._last_health_report is not None,
                "execution": bool(self._last_registry_snapshot),
                "sessions": bool(self._last_session_snapshots),
                "learning": bool(self._last_experience_summary),
            },
            "error_count": len(self._errors),
            "refresh_interval": self._refresh_interval,
        }

    def clear_errors(self) -> None:
        """Clear accumulated error messages."""
        self._errors.clear()

    # ── Properties ──────────────────────────────────────────────────

    @property
    def data_source(self) -> DashboardDataSource:
        return self._source

    @property
    def refresh_count(self) -> int:
        return self._refresh_count

    @property
    def last_refresh_duration_ms(self) -> float:
        return self._last_refresh_duration_ms

    @property
    def errors(self) -> List[str]:
        return list(self._errors)
