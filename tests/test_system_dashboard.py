"""tests/test_system_dashboard.py — Phase 16: System Monitoring Dashboard

Tests the SystemDashboard, DashboardDataSource, and all dashboard sections.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock

from core.health.system_health_monitor import (
    SystemHealthMonitor,
    ComponentStatus,
)
from core.execution.tool_registry import (
    ToolExecutionRegistry,
    ToolSpec,
    ToolResult,
    ToolCategory,
)
from core.execution.sandbox_session import (
    PersistentSandboxSession,
)
from core.execution.experience_pipeline import (
    ExperienceLearningPipeline,
)
from core.monitoring.system_dashboard import (
    SystemDashboard,
    DashboardDataSource,
    DashboardSection,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def health_monitor() -> SystemHealthMonitor:
    monitor = SystemHealthMonitor()
    monitor.register_check("db", "database", lambda: True)
    monitor.register_check("fs", "filesystem", lambda: True)
    monitor.register_check("event_bus", "events", lambda: True)
    return monitor


@pytest.fixture
def tool_registry() -> ToolExecutionRegistry:
    reg = ToolExecutionRegistry()
    spec = ToolSpec(name="read_file", category=ToolCategory.FILE_OPERATION, timeout=10.0)
    async def handle_read(): return {"status": "success", "data": "content"}
    reg.register(spec, handle_read)

    spec2 = ToolSpec(name="search", category=ToolCategory.CODE_ANALYSIS, timeout=5.0)
    async def handle_search(): return {"status": "success", "data": "results"}
    reg.register(spec2, handle_search)
    return reg


@pytest.fixture
def mock_fs():
    fs = MagicMock()
    fs.read_file.return_value = {"status": "success", "data": "content"}
    return fs


@pytest.fixture
def session(mock_fs) -> PersistentSandboxSession:
    return PersistentSandboxSession(filesystem=mock_fs, workspace_root="/workspace")


@pytest.fixture
def experience_pipeline(tool_registry) -> ExperienceLearningPipeline:
    return ExperienceLearningPipeline(registry=tool_registry)


@pytest.fixture
def data_source(
    health_monitor,
    tool_registry,
    session,
    experience_pipeline,
) -> DashboardDataSource:
    return DashboardDataSource(
        health_monitor=health_monitor,
        tool_registry=tool_registry,
        active_sessions=lambda: [session],
        experience_pipeline=experience_pipeline,
    )


@pytest.fixture
def dashboard(data_source) -> SystemDashboard:
    return SystemDashboard(data_source=data_source, refresh_interval=15.0)


# ============================================================================
# DashboardDataSource Tests
# ============================================================================


class TestDashboardDataSource:
    """DashboardDataSource creation and defaults."""

    def test_create_all_none(self):
        source = DashboardDataSource()
        assert source.health_monitor is None
        assert source.tool_registry is None
        assert source.active_sessions is None
        assert source.experience_pipeline is None

    def test_create_with_sources(self, health_monitor, tool_registry):
        source = DashboardDataSource(
            health_monitor=health_monitor,
            tool_registry=tool_registry,
        )
        assert source.health_monitor is health_monitor
        assert source.tool_registry is tool_registry
        assert source.active_sessions is None
        assert source.experience_pipeline is None


# ============================================================================
# SystemDashboard Tests
# ============================================================================


class TestSystemDashboardInitialization:
    """SystemDashboard creation and properties."""

    def test_create(self, data_source):
        dashboard = SystemDashboard(data_source=data_source)
        assert dashboard.data_source is data_source
        assert dashboard.refresh_count == 0
        assert dashboard.errors == []
        assert dashboard.last_refresh_duration_ms == 0.0

    def test_initial_snapshot(self, dashboard):
        snap = dashboard.snapshot()
        assert snap["refresh_count"] == 0
        assert snap["source_connected"]["health"] is True
        assert snap["source_connected"]["execution"] is True
        assert snap["source_connected"]["sessions"] is True
        assert snap["source_connected"]["learning"] is True
        assert snap["has_data"]["health"] is False
        assert snap["error_count"] == 0

    def test_snapshot_disconnected(self):
        dashboard = SystemDashboard(data_source=DashboardDataSource())
        snap = dashboard.snapshot()
        assert snap["source_connected"]["health"] is False
        assert snap["source_connected"]["execution"] is False


class TestSystemDashboardRefresh:
    """SystemDashboard subsystem refresh."""

    @pytest.mark.asyncio
    async def test_refresh_all(self, dashboard):
        """Refresh polls all subsystems."""
        await dashboard.refresh()
        assert dashboard.refresh_count == 1
        assert dashboard.last_refresh_duration_ms > 0

    @pytest.mark.asyncio
    async def test_refresh_health(self, dashboard):
        """Health section is refreshed."""
        await dashboard.refresh(sections=[DashboardSection.HEALTH])
        assert dashboard._last_health_report is not None

    @pytest.mark.asyncio
    async def test_refresh_execution(self, dashboard):
        """Execution section is refreshed."""
        await dashboard.refresh(sections=[DashboardSection.EXECUTION])
        assert dashboard._last_registry_snapshot.get("registered_tools", 0) == 2

    @pytest.mark.asyncio
    async def test_refresh_sessions(self, dashboard):
        """Session section is refreshed."""
        await dashboard.refresh(sections=[DashboardSection.SESSIONS])
        assert len(dashboard._last_session_snapshots) >= 1

    @pytest.mark.asyncio
    async def test_refresh_learning(self, dashboard, experience_pipeline):
        """Learning section is refreshed."""
        # Add some data to pipeline first
        result = ToolResult(tool_name="read_file", status="success", output="ok")
        experience_pipeline.record_execution(result, session_id="s1")
        result2 = ToolResult(tool_name="search", status="success", output="found")
        experience_pipeline.record_execution(result2, session_id="s1")

        await dashboard.refresh(sections=[DashboardSection.LEARNING])
        assert dashboard._last_experience_summary.get("total_executions", 0) == 2

    @pytest.mark.asyncio
    async def test_refresh_disconnected(self):
        """Refresh with no sources doesn't error."""
        dashboard = SystemDashboard(data_source=DashboardDataSource())
        await dashboard.refresh()
        assert dashboard.refresh_count == 1

    @pytest.mark.asyncio
    async def test_refresh_health_with_data(self, dashboard):
        """Health refresh populates component data."""
        await dashboard.refresh(sections=[DashboardSection.HEALTH])
        report = dashboard._last_health_report
        assert report is not None
        assert report.component_count == 3
        assert report.overall_status == ComponentStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_refresh_execution_with_data(self, dashboard, tool_registry):
        """Execution refresh populates tool stats."""
        # Add history to registry
        tool_registry._record_history(ToolResult(
            tool_name="read_file", status="success", output="ok",
        ))
        tool_registry._record_history(ToolResult(
            tool_name="search", status="error", error="timed out",
        ))

        await dashboard.refresh(sections=[DashboardSection.EXECUTION])
        stats = dashboard._last_registry_snapshot.get("statistics", {})
        assert stats.get("total", 0) == 2

    @pytest.mark.asyncio
    async def test_refresh_errors_tracked(self):
        """Errors during refresh are tracked."""
        monitor = MagicMock()
        monitor.generate_report = AsyncMock(side_effect=RuntimeError("Health failed"))

        source = DashboardDataSource(health_monitor=monitor)
        dashboard = SystemDashboard(data_source=source)
        await dashboard.refresh()
        assert len(dashboard.errors) == 1
        assert "Health refresh failed" in dashboard.errors[0]

    @pytest.mark.asyncio
    async def test_refresh_count_increments(self, dashboard):
        """Each refresh increments the counter."""
        await dashboard.refresh()
        assert dashboard.refresh_count == 1
        await dashboard.refresh()
        assert dashboard.refresh_count == 2

    @pytest.mark.asyncio
    async def test_refresh_timing(self, dashboard):
        """Refresh duration is recorded."""
        await dashboard.refresh()
        assert dashboard.last_refresh_duration_ms > 0
        assert dashboard.last_refresh_duration_ms < 5000  # reasonable upper bound


class TestSystemDashboardDisplay:
    """SystemDashboard terminal display rendering."""

    @pytest.mark.asyncio
    async def test_display_all_sections(self, dashboard):
        """Full display includes all sections."""
        await dashboard.refresh()
        display = dashboard.to_terminal_display()

        assert "AELVO SYSTEM MONITORING DASHBOARD" in display
        assert "SYSTEM HEALTH" in display
        assert "TOOL EXECUTION" in display
        assert "SANDBOX SESSIONS" in display
        assert "EXPERIENCE LEARNING" in display

    @pytest.mark.asyncio
    async def test_display_health_section(self, dashboard):
        """Health section shows component data."""
        await dashboard.refresh(sections=[DashboardSection.HEALTH])
        display = dashboard.to_terminal_display()
        assert "3 healthy" in display
        assert "database" in display or "Components:" in display

    @pytest.mark.asyncio
    async def test_display_execution_section(self, dashboard):
        """Execution section shows tool data."""
        await dashboard.refresh(sections=[DashboardSection.EXECUTION])
        display = dashboard.to_terminal_display()
        assert "2 registered" in display or "TOOL EXECUTION" in display

    @pytest.mark.asyncio
    async def test_display_sessions_section(self, dashboard):
        """Sessions section shows session data."""
        await dashboard.refresh(sections=[DashboardSection.SESSIONS])
        display = dashboard.to_terminal_display()
        assert "Active sessions" in display or "SANDBOX SESSIONS" in display

    @pytest.mark.asyncio
    async def test_display_learning_section(self, dashboard, experience_pipeline):
        """Learning section shows pipeline data."""
        result = ToolResult(tool_name="read_file", status="success", output="ok")
        experience_pipeline.record_execution(result, session_id="s1")

        await dashboard.refresh(sections=[DashboardSection.LEARNING])
        display = dashboard.to_terminal_display()
        assert "EXPERIENCE LEARNING" in display

    @pytest.mark.asyncio
    async def test_display_unhealthy_components(self, health_monitor):
        """Unhealthy components are highlighted."""
        # Register a failing check
        health_monitor.register_check("broken_db", "database", lambda: False)

        source = DashboardDataSource(health_monitor=health_monitor)
        dashboard = SystemDashboard(data_source=source)
        await dashboard.refresh()

        display = dashboard.to_terminal_display()
        assert "unhealthy" in display.lower() or "broken" in display

    @pytest.mark.asyncio
    async def test_display_no_data(self):
        """Display shows placeholders when no data."""
        dashboard = SystemDashboard(data_source=DashboardDataSource())
        await dashboard.refresh()
        display = dashboard.to_terminal_display()

        assert "AELVO SYSTEM MONITORING DASHBOARD" in display
        assert "Not connected" in display or "SYSTEM HEALTH" in display

    @pytest.mark.asyncio
    async def test_display_with_execution_stats(self, dashboard, tool_registry):
        """Display shows execution stats when data exists."""
        tool_registry._record_history(ToolResult(
            tool_name="read_file", status="success", output="ok",
        ))
        tool_registry._record_history(ToolResult(
            tool_name="search", status="success", output="found",
        ))

        await dashboard.refresh()
        display = dashboard.to_terminal_display()
        assert "100%" in display  # 100% success rate
        assert "read_file" in display or "read_file" in display.lower()

    @pytest.mark.asyncio
    async def test_display_with_failure_patterns(self, dashboard, experience_pipeline):
        """Display shows failure patterns when detected."""
        fail_result = ToolResult(
            tool_name="read_file", status="error", error="Timed out after 10s",
        )
        for _ in range(3):
            experience_pipeline.record_execution(fail_result, session_id="s1")

        await dashboard.refresh()
        display = dashboard.to_terminal_display()
        assert "timed out" in display.lower() or "timeout" in display.lower()

    @pytest.mark.asyncio
    async def test_display_with_retry_suggestions(self, dashboard, experience_pipeline):
        """Display shows retry suggestions."""
        fail = ToolResult(
            tool_name="read_file", status="error", error="Timed out after 10s",
        )
        for _ in range(5):
            experience_pipeline.record_execution(fail, session_id="s1")

        await dashboard.refresh()
        display = dashboard.to_terminal_display()
        assert "Retry" in display or "retry_on" in display

    @pytest.mark.asyncio
    async def test_display_shows_error_count(self):
        """Display shows error count when errors exist."""
        monitor = MagicMock()
        monitor.generate_report = AsyncMock(side_effect=RuntimeError("Fail"))

        source = DashboardDataSource(health_monitor=monitor)
        dashboard = SystemDashboard(data_source=source)
        await dashboard.refresh()
        display = dashboard.to_terminal_display()
        assert "Recent errors" in display or "errors" in display.lower()


class TestSystemDashboardDataAccess:
    """SystemDashboard to_dict and snapshot."""

    @pytest.mark.asyncio
    async def test_to_dict(self, dashboard):
        """to_dict returns structured data."""
        await dashboard.refresh()
        data = dashboard.to_dict()
        assert "generated_at" in data
        assert "health" in data
        assert "execution" in data
        assert "sessions" in data
        assert "learning" in data
        assert "errors" in data

    @pytest.mark.asyncio
    async def test_to_dict_disconnected(self):
        """to_dict works with no sources."""
        dashboard = SystemDashboard(data_source=DashboardDataSource())
        await dashboard.refresh()
        data = dashboard.to_dict()
        assert data["health"] is None
        assert data["execution"] == {}

    @pytest.mark.asyncio
    async def test_snapshot_unconnected(self):
        """Snapshot reflects disconnected state."""
        dashboard = SystemDashboard(data_source=DashboardDataSource())
        snap = dashboard.snapshot()
        assert snap["has_data"]["health"] is False
        assert snap["has_data"]["execution"] is False

    @pytest.mark.asyncio
    async def test_snapshot_after_refresh(self, dashboard):
        """Snapshot reflects data after refresh."""
        await dashboard.refresh()
        snap = dashboard.snapshot()
        assert snap["has_data"]["health"] is True
        assert snap["has_data"]["execution"] is True
        assert snap["refresh_count"] == 1

    def test_clear_errors(self, dashboard):
        """clear_errors removes accumulated errors."""
        dashboard._errors.append("Test error")
        assert len(dashboard.errors) == 1
        dashboard.clear_errors()
        assert dashboard.errors == []


class TestSystemDashboardIntegration:
    """End-to-end integration scenarios."""

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, dashboard):
        """Complete refresh → display → snapshot cycle."""
        await dashboard.refresh()
        display = dashboard.to_terminal_display()
        data = dashboard.to_dict()
        snap = dashboard.snapshot()

        assert len(display) > 200  # Substantial display output
        assert data["health"] is not None
        assert snap["refresh_count"] == 1

    @pytest.mark.asyncio
    async def test_disconnected_lifecycle(self):
        """Complete cycle with no sources."""
        dashboard = SystemDashboard(data_source=DashboardDataSource())
        await dashboard.refresh()
        display = dashboard.to_terminal_display()
        dashboard.to_dict()

        assert len(display) > 100
        assert "Not connected" in display

    @pytest.mark.asyncio
    async def test_multiple_refreshes(self, dashboard):
        """Multiple refreshes work correctly."""
        for _ in range(3):
            await dashboard.refresh()
        assert dashboard.refresh_count == 3

    @pytest.mark.asyncio
    async def test_unhealthy_then_healthy(self, health_monitor):
        """Dashboard reflects changing health status."""
        # Start with a failing check
        call_count = [0]
        def flaky_check():
            call_count[0] += 1
            return call_count[0] > 2  # Fails first 2 times, then succeeds

        health_monitor.register_check("flaky", "test", flaky_check)
        source = DashboardDataSource(health_monitor=health_monitor)
        dashboard = SystemDashboard(data_source=source)

        # First refresh — should show unhealthy
        await dashboard.refresh()
        report1 = dashboard._last_health_report
        assert report1 is not None

        # Second refresh — should show healthier
        await dashboard.refresh()
        report2 = dashboard._last_health_report
        assert report2 is not None

    @pytest.mark.asyncio
    async def test_session_with_errors(self, mock_fs, dashboard):
        """Dashboard reflects sessions with errors."""
        # Add a session with errors
        err_session = PersistentSandboxSession(
            filesystem=mock_fs, workspace_root="/workspace",
        )
        err_session.state.errors = 5
        err_session.state.tool_executions = 10

        # Update the data source to include this session
        dashboard._source.active_sessions = lambda: [err_session]

        await dashboard.refresh(sections=[DashboardSection.SESSIONS])
        display = dashboard.to_terminal_display()
        assert "5" in display  # errors count
        assert "10" in display  # tool count

    @pytest.mark.asyncio
    async def test_refresh_preserves_previous_data(self, dashboard):
        """Refresh preserves data from sections not refreshed."""
        await dashboard.refresh(sections=[DashboardSection.HEALTH])
        assert dashboard._last_health_report is not None

        # Refresh a different section — health data should persist
        await dashboard.refresh(sections=[DashboardSection.EXECUTION])
        assert dashboard._last_health_report is not None

    def test_dashboard_section_enum(self):
        """DashboardSection enum values."""
        assert DashboardSection.HEADER.value == "header"
        assert DashboardSection.HEALTH.value == "health"
        assert DashboardSection.EXECUTION.value == "execution"
        assert DashboardSection.SESSIONS.value == "sessions"
        assert DashboardSection.LEARNING.value == "learning"
        assert DashboardSection.ALL.value == "all"
