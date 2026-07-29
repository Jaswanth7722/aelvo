"""tests/test_system_health_monitor.py — Phase 13: System Health & Autonomous Healing

Tests the SystemHealthMonitor, SystemHealthReport, HealthCheck, HealingAction,
and autonomous healing integration.
"""

import asyncio
import time
import pytest
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

from core.health.system_health_monitor import (
    SystemHealthMonitor,
    SystemHealthReport,
    ComponentHealth,
    ComponentStatus,
    HealActionResult,
    HealthCheck,
    HealingAction,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def monitor() -> SystemHealthMonitor:
    """Fresh SystemHealthMonitor for each test."""
    return SystemHealthMonitor()


@pytest.fixture
def healthy_check_fn():
    """A health check that always succeeds."""
    async def check():
        return True
    return check


@pytest.fixture
def unhealthy_check_fn():
    """A health check that always fails."""
    async def check():
        return False
    return check


# ============================================================================
# ComponentHealth Tests
# ============================================================================


class TestComponentHealth:
    """ComponentHealth dataclass creation and helpers."""

    def test_create_healthy(self):
        c = ComponentHealth(
            component_id="test_db",
            component_type="database",
            status=ComponentStatus.HEALTHY,
            score=1.0,
        )
        assert c.component_id == "test_db"
        assert c.component_type == "database"
        assert c.status == ComponentStatus.HEALTHY
        assert c.score == 1.0

    def test_create_unhealthy(self):
        c = ComponentHealth(
            component_id="test_bus",
            component_type="event_bus",
            status=ComponentStatus.UNHEALTHY,
            score=0.0,
            error="Connection refused",
        )
        assert c.error == "Connection refused"
        assert c.status == ComponentStatus.UNHEALTHY

    def test_to_dict(self):
        c = ComponentHealth(
            component_id="mem_db",
            component_type="database",
            status=ComponentStatus.HEALTHY,
            score=0.95,
            latency_ms=12.5,
            detail="Connected",
        )
        d = c.to_dict()
        assert d["component_id"] == "mem_db"
        assert d["status"] == "healthy"
        assert d["score"] == 0.95
        assert d["latency_ms"] == 12.5


# ============================================================================
# SystemHealthReport Tests
# ============================================================================


class TestSystemHealthReport:
    """SystemHealthReport aggregation and display."""

    def test_is_healthy_property(self):
        report = SystemHealthReport(overall_status=ComponentStatus.HEALTHY)
        assert report.is_healthy is True

        report2 = SystemHealthReport(overall_status=ComponentStatus.UNHEALTHY)
        assert report2.is_healthy is False

    def test_to_dict(self):
        report = SystemHealthReport(
            overall_status=ComponentStatus.DEGRADED,
            overall_score=0.65,
            component_count=3,
            healthy_count=1,
            degraded_count=1,
            unhealthy_count=1,
            recommendations=["Check event bus"],
        )
        d = report.to_dict()
        assert d["overall_status"] == "degraded"
        assert d["overall_score"] == 0.65
        assert d["recommendations"] == ["Check event bus"]

    def test_to_terminal_display_healthy(self):
        report = SystemHealthReport(
            overall_status=ComponentStatus.HEALTHY,
            overall_score=1.0,
            component_count=2,
            healthy_count=2,
            components=[
                ComponentHealth(component_id="db", component_type="database", status=ComponentStatus.HEALTHY, score=1.0),
                ComponentHealth(component_id="bus", component_type="event_bus", status=ComponentStatus.HEALTHY, score=1.0),
            ],
            recommendations=["All systems healthy"],
        )
        display = report.to_terminal_display()
        assert "SYSTEM HEALTH REPORT" in display
        assert "HEALTHY" in display
        assert "100%" in display
        assert "All systems healthy" in display

    def test_to_terminal_display_unhealthy(self):
        report = SystemHealthReport(
            overall_status=ComponentStatus.UNHEALTHY,
            overall_score=0.25,
            component_count=2,
            healthy_count=0,
            unhealthy_count=1,
            components=[
                ComponentHealth(
                    component_id="event_bus", component_type="bus",
                    status=ComponentStatus.UNHEALTHY, score=0.0,
                    error="Not running",
                ),
                ComponentHealth(
                    component_id="memory", component_type="database",
                    status=ComponentStatus.HEALTHY, score=1.0,
                ),
            ],
            recommendations=["Restart event bus"],
            healing_actions=[
                HealActionResult(component_id="event_bus", action_name="restart", success=True, message="Restarted"),
            ],
        )
        display = report.to_terminal_display()
        assert "UNHEALTHY" in display
        assert "Restart event bus" in display
        assert "Not running" in display
        assert "restart" in display


# ============================================================================
# HealActionResult Tests
# ============================================================================


class TestHealActionResult:
    """HealActionResult dataclass."""

    def test_create(self):
        r = HealActionResult(
            component_id="event_bus",
            action_name="restart",
            success=True,
            message="Restarted successfully",
            duration_ms=150.0,
        )
        assert r.success is True
        assert r.message == "Restarted successfully"
        assert r.duration_ms == 150.0

    def test_to_dict(self):
        r = HealActionResult(
            component_id="db", action_name="reconnect",
            success=False, message="Failed",
        )
        d = r.to_dict()
        assert d["action_name"] == "reconnect"
        assert d["success"] is False


# ============================================================================
# HealthCheck Tests
# ============================================================================


class TestHealthCheck:
    """HealthCheck runs and returns ComponentHealth."""

    @pytest.mark.asyncio
    async def test_healthy_check(self):
        async def check():
            return True

        hc = HealthCheck("test", "database", check)
        result = await hc.run()
        assert result.status == ComponentStatus.HEALTHY
        assert result.score == 1.0
        assert result.latency_ms > 0

    @pytest.mark.asyncio
    async def test_unhealthy_check(self):
        async def check():
            return False

        hc = HealthCheck("test", "database", check)
        result = await hc.run()
        assert result.status == ComponentStatus.UNHEALTHY
        assert result.score == 0.0

    @pytest.mark.asyncio
    async def test_check_returns_component_health_directly(self):
        async def check():
            return ComponentHealth(
                component_id="custom", component_type="service",
                status=ComponentStatus.DEGRADED, score=0.5,
                detail="Custom result",
            )

        hc = HealthCheck("custom", "service", check)
        result = await hc.run()
        assert result.status == ComponentStatus.DEGRADED
        assert result.score == 0.5
        assert result.detail == "Custom result"

    @pytest.mark.asyncio
    async def test_check_times_out(self):
        async def slow_check():
            await asyncio.sleep(10)  # longer than timeout
            return True

        hc = HealthCheck("slow", "test", slow_check, timeout=0.1)
        result = await hc.run()
        assert result.status == ComponentStatus.UNHEALTHY
        assert "timed out" in result.detail

    @pytest.mark.asyncio
    async def test_check_raises_exception(self):
        async def broken_check():
            raise RuntimeError("Something broke")

        hc = HealthCheck("broken", "test", broken_check)
        result = await hc.run()
        assert result.status == ComponentStatus.UNHEALTHY
        assert "Something broke" in result.error

    @pytest.mark.asyncio
    async def test_sync_check_function(self):
        def sync_check():
            return True

        hc = HealthCheck("sync", "test", sync_check)
        result = await hc.run()
        assert result.status == ComponentStatus.HEALTHY

    def test_tags(self):
        async def check():
            return True

        hc = HealthCheck("tagged", "test", check, tags={"env": "test", "region": "us"})
        assert hc.tags["env"] == "test"


# ============================================================================
# HealingAction Tests
# ============================================================================


class TestHealingAction:
    """HealingAction executes and returns HealActionResult."""

    @pytest.mark.asyncio
    async def test_successful_action(self):
        async def heal():
            return True

        action = HealingAction("test", "restart", heal, description="Restart service")
        result = await action.execute()
        assert result.success is True
        assert result.action_name == "restart"
        assert result.duration_ms > 0

    @pytest.mark.asyncio
    async def test_failed_action(self):
        async def heal():
            return False

        action = HealingAction("test", "reconnect", heal)
        result = await action.execute()
        assert result.success is False

    @pytest.mark.asyncio
    async def test_action_returns_heal_result_directly(self):
        async def heal():
            return HealActionResult(
                component_id="test", action_name="custom",
                success=True, message="Custom result",
            )

        action = HealingAction("test", "custom", heal)
        result = await action.execute()
        assert result.success is True
        assert result.message == "Custom result"

    @pytest.mark.asyncio
    async def test_action_times_out(self):
        async def slow_heal():
            await asyncio.sleep(10)
            return True

        action = HealingAction("slow", "heal", slow_heal, timeout=0.1)
        result = await action.execute()
        assert result.success is False
        assert "timed out" in result.message

    @pytest.mark.asyncio
    async def test_action_raises_exception(self):
        async def broken_heal():
            raise ValueError("Can't heal")

        action = HealingAction("broken", "heal", broken_heal)
        result = await action.execute()
        assert result.success is False
        assert "Can't heal" in result.message

    @pytest.mark.asyncio
    async def test_sync_action_function(self):
        def sync_heal():
            return True

        action = HealingAction("sync", "heal", sync_heal)
        result = await action.execute()
        assert result.success is True


# ============================================================================
# SystemHealthMonitor Tests
# ============================================================================


class TestSystemHealthMonitor:
    """SystemHealthMonitor registration and checks."""

    @pytest.mark.asyncio
    async def test_register_and_run_check(self, monitor, healthy_check_fn):
        """Register a check and run it."""
        monitor.register_check(
            "test_db", "database", healthy_check_fn,
        )
        result = await monitor.run_check("test_db")
        assert result is not None
        assert result.status == ComponentStatus.HEALTHY
        assert result.component_id == "test_db"

    @pytest.mark.asyncio
    async def test_run_check_nonexistent(self, monitor):
        """run_check returns None for unknown component."""
        result = await monitor.run_check("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_run_all_checks(self, monitor, healthy_check_fn, unhealthy_check_fn):
        """run_all_checks runs all registered checks."""
        monitor.register_check("healthy_db", "database", healthy_check_fn)
        monitor.register_check("unhealthy_bus", "event_bus", unhealthy_check_fn)

        results = await monitor.run_all_checks()
        assert len(results) == 2

        statuses = {c.component_id: c.status for c in results}
        assert statuses["healthy_db"] == ComponentStatus.HEALTHY
        assert statuses["unhealthy_bus"] == ComponentStatus.UNHEALTHY

    @pytest.mark.asyncio
    async def test_run_all_checks_empty(self, monitor):
        """run_all_checks returns empty list when no checks."""
        results = await monitor.run_all_checks()
        assert results == []

    def test_get_check(self, monitor, healthy_check_fn):
        """get_check returns registered HealthCheck."""
        monitor.register_check("test", "database", healthy_check_fn)
        check = monitor.get_check("test")
        assert check is not None
        assert check.component_id == "test"

        assert monitor.get_check("nonexistent") is None

    def test_get_all_checks(self, monitor, healthy_check_fn):
        """get_all_checks returns all registered checks."""
        monitor.register_check("a", "type_a", healthy_check_fn)
        monitor.register_check("b", "type_b", healthy_check_fn)
        checks = monitor.get_all_checks()
        assert len(checks) == 2

    def test_unregister_check(self, monitor, healthy_check_fn):
        """unregister_check removes a check."""
        monitor.register_check("test", "database", healthy_check_fn)
        assert monitor.unregister_check("test") is True
        assert monitor.get_check("test") is None
        assert monitor.unregister_check("nonexistent") is False

    def test_register_healing_action(self, monitor):
        """register_healing_action registers an action for a component."""
        async def heal():
            return True

        monitor.register_healing_action("test_bus", "restart", heal)
        actions = monitor._healing_actions.get("test_bus", [])
        assert len(actions) == 1
        assert actions[0].action_name == "restart"

    @pytest.mark.asyncio
    async def test_heal_component(self, monitor):
        """heal_component runs all actions for a component."""
        async def heal():
            return True

        monitor.register_healing_action("test_bus", "restart", heal)
        monitor.register_healing_action("test_bus", "reconnect", heal)

        results = await monitor.heal_component("test_bus")
        assert len(results) == 2
        assert all(r.success for r in results)

    @pytest.mark.asyncio
    async def test_heal_component_no_actions(self, monitor):
        """heal_component returns empty list when no actions."""
        results = await monitor.heal_component("unknown")
        assert results == []

    @pytest.mark.asyncio
    async def test_heal_specific(self, monitor):
        """heal_specific runs a named action."""
        results = []

        async def heal_a():
            results.append("a")
            return True

        async def heal_b():
            results.append("b")
            return True

        monitor.register_healing_action("test", "action_a", heal_a)
        monitor.register_healing_action("test", "action_b", heal_b)

        result = await monitor.heal_specific("test", "action_a")
        assert result is not None
        assert result.success is True
        assert result.action_name == "action_a"
        assert results == ["a"]

    @pytest.mark.asyncio
    async def test_heal_specific_not_found(self, monitor):
        """heal_specific returns None for unknown action."""
        result = await monitor.heal_specific("test", "nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_heal_all_degraded(self, monitor, unhealthy_check_fn, healthy_check_fn):
        """heal_all_degraded runs actions for unhealthy components."""
        monitor.register_check("unhealthy_bus", "event_bus", unhealthy_check_fn)
        monitor.register_check("healthy_db", "database", healthy_check_fn)

        heal_called = []

        async def heal_bus():
            heal_called.append("bus")
            return True

        monitor.register_healing_action("unhealthy_bus", "restart", heal_bus)

        results = await monitor.heal_all_degraded()
        assert "unhealthy_bus" in results
        assert len(results["unhealthy_bus"]) == 1
        assert heal_called == ["bus"]

    @pytest.mark.asyncio
    async def test_heal_all_degraded_auto_heal_disabled(self, monitor, unhealthy_check_fn):
        """heal_all_degraded returns empty when auto-heal disabled."""
        monitor.register_check("bad", "test", unhealthy_check_fn)
        monitor.enable_auto_heal(False)

        async def heal():
            return True
        monitor.register_healing_action("bad", "fix", heal)

        results = await monitor.heal_all_degraded()
        assert results == {}

    @pytest.mark.asyncio
    async def test_generate_report(self, monitor, healthy_check_fn, unhealthy_check_fn):
        """generate_report returns a complete report."""
        monitor.register_check("db", "database", healthy_check_fn)
        monitor.register_check("bus", "event_bus", unhealthy_check_fn)

        report = await monitor.generate_report(include_healing_history=False)

        assert report.component_count == 2
        assert report.healthy_count >= 1
        assert report.unhealthy_count >= 1
        assert report.overall_status == ComponentStatus.UNHEALTHY
        assert report.overall_score < 1.0
        assert len(report.recommendations) > 0
        assert report.checks_duration_ms > 0

    @pytest.mark.asyncio
    async def test_generate_report_all_healthy(self, monitor, healthy_check_fn):
        """generate_report with all healthy components."""
        monitor.register_check("db", "database", healthy_check_fn)
        monitor.register_check("bus", "event_bus", healthy_check_fn)
        monitor.register_check("fs", "filesystem", healthy_check_fn)

        report = await monitor.generate_report()
        assert report.overall_status == ComponentStatus.HEALTHY
        assert report.overall_score == 1.0
        assert report.healthy_count == 3
        assert report.component_count == 3

    @pytest.mark.asyncio
    async def test_consecutive_failure_tracking(self, monitor, unhealthy_check_fn):
        """Consecutive failures are tracked correctly."""
        monitor.register_check("bad", "test", unhealthy_check_fn)

        await monitor.run_check("bad")
        await monitor.run_check("bad")
        await monitor.run_check("bad")

        history = monitor.get_component_history("bad")
        assert len(history) == 3
        assert all(h.status == ComponentStatus.UNHEALTHY for h in history)
        assert history[-1].consecutive_failures >= 2

    @pytest.mark.asyncio
    async def test_consecutive_failure_reset_on_healthy(self, monitor, unhealthy_check_fn, healthy_check_fn):
        """Consecutive failures reset when component becomes healthy."""
        # Use a mutable flag to toggle check behavior
        healthy = [False]

        async def toggle_check():
            return healthy[0]

        monitor.register_check("toggle", "test", toggle_check)

        # First check: unhealthy
        result = await monitor.run_check("toggle")
        assert result.consecutive_failures == 1

        # Switch to healthy
        healthy[0] = True
        result = await monitor.run_check("toggle")
        assert result.consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_auto_heal_triggers_on_threshold(self, monitor, unhealthy_check_fn):
        """Auto-heal triggers when consecutive failures reach threshold."""
        monitor.register_check("bad_bus", "event_bus", unhealthy_check_fn)

        heal_called = []
        async def heal_bus():
            heal_called.append("healed")
            return True

        monitor.register_healing_action("bad_bus", "restart", heal_bus)
        monitor.set_auto_heal_threshold(2)

        # One failure: below threshold
        await monitor.run_check("bad_bus")
        assert len(heal_called) == 0

        # Two failures: reaches threshold
        await monitor.run_check("bad_bus")
        # Auto-heal is deferred via ensure_future, so give it a tick
        await asyncio.sleep(0.05)
        assert len(heal_called) == 1

    def test_snapshot(self, monitor, healthy_check_fn):
        """snapshot() returns monitor state."""
        monitor.register_check("db", "database", healthy_check_fn)
        snap = monitor.snapshot()
        assert snap["registered_checks"] == 1
        assert "auto_heal_enabled" in snap
        assert snap["components_tracked"] == ["db"]

    def test_enable_auto_heal(self, monitor):
        """enable_auto_heal toggles auto healing."""
        assert monitor._auto_heal_enabled is True
        monitor.enable_auto_heal(False)
        assert monitor._auto_heal_enabled is False
        monitor.enable_auto_heal(True)
        assert monitor._auto_heal_enabled is True

    def test_set_auto_heal_threshold(self, monitor):
        """set_auto_heal_threshold changes threshold."""
        monitor.set_auto_heal_threshold(5)
        assert monitor._auto_heal_threshold == 5
        monitor.set_auto_heal_threshold(0)  # minimum is 1
        assert monitor._auto_heal_threshold == 1

    def test_clear_history(self, monitor, healthy_check_fn):
        """clear_history removes all stored data."""
        monitor.register_check("db", "database", healthy_check_fn)
        # Run check to populate history
        import asyncio
        asyncio.run(monitor.run_check("db"))

        monitor.clear_history()
        assert len(monitor._component_history) == 0
        assert len(monitor._recent_healing) == 0

    def test_to_terminal_display(self, monitor):
        """to_terminal_display() returns human-readable output."""
        display = monitor.to_terminal_display()
        assert "SYSTEM HEALTH MONITOR" in display
        assert "Checks registered:" in display

    def test_get_healing_history(self, monitor):
        """get_healing_history returns recent actions."""
        assert monitor.get_healing_history() == []

        async def heal():
            return True

        action = HealingAction("test", "fix", heal)
        result = asyncio.run(action.execute())
        monitor._recent_healing.append(result)

        history = monitor.get_healing_history()
        assert len(history) == 1
        assert history[0].success is True

    def test_get_component_history_empty(self, monitor):
        """get_component_history returns empty for unknown component."""
        assert monitor.get_component_history("nonexistent") == []


# ============================================================================
# Integration Tests
# ============================================================================


class TestSystemHealthIntegration:
    """End-to-end system health monitoring integration."""

    @pytest.mark.asyncio
    async def test_full_monitoring_lifecycle(self, monitor):
        """Complete lifecycle: register → check → report → heal."""
        # 1. Register checks
        healthy_flag = [True]

        async def db_check():
            return True

        async def bus_check():
            return healthy_flag[0]

        monitor.register_check("memory_db", "database", db_check)
        monitor.register_check("event_bus", "event_bus", bus_check)

        # 2. Generate report (all healthy)
        report = await monitor.generate_report()
        assert report.is_healthy is True
        assert report.overall_score == 1.0

        # 3. Make bus unhealthy
        healthy_flag[0] = False

        # 4. Register healing action
        healed = [False]

        async def heal_bus():
            healed[0] = True
            healthy_flag[0] = True  # healing fixes it
            return True

        monitor.register_healing_action("event_bus", "restart", heal_bus)

        # 5. Generate report (bus unhealthy)
        report2 = await monitor.generate_report()
        assert report2.is_healthy is False
        assert report2.unhealthy_count >= 1

        # 6. Heal the bus
        results = await monitor.heal_component("event_bus")
        assert len(results) == 1
        assert results[0].success is True
        assert healed[0] is True

        # 7. Verify bus is healthy again
        result = await monitor.run_check("event_bus")
        assert result.status == ComponentStatus.HEALTHY

        # 8. Final report should show all healthy
        report3 = await monitor.generate_report()
        assert report3.is_healthy is True

    @pytest.mark.asyncio
    async def test_multiple_components_with_varying_health(self, monitor):
        """Multiple components with different health states."""
        checks = {
            "db": lambda: True,
            "bus": lambda: True,
            "fs": lambda: False,  # unhealthy
            "provider": lambda: True,
            "specialist": lambda: True,
        }

        for cid, fn in checks.items():
            monitor.register_check(cid, cid, fn)

        report = await monitor.generate_report()
        assert report.component_count == 5
        assert report.healthy_count == 4
        assert report.unhealthy_count == 1
        assert report.overall_status == ComponentStatus.UNHEALTHY

        # Verify all components in report
        component_ids = [c.component_id for c in report.components]
        for cid in checks:
            assert cid in component_ids

    @pytest.mark.asyncio
    async def test_heal_action_recovery_cycle(self, monitor):
        """Multiple heal actions can recover a system."""
        state = {"failures": 0}

        async def flaky_check():
            state["failures"] += 1
            return state["failures"] < 2  # only passes first call, then fails

        async def reset_state():
            state["failures"] = 0
            return True

        monitor.register_check("flaky", "test", flaky_check)
        monitor.register_healing_action("flaky", "reset", reset_state)

        # First check: passes (baseline)
        result = await monitor.run_check("flaky")
        assert result.status == ComponentStatus.HEALTHY

        # Second run: fails (state is now 2, 2 < 2 = False)
        result = await monitor.run_check("flaky")
        assert result.status == ComponentStatus.UNHEALTHY

        # Heal resets state
        await monitor.heal_component("flaky")

        # After heal, state is reset, check passes again
        result = await monitor.run_check("flaky")
        assert result.status == ComponentStatus.HEALTHY
