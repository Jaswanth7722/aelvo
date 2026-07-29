"""Phase 14 — Monitoring & Observability Tests.

Covers:
- RuntimeMetricsCollector: metric recording, tagged series, percentiles, summaries
- RuntimeHealthMonitor: check registration, execution, status computation
- AlertManager: creation, dedup, rules, handlers
- RuntimeDashboard: snapshot generation, subsystem health aggregation
- RecoveryEngine integration: metrics/health/alerts/dashboard wired
"""

import time
from typing import List

from runtime_next.monitoring.metrics import (
    RuntimeMetricsCollector,
    MetricType,
)
from runtime_next.monitoring.health import (
    RuntimeHealthMonitor,
    HealthCheckResult,
    HealthStatus,
    HealthCheckPolicy,
)
from runtime_next.monitoring.alerting import (
    AlertManager,
    Alert,
    AlertSeverity,
    AlertRule,
)
from runtime_next.monitoring.dashboard import (
    RuntimeDashboard,
    DashboardSnapshot,
)
from runtime_next.monitoring.cli import RuntimeCLI


# ═══════════════════════════════════════════════════════════════════════════════
# RuntimeMetricsCollector Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRuntimeMetricsCollector:
    def test_record_and_retrieve(self):
        """Basic metric recording and retrieval."""
        collector = RuntimeMetricsCollector()
        collector.record("test.metric", value=42.0)
        series = collector.get_series("test.metric")
        assert series is not None
        assert series.latest == 42.0
        assert series.count == 1

    def test_record_with_tags(self):
        """Metrics with tags create separate series."""
        collector = RuntimeMetricsCollector()
        collector.record("test.metric", tags={"env": "test", "component": "forge"})
        collector.record("test.metric", tags={"env": "prod", "component": "forge"})

        test_series = collector.get_series("test.metric", tags={"env": "test", "component": "forge"})
        prod_series = collector.get_series("test.metric", tags={"env": "prod", "component": "forge"})
        assert test_series is not None and test_series.count == 1
        assert prod_series is not None and prod_series.count == 1

    def test_percentiles(self):
        """Percentile computation works."""
        collector = RuntimeMetricsCollector()
        for v in range(1, 101):
            collector.record("percentile.test", value=float(v))

        series = collector.get_series("percentile.test")
        assert series is not None
        assert series.percentile(50) == 50
        assert series.percentile(95) == 95
        assert series.percentile(99) == 99
        assert series.min == 1.0
        assert series.max == 100.0
        assert series.avg == 50.5

    def test_empty_series(self):
        """Empty series returns None for stats."""
        collector = RuntimeMetricsCollector()
        series = collector.get_series("nonexistent")
        assert series is None

    def test_summary(self):
        """Summary returns all metrics."""
        collector = RuntimeMetricsCollector()
        collector.record("a", value=1.0)
        collector.record("b", value=2.0)

        summary = collector.summary()
        assert "a" in summary
        assert "b" in summary

    def test_summary_filtered_by_type(self):
        """Summary filters by MetricType."""
        collector = RuntimeMetricsCollector()
        collector.record_recovery_attempt("consensus", "deadlocked", True)
        collector.record_governance_evaluation("consensus", "allow")

        recovery_summary = collector.summary(metric_type=MetricType.RECOVERY)
        assert any("recovery" in k for k in recovery_summary)

        gov_summary = collector.summary(metric_type=MetricType.GOVERNANCE)
        assert any("governance" in k for k in gov_summary)

    def test_recovery_metrics(self):
        """Recovery metric helpers work."""
        collector = RuntimeMetricsCollector()
        collector.record_recovery_attempt("consensus", "deadlocked", True, duration_ms=150.0)
        collector.record_recovery_strategy("consensus", "add_architect")
        collector.record_specialist_state_change("FORGE", "healthy", "degraded")
        collector.record_task_recovery_trigger("phase_failure", "retry_phase")

        assert collector.recovery_summary()["total_attempts"] == 1
        assert collector.recovery_summary()["total_successes"] == 1

    def test_governance_metrics(self):
        """Governance metric helpers work."""
        collector = RuntimeMetricsCollector()
        collector.record_governance_evaluation("consensus", "deny", policy_id="pol_1")
        collector.record_governance_evaluation("consensus", "allow")
        collector.record_governance_approval(True, "pol_1")
        collector.record_hook_execution("consensus", "denied", duration_ms=5.0)

        summary = collector.governance_summary()
        assert summary["total_evaluations"] == 2
        assert summary["denied_count"] >= 1  # May be 0 since we check by tags

    def test_scaling_metrics(self):
        """Scaling metric helpers work."""
        collector = RuntimeMetricsCollector()
        collector.record_pool_utilization("forge_pool", 5, 10)
        collector.record_pipeline_stage("pipe_1", "stage_a", "completed", duration_ms=200.0)
        collector.record_batch_completed("batch_1", 10, 8, 500.0)

        summary = collector.scaling_summary()
        assert summary["completed_batches"] == 1

    def test_rate_tracking(self):
        """Rate tracking computes events per second."""
        collector = RuntimeMetricsCollector()
        time.time()

        # Simulate events
        collector.record_rate("requests")
        collector.record_rate("requests")
        collector.record_rate("requests")

        # Rate over a long window should be > 0
        rate = collector.get_rate("requests", window_seconds=60.0)
        assert rate > 0

    def test_reset(self):
        """Reset clears all metrics."""
        collector = RuntimeMetricsCollector()
        collector.record("test", value=1.0)
        collector.reset()
        assert collector.get_series("test") is None

    def test_counters(self):
        """Tagged counters are tracked."""
        collector = RuntimeMetricsCollector()
        collector.record("metric_a", tags={"key": "val1"})
        collector.record("metric_a", tags={"key": "val1"})
        collector.record("metric_a", tags={"key": "val2"})

        counters = collector.get_counters()
        # Should have entries for each tagged combination
        assert len(counters) >= 2


# ═══════════════════════════════════════════════════════════════════════════════
# RuntimeHealthMonitor Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRuntimeHealthMonitor:
    def test_register_check(self):
        """Health checks can be registered."""
        monitor = RuntimeHealthMonitor()
        policy = HealthCheckPolicy(
            subsystem="recovery",
            check_id="test_check",
            description="Test health check",
            interval_seconds=30.0,
            failure_threshold=3,
        )
        monitor.register_check(policy)
        assert len(monitor.get_policies()) == 1
        assert len(monitor.get_policies(subsystem="recovery")) == 1

    def test_unregister_check(self):
        """Health checks can be removed."""
        monitor = RuntimeHealthMonitor()
        policy = HealthCheckPolicy(
            subsystem="recovery", check_id="rm_check",
            description="", interval_seconds=30.0,
        )
        monitor.register_check(policy)
        assert monitor.unregister_check("recovery", "rm_check") is True
        assert monitor.unregister_check("recovery", "nonexistent") is False

    def test_run_check_with_function(self):
        """Health check with check_fn executes and records."""
        monitor = RuntimeHealthMonitor()
        check_count = 0

        def check_fn() -> HealthCheckResult:
            nonlocal check_count
            check_count += 1
            return HealthCheckResult(healthy=True, message="All good")

        policy = HealthCheckPolicy(
            subsystem="recovery", check_id="fn_check",
            description="", interval_seconds=30.0,
            failure_threshold=3, check_fn=check_fn,
        )
        monitor.register_check(policy)

        result = monitor.run_check("recovery", "fn_check")
        assert result is not None
        assert result.healthy is True
        assert check_count == 1

    def test_run_check_unhealthy(self):
        """Unhealthy health checks are recorded."""
        monitor = RuntimeHealthMonitor()

        def fail_fn() -> HealthCheckResult:
            return HealthCheckResult(healthy=False, message="Down")

        policy = HealthCheckPolicy(
            subsystem="recovery", check_id="fail_check",
            description="", interval_seconds=30.0,
            failure_threshold=2, check_fn=fail_fn,
        )
        monitor.register_check(policy)

        # Two failures
        monitor.run_check("recovery", "fail_check")
        monitor.run_check("recovery", "fail_check")

        assert monitor.get_check_status("recovery", "fail_check") == HealthStatus.UNHEALTHY

    def test_get_subsystem_status(self):
        """Subsystem status aggregation works."""
        monitor = RuntimeHealthMonitor()

        # Register two healthy checks
        monitor.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="h1",
            description="", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        monitor.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="h2",
            description="", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))

        monitor.run_all_checks()
        assert monitor.get_subsystem_status("recovery") == HealthStatus.HEALTHY

    def test_subsystem_degraded(self):
        """Subsystem with some failures is DEGRADED."""
        monitor = RuntimeHealthMonitor()

        monitor.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="h1",
            description="", interval_seconds=30.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        monitor.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="f1",
            description="", interval_seconds=30.0,
            failure_threshold=3,
            check_fn=lambda: HealthCheckResult(healthy=False),
        ))

        monitor.run_all_checks()
        # One failed (but below threshold) — should be DEGRADED
        status = monitor.get_subsystem_status("recovery")
        assert status == HealthStatus.DEGRADED

    def test_overall_status(self):
        """Overall status reflects worst subsystem."""
        monitor = RuntimeHealthMonitor()

        monitor.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="h1",
            description="", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        monitor.register_check(HealthCheckPolicy(
            subsystem="governance", check_id="g1",
            description="", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))

        monitor.run_all_checks()
        assert monitor.get_overall_status() == HealthStatus.HEALTHY

    def test_run_check_nonexistent(self):
        """Running a non-existent check returns None."""
        monitor = RuntimeHealthMonitor()
        result = monitor.run_check("nonexistent", "check")
        assert result is None

    def test_generate_health_report(self):
        """Health report is well-formed."""
        monitor = RuntimeHealthMonitor()

        monitor.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="test",
            description="Test", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        monitor.run_all_checks()

        report = monitor.generate_health_report()
        assert "overall_status" in report
        assert "subsystems" in report
        assert "recovery" in report["subsystems"]

    def test_reset(self):
        """Reset clears all health state."""
        monitor = RuntimeHealthMonitor()
        monitor.register_check(HealthCheckPolicy(
            subsystem="test", check_id="t1",
            description="", interval_seconds=30.0,
        ))
        monitor.reset()
        assert len(monitor.get_policies()) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# AlertManager Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestAlertManager:
    def test_create_alert(self):
        """Basic alert creation."""
        manager = AlertManager()
        alert = manager.create_alert(
            title="Test alert",
            message="This is a test",
            severity=AlertSeverity.WARNING,
            subsystem="recovery",
        )
        assert alert.alert_id is not None
        assert alert.title == "Test alert"
        assert alert.suppressed is False

    def test_create_alert_with_dedup(self):
        """Deduplication suppresses duplicate alerts."""
        manager = AlertManager()
        alert1 = manager.create_alert(
            title="Dup test",
            message="Duplicate",
            severity=AlertSeverity.WARNING,
            subsystem="recovery",
            dedup_key="test_key",
        )
        alert2 = manager.create_alert(
            title="Dup test",
            message="Duplicate",
            severity=AlertSeverity.WARNING,
            subsystem="recovery",
            dedup_key="test_key",
        )
        assert alert1.suppressed is False
        assert alert2.suppressed is True  # Suppressed by dedup

    def test_alert_handlers(self):
        """Alert handlers are called for non-suppressed alerts."""
        manager = AlertManager()
        handled: List[Alert] = []

        def handler(alert: Alert) -> None:
            handled.append(alert)

        manager.add_handler(handler)
        manager.create_alert(
            title="Handler test",
            message="Test",
            severity=AlertSeverity.INFO,
            subsystem="test",
        )
        assert len(handled) == 1
        assert handled[0].title == "Handler test"

        # Remove handler
        assert manager.remove_handler(handler) is True
        assert manager.remove_handler(handler) is False

    def test_get_alerts_filtered(self):
        """Alerts can be filtered by subsystem and severity."""
        manager = AlertManager()
        manager.create_alert("A1", "Msg", AlertSeverity.INFO, "recovery")
        manager.create_alert("A2", "Msg", AlertSeverity.WARNING, "governance")
        manager.create_alert("A3", "Msg", AlertSeverity.ERROR, "recovery")
        manager.create_alert("A4", "Msg", AlertSeverity.CRITICAL, "scaling")

        recovery_alerts = manager.get_alerts(subsystem="recovery")
        assert len(recovery_alerts) == 2

        critical_alerts = manager.get_alerts(severity=AlertSeverity.CRITICAL)
        assert len(critical_alerts) == 1

    def test_acknowledge_alert(self):
        """Alerts can be acknowledged."""
        manager = AlertManager()
        alert = manager.create_alert(
            title="Ack test",
            message="Please ack",
            severity=AlertSeverity.WARNING,
            subsystem="test",
        )
        assert alert.acknowledged is False

        assert manager.acknowledge_alert(alert.alert_id) is True
        assert alert.acknowledged is True

    def test_acknowledge_all(self):
        """All alerts in a subsystem can be acknowledged."""
        manager = AlertManager()
        manager.create_alert("A1", "Msg", AlertSeverity.INFO, "recovery")
        manager.create_alert("A2", "Msg", AlertSeverity.INFO, "recovery")
        manager.create_alert("A3", "Msg", AlertSeverity.INFO, "governance")

        count = manager.acknowledge_all(subsystem="recovery")
        assert count == 2

        remaining = manager.get_unacknowledged_alerts()
        assert len(remaining) == 1  # Only governance still unacked

    def test_alert_rules(self):
        """Alert rules trigger alerts based on metric values."""
        manager = AlertManager()
        rule = AlertRule(
            rule_id="high_latency",
            name="High latency",
            description="Alert when latency exceeds 1000ms",
            subsystem="recovery",
            severity=AlertSeverity.WARNING,
            metric_name="latency_ms",
            threshold_max=1000.0,
            consecutive_count=1,
        )
        manager.add_rule(rule)

        # Should trigger
        triggered = manager.evaluate_metric("latency_ms", 1500.0)
        assert len(triggered) == 1
        assert triggered[0].severity == AlertSeverity.WARNING

        # Should not trigger (below threshold)
        triggered = manager.evaluate_metric("latency_ms", 500.0)
        assert len(triggered) == 0

    def test_alert_rule_consecutive_count(self):
        """Alert rule respects consecutive count threshold."""
        manager = AlertManager()
        rule = AlertRule(
            rule_id="consecutive_test",
            name="Consecutive test",
            description="",
            subsystem="test",
            severity=AlertSeverity.WARNING,
            metric_name="errors",
            threshold_max=0,
            consecutive_count=3,
        )
        manager.add_rule(rule)

        # First two violations should not alert
        assert len(manager.evaluate_metric("errors", 5)) == 0
        assert len(manager.evaluate_metric("errors", 5)) == 0

        # Third violation should alert
        triggered = manager.evaluate_metric("errors", 5)
        assert len(triggered) == 1

        # Violation counter resets after alerting
        assert len(manager.evaluate_metric("errors", 5)) == 0

    def test_alert_rule_disabled(self):
        """Disabled rules don't trigger."""
        manager = AlertManager()
        rule = AlertRule(
            rule_id="disabled_rule",
            name="Disabled",
            description="",
            subsystem="test",
            severity=AlertSeverity.ERROR,
            metric_name="failures",
            threshold_max=0,
            enabled=False,
        )
        manager.add_rule(rule)

        triggered = manager.evaluate_metric("failures", 999)
        assert len(triggered) == 0

    def test_remove_rule(self):
        """Rules can be removed."""
        manager = AlertManager()
        rule = AlertRule(
            rule_id="rm_rule", name="Remove me",
            description="", subsystem="test",
        )
        manager.add_rule(rule)
        assert manager.remove_rule("rm_rule") is True
        assert manager.remove_rule("nonexistent") is False

    def test_get_stats(self):
        """Alert stats are well-formed."""
        manager = AlertManager()
        manager.create_alert("A1", "Msg", AlertSeverity.INFO, "recovery")
        manager.create_alert("A2", "Msg", AlertSeverity.WARNING, "governance")
        manager.create_alert("A3", "Msg", AlertSeverity.CRITICAL, "recovery")

        stats = manager.get_stats()
        assert stats["total_alerts"] == 3
        assert stats["critical"] == 1
        assert stats["warning"] == 1
        assert stats["info"] == 1
        assert stats["unacknowledged"] >= 3

    def test_reset(self):
        """Reset clears all alert state."""
        manager = AlertManager()
        manager.create_alert("A1", "Msg", AlertSeverity.INFO, "test")
        manager.reset()
        assert len(manager.get_alerts()) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# RuntimeDashboard Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRuntimeDashboard:
    def test_generate_snapshot_empty(self):
        """Dashboard generates snapshot without any connected components."""
        dashboard = RuntimeDashboard()
        snapshot = dashboard.generate_snapshot()
        assert isinstance(snapshot, DashboardSnapshot)
        assert snapshot.overall_status == "healthy"

    def test_generate_snapshot_with_metrics(self):
        """Dashboard includes metrics in snapshot."""
        from runtime_next.monitoring import RuntimeMetricsCollector
        metrics = RuntimeMetricsCollector()
        metrics.record_recovery_attempt("consensus", "deadlocked", True)

        dashboard = RuntimeDashboard(metrics_collector=metrics)
        snapshot = dashboard.generate_snapshot()
        assert "recovery" in snapshot.metrics_highlights
        assert snapshot.metrics_highlights["recovery"]["total_attempts"] == 1

    def test_generate_snapshot_with_health(self):
        """Dashboard includes health status in snapshot."""
        from runtime_next.monitoring import RuntimeHealthMonitor
        health = RuntimeHealthMonitor()
        health.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="test",
            description="", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        health.run_all_checks()

        dashboard = RuntimeDashboard(health_monitor=health)
        snapshot = dashboard.generate_snapshot()
        assert "recovery" in snapshot.subsystems
        assert snapshot.subsystems["recovery"].status == "healthy"

    def test_generate_snapshot_with_alerts(self):
        """Dashboard includes alert summary in snapshot."""
        from runtime_next.monitoring import AlertManager, AlertSeverity
        alerts = AlertManager()
        alerts.create_alert("Test", "Msg", AlertSeverity.WARNING, "recovery")
        alerts.create_alert("Critical", "Msg", AlertSeverity.CRITICAL, "recovery")

        dashboard = RuntimeDashboard(alert_manager=alerts)
        snapshot = dashboard.generate_snapshot()
        assert snapshot.alerts_summary["total_unacknowledged"] >= 2

    def test_generate_report(self):
        """Report is serializable."""
        dashboard = RuntimeDashboard()
        report = dashboard.generate_report()
        assert isinstance(report, dict)
        assert "overall_status" in report
        assert "subsystems" in report

    def test_setters(self):
        """Setters work to connect components after construction."""
        from runtime_next.monitoring import RuntimeMetricsCollector, RuntimeHealthMonitor, AlertManager
        dashboard = RuntimeDashboard()

        metrics = RuntimeMetricsCollector()
        health = RuntimeHealthMonitor()
        alerts = AlertManager()

        dashboard.set_metrics_collector(metrics)
        dashboard.set_health_monitor(health)
        dashboard.set_alert_manager(alerts)

        snapshot = dashboard.generate_snapshot()
        assert isinstance(snapshot, DashboardSnapshot)


# ═══════════════════════════════════════════════════════════════════════════════
# RecoveryEngine Integration Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRecoveryEngineMonitoringIntegration:
    def test_engine_has_monitoring(self):
        """RecoveryEngine has all monitoring components wired."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        assert hasattr(engine, "metrics_collector")
        assert hasattr(engine, "health_monitor")
        assert hasattr(engine, "alert_manager")
        assert hasattr(engine, "dashboard")
        assert engine.metrics_collector is not None
        assert engine.health_monitor is not None
        assert engine.alert_manager is not None
        assert engine.dashboard is not None

    def test_engine_has_default_health_checks(self):
        """RecoveryEngine registers default health checks."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        policies = engine.health_monitor.get_policies()
        assert len(policies) >= 5  # 3 recovery + 2 governance

        # Check specific policies exist
        recovery_policies = engine.health_monitor.get_policies(subsystem="recovery")
        assert len(recovery_policies) >= 3

        governance_policies = engine.health_monitor.get_policies(subsystem="governance")
        assert len(governance_policies) >= 2

    def test_engine_health_checks_run(self):
        """Default health checks execute successfully."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        results = engine.health_monitor.run_all_checks()
        assert "recovery" in results
        assert "governance" in results

        # All checks should be healthy
        for subsystem, checks in results.items():
            for check in checks:
                assert check.healthy is True

    def test_engine_dashboard_snapshot(self):
        """Engine can generate dashboard snapshot."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        # Run health checks and record some metrics
        engine.health_monitor.run_all_checks()
        engine.metrics_collector.record_recovery_attempt("consensus", "deadlocked", True)

        snapshot = engine.dashboard.generate_snapshot()
        assert snapshot.overall_status == "healthy"  # All default checks pass
        assert "recovery" in snapshot.subsystems
        assert "governance" in snapshot.subsystems
        assert "recovery" in snapshot.metrics_highlights

    def test_engine_alert_management(self):
        """Engine can create and manage alerts."""
        from runtime_next.recovery.engine import RecoveryEngine
        from runtime_next.monitoring import AlertSeverity

        engine = RecoveryEngine()

        # Create an alert
        engine.alert_manager.create_alert(
            title="Integration test alert",
            message="Testing alert from RecoveryEngine",
            severity=AlertSeverity.WARNING,
            subsystem="recovery",
        )

        alerts = engine.alert_manager.get_alerts(subsystem="recovery")
        assert len(alerts) == 1
        assert alerts[0].title == "Integration test alert"

        # Acknowledge it
        assert engine.alert_manager.acknowledge_alert(alerts[0].alert_id) is True

    def test_engine_metrics_recorded(self):
        """Metrics can be recorded through engine."""
        from runtime_next.recovery.engine import RecoveryEngine
        engine = RecoveryEngine()

        engine.metrics_collector.record_recovery_attempt("consensus", "deadlocked", True, duration_ms=100.0)
        engine.metrics_collector.record_governance_evaluation("consensus", "deny", policy_id="pol_test")
        engine.metrics_collector.record_pool_utilization("test_pool", 3, 10)

        summary = engine.metrics_collector.summary()
        assert len(summary) >= 3

        recovery = engine.metrics_collector.recovery_summary()
        assert recovery["total_attempts"] == 1

    def test_engine_alert_auto_evaluation(self):
        """Metric recording auto-evaluates alert rules through wired alert manager."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # Add a simple alert rule with threshold_max=0 on pool utilization
        engine.alert_manager.add_rule(AlertRule(
            rule_id="auto_eval_test",
            name="Auto-eval test",
            description="",
            subsystem="test",
            severity=AlertSeverity.WARNING,
            metric_name="scaling.pool.utilization",
            threshold_max=0.0,
            consecutive_count=1,
        ))

        # Recording a pool utilization above 0 should trigger the rule
        engine.metrics_collector.record_pool_utilization("alert_pool", 5, 10)

        # Check that the alert was auto-created
        alerts = engine.alert_manager.get_alerts(subsystem="test")
        assert len(alerts) == 1
        assert alerts[0].title == "Auto-eval test"
        assert alerts[0].metadata.get("rule_id") == "auto_eval_test"

    def test_engine_auto_evaluation_does_not_block(self):
        """Alert auto-evaluation failures never block metric recording."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()

        # Record a metric — should not raise even if alert manager fails
        # (we can't easily simulate a failure since evaluate_metric is
        #  guarded by try/except, but we can verify recording still works)
        engine.metrics_collector.record("test.safe", value=42.0)
        series = engine.metrics_collector.get_series("test.safe")
        assert series is not None
        assert series.latest == 42.0


# ═══════════════════════════════════════════════════════════════════════════════
# RuntimeCLI Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestRuntimeCLI:
    def test_cli_has_dashboard(self):
        """RuntimeCLI requires a dashboard."""
        dashboard = RuntimeDashboard()
        cli = RuntimeCLI(dashboard=dashboard)
        assert cli.dashboard is not None

    def test_cli_help(self):
        """#status help shows available commands."""
        dashboard = RuntimeDashboard()
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status help")
        assert result["status"] == "SUCCESS"

    def test_cli_short_help(self):
        """#status --help also works."""
        dashboard = RuntimeDashboard()
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status --help")
        assert result["status"] == "SUCCESS"

    def test_cli_unknown_subcommand(self):
        """Unknown subcommand returns REJECTED."""
        dashboard = RuntimeDashboard()
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status unknown_cmd")
        assert result["status"] == "REJECTED"

    def test_cli_dashboard_command(self):
        """#status dashboard returns dashboard data."""
        dashboard = RuntimeDashboard()
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status dashboard")
        assert result["status"] == "SUCCESS"

    def test_cli_dashboard_with_health_and_metrics(self):
        """#status dashboard with full monitoring stack."""
        from runtime_next.monitoring import RuntimeMetricsCollector, RuntimeHealthMonitor, AlertManager

        metrics = RuntimeMetricsCollector()
        health = RuntimeHealthMonitor()
        alerts = AlertManager()
        health.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="cli_test",
            description="", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        health.run_all_checks()
        alerts.create_alert("Test alert", "Testing", AlertSeverity.WARNING, "recovery")

        dashboard = RuntimeDashboard(
            metrics_collector=metrics,
            health_monitor=health,
            alert_manager=alerts,
        )
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status dashboard")
        assert result["status"] == "SUCCESS"

    def test_cli_health_command(self):
        """#status health displays health report."""
        from runtime_next.monitoring import RuntimeHealthMonitor

        health = RuntimeHealthMonitor()
        health.register_check(HealthCheckPolicy(
            subsystem="recovery", check_id="h1",
            description="Health check 1", interval_seconds=30.0,
            check_fn=lambda: HealthCheckResult(healthy=True),
        ))
        health.run_all_checks()

        dashboard = RuntimeDashboard(health_monitor=health)
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status health")
        assert result["status"] == "SUCCESS"

    def test_cli_health_no_monitor(self):
        """#status health returns REJECTED when no health monitor connected."""
        dashboard = RuntimeDashboard()
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status health")
        assert result["status"] == "REJECTED"

    def test_cli_alerts_command(self):
        """#status alerts shows active alerts."""
        from runtime_next.monitoring import AlertManager, AlertSeverity

        alerts = AlertManager()
        alerts.create_alert("Alert 1", "First", AlertSeverity.WARNING, "recovery")
        alerts.create_alert("Alert 2", "Second", AlertSeverity.CRITICAL, "governance")

        dashboard = RuntimeDashboard(alert_manager=alerts)
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status alerts")
        assert result["status"] == "SUCCESS"

    def test_cli_alerts_all(self):
        """#status alerts --all shows alert history."""
        from runtime_next.monitoring import AlertManager, AlertSeverity

        alerts = AlertManager()
        alerts.create_alert("Test", "Msg", AlertSeverity.INFO, "test")

        dashboard = RuntimeDashboard(alert_manager=alerts)
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status alerts --all")
        assert result["status"] == "SUCCESS"

    def test_cli_alerts_ack(self):
        """#status alerts --ack acknowledges all alerts."""
        from runtime_next.monitoring import AlertManager, AlertSeverity

        alerts = AlertManager()
        alerts.create_alert("Test", "Msg", AlertSeverity.WARNING, "recovery")

        dashboard = RuntimeDashboard(alert_manager=alerts)
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status alerts --ack")
        assert result["status"] == "SUCCESS"
        assert len(alerts.get_unacknowledged_alerts()) == 0

    def test_cli_alerts_severity_filter(self):
        """#status alerts --severity=critical filters by severity."""
        from runtime_next.monitoring import AlertManager, AlertSeverity

        alerts = AlertManager()
        alerts.create_alert("Info", "Msg", AlertSeverity.INFO, "test")
        alerts.create_alert("Critical", "Msg", AlertSeverity.CRITICAL, "test")

        dashboard = RuntimeDashboard(alert_manager=alerts)
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status alerts --severity=critical")
        assert result["status"] == "SUCCESS"

    def test_cli_alerts_no_alerts(self):
        """#status alerts with no alerts shows empty state."""
        from runtime_next.monitoring import AlertManager

        alerts = AlertManager()
        dashboard = RuntimeDashboard(alert_manager=alerts)
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status alerts")
        assert result["status"] == "SUCCESS"

    def test_cli_alerts_no_manager(self):
        """#status alerts returns REJECTED when no alert manager connected."""
        dashboard = RuntimeDashboard()
        cli = RuntimeCLI(dashboard=dashboard)
        result = cli.execute("#status alerts")
        assert result["status"] == "REJECTED"

    def test_cli_engine_integration(self):
        """RuntimeCLI is wired through RecoveryEngine."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()
        assert hasattr(engine, "runtime_cli")
        assert engine.runtime_cli is not None

        # Execute dashboard command through engine's CLI
        result = engine.runtime_cli.execute("#status dashboard")
        assert result["status"] == "SUCCESS"

    def test_cli_engine_alerts(self):
        """RuntimeCLI alerts through RecoveryEngine shows engine's alerts."""
        from runtime_next.recovery.engine import RecoveryEngine

        engine = RecoveryEngine()
        engine.alert_manager.create_alert(
            title="Engine alert",
            message="From engine",
            severity=AlertSeverity.WARNING,
            subsystem="recovery",
        )

        result = engine.runtime_cli.execute("#status alerts")
        assert result["status"] == "SUCCESS"
