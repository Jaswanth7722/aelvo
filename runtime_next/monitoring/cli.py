"""Runtime CLI — interactive commands for viewing runtime monitoring data.

Provides #status subcommands that display:
- `#status dashboard` — Runtime dashboard snapshot (subsystem health, metrics, alerts)
- `#status health`   — Detailed health report for all monitored subsystems
- `#status alerts`   — Active (unacknowledged) alerts with severity breakdown

Follows the same pattern as MCPCommandLineInterface for consistency.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

from .dashboard import RuntimeDashboard, DashboardSnapshot
from .health import RuntimeHealthMonitor, HealthStatus
from .alerting import AlertManager, AlertSeverity, Alert

log = logging.getLogger("aelvo.runtime.monitoring.cli")


class RuntimeCLI:
    """Handles parsing and display of #status monitoring commands.

    Requires a RuntimeDashboard instance (which in turn connects to the
    metrics collector, health monitor, and alert manager).

    Usage:
        cli = RuntimeCLI(dashboard=engine.dashboard)
        result = await cli.execute("#status dashboard")
    """

    def __init__(self, dashboard: RuntimeDashboard):
        self.dashboard = dashboard

    def execute(self, command_line: str) -> Dict[str, Any]:
        """Parse and run a #status command.

        Args:
            command_line: The full command string (e.g., "#status dashboard").

        Returns:
            Dict with status and message.
        """
        parts = command_line.strip().split()
        if len(parts) < 2:
            return self._show_help()

        subcmd = parts[1].lower()

        try:
            if subcmd in ("dashboard", "dash", "status"):
                return self._cmd_dashboard()
            elif subcmd in ("health", "health-report", "hr"):
                return self._cmd_health()
            elif subcmd in ("alerts", "alert", "al"):
                return self._cmd_alerts(parts[2:])
            elif subcmd in ("help", "--help", "-h"):
                return self._show_help()
            else:
                return {"status": "REJECTED", "msg": f"Unknown #status subcommand: {subcmd}. Use: dashboard, health, alerts"}
        except Exception as e:
            log.exception("Error executing #status command: %s", command_line)
            return {"status": "REJECTED", "error": str(e)}

    # ── Dashboard Command ───────────────────────────────────────────────

    def _cmd_dashboard(self) -> Dict[str, Any]:
        """Display a formatted runtime dashboard snapshot."""
        snapshot = self.dashboard.generate_snapshot()

        print()
        print("=" * 72)
        print("  RUNTIME DASHBOARD")
        print("=" * 72)
        print(f"  Overall Status:  {self._status_badge(snapshot.overall_status)}")
        print(f"  Generated:       {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(snapshot.timestamp))}")
        print(f"  Generated in:    {snapshot.generation_duration_ms:.1f}ms")
        print()

        # Subsystem table
        print(f"  {'Subsystem':20s} | {'Status':12s} | {'Checks':10s} | {'Alerts'}")
        print(f"  {'-'*20}-+-{'-'*12}-+-{'-'*10}-+-{'------'}")
        for name in ["recovery", "governance", "scaling"]:
            sh = snapshot.subsystems.get(name)
            if sh:
                checks_str = f"{sh.checks_passing}/{sh.total_checks} pass"
                alerts_str = str(sh.active_alerts) if sh.active_alerts > 0 else "0"
                print(f"  {name:20s} | {self._status_badge(sh.status):12s} | {checks_str:10s} | {alerts_str}")
        print()

        # Metrics highlights
        mh = snapshot.metrics_highlights
        if mh:
            print("  ── Metrics Highlights ──")
            if "recovery" in mh:
                r = mh["recovery"]
                print(f"  Recovery:   {r.get('total_attempts', 0)} attempts, "
                      f"{r.get('total_successes', 0)} successes, "
                      f"rate={r.get('success_rate', 0):.0%}")
            if "governance" in mh:
                g = mh["governance"]
                print(f"  Governance: {g.get('total_evaluations', 0)} evaluations, "
                      f"{g.get('denied_count', 0)} denied, "
                      f"{g.get('approval_required_count', 0)} pending approval")
            if "scaling" in mh:
                s = mh["scaling"]
                print(f"  Scaling:    pool util={s.get('pool_utilization_avg', 0):.0%}, "
                      f"{s.get('completed_batches', 0)} batches completed")
            print()

        # Alert summary
        al = snapshot.alerts_summary
        if al:
            critical = al.get("critical", 0)
            warning = al.get("warning", 0)
            if critical > 0:
                print(f"  ⚠  Alerts: {al.get('total_unacknowledged', 0)} unacknowledged "
                      f"({critical} critical, {warning} warning)")
            else:
                print(f"  Alerts: {al.get('total_unacknowledged', 0)} unacknowledged "
                      f"({critical} critical, {warning} warning)")
            print()

        print("=" * 72)
        print()

        return {"status": "SUCCESS", "msg": "Dashboard displayed"}

    # ── Health Command ──────────────────────────────────────────────────

    def _cmd_health(self) -> Dict[str, Any]:
        """Display detailed health report."""
        health_monitor = self.dashboard.health_monitor
        if not health_monitor:
            return {"status": "REJECTED", "msg": "Health monitor not connected to dashboard"}

        report = health_monitor.generate_health_report()

        print()
        print("=" * 72)
        print("  HEALTH REPORT")
        print("=" * 72)
        print(f"  Overall: {self._status_badge(report.get('overall_status', 'unknown'))}")
        print()

        for sub_name, sub_data in sorted(report.get("subsystems", {}).items()):
            print(f"  ── {sub_name.upper():20s} ({self._status_badge(sub_data.get('status', 'unknown'))}) ──")
            for check_id, check_info in sub_data.get("checks", {}).items():
                status = check_info.get("status", "unknown")
                label = self._status_badge(status)
                failures = check_info.get("consecutive_failures", 0)
                failure_str = f" ({failures} consecutive failures)" if failures > 0 else ""
                desc = check_info.get("description", "")
                print(f"    {label} {check_id:25s} {desc}{failure_str}")
            print()

        print("=" * 72)
        print()

        return {"status": "SUCCESS", "msg": "Health report displayed"}

    # ── Alerts Command ──────────────────────────────────────────────────

    def _cmd_alerts(self, args: List[str]) -> Dict[str, Any]:
        """Display active alerts, optionally filtered by severity."""
        # Parse optional --severity flag
        severity_filter = None
        subsystem_filter = None
        show_all = False
        acknowledge = False
        for arg in args:
            if arg.startswith("--severity="):
                sev = arg.split("=")[1].lower()
                if sev in ("info", "warning", "error", "critical"):
                    severity_filter = AlertSeverity(sev)
            elif arg.startswith("--subsystem="):
                subsystem_filter = arg.split("=")[1].lower()
            elif arg == "--all":
                show_all = True
            elif arg == "--ack":
                acknowledge = True

        alert_manager = self.dashboard.alert_manager
        if not alert_manager:
            return {"status": "REJECTED", "msg": "Alert manager not connected to dashboard"}

        if acknowledge:
            count = alert_manager.acknowledge_all(subsystem=subsystem_filter)
            print(f"\n  Acknowledged {count} alert(s).\n")
            return {"status": "SUCCESS", "msg": f"Acknowledged {count} alert(s)"}

        if show_all:
            alerts = alert_manager.get_alerts(
                subsystem=subsystem_filter,
                severity=severity_filter,
                limit=50,
                include_suppressed=False,
            )
        else:
            alerts = alert_manager.get_unacknowledged_alerts(
                subsystem=subsystem_filter,
            )
            if severity_filter:
                alerts = [a for a in alerts if a.severity == severity_filter]

        print()
        print("=" * 72)
        if show_all:
            print("  ALERT HISTORY (last 50)")
        else:
            print(f"  ACTIVE ALERTS ({len(alerts)} unacknowledged)")
        print("=" * 72)

        if not alerts:
            print("  No alerts to display.")
        else:
            for i, alert in enumerate(alerts, 1):
                badge = self._severity_badge(alert.severity)
                timestamp = time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(alert.timestamp),
                )
                acked = " (acknowledged)" if alert.acknowledged else ""
                print(f"\n  {i:2d}. {badge} [{alert.severity.value.upper():8s}] {alert.title}{acked}")
                print(f"      Subsystem: {alert.subsystem:12s}  Source: {alert.source or 'N/A':20s}  Time: {timestamp}")
                if alert.message:
                    print(f"      Message: {alert.message[:120]}")
                if alert.metadata:
                    meta_str = json.dumps(alert.metadata, default=str)[:100]
                    print(f"      Meta:    {meta_str}")
            print()

        print("=" * 72)
        print()

        return {"status": "SUCCESS", "msg": f"Displayed {len(alerts)} alert(s)"}

    # ── Help ────────────────────────────────────────────────────────────

    def _show_help(self) -> Dict[str, Any]:
        print()
        print("=" * 60)
        print("  Runtime Status Commands")
        print("=" * 60)
        print("  #status dashboard      — Full runtime dashboard snapshot")
        print("  #status health         — Detailed health report per subsystem")
        print("  #status alerts         — Unacknowledged alerts")
        print("  #status alerts --all   — Alert history (last 50)")
        print("  #status alerts --severity=critical — Filter by severity")
        print("  #status alerts --subsystem=recovery — Filter by subsystem")
        print("  #status alerts --ack   — Acknowledge all alerts")
        print("=" * 60)
        print()
        return {"status": "SUCCESS", "msg": "Help displayed"}

    # ── Formatters ──────────────────────────────────────────────────────

    @staticmethod
    def _status_badge(status: str) -> str:
        badges = {
            "healthy": "✅ HEALTHY",
            "degraded": "⚠️  DEGRADED",
            "unhealthy": "❌ UNHEALTHY",
            "unknown": "❓ UNKNOWN",
        }
        return badges.get(status.lower(), status)

    @staticmethod
    def _severity_badge(severity: AlertSeverity) -> str:
        badges = {
            AlertSeverity.CRITICAL: "🔴",
            AlertSeverity.ERROR: "🟠",
            AlertSeverity.WARNING: "🟡",
            AlertSeverity.INFO: "🔵",
        }
        return badges.get(severity, "⚪")
