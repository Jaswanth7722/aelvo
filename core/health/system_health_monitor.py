"""core/health/system_health_monitor.py — System Health & Autonomous Healing

Phase 13: Centralized system health monitoring that aggregates signals
from all subsystems (database, filesystem, event bus, providers, specialists,
memory) and provides autonomous healing actions.

Key components:
  - ComponentHealth: Per-component health data
  - ComponentStatus: Enum of possible health statuses
  - HealActionResult: Result of a healing action
  - SystemHealthReport: Complete aggregated report
  - SystemHealthMonitor: Centralized monitor orchestrating checks and healing
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("aelvo.core.health")


class ComponentStatus(str, Enum):
    """Health status of a system component."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class ComponentHealth:
    """Health data for a single system component."""

    component_id: str
    """Unique identifier for this component (e.g., 'memory_db', 'event_bus')."""

    component_type: str
    """Type of component (e.g., 'database', 'filesystem', 'provider', 'specialist')."""

    status: ComponentStatus = ComponentStatus.UNKNOWN
    """Current health status."""

    score: float = 1.0
    """Health score from 0.0 (unhealthy) to 1.0 (healthy)."""

    latency_ms: float = 0.0
    """Optional latency measurement for this component."""

    error: Optional[str] = None
    """Optional error message if the component is unhealthy."""

    detail: str = ""
    """Human-readable detail about the component's health."""

    last_checked: float = 0.0
    """Timestamp of the last health check."""

    consecutive_failures: int = 0
    """Number of consecutive failed health checks."""

    tags: Dict[str, str] = field(default_factory=dict)
    """Tags for categorization."""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component_id": self.component_id,
            "component_type": self.component_type,
            "status": self.status.value,
            "score": self.score,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "detail": self.detail,
            "last_checked": self.last_checked,
            "consecutive_failures": self.consecutive_failures,
            "tags": self.tags,
        }


@dataclass
class HealActionResult:
    """Result of an autonomous healing action."""

    component_id: str
    """Which component the action targeted."""

    action_name: str
    """Name of the healing action (e.g., 'restart_event_bus', 'clear_circuit_breakers')."""

    success: bool
    """Whether the healing action succeeded."""

    message: str = ""
    """Human-readable result message."""

    duration_ms: float = 0.0
    """How long the healing action took."""

    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "component_id": self.component_id,
            "action_name": self.action_name,
            "success": self.success,
            "message": self.message,
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp,
        }


@dataclass
class SystemHealthReport:
    """Complete system health report with aggregated metrics and recommendations."""

    overall_status: ComponentStatus = ComponentStatus.UNKNOWN
    """Overall system health status."""

    overall_score: float = 1.0
    """Overall health score from 0.0 (unhealthy) to 1.0 (healthy)."""

    component_count: int = 0
    """Total number of monitored components."""

    healthy_count: int = 0
    """Number of healthy components."""

    degraded_count: int = 0
    """Number of degraded components."""

    unhealthy_count: int = 0
    """Number of unhealthy components."""

    components: List[ComponentHealth] = field(default_factory=list)
    """Health data for all monitored components."""

    healing_actions: List[HealActionResult] = field(default_factory=list)
    """Recent healing actions taken."""

    recommendations: List[str] = field(default_factory=list)
    """Actionable recommendations based on health data."""

    generated_at: float = field(default_factory=time.time)
    """When this report was generated."""

    checks_duration_ms: float = 0.0
    """How long the health checks took to run."""

    @property
    def is_healthy(self) -> bool:
        """Quick check if the overall system is healthy."""
        return self.overall_status == ComponentStatus.HEALTHY

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_status": self.overall_status.value,
            "overall_score": round(self.overall_score, 4),
            "component_count": self.component_count,
            "healthy_count": self.healthy_count,
            "degraded_count": self.degraded_count,
            "unhealthy_count": self.unhealthy_count,
            "components": [c.to_dict() for c in self.components],
            "healing_actions": [a.to_dict() for a in self.healing_actions[-10:]],
            "recommendations": self.recommendations,
            "generated_at": self.generated_at,
            "checks_duration_ms": round(self.checks_duration_ms, 2),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        status_icon = {
            ComponentStatus.HEALTHY: "🟢",
            ComponentStatus.DEGRADED: "🟡",
            ComponentStatus.UNHEALTHY: "🔴",
            ComponentStatus.UNKNOWN: "⚪",
        }.get(self.overall_status, "❓")

        lines = [
            f"  ── SYSTEM HEALTH REPORT [{datetime.fromtimestamp(self.generated_at).strftime('%H:%M:%S')}] ──",
            f"  Overall: {status_icon} {self.overall_status.value.upper()} "
            f"(score: {round(self.overall_score * 100)}%)",
            f"  Components: {self.component_count} total, "
            f"{self.healthy_count} healthy, "
            f"{self.degraded_count} degraded, "
            f"{self.unhealthy_count} unhealthy",
            f"  Check duration: {self.checks_duration_ms:.0f}ms",
            "",
        ]

        if self.components:
            lines.append("  Components:")
            by_status: Dict[ComponentStatus, List[ComponentHealth]] = {}
            for c in self.components:
                by_status.setdefault(c.status, []).append(c)

            for status in (ComponentStatus.UNHEALTHY, ComponentStatus.DEGRADED, ComponentStatus.HEALTHY, ComponentStatus.UNKNOWN):
                comps = by_status.get(status, [])
                if not comps:
                    continue
                icon = {ComponentStatus.HEALTHY: "✓", ComponentStatus.DEGRADED: "⚠", ComponentStatus.UNHEALTHY: "✗", ComponentStatus.UNKNOWN: "?"}[status]
                for c in comps:
                    score_pct = round(c.score * 100)
                    lines.append(f"    {icon} [{c.component_type}] {c.component_id}: {score_pct}%")
                    if c.detail:
                        lines.append(f"       {c.detail}")
                    if c.error:
                        lines.append(f"       Error: {c.error[:120]}")

        if self.healing_actions:
            lines.append("")
            lines.append("  Healing Actions:")
            for a in self.healing_actions[-5:]:
                icon = "✓" if a.success else "✗"
                lines.append(f"    {icon} {a.action_name} on {a.component_id}: {a.message[:80]}")

        if self.recommendations:
            lines.append("")
            lines.append("  Recommendations:")
            for r in self.recommendations:
                lines.append(f"    → {r}")

        lines.append("  ── ── ── ── ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)


# ============================================================================
# Health Check Types
# ============================================================================


class HealthCheck:
    """A registered health check that can be run against a component."""

    def __init__(
        self,
        component_id: str,
        component_type: str,
        check_fn: Callable[[], Any],
        timeout: float = 10.0,
        tags: Optional[Dict[str, str]] = None,
    ):
        self.component_id = component_id
        self.component_type = component_type
        self.check_fn = check_fn
        self.timeout = timeout
        self.tags = tags or {}

    async def run(self) -> ComponentHealth:
        """Run the health check and return ComponentHealth."""
        start = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                self._run_check(),
                timeout=self.timeout,
            )
            elapsed = (time.perf_counter() - start) * 1000

            if isinstance(result, ComponentHealth):
                result.latency_ms = elapsed
                result.last_checked = time.time()
                return result

            if isinstance(result, bool):
                if result:
                    return ComponentHealth(
                        component_id=self.component_id,
                        component_type=self.component_type,
                        status=ComponentStatus.HEALTHY,
                        score=1.0,
                        latency_ms=elapsed,
                        last_checked=time.time(),
                        detail="Health check passed",
                        tags=self.tags,
                    )
                else:
                    return ComponentHealth(
                        component_id=self.component_id,
                        component_type=self.component_type,
                        status=ComponentStatus.UNHEALTHY,
                        score=0.0,
                        latency_ms=elapsed,
                        last_checked=time.time(),
                        detail="Health check failed",
                        error="Check returned False",
                        tags=self.tags,
                    )

            # Treat any truthy result as healthy
            return ComponentHealth(
                component_id=self.component_id,
                component_type=self.component_type,
                status=ComponentStatus.HEALTHY,
                score=1.0,
                latency_ms=elapsed,
                last_checked=time.time(),
                detail=str(result)[:200] if result else "Health check passed",
                tags=self.tags,
            )

        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            return ComponentHealth(
                component_id=self.component_id,
                component_type=self.component_type,
                status=ComponentStatus.UNHEALTHY,
                score=0.0,
                latency_ms=elapsed,
                last_checked=time.time(),
                detail=f"Health check timed out after {self.timeout}s",
                error=f"TimeoutError: exceeded {self.timeout}s",
                tags=self.tags,
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return ComponentHealth(
                component_id=self.component_id,
                component_type=self.component_type,
                status=ComponentStatus.UNHEALTHY,
                score=0.0,
                latency_ms=elapsed,
                last_checked=time.time(),
                detail=f"Health check raised: {e}",
                error=str(e)[:200],
                tags=self.tags,
            )

    async def _run_check(self) -> Any:
        """Run the check function, handling both sync and async."""
        if asyncio.iscoroutinefunction(self.check_fn):
            return await self.check_fn()
        return self.check_fn()


# ============================================================================
# Healing Action Types
# ============================================================================


class HealingAction:
    """A registered healing action that can be triggered autonomously."""

    def __init__(
        self,
        component_id: str,
        action_name: str,
        action_fn: Callable[[], Any],
        description: str = "",
        timeout: float = 30.0,
    ):
        self.component_id = component_id
        self.action_name = action_name
        self.action_fn = action_fn
        self.description = description or f"Heal {component_id}: {action_name}"
        self.timeout = timeout

    async def execute(self) -> HealActionResult:
        """Execute the healing action."""
        start = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                self._run_action(),
                timeout=self.timeout,
            )
            elapsed = (time.perf_counter() - start) * 1000

            if isinstance(result, HealActionResult):
                result.duration_ms = elapsed
                return result

            success = bool(result) if not isinstance(result, bool) else result
            return HealActionResult(
                component_id=self.component_id,
                action_name=self.action_name,
                success=success,
                message=self.description if success else f"{self.description} failed",
                duration_ms=elapsed,
            )
        except asyncio.TimeoutError:
            elapsed = (time.perf_counter() - start) * 1000
            return HealActionResult(
                component_id=self.component_id,
                action_name=self.action_name,
                success=False,
                message=f"Healing action timed out after {self.timeout}s",
                duration_ms=elapsed,
            )
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return HealActionResult(
                component_id=self.component_id,
                action_name=self.action_name,
                success=False,
                message=f"Healing action raised: {e}",
                duration_ms=elapsed,
            )

    async def _run_action(self) -> Any:
        """Run the action function, handling both sync and async."""
        if asyncio.iscoroutinefunction(self.action_fn):
            return await self.action_fn()
        return self.action_fn()


# ============================================================================
# SystemHealthMonitor
# ============================================================================


class SystemHealthMonitor:
    """Centralized system health monitor with autonomous healing.

    Aggregates health signals from all subsystems:
    - Database (memory.db, SQLite connectivity)
    - Filesystem (read/write access)
    - Event bus (running status)
    - Provider runtimes (health scores)
    - Specialists (circuit breaker states)
    - Memory collection (ChromaDB connectivity)

    Provides autonomous healing actions that can be triggered
    automatically or on demand.
    """

    def __init__(self):
        self._checks: Dict[str, HealthCheck] = {}
        self._healing_actions: Dict[str, List[HealingAction]] = {}
        self._component_history: Dict[str, List[ComponentHealth]] = {}
        self._recent_healing: List[HealActionResult] = []

        # Tracking
        self._max_history: int = 100
        self._auto_heal_threshold: int = 3  # consecutive failures before auto-heal
        self._auto_heal_enabled: bool = True

    # ── Registration ────────────────────────────────────────────────

    def register_check(
        self,
        component_id: str,
        component_type: str,
        check_fn: Callable[[], Any],
        timeout: float = 10.0,
        tags: Optional[Dict[str, str]] = None,
    ) -> HealthCheck:
        """Register a health check for a component.

        Args:
            component_id: Unique component identifier.
            component_type: Type of component.
            check_fn: Async or sync callable that returns bool or ComponentHealth.
            timeout: Timeout for the check in seconds.
            tags: Optional tags for categorization.

        Returns:
            The registered HealthCheck.
        """
        check = HealthCheck(
            component_id=component_id,
            component_type=component_type,
            check_fn=check_fn,
            timeout=timeout,
            tags=tags or {},
        )
        self._checks[component_id] = check
        return check

    def register_healing_action(
        self,
        component_id: str,
        action_name: str,
        action_fn: Callable[[], Any],
        description: str = "",
        timeout: float = 30.0,
    ) -> HealingAction:
        """Register a healing action for a component.

        Args:
            component_id: Which component this action targets.
            action_name: Name of the action.
            action_fn: Async or sync callable that returns bool or HealActionResult.
            description: Human-readable description.
            timeout: Timeout for the action in seconds.

        Returns:
            The registered HealingAction.
        """
        action = HealingAction(
            component_id=component_id,
            action_name=action_name,
            action_fn=action_fn,
            description=description,
            timeout=timeout,
        )
        self._healing_actions.setdefault(component_id, []).append(action)
        return action

    def unregister_check(self, component_id: str) -> bool:
        """Remove a registered health check."""
        return self._checks.pop(component_id, None) is not None

    # ── Health Checks ───────────────────────────────────────────────

    async def run_all_checks(self) -> List[ComponentHealth]:
        """Run all registered health checks in parallel.

        Returns:
            List of ComponentHealth results.
        """
        if not self._checks:
            return []

        tasks = [check.run() for check in self._checks.values()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        components: List[ComponentHealth] = []
        for result in results:
            if isinstance(result, ComponentHealth):
                components.append(result)
                # Track consecutive failures
                self._update_failure_tracking(result)
                # Store in history
                self._store_history(result)
            elif isinstance(result, Exception):
                log.warning("Health check raised exception: %s", result)

        return components

    async def run_check(self, component_id: str) -> Optional[ComponentHealth]:
        """Run a single health check by component ID."""
        check = self._checks.get(component_id)
        if check is None:
            return None
        result = await check.run()
        self._update_failure_tracking(result)
        self._store_history(result)
        return result

    def get_check(self, component_id: str) -> Optional[HealthCheck]:
        """Get a registered health check by component ID."""
        return self._checks.get(component_id)

    def get_all_checks(self) -> List[HealthCheck]:
        """Get all registered health checks."""
        return list(self._checks.values())

    # ── Healing ─────────────────────────────────────────────────────

    async def heal_component(self, component_id: str) -> List[HealActionResult]:
        """Run all healing actions registered for a component.

        Args:
            component_id: Which component to heal.

        Returns:
            List of HealActionResult.
        """
        actions = self._healing_actions.get(component_id, [])
        if not actions:
            return []

        results = []
        for action in actions:
            result = await action.execute()
            self._recent_healing.append(result)
            results.append(result)
            log.info(
                "Healing action '%s' on %s: %s (%dms)",
                action.action_name, component_id,
                "success" if result.success else "failed",
                result.duration_ms,
            )

        return results

    async def heal_all_degraded(self) -> Dict[str, List[HealActionResult]]:
        """Run healing actions for all unhealthy/degraded components.

        Only runs actions for components that have a registered health
        check AND healing action AND are currently degraded/unhealthy.

        Returns:
            Dict mapping component_id to list of HealActionResult.
        """
        if not self._auto_heal_enabled:
            return {}

        # First run all checks to get current state
        components = await self.run_all_checks()

        results: Dict[str, List[HealActionResult]] = {}
        for component in components:
            if component.status in (ComponentStatus.UNHEALTHY, ComponentStatus.DEGRADED):
                actions = await self.heal_component(component.component_id)
                if actions:
                    results[component.component_id] = actions

        return results

    async def heal_specific(
        self, component_id: str, action_name: str
    ) -> Optional[HealActionResult]:
        """Run a specific healing action by name on a component."""
        actions = self._healing_actions.get(component_id, [])
        for action in actions:
            if action.action_name == action_name:
                result = await action.execute()
                self._recent_healing.append(result)
                return result
        return None

    # ── Report Generation ───────────────────────────────────────────

    async def generate_report(
        self,
        include_healing_history: bool = True,
    ) -> SystemHealthReport:
        """Generate a complete system health report.

        Runs all registered health checks, aggregates results,
        and generates recommendations.

        Args:
            include_healing_history: Include recent healing actions.

        Returns:
            A SystemHealthReport with all aggregated data.
        """
        start = time.perf_counter()

        components = await self.run_all_checks()
        elapsed = (time.perf_counter() - start) * 1000

        healthy = [c for c in components if c.status == ComponentStatus.HEALTHY]
        degraded = [c for c in components if c.status == ComponentStatus.DEGRADED]
        unhealthy = [c for c in components if c.status == ComponentStatus.UNHEALTHY]

        # Compute overall score (weighted average)
        scores = [c.score for c in components] if components else [1.0]
        overall_score = sum(scores) / len(scores)

        # Determine overall status
        if unhealthy:
            overall_status = ComponentStatus.UNHEALTHY
        elif degraded:
            overall_status = ComponentStatus.DEGRADED
        elif all(c.status == ComponentStatus.HEALTHY for c in components) if components else True:
            overall_status = ComponentStatus.HEALTHY
        else:
            overall_status = ComponentStatus.UNKNOWN

        # Generate recommendations
        recommendations = self._generate_recommendations(components)

        report = SystemHealthReport(
            overall_status=overall_status,
            overall_score=overall_score,
            component_count=len(components),
            healthy_count=len(healthy),
            degraded_count=len(degraded),
            unhealthy_count=len(unhealthy),
            components=components,
            healing_actions=self._recent_healing if include_healing_history else [],
            recommendations=recommendations,
            generated_at=time.time(),
            checks_duration_ms=elapsed,
        )

        return report

    def _generate_recommendations(
        self, components: List[ComponentHealth],
    ) -> List[str]:
        """Generate actionable recommendations based on health data."""
        recommendations = []

        unhealthy_components = [
            c for c in components
            if c.status == ComponentStatus.UNHEALTHY
        ]
        degraded_components = [
            c for c in components
            if c.status == ComponentStatus.DEGRADED
        ]

        if unhealthy_components:
            names = ", ".join(c.component_id for c in unhealthy_components)
            recommendations.append(
                f"Unhealthy components detected: {names}. "
                f"Run heal_all_degraded() to attempt autonomous recovery."
            )

        if degraded_components:
            names = ", ".join(c.component_id for c in degraded_components)
            recommendations.append(
                f"Degraded components: {names}. "
                f"These may need attention soon."
            )

        # Check for components with many consecutive failures
        for c in components:
            if c.consecutive_failures >= 5:
                recommendations.append(
                    f"Component '{c.component_id}' has {c.consecutive_failures} "
                    f"consecutive failures. Consider manual inspection."
                )

        if not recommendations:
            recommendations.append("All systems healthy. No action needed.")

        return recommendations

    # ── Config ──────────────────────────────────────────────────────

    def enable_auto_heal(self, enabled: bool = True) -> None:
        """Enable or disable autonomous healing."""
        self._auto_heal_enabled = enabled

    def set_auto_heal_threshold(self, failures: int) -> None:
        """Set the consecutive failures threshold for auto-healing."""
        self._auto_heal_threshold = max(1, failures)

    # ── History ─────────────────────────────────────────────────────

    def get_component_history(
        self, component_id: str, limit: int = 20,
    ) -> List[ComponentHealth]:
        """Get health check history for a component."""
        history = self._component_history.get(component_id, [])
        return history[-limit:]

    def get_healing_history(self, limit: int = 20) -> List[HealActionResult]:
        """Get recent healing action results."""
        return self._recent_healing[-limit:]

    def clear_history(self) -> None:
        """Clear all health and healing history."""
        self._component_history.clear()
        self._recent_healing.clear()

    # ── Internal ────────────────────────────────────────────────────

    def _update_failure_tracking(self, component: ComponentHealth) -> None:
        """Update consecutive failure tracking for a component."""
        if component.status in (ComponentStatus.UNHEALTHY, ComponentStatus.DEGRADED):
            # Get previous count from history
            history = self._component_history.get(component.component_id, [])
            prev_count = history[-1].consecutive_failures if history else 0
            component.consecutive_failures = prev_count + 1

            # Auto-heal if threshold exceeded
            if (self._auto_heal_enabled
                    and component.consecutive_failures >= self._auto_heal_threshold):
                actions = self._healing_actions.get(component.component_id, [])
                if actions:
                    log.info(
                        "Auto-healing %s: %d consecutive failures (threshold=%d)",
                        component.component_id,
                        component.consecutive_failures,
                        self._auto_heal_threshold,
                    )
                    # Defer healing to avoid blocking the check loop
                    for action in actions:
                        asyncio.ensure_future(self._execute_auto_heal(action))
        else:
            component.consecutive_failures = 0

    async def _execute_auto_heal(self, action: HealingAction) -> None:
        """Execute an auto-healing action in the background."""
        result = await action.execute()
        self._recent_healing.append(result)
        log.info(
            "Auto-heal '%s' on %s: %s (%dms)",
            action.action_name, action.component_id,
            "success" if result.success else "failed",
            result.duration_ms,
        )

    def _store_history(self, component: ComponentHealth) -> None:
        """Store a health result in component history."""
        if component.component_id not in self._component_history:
            self._component_history[component.component_id] = []
        history = self._component_history[component.component_id]
        history.append(component)
        # Prune if over max history
        if len(history) > self._max_history:
            self._component_history[component.component_id] = history[-self._max_history:]

    # ── Snapshot ────────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """Get a snapshot of the monitor's state."""
        return {
            "registered_checks": len(self._checks),
            "registered_healing_actions": sum(
                len(actions) for actions in self._healing_actions.values()
            ),
            "components_tracked": list(self._checks.keys()),
            "auto_heal_enabled": self._auto_heal_enabled,
            "auto_heal_threshold": self._auto_heal_threshold,
            "recent_healing_count": len(self._recent_healing),
        }

    def to_terminal_display(self) -> str:
        """Human-readable terminal display."""
        lines = [
            "  ── SYSTEM HEALTH MONITOR ──",
            f"  Checks registered: {len(self._checks)}",
            f"  Healing actions: {sum(len(a) for a in self._healing_actions.values())}",
            f"  Auto-heal: {'ON' if self._auto_heal_enabled else 'OFF'} "
            f"(threshold: {self._auto_heal_threshold} failures)",
            "",
        ]

        if self._checks:
            lines.append("  Registered Components:")
            for cid in sorted(self._checks.keys()):
                check = self._checks[cid]
                lines.append(f"    [{check.component_type}] {cid}")

        if self._recent_healing:
            lines.append("")
            lines.append("  Recent Healing:")
            for a in self._recent_healing[-5:]:
                icon = "✓" if a.success else "✗"
                lines.append(f"    {icon} {a.action_name} on {a.component_id}")

        lines.append("  ── ── ── ── ── ── ── ── ──")
        return "\n".join(lines)
