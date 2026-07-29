"""Runtime metrics — granular performance and reliability metrics for
recovery, governance, and scaling subsystems.

Three metric pillars:
1. Recovery Metrics — recovery attempts, success rates, strategy distribution
2. Governance Metrics — policy evaluations, approvals, denied actions
3. Scaling Metrics — pool utilization, pipeline throughput, batch processing
"""

from __future__ import annotations

import logging
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

log = logging.getLogger("aelvo.runtime.monitoring.metrics")


class MetricType(str, Enum):
    """Categorisation of runtime metrics."""
    RECOVERY = "recovery"
    GOVERNANCE = "governance"
    SCALING = "scaling"
    HEALTH = "health"
    SYSTEM = "system"


@dataclass
class MetricPoint:
    """A single metric data point with timestamp."""
    value: float
    tags: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class MetricSeries:
    """A time-bounded series of metric points.

    Automatically trims to max_len (default 1000) and supports
    percentile, average, min/max computations.
    """

    def __init__(self, name: str, max_len: int = 1000):
        self.name = name
        self._points: deque[MetricPoint] = deque(maxlen=max_len)

    def record(self, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        self._points.append(MetricPoint(value=value, tags=tags or {}))

    @property
    def count(self) -> int:
        return len(self._points)

    @property
    def latest(self) -> Optional[float]:
        if not self._points:
            return None
        return self._points[-1].value

    @property
    def min(self) -> Optional[float]:
        if not self._points:
            return None
        return min(p.value for p in self._points)

    @property
    def max(self) -> Optional[float]:
        if not self._points:
            return None
        return max(p.value for p in self._points)

    @property
    def avg(self) -> Optional[float]:
        if not self._points:
            return None
        return sum(p.value for p in self._points) / len(self._points)

    @property
    def sum(self) -> float:
        return sum(p.value for p in self._points)

    def percentile(self, pct: float) -> Optional[float]:
        if not self._points:
            return None
        sorted_vals = sorted(p.value for p in self._points)
        n = len(sorted_vals)
        # Nearest-rank method: rank = ceil(pct/100 * n), then index = rank - 1
        import math
        rank = math.ceil(pct / 100.0 * n)
        idx = min(max(rank - 1, 0), n - 1)
        return sorted_vals[idx]

    def get_points(self) -> List[MetricPoint]:
        return list(self._points)

    def reset(self) -> None:
        self._points.clear()


class RuntimeMetricsCollector:
    """Collects and aggregates runtime metrics for recovery, governance,
    and scaling subsystems.

    Usage:
        collector = RuntimeMetricsCollector()
        collector.record_recovery_attempt("consensus", "deadlocked", True)
        collector.record_governance_evaluation("consensus", "deny")
        collector.record_pool_utilization("forge_pool", 5, 10)
        summary = collector.summary()
    """

    def __init__(self, max_series_len: int = 1000):
        self._series: Dict[str, MetricSeries] = {}
        self._max_len = max_series_len
        self._tagged_counters: Dict[str, int] = defaultdict(int)
        self._rate_windows: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=300)
        )
        self._alert_manager: Any = None

    def set_alert_manager(self, alert_manager: Any) -> None:
        """Link an AlertManager for real-time rule evaluation on every metric recording.

        Every call to record() will automatically evaluate the metric against
        registered alert rules via alert_manager.evaluate_metric().
        """
        self._alert_manager = alert_manager

    def _series_key(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> str:
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}[{tag_str}]"

    def _get_or_create(self, name: str) -> MetricSeries:
        if name not in self._series:
            self._series[name] = MetricSeries(name, max_len=self._max_len)
        return self._series[name]

    def record(
        self,
        metric_name: str,
        value: float = 1.0,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a metric value with optional tags.

        If an AlertManager is linked via set_alert_manager(), this method
        also triggers automatic alert rule evaluation via evaluate_metric().
        """
        key = self._series_key(metric_name, tags)
        series = self._get_or_create(key)
        series.record(value, tags=tags)

        # Track counter for tagged metrics
        counter_key = f"{metric_name}:{self._series_key('', tags)}" if tags else metric_name
        self._tagged_counters[counter_key] = self._tagged_counters.get(counter_key, 0) + 1

        # Real-time alert rule evaluation — fire-and-forget
        # Rules filter by metric_name in matches() — no subsystem filter needed
        if self._alert_manager is not None:
            try:
                self._alert_manager.evaluate_metric(
                    metric_name=metric_name,
                    value=value,
                )
            except Exception:
                pass  # Don't let alerting interfere with metrics collection

    # ── Recovery Metrics ────────────────────────────────────────────────

    def record_recovery_attempt(
        self,
        level: str,
        failure_type: str,
        success: bool,
        duration_ms: float = 0.0,
    ) -> None:
        """Record a recovery attempt at any level."""
        # Tagged version (for per-level/per-type breakdown)
        self.record(
            "recovery.attempt",
            tags={"level": level, "failure_type": failure_type},
        )
        # Untagged version (for aggregate summary)
        self.record("recovery.attempt")

        if success:
            self.record(
                "recovery.success",
                tags={"level": level, "failure_type": failure_type},
            )
            self.record("recovery.success")
        else:
            self.record(
                "recovery.failure",
                tags={"level": level, "failure_type": failure_type},
            )
            self.record("recovery.failure")
        if duration_ms > 0:
            self.record(
                "recovery.duration_ms",
                value=duration_ms,
                tags={"level": level},
            )

    def record_recovery_strategy(
        self, level: str, strategy: str
    ) -> None:
        """Record which recovery strategy was selected."""
        self.record(
            "recovery.strategy",
            tags={"level": level, "strategy": strategy},
        )

    def record_consensus_action(
        self, action_type: str, consensus_type: str
    ) -> None:
        """Record a consensus recovery action."""
        self.record(
            "recovery.consensus.action",
            tags={"action": action_type, "consensus_type": consensus_type},
        )

    def record_specialist_state_change(
        self, specialist: str, from_state: str, to_state: str
    ) -> None:
        """Record a specialist health state transition."""
        self.record(
            "recovery.specialist.state_change",
            tags={
                "specialist": specialist,
                "from": from_state,
                "to": to_state,
            },
        )

    def record_task_recovery_trigger(
        self, trigger: str, action: str
    ) -> None:
        """Record a task recovery trigger and the chosen action."""
        self.record(
            "recovery.task.trigger",
            tags={"trigger": trigger, "action": action},
        )

    def record_specialist_reassign(
        self, original: str, replacement: str
    ) -> None:
        """Record a specialist reassignment event."""
        self.record(
            "recovery.specialist.reassign",
            tags={"from": original, "to": replacement},
        )
        self.record("recovery.specialist.reassign")  # Aggregate

    # ── Governance Metrics ──────────────────────────────────────────────

    def record_governance_evaluation(
        self, scope: str, effect: str, policy_id: Optional[str] = None
    ) -> None:
        """Record a governance policy evaluation."""
        # Detailed tagged version
        tags = {"scope": scope, "effect": effect}
        if policy_id:
            tags["policy_id"] = policy_id
        self.record("governance.evaluation", tags=tags)
        # Aggregate versions for summary lookups
        self.record("governance.evaluation")
        self.record("governance.evaluation", tags={"effect": effect})

    def record_governance_approval(
        self, approved: bool, policy_id: str
    ) -> None:
        """Record an approval decision."""
        self.record(
            "governance.approval",
            tags={"approved": str(approved), "policy_id": policy_id},
        )
        self.record("governance.approval")

    def record_hook_execution(
        self, level: str, result: str, duration_ms: float = 0.0
    ) -> None:
        """Record a governance hook execution."""
        self.record(
            "governance.hook",
            tags={"level": level, "result": result},
        )
        self.record("governance.hook")
        if duration_ms > 0:
            self.record(
                "governance.hook.duration_ms",
                value=duration_ms,
                tags={"level": level},
            )

    # ── Scaling Metrics ─────────────────────────────────────────────────

    def record_pool_utilization(
        self, pool_name: str, active: int, capacity: int
    ) -> None:
        """Record resource pool utilization."""
        utilization = active / max(capacity, 1)
        self.record(
            "scaling.pool.utilization",
            value=utilization,
            tags={"pool": pool_name},
        )
        self.record(
            "scaling.pool.active",
            value=float(active),
            tags={"pool": pool_name},
        )
        self.record(
            "scaling.pool.capacity",
            value=float(capacity),
            tags={"pool": pool_name},
        )

    def record_pool_acquire_wait(self, pool_name: str, wait_ms: float) -> None:
        """Record resource pool acquire wait time."""
        self.record(
            "scaling.pool.acquire_wait_ms",
            value=wait_ms,
            tags={"pool": pool_name},
        )

    def record_pool_timeout(self, pool_name: str) -> None:
        """Record a resource pool acquire timeout."""
        self.record(
            "scaling.pool.timeout", tags={"pool": pool_name},
        )

    def record_pipeline_stage(
        self, pipeline: str, stage_name: str, state: str, duration_ms: float = 0.0
    ) -> None:
        """Record a pipeline stage execution."""
        self.record(
            "scaling.pipeline.stage",
            tags={"pipeline": pipeline, "stage": stage_name, "state": state},
        )
        if duration_ms > 0:
            self.record(
                "scaling.pipeline.stage_duration_ms",
                value=duration_ms,
                tags={"pipeline": pipeline, "stage": stage_name},
            )

    def record_batch_completed(
        self, batch_id: str, item_count: int, success_count: int,
        duration_ms: float,
    ) -> None:
        """Record a completed batch processing run."""
        self.record(
            "scaling.batch.completed",
            tags={"batch_id": batch_id},
        )
        self.record("scaling.batch.completed")  # Aggregate
        self.record(
            "scaling.batch.items",
            value=float(item_count),
            tags={"batch_id": batch_id},
        )
        self.record(
            "scaling.batch.success_rate",
            value=success_count / max(item_count, 1),
            tags={"batch_id": batch_id},
        )
        self.record(
            "scaling.batch.duration_ms",
            value=duration_ms,
            tags={"batch_id": batch_id},
        )

    def record_batch_error(self, batch_id: str, error_type: str) -> None:
        """Record a batch processing error."""
        self.record(
            "scaling.batch.error",
            tags={"batch_id": batch_id, "error_type": error_type},
        )
        self.record("scaling.batch.error")  # Aggregate

    # ── Rate Tracking ───────────────────────────────────────────────────

    def record_rate(self, metric_name: str) -> None:
        """Record an event for rate computation (events per second)."""
        now = time.time()
        self._rate_windows[metric_name].append(now)
        self.record(metric_name)

    def get_rate(self, metric_name: str, window_seconds: float = 60.0) -> float:
        """Get the rate of events per second over the given window."""
        now = time.time()
        cutoff = now - window_seconds
        window = self._rate_windows.get(metric_name, deque())
        # Prune old entries
        while window and window[0] < cutoff:
            window.popleft()
        if window_seconds <= 0:
            return 0.0
        return len(window) / window_seconds

    # ── Reporting ───────────────────────────────────────────────────────

    def get_series(
        self, name: str, tags: Optional[Dict[str, str]] = None
    ) -> Optional[MetricSeries]:
        key = self._series_key(name, tags)
        return self._series.get(key)

    def summary(
        self, metric_type: Optional[MetricType] = None,
    ) -> Dict[str, Any]:
        """Get a summary of all collected metrics.

        Args:
            metric_type: Optional filter by MetricType prefix.

        Returns:
            Dict mapping metric names to their stats (count, avg, min, max, p50, p95, p99).
        """
        result: Dict[str, Any] = {}
        for key, series in self._series.items():
            if series.count == 0:
                continue
            if metric_type and not key.startswith(metric_type.value):
                continue

            result[key] = {
                "count": series.count,
                "avg": series.avg,
                "min": series.min,
                "max": series.max,
                "latest": series.latest,
                "p50": series.percentile(50),
                "p95": series.percentile(95),
                "p99": series.percentile(99),
                "sum": series.sum,
            }

        return result

    def get_counters(self) -> Dict[str, int]:
        """Get tagged counter values."""
        return dict(self._tagged_counters)

    def recovery_summary(self) -> Dict[str, Any]:
        """Get a focused summary of recovery metrics."""
        attempts = self.get_series("recovery.attempt")
        successes = self.get_series("recovery.success")
        failures = self.get_series("recovery.failure")

        return {
            "total_attempts": attempts.count if attempts else 0,
            "total_successes": successes.count if successes else 0,
            "total_failures": failures.count if failures else 0,
            "success_rate": (
                successes.count / attempts.count
                if attempts and attempts.count > 0
                else 0.0
            ),
        }

    def governance_summary(self) -> Dict[str, Any]:
        """Get a focused summary of governance metrics."""
        evaluations = self.get_series("governance.evaluation")
        denied_eval = self.get_series(
            "governance.evaluation",
            tags={"effect": "deny"},
        )
        approval_eval = self.get_series(
            "governance.evaluation",
            tags={"effect": "require_approval"},
        )

        return {
            "total_evaluations": evaluations.count if evaluations else 0,
            "denied_count": denied_eval.count if denied_eval else 0,
            "approval_required_count": approval_eval.count if approval_eval else 0,
            "approval_granted": sum(
                1 for k, v in self._tagged_counters.items()
                if "governance.approval" in k and "approved=True" in k
            ),
        }

    def scaling_summary(self) -> Dict[str, Any]:
        """Get a focused summary of scaling metrics."""
        pool_util = self.get_series("scaling.pool.utilization")

        return {
            "pool_utilization_avg": pool_util.avg if pool_util else 0.0,
            "pool_utilization_max": pool_util.max if pool_util else 0.0,
            "completed_batches": (
                self.get_series("scaling.batch.completed").count
                if self.get_series("scaling.batch.completed")
                else 0
            ),
            "batch_errors": (
                self.get_series("scaling.batch.error").count
                if self.get_series("scaling.batch.error")
                else 0
            ),
        }

    def reset(self) -> None:
        """Reset all collected metrics."""
        self._series.clear()
        self._tagged_counters.clear()
        self._rate_windows.clear()
