"""Health Tracker — per-server health state management with history and diagnostics."""

from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Field
from .models import HealthState

log = logging.getLogger("aelvo.mcp.health")


class HealthSnapshot(BaseModel):
    """A point-in-time health snapshot for a server."""
    server_id: str
    state: HealthState
    latency_ms: float = 0.0
    error_rate: float = 0.0
    last_ping: Optional[datetime] = None
    diagnostics: List[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class HealthTracker:
    """Tracks per-server health with configurable window and trend analysis.

    Maintains a sliding window of health snapshots for each server
    and computes aggregate health metrics.
    """

    def __init__(self, window_size: int = 100, degrade_threshold: float = 0.3):
        self._window_size = window_size
        self._degrade_threshold = degrade_threshold  # Error rate above which server is DEGRADED
        self._snapshots: Dict[str, deque] = {}  # server_id -> deque[HealthSnapshot]
        self._current_states: Dict[str, HealthState] = {}

    # ------------------------------------------------------------------
    # Health Recording
    # ------------------------------------------------------------------

    def record_ping(self, server_id: str, latency_ms: float, success: bool) -> HealthSnapshot:
        """Record a health check result."""
        current = self._current_states.get(server_id, HealthState.UNKNOWN)

        if success:
            if current == HealthState.UNREACHABLE:
                new_state = HealthState.DEGRADED
            else:
                new_state = HealthState.HEALTHY
        else:
            # Compute recent error rate to determine if degraded
            error_rate = self._compute_error_rate(server_id)
            if error_rate > self._degrade_threshold:
                new_state = HealthState.DEGRADED

        snapshot = HealthSnapshot(
            server_id=server_id,
            state=new_state,
            latency_ms=latency_ms,
            error_rate=self._compute_error_rate(server_id),
            last_ping=datetime.now(timezone.utc),
            diagnostics=[] if success else ["Ping failed"],
        )
        self._add_snapshot(server_id, snapshot)
        self._current_states[server_id] = new_state
        return snapshot

    def record_error(self, server_id: str, error: str) -> HealthSnapshot:
        """Record an error event for a server."""
        current = self._current_states.get(server_id, HealthState.UNKNOWN)
        error_rate = self._compute_error_rate(server_id) + 0.1

        if current == HealthState.HEALTHY:
            new_state = HealthState.DEGRADED
        elif current == HealthState.UNREACHABLE:
            new_state = HealthState.UNREACHABLE
        else:
            new_state = current

        snapshot = HealthSnapshot(
            server_id=server_id,
            state=new_state,
            latency_ms=0.0,
            error_rate=min(1.0, error_rate),
            diagnostics=[error],
        )
        self._add_snapshot(server_id, snapshot)
        self._current_states[server_id] = new_state
        return snapshot

    def mark_unreachable(self, server_id: str, reason: str) -> HealthSnapshot:
        """Mark a server as unreachable."""
        snapshot = HealthSnapshot(
            server_id=server_id,
            state=HealthState.UNREACHABLE,
            diagnostics=[reason],
        )
        self._add_snapshot(server_id, snapshot)
        self._current_states[server_id] = HealthState.UNREACHABLE
        return snapshot

    def mark_healthy(self, server_id: str) -> HealthSnapshot:
        """Mark a server as healthy (recovery)."""
        snapshot = HealthSnapshot(
            server_id=server_id,
            state=HealthState.HEALTHY,
            last_ping=datetime.now(timezone.utc),
            diagnostics=["Recovered"],
        )
        self._add_snapshot(server_id, snapshot)
        self._current_states[server_id] = HealthState.HEALTHY
        return snapshot

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_state(self, server_id: str) -> HealthState:
        """Get the current health state for a server."""
        return self._current_states.get(server_id, HealthState.UNKNOWN)

    def get_history(self, server_id: str, limit: int = 20) -> List[HealthSnapshot]:
        """Get recent health history for a server."""
        snapshots = self._snapshots.get(server_id, deque())
        return list(snapshots)[-limit:]

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all server health states."""
        return {
            "total": len(self._current_states),
            "healthy": sum(1 for s in self._current_states.values() if s == HealthState.HEALTHY),
            "degraded": sum(1 for s in self._current_states.values() if s == HealthState.DEGRADED),
            "unreachable": sum(1 for s in self._current_states.values() if s == HealthState.UNREACHABLE),
            "unknown": sum(1 for s in self._current_states.values() if s == HealthState.UNKNOWN),
        }

    def get_trend(self, server_id: str) -> str:
        """Get a textual trend description for a server."""
        history = self.get_history(server_id, limit=10)
        if not history:
            return "no_data"

        states = [h.state for h in history]
        healthy_count = sum(1 for s in states if s == HealthState.HEALTHY)
        degraded_count = sum(1 for s in states if s == HealthState.DEGRADED)

        if healthy_count == len(states):
            return "stable_healthy"
        elif degraded_count > healthy_count:
            return "degrading"
        elif states[-1] == HealthState.HEALTHY and states[0] != HealthState.HEALTHY:
            return "improving"
        elif states[-1] == HealthState.UNREACHABLE:
            return "critical"
        else:
            return "fluctuating"

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _add_snapshot(self, server_id: str, snapshot: HealthSnapshot) -> None:
        """Add a snapshot to the server's sliding window."""
        if server_id not in self._snapshots:
            self._snapshots[server_id] = deque(maxlen=self._window_size)
        self._snapshots[server_id].append(snapshot)

    def _compute_error_rate(self, server_id: str) -> float:
        """Compute error rate over the recent window."""
        history = self._snapshots.get(server_id, deque())
        if not history:
            return 0.0

        # Count errors (diagnostics indicate problems)
        total = len(history)
        errors = sum(1 for h in history if h.diagnostics)
        return errors / total if total > 0 else 0.0
