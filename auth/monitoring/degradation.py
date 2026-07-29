"""Provider degradation detection — proactive identification of provider issues."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

from ..types import ProviderStatus

logger = logging.getLogger(__name__)


class DegradationLevel(Enum):
    """Severity levels of provider degradation."""
    NONE = auto()
    MILD = auto()
    MODERATE = auto()
    SEVERE = auto()
    CRITICAL = auto()


@dataclass
class DegradationSignal:
    """A signal indicating potential provider degradation."""

    provider_id: str
    signal_type: str  # latency_spike, error_burst, rate_limit_wave, etc.
    severity: DegradationLevel
    value: float
    threshold: float
    message: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class DegradationState:
    """Current degradation state for a provider."""

    provider_id: str
    level: DegradationLevel = DegradationLevel.NONE
    signals: list[DegradationSignal] = field(default_factory=list)
    detected_at: Optional[float] = None
    last_healthy: Optional[float] = None
    is_degraded: bool = False


class DegradationDetector:
    """Detects provider degradation patterns from metrics and health data.

    Detects:
    - Latency spikes (sudden increase in response time)
    - Error bursts (clusters of errors in short time)
    - Rate limit waves (sustained rate limiting)
    - Gradual performance decline
    - Uptime degradation
    """

    def __init__(self) -> None:
        self._states: dict[str, DegradationState] = {}
        self._latency_history: dict[str, list[tuple[float, float]]] = {}
        self._error_history: dict[str, list[tuple[float, str]]] = {}
        self._rate_limit_history: dict[str, list[float]] = {}

        # Thresholds
        self.latency_spike_threshold: float = 2.0  # 2x normal
        self.error_burst_threshold: int = 5  # errors in window
        self.error_burst_window: float = 60.0  # 60 seconds
        self.rate_limit_burst_threshold: int = 3  # rate limits in window
        self.rate_limit_window: float = 120.0  # 2 minutes

    def record_latency(
        self, provider_id: str, latency_ms: float
    ) -> Optional[DegradationSignal]:
        """Record latency and check for spikes."""
        if provider_id not in self._latency_history:
            self._latency_history[provider_id] = []
        history = self._latency_history[provider_id]
        history.append((time.time(), latency_ms))

        # Keep last 100 points
        if len(history) > 100:
            self._latency_history[provider_id] = history[-100:]

        # Need at least 10 points for baseline
        if len(history) < 10:
            return None

        # Calculate baseline (median of recent history)
        recent = [l for _, l in history[-20:-5]] if len(history) >= 25 else [l for _, l in history[:-1]]
        if not recent:
            return None

        baseline = sorted(recent)[len(recent) // 2]
        if baseline == 0:
            baseline = latency_ms

        if latency_ms > baseline * self.latency_spike_threshold:
            signal = DegradationSignal(
                provider_id=provider_id,
                signal_type="latency_spike",
                severity=(
                    DegradationLevel.CRITICAL
                    if latency_ms > baseline * 5
                    else DegradationLevel.SEVERE
                    if latency_ms > baseline * 3
                    else DegradationLevel.MODERATE
                ),
                value=latency_ms,
                threshold=baseline * self.latency_spike_threshold,
                message=(
                    f"Latency spike: {latency_ms:.0f}ms "
                    f"(baseline: {baseline:.0f}ms, "
                    f"{latency_ms/baseline:.1f}x increase)"
                ),
            )
            self._record_signal(signal)
            return signal

        return None

    def record_error(
        self, provider_id: str, error: str
    ) -> Optional[DegradationSignal]:
        """Record error and check for bursts."""
        if provider_id not in self._error_history:
            self._error_history[provider_id] = []
        history = self._error_history[provider_id]
        history.append((time.time(), error))

        # Trim old entries
        cutoff = time.time() - self.error_burst_window
        recent = [(t, e) for t, e in history if t >= cutoff]
        self._error_history[provider_id] = recent

        if len(recent) >= self.error_burst_threshold:
            error_types = [e for _, e in recent]
            most_common = max(set(error_types), key=error_types.count)

            signal = DegradationSignal(
                provider_id=provider_id,
                signal_type="error_burst",
                severity=(
                    DegradationLevel.CRITICAL
                    if len(recent) >= 20
                    else DegradationLevel.SEVERE
                    if len(recent) >= 10
                    else DegradationLevel.MODERATE
                ),
                value=float(len(recent)),
                threshold=float(self.error_burst_threshold),
                message=(
                    f"Error burst: {len(recent)} errors in "
                    f"{self.error_burst_window:.0f}s "
                    f"(most common: {most_common[:50]})"
                ),
            )
            self._record_signal(signal)
            return signal

        return None

    def record_rate_limit(
        self, provider_id: str
    ) -> Optional[DegradationSignal]:
        """Record rate limit and check for waves."""
        if provider_id not in self._rate_limit_history:
            self._rate_limit_history[provider_id] = []
        history = self._rate_limit_history[provider_id]
        history.append(time.time())

        # Trim old entries
        cutoff = time.time() - self.rate_limit_window
        recent = [t for t in history if t >= cutoff]
        self._rate_limit_history[provider_id] = recent

        if len(recent) >= self.rate_limit_burst_threshold:
            signal = DegradationSignal(
                provider_id=provider_id,
                signal_type="rate_limit_wave",
                severity=(
                    DegradationLevel.SEVERE
                    if len(recent) >= 10
                    else DegradationLevel.MODERATE
                ),
                value=float(len(recent)),
                threshold=float(self.rate_limit_burst_threshold),
                message=(
                    f"Rate limit wave: {len(recent)} rate limits in "
                    f"{self.rate_limit_window:.0f}s"
                ),
            )
            self._record_signal(signal)
            return signal

        return None

    def _record_signal(self, signal: DegradationSignal) -> None:
        """Record a degradation signal and update state."""
        if signal.provider_id not in self._states:
            self._states[signal.provider_id] = DegradationState(
                provider_id=signal.provider_id
            )

        state = self._states[signal.provider_id]
        state.signals.append(signal)
        # Compare by ordinal value for enum ordering
        if signal.severity.value > state.level.value:
            state.level = signal.severity
        state.is_degraded = signal.severity != DegradationLevel.NONE
        if state.detected_at is None:
            state.detected_at = signal.timestamp

        logger.warning(
            "Degradation detected for %s: %s",
            signal.provider_id,
            signal.message,
        )

    def record_healthy(self, provider_id: str) -> None:
        """Record that a provider is healthy."""
        state = self._states.get(provider_id)
        if state is None:
            return

        state.level = DegradationLevel.NONE
        state.is_degraded = False
        state.last_healthy = time.time()
        state.detected_at = None
        state.signals.clear()

    def get_state(
        self, provider_id: str
    ) -> Optional[DegradationState]:
        return self._states.get(provider_id)

    def is_degraded(self, provider_id: str) -> bool:
        state = self._states.get(provider_id)
        return state.is_degraded if state else False

    def get_degraded_providers(
        self, min_level: DegradationLevel = DegradationLevel.MILD
    ) -> list[str]:
        return [
            pid
            for pid, state in self._states.items()
            if state.level.value >= min_level.value
        ]

    def summary(self) -> dict[str, Any]:
        return {
            pid: {
                "level": state.level.name,
                "signals_count": len(state.signals),
                "detected_at": state.detected_at,
                "last_healthy": state.last_healthy,
                "latest_signals": [
                    {
                        "type": s.signal_type,
                        "severity": s.severity.name,
                        "message": s.message,
                    }
                    for s in state.signals[-5:]
                ],
            }
            for pid, state in self._states.items()
            if state.is_degraded
        }
