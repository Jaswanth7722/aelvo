# circuit_breaker.py - Circuit breaker for provider API connections

from __future__ import annotations

import enum
import time
import logging
import threading
from typing import Callable, Dict, Optional

log = logging.getLogger("aelvo.auth.circuit_breaker")


class CircuitState(enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker for a single provider connection.

    States:
        CLOSED  → normal operation, requests pass through
        OPEN    → failures exceed threshold, requests fail fast
        HALF_OPEN → after cooldown, one test request is allowed
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_requests: int = 1,
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_requests = half_open_max_requests
        self._on_state_change = on_state_change

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: float = 0.0
        self._half_open_requests = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    @property
    def failure_count(self) -> int:
        with self._lock:
            return self._failure_count

    def allow_request(self) -> bool:
        """Check if a request should be allowed through."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self._recovery_timeout:
                    self._set_state(CircuitState.HALF_OPEN)
                    self._half_open_requests = 0
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_requests < self._half_open_max_requests:
                    self._half_open_requests += 1
                    return True
                return False

            return False

    def record_success(self) -> None:
        """Record a successful request."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._failure_count = 0
                self._half_open_requests = 0
                self._set_state(CircuitState.CLOSED)
            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed request."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                self._set_state(CircuitState.OPEN)
            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self._failure_threshold
            ):
                self._set_state(CircuitState.OPEN)

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._failure_count = 0
            self._half_open_requests = 0
            self._set_state(CircuitState.CLOSED)

    def _set_state(self, new_state: CircuitState) -> None:
        old_state = self._state
        self._state = new_state
        if old_state != new_state and self._on_state_change:
            try:
                self._on_state_change(old_state, new_state)
            except Exception:
                log.exception("Circuit breaker state change callback failed")
        log.info("Circuit breaker for provider: %s → %s", old_state.value, new_state.value)


class CircuitBreakerRegistry:
    """Registry of circuit breakers keyed by provider ID."""

    def __init__(self) -> None:
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(self, provider_id: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a provider."""
        with self._lock:
            if provider_id not in self._breakers:
                self._breakers[provider_id] = CircuitBreaker()
            return self._breakers[provider_id]

    def get_all_states(self) -> Dict[str, CircuitState]:
        """Get the state of all circuit breakers."""
        with self._lock:
            return {pid: cb.state for pid, cb in self._breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._lock:
            for cb in self._breakers.values():
                cb.reset()


_global_registry = CircuitBreakerRegistry()


def get_global_registry() -> CircuitBreakerRegistry:
    return _global_registry
