# ratelimiter.py - Token bucket rate limiter for provider API calls

from __future__ import annotations

import time
import threading
import logging
from typing import Dict, Optional

log = logging.getLogger("aelvo.auth.ratelimiter")


class RateLimitConfig:
    """Per-provider rate limit configuration."""

    def __init__(
        self,
        rpm: Optional[int] = None,
        tpm: Optional[int] = None,
        rpd: Optional[int] = None,
    ) -> None:
        self.rpm = rpm
        self.tpm = tpm
        self.rpd = rpd


class TokenBucket:
    """Token bucket rate limiter for a single provider."""

    def __init__(self, rate: float, burst: int) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume(self, tokens: float = 1.0) -> bool:
        """Consume tokens from the bucket. Returns False if not enough tokens."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_time(self, tokens: float = 1.0) -> float:
        """Return seconds until the requested tokens will be available."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                return 0.0
            deficit = tokens - self._tokens
            return deficit / self._rate

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last_refill = now

    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens


class RateLimiter:
    """Multi-provider rate limiter using token buckets."""

    def __init__(self) -> None:
        self._buckets: Dict[str, Dict[str, TokenBucket]] = {}
        self._lock = threading.Lock()

    def configure(self, provider_id: str, config: RateLimitConfig) -> None:
        """Configure rate limits for a provider."""
        with self._lock:
            buckets: Dict[str, TokenBucket] = {}
            if config.rpm:
                buckets["rpm"] = TokenBucket(rate=config.rpm / 60.0, burst=config.rpm)
            if config.tpm:
                buckets["tpm"] = TokenBucket(rate=config.tpm / 60.0, burst=config.tpm)
            if config.rpd:
                buckets["rpd"] = TokenBucket(rate=config.rpd / 86400.0, burst=config.rpd)
            self._buckets[provider_id] = buckets

    def check(self, provider_id: str, tokens: float = 1.0) -> bool:
        """Check if a request can proceed. Returns True if allowed."""
        with self._lock:
            buckets = self._buckets.get(provider_id, {})
        if not buckets:
            return True
        for bucket in buckets.values():
            if not bucket.consume(tokens):
                return False
        return True

    def wait_time(self, provider_id: str, tokens: float = 1.0) -> float:
        """Return seconds until next request can proceed for provider."""
        with self._lock:
            buckets = self._buckets.get(provider_id, {})
        if not buckets:
            return 0.0
        return max(b.wait_time(tokens) for b in buckets.values())

    def configure_from_registry(self, registry: Dict[str, object]) -> None:
        """Configure rate limits from PROVIDER_REGISTRY entries."""
        for provider_id, info in registry.items():
            if not isinstance(info, dict):
                continue
            rpm = info.get("rate_limit_rpm")
            tpm = info.get("rate_limit_tpm")
            rpd = info.get("rate_limit_rpd")
            if rpm or tpm or rpd:
                self.configure(
                    provider_id,
                    RateLimitConfig(rpm=rpm, tpm=tpm, rpd=rpd),
                )


_global_limiter = RateLimiter()


def get_global_limiter() -> RateLimiter:
    return _global_limiter
